#include "scheduler.hpp"

#include <utility>

#include "fiber.hpp"
#include "hook.hpp"

namespace pulsar {
// 当前线程的调度器，同一调度器下的所有线程共享同一调度器实例 （线程级调度器）
static thread_local Scheduler *cur_scheduler = nullptr;
// 当前线程的调度协程，每个线程一个 (协程级调度器)
static thread_local Fiber *cur_scheduler_fiber = nullptr;
// 当前线程在调度器中的 Worker 下标；调度器外部线程使用无效值。
static thread_local size_t cur_worker_index = std::numeric_limits<size_t>::max();

const std::string LOG_HEAD = "[scheduler] ";
static std::atomic<uint64_t> next_scheduler_owner_id{1};

Scheduler::Scheduler(size_t threads, bool use_caller, const std::string &name)
    : Scheduler(threads, use_caller, name, SchedulerReuseOptions{}) {}

Scheduler::Scheduler(size_t threads, bool use_caller, const std::string &name,
                     SchedulerReuseOptions reuseOptions)
    : reuseOptions_(std::move(reuseOptions)),
      schedulerOwnerId_(next_scheduler_owner_id.fetch_add(1, std::memory_order_relaxed)) {
  CondPanic(threads > 0, "threads <= 0");
  if (!reuseOptions_.stackAllocator) {
    reuseOptions_.stackAllocator = GetDefaultFiberStackAllocator();
  }

  isUseCaller_ = use_caller;
  name_ = name;

  workerQueues_.reserve(threads);
  for (size_t i = 0; i < threads; ++i) {
    workerQueues_.emplace_back(new WorkerQueue);
    workerQueues_.back()->callbackFiberCache.reserve(reuseOptions_.callbackFiberCachePerWorker);
  }

  // use_caller:是否将当前线程也作为被调度线程
  if (use_caller) {
#ifndef NDEBUG
    std::cout << LOG_HEAD << "current thread as called thread" << std::endl;
#endif
    // 总线程数减1
    --threads;
    // 初始化caller线程的主协程
    Fiber::GetThis();
#ifndef NDEBUG
    std::cout << LOG_HEAD << "init caller thread's main fiber success" << std::endl;
#endif
    CondPanic(GetThis() == nullptr, "GetThis err:cur scheduler is not nullptr");
    // 设置当前线程为调度器线程（caller thread）
    cur_scheduler = this;
    // 初始化当前线程的调度协程 （该线程不会被调度器带哦都），调度结束后，返回主协程
    rootFiber_.reset(
        new Fiber(std::bind(&Scheduler::run, this, 0), 0, false, reuseOptions_.stackAllocator));
#ifndef NDEBUG
    std::cout << LOG_HEAD << "init caller thread's caller fiber success" << std::endl;
#endif

    Thread::SetName(name_);
    cur_scheduler_fiber = rootFiber_.get();
    rootThread_ = GetThreadId();
    workerQueues_[0]->threadId.store(rootThread_, std::memory_order_release);
  } else {
    rootThread_ = -1;
  }
  threadCnt_ = threads;
#ifndef NDEBUG
  std::cout << "-------scheduler init success-------" << std::endl;
#endif
}

Scheduler *Scheduler::GetThis() { return cur_scheduler; }
Fiber *Scheduler::GetMainFiber() { return cur_scheduler_fiber; }
SchedulerReuseStats Scheduler::getReuseStats() const {
  SchedulerReuseStats result;
  result.stack = reuseOptions_.stackAllocator->stats();
  result.callbackCacheHits = callbackCacheHits_.load(std::memory_order_relaxed);
  result.callbackCacheMisses = callbackCacheMisses_.load(std::memory_order_relaxed);
  result.callbackCacheEvictions = callbackCacheEvictions_.load(std::memory_order_relaxed);
  result.callbackCachedCount = callbackCachedCount_.load(std::memory_order_relaxed);
  result.callbackCachedBytes = callbackCachedBytes_.load(std::memory_order_relaxed);
  return result;
}
void Scheduler::setThis() { cur_scheduler = this; }
void Scheduler::setCurrentWorkerActive(bool active) {
  CondPanic(GetThis() == this && cur_worker_index < workerQueues_.size(),
            "current thread is not a scheduler worker");
  workerQueues_[cur_worker_index]->active.store(active, std::memory_order_release);
}
Scheduler::~Scheduler() {
  CondPanic(isStopped_.load(std::memory_order_acquire), "isstopped is false");
  if (GetThis() == this) {
    cur_scheduler = nullptr;
  }
}

// 调度器启动
// 初始化调度线程池
void Scheduler::start() {
#ifndef NDEBUG
  std::cout << LOG_HEAD << "scheduler start" << std::endl;
#endif
  Mutex::Lock lock(mutex_);
  if (isStopped_.load(std::memory_order_acquire)) {
#ifndef NDEBUG
    std::cout << "scheduler has stopped" << std::endl;
#endif
    return;
  }
  CondPanic(threadPool_.empty(), "thread pool is not empty");
  threadPool_.resize(threadCnt_);
  for (size_t i = 0; i < threadCnt_; i++) {
    const size_t workerIndex = i + (isUseCaller_ ? 1 : 0);
    threadPool_[i].reset(
        new Thread(std::bind(&Scheduler::run, this, workerIndex), name_ + "_" + std::to_string(i)));
  }
}

size_t Scheduler::findWorkerByThread(int thread) const {
  if (thread == -1) return kInvalidWorker;
  for (size_t i = 0; i < workerQueues_.size(); ++i) {
    if (workerQueues_[i]->threadId.load(std::memory_order_acquire) == thread) return i;
  }
  return kInvalidWorker;
}

bool Scheduler::enqueueTask(SchedulerTask task) {
  if (!task.fiber_ && !task.cb_) return false;

  size_t workerIndex = kInvalidWorker;
  if (task.thread_ != -1) {
    workerIndex = findWorkerByThread(task.thread_);
  } else if (GetThis() == this && cur_worker_index < workerQueues_.size()) {
    // Tasks spawned by a Worker stay local; idle Workers steal when needed.
    workerIndex = cur_worker_index;
  }
  if (workerIndex == kInvalidWorker) {
    workerIndex = nextQueue_.fetch_add(1, std::memory_order_relaxed) % workerQueues_.size();
  }

  WorkerQueue &queue = *workerQueues_[workerIndex];
  bool wasEmpty = false;
  {
    Mutex::Lock lock(queue.mutex);
    wasEmpty = queue.tasks.empty();
    queue.tasks.emplace_back(std::move(task));
  }
  return wasEmpty;
}

bool Scheduler::tryTakeLocal(size_t workerIndex, int threadId, SchedulerTask &task) {
  WorkerQueue &queue = *workerQueues_[workerIndex];
  Mutex::Lock lock(queue.mutex);
  for (auto it = queue.tasks.begin(); it != queue.tasks.end(); ++it) {
    if (it->thread_ == -1 || it->thread_ == threadId) {
      task = std::move(*it);
      queue.tasks.erase(it);
      return true;
    }
  }
  return false;
}

bool Scheduler::trySteal(size_t workerIndex, int threadId, SchedulerTask &task) {
  const size_t workerCount = workerQueues_.size();
  for (size_t offset = 1; offset < workerCount; ++offset) {
    const size_t victimIndex = (workerIndex + offset) % workerCount;
    WorkerQueue &victim = *workerQueues_[victimIndex];
    Mutex::Lock lock(victim.mutex);
    for (auto it = victim.tasks.end(); it != victim.tasks.begin();) {
      --it;
      // An unpinned task may migrate. A pinned task is only taken by its
      // owning OS thread, including tasks queued before that Worker registered.
      if (it->thread_ == -1 || it->thread_ == threadId) {
        task = std::move(*it);
        victim.tasks.erase(it);
        return true;
      }
    }
  }
  return false;
}

bool Scheduler::tryTakeTask(size_t workerIndex, SchedulerTask &task) {
  const int threadId = workerQueues_[workerIndex]->threadId.load(std::memory_order_acquire);
  return tryTakeLocal(workerIndex, threadId, task) || trySteal(workerIndex, threadId, task);
}

bool Scheduler::hasPendingTasks() {
  for (const auto &queuePtr : workerQueues_) {
    WorkerQueue &queue = *queuePtr;
    Mutex::Lock lock(queue.mutex);
    if (!queue.tasks.empty()) return true;
  }
  return false;
}

Fiber::ptr Scheduler::acquireCallbackFiber(size_t workerIndex, std::function<void()> cb) {
  WorkerQueue &worker = *workerQueues_[workerIndex];
  if (!worker.callbackFiberCache.empty()) {
    Fiber::ptr fiber = std::move(worker.callbackFiberCache.back());
    worker.callbackFiberCache.pop_back();
    callbackCacheHits_.fetch_add(1, std::memory_order_relaxed);
    callbackCachedCount_.fetch_sub(1, std::memory_order_relaxed);
    callbackCachedBytes_.fetch_sub(fiber->stackBlock_.allocationSize, std::memory_order_relaxed);
    fiber->reset(std::move(cb));
    return fiber;
  }

  callbackCacheMisses_.fetch_add(1, std::memory_order_relaxed);
  Fiber::ptr fiber(new Fiber(std::move(cb), 0, true, reuseOptions_.stackAllocator));
  fiber->MarkSchedulerCallback(schedulerOwnerId_, workerIndex);
  return fiber;
}

void Scheduler::tryCacheCallbackFiber(size_t workerIndex, Fiber::ptr &fiber) noexcept {
  if (!fiber || fiber.use_count() != 1 ||
      !fiber->IsReusableSchedulerCallback(schedulerOwnerId_, workerIndex)) {
    return;
  }

  WorkerQueue &worker = *workerQueues_[workerIndex];
  if (worker.callbackFiberCache.size() >= reuseOptions_.callbackFiberCachePerWorker) {
    callbackCacheEvictions_.fetch_add(1, std::memory_order_relaxed);
    return;
  }

  const uint64_t stackBytes = fiber->stackBlock_.allocationSize;
  try {
    worker.callbackFiberCache.push_back(std::move(fiber));
    callbackCachedCount_.fetch_add(1, std::memory_order_relaxed);
    callbackCachedBytes_.fetch_add(stackBytes, std::memory_order_relaxed);
  } catch (...) {
    callbackCacheEvictions_.fetch_add(1, std::memory_order_relaxed);
  }
}

void Scheduler::clearCallbackFiberCaches() noexcept {
  for (const auto &queuePtr : workerQueues_) {
    WorkerQueue &worker = *queuePtr;
    for (const Fiber::ptr &fiber : worker.callbackFiberCache) {
      callbackCachedCount_.fetch_sub(1, std::memory_order_relaxed);
      callbackCachedBytes_.fetch_sub(fiber->stackBlock_.allocationSize, std::memory_order_relaxed);
    }
    worker.callbackFiberCache.clear();
  }
}

// 调度协程
void Scheduler::run(size_t workerIndex) {
#ifndef NDEBUG
  std::cout << LOG_HEAD << "begin run" << std::endl;
#endif
  CondPanic(workerIndex < workerQueues_.size(), "worker index out of range");
  set_hook_enable(true);
  setThis();
  cur_worker_index = workerIndex;
  WorkerQueue &worker = *workerQueues_[workerIndex];
  worker.threadId.store(GetThreadId(), std::memory_order_release);
  if (GetThreadId() != rootThread_) {
    // 如果当前线程不是caller线程，则初始化该线程的调度协程
    cur_scheduler_fiber = Fiber::GetThis().get();
  }

  // 创建idle协程
  Fiber::ptr idleFiber(
      new Fiber(std::bind(&Scheduler::idle, this), 0, true, reuseOptions_.stackAllocator));

  SchedulerTask task;
  while (true) {
    task.reset();
    worker.active.store(true, std::memory_order_release);
    const bool hasTask = tryTakeTask(workerIndex, task);

    if (hasTask && task.fiber_) {
      CondPanic(task.fiber_->getState() == Fiber::READY, "fiber task state error");
      // 开始执行 协程任务
      try {
        task.fiber_->resume();
      } catch (const std::exception &error) {
        std::cerr << LOG_HEAD << "fiber callback failed: " << error.what() << std::endl;
      } catch (...) {
        std::cerr << LOG_HEAD << "fiber callback failed with unknown exception" << std::endl;
      }
      tryCacheCallbackFiber(workerIndex, task.fiber_);
      // 执行结束
      task.reset();
    } else if (hasTask && task.cb_) {
      Fiber::ptr cbFiber;
      try {
        cbFiber = acquireCallbackFiber(workerIndex, std::move(task.cb_));
      } catch (const std::exception &error) {
        std::cerr << LOG_HEAD << "callback fiber creation failed: " << error.what() << std::endl;
        task.reset();
        continue;
      } catch (...) {
        std::cerr << LOG_HEAD << "callback fiber creation failed with unknown exception" << std::endl;
        task.reset();
        continue;
      }
      task.reset();
      try {
        cbFiber->resume();
      } catch (const std::exception &error) {
        std::cerr << LOG_HEAD << "callback fiber failed: " << error.what() << std::endl;
      } catch (...) {
        std::cerr << LOG_HEAD << "callback fiber failed with unknown exception" << std::endl;
      }
      // A yielded callback remains owned by Hook/Timer/synchronization state.
      // Only a terminal, unaliased Scheduler callback can enter this Worker's
      // bounded object cache.
      tryCacheCallbackFiber(workerIndex, cbFiber);
    } else {
      worker.active.store(false, std::memory_order_release);
      // 任务队列为空
      if (idleFiber->getState() == Fiber::TERM) {
#ifndef NDEBUG
        std::cout << "idle fiber term" << std::endl;
#endif
        break;
      }
      // A pipe tickle may wake a Worker that cannot consume a task pinned to
      // another Worker. Propagate the notification before blocking again.
      if (hasPendingTasks()) tickle();
      // idle协程不断空轮转
      ++idleThreadCnt_;
      idleFiber->resume();
      --idleThreadCnt_;
    }
  }
  worker.active.store(false, std::memory_order_release);
  cur_worker_index = kInvalidWorker;
#ifndef NDEBUG
  std::cout << "run exit" << std::endl;
#endif
}

void Scheduler::tickle() {
#ifndef NDEBUG
  std::cout << "tickle" << std::endl;
#endif
}

bool Scheduler::stopping() {
  if (!isStopped_.load(std::memory_order_acquire)) return false;
  if (hasPendingTasks()) return false;
  for (const auto &queue : workerQueues_) {
    if (queue->active.load(std::memory_order_acquire)) return false;
  }
  return true;
}

void Scheduler::idle() {
  while (!stopping()) {
    Fiber::GetThis()->yield();
  }
}

// 使用caller线程，则调度线程依赖stop()来执行caller线程的调度协程
// 不使用caller线程，只用caller线程去调度，则调度器真正开始执行的位置是stop()
void Scheduler::stop() {
#ifndef NDEBUG
  std::cout << LOG_HEAD << "stop" << std::endl;
#endif
  if (stopping()) {
    return;
  }
  isStopped_.store(true, std::memory_order_release);

  // stop指令只能由caller线程发起
  if (isUseCaller_) {
    CondPanic(GetThis() == this, "cur thread is not caller thread");
  } else {
    CondPanic(GetThis() != this, "cur thread is caller thread");
  }

  for (size_t i = 0; i < threadCnt_; i++) {
    tickle();
  }
  if (rootFiber_) {
    tickle();
  }

  // 在user_caller情况下，调度器协程（rootFiber）结束后，应该返回caller协程
  if (rootFiber_) {
    // 切换到调度协程，开始调度
    rootFiber_->resume();
#ifndef NDEBUG
    std::cout << "root fiber end" << std::endl;
#endif
  }

  std::vector<Thread::ptr> threads;
  {
    Mutex::Lock lock(mutex_);
    threads.swap(threadPool_);
  }
  for (auto &i : threads) {
    i->join();
  }
  clearCallbackFiberCaches();
}

}  // namespace pulsar
