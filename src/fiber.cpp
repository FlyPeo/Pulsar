#include "fiber.hpp"
#include <atomic>
#include <iostream>
#include <new>
#include <utility>

#include "scheduler.hpp"
#include "stack_allocator.hpp"
#include "utils.hpp"

namespace pulsar {
const bool DEBUG = true;
// 当前线程正在运行的协程
static thread_local Fiber *cur_fiber = nullptr;
// 当前线程的主协程
static thread_local Fiber::ptr cur_thread_fiber = nullptr;
// 用于生成协程Id
static std::atomic<uint64_t> cur_fiber_id{0};
// 统计当前协程数
static std::atomic<uint64_t> fiber_count{0};
// 协程栈默认大小 128 KiB，与迁移前的 ucontext 实现保持一致。
static constexpr size_t g_fiber_stack_size = 128 * 1024;

class Fiber::ContextStackAllocator {
 public:
  explicit ContextStackAllocator(Fiber *fiber) noexcept : fiber_(fiber) {}

  boost::context::stack_context allocate() {
    CondPanic(fiber_ != nullptr, "fiber stack owner is nullptr");
    CondPanic(static_cast<bool>(fiber_->stackBlock_), "fiber stack block is empty");

    boost::context::stack_context context;
    context.size = fiber_->stackBlock_.usableSize;
    context.sp = fiber_->stackBlock_.stackPointer();
    return context;
  }

  // Fiber owns the allocation so a terminated one-shot context can be reset
  // on the same stack. Fiber::~Fiber performs the single matching release.
  void deallocate(boost::context::stack_context &) noexcept {}

 private:
  Fiber *fiber_;
};

// only for GetThis
Fiber::Fiber() {
  SetThis(this);
  state_ = RUNNING;
  isMainFiber_ = true;
  ++fiber_count;
  id_ = cur_fiber_id++;
#ifndef NDEBUG
  std::cout << "[fiber] create fiber , id = " << id_ << std::endl;
#endif
  //",backtrace:\n"<< BacktraceToString(6, 3, "") << std::endl;
}

// 设置当前协程
void Fiber::SetThis(Fiber *f) { cur_fiber = f; }
// 获取当前执行协程，不存在则创建
Fiber::ptr Fiber::GetThis() {
  if (cur_fiber) {
    return cur_fiber->shared_from_this();
  }
  // 创建主协程并初始化
  Fiber::ptr main_fiber(new Fiber);
  CondPanic(cur_fiber == main_fiber.get(), "cur_fiber need to be main_fiber");
  cur_thread_fiber = main_fiber;
  return cur_fiber->shared_from_this();
}

uint64_t Fiber::TotalFiberNum() { return fiber_count.load(std::memory_order_relaxed); }

// 有参构造，并为新的子协程创建栈空间
Fiber::Fiber(std::function<void()> cb, size_t stacksize, bool run_inscheduler)
    : Fiber(std::move(cb), stacksize, run_inscheduler, GetDefaultFiberStackAllocator()) {}

Fiber::Fiber(std::function<void()> cb, size_t stacksize, bool run_inscheduler,
             std::shared_ptr<FiberStackAllocator> stackAllocator)
    : id_(cur_fiber_id++),
      stackAllocator_(stackAllocator ? std::move(stackAllocator) : GetDefaultFiberStackAllocator()),
      cb_(std::move(cb)),
      isRunInScheduler_(run_inscheduler) {
  const size_t requestedStackSize = stacksize > 0 ? stacksize : g_fiber_stack_size;
  try {
    stackBlock_ = stackAllocator_->acquire(requestedStackSize);
    if (!stackBlock_ || stackBlock_.stackBase == nullptr ||
        stackBlock_.usableSize < requestedStackSize) {
      throw std::bad_alloc();
    }
    stackSize_ = stackBlock_.usableSize;
    context_ = CreateContext();
  } catch (...) {
    ReleaseStack();
    throw;
  }
  ++fiber_count;

  // std::cout << "create son fiber , id = " << id_ << ",backtrace:\n"
  //           << BacktraceToString(6, 3, "") << std::endl;
  // std::cout << "[fiber]create son fiber , id = " << id_ << std::endl;
}

boost::context::fiber Fiber::CreateContext() {
  stackAllocator_->beforeContextCreate(stackBlock_);
  ContextStackAllocator allocator(this);
  return boost::context::fiber(
      std::allocator_arg, std::move(allocator),
      [this](boost::context::fiber &&caller) mutable {
        callerContext_ = std::move(caller);
        SetThis(this);
        MainFunc();
        return std::move(callerContext_);
      });
}

// 切换当前协程到执行态,并保存主协程的上下文
void Fiber::resume() {
  if (resumeInProgress_.test_and_set(std::memory_order_acquire)) {
    throw std::logic_error("fiber is already being resumed");
  }
  struct ResumeGuard {
    std::atomic_flag &flag;
    ~ResumeGuard() { flag.clear(std::memory_order_release); }
  } guard{resumeInProgress_};

  CondPanic(state_ != TERM && state_ != RUNNING, "state error");
  CondPanic(static_cast<bool>(context_), "fiber context is empty");

  Fiber *caller = cur_fiber;
  CondPanic(caller != nullptr, "caller fiber is nullptr");
  SetThis(this);
  state_ = RUNNING;
  context_ = std::move(context_).resume();
  SetThis(caller);

  if (state_ == TERM && exception_) {
    auto exception = std::exchange(exception_, nullptr);
    std::rethrow_exception(exception);
  }
}

// 当前协程让出执行权
// 协程执行完成之后会自动回到主协程，此时状态为 TERM。
void Fiber::yield() {
  CondPanic(state_ == RUNNING, "state error");
  CondPanic(static_cast<bool>(callerContext_), "caller context is empty");

  state_ = READY;
  Fiber *return_fiber = isRunInScheduler_ ? Scheduler::GetMainFiber() : cur_thread_fiber.get();
  CondPanic(return_fiber != nullptr, "return fiber is nullptr");
  SetThis(return_fiber);
  callerContext_ = std::move(callerContext_).resume();
  SetThis(this);
}

// 协程入口函数
void Fiber::MainFunc() {
  Fiber::ptr cur = GetThis();
  CondPanic(cur != nullptr, "cur is nullptr");

  try {
    cur->cb_();
  } catch (...) {
    cur->exception_ = std::current_exception();
  }
  cur->cb_ = nullptr;
  cur->state_ = TERM;

  Fiber *return_fiber = cur->isRunInScheduler_ ? Scheduler::GetMainFiber() : cur_thread_fiber.get();
  CondPanic(return_fiber != nullptr, "return fiber is nullptr");
  SetThis(return_fiber);
}

// Reset a terminated Fiber by constructing a fresh one-shot context.
void Fiber::reset(std::function<void()> cb) {
  CondPanic(!isMainFiber_, "main fiber cannot be reset");
  CondPanic(state_ == TERM, "state isn't TERM");
  CondPanic(!context_, "terminated fiber context should be empty");
  boost::context::fiber newContext = CreateContext();
  cb_ = std::move(cb);
  exception_ = nullptr;
  context_ = std::move(newContext);
  state_ = READY;
}

void Fiber::ReleaseStack() noexcept {
  if (stackBlock_) {
    CondPanic(stackAllocator_ != nullptr, "fiber stack allocator is nullptr");
    stackAllocator_->release(std::move(stackBlock_));
  }
  stackAllocator_.reset();
  stackSize_ = 0;
}

void Fiber::MarkSchedulerCallback(uint64_t schedulerOwnerId, size_t originWorker) noexcept {
  isSchedulerCallback_ = true;
  schedulerOwnerId_ = schedulerOwnerId;
  schedulerOriginWorker_ = originWorker;
}

bool Fiber::IsReusableSchedulerCallback(uint64_t schedulerOwnerId, size_t originWorker) const noexcept {
  return isSchedulerCallback_ && schedulerOwnerId_ == schedulerOwnerId &&
         schedulerOriginWorker_ == originWorker && !isMainFiber_ && state_ == TERM && !context_ &&
         !callerContext_;
}

Fiber::~Fiber() {
  --fiber_count;
  if (!isMainFiber_) {
    CondPanic(state_ == TERM, "fiber state should be term");
    ReleaseStack();
  } else {
    CondPanic(!cb_, "main fiber no callback");
    CondPanic(state_ == RUNNING, "main fiber state should be running");

    Fiber *cur = cur_fiber;
    if (cur == this) {
      SetThis(nullptr);
    }
  }
}

}  // namespace pulsar
