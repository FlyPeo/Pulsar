#include "fiber.hpp"
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <new>
#include <utility>

#ifdef PULSAR_FIBER_GUARD_PAGES
#include <sys/mman.h>
#include <unistd.h>
#endif

#include "scheduler.hpp"
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

class Fiber::StackAllocator {
 public:
  explicit StackAllocator(Fiber *fiber) noexcept : fiber_(fiber) {}

  boost::context::stack_context allocate() {
    CondPanic(fiber_ != nullptr, "fiber stack owner is nullptr");
    if (fiber_->stackAllocation_ == nullptr) {
#ifdef PULSAR_FIBER_GUARD_PAGES
      const long pageSize = sysconf(_SC_PAGESIZE);
      if (pageSize <= 0) throw std::bad_alloc();
      const size_t guardSize = static_cast<size_t>(pageSize);
      const size_t allocationSize = fiber_->stackSize_ + guardSize;
      void *allocation = mmap(nullptr, allocationSize, PROT_READ | PROT_WRITE,
                              MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
      if (allocation == MAP_FAILED) throw std::bad_alloc();
      if (mprotect(allocation, guardSize, PROT_NONE) != 0) {
        munmap(allocation, allocationSize);
        throw std::bad_alloc();
      }
      fiber_->stackAllocation_ = allocation;
      fiber_->stackBase_ = static_cast<char *>(allocation) + guardSize;
      fiber_->stackAllocationSize_ = allocationSize;
#else
      void *allocation = std::malloc(fiber_->stackSize_);
      if (allocation == nullptr) throw std::bad_alloc();
      fiber_->stackAllocation_ = allocation;
      fiber_->stackBase_ = allocation;
      fiber_->stackAllocationSize_ = fiber_->stackSize_;
#endif
    }

    boost::context::stack_context context;
    context.size = fiber_->stackSize_;
    context.sp = static_cast<char *>(fiber_->stackBase_) + fiber_->stackSize_;
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
    : id_(cur_fiber_id++), cb_(std::move(cb)), isRunInScheduler_(run_inscheduler) {
  stackSize_ = stacksize > 0 ? stacksize : g_fiber_stack_size;
  try {
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
  StackAllocator allocator(this);
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

// 协程重置。Boost.Context 的 fiber 句柄是 one-shot，结束后创建新上下文。
// TODO:暂时不允许Ready状态下的重置
void Fiber::reset(std::function<void()> cb) {
  CondPanic(!isMainFiber_, "main fiber cannot be reset");
  CondPanic(state_ == TERM, "state isn't TERM");
  CondPanic(!context_, "terminated fiber context should be empty");
  cb_ = std::move(cb);
  exception_ = nullptr;
  context_ = CreateContext();
  state_ = READY;
}

void Fiber::ReleaseStack() noexcept {
#ifdef PULSAR_FIBER_GUARD_PAGES
  if (stackAllocation_ != nullptr) {
    munmap(stackAllocation_, stackAllocationSize_);
  }
#else
  std::free(stackAllocation_);
#endif
  stackAllocation_ = nullptr;
  stackBase_ = nullptr;
  stackAllocationSize_ = 0;
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
