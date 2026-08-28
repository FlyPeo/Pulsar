#ifndef __PULSAR_FIBER_H__
#define __PULSAR_FIBER_H__

#include <boost/context/fiber.hpp>

#include <atomic>
#include <exception>
#include <functional>
#include <memory>
#include <stddef.h>
#include <stdint.h>

#include "utils.hpp"

#ifdef BOOST_USE_UCONTEXT
#error "Pulsar requires Boost.Context's native fcontext backend"
#endif

namespace pulsar {
class Fiber : public std::enable_shared_from_this<Fiber> {
 public:
  typedef std::shared_ptr<Fiber> ptr;
  // Fiber状态机
  enum State {
    // 就绪态，刚创建后者yield后状态
    READY,
    // 运行态，resume之后的状态
    RUNNING,
    // 结束态，协程的回调函数执行完之后的状态
    TERM,
  };

 private:
  // 初始化当前线程的协程功能，构造线程主协程对象
  Fiber();

 public:
  // 构造子协程
  Fiber(std::function<void()> cb, size_t stackSz = 0, bool run_in_scheduler = true);
  ~Fiber();
  // 重置协程状态，复用栈空间
  void reset(std::function<void()> cb);
  // 切换协程到运行态
  void resume();
  // 让出协程执行权
  void yield();
  // 获取协程Id
  uint64_t getId() const { return id_; }
  // 获取协程状态
  State getState() const { return state_; }

  // 设置当前正在运行的协程
  static void SetThis(Fiber *f);
  // 获取当前线程中的执行线程
  // 如果当前线程没有创建协程，则创建第一个协程，且该协程为当前线程的
  // 主协程，其他协程通过该协程来调度
  static Fiber::ptr GetThis();
  // 协程总数
  static uint64_t TotalFiberNum();
  // 协程回调函数
  static void MainFunc();
  // 获取当前协程Id
  static uint64_t GetCurFiberID();

 private:
  class StackAllocator;

  // 使用 Boost.Context 创建一个拥有独立栈的可恢复执行上下文。
  boost::context::fiber CreateContext();
  void ReleaseStack() noexcept;

  // 协程ID
  uint64_t id_ = 0;
  // 协程栈大小
  uint32_t stackSize_ = 0;
  // 协程状态
  State state_ = READY;
  // Boost.Context 句柄是 move-only/one-shot：每次 resume 后必须保存返回的新句柄。
  boost::context::fiber context_;
  boost::context::fiber callerContext_;
  // 栈由 Fiber 持有，Boost.Context 结束一次执行后不释放，reset 可安全复用。
  void *stackAllocation_ = nullptr;
  void *stackBase_ = nullptr;
  size_t stackAllocationSize_ = 0;
  // 协程回调函数
  std::function<void()> cb_;
  // Fiber 入口捕获异常，切回调用者后再抛出，禁止异常跨越 fcontext 边界。
  std::exception_ptr exception_;
  // state_ 表示生命周期；该门闩额外拒绝两个线程同时恢复同一 Fiber。
  std::atomic_flag resumeInProgress_ = ATOMIC_FLAG_INIT;
  // 本协程是否参与调度器调度
  bool isRunInScheduler_ = false;
  // 主协程没有独立 Boost.Context 栈。
  bool isMainFiber_ = false;
};
}  // namespace pulsar

#endif
