#ifndef PULSAR_IOMANAGER_HPP
#define PULSAR_IOMANAGER_HPP

#include "fcntl.h"
#include "scheduler.hpp"
#include "string.h"
#include "sys/epoll.h"
#include "timer.hpp"

#include <type_traits>

namespace pulsar {
class BlockingThreadPool;

enum Event {
  NONE = 0x0,
  READ = 0x1,
  WRITE = 0x4,
};

struct EventContext {
  Scheduler *scheduler = nullptr;
  Fiber::ptr fiber;
  std::function<void()> cb;
  // Resume an I/O waiter on the worker that parked it. A stackful Fiber owns
  // thread-local scheduler state and must not race with its original worker
  // by being resumed on an arbitrary epoll worker.
  int thread_id = -1;
};

class FdContext {
  friend class IOManager;

 public:
  // 获取事件上下文
  EventContext &getEveContext(Event event);
  // 重置事件上下文
  void resetEveContext(EventContext &ctx);
  // 触发事件
  void triggerEvent(Event event);

 private:
  EventContext read;
  EventContext write;
  int fd = 0;
  Event events = NONE;
  Mutex mutex;
};

class IOManager : public Scheduler, public TimerManager {
 public:
  typedef std::shared_ptr<IOManager> ptr;

  IOManager(size_t threads = 1, bool use_caller = true, const std::string &name = "IOManager",
            size_t blocking_threads = 4);
  IOManager(size_t threads, bool use_caller, const std::string &name,
            SchedulerReuseOptions reuseOptions, size_t blocking_threads = 4);
  ~IOManager();
  // 添加事件
  int addEvent(int fd, Event event, std::function<void()> cb = nullptr);
  // 删除事件
  bool delEvent(int fd, Event event);
  // 取消事件
  bool cancelEvent(int fd, Event event);
  // 取消所有事件
  bool cancelAll(int fd);
  static IOManager *GetThis();

  // 提交阻塞任务至专用线程池，完成后重新将协程调度回原 worker
  void submitBlocking(std::function<void()> task);

  // 在独立阻塞线程池中执行阻塞操作，当前协程让出执行权，完成后自动 resume
  template <typename F>
  auto asyncBlocking(F &&f) -> typename std::invoke_result<F>::type {
    using R = typename std::invoke_result<F>::type;
    Fiber::ptr cur = Fiber::GetThis();
    if (!cur || cur->getId() == 0) {
      return f();
    }

    struct State {
      typename std::conditional<std::is_void<R>::value, int, R>::type result{};
      int savedErrno = 0;
      std::exception_ptr ex;
    };

    auto state = std::make_shared<State>();
    int thread_id = GetThreadId();

    pendingBlockingTasks_++;
    submitBlocking([state, f = std::forward<F>(f), cur, this, thread_id]() mutable {
      try {
        if constexpr (std::is_void<R>::value) {
          f();
        } else {
          state->result = f();
        }
        state->savedErrno = errno;
      } catch (...) {
        state->ex = std::current_exception();
      }
      this->scheduler(cur, thread_id);
      this->pendingBlockingTasks_--;
    });

    cur->yield();

    if (state->ex) {
      std::rethrow_exception(state->ex);
    }
    errno = state->savedErrno;
    if constexpr (!std::is_void<R>::value) {
      return std::move(state->result);
    }
  }

  template <typename F>
  static auto AsyncBlocking(F &&f) -> typename std::invoke_result<F>::type {
    IOManager *iom = IOManager::GetThis();
    if (iom) {
      return iom->asyncBlocking(std::forward<F>(f));
    }
    return f();
  }

 protected:
  // 通知调度器有任务要调度
  void tickle() override;
  // 判断是否可以停止
  bool stopping() override;
  // idle协程
  void idle() override;
  // 判断是否可以停止，同时获取最近一个定时超时时间
  bool stopping(uint64_t &timeout);

  void OnTimerInsertedAtFront() override;
  void contextResize(size_t size);

 private:
  int epfd_ = 0;
  int tickleFds_[2];
  // 正在等待执行的IO事件数量
  std::atomic<size_t> pendingEventCnt_ = {0};
  std::atomic<size_t> pendingBlockingTasks_ = {0};
  std::unique_ptr<BlockingThreadPool> blockingThreadPool_;
  RWMutex mutex_;
  std::vector<FdContext *> fdContexts_;
};
}  // namespace pulsar

#endif  // PULSAR_IOMANAGER_HPP
