#ifndef __PULSAR_SCHEDULER_H__
#define __PULSAR_SCHEDULER_H__

#include <atomic>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "fiber.hpp"
#include "mutex.hpp"
#include "thread.hpp"
#include "utils.hpp"

namespace pulsar {
// 调度任务
class SchedulerTask {
 public:
  friend class Scheduler;
  SchedulerTask() { thread_ = -1; }
  SchedulerTask(Fiber::ptr f, int t) : fiber_(std::move(f)), thread_(t) {}
  SchedulerTask(Fiber::ptr *f, int t) {
    fiber_.swap(*f);
    thread_ = t;
  }
  SchedulerTask(std::function<void()> f, int t) : cb_(std::move(f)), thread_(t) {}
  // 清空任务
  void reset() {
    fiber_ = nullptr;
    cb_ = nullptr;
    thread_ = -1;
  }

 private:
  Fiber::ptr fiber_;
  std::function<void()> cb_;
  int thread_;
};

// N->M协程调度器
class Scheduler {
 public:
  typedef std::shared_ptr<Scheduler> ptr;

  Scheduler(size_t threads = 1, bool use_caller = true, const std::string &name = "Scheduler");
  virtual ~Scheduler();
  const std::string &getName() const { return name_; }
  // 获取当前线程调度器
  static Scheduler *GetThis();
  // 获取当前线程的调度器协程
  static Fiber *GetMainFiber();

  /**
   * \brief 添加调度任务
   * \tparam TaskType 任务类型，可以是协程对象或函数指针
   * \param task 任务
   * \param thread 指定执行函数的线程，-1为不指定
   */
  template <class TaskType>
  void scheduler(TaskType task, int thread = -1) {
    SchedulerTask schedulerTask(std::move(task), thread);
    if (enqueueTask(std::move(schedulerTask))) {
      tickle();  // 唤醒idle协程
    }
  }
  // 启动调度器
  void start();
  // 停止调度器,等待所有任务结束
  void stop();

 protected:
  // 通知调度器任务到达
  virtual void tickle();
  // 无任务时执行idle协程
  virtual void idle();
  // 返回是否可以停止
  virtual bool stopping();
  // 设置当前线程调度器
  void setThis();
  // IOManager 在 epoll 返回后会处理 timer/event；这段时间属于活跃工作，
  // 必须阻止其他 Worker 把 Scheduler 误判为已经排空。
  void setCurrentWorkerActive(bool active);
  // 返回是否有空闲进程
  bool isHasIdleThreads() { return idleThreadCnt_ > 0; }

 private:
  static constexpr size_t kInvalidWorker = std::numeric_limits<size_t>::max();

  // Each worker normally touches only its own queue. Thieves take from the
  // opposite end and never migrate a task pinned to another OS thread.
  struct alignas(64) WorkerQueue {
    Mutex mutex;
    std::deque<SchedulerTask> tasks;
    std::atomic<int> threadId{-1};
    std::atomic<bool> active{false};
  };

  bool enqueueTask(SchedulerTask task);
  bool tryTakeTask(size_t workerIndex, SchedulerTask &task);
  bool tryTakeLocal(size_t workerIndex, int threadId, SchedulerTask &task);
  bool trySteal(size_t workerIndex, int threadId, SchedulerTask &task);
  bool hasPendingTasks();
  size_t findWorkerByThread(int thread) const;
  void run(size_t workerIndex);

  // 调度器名称
  std::string name_;
  // 生命周期锁，仅保护 start/stop 和线程池；任务热路径不访问它。
  Mutex mutex_;
  // 线程池
  std::vector<Thread::ptr> threadPool_;
  // 每个 Worker 独立的双端队列；外部提交轮询分发，内部提交保持本地性。
  std::vector<std::unique_ptr<WorkerQueue>> workerQueues_;
  std::atomic<size_t> nextQueue_{0};
  // 工作线程数量（不包含use_caller的主线程）
  size_t threadCnt_ = 0;
  // IDLE线程数
  std::atomic<size_t> idleThreadCnt_ = {0};
  // 是否是use caller
  bool isUseCaller_;
  // use caller= true,调度器所在线程的调度协程
  Fiber::ptr rootFiber_;
  // use caller = true,调度器协程所在线程的id
  int rootThread_ = 0;
  std::atomic<bool> isStopped_{false};
};
}  // namespace pulsar

#endif
