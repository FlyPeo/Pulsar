#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <set>
#include <vector>

#include <pulsar/fiber.hpp>
#include <pulsar/scheduler.hpp>
#include <pulsar/utils.hpp>

int main() {
  constexpr size_t kWorkerCount = 4;
  constexpr size_t kStealableTasks = 20000;
  constexpr size_t kPinnedTasks = 1000;
  constexpr size_t kTotalTasks = kStealableTasks + kPinnedTasks;

  pulsar::Scheduler scheduler(kWorkerCount, false, "work-stealing-check");
  std::vector<int> executedBy(kStealableTasks, -1);
  std::vector<uint64_t> fiberIds(kStealableTasks, 0);
  std::atomic<size_t> completed{0};
  std::atomic<size_t> affinityFailures{0};
  auto finished = std::make_shared<std::promise<void>>();
  std::future<void> result = finished->get_future();

  auto complete = [&]() {
    if (completed.fetch_add(1, std::memory_order_acq_rel) + 1 == kTotalTasks) finished->set_value();
  };

  // This producer runs on one Worker. All unpinned child tasks therefore enter
  // its local queue and require idle peers to steal in order to spread out.
  scheduler.scheduler([&]() {
    const int ownerThread = pulsar::GetThreadId();
    for (size_t i = 0; i < kStealableTasks; ++i) {
      scheduler.scheduler([&, i]() {
        executedBy[i] = pulsar::GetThreadId();
        fiberIds[i] = pulsar::Fiber::GetThis()->getId();
        complete();
      });
    }
    for (size_t i = 0; i < kPinnedTasks; ++i) {
      scheduler.scheduler(
          [&, ownerThread]() {
            if (pulsar::GetThreadId() != ownerThread) {
              affinityFailures.fetch_add(1, std::memory_order_relaxed);
            }
            complete();
          },
          ownerThread);
    }
  });

  scheduler.start();
  const bool ready = result.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
  scheduler.stop();

  if (!ready || completed.load(std::memory_order_acquire) != kTotalTasks) {
    std::cerr << "scheduler-work-stealing-check: timed out or lost tasks" << std::endl;
    return 1;
  }
  if (affinityFailures.load(std::memory_order_relaxed) != 0) {
    std::cerr << "scheduler-work-stealing-check: a pinned task migrated" << std::endl;
    return 2;
  }

  const std::set<int> workerThreads(executedBy.begin(), executedBy.end());
  if (workerThreads.size() < 2) {
    std::cerr << "scheduler-work-stealing-check: peer Workers did not steal local work" << std::endl;
    return 3;
  }

  const std::set<uint64_t> callbackFibers(fiberIds.begin(), fiberIds.end());
  if (callbackFibers.size() > kWorkerCount) {
    std::cerr << "scheduler-work-stealing-check: callback Fibers were not reused per Worker" << std::endl;
    return 4;
  }

  // The caller thread owns queue 0 in use_caller mode. Background Workers may
  // steal from it before stop(), while stop() may also run the root Fiber.
  std::atomic<size_t> callerModeCompleted{0};
  {
    pulsar::Scheduler callerScheduler(2, true, "work-stealing-caller-check");
    for (size_t i = 0; i < 1000; ++i) {
      callerScheduler.scheduler(
          [&]() { callerModeCompleted.fetch_add(1, std::memory_order_relaxed); });
    }
    callerScheduler.start();
    callerScheduler.stop();
  }
  if (callerModeCompleted.load(std::memory_order_relaxed) != 1000) {
    std::cerr << "scheduler-work-stealing-check: use_caller lost tasks" << std::endl;
    return 5;
  }

  std::cout << "workers_used=" << workerThreads.size() << '\n'
            << "callback_fibers=" << callbackFibers.size() << '\n'
            << "pinned_tasks=" << kPinnedTasks << '\n'
            << "caller_mode_tasks=" << callerModeCompleted.load() << '\n'
            << "correctness=PASS" << std::endl;
  return 0;
}
