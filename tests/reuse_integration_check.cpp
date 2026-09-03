#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <memory>

#include <sys/socket.h>
#include <unistd.h>

#include <pulsar/fiber.hpp>
#include <pulsar/iomanager.hpp>
#include <pulsar/stack_allocator.hpp>
#include <pulsar/sync.hpp>
#include <pulsar/utils.hpp>

namespace {

constexpr size_t kRounds = 100;

void RecordFailure(std::atomic<size_t> &failures, const char *message) {
  failures.fetch_add(1, std::memory_order_relaxed);
  std::cerr << "pulsar-reuse-integration-check: " << message << std::endl;
}

}  // namespace

int main() {
  pulsar::StackPoolOptions stackOptions;
  stackOptions.maxCachedBytes = 32ULL * 1024 * 1024;
  auto allocator = std::make_shared<pulsar::PooledStackAllocator>(stackOptions);
  pulsar::SchedulerReuseOptions reuseOptions;
  reuseOptions.stackAllocator = allocator;
  reuseOptions.callbackFiberCachePerWorker = 8;

  pulsar::IOManager iom(4, false, "reuse-integration-check", reuseOptions);
  pulsar::FiberSemaphore startReaders;
  pulsar::FiberSemaphore ioPermits;
  pulsar::FiberMutex checksumMutex;
  std::array<std::array<int, 2>, kRounds> sockets{};
  for (auto &pair : sockets) {
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, pair.data()) != 0) {
      std::cerr << "pulsar-reuse-integration-check: socketpair failed" << std::endl;
      return 1;
    }
  }

  std::atomic<size_t> failures{0};
  std::atomic<size_t> completed{0};
  uint64_t checksum = 0;
  auto finished = std::make_shared<std::promise<void>>();
  auto ready = finished->get_future();
  auto complete = [&, finished]() {
    if (completed.fetch_add(1, std::memory_order_acq_rel) + 1 == kRounds * 2) {
      finished->set_value();
    }
  };

  // Queue all readers first. Each one enters WaitQueue before writers are
  // submitted, so the run covers semaphore ownership as well as epoll/timer.
  for (size_t round = 0; round < kRounds; ++round) {
    iom.scheduler([&, round]() {
      if (startReaders.wait() != 0) RecordFailure(failures, "reader gate failed");
      const int originalThread = pulsar::GetThreadId();
      const uint64_t originalFiber = pulsar::Fiber::GetThis()->getId();
      if (iom.addEvent(sockets[round][0], pulsar::READ) != 0) {
        RecordFailure(failures, "epoll registration failed");
      } else {
        pulsar::Fiber::GetThis()->yield();
        if (pulsar::GetThreadId() != originalThread ||
            pulsar::Fiber::GetThis()->getId() != originalFiber) {
          RecordFailure(failures, "I/O continuation changed worker or Fiber");
        }
      }

      char byte = 0;
      if (::read(sockets[round][0], &byte, 1) != 1 ||
          byte != static_cast<char>((round % 93) + 33)) {
        RecordFailure(failures, "socket payload mismatch");
      }
      ::close(sockets[round][0]);

      usleep(1000);
      if (pulsar::GetThreadId() != originalThread ||
          pulsar::Fiber::GetThis()->getId() != originalFiber) {
        RecordFailure(failures, "timer continuation changed worker or Fiber");
      }
      if (ioPermits.wait() != 0) RecordFailure(failures, "semaphore wait failed");

      if (checksumMutex.lock() != 0) {
        RecordFailure(failures, "FiberMutex lock failed");
      } else {
        // Yield while holding the lock so other readers exercise its WaitQueue.
        usleep(1000);
        checksum += round + 1;
        if (checksumMutex.unlock() != 0) {
          RecordFailure(failures, "FiberMutex unlock failed");
        }
      }
      complete();
    });
  }

  startReaders.signal(kRounds);
  for (size_t round = 0; round < kRounds; ++round) {
    iom.scheduler([&, round]() {
      const char byte = static_cast<char>((round % 93) + 33);
      if (::write(sockets[round][1], &byte, 1) != 1) {
        RecordFailure(failures, "socket write failed");
      }
      ::close(sockets[round][1]);
      ioPermits.signal();
      complete();
    });
  }

  bool passed = true;
  if (ready.wait_for(std::chrono::seconds(15)) != std::future_status::ready) {
    RecordFailure(failures, "timed out");
    passed = false;
  }
  iom.stop();

  const auto schedulerStats = iom.getReuseStats();
  if (schedulerStats.callbackCachedCount != 0 || schedulerStats.callbackCachedBytes != 0) {
    RecordFailure(failures, "stop did not drain callback cache");
  }
  if (schedulerStats.callbackCacheEvictions == 0) {
    RecordFailure(failures, "bounded callback cache never exercised full-capacity eviction");
  }
  const uint64_t expectedChecksum = kRounds * (kRounds + 1) / 2;
  if (checksum != expectedChecksum || completed.load() != kRounds * 2) {
    RecordFailure(failures, "completion checksum mismatch");
  }

  allocator->trim(0);
  const auto stackStats = allocator->stats();
  if (stackStats.checkedOutBytes != 0 || stackStats.cachedBytes != 0 ||
      stackStats.systemAllocations != stackStats.systemFrees) {
    RecordFailure(failures, "stack lifecycle did not balance after trim");
  }

  passed = passed && failures.load() == 0;
  std::cout << "rounds=" << kRounds << '\n'
            << "checksum=" << checksum << '\n'
            << "callback_evictions=" << schedulerStats.callbackCacheEvictions << '\n'
            << "system_allocations=" << stackStats.systemAllocations << '\n'
            << "system_frees=" << stackStats.systemFrees << '\n'
            << "correctness=" << (passed ? "PASS" : "FAIL") << std::endl;
  return passed ? 0 : 1;
}
