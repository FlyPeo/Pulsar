#include <atomic>
#include <chrono>
#include <future>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>

#include <unistd.h>

#include <pulsar/fiber.hpp>
#include <pulsar/iomanager.hpp>
#include <pulsar/scheduler.hpp>
#include <pulsar/stack_allocator.hpp>

namespace {

int Fail(const char *message) {
  std::cerr << "pulsar-scheduler-cache-check: " << message << std::endl;
  return 1;
}

pulsar::SchedulerReuseOptions ReuseOptions(size_t cacheSize) {
  pulsar::StackPoolOptions stackOptions;
  stackOptions.maxCachedBytes = 8ULL * 1024 * 1024;
  pulsar::SchedulerReuseOptions options;
  options.stackAllocator = pulsar::MakePooledStackAllocator(stackOptions);
  options.callbackFiberCachePerWorker = cacheSize;
  return options;
}

bool WaitUntil(const std::function<bool()> &predicate) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return predicate();
}

}  // namespace

int main() {
  {
    constexpr size_t kTasks = 1000;
    pulsar::Scheduler scheduler(1, false, "cache-hit-check", ReuseOptions(4));
    std::atomic<size_t> completed{0};
    auto finished = std::make_shared<std::promise<void>>();
    auto ready = finished->get_future();
    for (size_t i = 0; i < kTasks; ++i) {
      scheduler.scheduler([&, finished]() {
        if (completed.fetch_add(1) + 1 == kTasks) finished->set_value();
      });
    }
    scheduler.start();
    if (ready.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
      return Fail("cached callbacks timed out");
    }
    if (!WaitUntil([&]() {
          const auto current = scheduler.getReuseStats();
          return current.callbackCacheHits == kTasks - 1 && current.callbackCachedCount == 1;
        })) {
      scheduler.stop();
      return Fail("completed callbacks did not settle in the cache");
    }
    auto stats = scheduler.getReuseStats();
    if (stats.callbackCacheMisses != 1 || stats.callbackCacheHits != kTasks - 1 ||
        stats.callbackCachedCount != 1) {
      return Fail("completed callbacks were not reused");
    }
    scheduler.stop();
    stats = scheduler.getReuseStats();
    if (stats.callbackCachedCount != 0 || stats.callbackCachedBytes != 0) {
      return Fail("stop did not drain callback cache");
    }
  }

  {
    constexpr size_t kTasks = 32;
    pulsar::Scheduler scheduler(1, false, "cache-disabled-check", ReuseOptions(0));
    std::atomic<size_t> completed{0};
    auto finished = std::make_shared<std::promise<void>>();
    auto ready = finished->get_future();
    for (size_t i = 0; i < kTasks; ++i) {
      scheduler.scheduler([&, finished]() {
        if (completed.fetch_add(1) + 1 == kTasks) finished->set_value();
      });
    }
    scheduler.start();
    if (ready.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
      return Fail("uncached callbacks timed out");
    }
    if (!WaitUntil([&]() {
          return scheduler.getReuseStats().callbackCacheEvictions == kTasks;
        })) {
      scheduler.stop();
      return Fail("uncached callback accounting did not settle");
    }
    const auto stats = scheduler.getReuseStats();
    if (stats.callbackCacheHits != 0 || stats.callbackCacheMisses != kTasks ||
        stats.callbackCacheEvictions != kTasks || stats.callbackCachedCount != 0) {
      return Fail("zero-sized callback cache retained an object");
    }
    scheduler.stop();
  }

  {
    pulsar::Scheduler scheduler(1, false, "external-alias-check", ReuseOptions(4));
    pulsar::Fiber::ptr escaped;
    uint64_t escapedId = 0;
    uint64_t nextId = 0;
    std::promise<void> finished;
    auto ready = finished.get_future();
    scheduler.scheduler([&]() {
      escaped = pulsar::Fiber::GetThis();
      escapedId = escaped->getId();
    });
    scheduler.scheduler([&]() {
      nextId = pulsar::Fiber::GetThis()->getId();
      finished.set_value();
    });
    scheduler.start();
    if (ready.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
      return Fail("external alias case timed out");
    }
    if (!escaped || escaped->getState() != pulsar::Fiber::TERM || escapedId == nextId) {
      return Fail("externally aliased callback Fiber was reused");
    }
    scheduler.stop();
    escaped.reset();
  }

  {
    pulsar::IOManager iom(1, false, "yield-reuse-check", ReuseOptions(4));
    std::promise<void> finished;
    auto ready = finished.get_future();
    std::atomic<int> stage{0};
    int beforeThread = -1;
    int afterThread = -2;
    uint64_t yieldedFiberId = 0;
    iom.scheduler([&]() {
      beforeThread = pulsar::GetThreadId();
      yieldedFiberId = pulsar::Fiber::GetThis()->getId();
      stage.store(1);
      usleep(2 * 1000);
      afterThread = pulsar::GetThreadId();
      stage.store(2);
      finished.set_value();
    });
    if (ready.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
      return Fail("yielded callback timed out");
    }
    std::promise<void> reused;
    auto reusedReady = reused.get_future();
    uint64_t reusedFiberId = 0;
    iom.scheduler([&]() {
      reusedFiberId = pulsar::Fiber::GetThis()->getId();
      reused.set_value();
    });
    if (reusedReady.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
      return Fail("post-yield reuse callback timed out");
    }
    const auto stats = iom.getReuseStats();
    if (stage.load() != 2 || beforeThread != afterThread || yieldedFiberId != reusedFiberId ||
        stats.callbackCachedCount < 1) {
      return Fail("yielded callback was not pinned and safely recycled");
    }
    iom.stop();
  }

  {
    pulsar::Scheduler scheduler(1, false, "exception-reuse-check", ReuseOptions(2));
    std::promise<void> finished;
    auto ready = finished.get_future();
    scheduler.scheduler([]() { throw std::runtime_error("expected test failure"); });
    scheduler.scheduler([&]() { finished.set_value(); });
    scheduler.start();
    if (ready.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
      return Fail("scheduler stopped after callback exception");
    }
    if (!WaitUntil([&]() { return scheduler.getReuseStats().callbackCacheHits >= 1; })) {
      scheduler.stop();
      return Fail("terminal exception Fiber did not settle in the cache");
    }
    const auto stats = scheduler.getReuseStats();
    if (stats.callbackCacheHits < 1) return Fail("terminal exception Fiber was not reusable");
    scheduler.stop();
  }

  {
    pulsar::Scheduler scheduler(1, false, "user-fiber-check", ReuseOptions(4));
    std::promise<void> finished;
    auto ready = finished.get_future();
    pulsar::Fiber::ptr userFiber(new pulsar::Fiber([&]() { finished.set_value(); }));
    scheduler.scheduler(userFiber);
    scheduler.start();
    if (ready.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
      return Fail("user Fiber timed out");
    }
    const auto stats = scheduler.getReuseStats();
    if (stats.callbackCachedCount != 0 || stats.callbackCacheMisses != 0) {
      return Fail("user-created Fiber entered the callback cache");
    }
    scheduler.stop();
    userFiber.reset();
  }

  std::cout << "correctness=PASS" << std::endl;
  return 0;
}
