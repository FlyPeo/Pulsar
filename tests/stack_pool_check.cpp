#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include <pulsar/fiber.hpp>
#include <pulsar/stack_allocator.hpp>

namespace {

using pulsar::Fiber;
using pulsar::FiberStackAllocator;
using pulsar::FiberStackBackend;
using pulsar::FiberStackBlock;
using pulsar::PooledStackAllocator;
using pulsar::StackPoolOptions;

int Fail(const char *message) {
  std::cerr << "pulsar-stack-pool-check: " << message << std::endl;
  return 1;
}

class AlwaysFailAllocator final : public FiberStackAllocator {
 public:
  FiberStackBlock acquire(size_t) override { throw std::bad_alloc(); }
  void release(FiberStackBlock &&) noexcept override {}
  void trim(size_t) override {}
  pulsar::FiberStackStats stats() const override { return {}; }
  void resetStats() noexcept override {}
};

class FailingContextAllocator final : public FiberStackAllocator {
 public:
  FailingContextAllocator() : direct_(pulsar::MakeDirectStackAllocator()) {}

  FiberStackBlock acquire(size_t size) override { return direct_->acquire(size); }
  void release(FiberStackBlock &&block) noexcept override { direct_->release(std::move(block)); }
  void beforeContextCreate(const FiberStackBlock &) override {
    if (failNext_.exchange(false)) throw std::bad_alloc();
  }
  void trim(size_t keepBytes) override { direct_->trim(keepBytes); }
  pulsar::FiberStackStats stats() const override { return direct_->stats(); }
  void resetStats() noexcept override { direct_->resetStats(); }
  void failNextContext() noexcept { failNext_.store(true); }

 private:
  std::shared_ptr<FiberStackAllocator> direct_;
  std::atomic<bool> failNext_{false};
};

bool GuardPageFaults(FiberStackBlock &block) {
  if (block.backend != FiberStackBackend::MMAP) return true;
  const pid_t child = fork();
  if (child == 0) {
    static_cast<volatile unsigned char *>(block.allocation)[0] = 0x5a;
    _exit(0);
  }
  if (child < 0) return false;
  int status = 0;
  if (waitpid(child, &status, 0) != child) return false;
  return WIFSIGNALED(status) &&
         (WTERMSIG(status) == SIGSEGV || WTERMSIG(status) == SIGBUS);
}

}  // namespace

int main(int argc, char **argv) {
  const bool allocatorOnly = argc == 2 && std::string_view(argv[1]) == "--allocator-only";
  if (argc > 2 || (argc == 2 && !allocatorOnly)) {
    return Fail("usage: pulsar-stack-pool-check [--allocator-only]");
  }
  static_assert(!std::is_copy_constructible<FiberStackBlock>::value, "stack blocks must be move-only");
  static_assert(!std::is_copy_assignable<FiberStackBlock>::value, "stack blocks must be move-only");

  auto direct = pulsar::MakeDirectStackAllocator();
  FiberStackBlock directBlock = direct->acquire(1);
  if (!directBlock || directBlock.usableSize < 1 || directBlock.stackPointer() == nullptr) {
    return Fail("direct allocator returned an invalid block");
  }
  if (!GuardPageFaults(directBlock)) return Fail("direct guard page did not fault");
  direct->release(std::move(directBlock));
  const auto directStats = direct->stats();
  if (directStats.acquireRequests != 1 || directStats.cacheMisses != 1 ||
      directStats.passthroughAllocations != 1 || directStats.systemAllocations != 1 ||
      directStats.systemFrees != 1 || directStats.checkedOutBytes != 0) {
    return Fail("direct allocator statistics are inconsistent");
  }

  StackPoolOptions options;
  options.maxCachedBytes = 4ULL * 1024 * 1024;
  options.maxPooledStackSize = 1ULL * 1024 * 1024;
  auto pool = std::make_shared<PooledStackAllocator>(options);
  if (pool->pooledSizeFor(64 * 1024 - 1) != 64 * 1024 ||
      pool->pooledSizeFor(64 * 1024 + 1) != 128 * 1024 ||
      pool->pooledSizeFor(128 * 1024) != 128 * 1024) {
    return Fail("size-class rounding is incorrect");
  }

  FiberStackBlock cold = pool->acquire(100 * 1024);
  if (cold.usableSize != 128 * 1024) return Fail("pooled stack did not round up");
  void *coldAddress = cold.allocation;
  pool->release(std::move(cold));
  FiberStackBlock warm = pool->acquire(100 * 1024);
  if (warm.allocation != coldAddress) return Fail("warm same-class acquire missed the cached block");
  if (!GuardPageFaults(warm)) return Fail("reused guard page did not fault");
  pool->release(std::move(warm));

  FiberStackBlock oversized = pool->acquire(2 * 1024 * 1024);
  if (oversized.usableSize < 2 * 1024 * 1024) return Fail("oversized stack is too small");
  pool->release(std::move(oversized));
  auto poolStats = pool->stats();
  if (poolStats.acquireRequests != poolStats.cacheHits + poolStats.cacheMisses ||
      poolStats.cacheHits < 1 || poolStats.passthroughAllocations != 1 ||
      poolStats.cachedBytes > options.maxCachedBytes || poolStats.checkedOutBytes != 0) {
    return Fail("pooled allocator statistics are inconsistent");
  }

#ifdef PULSAR_ENABLE_TEST_HOOKS
  pool->trim(0);
  pool->resetStats();
  FiberStackBlock metadataFailure = pool->acquire(128 * 1024);
  pool->failNextCacheInsertForTesting();
  pool->release(std::move(metadataFailure));
  poolStats = pool->stats();
  if (poolStats.evictions != 1 || poolStats.systemAllocations != 1 ||
      poolStats.systemFrees != 1 || poolStats.checkedOutBytes != 0 ||
      poolStats.cachedBytes != 0) {
    return Fail("freelist metadata failure did not fall back to direct release");
  }
#endif

  if (!allocatorOnly) {
    Fiber::GetThis();
    const uint64_t fiberBaseline = Fiber::TotalFiberNum();
    pool->trim(0);
    pool->resetStats();
    {
      int runs = 0;
      Fiber::ptr fiber(new Fiber([&]() { ++runs; }, 0, false, pool));
      fiber->resume();
      const uint64_t acquireCount = pool->stats().acquireRequests;
      fiber->reset([&]() { ++runs; });
      fiber->resume();
      if (runs != 2 || pool->stats().acquireRequests != acquireCount) {
        return Fail("Fiber reset did not reuse its exclusively owned stack");
      }
    }
    if (Fiber::TotalFiberNum() != fiberBaseline || pool->stats().checkedOutBytes != 0) {
      return Fail("Fiber destruction did not return its stack");
    }

    auto failing = std::make_shared<AlwaysFailAllocator>();
    try {
      Fiber::ptr shouldFail(new Fiber([]() {}, 0, false, failing));
      return Fail("allocation failure unexpectedly constructed a Fiber");
    } catch (const std::bad_alloc &) {
    }
    if (Fiber::TotalFiberNum() != fiberBaseline) {
      return Fail("failed construction changed Fiber count");
    }

    auto contextFailing = std::make_shared<FailingContextAllocator>();
    contextFailing->failNextContext();
    try {
      Fiber::ptr shouldFail(new Fiber([]() {}, 0, false, contextFailing));
      return Fail("context failure unexpectedly constructed a Fiber");
    } catch (const std::bad_alloc &) {
    }
    if (contextFailing->stats().checkedOutBytes != 0 || Fiber::TotalFiberNum() != fiberBaseline) {
      return Fail("failed context construction leaked its acquired stack");
    }

    int retriedRuns = 0;
    {
      Fiber::ptr retryable(new Fiber([&]() { ++retriedRuns; }, 0, false, contextFailing));
      retryable->resume();
      contextFailing->failNextContext();
      try {
        retryable->reset([&]() { ++retriedRuns; });
        return Fail("reset context failure was not reported");
      } catch (const std::bad_alloc &) {
      }
      if (retryable->getState() != Fiber::TERM) return Fail("failed reset changed Fiber state");
      retryable->reset([&]() { ++retriedRuns; });
      retryable->resume();
    }
    if (retriedRuns != 2 || contextFailing->stats().checkedOutBytes != 0) {
      return Fail("Fiber was not recoverable after reset failure");
    }
  }

  FiberStackBlock crossThread = pool->acquire(128 * 1024);
  std::thread returner([&pool, block = std::move(crossThread)]() mutable {
    pool->release(std::move(block));
  });
  returner.join();

  std::mutex liveMutex;
  std::set<void *> liveBlocks;
  std::atomic<bool> concurrentOk{true};
  std::vector<std::thread> workers;
  for (size_t worker = 0; worker < 8; ++worker) {
    workers.emplace_back([&, worker]() {
      for (size_t iteration = 0; iteration < 2000; ++iteration) {
        FiberStackBlock block = pool->acquire((64 + ((iteration + worker) % 4) * 64) * 1024);
        {
          std::lock_guard<std::mutex> lock(liveMutex);
          if (!liveBlocks.insert(block.allocation).second) concurrentOk.store(false);
        }
        static_cast<volatile unsigned char *>(block.stackBase)[block.usableSize - 1] =
            static_cast<unsigned char>(worker);
        {
          std::lock_guard<std::mutex> lock(liveMutex);
          if (liveBlocks.erase(block.allocation) != 1) concurrentOk.store(false);
        }
        pool->release(std::move(block));
        if ((iteration % 257) == 0) pool->trim(options.maxCachedBytes / 2);
      }
    });
  }
  for (auto &worker : workers) worker.join();
  if (!concurrentOk.load() || !liveBlocks.empty()) return Fail("concurrent ownership was violated");
  poolStats = pool->stats();
  if (poolStats.acquireRequests != poolStats.cacheHits + poolStats.cacheMisses ||
      poolStats.cachedBytes > options.maxCachedBytes || poolStats.checkedOutBytes != 0) {
    return Fail("concurrent pool accounting is inconsistent");
  }

  pool->trim(0);
  pool->trim(0);
  poolStats = pool->stats();
  if (poolStats.cachedBytes != 0 || poolStats.checkedOutBytes != 0 ||
      poolStats.systemAllocations != poolStats.systemFrees) {
    return Fail("trim did not release every idle stack");
  }

  std::cout << "acquire_requests=" << poolStats.acquireRequests << '\n'
            << "cache_hits=" << poolStats.cacheHits << '\n'
            << "cache_misses=" << poolStats.cacheMisses << '\n'
            << "system_allocations=" << poolStats.systemAllocations << '\n'
            << "system_frees=" << poolStats.systemFrees << '\n'
            << "correctness=PASS" << std::endl;
  return 0;
}
