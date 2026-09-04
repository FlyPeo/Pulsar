#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sched.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <pulsar/fiber.hpp>
#include <pulsar/hook.hpp>
#include <pulsar/iomanager.hpp>
#include <pulsar/scheduler.hpp>
#include <pulsar/sync.hpp>
#include <pulsar/stack_allocator.hpp>
#include <boost/version.hpp>

namespace {

using Clock = std::chrono::steady_clock;
using pulsar::Fiber;
using pulsar::FiberMutex;
using pulsar::FiberSemaphore;
using pulsar::IOManager;
using pulsar::Scheduler;

struct Options {
  std::string caseName;
  std::string mode = "direct-single";
  size_t count = 0;
  size_t iterations = 1000000;
  size_t threads = 1;
  uint64_t delayMs = 10;
  size_t payloadBytes = 64;
  size_t roundTrips = 1;
  int cpu = -1;
  size_t rounds = 5;
  size_t warmup = 10000;
  size_t stackBytes = 131072;
  size_t poolMib = 64;
  size_t perWorkerCache = 1;
};

std::string DisplayModeName(const std::string& mode) {
  if (mode == "direct-single") return "Direct+单槽";
  if (mode == "pool-single") return "StackPool+单槽";
  if (mode == "pool-multi") return "StackPool+多槽";
  return mode;
}

double PercentileD(std::vector<double> values, double quantile) {
  if (values.empty()) return 0.0;
  std::sort(values.begin(), values.end());
  const size_t index = static_cast<size_t>(quantile * static_cast<double>(values.size() - 1));
  return values[index];
}

void Usage(const char* program) {
  std::cout
      << "Usage: " << program << " --case CASE [options]\n"
      << "Cases: context, lifecycle, scheduler, timer, hook-sleep, hook-echo, sync, wave-burst, sleep-footprint, callback-ab\n"
      << "Options:\n"
      << "  --mode MODE        direct-single|pool-single|pool-multi (or Direct+单槽, StackPool+单槽, StackPool+多槽)\n"
      << "  --count N          Objects/tasks/connections (case-specific default)\n"
      << "  --iterations N     Yield iterations for context case (default 1000000)\n"
      << "  --threads N        Scheduler/IOManager worker threads (default 1)\n"
      << "  --delay-ms N       Timer/sleep/mutex delay (default 10)\n"
      << "  --payload-bytes N  Echo payload bytes (default 64)\n"
      << "  --round-trips N    Echo round trips per connection (default 1)\n"
      << "  --cpu N            Pin the process to one logical CPU; -1 keeps caller affinity\n"
      << "  --rounds N         Measurement rounds (default 5)\n"
      << "  --warmup N         Warmup tasks/iterations (default 10000)\n"
      << "  --stack-bytes N    Fiber stack size in bytes (default 131072)\n"
      << "  --pool-mib N       Max cached pool size in MiB (default 64)\n"
      << "  --fiber-cache-per-worker N  Callback fiber cache per worker (default 1, 4 for multi-slot)\n";
}

size_t ParseSize(const std::string& text, const char* name, bool allowZero = false) {
  try {
    size_t consumed = 0;
    const unsigned long long value = std::stoull(text, &consumed);
    if (consumed != text.size() || (!allowZero && value == 0) || value > 1000000000ULL) {
      throw std::invalid_argument("range");
    }
    return static_cast<size_t>(value);
  } catch (const std::exception&) {
    throw std::invalid_argument(std::string(name) + " must be a valid integer");
  }
}

Options ParseOptions(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      Usage(argv[0]);
      std::exit(0);
    }
    if (i + 1 >= argc) throw std::invalid_argument("missing value for " + arg);
    const std::string value = argv[++i];
    if (arg == "--case") options.caseName = value;
    else if (arg == "--mode") {
      options.mode = value;
      if (options.mode == "Direct+单槽") options.mode = "direct-single";
      else if (options.mode == "StackPool+单槽") options.mode = "pool-single";
      else if (options.mode == "StackPool+多槽") options.mode = "pool-multi";
    }
    else if (arg == "--count") options.count = ParseSize(value, "--count");
    else if (arg == "--iterations") options.iterations = ParseSize(value, "--iterations");
    else if (arg == "--threads") options.threads = ParseSize(value, "--threads");
    else if (arg == "--delay-ms") options.delayMs = ParseSize(value, "--delay-ms", true);
    else if (arg == "--payload-bytes") options.payloadBytes = ParseSize(value, "--payload-bytes");
    else if (arg == "--round-trips") options.roundTrips = ParseSize(value, "--round-trips");
    else if (arg == "--cpu") options.cpu = static_cast<int>(ParseSize(value, "--cpu", true));
    else if (arg == "--rounds") options.rounds = ParseSize(value, "--rounds");
    else if (arg == "--warmup") options.warmup = ParseSize(value, "--warmup", true);
    else if (arg == "--stack-bytes") options.stackBytes = ParseSize(value, "--stack-bytes");
    else if (arg == "--pool-mib") options.poolMib = ParseSize(value, "--pool-mib");
    else if (arg == "--fiber-cache-per-worker") options.perWorkerCache = ParseSize(value, "--fiber-cache-per-worker");
    else throw std::invalid_argument("unknown option " + arg);
  }
  if (options.caseName.empty()) throw std::invalid_argument("--case is required");
  if (options.mode == "pool-multi" && options.perWorkerCache == 1) {
    options.perWorkerCache = 4;
  }
  if (options.count == 0) {
    if (options.caseName == "lifecycle") options.count = 10000;
    else if (options.caseName == "scheduler") options.count = 100000;
    else if (options.caseName == "timer") options.count = 10000;
    else if (options.caseName == "hook-sleep") options.count = 1000;
    else if (options.caseName == "hook-echo") options.count = 1000;
    else if (options.caseName == "sync") options.count = 1000;
    else if (options.caseName == "wave-burst") options.count = 50000;
    else if (options.caseName == "sleep-footprint") options.count = 10000;
    else if (options.caseName == "callback-ab") options.count = 50000;
  }
  return options;
}

void PinToCpu(int cpu) {
  if (cpu < 0) return;
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  if (sched_setaffinity(0, sizeof(set), &set) != 0) {
    throw std::runtime_error("sched_setaffinity failed: " + std::string(std::strerror(errno)));
  }
}

long long Nanoseconds(Clock::duration duration) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
}

double PerSecond(uint64_t operations, long long nanoseconds) {
  return nanoseconds > 0 ? static_cast<double>(operations) * 1e9 / static_cast<double>(nanoseconds) : 0.0;
}

long long Percentile(std::vector<long long> values, double quantile) {
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  const size_t index = static_cast<size_t>(quantile * static_cast<double>(values.size() - 1));
  return values[index];
}

long long ClockPairOverheadNs() {
  long long best = std::numeric_limits<long long>::max();
  for (int i = 0; i < 10000; ++i) {
    const auto start = Clock::now();
    const auto end = Clock::now();
    best = std::min(best, Nanoseconds(end - start));
  }
  return best;
}

long long ProcStatusKiB(const std::string& key) {
  std::ifstream input("/proc/self/status");
  std::string line;
  const std::string prefix = key + ":";
  while (std::getline(input, line)) {
    if (line.rfind(prefix, 0) != 0) continue;
    size_t position = prefix.size();
    while (position < line.size() && (line[position] == ' ' || line[position] == '\t')) ++position;
    size_t consumed = 0;
    const long long value = std::stoll(line.substr(position), &consumed);
    return value;
  }
  return -1;
}

void Header(const Options& options) {
  std::cout << std::fixed << std::setprecision(3)
            << "benchmark_case=" << options.caseName << '\n'
            << "mode=" << DisplayModeName(options.mode) << '\n'
            << "build_type="
#ifdef NDEBUG
            << "Release\n"
#else
            << "Debug\n"
#endif
            << "compiler=" << __VERSION__ << '\n'
            << "boost_version=" << BOOST_LIB_VERSION << '\n'
#ifdef PULSAR_FIBER_GUARD_PAGES
            << "guard_pages_enabled=1\n"
#else
            << "guard_pages_enabled=0\n"
#endif
            << "logical_cpu=" << sched_getcpu() << '\n'
            << "worker_threads=" << options.threads << '\n';
}

void RunContext(const Options& options) {
  Fiber::GetThis();
  auto run = [](size_t iterations) {
    size_t yielded = 0;
    Fiber::ptr fiber(new Fiber([&]() {
      Fiber* self = Fiber::GetThis().get();
      for (size_t i = 0; i < iterations; ++i) {
        ++yielded;
        self->yield();
      }
    }, 0, false));
    const auto start = Clock::now();
    while (fiber->getState() != Fiber::TERM) fiber->resume();
    const auto end = Clock::now();
    if (yielded != iterations) throw std::runtime_error("context yield count mismatch");
    return Nanoseconds(end - start);
  };

  run(std::min<size_t>(options.iterations, 10000));
  const long long overhead = ClockPairOverheadNs();
  const long long elapsed = std::max<long long>(0, run(options.iterations) - overhead);
  const uint64_t transfers = 2ULL * (options.iterations + 1ULL);
  std::cout << "iterations=" << options.iterations << '\n'
            << "context_transfers=" << transfers << '\n'
            << "clock_pair_overhead_ns=" << overhead << '\n'
            << "elapsed_ns=" << elapsed << '\n'
            << "ns_per_transfer=" << static_cast<double>(elapsed) / transfers << '\n'
            << "transfers_per_sec=" << PerSecond(transfers, elapsed) << '\n'
            << "correctness=PASS\n";
}

void RunLifecycle(const Options& options) {
  Fiber::GetThis();
  const std::string modeName = DisplayModeName(options.mode);

  pulsar::StackPoolOptions poolOpts;
  poolOpts.maxCachedBytes = options.poolMib * 1024 * 1024;
  std::shared_ptr<pulsar::FiberStackAllocator> allocator;
  if (options.mode == "direct-single") {
    allocator = pulsar::MakeDirectStackAllocator();
  } else {
    allocator = pulsar::MakePooledStackAllocator(poolOpts);
  }

  // Pre-warmup
  if (options.warmup > 0) {
    for (size_t i = 0; i < options.warmup; ++i) {
      Fiber::ptr f(new Fiber([]() {}, options.stackBytes, false, allocator));
      f->resume();
    }
  }

  const auto baselineAllocStats = allocator->stats();
  allocator->resetStats();

  const uint64_t expectedChecksum = static_cast<uint64_t>(options.count) * (options.count + 1ULL) / 2ULL;
  std::vector<long long> roundCreateDestroyNs;
  std::vector<long long> roundRunNs;
  std::vector<long long> roundTotalNs;
  std::vector<uint64_t> roundChecksums;
  std::vector<uint64_t> roundSystemAllocs;

  for (size_t r = 0; r < options.rounds; ++r) {
    const auto statsBefore = allocator->stats();
    uint64_t roundChecksum = 0;
    long long createDestroyNs = 0;
    long long runNs = 0;

    const auto roundStart = Clock::now();
    constexpr size_t kBatch = 100;
    for (size_t i = 0; i < options.count; i += kBatch) {
      const size_t batch = std::min<size_t>(kBatch, options.count - i);
      const auto t0 = Clock::now();
      std::vector<Fiber::ptr> fibers(batch);
      for (size_t b = 0; b < batch; ++b) {
        const size_t idx = i + b;
        fibers[b] = std::make_shared<Fiber>([&roundChecksum, idx]() {
          roundChecksum += (idx + 1);
        }, options.stackBytes, false, allocator);
      }
      const auto t1 = Clock::now();
      for (size_t b = 0; b < batch; ++b) {
        fibers[b]->resume();
      }
      const auto t2 = Clock::now();
      fibers.clear();
      const auto t3 = Clock::now();

      createDestroyNs += Nanoseconds(t1 - t0) + Nanoseconds(t3 - t2);
      runNs += Nanoseconds(t2 - t1);
    }
    const auto roundEnd = Clock::now();
    const long long totalNs = Nanoseconds(roundEnd - roundStart);
    const auto statsAfter = allocator->stats();

    roundCreateDestroyNs.push_back(createDestroyNs);
    roundRunNs.push_back(runNs);
    roundTotalNs.push_back(totalNs);
    roundChecksums.push_back(roundChecksum);
    roundSystemAllocs.push_back(statsAfter.systemAllocations - statsBefore.systemAllocations);
  }

  const auto finalStats = allocator->stats();

  std::vector<long long> sortedCD = roundCreateDestroyNs;
  std::sort(sortedCD.begin(), sortedCD.end());
  const long long medianCD = sortedCD[sortedCD.size() / 2];

  std::vector<long long> sortedTotal = roundTotalNs;
  std::sort(sortedTotal.begin(), sortedTotal.end());
  const long long medianTotal = sortedTotal[sortedTotal.size() / 2];

  const uint64_t measuredSystemAllocs = finalStats.systemAllocations;
  const uint64_t totalMeasuredFibers = options.count * options.rounds;
  double allocReductionPct = 0.0;
  if (options.mode != "direct-single") {
    if (totalMeasuredFibers > 0) {
      allocReductionPct = (1.0 - static_cast<double>(measuredSystemAllocs) / totalMeasuredFibers) * 100.0;
    }
  }

  pulsar::SchedulerReuseOptions schedOpts;
  schedOpts.stackAllocator = allocator;
  schedOpts.callbackFiberCachePerWorker = options.mode == "pool-multi" ? options.perWorkerCache : 1;
  Scheduler sched(1, false, "benchmark-scheduler", schedOpts);
  for (size_t i = 0; i < std::min<size_t>(options.count, 1000); ++i) {
    sched.scheduler([]() {});
  }
  sched.start();
  sched.stop();
  const auto schedStats = sched.getReuseStats();

  bool correct = true;
  for (size_t r = 0; r < options.rounds; ++r) {
    if (roundChecksums[r] != expectedChecksum) correct = false;
  }
  if (Fiber::TotalFiberNum() != 1) correct = false;

  std::cout << std::fixed << std::setprecision(3)
            << "stack_bytes=" << options.stackBytes << '\n'
            << "task_count=" << options.count << '\n'
            << "warmup_count=" << options.warmup << '\n'
            << "rounds=" << options.rounds << '\n';

  for (size_t r = 0; r < options.rounds; ++r) {
    std::cout << "round_" << (r + 1) << "_create_destroy_ns=" << roundCreateDestroyNs[r] << '\n'
              << "round_" << (r + 1) << "_run_ns=" << roundRunNs[r] << '\n'
              << "round_" << (r + 1) << "_total_ns=" << roundTotalNs[r] << '\n'
              << "round_" << (r + 1) << "_ns_per_fiber=" << (static_cast<double>(roundTotalNs[r]) / options.count) << '\n'
              << "round_" << (r + 1) << "_system_allocations=" << roundSystemAllocs[r] << '\n';
  }

  std::cout << "median_create_destroy_ns_per_fiber=" << (static_cast<double>(medianCD) / options.count) << '\n'
            << "median_total_ns_per_fiber=" << (static_cast<double>(medianTotal) / options.count) << '\n'
            << "warmup_system_allocations=" << baselineAllocStats.systemAllocations << '\n'
            << "measured_system_allocations=" << measuredSystemAllocs << '\n';
  if (options.mode != "direct-single") {
    std::cout << "system_allocation_reduction_pct=" << allocReductionPct << '\n';
  }

  std::cout << "allocator_acquire_requests=" << finalStats.acquireRequests << '\n'
            << "allocator_cache_hits=" << finalStats.cacheHits << '\n'
            << "allocator_cache_misses=" << finalStats.cacheMisses << '\n'
            << "allocator_returns=" << finalStats.returns << '\n'
            << "allocator_evictions=" << finalStats.evictions << '\n'
            << "allocator_passthrough=" << finalStats.passthroughAllocations << '\n'
            << "allocator_system_allocations=" << finalStats.systemAllocations << '\n'
            << "allocator_system_frees=" << finalStats.systemFrees << '\n'
            << "allocator_cached_bytes=" << finalStats.cachedBytes << '\n'
            << "allocator_checked_out_bytes=" << finalStats.checkedOutBytes << '\n'
            << "allocator_peak_checked_out_bytes=" << finalStats.peakCheckedOutBytes << '\n'
            << "callback_cache_hits=" << schedStats.callbackCacheHits << '\n'
            << "callback_cache_misses=" << schedStats.callbackCacheMisses << '\n'
            << "callback_cache_evictions=" << schedStats.callbackCacheEvictions << '\n'
            << "callback_cached_count=" << schedStats.callbackCachedCount << '\n'
            << "callback_cached_bytes=" << schedStats.callbackCachedBytes << '\n'
            << "checksum=" << roundChecksums[0] << '\n'
            << "expected_checksum=" << expectedChecksum << '\n'
            << "correctness=" << (correct ? "PASS" : "FAIL") << '\n';
  if (!correct) throw std::runtime_error("fiber lifecycle benchmark failed");
}

void RunWaveBurst(const Options& options) {
  const std::vector<size_t> sizes = {64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 2048 * 1024};
  pulsar::StackPoolOptions poolOpts;
  poolOpts.maxCachedBytes = options.poolMib * 1024 * 1024;
  auto allocator = pulsar::MakePooledStackAllocator(poolOpts);

  pulsar::SchedulerReuseOptions schedOpts;
  schedOpts.stackAllocator = allocator;
  schedOpts.callbackFiberCachePerWorker = 4;
  const size_t workers = options.threads > 0 ? options.threads : 4;

  std::cout << std::fixed << std::setprecision(3)
            << "threads=" << workers << '\n'
            << "pool_max_cached_bytes=" << poolOpts.maxCachedBytes << '\n'
            << "rounds=" << options.rounds << '\n';

  bool allPass = true;
  for (size_t r = 0; r < options.rounds; ++r) {
    Scheduler scheduler(workers, false, "wave-burst-sched", schedOpts);
    std::atomic<uint64_t> checksum{0};
    std::atomic<size_t> completed{0};
    const size_t burstCount = options.count > 0 ? options.count : 50000;
    const uint64_t expectedChecksum = static_cast<uint64_t>(burstCount) * (burstCount + 1ULL) / 2ULL;

    const auto start = Clock::now();
    scheduler.start();
    for (size_t i = 0; i < burstCount; ++i) {
      scheduler.scheduler([&checksum, &completed, i]() {
        checksum.fetch_add(i + 1, std::memory_order_relaxed);
        completed.fetch_add(1, std::memory_order_release);
      });
    }

    while (completed.load(std::memory_order_acquire) < burstCount) {
      std::this_thread::yield();
    }
    const auto end = Clock::now();
    scheduler.stop();

    const long long roundNs = Nanoseconds(end - start);
    const auto stats = allocator->stats();
    const bool capHolds = stats.cachedBytes <= poolOpts.maxCachedBytes;
    const bool cksumOk = checksum.load() == expectedChecksum;
    if (!capHolds || !cksumOk) allPass = false;

    const double hitRate = (stats.acquireRequests > 0) ?
        static_cast<double>(stats.cacheHits) / stats.acquireRequests * 100.0 : 0.0;

    std::cout << "round_" << (r + 1) << "_wall_ms=" << (static_cast<double>(roundNs) / 1e6) << '\n'
              << "round_" << (r + 1) << "_throughput_ops_per_sec=" << PerSecond(burstCount, roundNs) << '\n'
              << "round_" << (r + 1) << "_cache_hit_rate_pct=" << hitRate << '\n'
              << "round_" << (r + 1) << "_evictions=" << stats.evictions << '\n'
              << "round_" << (r + 1) << "_passthrough=" << stats.passthroughAllocations << '\n'
              << "round_" << (r + 1) << "_cached_bytes=" << stats.cachedBytes << '\n'
              << "round_" << (r + 1) << "_cap_holds=" << (capHolds ? "YES" : "NO") << '\n'
              << "round_" << (r + 1) << "_checksum=" << checksum.load() << '\n';
  }

  std::cout << "correctness=" << (allPass ? "PASS" : "FAIL") << '\n';
  if (!allPass) throw std::runtime_error("wave-burst benchmark failed");
}

void RunSleepFootprint(const Options& options) {
  Fiber::GetThis();
  const size_t targetFibers = options.count > 0 ? options.count : 10000;
  const size_t stackBytes = options.stackBytes > 0 ? options.stackBytes : 131072;

  pulsar::StackPoolOptions poolOpts;
  poolOpts.maxCachedBytes = options.poolMib * 1024 * 1024;
  std::shared_ptr<pulsar::FiberStackAllocator> allocator;
  if (options.mode == "direct-single") {
    allocator = pulsar::MakeDirectStackAllocator();
  } else {
    allocator = pulsar::MakePooledStackAllocator(poolOpts);
  }

  const long long vmsizeBefore = ProcStatusKiB("VmSize");
  const long long rssBefore = ProcStatusKiB("VmRSS");

  std::vector<Fiber::ptr> sleepingFibers;
  sleepingFibers.reserve(targetFibers);

  for (size_t i = 0; i < targetFibers; ++i) {
    sleepingFibers.emplace_back(new Fiber([]() {
      Fiber::GetThis()->yield();
    }, stackBytes, false, allocator));
    sleepingFibers.back()->resume();
  }

  const long long vmsizeParked = ProcStatusKiB("VmSize");
  const long long rssParked = ProcStatusKiB("VmRSS");
  const auto statsParked = allocator->stats();

  for (auto& f : sleepingFibers) {
    f->resume();
  }
  sleepingFibers.clear();

  const long long vmsizeAfter = ProcStatusKiB("VmSize");
  const long long rssAfter = ProcStatusKiB("VmRSS");
  const auto statsAfter = allocator->stats();

  const bool correct = (statsParked.checkedOutBytes >= targetFibers * stackBytes) &&
                       (Fiber::TotalFiberNum() == 1);

  std::cout << std::fixed << std::setprecision(3)
            << "benchmark_case=sleep-footprint\n"
            << "mode=" << DisplayModeName(options.mode) << '\n'
            << "sleeping_fibers=" << targetFibers << '\n'
            << "stack_bytes=" << stackBytes << '\n'
            << "vmsize_before_kib=" << vmsizeBefore << '\n'
            << "vmsize_parked_kib=" << vmsizeParked << '\n'
            << "vmsize_after_kib=" << vmsizeAfter << '\n'
            << "rss_before_kib=" << rssBefore << '\n'
            << "rss_parked_kib=" << rssParked << '\n'
            << "rss_after_kib=" << rssAfter << '\n'
            << "checked_out_stack_bytes=" << statsParked.checkedOutBytes << '\n'
            << "stack_pool_cached_bytes_parked=" << statsParked.cachedBytes << '\n'
            << "stack_pool_cached_bytes_after=" << statsAfter.cachedBytes << '\n'
            << "callback_cached_bytes=0\n"
            << "pool_reduces_active_stacks=NO\n"
            << "correctness=" << (correct ? "PASS" : "FAIL") << '\n';
  if (!correct) throw std::runtime_error("sleep footprint benchmark failed");
}

void RunCallbackAB(const Options& options) {
  const size_t taskCount = options.count > 0 ? options.count : 50000;
  const std::string modeName = DisplayModeName(options.mode);

  pulsar::StackPoolOptions poolOpts;
  auto allocator = pulsar::MakePooledStackAllocator(poolOpts);

  pulsar::SchedulerReuseOptions schedOpts;
  schedOpts.stackAllocator = allocator;
  schedOpts.callbackFiberCachePerWorker = (options.mode == "pool-multi") ? options.perWorkerCache : 1;

  std::cout << std::fixed << std::setprecision(3)
            << "callback_cache_capacity_per_worker=" << schedOpts.callbackFiberCachePerWorker << '\n'
            << "task_count=" << taskCount << '\n'
            << "rounds=" << options.rounds << '\n';

  const std::vector<std::string> scenarios = {"no-yield", "yield-timer", "external-alias"};

  bool allCorrect = true;
  for (const auto& scenario : scenarios) {
    std::vector<double> roundThroughput;
    std::vector<long long> roundP50;
    std::vector<long long> roundP99;
    uint64_t totalHits = 0;
    uint64_t totalMisses = 0;

    for (size_t r = 0; r < options.rounds; ++r) {
      Scheduler scheduler(1, false, "cb-benchmark", schedOpts);
      std::atomic<size_t> completed{0};
      std::vector<long long> latencies(taskCount);
      std::vector<Fiber::ptr> aliasHolders;
      if (scenario == "external-alias") aliasHolders.resize(taskCount);

      scheduler.start();
      const auto start = Clock::now();

      for (size_t i = 0; i < taskCount; ++i) {
        const auto t0 = Clock::now();
        if (scenario == "no-yield") {
          scheduler.scheduler([&completed, &latencies, i, t0]() {
            latencies[i] = Nanoseconds(Clock::now() - t0);
            completed.fetch_add(1, std::memory_order_release);
          });
        } else if (scenario == "yield-timer") {
          scheduler.scheduler([&completed, &latencies, &scheduler, i, t0]() {
            scheduler.scheduler(Fiber::GetThis());
            Fiber::GetThis()->yield();
            latencies[i] = Nanoseconds(Clock::now() - t0);
            completed.fetch_add(1, std::memory_order_release);
          });
        } else {
          scheduler.scheduler([&completed, &latencies, &aliasHolders, i, t0]() {
            aliasHolders[i] = Fiber::GetThis();
            latencies[i] = Nanoseconds(Clock::now() - t0);
            completed.fetch_add(1, std::memory_order_release);
          });
        }
      }

      while (completed.load(std::memory_order_acquire) < taskCount) {
        std::this_thread::yield();
      }
      const auto end = Clock::now();
      const auto reuseStats = scheduler.getReuseStats();
      scheduler.stop();
      aliasHolders.clear();

      const long long totalNs = Nanoseconds(end - start);
      roundThroughput.push_back(PerSecond(taskCount, totalNs));
      roundP50.push_back(Percentile(latencies, 0.50));
      roundP99.push_back(Percentile(latencies, 0.99));
      totalHits += reuseStats.callbackCacheHits;
      totalMisses += reuseStats.callbackCacheMisses;
    }

    if (scenario == "external-alias" && totalHits > 0) {
      allCorrect = false;
    }

    std::cout << "scenario=" << scenario << '\n'
              << "scenario_" << scenario << "_total_hits=" << totalHits << '\n'
              << "scenario_" << scenario << "_total_misses=" << totalMisses << '\n'
              << "scenario_" << scenario << "_median_throughput_ops_per_sec=" << PercentileD(roundThroughput, 0.50) << '\n'
              << "scenario_" << scenario << "_median_p50_ns=" << Percentile(roundP50, 0.50) << '\n'
              << "scenario_" << scenario << "_median_p99_ns=" << Percentile(roundP99, 0.50) << '\n'
              << "scenario_" << scenario << "_alias_rejected="
              << (scenario == "external-alias" ? (totalHits == 0 ? "YES" : "NO") : "N/A") << '\n';
  }

  std::cout << "correctness=" << (allCorrect ? "PASS" : "FAIL") << '\n';
  if (!allCorrect) throw std::runtime_error("callback-ab benchmark failed");
}

void RunScheduler(const Options& options) {
  Scheduler scheduler(options.threads, false, "fiber-benchmark-scheduler");
  std::atomic<size_t> completed{0};
  std::atomic<uint64_t> checksum{0};
  auto finished = std::make_shared<std::promise<void>>();
  std::future<void> result = finished->get_future();

  const auto enqueueStart = Clock::now();
  for (size_t i = 0; i < options.count; ++i) {
    scheduler.scheduler([&, i]() {
      checksum.fetch_add(i + 1, std::memory_order_relaxed);
      if (completed.fetch_add(1, std::memory_order_acq_rel) + 1 == options.count) finished->set_value();
    });
  }
  const auto enqueueEnd = Clock::now();
  const auto executeStart = Clock::now();
  scheduler.start();
  if (result.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
    throw std::runtime_error("scheduler benchmark timed out");
  }
  const auto executeEnd = Clock::now();
  scheduler.stop();

  const uint64_t expected = static_cast<uint64_t>(options.count) * (options.count + 1ULL) / 2ULL;
  const long long enqueueNs = Nanoseconds(enqueueEnd - enqueueStart);
  const long long executeNs = Nanoseconds(executeEnd - executeStart);
  const bool correct = completed == options.count && checksum == expected;
  std::cout << "tasks=" << options.count << '\n'
            << "enqueue_ns_per_task=" << static_cast<double>(enqueueNs) / options.count << '\n'
            << "execute_ns_per_task=" << static_cast<double>(executeNs) / options.count << '\n'
            << "tasks_per_sec=" << PerSecond(options.count, executeNs) << '\n'
            << "checksum=" << checksum.load() << '\n'
            << "correctness=" << (correct ? "PASS" : "FAIL") << '\n';
  if (!correct) throw std::runtime_error("scheduler checksum mismatch");
}

void RunTimer(const Options& options) {
  IOManager iom(options.threads, false, "fiber-benchmark-timer");
  std::vector<long long> latenessUs(options.count, 0);
  std::atomic<size_t> completed{0};
  auto finished = std::make_shared<std::promise<void>>();
  std::future<void> result = finished->get_future();
  const auto overallStart = Clock::now();
  const auto insertStart = Clock::now();
  for (size_t i = 0; i < options.count; ++i) {
    const auto due = Clock::now() + std::chrono::milliseconds(options.delayMs);
    iom.addTimer(options.delayMs, [&, i, due]() {
      const auto now = Clock::now();
      latenessUs[i] = std::max<long long>(0, std::chrono::duration_cast<std::chrono::microseconds>(now - due).count());
      if (completed.fetch_add(1, std::memory_order_acq_rel) + 1 == options.count) finished->set_value();
    });
  }
  const auto insertEnd = Clock::now();
  if (result.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
    throw std::runtime_error("timer benchmark timed out");
  }
  const auto overallEnd = Clock::now();
  iom.stop();

  const long long insertNs = Nanoseconds(insertEnd - insertStart);
  const long long overallNs = Nanoseconds(overallEnd - overallStart);
  const bool correct = completed == options.count;
  std::cout << "timers=" << options.count << '\n'
            << "delay_ms=" << options.delayMs << '\n'
            << "insert_ns_per_timer=" << static_cast<double>(insertNs) / options.count << '\n'
            << "completion_wall_ms=" << static_cast<double>(overallNs) / 1e6 << '\n'
            << "lateness_p50_us=" << Percentile(latenessUs, 0.50) << '\n'
            << "lateness_p95_us=" << Percentile(latenessUs, 0.95) << '\n'
            << "lateness_p99_us=" << Percentile(latenessUs, 0.99) << '\n'
            << "correctness=" << (correct ? "PASS" : "FAIL") << '\n';
  if (!correct) throw std::runtime_error("timer completion count mismatch");
}

void RunHookSleep(const Options& options) {
  IOManager iom(options.threads, false, "fiber-benchmark-hook-sleep");
  std::atomic<size_t> completed{0};
  auto finished = std::make_shared<std::promise<void>>();
  std::future<void> result = finished->get_future();
  const auto start = Clock::now();
  for (size_t i = 0; i < options.count; ++i) {
    iom.scheduler([&]() {
      usleep(static_cast<useconds_t>(options.delayMs * 1000));
      if (completed.fetch_add(1, std::memory_order_acq_rel) + 1 == options.count) finished->set_value();
    });
  }
  if (result.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
    throw std::runtime_error("hook sleep benchmark timed out");
  }
  const auto end = Clock::now();
  iom.stop();
  const long long elapsedNs = Nanoseconds(end - start);
  const double sequentialMs = static_cast<double>(options.count * options.delayMs);
  const double wallMs = static_cast<double>(elapsedNs) / 1e6;
  const bool correct = completed == options.count;
  std::cout << "sleeping_fibers=" << options.count << '\n'
            << "sleep_ms_each=" << options.delayMs << '\n'
            << "sequential_baseline_ms=" << sequentialMs << '\n'
            << "completion_wall_ms=" << wallMs << '\n'
            << "overlap_factor=" << (wallMs > 0 ? sequentialMs / wallMs : 0.0) << '\n'
            << "correctness=" << (correct ? "PASS" : "FAIL") << '\n';
  if (!correct) throw std::runtime_error("hook sleep completion count mismatch");
}

bool SendExact(int fd, const char* data, size_t size) {
  size_t sent = 0;
  while (sent < size) {
    const ssize_t result = send(fd, data + sent, size - sent, MSG_NOSIGNAL);
    if (result < 0 && errno == EINTR) continue;
    if (result <= 0) return false;
    sent += static_cast<size_t>(result);
  }
  return true;
}

bool ReceiveExact(int fd, char* data, size_t size, int* failureErrno = nullptr) {
  size_t received = 0;
  while (received < size) {
    const ssize_t result = recv(fd, data + received, size - received, 0);
    if (result < 0 && errno == EINTR) continue;
    if (result <= 0) {
      if (failureErrno) *failureErrno = result == 0 ? 0 : errno;
      return false;
    }
    received += static_cast<size_t>(result);
  }
  return true;
}

void RunHookEcho(const Options& options) {
  const int listener = socket(AF_INET, SOCK_STREAM, 0);
  if (listener < 0) throw std::runtime_error("cannot create echo listener");
  int reuse = 1;
  setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
      listen(listener, static_cast<int>(std::min<size_t>(options.count, 4096))) != 0) {
    close(listener);
    throw std::runtime_error("cannot bind/listen echo socket");
  }
  socklen_t addressLength = sizeof(address);
  if (getsockname(listener, reinterpret_cast<sockaddr*>(&address), &addressLength) != 0) {
    close(listener);
    throw std::runtime_error("cannot read echo listener address");
  }

  std::atomic<size_t> serverFailures{0};
  std::atomic<size_t> serverAcceptFailures{0};
  std::atomic<size_t> serverReceiveFailures{0};
  std::atomic<size_t> serverSendFailures{0};
  std::atomic<size_t> acceptedNonblocking{0};
  std::atomic<int> serverReceiveErrno{-1};
  std::thread server([&]() {
    std::vector<int> clients;
    clients.reserve(options.count);
    for (size_t i = 0; i < options.count; ++i) {
      int fd;
      do {
        // The server thread keeps Hook disabled, so accept() must retain native
        // blocking semantics while only the coroutine clients are measured.
        fd = accept(listener, nullptr, nullptr);
      } while (fd < 0 && errno == EINTR);
      if (fd < 0) {
        ++serverFailures;
        ++serverAcceptFailures;
        break;
      }
      const int flags = fcntl_f(fd, F_GETFL, 0);
      if (flags >= 0 && (flags & O_NONBLOCK) != 0) {
        ++acceptedNonblocking;
        if (fcntl_f(fd, F_SETFL, flags & ~O_NONBLOCK) != 0) {
          ++serverFailures;
          ++serverAcceptFailures;
          close(fd);
          break;
        }
      }
      clients.push_back(fd);
    }
    std::vector<std::vector<char>> payloads(clients.size(), std::vector<char>(options.payloadBytes));
    for (size_t round = 0; round < options.roundTrips; ++round) {
      for (size_t i = 0; i < clients.size(); ++i) {
        int receiveErrno = -1;
        if (!ReceiveExact(clients[i], payloads[i].data(), payloads[i].size(), &receiveErrno)) {
          ++serverFailures;
          ++serverReceiveFailures;
          serverReceiveErrno.store(receiveErrno, std::memory_order_relaxed);
        }
      }
      for (size_t i = 0; i < clients.size(); ++i) {
        if (!SendExact(clients[i], payloads[i].data(), payloads[i].size())) {
          ++serverFailures;
          ++serverSendFailures;
        }
      }
    }
    for (size_t i = 0; i < clients.size(); ++i) {
      close(clients[i]);
    }
  });

  IOManager iom(options.threads, false, "fiber-benchmark-hook-echo");
  std::atomic<size_t> completed{0};
  std::atomic<size_t> clientFailures{0};
  std::atomic<size_t> clientSocketFailures{0};
  std::atomic<size_t> clientConnectFailures{0};
  std::atomic<size_t> clientSendFailures{0};
  std::atomic<size_t> clientReceiveFailures{0};
  std::atomic<size_t> clientMismatchFailures{0};
  auto finished = std::make_shared<std::promise<void>>();
  std::future<void> result = finished->get_future();
  const auto start = Clock::now();
  for (size_t i = 0; i < options.count; ++i) {
    iom.scheduler([&, i]() {
      std::vector<char> sent(options.payloadBytes, static_cast<char>('a' + (i % 26)));
      std::vector<char> received(options.payloadBytes, 0);
      const int fd = socket(AF_INET, SOCK_STREAM, 0);
      bool ok = fd >= 0;
      if (!ok) ++clientSocketFailures;
      if (ok) {
        timeval timeout{5, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        ok = connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0;
        if (!ok) ++clientConnectFailures;
      }
      for (size_t round = 0; ok && round < options.roundTrips; ++round) {
        if (!SendExact(fd, sent.data(), sent.size())) {
          ok = false;
          ++clientSendFailures;
        }
        if (ok && !ReceiveExact(fd, received.data(), received.size())) {
          ok = false;
          ++clientReceiveFailures;
        }
        if (ok && sent != received) {
          ok = false;
          ++clientMismatchFailures;
        }
      }
      if (fd >= 0) close(fd);
      if (!ok) ++clientFailures;
      if (completed.fetch_add(1, std::memory_order_acq_rel) + 1 == options.count) finished->set_value();
    });
  }
  if (result.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
    close(listener);
    throw std::runtime_error("hook echo benchmark timed out");
  }
  const auto end = Clock::now();
  iom.stop();
  server.join();
  close(listener);

  const long long elapsedNs = Nanoseconds(end - start);
  const size_t failures = clientFailures.load() + serverFailures.load();
  const size_t requests = options.count * options.roundTrips;
  std::cout << "connections=" << options.count << '\n'
            << "payload_bytes=" << options.payloadBytes << '\n'
            << "round_trips_per_connection=" << options.roundTrips << '\n'
            << "echo_requests=" << requests << '\n'
            << "completion_wall_ms=" << static_cast<double>(elapsedNs) / 1e6 << '\n'
            << "connections_per_sec=" << PerSecond(options.count, elapsedNs) << '\n'
            << "echo_requests_per_sec=" << PerSecond(requests, elapsedNs) << '\n'
            << "failures=" << failures << '\n'
            << "client_socket_failures=" << clientSocketFailures.load() << '\n'
            << "client_connect_failures=" << clientConnectFailures.load() << '\n'
            << "client_send_failures=" << clientSendFailures.load() << '\n'
            << "client_receive_failures=" << clientReceiveFailures.load() << '\n'
            << "client_mismatch_failures=" << clientMismatchFailures.load() << '\n'
            << "server_accept_failures=" << serverAcceptFailures.load() << '\n'
            << "accepted_nonblocking_sockets=" << acceptedNonblocking.load() << '\n'
            << "server_receive_failures=" << serverReceiveFailures.load() << '\n'
            << "server_receive_errno=" << serverReceiveErrno.load() << '\n'
            << "server_send_failures=" << serverSendFailures.load() << '\n'
            << "correctness=" << (completed == options.count && failures == 0 ? "PASS" : "FAIL") << '\n';
  if (completed != options.count || failures != 0) throw std::runtime_error("hook echo correctness failed");
}

void RunSync(const Options& options) {
  long long semaphoreNs = 0;
  {
    IOManager iom(options.threads, false, "fiber-benchmark-semaphore");
    FiberSemaphore semaphore(0);
    std::atomic<size_t> completed{0};
    std::atomic<size_t> failures{0};
    auto finished = std::make_shared<std::promise<void>>();
    std::future<void> result = finished->get_future();
    const auto start = Clock::now();
    for (size_t i = 0; i < options.count; ++i) {
      iom.scheduler([&]() {
        if (semaphore.wait() != 0) ++failures;
        if (completed.fetch_add(1, std::memory_order_acq_rel) + 1 == options.count) finished->set_value();
      });
    }
    iom.scheduler([&]() { semaphore.signal(options.count); });
    if (result.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
      throw std::runtime_error("semaphore stress timed out");
    }
    semaphoreNs = Nanoseconds(Clock::now() - start);
    iom.stop();
    if (failures != 0 || completed != options.count) throw std::runtime_error("semaphore stress failed");
  }

  long long mutexNs = 0;
  size_t protectedCount = 0;
  {
    IOManager iom(options.threads, false, "fiber-benchmark-mutex");
    FiberMutex mutex;
    std::atomic<size_t> completed{0};
    std::atomic<size_t> failures{0};
    auto finished = std::make_shared<std::promise<void>>();
    std::future<void> result = finished->get_future();
    const auto start = Clock::now();
    for (size_t i = 0; i < options.count; ++i) {
      iom.scheduler([&]() {
        if (mutex.lock() != 0) {
          ++failures;
        } else {
          const size_t current = protectedCount;
          usleep(static_cast<useconds_t>(options.delayMs * 1000));
          protectedCount = current + 1;
          if (mutex.unlock() != 0) ++failures;
        }
        if (completed.fetch_add(1, std::memory_order_acq_rel) + 1 == options.count) finished->set_value();
      });
    }
    if (result.wait_for(std::chrono::seconds(60)) != std::future_status::ready) {
      throw std::runtime_error("mutex stress timed out");
    }
    mutexNs = Nanoseconds(Clock::now() - start);
    iom.stop();
    if (failures != 0 || completed != options.count || protectedCount != options.count) {
      throw std::runtime_error("mutex stress failed");
    }
  }

  std::cout << "waiters=" << options.count << '\n'
            << "semaphore_wall_ms=" << static_cast<double>(semaphoreNs) / 1e6 << '\n'
            << "semaphore_wakeups_per_sec=" << PerSecond(options.count, semaphoreNs) << '\n'
            << "mutex_hold_ms=" << options.delayMs << '\n'
            << "mutex_wall_ms=" << static_cast<double>(mutexNs) / 1e6 << '\n'
            << "mutex_handoffs_per_sec=" << PerSecond(options.count, mutexNs) << '\n'
            << "protected_count=" << protectedCount << '\n'
            << "correctness=PASS\n";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const Options options = ParseOptions(argc, argv);
    PinToCpu(options.cpu);
    Header(options);
    if (options.caseName == "context") RunContext(options);
    else if (options.caseName == "lifecycle") RunLifecycle(options);
    else if (options.caseName == "scheduler") RunScheduler(options);
    else if (options.caseName == "timer") RunTimer(options);
    else if (options.caseName == "hook-sleep") RunHookSleep(options);
    else if (options.caseName == "hook-echo") RunHookEcho(options);
    else if (options.caseName == "sync") RunSync(options);
    else if (options.caseName == "wave-burst") RunWaveBurst(options);
    else if (options.caseName == "sleep-footprint") RunSleepFootprint(options);
    else if (options.caseName == "callback-ab") RunCallbackAB(options);
    else throw std::invalid_argument("unknown --case " + options.caseName);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "pulsar-benchmark: " << error.what() << '\n';
    return 1;
  }
}
