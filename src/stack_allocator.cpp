#include "stack_allocator.hpp"

#include <boost/context/stack_traits.hpp>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <exception>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <utility>
#include <vector>

#ifdef PULSAR_FIBER_GUARD_PAGES
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace pulsar {
namespace {

size_t NormalizeMinimum(size_t requestedSize) {
  const size_t minimum = boost::context::stack_traits::minimum_size();
  return std::max(requestedSize, minimum);
}

size_t NextPowerOfTwo(size_t value) {
  if (value <= 1) return 1;
  if (value > (std::numeric_limits<size_t>::max() >> 1) + 1) throw std::bad_alloc();
  --value;
  for (size_t shift = 1; shift < std::numeric_limits<size_t>::digits; shift <<= 1) {
    value |= value >> shift;
  }
  if (value == std::numeric_limits<size_t>::max()) throw std::bad_alloc();
  return value + 1;
}

#ifdef PULSAR_FIBER_GUARD_PAGES
size_t PageSize() {
  const long pageSize = sysconf(_SC_PAGESIZE);
  if (pageSize <= 0) throw std::bad_alloc();
  return static_cast<size_t>(pageSize);
}

size_t AlignUp(size_t value, size_t alignment) {
  if (value > std::numeric_limits<size_t>::max() - (alignment - 1)) throw std::bad_alloc();
  return (value + alignment - 1) / alignment * alignment;
}
#endif

FiberStackBlock AllocateRaw(size_t requestedSize) {
  FiberStackBlock block;
  const size_t normalized = NormalizeMinimum(requestedSize);
#ifdef PULSAR_FIBER_GUARD_PAGES
  const size_t guardSize = PageSize();
  const size_t usableSize = AlignUp(normalized, guardSize);
  if (usableSize > std::numeric_limits<size_t>::max() - guardSize) throw std::bad_alloc();
  const size_t allocationSize = usableSize + guardSize;
  void *allocation = mmap(nullptr, allocationSize, PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (allocation == MAP_FAILED) throw std::bad_alloc();
  if (mprotect(allocation, guardSize, PROT_NONE) != 0) {
    munmap(allocation, allocationSize);
    throw std::bad_alloc();
  }
  block.allocation = allocation;
  block.stackBase = static_cast<char *>(allocation) + guardSize;
  block.usableSize = usableSize;
  block.allocationSize = allocationSize;
  block.backend = FiberStackBackend::MMAP;
#else
  void *allocation = std::malloc(normalized);
  if (allocation == nullptr) throw std::bad_alloc();
  block.allocation = allocation;
  block.stackBase = allocation;
  block.usableSize = normalized;
  block.allocationSize = normalized;
  block.backend = FiberStackBackend::MALLOC;
#endif
  return block;
}

void FreeRaw(FiberStackBlock &block) noexcept {
  if (!block) return;
  if (block.backend == FiberStackBackend::MMAP) {
#ifdef PULSAR_FIBER_GUARD_PAGES
    munmap(block.allocation, block.allocationSize);
#else
    // A block must be returned to an allocator built with the same guard-page
    // mode. Avoid passing an mmap address to free if a custom allocator breaks
    // that contract.
    std::terminate();
#endif
  } else {
    std::free(block.allocation);
  }
  block.clear();
}

void UpdatePeak(std::atomic<uint64_t> &peak, uint64_t value) noexcept {
  uint64_t observed = peak.load(std::memory_order_relaxed);
  while (observed < value &&
         !peak.compare_exchange_weak(observed, value, std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
  }
}

}  // namespace

FiberStackBlock::FiberStackBlock(FiberStackBlock &&other) noexcept
    : allocation(other.allocation),
      stackBase(other.stackBase),
      usableSize(other.usableSize),
      allocationSize(other.allocationSize),
      backend(other.backend) {
  other.clear();
}

FiberStackBlock &FiberStackBlock::operator=(FiberStackBlock &&other) noexcept {
  if (this == &other) return *this;
  if (allocation != nullptr) std::terminate();
  allocation = other.allocation;
  stackBase = other.stackBase;
  usableSize = other.usableSize;
  allocationSize = other.allocationSize;
  backend = other.backend;
  other.clear();
  return *this;
}

void *FiberStackBlock::stackPointer() const noexcept {
  return stackBase == nullptr ? nullptr : static_cast<char *>(stackBase) + usableSize;
}

void FiberStackBlock::clear() noexcept {
  allocation = nullptr;
  stackBase = nullptr;
  usableSize = 0;
  allocationSize = 0;
  backend = FiberStackBackend::MALLOC;
}

struct DirectStackAllocator::Impl {
  std::atomic<uint64_t> acquireRequests{0};
  std::atomic<uint64_t> cacheMisses{0};
  std::atomic<uint64_t> returns{0};
  std::atomic<uint64_t> passthroughAllocations{0};
  std::atomic<uint64_t> systemAllocations{0};
  std::atomic<uint64_t> systemFrees{0};
  std::atomic<uint64_t> checkedOutBytes{0};
  std::atomic<uint64_t> peakCheckedOutBytes{0};
};

DirectStackAllocator::DirectStackAllocator() : impl_(new Impl) {}
DirectStackAllocator::~DirectStackAllocator() = default;

FiberStackBlock DirectStackAllocator::acquire(size_t requestedSize) {
  impl_->acquireRequests.fetch_add(1, std::memory_order_relaxed);
  impl_->cacheMisses.fetch_add(1, std::memory_order_relaxed);
  impl_->passthroughAllocations.fetch_add(1, std::memory_order_relaxed);
  FiberStackBlock block = AllocateRaw(requestedSize);
  impl_->systemAllocations.fetch_add(1, std::memory_order_relaxed);
  const uint64_t checkedOut = impl_->checkedOutBytes.fetch_add(block.allocationSize, std::memory_order_relaxed) +
                              block.allocationSize;
  UpdatePeak(impl_->peakCheckedOutBytes, checkedOut);
  return block;
}

void DirectStackAllocator::release(FiberStackBlock &&block) noexcept {
  if (!block) return;
  impl_->returns.fetch_add(1, std::memory_order_relaxed);
  impl_->checkedOutBytes.fetch_sub(block.allocationSize, std::memory_order_relaxed);
  FreeRaw(block);
  impl_->systemFrees.fetch_add(1, std::memory_order_relaxed);
}

void DirectStackAllocator::trim(size_t) {}

FiberStackStats DirectStackAllocator::stats() const {
  FiberStackStats result;
  result.acquireRequests = impl_->acquireRequests.load(std::memory_order_relaxed);
  result.cacheMisses = impl_->cacheMisses.load(std::memory_order_relaxed);
  result.returns = impl_->returns.load(std::memory_order_relaxed);
  result.passthroughAllocations = impl_->passthroughAllocations.load(std::memory_order_relaxed);
  result.systemAllocations = impl_->systemAllocations.load(std::memory_order_relaxed);
  result.systemFrees = impl_->systemFrees.load(std::memory_order_relaxed);
  result.checkedOutBytes = impl_->checkedOutBytes.load(std::memory_order_relaxed);
  result.peakCheckedOutBytes = impl_->peakCheckedOutBytes.load(std::memory_order_relaxed);
  return result;
}

void DirectStackAllocator::resetStats() noexcept {
  impl_->acquireRequests.store(0, std::memory_order_relaxed);
  impl_->cacheMisses.store(0, std::memory_order_relaxed);
  impl_->returns.store(0, std::memory_order_relaxed);
  impl_->passthroughAllocations.store(0, std::memory_order_relaxed);
  impl_->systemAllocations.store(0, std::memory_order_relaxed);
  impl_->systemFrees.store(0, std::memory_order_relaxed);
  const uint64_t current = impl_->checkedOutBytes.load(std::memory_order_relaxed);
  impl_->peakCheckedOutBytes.store(current, std::memory_order_relaxed);
}

struct PooledStackAllocator::Impl {
  explicit Impl(StackPoolOptions input) : options(input) {
    if (options.maxPooledStackSize != 0) {
      options.maxPooledStackSize = NextPowerOfTwo(NormalizeMinimum(options.maxPooledStackSize));
    }
  }

  StackPoolOptions options;
  mutable std::mutex mutex;
  std::map<size_t, std::vector<FiberStackBlock>> freeLists;
  FiberStackStats counters;
#ifdef PULSAR_ENABLE_TEST_HOOKS
  bool failNextCacheInsert = false;
#endif
};

PooledStackAllocator::PooledStackAllocator(StackPoolOptions options) : impl_(new Impl(options)) {}

PooledStackAllocator::~PooledStackAllocator() { trim(0); }

size_t PooledStackAllocator::pooledSizeFor(size_t requestedSize) const {
  return NextPowerOfTwo(NormalizeMinimum(requestedSize));
}

FiberStackBlock PooledStackAllocator::acquire(size_t requestedSize) {
  const size_t pooledSize = pooledSizeFor(requestedSize);
  const bool cacheable = impl_->options.maxCachedBytes != 0 &&
                         impl_->options.maxPooledStackSize != 0 &&
                         pooledSize <= impl_->options.maxPooledStackSize;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    ++impl_->counters.acquireRequests;
    if (cacheable) {
      auto found = impl_->freeLists.find(pooledSize);
      if (found != impl_->freeLists.end() && !found->second.empty()) {
        FiberStackBlock block = std::move(found->second.back());
        found->second.pop_back();
        ++impl_->counters.cacheHits;
        impl_->counters.cachedBytes -= block.allocationSize;
        impl_->counters.checkedOutBytes += block.allocationSize;
        impl_->counters.peakCheckedOutBytes =
            std::max(impl_->counters.peakCheckedOutBytes, impl_->counters.checkedOutBytes);
        return block;
      }
    }
    ++impl_->counters.cacheMisses;
    if (!cacheable) ++impl_->counters.passthroughAllocations;
  }

  FiberStackBlock block = AllocateRaw(cacheable ? pooledSize : requestedSize);
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    ++impl_->counters.systemAllocations;
    impl_->counters.checkedOutBytes += block.allocationSize;
    impl_->counters.peakCheckedOutBytes =
        std::max(impl_->counters.peakCheckedOutBytes, impl_->counters.checkedOutBytes);
  }
  return block;
}

void PooledStackAllocator::release(FiberStackBlock &&block) noexcept {
  if (!block) return;
  bool cached = false;
  const size_t classSize = block.usableSize;
  const size_t allocationSize = block.allocationSize;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    ++impl_->counters.returns;
    if (impl_->counters.checkedOutBytes >= allocationSize) {
      impl_->counters.checkedOutBytes -= allocationSize;
    } else {
      std::terminate();
    }

    const bool cacheable = impl_->options.maxCachedBytes != 0 &&
                           classSize != 0 && (classSize & (classSize - 1)) == 0 &&
                           classSize <= impl_->options.maxPooledStackSize &&
                           impl_->counters.cachedBytes <= impl_->options.maxCachedBytes &&
                           allocationSize <= impl_->options.maxCachedBytes - impl_->counters.cachedBytes;
    if (cacheable) {
      try {
#ifdef PULSAR_ENABLE_TEST_HOOKS
        if (impl_->failNextCacheInsert) {
          impl_->failNextCacheInsert = false;
          throw std::bad_alloc();
        }
#endif
        impl_->freeLists[classSize].push_back(std::move(block));
        impl_->counters.cachedBytes += allocationSize;
        cached = true;
      } catch (...) {
        cached = false;
      }
    }
    if (!cached) ++impl_->counters.evictions;
  }
  if (!cached) {
    FreeRaw(block);
    std::lock_guard<std::mutex> lock(impl_->mutex);
    ++impl_->counters.systemFrees;
  }
}

void PooledStackAllocator::trim(size_t keepBytes) {
  while (true) {
    FiberStackBlock victim;
    {
      std::lock_guard<std::mutex> lock(impl_->mutex);
      if (impl_->counters.cachedBytes <= keepBytes) break;
      auto found = impl_->freeLists.rbegin();
      while (found != impl_->freeLists.rend() && found->second.empty()) ++found;
      if (found == impl_->freeLists.rend()) break;
      victim = std::move(found->second.back());
      found->second.pop_back();
      impl_->counters.cachedBytes -= victim.allocationSize;
      ++impl_->counters.evictions;
    }
    FreeRaw(victim);
    std::lock_guard<std::mutex> lock(impl_->mutex);
    ++impl_->counters.systemFrees;
  }
}

FiberStackStats PooledStackAllocator::stats() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->counters;
}

void PooledStackAllocator::resetStats() noexcept {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const uint64_t cachedBytes = impl_->counters.cachedBytes;
  const uint64_t checkedOutBytes = impl_->counters.checkedOutBytes;
  impl_->counters = FiberStackStats{};
  impl_->counters.cachedBytes = cachedBytes;
  impl_->counters.checkedOutBytes = checkedOutBytes;
  impl_->counters.peakCheckedOutBytes = checkedOutBytes;
}

StackPoolOptions PooledStackAllocator::options() const noexcept { return impl_->options; }

#ifdef PULSAR_ENABLE_TEST_HOOKS
void PooledStackAllocator::failNextCacheInsertForTesting() noexcept {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->failNextCacheInsert = true;
}
#endif

std::shared_ptr<FiberStackAllocator> MakeDirectStackAllocator() {
  return std::make_shared<DirectStackAllocator>();
}

std::shared_ptr<FiberStackAllocator> MakePooledStackAllocator(StackPoolOptions options) {
  return std::make_shared<PooledStackAllocator>(options);
}

std::shared_ptr<FiberStackAllocator> GetDefaultFiberStackAllocator() {
  static std::shared_ptr<FiberStackAllocator> allocator = MakeDirectStackAllocator();
  return allocator;
}

}  // namespace pulsar
