#ifndef PULSAR_STACK_ALLOCATOR_HPP
#define PULSAR_STACK_ALLOCATOR_HPP

#include <cstddef>
#include <cstdint>
#include <memory>

namespace pulsar {

enum class FiberStackBackend : uint8_t {
  MALLOC,
  MMAP,
};

// A stack allocation and all metadata required to release it correctly.
// The block is move-only so a stack cannot accidentally acquire two owners.
struct FiberStackBlock {
  void *allocation = nullptr;
  void *stackBase = nullptr;
  size_t usableSize = 0;
  size_t allocationSize = 0;
  FiberStackBackend backend = FiberStackBackend::MALLOC;

  FiberStackBlock() noexcept = default;
  FiberStackBlock(const FiberStackBlock &) = delete;
  FiberStackBlock &operator=(const FiberStackBlock &) = delete;
  FiberStackBlock(FiberStackBlock &&other) noexcept;
  FiberStackBlock &operator=(FiberStackBlock &&other) noexcept;

  explicit operator bool() const noexcept { return allocation != nullptr; }
  void *stackPointer() const noexcept;
  void clear() noexcept;
};

struct FiberStackStats {
  uint64_t acquireRequests = 0;
  uint64_t cacheHits = 0;
  uint64_t cacheMisses = 0;
  uint64_t returns = 0;
  uint64_t evictions = 0;
  uint64_t passthroughAllocations = 0;
  uint64_t systemAllocations = 0;
  uint64_t systemFrees = 0;
  uint64_t cachedBytes = 0;
  uint64_t checkedOutBytes = 0;
  uint64_t peakCheckedOutBytes = 0;
};

struct StackPoolOptions {
  static constexpr size_t kDefaultMaxCachedBytes = 64ULL * 1024 * 1024;
  static constexpr size_t kDefaultMaxPooledStackSize = 1ULL * 1024 * 1024;

  size_t maxCachedBytes = kDefaultMaxCachedBytes;
  size_t maxPooledStackSize = kDefaultMaxPooledStackSize;
};

class FiberStackAllocator {
 public:
  virtual ~FiberStackAllocator() = default;

  virtual FiberStackBlock acquire(size_t requestedSize) = 0;
  virtual void release(FiberStackBlock &&block) noexcept = 0;
  // Called immediately before Boost.Context is constructed on an acquired
  // block. Custom allocators can validate or prepare memory; throwing leaves
  // the owning Fiber in its previous valid state.
  virtual void beforeContextCreate(const FiberStackBlock &) {}
  virtual void trim(size_t keepBytes) = 0;
  virtual FiberStackStats stats() const = 0;
  // Reset cumulative counters. Current gauges remain accurate and the peak is
  // reset to the current checked-out value.
  virtual void resetStats() noexcept = 0;
};

class DirectStackAllocator final : public FiberStackAllocator {
 public:
  DirectStackAllocator();
  ~DirectStackAllocator() override;

  FiberStackBlock acquire(size_t requestedSize) override;
  void release(FiberStackBlock &&block) noexcept override;
  void trim(size_t keepBytes) override;
  FiberStackStats stats() const override;
  void resetStats() noexcept override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

class PooledStackAllocator final : public FiberStackAllocator {
 public:
  explicit PooledStackAllocator(StackPoolOptions options = {});
  ~PooledStackAllocator() override;

  FiberStackBlock acquire(size_t requestedSize) override;
  void release(FiberStackBlock &&block) noexcept override;
  void trim(size_t keepBytes) override;
  FiberStackStats stats() const override;
  void resetStats() noexcept override;

  StackPoolOptions options() const noexcept;
  size_t pooledSizeFor(size_t requestedSize) const;

#ifdef PULSAR_ENABLE_TEST_HOOKS
  // Deterministically exercise the noexcept fallback when a freelist needs
  // metadata but allocation fails. Not present in production builds.
  void failNextCacheInsertForTesting() noexcept;
#endif

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

std::shared_ptr<FiberStackAllocator> MakeDirectStackAllocator();
std::shared_ptr<FiberStackAllocator> MakePooledStackAllocator(StackPoolOptions options = {});
std::shared_ptr<FiberStackAllocator> GetDefaultFiberStackAllocator();

}  // namespace pulsar

#endif  // PULSAR_STACK_ALLOCATOR_HPP
