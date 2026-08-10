#ifndef __PULSAR_FIBER_SYNC_H__
#define __PULSAR_FIBER_SYNC_H__

#include <deque>
#include <limits>
#include <memory>

#include "fiber.hpp"
#include "mutex.hpp"

namespace pulsar {

// A timeout value shared by all coroutine synchronization primitives.
constexpr uint64_t kFiberWaitForever = std::numeric_limits<uint64_t>::max();

// WaitQueue only owns the Fiber waiters.  Its caller supplies the short-lived
// ThreadMutex that protects the associated predicate (for example, mutex
// ownership or a semaphore count).  This lets a waiter enqueue itself before
// that predicate lock is released, avoiding a lost wake-up.
class WaitQueue : Nonecopyable {
 public:
  // `guard` must be locked by the caller.  It is released before this method
  // yields and is not locked on return.
  int waitLocked(ThreadMutex& guard, uint64_t timeout_ms = kFiberWaitForever);

  // `guard` must be locked by the caller.
  bool wakeOneLocked(int error = 0);
  size_t wakeAllLocked(int error = 0);
  bool emptyLocked() const { return waiters_.empty(); }

 private:
  struct Waiter;

  bool completeLocked(const std::shared_ptr<Waiter>& waiter, int error);
  void timeout(const std::shared_ptr<Waiter>& waiter, ThreadMutex* guard);

  std::deque<std::shared_ptr<Waiter>> waiters_;
};

class FiberMutex : Nonecopyable {
 public:
  int lock(uint64_t timeout_ms = kFiberWaitForever);
  int try_lock();
  int unlock();
  bool locked() const;
  bool ownedByCurrent() const;
  size_t cancelWaiters(int error = ECANCELED);

 private:
  friend class FiberConditionVariable;

  ThreadMutex guard_;
  Fiber::ptr owner_;
  // Reserve an unlocked mutex for the waiter selected by unlock(). Without
  // handoff, newly scheduled Fibers can repeatedly barge ahead of an older
  // RPC waiter and turn sustained load into multi-second tail latency.
  bool handoff_ = false;
  WaitQueue waiters_;
};

class FiberConditionVariable : Nonecopyable {
 public:
  // Returns 0 when notified; otherwise -1 and sets errno to ETIMEDOUT or the
  // cancellation error.  The supplied mutex is always re-acquired first.
  int wait(FiberMutex& mutex, uint64_t timeout_ms = kFiberWaitForever);
  bool notifyOne();
  size_t notifyAll();
  size_t cancelWaiters(int error = ECANCELED);

 private:
  ThreadMutex guard_;
  WaitQueue waiters_;
};

class FiberSemaphore : Nonecopyable {
 public:
  explicit FiberSemaphore(uint64_t count = 0) : count_(count) {}

  int wait(uint64_t count = 1, uint64_t timeout_ms = kFiberWaitForever);
  void signal(uint64_t count = 1);
  uint64_t count() const;
  size_t cancelWaiters(int error = ECANCELED);

 private:
  ThreadMutex guard_;
  uint64_t count_ = 0;
  WaitQueue waiters_;
};

}  // namespace pulsar

#endif  // __PULSAR_FIBER_SYNC_H__
