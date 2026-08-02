#include "sync.hpp"

#include <algorithm>
#include <cerrno>

#include "iomanager.hpp"
#include "scheduler.hpp"
#include "utils.hpp"

namespace pulsar {

struct WaitQueue::Waiter {
  Fiber::ptr fiber;
  Scheduler* scheduler = nullptr;
  int thread_id = -1;
  bool waiting = true;
  int error = 0;
};

bool WaitQueue::completeLocked(const std::shared_ptr<Waiter>& waiter, int error) {
  if (!waiter || !waiter->waiting) return false;

  const auto it = std::find(waiters_.begin(), waiters_.end(), waiter);
  if (it == waiters_.end()) return false;
  waiters_.erase(it);
  waiter->waiting = false;
  waiter->error = error;

  // A waiting Fiber is always resumed on the worker that parked it.  Besides
  // preserving thread-local scheduler state, this prevents a remote worker
  // from resuming a Fiber in the small interval before its yield completes.
  waiter->scheduler->scheduler(waiter->fiber, waiter->thread_id);
  return true;
}

void WaitQueue::timeout(const std::shared_ptr<Waiter>& waiter, ThreadMutex* guard) {
  ThreadMutex::Lock lock(*guard);
  completeLocked(waiter, ETIMEDOUT);
}

int WaitQueue::waitLocked(ThreadMutex& guard, uint64_t timeout_ms) {
  Scheduler* scheduler = Scheduler::GetThis();
  if (scheduler == nullptr) {
    guard.unlock();
    errno = EINVAL;
    return -1;
  }
  if (timeout_ms == 0) {
    guard.unlock();
    errno = ETIMEDOUT;
    return -1;
  }

  auto waiter = std::make_shared<Waiter>();
  waiter->fiber = Fiber::GetThis();
  waiter->scheduler = scheduler;
  waiter->thread_id = GetThreadId();
  waiters_.push_back(waiter);
  guard.unlock();

  Timer::ptr timer;
  if (timeout_ms != kFiberWaitForever) {
    IOManager* iom = IOManager::GetThis();
    if (iom == nullptr) {
      ThreadMutex::Lock lock(guard);
      completeLocked(waiter, ENOTSUP);
      errno = ENOTSUP;
      return -1;
    }
    std::weak_ptr<Waiter> weak_waiter(waiter);
    timer = iom->addTimer(timeout_ms, [this, weak_waiter, &guard]() {
      if (auto timed_out = weak_waiter.lock()) timeout(timed_out, &guard);
    });
  }

  Fiber::GetThis()->yield();
  if (timer) timer->cancel();

  ThreadMutex::Lock lock(guard);
  const int error = waiter->error;
  // A wakeup always removes the waiter.  This defensive branch also makes a
  // future scheduler interruption fail safely instead of leaking the waiter.
  if (waiter->waiting) completeLocked(waiter, ECANCELED);
  if (error != 0) {
    errno = error;
    return -1;
  }
  return 0;
}

bool WaitQueue::wakeOneLocked(int error) {
  while (!waiters_.empty()) {
    const auto waiter = waiters_.front();
    if (completeLocked(waiter, error)) return true;
  }
  return false;
}

size_t WaitQueue::wakeAllLocked(int error) {
  size_t count = 0;
  while (wakeOneLocked(error)) ++count;
  return count;
}

int FiberMutex::lock(uint64_t timeout_ms) {
  const uint64_t deadline = timeout_ms == kFiberWaitForever ? 0 : GetElapsedMS() + timeout_ms;
  const Fiber::ptr current = Fiber::GetThis();

  for (;;) {
    guard_.lock();
    if (!owner_) {
      owner_ = current;
      guard_.unlock();
      return 0;
    }
    if (owner_ == current) {
      guard_.unlock();
      errno = EDEADLK;
      return -1;
    }

    uint64_t remaining = kFiberWaitForever;
    if (timeout_ms != kFiberWaitForever) {
      const uint64_t now = GetElapsedMS();
      if (now >= deadline) {
        guard_.unlock();
        errno = ETIMEDOUT;
        return -1;
      }
      remaining = deadline - now;
    }
    if (waiters_.waitLocked(guard_, remaining) != 0) return -1;
  }
}

int FiberMutex::try_lock() {
  const Fiber::ptr current = Fiber::GetThis();
  ThreadMutex::Lock lock(guard_);
  if (!owner_) {
    owner_ = current;
    return 0;
  }
  errno = owner_ == current ? EDEADLK : EBUSY;
  return -1;
}

int FiberMutex::unlock() {
  const Fiber::ptr current = Fiber::GetThis();
  ThreadMutex::Lock lock(guard_);
  if (owner_ != current) {
    errno = EPERM;
    return -1;
  }
  owner_.reset();
  waiters_.wakeOneLocked();
  return 0;
}

bool FiberMutex::locked() const {
  auto* self = const_cast<FiberMutex*>(this);
  ThreadMutex::Lock lock(self->guard_);
  return static_cast<bool>(self->owner_);
}

bool FiberMutex::ownedByCurrent() const {
  auto* self = const_cast<FiberMutex*>(this);
  ThreadMutex::Lock lock(self->guard_);
  return self->owner_ == Fiber::GetThis();
}

size_t FiberMutex::cancelWaiters(int error) {
  ThreadMutex::Lock lock(guard_);
  return waiters_.wakeAllLocked(error);
}

int FiberConditionVariable::wait(FiberMutex& mutex, uint64_t timeout_ms) {
  if (!mutex.ownedByCurrent()) {
    errno = EPERM;
    return -1;
  }

  // Hold the condition queue guard before releasing the user mutex.  A
  // notifier cannot pass this point until our Fiber has been enqueued.
  guard_.lock();
  mutex.unlock();
  const int result = waiters_.waitLocked(guard_, timeout_ms);
  const int wait_error = result == 0 ? 0 : errno;

  // C++ condition-variable semantics require that the mutex is held again on
  // return, even after a timeout or cancellation.
  while (mutex.lock() != 0) {
    if (errno != EINTR) return -1;
  }
  if (wait_error != 0) {
    errno = wait_error;
    return -1;
  }
  return 0;
}

bool FiberConditionVariable::notifyOne() {
  ThreadMutex::Lock lock(guard_);
  return waiters_.wakeOneLocked();
}

size_t FiberConditionVariable::notifyAll() {
  ThreadMutex::Lock lock(guard_);
  return waiters_.wakeAllLocked();
}

size_t FiberConditionVariable::cancelWaiters(int error) {
  ThreadMutex::Lock lock(guard_);
  return waiters_.wakeAllLocked(error);
}

int FiberSemaphore::wait(uint64_t count, uint64_t timeout_ms) {
  if (count == 0) return 0;
  const uint64_t deadline = timeout_ms == kFiberWaitForever ? 0 : GetElapsedMS() + timeout_ms;
  for (;;) {
    guard_.lock();
    if (count_ >= count) {
      count_ -= count;
      guard_.unlock();
      return 0;
    }

    uint64_t remaining = kFiberWaitForever;
    if (timeout_ms != kFiberWaitForever) {
      const uint64_t now = GetElapsedMS();
      if (now >= deadline) {
        guard_.unlock();
        errno = ETIMEDOUT;
        return -1;
      }
      remaining = deadline - now;
    }
    if (waiters_.waitLocked(guard_, remaining) != 0) return -1;
  }
}

void FiberSemaphore::signal(uint64_t count) {
  if (count == 0) return;
  ThreadMutex::Lock lock(guard_);
  count_ += count;
  // Waiters may request different counts, so wake all and let them compete for
  // the updated count instead of allowing the first oversized request to hide
  // a later satisfiable one.
  waiters_.wakeAllLocked();
}

uint64_t FiberSemaphore::count() const {
  auto* self = const_cast<FiberSemaphore*>(this);
  ThreadMutex::Lock lock(self->guard_);
  return self->count_;
}

size_t FiberSemaphore::cancelWaiters(int error) {
  ThreadMutex::Lock lock(guard_);
  return waiters_.wakeAllLocked(error);
}

}  // namespace pulsar
