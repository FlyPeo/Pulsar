#ifndef __PULSAR_MUTEX_H_
#define __PULSAR_MUTEX_H_

#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <atomic>
#include <cerrno>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <thread>

#include "noncopyable.hpp"
#include "utils.hpp"

namespace pulsar {
// 信号量
class Semaphore : Nonecopyable {
 public:
  Semaphore(uint32_t count = 0) { CondPanic(sem_init(&semaphore_, 0, count) == 0, "sem_init error"); }

  ~Semaphore() { CondPanic(sem_destroy(&semaphore_) == 0, "sem_destroy error"); }

  void wait() {
    int rt = 0;
    do {
      rt = sem_wait(&semaphore_);
    } while (rt == -1 && errno == EINTR);
    CondPanic(rt == 0, "sem_wait error");
  }

  void notify() { CondPanic(sem_post(&semaphore_) == 0, "sem_post error"); }

  void signal() { notify(); }

 private:
  sem_t semaphore_;
};

// 局部锁类模板
template <class T>
struct ScopedLockImpl {
 public:
  ScopedLockImpl(T &mutex) : m_(mutex) {
    // std::cout << "n lock" << std::endl;
    m_.lock();
    isLocked_ = true;
  }

  void lock() {
    if (!isLocked_) {
      std::cout << "lock" << std::endl;
      m_.lock();
      isLocked_ = true;
    }
  }

  void unlock() {
    if (isLocked_) {
      // std::cout << "unlock" << std::endl;
      m_.unlock();
      isLocked_ = false;
    }
  }

  ~ScopedLockImpl() {
    // std::cout << "unlock" << std::endl;
    unlock();
  }

 private:
  // mutex
  T &m_;
  // 是否已经上锁
  bool isLocked_;
};

template <class T>
struct ReadScopedLockImpl {
 public:
  ReadScopedLockImpl(T &mutex) : mutex_(mutex) {
    mutex_.rdlock();
    isLocked_ = true;
  }
  ~ReadScopedLockImpl() { unlock(); }
  void lock() {
    if (!isLocked_) {
      mutex_.rdlock();
      isLocked_ = true;
    }
  }
  void unlock() {
    if (isLocked_) {
      mutex_.unlock();
      isLocked_ = false;
    }
  }

 private:
  /// mutex
  T &mutex_;
  /// 是否已上锁
  bool isLocked_;
};

template <class T>
struct WriteScopedLockImpl {
 public:
  WriteScopedLockImpl(T &mutex) : mutex_(mutex) {
    mutex_.wrlock();
    isLocked_ = true;
  }

  ~WriteScopedLockImpl() { unlock(); }
  void lock() {
    if (!isLocked_) {
      mutex_.wrlock();
      isLocked_ = true;
    }
  }
  void unlock() {
    if (isLocked_) {
      mutex_.unlock();
      isLocked_ = false;
    }
  }

 private:
  /// Mutex
  T &mutex_;
  /// 是否已上锁
  bool isLocked_;
};

class Spinlock : Nonecopyable {
 public:
  typedef ScopedLockImpl<Spinlock> Lock;

  Spinlock() { CondPanic(pthread_spin_init(&m_, PTHREAD_PROCESS_PRIVATE) == 0, "spinlock init error"); }

  ~Spinlock() { CondPanic(pthread_spin_destroy(&m_) == 0, "spinlock destroy error"); }

  void lock() { CondPanic(pthread_spin_lock(&m_) == 0, "spinlock lock error"); }

  void unlock() { CondPanic(pthread_spin_unlock(&m_) == 0, "spinlock unlock error"); }

 private:
  pthread_spinlock_t m_;
};

// A kernel-thread mutex.  Keep this for runtime metadata that is accessed by
// multiple OS threads (scheduler queues, fd tables, timer tables, ...).  It is
// deliberately not coroutine-aware: contending on it blocks the OS thread.
class ThreadMutex : Nonecopyable {
 public:
  typedef ScopedLockImpl<ThreadMutex> Lock;

  ThreadMutex() { CondPanic(0 == pthread_mutex_init(&m_, nullptr), "lock init success"); }

  void lock() { CondPanic(0 == pthread_mutex_lock(&m_), "lock error"); }

  void unlock() { CondPanic(0 == pthread_mutex_unlock(&m_), "unlock error"); }

  ~ThreadMutex() { CondPanic(0 == pthread_mutex_destroy(&m_), "destroy lock error"); }

 private:
  pthread_mutex_t m_;
};

// Compatibility alias for the existing runtime code.  New code should use
// ThreadMutex when it specifically needs an OS-thread blocking mutex.
using Mutex = ThreadMutex;

class RWMutex : Nonecopyable {
 public:
  // 局部读锁
  typedef ReadScopedLockImpl<RWMutex> ReadLock;
  // 局部写锁
  typedef WriteScopedLockImpl<RWMutex> WriteLock;

  RWMutex() { pthread_rwlock_init(&m_, nullptr); }
  ~RWMutex() { pthread_rwlock_destroy(&m_); }

  void rdlock() { pthread_rwlock_rdlock(&m_); }

  void wrlock() { pthread_rwlock_wrlock(&m_); }

  void unlock() { pthread_rwlock_unlock(&m_); }

 private:
  pthread_rwlock_t m_;
};
}  // namespace pulsar

#endif
