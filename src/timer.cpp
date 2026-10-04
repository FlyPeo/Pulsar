#include "timer.hpp"
#include "utils.hpp"

namespace pulsar {
bool Timer::Comparator::operator()(const Timer::ptr &lhs, const Timer::ptr &rhs) const {
  if (!lhs && !rhs) {
    return false;
  }
  if (!lhs) {
    return true;
  }
  if (!rhs) {
    return false;
  }
  if (lhs->next_ < rhs->next_) {
    return true;
  }
  if (rhs->next_ < lhs->next_) {
    return false;
  }
  return lhs.get() < rhs.get();
}
Timer::Timer(uint64_t ms, std::function<void()> cb, bool recuring, TimerManager *manager)
    : recurring_(recuring), ms_(ms), cb_(cb), manager_(manager) {
  next_ = GetElapsedMS() + ms_;
}
Timer::Timer(uint64_t next) : next_(next) {}
bool Timer::cancel() {
  if (cancelled_.exchange(true)) {
    return false;
  }
  RWMutex::WriteLock lock(manager_->mutex_);
  cb_ = nullptr;
  return true;
}

TimerManager::TimerManager() {
  previouseTime_ = GetElapsedMS();
  wheel0_.resize(256);
  wheel1_.resize(64);
  wheel2_.resize(64);
  wheel3_.resize(64);
}

TimerManager::~TimerManager() {}

Timer::ptr TimerManager::addTimer(uint64_t ms, std::function<void()> cb, bool recurring) {
  Timer::ptr timer(new Timer(ms, cb, recurring, this));
  RWMutex::WriteLock lock(mutex_);
  addTimer(timer, lock);
  return timer;
}

static void OnTimer(std::weak_ptr<void> weak_cond, std::function<void()> cb) {
  std::shared_ptr<void> tmp = weak_cond.lock();
  if (tmp) {
    cb();
  }
}

Timer::ptr TimerManager::addConditionTimer(uint64_t ms, std::function<void()> cb, std::weak_ptr<void> weak_cond,
                                           bool recurring) {
  return addTimer(ms, std::bind(&OnTimer, weak_cond, cb), recurring);
}

uint64_t TimerManager::getNextTimer() {
  tickled_ = false;
  uint64_t earliest = earliestNext_.load(std::memory_order_relaxed);
  uint64_t now_ms = GetElapsedMS();
  if (earliest == ~0ull) {
    RWMutex::ReadLock lock(mutex_);
    if (!timers_.empty()) {
      earliest = (*timers_.begin())->next_;
    }
  }
  if (earliest == ~0ull) {
    return ~0ull;
  }
  if (now_ms >= earliest) {
    return 0;
  }
  return earliest - now_ms;
}

void TimerManager::updateEarliestNext(uint64_t next_ms) {
  uint64_t cur = earliestNext_.load(std::memory_order_relaxed);
  while (next_ms < cur) {
    if (earliestNext_.compare_exchange_weak(cur, next_ms, std::memory_order_relaxed)) {
      break;
    }
  }
}

void TimerManager::insertToWheel(Timer::ptr val) {
  uint64_t now_ms = GetElapsedMS();
  uint64_t diff = val->next_ > now_ms ? val->next_ - now_ms : 0;
  updateEarliestNext(val->next_);

  if (diff < 256) {
    size_t slot = (val->next_) & 0xFF;
    wheel0_[slot].push_back(val);
  } else if (diff < (1ULL << 14)) {
    size_t slot = (val->next_ >> 8) & 0x3F;
    wheel1_[slot].push_back(val);
  } else if (diff < (1ULL << 20)) {
    size_t slot = (val->next_ >> 14) & 0x3F;
    wheel2_[slot].push_back(val);
  } else if (diff < (1ULL << 26)) {
    size_t slot = (val->next_ >> 20) & 0x3F;
    wheel3_[slot].push_back(val);
  } else {
    timers_.insert(val);
  }
}

void TimerManager::advanceWheel(uint64_t now_ms, std::vector<std::function<void()>> &cbs) {
  uint64_t nextEarliest = ~0ull;

  // Process wheel0
  for (size_t i = 0; i < wheel0_.size(); ++i) {
    auto &slot = wheel0_[i];
    if (slot.empty()) continue;
    std::vector<Timer::ptr> remaining;
    for (auto &timer : slot) {
      if (timer->cancelled_.load()) continue;
      if (timer->next_ <= now_ms) {
        if (timer->cb_) cbs.push_back(timer->cb_);
        if (timer->recurring_) {
          timer->next_ = now_ms + timer->ms_;
          timer->cancelled_ = false;
          insertToWheel(timer);
        } else {
          timer->cb_ = nullptr;
          timer->cancelled_ = true;
        }
      } else {
        remaining.push_back(timer);
        if (timer->next_ < nextEarliest) nextEarliest = timer->next_;
      }
    }
    slot.swap(remaining);
  }

  // Cascade upper wheels
  auto cascade = [&](std::vector<std::vector<Timer::ptr>> &wheel) {
    for (auto &slot : wheel) {
      if (slot.empty()) continue;
      std::vector<Timer::ptr> toInsert;
      toInsert.swap(slot);
      for (auto &timer : toInsert) {
        if (!timer->cancelled_.load()) {
          insertToWheel(timer);
        }
      }
    }
  };
  cascade(wheel1_);
  cascade(wheel2_);
  cascade(wheel3_);

  // Fallback tree timers
  while (!timers_.empty() && (*timers_.begin())->next_ <= now_ms) {
    auto timer = *timers_.begin();
    timers_.erase(timers_.begin());
    if (timer->cancelled_.load()) continue;
    if (timer->cb_) cbs.push_back(timer->cb_);
    if (timer->recurring_) {
      timer->next_ = now_ms + timer->ms_;
      timer->cancelled_ = false;
      insertToWheel(timer);
    } else {
      timer->cb_ = nullptr;
      timer->cancelled_ = true;
    }
  }
  if (!timers_.empty()) {
    if ((*timers_.begin())->next_ < nextEarliest) {
      nextEarliest = (*timers_.begin())->next_;
    }
  }

  earliestNext_.store(nextEarliest, std::memory_order_relaxed);
}

void TimerManager::listExpiredCb(std::vector<std::function<void()>> &cbs) {
  uint64_t now_ms = GetElapsedMS();
  RWMutex::WriteLock lock(mutex_);
  detectClockRolllover(now_ms);
  advanceWheel(now_ms, cbs);
}

void TimerManager::addTimer(Timer::ptr val, RWMutex::WriteLock &lock) {
  bool at_front = val->next_ < earliestNext_.load(std::memory_order_relaxed);
  insertToWheel(val);
  if (at_front && !tickled_) {
    tickled_ = true;
    lock.unlock();
    OnTimerInsertedAtFront();
  } else {
    lock.unlock();
  }
}

bool TimerManager::detectClockRolllover(uint64_t now_ms) {
  bool rollover = false;
  if (now_ms < previouseTime_ && now_ms < (previouseTime_ - 60 * 60 * 1000)) {
    rollover = true;
  }
  previouseTime_ = now_ms;
  return rollover;
}

}  // namespace pulsar

