#include <atomic>
#include <chrono>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

#include <pulsar/fiber.hpp>
#include <pulsar/scheduler.hpp>

int main() {
  using pulsar::Fiber;

  Fiber::GetThis();
  int stage = 0;
  Fiber::ptr fiber(new Fiber(
      [&]() {
        stage = 1;
        Fiber::GetThis()->yield();
        stage = 2;
      },
      0, false));

  fiber->resume();
  if (stage != 1 || fiber->getState() != Fiber::READY) return 1;
  fiber->resume();
  if (stage != 2 || fiber->getState() != Fiber::TERM) return 2;

  fiber->reset([&]() { stage = 3; });
  fiber->resume();
  if (stage != 3 || fiber->getState() != Fiber::TERM) return 3;

  Fiber::ptr throwing(new Fiber([]() { throw std::runtime_error("fiber failure"); }, 0, false));
  try {
    throwing->resume();
    return 4;
  } catch (const std::runtime_error &) {
    if (throwing->getState() != Fiber::TERM) return 5;
  }

  pulsar::Scheduler scheduler(1, false, "fiber-exception-check");
  std::promise<void> continued;
  std::future<void> result = continued.get_future();
  scheduler.scheduler([]() { throw std::runtime_error("scheduled fiber failure"); });
  scheduler.scheduler([&continued]() { continued.set_value(); });
  scheduler.start();
  if (result.wait_for(std::chrono::seconds(3)) != std::future_status::ready) return 6;
  scheduler.stop();

  std::promise<void> entered;
  std::atomic<bool> release{false};
  Fiber::ptr running(new Fiber(
      [&]() {
        entered.set_value();
        while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
      },
      0, false));
  std::thread owner([&]() {
    Fiber::GetThis();
    running->resume();
  });
  entered.get_future().wait();
  std::atomic<size_t> concurrentResumeRejected{0};
  try {
    Fiber::GetThis();
    running->resume();
  } catch (const std::logic_error &) {
    concurrentResumeRejected.fetch_add(1);
  }
  std::vector<std::thread> contenders;
  for (size_t thread = 0; thread < 4; ++thread) {
    contenders.emplace_back([&]() {
      Fiber::GetThis();
      for (size_t attempt = 0; attempt < 64; ++attempt) {
        try {
          running->resume();
        } catch (const std::logic_error &) {
          concurrentResumeRejected.fetch_add(1);
        }
      }
    });
  }
  for (auto &contender : contenders) contender.join();
  release.store(true, std::memory_order_release);
  owner.join();
  if (concurrentResumeRejected.load() != 257 || running->getState() != Fiber::TERM) return 7;

  return Fiber::TotalFiberNum() == 4 ? 0 : 8;
}
