#include <pulsar/file.hpp>
#include <pulsar/iomanager.hpp>

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;

int main() {
  std::cout << "[test] file_check starting..." << std::endl;

  pulsar::IOManager io(1, false, "file_check_io", 4);

  // Test 1: Verify asyncBlocking does not block the worker thread
  std::atomic<bool> fast_fiber_ran{false};
  std::atomic<bool> slow_fiber_done{false};

  io.scheduler([&]() {
    // Fiber 1: does a blocking call that takes 100ms
    auto ret = pulsar::IOManager::AsyncBlocking([]() -> int {
      std::this_thread::sleep_for(100ms);
      return 42;
    });
    assert(ret == 42);
    slow_fiber_done = true;
  });

  io.scheduler([&]() {
    // Fiber 2: runs on the same single-threaded IOManager worker!
    // Since Fiber 1 yielded cooperatively, Fiber 2 MUST run while Fiber 1 is sleeping!
    std::this_thread::sleep_for(10ms);
    fast_fiber_ran = true;
    assert(!slow_fiber_done); // Fiber 1 must not be finished yet!
  });

  // Give fibers time to run
  int waited = 0;
  while ((!fast_fiber_ran || !slow_fiber_done) && waited++ < 200) {
    std::this_thread::sleep_for(10ms);
  }
  assert(fast_fiber_ran);
  assert(slow_fiber_done);
  std::cout << "[test] asyncBlocking non-blocking offload passed!" << std::endl;

  // Test 2: File API (open, write, read, close)
  std::atomic<bool> file_test_passed{false};
  const std::string test_path = "/tmp/pulsar_test_file_" + std::to_string(getpid()) + ".tmp";

  io.scheduler([&]() {
    {
      auto file = pulsar::file::File::Open(test_path, O_CREAT | O_RDWR | O_TRUNC, 0644);
      assert(file.isOpen());

      const std::string content = "Hello Pulsar File Offload!";
      ssize_t written = file.write(content.data(), content.size());
      assert(written == static_cast<ssize_t>(content.size()));

      int sync_ret = file.fsync();
      assert(sync_ret == 0);

      // lseek to beginning
      off_t off = file.lseek(0, SEEK_SET);
      assert(off == 0);

      char buf[64] = {0};
      ssize_t read_bytes = file.read(buf, sizeof(buf) - 1);
      assert(read_bytes == static_cast<ssize_t>(content.size()));
      assert(std::string(buf) == content);

      // Test pread / pwrite
      const std::string extra = "Pulsar";
      ssize_t pw = file.pwrite(extra.data(), extra.size(), 6);
      assert(pw == static_cast<ssize_t>(extra.size()));

      char pbuf[32] = {0};
      ssize_t pr = file.pread(pbuf, extra.size(), 6);
      assert(pr == static_cast<ssize_t>(extra.size()));
      assert(std::string(pbuf, pr) == extra);

      file.close();
      assert(!file.isOpen());
    }

    // Clean up
    ::unlink(test_path.c_str());
    file_test_passed = true;
  });

  waited = 0;
  while (!file_test_passed && waited++ < 100) {
    std::this_thread::sleep_for(10ms);
  }
  assert(file_test_passed);
  std::cout << "[test] pulsar::file operations passed!" << std::endl;

  io.stop();
  std::cout << "[test] file_check completed successfully!" << std::endl;
  return 0;
}
