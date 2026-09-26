#ifndef PULSAR_NET_HPP
#define PULSAR_NET_HPP

#include <pulsar/iomanager.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <sys/types.h>

namespace pulsar::net {

// Explicit, nonblocking TCP API. All I/O methods run in a Fiber on owner().
// At most one reader and one writer may wait on a connection at a time.
class Connection final : public std::enable_shared_from_this<Connection> {
 public:
  using ptr = std::shared_ptr<Connection>;
  ~Connection();
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  static ptr Connect(IOManager& owner, const std::string& ipv4, uint16_t port,
                     std::chrono::milliseconds timeout = std::chrono::seconds(5));
  int fd() const noexcept;
  IOManager& owner() const noexcept { return owner_; }
  bool closed() const noexcept { return closed_.load(); }
  void Close() noexcept;
  ssize_t ReadSome(void* buffer, size_t capacity, std::chrono::milliseconds timeout);
  bool ReadExact(void* buffer, size_t size, std::chrono::milliseconds timeout);
  bool WriteAll(const void* buffer, size_t size, std::chrono::milliseconds timeout);

 private:
  friend class Server;
  Connection(IOManager& owner, int fd);
  bool Wait(Event event, std::chrono::steady_clock::time_point deadline);
  ssize_t DoRead(void* buffer, size_t capacity,
                 std::chrono::steady_clock::time_point deadline);
  IOManager& owner_;
  mutable std::mutex mutex_;
  int fd_;
  bool reading_ = false;
  bool writing_ = false;
  std::atomic<bool> closed_{false};
};

class Server final {
 public:
  using Handler = std::function<void(Connection::ptr)>;
  Server(IOManager& owner, Handler handler, size_t max_connections = 1024);
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;
  bool Start(const std::string& ipv4, uint16_t port, int backlog = 128);
  void Stop() noexcept;
  uint16_t port() const noexcept;
  size_t active_connections() const noexcept;

 private:
  struct State;
  std::shared_ptr<State> state_;
};

}  // namespace pulsar::net
#endif
