#include <pulsar/net.hpp>

#include <arpa/inet.h>
#include <cerrno>
#include <climits>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <exception>
#include <utility>

namespace pulsar::net {
namespace {
using Clock = std::chrono::steady_clock;

Clock::time_point Deadline(std::chrono::milliseconds timeout) {
  return timeout.count() < 0 ? Clock::time_point::max() : Clock::now() + timeout;
}

bool Address(const std::string& ip, uint16_t port, sockaddr_in* out) {
  std::memset(out, 0, sizeof(*out));
  out->sin_family = AF_INET;
  out->sin_port = htons(port);
  if (::inet_pton(AF_INET, ip.c_str(), &out->sin_addr) != 1) {
    errno = EINVAL;
    return false;
  }
  return true;
}

bool Nonblocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  return flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

struct WaitState {
  std::atomic<unsigned> outcome{0};  // 1: readiness/close, 2: timeout
  Fiber::ptr fiber;
  IOManager* owner = nullptr;
  int worker = -1;
  void Finish(unsigned value) {
    unsigned expected = 0;
    if (outcome.compare_exchange_strong(expected, value)) {
      owner->scheduler(fiber, worker);
    }
  }
};
}  // namespace

Connection::Connection(IOManager& owner, int fd) : owner_(owner), fd_(fd) {}
Connection::~Connection() { Close(); }

int Connection::fd() const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  return fd_;
}

Connection::ptr Connection::Connect(IOManager& owner, const std::string& ip,
                                    uint16_t port, std::chrono::milliseconds timeout) {
  if (IOManager::GetThis() != &owner) {
    errno = EINVAL;
    return {};
  }
  sockaddr_in addr;
  if (!Address(ip, port, &addr)) return {};
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) return {};
  if (!Nonblocking(fd)) {
    ::close(fd);
    return {};
  }
  ptr conn(new Connection(owner, fd));
  const auto deadline = Deadline(timeout);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) return conn;
  if (errno != EINPROGRESS || !conn->Wait(WRITE, deadline)) return {};
  int error = 0;
  socklen_t length = sizeof(error);
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) < 0) return {};
  if (error != 0) {
    errno = error;
    return {};
  }
  return conn;
}

void Connection::Close() noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_.exchange(true)) return;
  if (fd_ >= 0) {
    owner_.cancelAll(fd_);
    ::shutdown(fd_, SHUT_RDWR);
    ::close(fd_);
    fd_ = -1;
  }
}

bool Connection::Wait(Event event, Clock::time_point deadline) {
  if (IOManager::GetThis() != &owner_) {
    errno = EINVAL;
    return false;
  }
  auto state = std::make_shared<WaitState>();
  state->fiber = Fiber::GetThis();
  state->owner = &owner_;
  state->worker = GetThreadId();
  int fd;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || fd_ < 0) {
      errno = EBADF;
      return false;
    }
    fd = fd_;
    if (owner_.addEvent(fd, event, [state] { state->Finish(1); }) != 0) return false;
  }
  Timer::ptr timer;
  if (deadline != Clock::time_point::max()) {
    auto self = shared_from_this();
    const auto remaining = deadline - Clock::now();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(remaining).count();
    timer = owner_.addTimer(static_cast<uint64_t>(std::max<int64_t>(0, ms)),
                            [state, self, fd, event] {
      unsigned expected = 0;
      if (state->outcome.compare_exchange_strong(expected, 2)) {
        {
          std::lock_guard<std::mutex> lock(self->mutex_);
          if (self->fd_ == fd && !self->closed_) self->owner_.cancelEvent(fd, event);
        }
        self->owner_.scheduler(state->fiber, state->worker);
      }
    });
  }
  state->fiber->yield();
  if (timer) timer->cancel();
  if (state->outcome.load() == 2) {
    errno = ETIMEDOUT;
    return false;
  }
  if (closed_) {
    errno = EBADF;
    return false;
  }
  return true;
}

ssize_t Connection::DoRead(void* buffer, size_t capacity, Clock::time_point deadline) {
  if (capacity == 0) return 0;
  for (;;) {
    ssize_t count;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (closed_) {
        errno = EBADF;
        return -1;
      }
      count = ::recv(fd_, buffer, capacity, 0);
    }
    if (count >= 0) return count;
    if (errno == EINTR) continue;
    if (errno != EAGAIN && errno != EWOULDBLOCK) return -1;
    if (!Wait(READ, deadline)) return -1;
  }
}

ssize_t Connection::ReadSome(void* buffer, size_t capacity,
                             std::chrono::milliseconds timeout) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (reading_) {
      errno = EBUSY;
      return -1;
    }
    reading_ = true;
  }
  const ssize_t result = DoRead(buffer, capacity, Deadline(timeout));
  {
    std::lock_guard<std::mutex> lock(mutex_);
    reading_ = false;
  }
  return result;
}

bool Connection::ReadExact(void* buffer, size_t size,
                           std::chrono::milliseconds timeout) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (reading_) {
      errno = EBUSY;
      return false;
    }
    reading_ = true;
  }
  const auto deadline = Deadline(timeout);
  size_t received = 0;
  while (received < size) {
    const ssize_t count = DoRead(static_cast<char*>(buffer) + received, size - received, deadline);
    if (count <= 0) {
      if (count == 0) errno = ECONNRESET;
      break;
    }
    received += static_cast<size_t>(count);
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    reading_ = false;
  }
  return received == size;
}

bool Connection::WriteAll(const void* buffer, size_t size,
                          std::chrono::milliseconds timeout) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (writing_) {
      errno = EBUSY;
      return false;
    }
    writing_ = true;
  }
  const auto deadline = Deadline(timeout);
  size_t sent = 0;
  while (sent < size) {
    ssize_t count;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (closed_) {
        errno = EBADF;
        break;
      }
      count = ::send(fd_, static_cast<const char*>(buffer) + sent,
                     size - sent, MSG_NOSIGNAL);
    }
    if (count > 0) {
      sent += static_cast<size_t>(count);
      continue;
    }
    if (count == 0) {
      errno = EPIPE;
      break;
    }
    if (errno == EINTR) continue;
    if (errno != EAGAIN && errno != EWOULDBLOCK) break;
    if (!Wait(WRITE, deadline)) break;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    writing_ = false;
  }
  return sent == size;
}

struct Server::State {
  State(IOManager& io, Handler cb, size_t limit)
      : owner(io), handler(std::move(cb)), max_connections(limit) {}
  IOManager& owner;
  Handler handler;
  const size_t max_connections;
  std::mutex mutex;
  Connection::ptr listener;
  std::vector<std::weak_ptr<Connection>> connections;
  std::atomic<bool> running{false};
  std::atomic<size_t> active{0};
  std::atomic<uint16_t> bound_port{0};
};

Server::Server(IOManager& owner, Handler handler, size_t max_connections)
    : state_(std::make_shared<State>(owner, std::move(handler), max_connections)) {}
Server::~Server() { Stop(); }

bool Server::Start(const std::string& ip, uint16_t port, int backlog) {
  auto state = state_;
  sockaddr_in addr;
  if (!Address(ip, port, &addr)) return false;
  std::lock_guard<std::mutex> lock(state->mutex);
  if (state->running || !state->handler || !state->max_connections) {
    errno = EINVAL;
    return false;
  }
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) return false;
  int reuse = 1;
  if (!Nonblocking(fd) ||
      ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0 ||
      ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
      ::listen(fd, backlog) != 0) {
    ::close(fd);
    return false;
  }
  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
    ::close(fd);
    return false;
  }
  state->listener.reset(new Connection(state->owner, fd));
  auto listener = state->listener;
  state->bound_port = ntohs(addr.sin_port);
  state->running = true;
  state->owner.scheduler([state, listener] {
    while (state->running) {
      int client_fd;
      {
        std::lock_guard<std::mutex> lock(listener->mutex_);
        if (listener->closed_) break;
        client_fd = ::accept4(listener->fd_, nullptr, nullptr,
                              SOCK_NONBLOCK | SOCK_CLOEXEC);
      }
      if (client_fd < 0) {
        if (errno == EINTR) continue;
        if ((errno == EAGAIN || errno == EWOULDBLOCK) &&
            listener->Wait(READ, Clock::time_point::max())) continue;
        break;
      }
      size_t count = state->active.load();
      while (count < state->max_connections &&
             !state->active.compare_exchange_weak(count, count + 1)) {}
      if (count >= state->max_connections) {
        ::close(client_fd);
        continue;
      }
      auto conn = Connection::ptr(new Connection(state->owner, client_fd));
      {
        std::lock_guard<std::mutex> guard(state->mutex);
        state->connections.erase(
            std::remove_if(state->connections.begin(), state->connections.end(),
                           [](const std::weak_ptr<Connection>& item) { return item.expired(); }),
            state->connections.end());
        state->connections.emplace_back(conn);
      }
      state->owner.scheduler([state, conn] {
        try {
          state->handler(conn);
        } catch (const std::exception&) {
        }
        conn->Close();
        state->active.fetch_sub(1);
      });
    }
  });
  return true;
}

void Server::Stop() noexcept {
  auto state = state_;
  if (!state->running.exchange(false)) return;
  Connection::ptr listener;
  std::vector<Connection::ptr> connections;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    listener = std::move(state->listener);
    for (auto& item : state->connections)
      if (auto conn = item.lock()) connections.push_back(std::move(conn));
    state->connections.clear();
  }
  if (listener) listener->Close();
  for (auto& conn : connections) conn->Close();
  state->bound_port = 0;
}

uint16_t Server::port() const noexcept { return state_->bound_port.load(); }
size_t Server::active_connections() const noexcept { return state_->active.load(); }
}  // namespace pulsar::net
