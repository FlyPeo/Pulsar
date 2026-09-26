#include <pulsar/net.hpp>

#include <cerrno>
#include <chrono>
#include <future>
#include <string>
#include <vector>
#include <sys/socket.h>
#include <unistd.h>

using namespace std::chrono_literals;

int main() {
  pulsar::IOManager io(4, false, "net-check");
  pulsar::net::Server server(io, [](pulsar::net::Connection::ptr conn) {
    char data[64];
    if (conn->ReadExact(data, sizeof(data), 2s))
      conn->WriteAll(data, sizeof(data), 2s);
  }, 64);
  if (!server.Start("127.0.0.1", 0)) return 1;
  const auto port = server.port();
  std::vector<std::future<bool>> results;
  for (int i = 0; i < 32; ++i) {
    auto done = std::make_shared<std::promise<bool>>();
    results.push_back(done->get_future());
    io.scheduler([&, done, i] {
      auto conn = pulsar::net::Connection::Connect(io, "127.0.0.1", port, 2s);
      const std::string message(64, static_cast<char>('a' + i % 26));
      char reply[64];
      const bool ok = conn && conn->WriteAll(message.data(), 1, 2s) &&
                      conn->WriteAll(message.data() + 1, 63, 2s) &&
                      conn->ReadExact(reply, sizeof(reply), 2s) &&
                      std::string(reply, sizeof(reply)) == message;
      if (conn) conn->Close();
      done->set_value(ok);
    });
  }
  for (auto& result : results)
    if (result.wait_for(5s) != std::future_status::ready || !result.get()) return 2;

  auto done = std::make_shared<std::promise<bool>>();
  auto result = done->get_future();
  io.scheduler([&, done] {
    auto conn = pulsar::net::Connection::Connect(io, "127.0.0.1", port, 2s);
    char byte;
    const bool timed_out = conn && conn->ReadSome(&byte, 1, 20ms) < 0 && errno == ETIMEDOUT;
    if (conn) conn->Close();
    done->set_value(timed_out);
  });
  if (result.wait_for(5s) != std::future_status::ready || !result.get()) return 3;
  server.Stop();

  // Closing a connection from another thread must wake a parked reader once.
  auto parked = std::make_shared<std::promise<pulsar::net::Connection::ptr>>();
  auto parked_result = parked->get_future();
  auto resumed = std::make_shared<std::promise<bool>>();
  auto resumed_result = resumed->get_future();
  pulsar::net::Server closing(io, [parked, resumed](pulsar::net::Connection::ptr conn) {
    parked->set_value(conn);
    char byte;
    resumed->set_value(conn->ReadSome(&byte, 1, 5s) < 0 && errno == EBADF);
  });
  if (!closing.Start("127.0.0.1", 0)) return 4;
  auto connect_done = std::make_shared<std::promise<bool>>();
  auto connect_result = connect_done->get_future();
  io.scheduler([&, connect_done] {
    auto conn = pulsar::net::Connection::Connect(io, "127.0.0.1", closing.port(), 2s);
    connect_done->set_value(static_cast<bool>(conn));
    if (conn) {
      char byte;
      conn->ReadSome(&byte, 1, 2s);
    }
  });
  if (parked_result.wait_for(3s) != std::future_status::ready) return 5;
  auto server_conn = parked_result.get();
  server_conn->Close();
  if (resumed_result.wait_for(3s) != std::future_status::ready || !resumed_result.get()) return 6;
  closing.Stop();
  if (connect_result.wait_for(3s) != std::future_status::ready || !connect_result.get()) return 7;

  // A one-connection limit rejects overload without growing the handler set.
  pulsar::net::Server bounded(io, [](pulsar::net::Connection::ptr conn) {
    char byte;
    conn->ReadSome(&byte, 1, 2s);
  }, 1);
  if (!bounded.Start("127.0.0.1", 0)) return 8;
  auto first = std::make_shared<std::promise<pulsar::net::Connection::ptr>>();
  auto first_result = first->get_future();
  io.scheduler([&, first] {
    first->set_value(pulsar::net::Connection::Connect(io, "127.0.0.1", bounded.port(), 2s));
  });
  if (first_result.wait_for(3s) != std::future_status::ready) return 9;
  auto first_conn = first_result.get();
  if (!first_conn) return 10;
  for (int n = 0; n < 100 && bounded.active_connections() != 1; ++n) ::usleep(1000);
  if (bounded.active_connections() != 1) return 11;
  auto second = std::make_shared<std::promise<bool>>();
  auto second_result = second->get_future();
  io.scheduler([&, second] {
    auto conn = pulsar::net::Connection::Connect(io, "127.0.0.1", bounded.port(), 2s);
    char byte;
    const bool rejected = conn && conn->ReadSome(&byte, 1, 2s) <= 0;
    second->set_value(rejected);
  });
  if (second_result.wait_for(3s) != std::future_status::ready || !second_result.get()) return 12;
  if (bounded.active_connections() != 1) return 13;
  first_conn->Close();
  bounded.Stop();

  // A non-reading peer must not let WriteAll occupy an I/O worker indefinitely.
  pulsar::net::Server stalled(io, [](pulsar::net::Connection::ptr) {
    ::usleep(200000);
  });
  if (!stalled.Start("127.0.0.1", 0)) return 14;
  auto pressure = std::make_shared<std::promise<bool>>();
  auto pressure_result = pressure->get_future();
  io.scheduler([&, pressure] {
    auto conn = pulsar::net::Connection::Connect(io, "127.0.0.1", stalled.port(), 2s);
    if (!conn) {
      pressure->set_value(false);
      return;
    }
    const int small = 4096;
    ::setsockopt(conn->fd(), SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
    const std::string large(8 * 1024 * 1024, 'x');
    const bool bounded_write = !conn->WriteAll(large.data(), large.size(), 20ms) &&
                               errno == ETIMEDOUT;
    conn->Close();
    pressure->set_value(bounded_write);
  });
  if (pressure_result.wait_for(3s) != std::future_status::ready || !pressure_result.get())
    return 15;
  stalled.Stop();
  return 0;
}
