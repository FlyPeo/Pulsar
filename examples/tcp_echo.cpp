#include <pulsar/net.hpp>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <string>

using namespace std::chrono_literals;

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: pulsar-tcp-echo server|client PORT\n";
    return 2;
  }
  const int parsed = std::atoi(argv[2]);
  if (parsed < 1 || parsed > 65535) return 2;
  const auto port = static_cast<uint16_t>(parsed);
  pulsar::IOManager io(2, false, "tcp-echo");

  if (std::string(argv[1]) == "server") {
    std::promise<bool> done;
    auto result = done.get_future();
    pulsar::net::Server server(io, [&done](pulsar::net::Connection::ptr conn) {
      char data[5];
      const bool ok = conn->ReadExact(data, sizeof(data), 5s) &&
                      conn->WriteAll(data, sizeof(data), 5s);
      done.set_value(ok);
    });
    if (!server.Start("127.0.0.1", port)) return 1;
    if (result.wait_for(30s) != std::future_status::ready) {
      server.Stop();
      return 1;
    }
    const bool ok = result.get();
    server.Stop();
    return ok ? 0 : 1;
  }

  if (std::string(argv[1]) != "client") return 2;
  std::promise<bool> done;
  auto result = done.get_future();
  io.scheduler([&] {
    auto conn = pulsar::net::Connection::Connect(io, "127.0.0.1", port, 5s);
    char reply[5];
    const bool ok = conn && conn->WriteAll("hello", 5, 5s) &&
                    conn->ReadExact(reply, sizeof(reply), 5s) &&
                    std::string(reply, sizeof(reply)) == "hello";
    if (conn) conn->Close();
    done.set_value(ok);
  });
  if (result.wait_for(10s) != std::future_status::ready) return 1;
  if (!result.get()) return 1;
  std::cout << "hello\n";
  return 0;
}
