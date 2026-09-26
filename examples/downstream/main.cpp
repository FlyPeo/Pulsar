#include <pulsar/pulsar.h>

#ifdef PULSAR_HAS_NET
#include <pulsar/net.hpp>
#endif

#include <future>
#include <chrono>

int main() {
  pulsar::IOManager io(1, false, "consumer");
  std::promise<void> done;
  auto ready = done.get_future();
  io.scheduler([&done] { done.set_value(); });
  if (ready.wait_for(std::chrono::seconds(2)) != std::future_status::ready) return 1;
#ifdef PULSAR_HAS_NET
  pulsar::net::Server server(io, [](pulsar::net::Connection::ptr) {});
  if (!server.Start("127.0.0.1", 0)) return 2;
  server.Stop();
#endif
  return 0;
}
