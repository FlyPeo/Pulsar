#include <pulsar/rpc.hpp>
#include <rpc_echo.pb.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

class MultiplexEchoImpl final : public example::EchoService {
 public:
  std::atomic<int> call_count{0};

  void Echo(google::protobuf::RpcController*, const example::EchoRequest* request,
            example::EchoResponse* response, google::protobuf::Closure* done) override {
    int idx = call_count.fetch_add(1);
    // Introduce jitter so responses finish out-of-order
    if (idx % 2 == 0) {
      std::this_thread::sleep_for(20ms);
    } else {
      std::this_thread::sleep_for(5ms);
    }
    response->set_text(request->text());
    done->Run();
  }
};

}  // namespace

int main() {
  std::cout << "[test] rpc_multiplex_check starting..." << std::endl;

  pulsar::IOManager io(4, false, "rpc_mux_io");

  MultiplexEchoImpl service;
  pulsar::rpc::Provider provider(io, 8, 256, 64, 5s);
  assert(provider.Register(&service));
  assert(provider.Start("127.0.0.1", 0));
  const uint16_t port = provider.port();
  assert(port > 0);
  std::cout << "[test] Provider listening on port " << port << std::endl;

  // Single-connection multiplexed channel
  pulsar::rpc::Channel channel(io, "127.0.0.1", port, 1, 5s, true);
  example::EchoService::Stub stub(&channel);

  constexpr int kTotalRequests = 50;
  std::atomic<int> completed{0};
  std::atomic<int> failed{0};

  // Launch kTotalRequests concurrent fibers calling Echo on the single connection
  for (int i = 0; i < kTotalRequests; ++i) {
    const std::string text = "echo_req_" + std::to_string(i);
    io.scheduler([&stub, &completed, &failed, text]() {
      example::EchoRequest request;
      request.set_text(text);
      example::EchoResponse response;
      pulsar::rpc::Controller controller;

      stub.Echo(&controller, &request, &response, nullptr);

      if (controller.Failed()) {
        std::cerr << "RPC failed: " << controller.ErrorText() << std::endl;
        failed.fetch_add(1);
      } else if (response.text() != text) {
        std::cerr << "Mismatch: expected " << text << ", got " << response.text() << std::endl;
        failed.fetch_add(1);
      } else {
        completed.fetch_add(1);
      }
    });
  }

  // Wait for all requests to finish
  int waited = 0;
  while (completed.load() + failed.load() < kTotalRequests && waited++ < 500) {
    std::this_thread::sleep_for(20ms);
  }

  std::cout << "[test] Completed: " << completed.load()
            << ", Failed: " << failed.load()
            << " out of " << kTotalRequests << " requests." << std::endl;

  assert(failed.load() == 0);
  assert(completed.load() == kTotalRequests);
  assert(service.call_count.load() == kTotalRequests);

  assert(provider.Stop(5s));
  io.stop();

  std::cout << "[test] rpc_multiplex_check passed successfully!" << std::endl;
  return 0;
}
