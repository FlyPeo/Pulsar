#include <pulsar/rpc.hpp>
#include <rpc_echo.pb.h>

#include <chrono>
#include <atomic>
#include <future>
#include <iostream>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

class EchoService final : public example::EchoService {
 public:
  std::atomic<int> entered{0};
  void Echo(google::protobuf::RpcController*, const example::EchoRequest* request,
            example::EchoResponse* response, google::protobuf::Closure* done) override {
    entered.fetch_add(1);
    if (request->has_delay_ms())
      std::this_thread::sleep_for(std::chrono::milliseconds(request->delay_ms()));
    response->set_text(request->text());
    done->Run();
  }
};

int main() {
  pulsar::IOManager io(1, false, "rpc-echo");
  EchoService service;
  pulsar::rpc::Provider provider(io, 4, 32);
  if (!provider.Register(&service) || !provider.Start("127.0.0.1", 0)) return 1;
  pulsar::rpc::Channel channel(io, "127.0.0.1", provider.port(), 16);
  example::EchoService::Stub stub(&channel);

  std::vector<std::future<bool>> results;
  for (int i = 0; i < 16; ++i) {
    auto done = std::make_shared<std::promise<bool>>();
    results.push_back(done->get_future());
    io.scheduler([&stub, done, i] {
      example::EchoRequest request;
      request.set_text("echo-" + std::to_string(i));
      request.set_delay_ms(200);
      example::EchoResponse response;
      pulsar::rpc::Controller controller;
      stub.Echo(&controller, &request, &response, nullptr);
      done->set_value(!controller.Failed() && response.text() == request.text());
    });
  }
  for (int n = 0; n < 5000 && service.entered.load() < 4; ++n)
    std::this_thread::sleep_for(1ms);
  if (service.entered.load() < 4) return 2;
  std::promise<void> heartbeat;
  auto heartbeat_result = heartbeat.get_future();
  io.scheduler([&heartbeat] { heartbeat.set_value(); });
  if (heartbeat_result.wait_for(100ms) != std::future_status::ready) return 3;
  for (auto& result : results)
    if (result.wait_for(5s) != std::future_status::ready || !result.get()) return 4;
  if (!provider.Stop()) return 5;
  std::cout << "16 RPC calls passed\n";
  return 0;
}
