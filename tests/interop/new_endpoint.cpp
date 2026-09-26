#include <pulsar/rpc.hpp>
#include <rpc_echo.pb.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <future>
#include <iostream>
#include <thread>

namespace {
std::atomic<bool> stopping{false};
void Signal(int) { stopping = true; }
class EchoImpl final : public example::EchoService {
 public:
  void Echo(google::protobuf::RpcController*, const example::EchoRequest* request,
            example::EchoResponse* response, google::protobuf::Closure* done) override {
    response->set_text(request->text());
    done->Run();
  }
};
}  // namespace

int main(int argc, char** argv) {
  if (argc != 3) return 2;
  const auto port = static_cast<uint16_t>(std::atoi(argv[2]));
  if (!port) return 2;
  pulsar::IOManager io(2, false, "interop-new");
  if (std::string(argv[1]) == "server") {
    EchoImpl service;
    pulsar::rpc::Provider provider(io);
    if (!provider.Register(&service) || !provider.Start("127.0.0.1", port)) return 1;
    std::signal(SIGTERM, Signal);
    while (!stopping) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    return provider.Stop() ? 0 : 1;
  }
  if (std::string(argv[1]) != "client") return 2;
  pulsar::rpc::Channel channel(io, "127.0.0.1", port);
  example::EchoService::Stub stub(&channel);
  std::promise<bool> done;
  auto result = done.get_future();
  io.scheduler([&] {
    example::EchoRequest request;
    request.set_text("wire-compatible");
    example::EchoResponse response;
    pulsar::rpc::Controller controller;
    stub.Echo(&controller, &request, &response, nullptr);
    if (controller.Failed()) std::cerr << controller.ErrorText() << '\n';
    done.set_value(!controller.Failed() && response.text() == request.text());
  });
  if (result.wait_for(std::chrono::seconds(8)) != std::future_status::ready) return 1;
  return result.get() ? 0 : 1;
}
