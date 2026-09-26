#include <mprpc_channel.h>
#include <mprpc_controller.h>
#include <rpc_provider.h>
#include <rpc_echo.pb.h>

#include <cstdlib>
#include <iostream>
#include <string>

class EchoImpl final : public example::EchoService {
 public:
  void Echo(google::protobuf::RpcController*, const example::EchoRequest* request,
            example::EchoResponse* response, google::protobuf::Closure* done) override {
    response->set_text(request->text());
    done->Run();
  }
};

int main(int argc, char** argv) {
  if (argc != 3) return 2;
  const auto port = static_cast<short>(std::atoi(argv[2]));
  if (!port) return 2;
  if (std::string(argv[1]) == "server") {
    EchoImpl service;
    RpcProvider provider;
    provider.NotifyService(&service);
    provider.Run(0, port);
    return 0;
  }
  if (std::string(argv[1]) != "client") return 2;
  MprpcChannel channel("127.0.0.1", port, false);
  example::EchoService::Stub stub(&channel);
  example::EchoRequest request;
  request.set_text("wire-compatible");
  example::EchoResponse response;
  MprpcController controller;
  stub.Echo(&controller, &request, &response, nullptr);
  if (controller.Failed()) std::cerr << controller.ErrorText() << '\n';
  return !controller.Failed() && response.text() == request.text() ? 0 : 1;
}
