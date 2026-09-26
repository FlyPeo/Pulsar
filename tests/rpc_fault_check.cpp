#include <pulsar/rpc.hpp>
#include <pulsar_rpc_header.pb.h>
#include <rpc_echo.pb.h>

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <string>
#include <thread>

using namespace std::chrono_literals;

namespace {
class EchoImpl final : public example::EchoService {
 public:
  std::atomic<int> calls{0};
  std::atomic<int> leader{1};
  void Echo(google::protobuf::RpcController*, const example::EchoRequest* request,
            example::EchoResponse* response, google::protobuf::Closure* done) override {
    ++calls;
    if (request->text() == "late") {
      std::thread([response, done] {
        std::this_thread::sleep_for(150ms);
        response->set_text("late");
        done->Run();
      }).detach();
      return;
    }
    if (request->text() == "slow") std::this_thread::sleep_for(30ms);
    response->set_text(request->text() == "leader"
                           ? "leader-" + std::to_string(leader.load())
                           : request->text());
    done->Run();
  }
};

int Connect(uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  timeval timeout{2, 0};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

std::string Frame(const std::string& text) {
  example::EchoRequest request;
  request.set_text(text);
  std::string args;
  request.SerializeToString(&args);
  RPC::RpcHeader header;
  header.set_service_name("EchoService");
  header.set_method_name("Echo");
  header.set_args_size(static_cast<uint32_t>(args.size()));
  std::string metadata;
  header.SerializeToString(&metadata);
  std::string frame;
  uint32_t length = metadata.size();
  while (length >= 128) {
    frame.push_back(static_cast<char>((length & 0x7f) | 0x80));
    length >>= 7;
  }
  frame.push_back(static_cast<char>(length));
  return frame + metadata + args;
}

bool Response(int fd, const std::string& expected) {
  uint32_t network_size = 0;
  if (::recv(fd, &network_size, 4, MSG_WAITALL) != 4) return false;
  const uint32_t size = ntohl(network_size);
  if (size > 1024) return false;
  std::string payload(size, '\0');
  if (size && ::recv(fd, payload.data(), size, MSG_WAITALL) != static_cast<ssize_t>(size))
    return false;
  example::EchoResponse response;
  return response.ParseFromString(payload) && response.text() == expected;
}

bool Call(pulsar::IOManager& io, example::EchoService::Stub& stub,
          const std::string& text, const std::string& expected) {
  auto done = std::make_shared<std::promise<bool>>();
  auto result = done->get_future();
  io.scheduler([&stub, done, text, expected] {
    example::EchoRequest request;
    request.set_text(text);
    example::EchoResponse response;
    pulsar::rpc::Controller controller;
    stub.Echo(&controller, &request, &response, nullptr);
    done->set_value(!controller.Failed() && response.text() == expected);
  });
  return result.wait_for(3s) == std::future_status::ready && result.get();
}

bool BadResponse(pulsar::IOManager& io, bool truncated) {
  const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listener < 0) return false;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = 0;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
      ::listen(listener, 1) != 0) {
    ::close(listener);
    return false;
  }
  socklen_t size = sizeof(address);
  ::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &size);
  std::thread peer([listener, truncated] {
    pollfd pending{listener, POLLIN, 0};
    const int fd = ::poll(&pending, 1, 3000) > 0
                       ? ::accept(listener, nullptr, nullptr)
                       : -1;
    if (fd >= 0) {
      char request[1024];
      ::recv(fd, request, sizeof(request), 0);
      const uint32_t length = htonl(truncated ? 32 : 0xffffffffu);
      ::send(fd, &length, 4, 0);
      if (truncated) ::send(fd, "xx", 2, 0);
      ::close(fd);
    }
    ::close(listener);
  });
  pulsar::rpc::Channel channel(io, "127.0.0.1", ntohs(address.sin_port), 1, 500ms);
  example::EchoService::Stub stub(&channel);
  auto done = std::make_shared<std::promise<bool>>();
  auto result = done->get_future();
  io.scheduler([&stub, done] {
    example::EchoRequest request;
    request.set_text("bad-response");
    example::EchoResponse response;
    pulsar::rpc::Controller controller;
    stub.Echo(&controller, &request, &response, nullptr);
    done->set_value(controller.Failed());
  });
  const bool failed = result.wait_for(3s) == std::future_status::ready && result.get();
  peer.join();
  return failed;
}
}  // namespace

int main() {
  pulsar::IOManager io(2, false, "rpc-fault");
  EchoImpl service;
  auto provider = std::make_unique<pulsar::rpc::Provider>(io, 2, 8, 32, 50ms);
  if (!provider->Register(&service) || !provider->Start("127.0.0.1", 0)) return 1;
  const uint16_t port = provider->port();
  pulsar::rpc::Channel channel(io, "127.0.0.1", port, 2, 500ms);
  example::EchoService::Stub stub(&channel);

  // Invalid varint and oversized header must close only the offending peer.
  int bad = Connect(port);
  if (bad < 0) return 2;
  const unsigned char overflow[5] = {0xff, 0xff, 0xff, 0xff, 0xff};
  if (::send(bad, overflow, sizeof(overflow), 0) != 5) return 3;
  char byte = 0;
  if (::recv(bad, &byte, 1, 0) > 0) return 4;
  ::close(bad);
  if (!Call(io, stub, "valid", "valid")) return 5;

  // A split request and two coalesced duplicates get two ordered replies.
  int raw = Connect(port);
  if (raw < 0) return 6;
  const auto frame = Frame("duplicate");
  for (size_t i = 0; i < frame.size(); ++i)
    if (::send(raw, frame.data() + i, 1, 0) != 1) return 7;
  if (::send(raw, frame.data(), frame.size(), 0) != static_cast<ssize_t>(frame.size()))
    return 8;
  if (!Response(raw, "duplicate") || !Response(raw, "duplicate")) return 9;
  ::close(raw);

  service.leader = 2;
  if (!Call(io, stub, "leader", "leader-2")) return 10;
  service.leader = 3;
  if (!Call(io, stub, "leader", "leader-3")) return 11;
  if (!BadResponse(io, false) || !BadResponse(io, true)) return 18;

  // With one pool slot, a second concurrent call is rejected before sending.
  pulsar::rpc::Channel narrow(io, "127.0.0.1", port, 1, 500ms);
  example::EchoService::Stub narrow_stub(&narrow);
  auto slow_done = std::make_shared<std::promise<bool>>();
  auto slow_result = slow_done->get_future();
  const int before_slow = service.calls.load();
  io.scheduler([&narrow_stub, slow_done] {
    example::EchoRequest request;
    request.set_text("slow");
    example::EchoResponse response;
    pulsar::rpc::Controller controller;
    narrow_stub.Echo(&controller, &request, &response, nullptr);
    slow_done->set_value(!controller.Failed() && response.text() == "slow");
  });
  for (int n = 0; n < 1000 && service.calls.load() == before_slow; ++n)
    std::this_thread::sleep_for(1ms);
  if (service.calls.load() == before_slow) return 19;
  auto overload = std::make_shared<std::promise<bool>>();
  auto overload_result = overload->get_future();
  io.scheduler([&narrow_stub, overload] {
    example::EchoRequest request;
    request.set_text("overload");
    example::EchoResponse response;
    pulsar::rpc::Controller controller;
    narrow_stub.Echo(&controller, &request, &response, nullptr);
    overload->set_value(controller.Failed() &&
                        controller.ErrorText().find("overloaded") != std::string::npos);
  });
  if (overload_result.wait_for(3s) != std::future_status::ready ||
      !overload_result.get()) return 20;
  if (slow_result.wait_for(3s) != std::future_status::ready || !slow_result.get())
    return 21;

  // Timed-out asynchronous completion must not send to a later connection.
  auto late = std::make_shared<std::promise<bool>>();
  auto late_result = late->get_future();
  io.scheduler([&stub, late] {
    example::EchoRequest request;
    request.set_text("late");
    example::EchoResponse response;
    pulsar::rpc::Controller controller;
    stub.Echo(&controller, &request, &response, nullptr);
    late->set_value(controller.Failed());
  });
  if (late_result.wait_for(3s) != std::future_status::ready || !late_result.get()) return 12;
  std::this_thread::sleep_for(200ms);
  if (!Call(io, stub, "after-late", "after-late")) return 13;

  // Reusing an old socket after server restart fails once; the next call
  // reconnects. The transport never replays the first call automatically.
  if (!provider->Stop()) return 14;
  provider.reset();
  auto restarted = std::make_unique<pulsar::rpc::Provider>(io, 2, 8, 32, 50ms);
  if (!restarted->Register(&service) || !restarted->Start("127.0.0.1", port))
    return 15;
  (void)Call(io, stub, "stale", "stale");
  if (!Call(io, stub, "reconnected", "reconnected")) return 16;
  auto late_after_stop = std::make_shared<std::promise<bool>>();
  auto after_stop_result = late_after_stop->get_future();
  io.scheduler([&stub, late_after_stop] {
    example::EchoRequest request;
    request.set_text("late");
    example::EchoResponse response;
    pulsar::rpc::Controller controller;
    stub.Echo(&controller, &request, &response, nullptr);
    late_after_stop->set_value(controller.Failed());
  });
  if (after_stop_result.wait_for(3s) != std::future_status::ready ||
      !after_stop_result.get()) return 22;
  if (restarted->Stop(10ms)) return 17;  // reports the outstanding done callback
  std::this_thread::sleep_for(200ms);
  if (!restarted->Stop()) return 23;
  restarted.reset();
  return 0;
}
