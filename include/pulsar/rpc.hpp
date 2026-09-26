#ifndef PULSAR_RPC_HPP
#define PULSAR_RPC_HPP

#include <pulsar/net.hpp>

#include <google/protobuf/service.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace pulsar::rpc {

class Controller final : public google::protobuf::RpcController {
 public:
  void Reset() override;
  bool Failed() const override;
  std::string ErrorText() const override;
  void StartCancel() override;
  void SetFailed(const std::string& reason) override;
  bool IsCanceled() const override;
  void NotifyOnCancel(google::protobuf::Closure* callback) override;

 private:
  mutable std::mutex mutex_;
  std::string error_;
  bool canceled_ = false;
  std::vector<google::protobuf::Closure*> cancel_callbacks_;
};

// V1 permits one in-flight request per connection. Concurrent fibers use
// distinct pool slots; an exhausted pool fails explicitly with overload.
class Channel final : public google::protobuf::RpcChannel {
 public:
  Channel(IOManager& owner, std::string ipv4, uint16_t port,
          size_t max_connections = 4,
          std::chrono::milliseconds timeout = std::chrono::seconds(5));
  ~Channel() override;
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;
  void CallMethod(const google::protobuf::MethodDescriptor* method,
                  google::protobuf::RpcController* controller,
                  const google::protobuf::Message* request,
                  google::protobuf::Message* response,
                  google::protobuf::Closure* done) override;

 private:
  struct State;
  std::shared_ptr<State> state_;
};

// Services are borrowed: they must outlive Provider and every asynchronous
// done callback. Each handler must invoke done exactly once.
class Provider final {
 public:
  Provider(IOManager& owner, size_t handler_threads = 8,
           size_t queued_handlers = 128, size_t max_connections = 1024,
           std::chrono::milliseconds request_timeout = std::chrono::seconds(5),
           size_t max_buffered_bytes = 128 * 1024 * 1024);
  ~Provider();
  Provider(const Provider&) = delete;
  Provider& operator=(const Provider&) = delete;
  bool Register(google::protobuf::Service* service);
  bool Start(const std::string& ipv4, uint16_t port);
  // Returns false if asynchronous handlers have not called done by deadline.
  bool Stop(std::chrono::milliseconds drain = std::chrono::seconds(5));
  uint16_t port() const noexcept;

 private:
  struct State;
  std::shared_ptr<State> state_;
};

}  // namespace pulsar::rpc
#endif
