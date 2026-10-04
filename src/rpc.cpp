#include <pulsar/rpc.hpp>
#include <pulsar/sync.hpp>

#include <pulsar_rpc_header.pb.h>

#include <arpa/inet.h>
#include <algorithm>
#include <cerrno>
#include <condition_variable>
#include <deque>
#include <functional>
#include <limits>
#include <map>
#include <thread>
#include <utility>

namespace pulsar::rpc {
namespace {
constexpr uint32_t kMaxHeader = 64 * 1024;
constexpr uint32_t kMaxMessage = 64 * 1024 * 1024;

void EncodeVarint32(uint32_t value, std::string* output) {
  while (value >= 128) {
    output->push_back(static_cast<char>((value & 0x7f) | 0x80));
    value >>= 7;
  }
  output->push_back(static_cast<char>(value));
}

bool ReadVarint32(net::Connection& conn, uint32_t* value,
                  std::chrono::milliseconds timeout) {
  *value = 0;
  for (unsigned i = 0; i < 5; ++i) {
    uint8_t byte = 0;
    if (!conn.ReadExact(&byte, 1, timeout)) return false;
    if (i == 4 && (byte & 0xf0)) {
      errno = EPROTO;
      return false;
    }
    *value |= static_cast<uint32_t>(byte & 0x7f) << (i * 7);
    if ((byte & 0x80) == 0) return true;
  }
  errno = EPROTO;
  return false;
}

class Executor {
 public:
  Executor(size_t threads, size_t capacity) : capacity_(capacity) {
    for (size_t i = 0; i < threads; ++i) {
      workers_.emplace_back([this] {
        for (;;) {
          std::function<void()> task;
          {
            std::unique_lock<std::mutex> lock(mutex_);
            wake_.wait(lock, [this] { return stopped_ || !queue_.empty(); });
            if (stopped_ && queue_.empty()) return;
            task = std::move(queue_.front());
            queue_.pop_front();
          }
          task();
        }
      });
    }
  }
  ~Executor() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopped_ = true;
    }
    wake_.notify_all();
    for (auto& worker : workers_) worker.join();
  }
  bool Submit(std::function<void()> task) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_ || queue_.size() >= capacity_) return false;
    queue_.push_back(std::move(task));
    wake_.notify_one();
    return true;
  }

 private:
  const size_t capacity_;
  std::mutex mutex_;
  std::condition_variable wake_;
  std::deque<std::function<void()>> queue_;
  std::vector<std::thread> workers_;
  bool stopped_ = false;
};

void Fail(google::protobuf::RpcController* controller, const std::string& error) {
  if (controller) controller->SetFailed(error);
}
}  // namespace

void Controller::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);
  error_.clear();
  canceled_ = false;
  cancel_callbacks_.clear();
}
bool Controller::Failed() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return !error_.empty();
}
std::string Controller::ErrorText() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return error_;
}
void Controller::StartCancel() {
  std::vector<google::protobuf::Closure*> callbacks;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (canceled_) return;
    canceled_ = true;
    callbacks.swap(cancel_callbacks_);
  }
  for (auto* callback : callbacks) callback->Run();
}
void Controller::SetFailed(const std::string& reason) {
  std::lock_guard<std::mutex> lock(mutex_);
  error_ = reason;
}
bool Controller::IsCanceled() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return canceled_;
}
void Controller::NotifyOnCancel(google::protobuf::Closure* callback) {
  bool run = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (canceled_)
      run = true;
    else
      cancel_callbacks_.push_back(callback);
  }
  if (run) callback->Run();
}

struct Channel::State : std::enable_shared_from_this<Channel::State> {
  struct Slot {
    bool busy = false;
    net::Connection::ptr conn;
  };
  struct PendingCall {
    Fiber::ptr waiter;
    int worker = -1;
    std::string payload;
    std::string error;
    bool done = false;
    bool failed = false;
  };

  State(IOManager& io, std::string address, uint16_t p, size_t limit,
        std::chrono::milliseconds duration, bool is_multiplex)
      : owner(io), ip(std::move(address)), port(p), timeout(duration),
        multiplex(is_multiplex) {
    if (!multiplex) {
      for (size_t i = 0; i < limit; ++i) slots.emplace_back(std::make_unique<Slot>());
    }
  }

  void ReaderLoop() {
    while (!closed && conn && !conn->closed()) {
      uint32_t header_size = 0;
      if (!ReadVarint32(*conn, &header_size, std::chrono::seconds(60))) {
        if (errno == ETIMEDOUT && !closed) {
          continue;
        }
        break;
      }
      if (header_size == 0 || header_size > kMaxHeader) {
        break;
      }
      std::string header_data(header_size, '\0');
      if (!conn->ReadExact(header_data.data(), header_size, std::chrono::seconds(10))) {
        break;
      }
      RPC::RpcHeader header;
      if (!header.ParseFromArray(header_data.data(), static_cast<int>(header_size))) {
        break;
      }
      std::string payload(header.args_size(), '\0');
      if (header.args_size() &&
          !conn->ReadExact(payload.data(), header.args_size(), std::chrono::seconds(10))) {
        break;
      }

      std::shared_ptr<PendingCall> call;
      {
        std::lock_guard<std::mutex> lock(pending_mutex);
        auto it = pending_calls.find(header.request_id());
        if (it != pending_calls.end()) {
          call = it->second;
          pending_calls.erase(it);
        }
      }
      if (call) {
        if (!header.error().empty()) {
          call->failed = true;
          call->error = header.error();
        } else {
          call->payload = std::move(payload);
        }
        call->done = true;
        owner.scheduler(call->waiter, call->worker);
      }
    }

    reader_running = false;
    if (conn && !conn->closed()) {
      conn->Close();
    }
    std::map<uint64_t, std::shared_ptr<PendingCall>> remaining;
    {
      std::lock_guard<std::mutex> lock(pending_mutex);
      remaining.swap(pending_calls);
    }
    for (auto& [id, call] : remaining) {
      if (!call->done) {
        call->failed = true;
        call->error = "RPC connection closed";
        call->done = true;
        owner.scheduler(call->waiter, call->worker);
      }
    }
  }

  IOManager& owner;
  std::string ip;
  uint16_t port;
  std::chrono::milliseconds timeout;
  const bool multiplex;
  std::mutex mutex;
  std::vector<std::unique_ptr<Slot>> slots;

  // Multiplexed channel members
  net::Connection::ptr conn;
  FiberMutex write_mutex;
  std::mutex pending_mutex;
  std::map<uint64_t, std::shared_ptr<PendingCall>> pending_calls;
  std::atomic<uint64_t> next_request_id{1};
  std::atomic<bool> reader_running{false};
  std::atomic<bool> closed{false};
};

Channel::Channel(IOManager& owner, std::string ipv4, uint16_t port,
                 size_t max_connections, std::chrono::milliseconds timeout,
                 bool multiplex)
    : state_(std::make_shared<State>(owner, std::move(ipv4), port, max_connections, timeout, multiplex)) {}

Channel::~Channel() {
  state_->closed = true;
  if (state_->conn) {
    state_->conn->Close();
  }
  for (auto& slot : state_->slots)
    if (slot->conn) slot->conn->Close();
}

void Channel::CallMethod(const google::protobuf::MethodDescriptor* method,
                         google::protobuf::RpcController* controller,
                         const google::protobuf::Message* request,
                         google::protobuf::Message* response,
                         google::protobuf::Closure* done) {
  auto state = state_;
  if (state->multiplex) {
    if (IOManager::GetThis() != &state->owner || !method || !request || !response) {
      Fail(controller, "Pulsar RPC must run in its IOManager Fiber");
      if (done) done->Run();
      return;
    }
    std::string args;
    if (!request->SerializeToString(&args) || args.size() > kMaxMessage) {
      Fail(controller, "RPC request serialization or size failure");
      if (done) done->Run();
      return;
    }

    uint64_t req_id = state->next_request_id.fetch_add(1);
    if (req_id == 0) req_id = state->next_request_id.fetch_add(1);

    RPC::RpcHeader header;
    header.set_service_name(method->service()->name());
    header.set_method_name(method->name());
    header.set_args_size(static_cast<uint32_t>(args.size()));
    header.set_request_id(req_id);

    std::string encoded;
    if (!header.SerializeToString(&encoded) || encoded.empty() ||
        encoded.size() > kMaxHeader ||
        args.size() > kMaxMessage - encoded.size() - 5) {
      Fail(controller, "RPC header serialization or size failure");
      if (done) done->Run();
      return;
    }
    std::string frame;
    frame.reserve(5 + encoded.size() + args.size());
    EncodeVarint32(static_cast<uint32_t>(encoded.size()), &frame);
    frame += encoded;
    frame += args;

    auto call = std::make_shared<State::PendingCall>();
    call->waiter = Fiber::GetThis();
    call->worker = GetThreadId();

    bool send_ok = false;
    state->write_mutex.lock();
    if (!state->conn || state->conn->closed()) {
      state->conn = net::Connection::Connect(state->owner, state->ip, state->port, state->timeout);
      if (!state->conn) {
        state->write_mutex.unlock();
        Fail(controller, "RPC connect failed: " + std::to_string(errno));
        if (done) done->Run();
        return;
      }
      if (!state->reader_running) {
        state->reader_running = true;
        auto s = state;
        state->owner.scheduler([s]() { s->ReaderLoop(); });
      }
    }

    {
      std::lock_guard<std::mutex> lock(state->pending_mutex);
      state->pending_calls[req_id] = call;
    }

    if (state->conn && !state->conn->closed()) {
      send_ok = state->conn->WriteAll(frame.data(), frame.size(), state->timeout);
    }
    state->write_mutex.unlock();

    if (!send_ok) {
      {
        std::lock_guard<std::mutex> lock(state->pending_mutex);
        state->pending_calls.erase(req_id);
      }
      Fail(controller, "RPC send failed: " + std::to_string(errno));
      if (done) done->Run();
      return;
    }

    Timer::ptr timer;
    if (state->timeout.count() > 0) {
      auto s = state;
      timer = state->owner.addTimer(static_cast<uint64_t>(state->timeout.count()), [s, req_id, call]() {
        bool need_wake = false;
        {
          std::lock_guard<std::mutex> lock(s->pending_mutex);
          auto it = s->pending_calls.find(req_id);
          if (it != s->pending_calls.end()) {
            s->pending_calls.erase(it);
            call->failed = true;
            call->error = "RPC request timeout";
            call->done = true;
            need_wake = true;
          }
        }
        if (need_wake) {
          s->owner.scheduler(call->waiter, call->worker);
        }
      });
    }

    call->waiter->yield();
    if (timer) timer->cancel();

    if (call->failed) {
      Fail(controller, call->error);
    } else if (!response->ParseFromArray(call->payload.data(), static_cast<int>(call->payload.size()))) {
      Fail(controller, "RPC response parse failed");
    }

    if (done) done->Run();
    return;
  }

  State::Slot* slot = nullptr;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    for (auto& candidate : state->slots) {
      if (!candidate->busy) {
        candidate->busy = true;
        slot = candidate.get();
        break;
      }
    }
  }
  if (!slot) {
    Fail(controller, "Pulsar RPC connection pool overloaded");
    if (done) done->Run();
    return;
  }
  const auto call = [&]() -> bool {
    if (IOManager::GetThis() != &state->owner || !method || !request || !response) {
      Fail(controller, "Pulsar RPC must run in its IOManager Fiber");
      return false;
    }
    std::string args;
    if (!request->SerializeToString(&args) || args.size() > kMaxMessage) {
      Fail(controller, "RPC request serialization or size failure");
      return false;
    }
    RPC::RpcHeader header;
    header.set_service_name(method->service()->name());
    header.set_method_name(method->name());
    header.set_args_size(static_cast<uint32_t>(args.size()));
    std::string encoded;
    if (!header.SerializeToString(&encoded) || encoded.empty() ||
        encoded.size() > kMaxHeader ||
        args.size() > kMaxMessage - encoded.size() - 5) {
      Fail(controller, "RPC header serialization or size failure");
      return false;
    }
    std::string frame;
    frame.reserve(5 + encoded.size() + args.size());
    EncodeVarint32(static_cast<uint32_t>(encoded.size()), &frame);
    frame += encoded;
    frame += args;
    if (!slot->conn || slot->conn->closed())
      slot->conn = net::Connection::Connect(state->owner, state->ip, state->port, state->timeout);
    if (!slot->conn) {
      Fail(controller, "RPC connect failed: " + std::to_string(errno));
      return false;
    }
    if (!slot->conn->WriteAll(frame.data(), frame.size(), state->timeout)) {
      Fail(controller, "RPC send failed; outcome unknown after partial send: " +
                           std::to_string(errno));
      return false;
    }
    uint32_t network_length = 0;
    if (!slot->conn->ReadExact(&network_length, sizeof(network_length), state->timeout)) {
      Fail(controller, "RPC response length read failed: " + std::to_string(errno));
      return false;
    }
    const uint32_t length = ntohl(network_length);
    if (length > kMaxMessage) {
      Fail(controller, "RPC response exceeds 64 MiB");
      return false;
    }
    std::string payload(length, '\0');
    if (length && !slot->conn->ReadExact(payload.data(), length, state->timeout)) {
      Fail(controller, "RPC response body read failed: " + std::to_string(errno));
      return false;
    }
    if (!response->ParseFromArray(payload.data(), static_cast<int>(length))) {
      Fail(controller, "RPC response parse failed");
      return false;
    }
    return true;
  };
  const bool success = call();
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (!success && slot->conn) {
      slot->conn->Close();
      slot->conn.reset();
    }
    slot->busy = false;
  }
  if (done) done->Run();
}

struct Provider::State : std::enable_shared_from_this<Provider::State> {
  struct ServiceInfo {
    google::protobuf::Service* service = nullptr;
    std::map<std::string, const google::protobuf::MethodDescriptor*> methods;
  };
  struct CallContext {
    std::weak_ptr<State> owner;
    std::unique_ptr<google::protobuf::Message> request;
    std::unique_ptr<google::protobuf::Message> response;
    Fiber::ptr waiter;
    int worker = -1;
    std::atomic<unsigned> outcome{0};  // 1 done, 2 timeout/stop
    void Finish(unsigned value) {
      unsigned expected = 0;
      if (!outcome.compare_exchange_strong(expected, value)) return;
      if (auto state = owner.lock()) state->io.scheduler(waiter, worker);
    }
  };
  class DoneClosure final : public google::protobuf::Closure {
   public:
    explicit DoneClosure(std::shared_ptr<CallContext> ctx) : ctx_(std::move(ctx)) {}
    void Run() override {
      if (auto state = ctx_->owner.lock()) {
        std::lock_guard<std::mutex> lock(state->mutex);
        --state->outstanding;
        state->drained.notify_all();
      }
      ctx_->Finish(1);
      delete this;
    }
   private:
    std::shared_ptr<CallContext> ctx_;
  };

  State(IOManager& owner, size_t threads, size_t queued, size_t connections,
        std::chrono::milliseconds timeout, size_t buffer_limit)
      : io(owner), executor(threads, queued), max_connections(connections),
        request_timeout(timeout), max_buffered_bytes(buffer_limit) {}
  struct Reservation {
    explicit Reservation(State& owner) : owner(owner) {}
    ~Reservation() { owner.buffered_bytes.fetch_sub(bytes); }
    bool Add(size_t amount) {
      size_t current = owner.buffered_bytes.load();
      do {
        if (amount > owner.max_buffered_bytes ||
            current > owner.max_buffered_bytes - amount) return false;
      } while (!owner.buffered_bytes.compare_exchange_weak(current, current + amount));
      bytes += amount;
      return true;
    }
    State& owner;
    size_t bytes = 0;
  };
  IOManager& io;
  Executor executor;
  const size_t max_connections;
  const std::chrono::milliseconds request_timeout;
  const size_t max_buffered_bytes;
  std::atomic<size_t> buffered_bytes{0};
  std::mutex mutex;
  std::condition_variable drained;
  std::map<std::string, ServiceInfo> services;
  std::unique_ptr<net::Server> server;
  std::vector<std::weak_ptr<CallContext>> pending;
  size_t pending_count = 0;
  size_t outstanding = 0;
  bool started = false;
  std::atomic<bool> accepting{false};

  void Handle(net::Connection::ptr conn) {
    auto write_mutex = std::make_shared<FiberMutex>();
    while (accepting && !conn->closed()) {
      uint32_t header_size = 0;
      if (!ReadVarint32(*conn, &header_size, request_timeout)) break;
      if (header_size == 0 || header_size > kMaxHeader) break;
      Reservation reservation(*this);
      if (!reservation.Add(header_size)) break;
      std::string header_data(header_size, '\0');
      if (!conn->ReadExact(header_data.data(), header_size, request_timeout)) break;
      RPC::RpcHeader header;
      if (!header.ParseFromArray(header_data.data(), static_cast<int>(header_size)) ||
          header.args_size() > kMaxMessage - header_size - 5) break;
      if (!reservation.Add(header.args_size())) break;
      std::string args(header.args_size(), '\0');
      if (header.args_size() &&
          !conn->ReadExact(args.data(), args.size(), request_timeout)) break;
      ServiceInfo service;
      const google::protobuf::MethodDescriptor* method = nullptr;
      {
        std::lock_guard<std::mutex> lock(mutex);
        const auto found = services.find(header.service_name());
        if (found != services.end()) {
          service = found->second;
          const auto entry = service.methods.find(header.method_name());
          if (entry != service.methods.end()) method = entry->second;
        }
      }
      if (!method) break;
      auto ctx = std::make_shared<CallContext>();
      ctx->owner = shared_from_this();
      ctx->request.reset(service.service->GetRequestPrototype(method).New());
      ctx->response.reset(service.service->GetResponsePrototype(method).New());
      if (!ctx->request->ParseFromArray(args.data(), static_cast<int>(args.size()))) break;

      const uint64_t req_id = header.request_id();

      if (req_id == 0) {
        ctx->waiter = Fiber::GetThis();
        ctx->worker = GetThreadId();
        {
          std::lock_guard<std::mutex> lock(mutex);
          if (!accepting || outstanding >= max_connections) break;
          pending.erase(
              std::remove_if(pending.begin(), pending.end(),
                             [](const std::weak_ptr<CallContext>& item) {
                               return item.expired();
                             }),
              pending.end());
          pending.push_back(ctx);
          ++pending_count;
          ++outstanding;
        }
        const bool queued = executor.Submit([ctx, service, method] {
          auto* done = new DoneClosure(ctx);
          try {
            service.service->CallMethod(method, nullptr, ctx->request.get(),
                                        ctx->response.get(), done);
          } catch (...) {
            // A throwing service violates the Protobuf completion contract.
            // The closure may have been retained, so leave its lifetime to Run().
            ctx->Finish(2);
          }
        });
        if (!queued) {
          {
            std::lock_guard<std::mutex> lock(mutex);
            --outstanding;
            drained.notify_all();
          }
          ctx->Finish(2);
        }
        Timer::ptr timer;
        if (request_timeout.count() >= 0)
          timer = io.addTimer(static_cast<uint64_t>(request_timeout.count()),
                              [ctx] { ctx->Finish(2); });
        ctx->waiter->yield();
        if (timer) timer->cancel();
        const bool completed = ctx->outcome == 1;
        {
          std::lock_guard<std::mutex> lock(mutex);
          --pending_count;
          drained.notify_all();
        }
        if (!completed || conn->closed() || !accepting) break;
        const size_t response_size = ctx->response->ByteSizeLong();
        if (response_size > kMaxMessage || !reservation.Add(response_size)) break;
        std::string payload;
        if (!ctx->response->SerializeToString(&payload) || payload.size() > kMaxMessage) break;
        const uint32_t length = htonl(static_cast<uint32_t>(payload.size()));
        write_mutex->lock();
        bool write_ok = conn->WriteAll(&length, sizeof(length), request_timeout) &&
                        conn->WriteAll(payload.data(), payload.size(), request_timeout);
        write_mutex->unlock();
        if (!write_ok) break;
      } else {
        // Multiplexed mode: async pipeline, Handle never yields for business logic!
        {
          std::lock_guard<std::mutex> lock(mutex);
          if (!accepting || outstanding >= max_connections * 64) break;
          ++outstanding;
          ++pending_count;
        }

        auto self = shared_from_this();
        class MultiplexDoneClosure final : public google::protobuf::Closure {
         public:
          MultiplexDoneClosure(std::shared_ptr<State> state,
                               net::Connection::ptr conn,
                               std::shared_ptr<FiberMutex> write_mutex,
                               std::shared_ptr<CallContext> ctx,
                               uint64_t req_id,
                               std::chrono::milliseconds timeout)
              : state_(std::move(state)), conn_(std::move(conn)),
                write_mutex_(std::move(write_mutex)), ctx_(std::move(ctx)),
                req_id_(req_id), timeout_(timeout) {}

          void Run() override {
            auto s = state_;
            auto c = conn_;
            auto wm = write_mutex_;
            auto call = ctx_;
            auto req_id = req_id_;
            auto to = timeout_;

            s->io.scheduler([s, c, wm, call, req_id, to]() {
              std::string payload;
              std::string error_msg;
              if (call->outcome.load() == 2) {
                error_msg = "RPC handler execution error or timeout";
              } else if (!call->response->SerializeToString(&payload) || payload.size() > kMaxMessage) {
                error_msg = "RPC response serialization failure";
              }

              RPC::RpcHeader resp_header;
              resp_header.set_request_id(req_id);
              if (!error_msg.empty()) {
                resp_header.set_error(error_msg);
              } else {
                resp_header.set_args_size(static_cast<uint32_t>(payload.size()));
              }

              std::string encoded;
              resp_header.SerializeToString(&encoded);
              std::string frame;
              frame.reserve(5 + encoded.size() + payload.size());
              EncodeVarint32(static_cast<uint32_t>(encoded.size()), &frame);
              frame += encoded;
              if (error_msg.empty()) {
                frame += payload;
              }

              wm->lock();
              if (c && !c->closed()) {
                c->WriteAll(frame.data(), frame.size(), to);
              }
              wm->unlock();

              {
                std::lock_guard<std::mutex> lock(s->mutex);
                --s->outstanding;
                --s->pending_count;
                s->drained.notify_all();
              }
            });

            delete this;
          }

         private:
          std::shared_ptr<State> state_;
          net::Connection::ptr conn_;
          std::shared_ptr<FiberMutex> write_mutex_;
          std::shared_ptr<CallContext> ctx_;
          uint64_t req_id_;
          std::chrono::milliseconds timeout_;
        };

        const bool queued = executor.Submit([ctx, service, method, self, conn, write_mutex, req_id, to = request_timeout] {
          auto* done = new MultiplexDoneClosure(self, conn, write_mutex, ctx, req_id, to);
          try {
            service.service->CallMethod(method, nullptr, ctx->request.get(),
                                        ctx->response.get(), done);
          } catch (...) {
            ctx->outcome.store(2);
            done->Run();
          }
        });

        if (!queued) {
          {
            std::lock_guard<std::mutex> lock(mutex);
            --outstanding;
            --pending_count;
            drained.notify_all();
          }
          break;
        }
      }
    }
  }
};

Provider::Provider(IOManager& owner, size_t handler_threads, size_t queued_handlers,
                   size_t max_connections, std::chrono::milliseconds request_timeout,
                   size_t max_buffered_bytes)
    : state_(std::make_shared<State>(owner, handler_threads, queued_handlers,
                                     max_connections, request_timeout,
                                     max_buffered_bytes)) {}
Provider::~Provider() { Stop(); }

bool Provider::Register(google::protobuf::Service* service) {
  if (!service) return false;
  auto state = state_;
  std::lock_guard<std::mutex> lock(state->mutex);
  if (state->started) return false;
  const auto* descriptor = service->GetDescriptor();
  if (!descriptor || state->services.count(descriptor->name())) return false;
  State::ServiceInfo info;
  info.service = service;
  for (int i = 0; i < descriptor->method_count(); ++i) {
    const auto* method = descriptor->method(i);
    info.methods.emplace(method->name(), method);
  }
  state->services.emplace(descriptor->name(), std::move(info));
  return true;
}

bool Provider::Start(const std::string& ipv4, uint16_t port) {
  auto state = state_;
  std::lock_guard<std::mutex> lock(state->mutex);
  if (state->started || state->services.empty()) return false;
  std::weak_ptr<State> weak = state;
  auto server = std::make_unique<net::Server>(
      state->io,
      [weak](net::Connection::ptr conn) {
        if (auto current = weak.lock()) current->Handle(std::move(conn));
      },
      state->max_connections);
  if (!server->Start(ipv4, port)) return false;
  state->server = std::move(server);
  state->accepting = true;
  state->started = true;
  return true;
}

bool Provider::Stop(std::chrono::milliseconds drain) {
  auto state = state_;
  if (!state) return true;
  std::unique_ptr<net::Server> server;
  std::vector<std::shared_ptr<State::CallContext>> pending;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->accepting = false;
    server = std::move(state->server);
    for (auto& item : state->pending)
      if (auto ctx = item.lock()) pending.push_back(std::move(ctx));
    state->pending.clear();
  }
  if (server) server->Stop();
  for (auto& ctx : pending) ctx->Finish(2);
  std::unique_lock<std::mutex> lock(state->mutex);
  return state->drained.wait_for(lock, drain, [state] {
    return state->pending_count == 0 && state->outstanding == 0;
  });
}

uint16_t Provider::port() const noexcept {
  auto state = state_;
  std::lock_guard<std::mutex> lock(state->mutex);
  return state->server ? state->server->port() : 0;
}
}  // namespace pulsar::rpc
