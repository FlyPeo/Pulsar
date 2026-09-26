# Pulsar

Pulsar 是一个面向 Linux 的 C++17 有栈协程与模块化网络 I/O 库，
可选配 Protobuf RPC。
它使用 Boost.Context 的原生 `fcontext` 保存 Fiber 上下文，通过 M:N Scheduler
将协程调度到少量 pthread Worker，并结合 epoll 和定时器，把常见同步阻塞等待
转换为协程挂起与事件恢复。

> 本项目与 Apache Pulsar 消息系统无关。

核心运行时适合源码学习、运行时实验和本地性能测试；显式 TCP 与 RPC 模块
仍处于实验阶段。当前没有长期负载与跨发行版验证，不应直接作为生产级
网络/RPC 运行时使用。

| 模块 | CMake 目标 | 默认 | 当前范围 |
| --- | --- | --- | --- |
| Fiber、Scheduler、epoll/Timer、Hook | `Pulsar::pulsar` | 开启 | 已有核心接口 |
| 显式非阻塞 TCP | `Pulsar::net` | 关闭 | IPv4 TCP、限流、超时与关闭 |
| Protobuf RPC | `Pulsar::rpc` | 关闭 | 兼容 StrataKV RPC v1 的通用 Service/Channel |

依赖方向为 `Pulsar::rpc → Pulsar::net → Pulsar::pulsar`。这三个目标是功能分层，
不表示已经覆盖 PhotonLibOS 的文件 I/O、HTTP/TLS、io_uring 或全部 LibOS 能力。

## 实现贡献与来源

当前版本的主要开发工作包括：

- 将 Fiber 上下文迁移至 Boost.Context `fcontext`，完善生命周期与异常边界；
- 实现 per-worker deque、work stealing、Worker 亲和与回调 Fiber 复用；
- 增加协程栈分配与复用、同步原语，以及正确性测试和性能基准。

早期代码保留过 Sylar 协程项目的标识。准确的上游版本与许可证仍需核对；
在完成核对前，不将整个运行时表述为从零独立实现。

## 1. 核心能力

- `Fiber`：独立协程栈、生命周期状态以及 Resume/Yield 上下文切换；
- `Scheduler`：每 Worker 本地 deque、work stealing、Fiber/回调任务和指定 Worker；
- `IOManager`：统一管理 epoll READ/WRITE 事件和 `TimerManager`；
- Hook I/O：覆盖 sleep、connect、accept、read/write、recv/send 等常用调用；
- 同步原语：`FiberMutex`、`FiberConditionVariable`、`FiberSemaphore`、超时与取消；
- 测试与基准：同步正确性、上下文切换、生命周期、调度、定时器、Hook sleep、
  TCP echo 和同步压力。

```text
Application callback / synchronous-style I/O
                  |
                  v
       Fiber + cooperative Yield
                  |
                  v
       M:N Scheduler / pthread Workers
       local deque + work stealing
                  |
          +-------+--------+
          |                |
          v                v
   epoll IOManager     TimerManager
          |                |
          +--- resume waiting Fiber
```

## 2. 快速开始

### 2.1 环境要求

- Linux；
- 支持 C++17 的 GCC/Clang；
- CMake 3.16 或更高版本；
- 下表列出的系统依赖。

Pulsar 不依赖 Muduo、Protobuf 或 RocksDB；Fiber 上下文切换直接依赖
Boost.Context：

| 依赖 | CMake/系统名称 | 用途 |
| --- | --- | --- |
| C++ 标准库 | C++17 | 容器、智能指针、函数对象、原子变量和线程辅助类型 |
| Boost.Context | `find_package(Boost COMPONENTS context)`、`Boost::context` | Fiber 的原生 `fcontext` 创建与切换 |
| POSIX Threads | `find_package(Threads)`、`Threads::Threads` | Scheduler Worker、线程封装和同步基础设施 |
| Dynamic Loader | `${CMAKE_DL_LIBS}`，Linux 通常为 `libdl` | 通过 `dlsym` 获取被 Hook 系统调用的原始入口 |
| Linux libc/API | epoll、socket、timer、pipe、mmap/mprotect（可选） | I/O 多路复用、事件唤醒和 guard page |

构建需要 CMake 和 C++17 编译器；运行基准时，CPU 绑定命令 `taskset` 和环境
采集命令 `lscpu` 来自 `util-linux`，属于可选测试工具。

Ubuntu/WSL 可使用：

```bash
sudo apt update
sudo apt install -y build-essential cmake util-linux libboost-context-dev
```

项目依赖 Linux epoll、pthread、`dlsym` 和 Boost.Context，不支持 Windows 或
macOS。默认构建不会定义 `BOOST_USE_UCONTEXT`，公共头还会在该宏出现时直接
报错，避免生产构建静默回退。以下迁移环境已经实际用于构建和测试：Ubuntu
22.04/WSL2、GCC 11.4、Boost 1.74、Debug 与 Release。

### 2.2 获取、构建和测试

```bash
git clone https://github.com/FlyPeo/Pulsar.git
cd Pulsar

cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

构建后主要产物：

```text
build/libpulsar.a
build/pulsar-sync-check
build/pulsar-fiber-context-check
build/pulsar-scheduler-work-stealing-check
build/pulsar-stack-pool-check
build/pulsar-scheduler-cache-check
build/pulsar-reuse-integration-check
build/pulsar-benchmark
```

六个 `*-check` 都注册到 CTest：分别覆盖同步原语；Resume/Yield、reset 与异常边界；
本地队列窃取、Worker 亲和和 Callback Fiber 复用；栈池尺寸分级/容量/trim/统计；
回调对象缓存资格与命中；以及 I/O/Timer/WaitQueue 挂起下的复用集成。性能基准
不会自动加入 CTest，需要显式运行。

### 2.3 构建选项

| CMake 选项 | 默认值 | 作用 |
| --- | --- | --- |
| `PULSAR_BUILD_TESTS` | `ON` | 构建同步正确性测试 |
| `PULSAR_BUILD_BENCHMARKS` | `ON` | 构建性能与压力基准 |
| `PULSAR_BUILD_NET` | `OFF` | 构建并导出显式 TCP 目标 `Pulsar::net` |
| `PULSAR_BUILD_RPC` | `OFF` | 构建并导出 `Pulsar::rpc`，同时开启 net；需要 Protobuf |
| `PULSAR_FIBER_GUARD_PAGES` | `OFF` | 用 `mmap/mprotect` 在 Fiber 栈底加入 guard page |
| `BUILD_TESTING` | `ON` | 控制 CTest 测试目标 |

默认关闭 guard page 是为了保持迁移前后均使用 128 KiB、malloc/free 栈的公平
A/B。服务部署更重视栈溢出 fail-fast 时可显式打开；该配置已经通过两项 CTest。

只构建运行时库：

```bash
cmake -S . -B build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DPULSAR_BUILD_TESTS=OFF \
  -DPULSAR_BUILD_BENCHMARKS=OFF
cmake --build build-release -j"$(nproc)"
```

## 3. 使用示例

```cpp
#include <pulsar/pulsar.h>

int main() {
  pulsar::IOManager iom(1);
  iom.scheduler([] {
    // Scheduler Worker 会自动启用 Hook；sleep 不会阻塞整个 Worker。
    sleep(1);
  });
  // IOManager 析构时会等待已调度任务和事件完成。
  return 0;
}
```

Hook 只在 Pulsar Scheduler Worker 中自动启用。在普通线程中调用相同系统调用
仍保持原生阻塞语义。

### 3.2 协程栈分配器与对象池 (Stack Allocator & Object Cache)

Pulsar 提供分层的协程栈分配管理机制：
1. **`DirectStackAllocator`**：默认直通分配器，通过 `malloc/free`（或开启 Guard Page 时的 `mmap/munmap`）进行无池化即时分配与释放。
2. **`PooledStackAllocator`**：线程安全的高性能栈复用池，按 2 的幂次分级管理空闲栈。
   - **默认容量上限**：`maxCachedBytes = 64 MiB`（67,108,864 字节）。
   - **最大池化尺寸**：`maxPooledStackSize = 1 MiB`（1,048,576 字节），超过该阈值的超大栈直通底层释放。
   - **Guard Page 配合**：构建时开启 `-DPULSAR_FIBER_GUARD_PAGES=ON`，分配器使用 `mmap/mprotect` 在栈底设置不可访问保护页，池化释放时重设权限，驱逐与 trim 时通过 `munmap` 彻底归还操作系统。
   - **主动收缩 (Trim)**：支持 `allocator->trim(targetBytes)` 动态缩容；`trim(0)` 清空全部闲置缓存。
   - **监控与记账**：通过 `allocator->stats()` 获取完整命中、未命中、借出与缓存字节等细粒度统计。

#### Scheduler / IOManager 注入与每 Worker 多槽配置

```cpp
#include <pulsar/pulsar.h>
#include <cassert>
#include <iostream>

int main() {
  pulsar::StackPoolOptions poolOpts;
  poolOpts.maxCachedBytes = 64ULL * 1024 * 1024;    // 默认 64 MiB 上限
  poolOpts.maxPooledStackSize = 1ULL * 1024 * 1024; // 默认 1 MiB 最大池化尺寸
  auto allocator = pulsar::MakePooledStackAllocator(poolOpts);

  pulsar::SchedulerReuseOptions schedOpts;
  schedOpts.stackAllocator = allocator;
  schedOpts.callbackFiberCachePerWorker = 4; // 每 Worker 对象缓存容量（默认 1，可设为 N）

  {
    // use_caller=false 时构造函数已内部 start()，无需再显式调用；
    // IOManager 析构时会 join 全部 Worker 并清空回调 Fiber 缓存。
    pulsar::IOManager iom(1, false, "example-iom", schedOpts);
    for (int i = 0; i < 10; ++i) {
      iom.scheduler([] {
        // 短任务执行
      });
    }
    iom.stop();
  }

  auto stats = allocator->stats();
  assert(stats.acquireRequests >= 1);
  assert(stats.cachedBytes > 0);

  // 主动收缩释放内存
  allocator->trim(0);
  assert(allocator->stats().cachedBytes == 0);

  std::cout << "Pulsar stack pool example verified successfully!\n";
  return 0;
}
```

#### 对象复用资格与设计认知

- **对象复用资格 (Qualification)**：调度器内部对象缓存仅回收**由调度器自动创建、执行至 `TERM` 状态、执行上下文为空、完全脱离任何等待源且不存在外部引用（`use_count() == 1`）**的回调 Fiber。用户显式创建的 Fiber 或被外部容器持久引用的 Fiber 绝对不进入缓存。
- **休眠 Fiber 仍独占栈**：栈池的作用是复用已完工的栈资源以消除创建/销毁的系统调用。处于运行中、等待 I/O 或休眠挂起中的 Fiber 必须独占自身执行栈，栈池**不减少同时在用或休眠协程的常驻内存开销**。

### 3.3 作为子目录/安装目标使用

作为另一个 CMake 工程的子目录使用：

```cmake
add_subdirectory(third_party/Pulsar)
target_link_libraries(your_target PRIVATE Pulsar::pulsar)
```

也可以安装静态库和头文件：

```bash
cmake --install build --prefix "$PWD/install"
```

安装规则会生成 `PulsarConfig.cmake`、版本文件与 `PulsarTargets.cmake`；
下游可以用 `find_package(Pulsar CONFIG REQUIRED)`，配置文件会自动查找
Threads 与 Boost.Context；安装 RPC 目标时还会查找 Protobuf。

### 3.4 可选 TCP 与 RPC

```bash
cmake -S . -B build-net -DCMAKE_BUILD_TYPE=Release -DPULSAR_BUILD_NET=ON
cmake --build build-net -j
./build-net/pulsar-tcp-echo server 42571
# 另一个终端：
./build-net/pulsar-tcp-echo client 42571

cmake -S . -B build-rpc -DCMAKE_BUILD_TYPE=Release -DPULSAR_BUILD_RPC=ON
cmake --build build-rpc -j
ctest --test-dir build-rpc --output-on-failure
cmake --install build-rpc --prefix "$PWD/install"
```

下游链接 `Pulsar::net` 或 `Pulsar::rpc` 时会自动获得下层目标。完整的
`add_subdirectory`/`find_package` 消费者位于 `examples/downstream`；
`examples/rpc_echo.cpp` 展示通用 Protobuf Service、Channel 和独立处理线程池。
将 `examples/rpc_external/CMakeLists.txt`、`rpc_echo.cpp` 与
`rpc_echo.proto` 一起复制到仓库外，再以 `CMAKE_PREFIX_PATH` 指向安装目录，
可验证独立消费。新模块没有引入 StrataKV 业务消息、Muduo 或公共工具代码。

显式 TCP 的 `Connect`、`ReadSome`、`ReadExact` 和 `WriteAll` 必须在所属
`IOManager` 的 Fiber 上调用；目前地址使用数字 IPv4。连接对象拥有 fd，同一连接
最多一位读取者和一位写入者；读写期限、关闭、连接上限及非读取对端的写入超时
都有明确结果。调用方须让 `IOManager` 存活到所有连接和服务对象销毁后。

RPC v1 请求为 `[varint32 header_size][RpcHeader][args]`，响应为
`[network-order uint32 length][serialized response]`；头最多 64 KiB，消息最多
64 MiB。每连接只允许一个在途请求，通道使用有界连接池；连接池或处理队列满时
明确失败。v1 没有请求 ID、错误帧或自动重试：发送后断线时执行结果未知，
幂等判断、leader 切换及业务重试由应用处理。同步 Service 在有界 pthread
执行器上运行，不占用 I/O Worker；异步 Service 必须最终且仅一次调用 `done`，
并在所有回调结束前保持 Service 存活。超时或关闭后晚到的 `done` 不会向旧连接
发送响应。Provider 默认最多 1024 个连接、128 个排队处理任务和 128 MiB
在途线协议缓冲；构造参数可以收紧这些上限。若停止期限内仍有异步回调，
`Stop` 返回 `false`，调用方应保留 Service 并在回调结束后再次停止。

## 4. 测试与基准

### 4.1 正确性测试

```bash
ctest --test-dir build --output-on-failure
# 或直接运行
./build/pulsar-sync-check
```

自动测试覆盖 Fiber Resume/Yield、结束后 reset、异常隔离、调度线程继续运行，
本地队列 work stealing、指定 Worker 亲和、Callback Fiber 复用，以及 Fiber
Mutex 的竞争、超时、取消和 Semaphore 唤醒。基准程序还会在各场景检查完成
计数、校验和、超时以及 TCP echo 数据一致性。

### 4.2 基准场景

查看完整参数：

```bash
./build/pulsar-benchmark --help
```

| 场景 | 命令示例 | 测量内容 |
| --- | --- | --- |
| 上下文切换 | `--case context --iterations 5000000 --cpu 0` | Resume/Yield transfer |
| 生命周期 | `--case lifecycle --count 10000 --cpu 0 --mode MODE` | 创建、首次运行、销毁和内存 |
| 调度 | `--case scheduler --count 100000 --threads 1` | Callback 调度吞吐 |
| 定时器 | `--case timer --count 10000 --delay-ms 50 --cpu 0` | 插入成本和到期延迟 |
| Hook sleep | `--case hook-sleep --count 10000 --delay-ms 10 --cpu 0` | 定时挂起与恢复 |
| Hook TCP echo | `--case hook-echo --count 1000 --round-trips 10` | Loopback socket Hook |
| 同步压力 | `--case sync --count 1000 --threads 4` | Semaphore 与 Mutex |
| 波次突发 | `--case wave-burst --rounds 5 --pool-mib 64` | 栈池冷热命中、驱逐与容量上限 |
| 休眠足迹 | `--case sleep-footprint --count 10000 --pool-mib 64 --mode pool-single` | VmSize/RSS 与 checked-out/cached 字节记账 |
| 回调 A/B | `--case callback-ab --mode pool-multi --rounds 5 --count 50000` | 单槽/多槽命中与外部别名拒绝 |

生命周期、波次突发、休眠足迹和回调 A/B 支持 `--mode
direct-single|pool-single|pool-multi`（默认 `direct-single`，即 Direct 分配器 +
每 Worker 单槽回调缓存的基线）；`--pool-mib` 设置栈池空闲上限（默认 64 MiB），
`--fiber-cache-per-worker` 设置回调对象缓存容量。示例：固定 CPU 运行上下文切换基准：

```bash
./build/pulsar-benchmark \
  --case context \
  --iterations 5000000 \
  --cpu 0
```

如果当前容器或 CI 不允许绑定 CPU，请省略 `--cpu`。多 Worker 测试可使用
`taskset` 限制 CPU 集合，例如：

```bash
taskset -c 0-3 ./build/pulsar-benchmark \
  --case scheduler \
  --count 100000 \
  --threads 4
```

### 4.3 参考性能基线

以下结果来自 2026-08-29 的同机测试：WSL2、AMD Ryzen 7 9700X（环境可见
4 个逻辑 CPU）、Ubuntu 22.04、GCC 11.4、Boost 1.74、Release、默认 128 KiB
栈。每项预热后运行 5 轮，报告中位数：

| 场景 | 负载 | 中位数 |
| --- | --- | ---: |
| Fiber 上下文切换 | 每轮 5,000,000 次 Yield | 28.543 ns/transfer |
| 单 Worker callback 调度 | 100,000 task | 5.589 M task/s |
| 四 Worker callback 调度 | 100,000 task | 16.985 M task/s，3.04× 单 Worker |
| Loopback Hook echo | 1,000 连接 × 10 次 × 64 B | 56.072 K request/s，0 失败 |

复现对应负载：

```bash
./build/pulsar-benchmark --case context --iterations 5000000 --cpu 0
taskset -c 0 ./build/pulsar-benchmark \
  --case scheduler --count 100000 --threads 1
taskset -c 0-1 ./build/pulsar-benchmark \
  --case hook-echo --count 1000 --threads 1 \
  --payload-bytes 64 --round-trips 10
```

这些数字只代表指定机器和负载，不用于与 Photon、libco 或 libgo 做跨机器绝对
性能排名。上下文指标包含 Pulsar 状态检查、句柄移动和完整 Resume/Yield 封装，
并非裸 `jump_fcontext` 指令成本。Boost.Context 迁移数据见 StrataKV 文档
`docs/性能报告/2026-08-28-Boost.Context迁移.md`；本地队列/work stealing 的严格
交错 A/B、perf/futex 数据与已知限制见
`docs/性能报告/2026-08-29-Pulsar-Work-Stealing调度器.md`。

### 4.4 模块化变更验证

2026-09-26 在同一 WSL/Release 环境中，开启 RPC 目标后 Pulsar CTest 通过
10/10；包含独立 TCP 故障测试、通用 RPC 阻塞处理测试和分进程的新旧 RPC
双向互通。未开启新目标的 `libpulsar.a` 和 `pulsar-benchmark` 与变更前
构建产物逐字节相同。StrataKV 默认链接仍使用旧 `stratakv_rpc`；其 59 项
CTest 前后均为 58 项通过，同一个 Auto-Balancer 检查失败。全量构建中原有
`client_main.cpp` 编译错误仍在。五轮同机性能测试有几百分点的自然波动，
因此不宣称绝对零开销或生产级性能保证；原始 A/B 数据和复跑脚本保存在
关联 StrataKV OpenSpec 变更的 `evidence/` 目录。

上述 58/59 与编译错误是模块化变更时的历史基线。随后修复客户端源码和
Auto-Balancer 测试约定后，StrataKV Release 全量构建成功，CTest 通过 59/59。

## 5. 项目结构

```text
Pulsar/
├── CMakeLists.txt
├── include/pulsar/       # 公共头文件
├── src/                  # Fiber、Scheduler、IOManager、Hook 和同步实现
├── proto/                # 可选 RPC v1 元数据协议
├── examples/             # TCP、通用 RPC 与独立下游示例
├── tests/                # CTest 正确性测试
└── benchmarks/           # 性能与压力基准
```

`build*/`、`bin/`、`lib/`、`test-results/`、对象文件和性能采样文件均为可再
生成产物，不应提交到 Git。

## 6. 当前边界

- 默认上下文后端是 Boost.Context/fcontext；当前只验证了 Linux x86_64；
- 调度采用协作式模型，CPU 密集任务若不主动 Yield 会占用所在 Worker；
- 默认 Fiber 使用 128 KiB 固定独立栈，大量常驻/挂起协程会消耗较多虚拟地址
  空间；可选注入 `PooledStackAllocator` 复用已销毁 Fiber 的空闲栈（见 3.2），
  但它不减少同时在用或休眠协程的栈；
- guard page 是可选项，默认关闭；
- work stealing 使用带 mutex 的每 Worker deque，并线性扫描 victim；它不是无锁
  队列，也不迁移指定线程或正在运行的 Fiber；
- 尚未实现共享栈、io_uring 和常规文件 I/O Hook；
- GCC ASan 与发行版预编译的 fcontext 在重复切换压力下仍会不稳定；普通与 guard
  page 测试通过，但不能把当前 ASan 结果当作完整 Fiber 栈覆盖；
- 尚未完成长期 soak 和跨发行版兼容性验证；
- 可选网络/RPC 目前只支持数字 IPv4 TCP，不含 DNS、TLS、HTTP 和多路复用；
- RPC v1 的部分发送失败无法判断服务端是否执行，不能在传输层安全重试；
- API 和 ABI 仍可能变化，不承诺稳定兼容。

## 7. 许可

本仓库当前未附带开源许可证；在添加明确许可证前，不默认授予复制、修改或
再分发权利。早期代码标识及待核查事项记录在 `docs/source-and-license.md`；
在核实来源、对应版本和许可义务之前，不发布再分发包。
