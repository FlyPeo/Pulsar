# Pulsar

Pulsar 是一个面向 Linux 的 C++17 用户态有栈协程与 Hook I/O 运行时。
它使用 Boost.Context 的原生 `fcontext` 保存 Fiber 上下文，通过 M:N Scheduler
将协程调度到少量 pthread Worker，并结合 epoll 和定时器，把常见同步阻塞等待
转换为协程挂起与事件恢复。

> 本项目与 Apache Pulsar 消息系统无关。

当前版本适合源码学习、运行时实验和本地性能测试，不应直接作为生产级异步
运行时使用。

## 来源说明

Pulsar 的早期代码保留过来自 Sylar 协程项目的标识，当前仓库在此基础上继续
进行了 Boost.Context 迁移、Fiber 生命周期处理、per-worker deque/work
stealing、同步原语和测试基准等改造。准确的上游版本与许可证仍需在再次分发
衍生代码前完成核对，因此不应把整个运行时描述为从空目录独立实现。

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
build/pulsar-benchmark
```

三个 `*-check` 都注册到 CTest：分别覆盖同步原语，Resume/Yield、reset 与异常边界，
以及本地队列窃取、Worker 亲和和 Callback Fiber 复用；性能基准不会自动加入
CTest，需要显式运行。

### 2.3 构建选项

| CMake 选项 | 默认值 | 作用 |
| --- | --- | --- |
| `PULSAR_BUILD_TESTS` | `ON` | 构建同步正确性测试 |
| `PULSAR_BUILD_BENCHMARKS` | `ON` | 构建性能与压力基准 |
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
Threads 与 Boost.Context。

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
| 生命周期 | `--case lifecycle --count 10000 --cpu 0` | 创建、首次运行、销毁和内存 |
| 调度 | `--case scheduler --count 100000 --threads 1` | Callback 调度吞吐 |
| 定时器 | `--case timer --count 10000 --delay-ms 50 --cpu 0` | 插入成本和到期延迟 |
| Hook sleep | `--case hook-sleep --count 10000 --delay-ms 10 --cpu 0` | 定时挂起与恢复 |
| Hook TCP echo | `--case hook-echo --count 1000 --round-trips 10` | Loopback socket Hook |
| 同步压力 | `--case sync --count 1000 --threads 4` | Semaphore 与 Mutex |

示例：固定 CPU 运行上下文切换基准：

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

## 5. 项目结构

```text
Pulsar/
├── CMakeLists.txt
├── include/pulsar/       # 公共头文件
├── src/                  # Fiber、Scheduler、IOManager、Hook 和同步实现
├── tests/                # CTest 正确性测试
└── benchmarks/           # 性能与压力基准
```

`build*/`、`bin/`、`lib/`、`test-results/`、对象文件和性能采样文件均为可再
生成产物，不应提交到 Git。

## 6. 当前边界

- 默认上下文后端是 Boost.Context/fcontext；当前只验证了 Linux x86_64；
- 调度采用协作式模型，CPU 密集任务若不主动 Yield 会占用所在 Worker；
- 默认 Fiber 使用 128 KiB 固定独立栈，大量常驻协程会消耗较多虚拟地址空间；
- guard page 是可选项，默认关闭；
- work stealing 使用带 mutex 的每 Worker deque，并线性扫描 victim；它不是无锁
  队列，也不迁移指定线程或正在运行的 Fiber；
- 尚未实现共享栈、io_uring 和常规文件 I/O Hook；
- GCC ASan 与发行版预编译的 fcontext 在重复切换压力下仍会不稳定；普通与 guard
  page 测试通过，但不能把当前 ASan 结果当作完整 Fiber 栈覆盖；
- 尚未完成长期 soak 和跨发行版兼容性验证；
- API 和 ABI 仍可能变化，不承诺稳定兼容。

## 7. 许可

本仓库当前未附带开源许可证；在添加明确许可证前，不默认授予复制、修改或
再分发权利。
