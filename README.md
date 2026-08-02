# Pulsar

Pulsar 是一个面向 Linux 的 C++17 用户态有栈协程与 Hook I/O 运行时。
它使用 `ucontext` 保存 Fiber 上下文，通过 M:N Scheduler 将协程调度到少量
pthread Worker，并结合 epoll 和定时器，把常见同步阻塞等待转换为协程挂起与
事件恢复。

> 本项目与 Apache Pulsar 消息系统无关。

当前版本适合源码学习、运行时实验和本地性能测试，不应直接作为生产级异步
运行时使用。

## 1. 核心能力

- `Fiber`：独立协程栈、生命周期状态以及 Resume/Yield 上下文切换；
- `Scheduler`：M:N 任务调度，支持 Fiber、回调任务和指定 Worker；
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
- pthread 和 `dl`。

项目依赖 Linux epoll、`ucontext`、pthread 和 `dlsym`，不支持 Windows 或
macOS。以下环境已经实际用于构建和测试：WSL2、GCC 13.3、Release 构建。

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
build/pulsar-benchmark
```

`pulsar-sync-check` 是当前注册到 CTest 的自动正确性测试；性能基准不会自动加入
CTest，需要显式运行。

### 2.3 构建选项

| CMake 选项 | 默认值 | 作用 |
| --- | --- | --- |
| `PULSAR_BUILD_TESTS` | `ON` | 构建同步正确性测试 |
| `PULSAR_BUILD_BENCHMARKS` | `ON` | 构建性能与压力基准 |
| `BUILD_TESTING` | `ON` | 控制 CTest 测试目标 |

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

当前安装规则会生成 `PulsarTargets.cmake`，但尚未提供完整的
`PulsarConfig.cmake` 包装文件；因此推荐优先使用 `add_subdirectory`。

## 4. 测试与基准

### 4.1 正确性测试

```bash
ctest --test-dir build --output-on-failure
# 或直接运行
./build/pulsar-sync-check
```

当前自动测试覆盖 Fiber Mutex 的竞争、超时、取消和 Semaphore 唤醒。基准程序
还会在各场景中检查完成计数、校验和、超时以及 TCP echo 数据一致性。

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

以下结果来自 2026-08-01 的 WSL2、Intel Core Ultra 7 155H、GCC 13.3、
Release 构建。固定 CPU、多轮运行后报告中位数：

| 场景 | 负载 | 中位数 |
| --- | --- | ---: |
| Fiber 上下文切换 | 每轮 5,000,000 次 Yield | 173 ns/transfer |
| 单 Worker callback 调度 | 100,000 task | 1.34 M task/s |
| Loopback Hook echo | 1,000 连接 × 10 次 × 64 B | 47.2 K request/s，0 失败 |

复现对应负载：

```bash
./build/pulsar-benchmark --case context --iterations 5000000 --cpu 0
taskset -c 0 ./build/pulsar-benchmark \
  --case scheduler --count 100000 --threads 1
taskset -c 0-1 ./build/pulsar-benchmark \
  --case hook-echo --count 1000 --threads 1 \
  --payload-bytes 64 --round-trips 10
```

这些数字只代表指定机器和负载，不用于与 Boost.Context、Photon、libco 或 libgo
做跨机器绝对性能排名。上下文指标包含 Pulsar 状态检查、智能指针和
`swapcontext` 完整封装路径，并非裸汇编切换成本。

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

- `ucontext` 已退出 POSIX 标准，但在当前目标 Linux/glibc 环境中可用；
- 调度采用协作式模型，CPU 密集任务若不主动 Yield 会占用所在 Worker；
- 默认 Fiber 使用 128 KiB 固定独立栈，大量常驻协程会消耗较多虚拟地址空间；
- 尚未实现 work stealing、共享栈、io_uring 和常规文件 I/O Hook；
- 尚未完成长期 soak、完整 Sanitizer 矩阵和跨发行版兼容性验证；
- API 和 ABI 仍可能变化，不承诺稳定兼容。

## 7. 许可

本仓库当前未附带开源许可证；在添加明确许可证前，不默认授予复制、修改或
再分发权利。
