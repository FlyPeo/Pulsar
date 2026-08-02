# Pulsar

Pulsar 是一个面向 Linux 的 C++17 用户态有栈协程与 Hook I/O 框架。它以 `ucontext` 管理 Fiber 上下文，通过 M:N Scheduler 将协程调度到少量 pthread 工作线程，并结合 epoll、定时器与系统调用 Hook，把同步阻塞等待转换为协程挂起和事件恢复。

## 核心能力

- `Fiber`：独立协程栈、生命周期状态及 Resume/Yield 上下文切换；
- `Scheduler`：M:N 任务调度，支持 Fiber、回调任务和指定 Worker；
- `IOManager`：统一管理 epoll READ/WRITE 事件和 TimerManager；
- Hook I/O：覆盖 sleep、connect、accept、read/write、recv/send 等常用阻塞调用；
- 同步原语：FiberMutex、FiberConditionVariable、FiberSemaphore、超时与取消；
- 正确性与性能测试：上下文切换、生命周期、调度、定时器、Hook sleep、TCP echo 和同步压力。

## 架构

```text
Application callback / synchronous-style I/O
                  │
                  ▼
       Fiber + cooperative Yield
                  │
                  ▼
       M:N Scheduler / pthread workers
                  │
          ┌───────┴────────┐
          ▼                ▼
  epoll IOManager      TimerManager
          │                │
          └──── resume waiting Fiber
```

## 构建与测试

依赖 Linux、支持 C++17 的编译器、CMake 3.16+ 和 pthread/dl：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

运行轻量上下文切换基准：

```bash
./build/pulsar-benchmark \
  --case context \
  --iterations 5000000 \
  --cpu 0
```

其他场景可通过 `--case lifecycle|scheduler|timer|hook-sleep|hook-echo|sync` 执行。

## 使用示例

```cpp
#include <pulsar/pulsar.h>

int main() {
  pulsar::IOManager iom(1);
  iom.scheduler([] {
    // Hook 开启后，sleep/socket 等待会挂起当前 Fiber，
    // Worker 可以继续执行其他就绪任务。
    sleep(1);
  });
  return 0;
}
```

作为 CMake 子目录使用：

```cmake
add_subdirectory(third_party/Pulsar)
target_link_libraries(your_target PRIVATE Pulsar::pulsar)
```

## 本机实测结果

在 WSL2、Intel Core Ultra 7 155H、GCC 13.3、Release 构建下，固定 CPU 多轮测试的中位数为：

| 场景 | 结果 |
| --- | ---: |
| Fiber 上下文切换 | 173 ns/transfer |
| 单 Worker callback 调度 | 1.34 M task/s |
| 1000 连接 × 10 次、64 B loopback echo | 47.2 K request/s，0 失败 |

这些数字只代表本机同环境结果，不用于与 Boost.Context、Photon、libco 或 libgo 做跨机器绝对性能排名。

## 当前边界

- 依赖 Linux epoll、pthread、`ucontext` 和 `dlsym`，不是跨平台运行时；
- 采用协作式调度，CPU 密集任务若不主动 Yield 会占用所在 Worker；
- 默认 Fiber 使用 128 KiB 固定独立栈，10 万级常驻协程需要进一步引入栈池、按需提交或共享栈；
- 尚未覆盖 io_uring、work stealing、文件 I/O Hook、长期 soak 和 Sanitizer 全矩阵。

本仓库当前未附带开源许可证，未经许可不授予复制、修改或再分发权利。
