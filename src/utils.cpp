#include "utils.hpp"

namespace pulsar {
pid_t GetThreadId() { return syscall(SYS_gettid); }
}  // namespace pulsar
