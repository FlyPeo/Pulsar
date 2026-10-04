#ifndef PULSAR_FILE_HPP
#define PULSAR_FILE_HPP

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <string>

#include "iomanager.hpp"

namespace pulsar {
namespace file {

inline int open(const char* pathname, int flags, mode_t mode = 0644) {
  std::string path(pathname ? pathname : "");
  return IOManager::AsyncBlocking([path, flags, mode]() {
    return ::open(path.c_str(), flags, mode);
  });
}

inline int close(int fd) {
  return IOManager::AsyncBlocking([fd]() {
    return ::close(fd);
  });
}

inline ssize_t read(int fd, void* buf, size_t count) {
  return IOManager::AsyncBlocking([fd, buf, count]() {
    return ::read(fd, buf, count);
  });
}

inline ssize_t write(int fd, const void* buf, size_t count) {
  return IOManager::AsyncBlocking([fd, buf, count]() {
    return ::write(fd, buf, count);
  });
}

inline ssize_t pread(int fd, void* buf, size_t count, off_t offset) {
  return IOManager::AsyncBlocking([fd, buf, count, offset]() {
    return ::pread(fd, buf, count, offset);
  });
}

inline ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset) {
  return IOManager::AsyncBlocking([fd, buf, count, offset]() {
    return ::pwrite(fd, buf, count, offset);
  });
}

inline off_t lseek(int fd, off_t offset, int whence) {
  return IOManager::AsyncBlocking([fd, offset, whence]() {
    return ::lseek(fd, offset, whence);
  });
}

inline int fsync(int fd) {
  return IOManager::AsyncBlocking([fd]() {
    return ::fsync(fd);
  });
}

inline int fdatasync(int fd) {
  return IOManager::AsyncBlocking([fd]() {
    return ::fdatasync(fd);
  });
}

class File {
 public:
  File() = default;
  ~File() { close(); }
  File(File&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
  File& operator=(File&& other) noexcept {
    if (this != &other) {
      close();
      fd_ = other.fd_;
      other.fd_ = -1;
    }
    return *this;
  }
  File(const File&) = delete;
  File& operator=(const File&) = delete;

  static File Open(const std::string& path, int flags, mode_t mode = 0644) {
    int fd = pulsar::file::open(path.c_str(), flags, mode);
    return File(fd);
  }

  bool isOpen() const noexcept { return fd_ >= 0; }
  int getFd() const noexcept { return fd_; }

  ssize_t read(void* buf, size_t count) { return pulsar::file::read(fd_, buf, count); }
  ssize_t write(const void* buf, size_t count) { return pulsar::file::write(fd_, buf, count); }
  ssize_t pread(void* buf, size_t count, off_t offset) { return pulsar::file::pread(fd_, buf, count, offset); }
  ssize_t pwrite(const void* buf, size_t count, off_t offset) { return pulsar::file::pwrite(fd_, buf, count, offset); }
  off_t lseek(off_t offset, int whence) { return pulsar::file::lseek(fd_, offset, whence); }
  int fsync() { return pulsar::file::fsync(fd_); }
  int fdatasync() { return pulsar::file::fdatasync(fd_); }
  void close() {
    if (fd_ >= 0) {
      pulsar::file::close(fd_);
      fd_ = -1;
    }
  }

 private:
  explicit File(int fd) : fd_(fd) {}
  int fd_ = -1;
};

}  // namespace file
}  // namespace pulsar

#endif  // PULSAR_FILE_HPP
