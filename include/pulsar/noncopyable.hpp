#ifndef __SYLAR_NONCOPYABLE_H__
#define __SYLAR_NONCOPYABLE_H__

namespace pulsar {
class Nonecopyable {
 public:
  Nonecopyable() = default;
  ~Nonecopyable() = default;
  Nonecopyable(const Nonecopyable &) = delete;
  Nonecopyable operator=(const Nonecopyable) = delete;
};
}  // namespace pulsar

#endif