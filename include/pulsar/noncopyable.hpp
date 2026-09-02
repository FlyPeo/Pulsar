#ifndef PULSAR_NONCOPYABLE_HPP
#define PULSAR_NONCOPYABLE_HPP

namespace pulsar {
class Nonecopyable {
 public:
  Nonecopyable() = default;
  ~Nonecopyable() = default;
  Nonecopyable(const Nonecopyable &) = delete;
  Nonecopyable operator=(const Nonecopyable) = delete;
};
}  // namespace pulsar

#endif  // PULSAR_NONCOPYABLE_HPP
