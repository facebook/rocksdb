#pragma once

#include <cstdint>
#include <memory>

namespace facebook {
namespace cachelib {
class NvmDevice {
 public:
  virtual ~NvmDevice() = default;
  virtual bool write(uint64_t offset, const void* buffer, uint32_t size) = 0;
  virtual bool read(uint64_t offset, void* buffer, uint32_t size) = 0;
};

// Takes ownership of the file descriptor
std::unique_ptr<NvmDevice> createFileDevice(int fd);
}
}
