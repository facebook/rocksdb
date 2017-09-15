#include "NvmDevice.hpp"

#include <unistd.h>
#include <utility>

namespace facebook {
namespace cachelib {
namespace {
class FileDevice : public NvmDevice {
 public:
  explicit FileDevice(int fd): fd_{fd} {}
  FileDevice(const FileDevice&) = delete;
  FileDevice& operator=(const FileDevice&) = delete;

  ~FileDevice() override {
    if (fd_ != 0) {
      ::close(fd_);
    }
  }

  bool write(uint64_t offset, const void* buffer, uint32_t size) override {
    return seek(offset) && ::write(fd_, buffer, size) >= 0;
  }

  bool read(uint64_t offset, void* buffer, uint32_t size) override {
    return seek(offset) && ::read(fd_, buffer, size) >= 0;
  }

 private:
  bool seek(uint64_t offset) const {
    return ::lseek(fd_, base_ + offset, SEEK_SET) >= 0;
  }

  int fd_{};
  const uint64_t base_{};
};
}

std::unique_ptr<NvmDevice> createFileDevice(int fd) {
  return std::make_unique<FileDevice>(fd);
}
}
}
