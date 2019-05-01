#pragma once

#include "rocksdb/env.h"
#include "util/fault_injection_test_env.h"

#include <memory>

namespace rocksdb {
namespace titandb {

class TitanFaultInjectionTestEnv;

class TitanTestRandomAccessFile : public RandomAccessFile {
 public:
  explicit TitanTestRandomAccessFile(std::unique_ptr<RandomAccessFile>&& f,
                                     TitanFaultInjectionTestEnv* env)
      : target_(std::move(f)),
        env_(env) {
    assert(target_ != nullptr);
  }
  virtual ~TitanTestRandomAccessFile() { }
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override;
  Status Prefetch(uint64_t offset, size_t n) override;
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }
  void Hint(AccessPattern pattern) override {
    return target_->Hint(pattern);
  }
  bool use_direct_io() const override {
    return target_->use_direct_io();
  }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status InvalidateCache(size_t offset, size_t length) override;
 private:
  std::unique_ptr<RandomAccessFile> target_;
  TitanFaultInjectionTestEnv* env_;
};

class TitanFaultInjectionTestEnv : public FaultInjectionTestEnv {
 public:
  TitanFaultInjectionTestEnv(Env* t)
      : FaultInjectionTestEnv(t) { }
  virtual ~TitanFaultInjectionTestEnv() { }
  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& soptions) {
    if (!IsFilesystemActive()) {
      return GetError();
    }
    Status s = target()->NewRandomAccessFile(fname, result, soptions);
    if (s.ok()) {
      result->reset(new TitanTestRandomAccessFile(std::move(*result), this));
    }
    return s;
  }
};

Status TitanTestRandomAccessFile::Read(uint64_t offset, size_t n, 
                                       Slice* result, char* scratch) const {
  if(!env_->IsFilesystemActive()) {
    return env_->GetError();
  }
  return target_->Read(offset, n, result, scratch);
}

Status TitanTestRandomAccessFile::Prefetch(uint64_t offset, size_t n) {
  if(!env_->IsFilesystemActive()) {
    return env_->GetError();
  }
  return target_->Prefetch(offset, n);
}

Status TitanTestRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  if(!env_->IsFilesystemActive()) {
    return env_->GetError();
  }
  return target_->InvalidateCache(offset, length);
}

} // namespace titandb
} // namespace rocksdb