//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>

#include "rocksdb/memory_allocator.h"

namespace ROCKSDB_NAMESPACE {
// A memory allocator using new/delete
class DefaultMemoryAllocator : public MemoryAllocator {
 public:
  static const char* kClassName() { return "DefaultMemoryAllocator"; }
  const char* Name() const override { return kClassName(); }
  void* Allocate(size_t size) override {
    return static_cast<void*>(new char[size]);
  }

  void Deallocate(void* p) override { delete[] static_cast<char*>(p); }
};

// Base class for a MemoryAllocator.  This implementation does nothing
// and asserts if the methods are invoked.  Implementations can extend
// this class and override these methods when they are enabled via
// compiler switches (e.g., the JeMallocMemoryAllocator can define these methods
// if ROCKSDB_JEMALLOC is defined at compile time.
class BaseMemoryAllocator : public MemoryAllocator {
 public:
  void* Allocate(size_t /*size*/) override {
    assert(false);
    return nullptr;
  }

  void Deallocate(void* /*p*/) override { assert(false); }
};

// A memory allocator that counts the number of allocations and deallocations
class CountedMemoryAllocator : public MemoryAllocatorWrapper {
 public:
  CountedMemoryAllocator()
      : MemoryAllocatorWrapper(std::make_shared<DefaultMemoryAllocator>()),
        allocations_(0),
        deallocations_(0) {}

  explicit CountedMemoryAllocator(const std::shared_ptr<MemoryAllocator>& t)
      : MemoryAllocatorWrapper(t), allocations_(0), deallocations_(0) {}
  static const char* kClassName() { return "CountedMemoryAllocator"; }
  const char* Name() const override { return kClassName(); }
  std::string GetId() const override { return std::string(Name()); }
  void* Allocate(size_t size) override {
    allocations_++;
    return MemoryAllocatorWrapper::Allocate(size);
  }

  void Deallocate(void* p) override {
    deallocations_++;
    MemoryAllocatorWrapper::Deallocate(p);
  }
  uint64_t GetNumAllocations() const { return allocations_; }
  uint64_t GetNumDeallocations() const { return deallocations_; }

 private:
  std::atomic<uint64_t> allocations_;
  std::atomic<uint64_t> deallocations_;
};
}  // namespace ROCKSDB_NAMESPACE
