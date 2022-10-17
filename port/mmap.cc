//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "port/mmap.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <new>
#include <utility>

#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

MemMapping::~MemMapping() {
#ifdef OS_WIN
  if (addr_ != nullptr) {
    (void)::UnmapViewOfFile(addr_);
  }
  if (page_file_handle_ != NULL) {
    (void)::CloseHandle(page_file_handle_);
  }
#else   // OS_WIN -> !OS_WIN
  if (addr_ != nullptr) {
    auto status = munmap(addr_, length_);
    assert(status == 0);
    if (status != 0) {
      // TODO: handle error?
    }
  }
#endif  // OS_WIN
}

MemMapping::MemMapping(MemMapping&& other) noexcept {
  *this = std::move(other);
}

MemMapping& MemMapping::operator=(MemMapping&& other) noexcept {
  if (&other == this) {
    return *this;
  }
  this->~MemMapping();
  std::memcpy(this, &other, sizeof(*this));
  new (&other) MemMapping();
  return *this;
}

MemMapping MemMapping::AllocateAnonymous(size_t length, bool huge) {
  MemMapping mm;
  mm.length_ = length;
  assert(mm.addr_ == nullptr);
  if (length == 0) {
    // OK to leave addr as nullptr
    return mm;
  }
  int huge_flag = 0;
#ifdef OS_WIN
  if (huge) {
#ifdef FILE_MAP_LARGE_PAGES
    huge_flag = FILE_MAP_LARGE_PAGES;
#endif  // FILE_MAP_LARGE_PAGES
  }
  mm.page_file_handle_ = ::CreateFileMapping(
      INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE | SEC_COMMIT,
      Upper32of64(length), Lower32of64(length), nullptr);
  if (mm.page_file_handle_ == NULL) {
    // Failure
    return mm;
  }
  mm.addr_ = ::MapViewOfFile(mm.page_file_handle_, FILE_MAP_WRITE | huge_flag,
                             0, 0, length);
#else  // OS_WIN -> !OS_WIN
  if (huge) {
#ifdef MAP_HUGETLB
    huge_flag = MAP_HUGETLB;
#endif  // MAP_HUGE_TLB
  }
  mm.addr_ = mmap(nullptr, length, PROT_READ | PROT_WRITE,
                  MAP_PRIVATE | MAP_ANONYMOUS | huge_flag, -1, 0);
  if (mm.addr_ == MAP_FAILED) {
    mm.addr_ = nullptr;
  }
#endif  // OS_WIN
  return mm;
}

MemMapping MemMapping::AllocateHuge(size_t length) {
  return AllocateAnonymous(length, /*huge*/ true);
}

MemMapping MemMapping::AllocateLazyZeroed(size_t length) {
  return AllocateAnonymous(length, /*huge*/ false);
}

}  // namespace ROCKSDB_NAMESPACE
