//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "port/mmap.h"

#include <sys/mman.h>

#ifdef OS_WIN
#include <memoryapi.h>
#include <windows.h>
#endif  // OS_WIN

#include <cassert>
#include <cstdio>
#include <cstring>
#include <new>
#include <utility>

#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

MemMapping::~MemMapping() {
#ifdef OS_WIN
  if (addr != nullptr) {
    (void)::UnmapViewOfFile(addr);
  }
  if (page_file_handle != NULL) {
    (void)::CloseHandle(page_file_handle);
  }
#else
  if (addr != nullptr) {
    auto status = munmap(addr, length);
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

namespace {

void AnonymousMmap(MemMapping& mm, bool huge) {
  assert(mm.addr == nullptr);
  if (mm.length == 0) {
    // OK to leave addr as nullptr
    return;
  }
#ifdef OS_WIN
  mm.page_file_handle = ::CreateFileMapping(
      INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE | SEC_COMMIT,
      Upper32of64(mm.length), Lower32of64(mm.length), nullptr);
  if (mm.page_file_handle == NULL) {
    // Failure
    return;
  }
  mm.addr = ::MapViewOfFile(mm.page_file_handle,
                            FILE_MAP_WRITE | (huge ? FILE_MAP_LARGE_PAGES : 0),
                            0, 0, mm.length);
#else
  int huge_flag = 0;
  if (huge) {
#ifdef MAP_HUGETLB
    huge_flag = MAP_HUGETLB;
#endif  // MAP_HUGE_TLB
  }
  mm.addr = mmap(nullptr, mm.length, (PROT_READ | PROT_WRITE),
                 (MAP_PRIVATE | MAP_ANONYMOUS | huge_flag), -1, 0);
  if (mm.addr == MAP_FAILED) {
    mm.addr = nullptr;
  }
#endif  // OS_WIN
}

}  // namespace

MemMapping MemMapping::AllocateHuge(size_t length) {
  MemMapping rv;
  rv.length = length;
  AnonymousMmap(rv, /*huge*/ true);
  return rv;
}

MemMapping MemMapping::AllocateLazyZeroed(size_t length) {
  MemMapping rv;
  rv.length = length;
  AnonymousMmap(rv, /*huge*/ false);
  return rv;
}

}  // namespace ROCKSDB_NAMESPACE
