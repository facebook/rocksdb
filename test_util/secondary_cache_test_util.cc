//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "test_util/secondary_cache_test_util.h"

#include <array>

namespace ROCKSDB_NAMESPACE::secondary_cache_test_util {

namespace {
using TestItem = WithCacheType::TestItem;

size_t SizeCallback(Cache::ObjectPtr obj) {
  return static_cast<TestItem*>(obj)->Size();
}

Status SaveToCallback(Cache::ObjectPtr from_obj, size_t from_offset,
                      size_t length, char* out) {
  auto item = static_cast<TestItem*>(from_obj);
  const char* buf = item->Buf();
  EXPECT_EQ(length, item->Size());
  EXPECT_EQ(from_offset, 0);
  memcpy(out, buf, length);
  return Status::OK();
}

void DeletionCallback(Cache::ObjectPtr obj, MemoryAllocator* /*alloc*/) {
  delete static_cast<TestItem*>(obj);
}

Status SaveToCallbackFail(Cache::ObjectPtr /*obj*/, size_t /*offset*/,
                          size_t /*size*/, char* /*out*/) {
  return Status::NotSupported();
}

Status CreateCallback(const Slice& data, CompressionType /*type*/,
                      CacheTier /*source*/, Cache::CreateContext* context,
                      MemoryAllocator* /*allocator*/, Cache::ObjectPtr* out_obj,
                      size_t* out_charge) {
  auto t = static_cast<TestCreateContext*>(context);
  if (t->fail_create_) {
    return Status::NotSupported();
  }
  *out_obj = new TestItem(data.data(), data.size());
  *out_charge = data.size();
  return Status::OK();
}

// If helpers without_secondary are provided, returns helpers with secondary
// support. If not provided, returns helpers without secondary support.
auto GenerateHelpersByRole(
    const std::array<Cache::CacheItemHelper, kNumCacheEntryRoles>*
        without_secondary,
    bool fail) {
  std::array<Cache::CacheItemHelper, kNumCacheEntryRoles> a;
  for (uint32_t i = 0; i < kNumCacheEntryRoles; ++i) {
    if (without_secondary) {
      a[i] =
          Cache::CacheItemHelper{static_cast<CacheEntryRole>(i),
                                 &DeletionCallback,
                                 &SizeCallback,
                                 fail ? &SaveToCallbackFail : &SaveToCallback,
                                 &CreateCallback,
                                 &(*without_secondary)[i]};
    } else {
      a[i] = Cache::CacheItemHelper{static_cast<CacheEntryRole>(i),
                                    &DeletionCallback};
    }
  }
  return a;
}
}  // namespace

const Cache::CacheItemHelper* WithCacheType::GetHelper(
    CacheEntryRole r, bool secondary_compatible, bool fail) {
  static const std::array<Cache::CacheItemHelper, kNumCacheEntryRoles>
      without_secondary = GenerateHelpersByRole(nullptr, false);
  static const std::array<Cache::CacheItemHelper, kNumCacheEntryRoles>
      with_secondary = GenerateHelpersByRole(&without_secondary, false);
  static const std::array<Cache::CacheItemHelper, kNumCacheEntryRoles>
      with_secondary_fail = GenerateHelpersByRole(&without_secondary, true);
  return &(fail                   ? with_secondary_fail
           : secondary_compatible ? with_secondary
                                  : without_secondary)[static_cast<int>(r)];
}

const Cache::CacheItemHelper* WithCacheType::GetHelperFail(CacheEntryRole r) {
  return GetHelper(r, true, true);
}

}  // namespace ROCKSDB_NAMESPACE::secondary_cache_test_util
