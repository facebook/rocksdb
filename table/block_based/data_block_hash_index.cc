// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "table/block_based/data_block_hash_index.h"

#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

void DataBlockHashIndexBuilder::Add(const Slice& key,
                                    const size_t restart_index) {
  assert(Valid());
  if (restart_index > kMaxRestartSupportedByHashIndex) {
    valid_ = false;
    return;
  }

  uint32_t hash_value = GetSliceHash(key);
  hash_and_restart_pairs_.emplace_back(hash_value,
                                       static_cast<uint8_t>(restart_index));
  estimated_num_buckets_ += bucket_per_key_;
}

void DataBlockHashIndexBuilder::Finish(std::string& buffer) {
  assert(Valid());
  uint16_t num_buckets = static_cast<uint16_t>(estimated_num_buckets_);

  if (num_buckets == 0) {
    num_buckets = 1;  // sanity check
  }

  // The build-in hash cannot well distribute strings when into different
  // buckets when num_buckets is power of two, resulting in high hash
  // collision.
  // We made the num_buckets to be odd to avoid this issue.
  num_buckets |= 1;

  std::vector<uint8_t> buckets(num_buckets, kNoEntry);
  // write the restart_index array
  for (auto& entry : hash_and_restart_pairs_) {
    uint32_t hash_value = entry.first;
    uint8_t restart_index = entry.second;
    uint16_t buck_idx = static_cast<uint16_t>(hash_value % num_buckets);
    if (buckets[buck_idx] == kNoEntry) {
      buckets[buck_idx] = restart_index;
    } else if (buckets[buck_idx] != restart_index) {
      // same bucket cannot store two different restart_index, mark collision
      buckets[buck_idx] = kCollision;
    }
  }

  for (uint8_t restart_index : buckets) {
    buffer.append(
        const_cast<const char*>(reinterpret_cast<char*>(&restart_index)),
        sizeof(restart_index));
  }

  // write NUM_BUCK
  PutFixed16(&buffer, num_buckets);

  assert(buffer.size() <= kMaxBlockSizeSupportedByHashIndex);
}

void DataBlockHashIndexBuilder::Reset() {
  estimated_num_buckets_ = 0;
  valid_ = true;
  hash_and_restart_pairs_.clear();
}

bool DataBlockHashIndex::Initialize(const char* data, size_t size,
                                    uint16_t* map_offset) {
  num_buckets_ = 0;
  if (size < sizeof(uint16_t) /* NUM_BUCKETS */ ||
      size >= kMaxBlockSizeSupportedByHashIndex) {
    return false;
  }
  num_buckets_ = DecodeFixed16(data + size - sizeof(uint16_t));
  // The bucket array plus the NUM_BUCKETS field must fit within `size`. A
  // corrupted block can encode a `num_buckets_` large enough to underflow the
  // `map_offset` computation below, so reject it here rather than relying on
  // the debug-only asserts (compiled out in release/ASAN builds).
  if (num_buckets_ == 0 ||
      num_buckets_ * sizeof(uint8_t) + sizeof(uint16_t) > size) {
    num_buckets_ = 0;
    return false;
  }
  *map_offset = static_cast<uint16_t>(size - sizeof(uint16_t) -
                                      num_buckets_ * sizeof(uint8_t));
  return true;
}

uint8_t DataBlockHashIndex::Lookup(const char* data, uint32_t map_offset,
                                   const Slice& key) const {
  uint32_t hash_value = GetSliceHash(key);
  uint16_t idx = static_cast<uint16_t>(hash_value % num_buckets_);
  const char* bucket_table = data + map_offset;
  return static_cast<uint8_t>(*(bucket_table + idx * sizeof(uint8_t)));
}

}  // namespace ROCKSDB_NAMESPACE
