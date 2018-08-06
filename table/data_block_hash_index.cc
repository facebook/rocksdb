// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "table/data_block_hash_index.h"
#include "util/coding.h"
#include "util/xxhash.h"

namespace rocksdb {

const uint32_t kSeed = 2018;

inline uint16_t HashToBucket(const Slice& s, uint16_t num_buckets) {
  return static_cast<uint16_t>(
      rocksdb::XXH32(s.data(), static_cast<int>(s.size()), kSeed) % num_buckets);
}

void DataBlockHashIndexBuilder::Add(const Slice& key,
                                    const uint8_t& restart_index) {
  // TODO(fwu): error handling
  uint16_t idx = HashToBucket(key, num_buckets_);
  if (buckets_[idx] == kNoEntry) {
    buckets_[idx] = restart_index;
  } else if (buckets_[idx] != kCollision) {
    buckets_[idx] = kCollision;
  } // if buckets_[idx] is already kCollision, we do not have to do anything.
}

void DataBlockHashIndexBuilder::Finish(std::string& buffer) {
  // write the restart_index array
  for (uint8_t restart_index: buckets_) {
    buffer.append(const_cast<const char*>(
                      reinterpret_cast<char*>(&restart_index)),
                  sizeof(restart_index));
  }

  // write NUM_BUCK
  PutFixed16(&buffer, num_buckets_);

  // Because we use uint16_t address, we only support block less than 64KB
  // TODO(fwu): gracefully handle error
  assert(buffer.size() < (1 << 16));
}

void DataBlockHashIndexBuilder::Reset(uint16_t estimated_num_keys) {
  // update the num_bucket using the new estimated_num_keys for this block
  if (util_ratio_ <= 0) {
    util_ratio_ = 0.75; // sanity check
  }
  num_buckets_ = static_cast<uint16_t>(
      static_cast<double>(estimated_num_keys) / util_ratio_);
  if (num_buckets_ == 0) {
    num_buckets_ = kInitNumBuckets; // sanity check
  }
  buckets_.resize(num_buckets_);
  std::fill(buckets_.begin(), buckets_.end(), kNoEntry);
  estimate_ = sizeof(uint16_t) /*num_buckets*/ +
              num_buckets_ * sizeof(uint8_t) /*n buckets*/;
}

void DataBlockHashIndex::Initialize(const char* data, uint16_t size,
                                    uint16_t* map_offset) {
  assert(size >= sizeof(uint16_t));  // NUM_BUCKETS
  num_buckets_ = DecodeFixed16(data + size - sizeof(uint16_t));
  assert(num_buckets_ > 0);
  assert(size > num_buckets_ * sizeof(uint8_t));
  *map_offset = size - sizeof(uint16_t) - num_buckets_ * sizeof(uint8_t);
}

uint8_t DataBlockHashIndex::Seek(const char* data, uint16_t map_offset,
                                 const Slice& key) const {
  uint16_t idx = HashToBucket(key, num_buckets_);
  const char* bucket_table = data + map_offset;
  return static_cast<uint8_t>(*(bucket_table + idx * sizeof(uint8_t)));
}

}  // namespace rocksdb
