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
  uint16_t idx = HashToBucket(key, num_buckets_);
  if (buckets_[idx] == kNoEntry) {
    buckets_[idx] = restart_index;
  } else if (buckets_[idx] != kCollision) {
    buckets_[idx] = kCollision;
  } // if buckets_[idx] is already kCollision, we do not have to do anything.
}

void DataBlockHashIndexBuilder::Finish(std::string& buffer) {
  // offset is the byte offset within the buffer
  uint16_t map_start = static_cast<uint16_t>(buffer.size());

  // write the restart_index array
//  for (uint16_t i = 0; i < num_buckets_; i++) {
  for (uint8_t restart_index: buckets_) {
    buffer.append(const_cast<const char*>(
                      reinterpret_cast<char*>(&restart_index)),
                  sizeof(restart_index));
  }

  // write NUM_BUCK
  PutFixed16(&buffer, num_buckets_);

  // write MAP_START
  PutFixed16(&buffer, map_start);

  // Because we use uint16_t address, we only support block less than 64KB
  assert(buffer.size() < (1 << 16));
}

void DataBlockHashIndexBuilder::Reset() {
  std::fill(buckets_.begin(), buckets_.end(), kNoEntry);
  estimate_ = 2 * sizeof(uint16_t) /*num_buck and maps_start*/+
    num_buckets_ * sizeof(uint8_t) /*n buckets*/;
}

void DataBlockHashIndex::Initialize(Slice block_content) {
  assert(block_content.size() >=
         2 * sizeof(uint16_t));  // NUM_BUCK and MAP_START

  data_ = block_content.data();
  size_ = static_cast<uint16_t>(block_content.size());

  map_start_ = data_ + DecodeFixed16(data_ + size_ - sizeof(uint16_t));
  assert(map_start_ < data_ + size_);

  num_buckets_ = DecodeFixed16(data_ + size_ - 2 * sizeof(uint16_t));
  assert(num_buckets_ > 0);

  assert(size_ >= 2 * sizeof(uint16_t) + num_buckets_ * sizeof(uint8_t));
  bucket_table_ = data_ + size_ - 2 * sizeof(uint16_t)
    - num_buckets_ * sizeof(uint8_t);

  assert(map_start_ <=  bucket_table_);
}

uint8_t DataBlockHashIndex::Seek(const Slice& key) const {
  uint16_t idx = HashToBucket(key, num_buckets_);
  return static_cast<uint8_t>(*(bucket_table_ + idx * sizeof(uint8_t)));
}

}  // namespace rocksdb
