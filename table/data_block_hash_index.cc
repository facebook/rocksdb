// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <iostream>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "table/data_block_hash_index.h"
#include "util/coding.h"
#include "util/hash.h"

namespace rocksdb {

const uint32_t kSeed = 2018;
const uint32_t kSeed_tag = 214; /* second hash seed */

inline uint32_t HashToBucket(const Slice& s, uint32_t num_buckets) {
  return rocksdb::Hash(s.data(), s.size(), kSeed) % num_buckets;
}

void DataBlockHashIndexBuilder::Add(const Slice& key,
                                  const uint32_t& restart_index) {
  uint32_t idx = HashToBucket(key, num_buckets_);
  /* push a TAG to avoid false postive */
  /* the TAG is the hash function value of another seed */
  buckets_[idx].push_back(rocksdb::Hash(key.data(), key.size(), kSeed_tag));
  buckets_[idx].push_back(restart_index);
  estimate_ += 2 * sizeof(uint32_t);
}

void DataBlockHashIndexBuilder::Finish(std::string& buffer) {
  // offset is the byte offset within the buffer
  std::vector<uint32_t> bucket_offsets(num_buckets_, 0);

  uint32_t map_start = static_cast<uint32_t>(buffer.size());

  // write each bucket to the string
  for (uint32_t i = 0; i < num_buckets_; i++) {
    // remember the start offset of the buckets in bucket_offsets
    bucket_offsets[i] = static_cast<uint32_t>(buffer.size());
    for (uint32_t elem : buckets_[i]) {
      // the elem is alternative "TAG" and "offset"
      PutFixed32(&buffer, elem);
    }
  }

  // write the bucket_offsets
  for (uint32_t i = 0; i < num_buckets_; i++) {
    PutFixed32(&buffer, bucket_offsets[i]);
  }

  // write NUM_BUCK
  PutFixed32(&buffer, num_buckets_);

  // write MAP_START
  PutFixed32(&buffer, map_start);
}

void DataBlockHashIndexBuilder::Reset() {
//  buckets_.clear();
  std::fill(buckets_.begin(), buckets_.end(), std::vector<uint32_t>());
  estimate_ = 0;
}

DataBlockHashIndex::DataBlockHashIndex(Slice block_content) {
  assert(block_content.size() >=
         2 * sizeof(uint32_t));  // NUM_BUCK and MAP_START

  data_ = block_content.data();
  size_ = static_cast<uint32_t>(block_content.size());

  map_start_ = data_ + DecodeFixed32(data_ + size_ - sizeof(uint32_t));
  assert(map_start_ < data_ + size_);

  num_buckets_ = DecodeFixed32(data_ + size_ - 2 * sizeof(uint32_t));
  assert(num_buckets_ > 0);

  assert(size_ >= sizeof(uint32_t) * (2 + num_buckets_));
  bucket_table_ = data_ + size_ - sizeof(uint32_t) * (2 + num_buckets_);

  assert(map_start_ <  bucket_table_);
}

DataBlockHashIndexIterator* DataBlockHashIndex::NewIterator(
    const Slice& key) const {
  uint32_t idx = HashToBucket(key, num_buckets_);
  uint32_t bucket_off = DecodeFixed32(bucket_table_ + idx * sizeof(uint32_t));
  const char* limit;
  if (idx < num_buckets_ - 1) {
    // limited by the start offset of the next bucket
    limit = data_ +
            DecodeFixed32(bucket_table_ + (idx + 1) * sizeof(uint32_t));
  } else {
    // limited by the location of the NUM_BUCK
    limit = data_ + (size_ - 2 * sizeof(uint32_t));
  }

  uint32_t tag = rocksdb::Hash(key.data(), key.size(), kSeed_tag);

  return new DataBlockHashIndexIterator(data_ + bucket_off, limit, tag);
}

bool DataBlockHashIndexIterator::Valid() {
  return current_ < end_;
}

void DataBlockHashIndexIterator::Next() {
  for (current_ += 2 * sizeof(uint32_t); current_ < end_;
       current_ += 2 * sizeof(uint32_t)) {
    // stop at a offset that match the tag, i.e. a possible match
    if (DecodeFixed32(current_) == tag_) {
      break;
    }
  }
}

uint32_t DataBlockHashIndexIterator::Value() {
  return DecodeFixed32(current_+ sizeof(uint32_t));
}

}  // namespace rocksdb
