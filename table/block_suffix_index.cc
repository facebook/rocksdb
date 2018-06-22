// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <iostream>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "table/block_suffix_index.h"
#include "util/coding.h"
#include "util/hash.h"

namespace rocksdb {

const uint32_t kSeed = 2018;

inline uint32_t SuffixToBucket(const Slice& s, uint32_t num_buckets) {
  return rocksdb::Hash(s.data(), s.size(), kSeed) % num_buckets;
}

void BlockSuffixIndexBuilder::Add(const Slice& key, const uint32_t& pos) {
  uint32_t idx = SuffixToBucket(key, num_buckets_);
  buckets_[idx].push_back(pos);
}

std::string BlockSuffixIndexBuilder::Finish() {
  std::string index_content;

  // offset is relative to the start of map
  std::vector<uint32_t> bucket_offsets(num_buckets_, 0);

  // write each bucket to the string
  for (uint32_t i = 0; i < num_buckets_; i++) {
    // remember the start offset of the buckets in bucket_offsets
    bucket_offsets[i] = index_content.size();
    for (uint32_t restart_offset : buckets_[i])
      PutFixed32(&index_content, restart_offset);
  }

  // write the bucket_offsets
  for (uint32_t i = 0; i < num_buckets_; i++) {
    PutFixed32(&index_content, bucket_offsets[i]);
  }

  // write NUM_BUCK
  PutFixed32(&index_content, num_buckets_);

  // write MAP_SIZE
  uint32_t map_size = index_content.size() + sizeof(uint32_t) /* MAP_SIZE */;
  PutFixed32(&index_content, map_size);

  return index_content;
}

BlockSuffixIndex::BlockSuffixIndex(std::string& s) {
  assert(s.size() >= 2 * sizeof(uint32_t));  // NUM_BUCK and MAP_SIZE
  suffix_index_ = s;                         // can we avoid this memory copy?

  data_ = suffix_index_.data();
  size_ = suffix_index_.size();

  map_size_ = DecodeFixed32(data_ + size_ - sizeof(uint32_t));
  assert(map_size_ > 0);

  num_buckets_ = DecodeFixed32(data_ + size_ - 2 * sizeof(uint32_t));
  assert(num_buckets_ > 0);

  assert(size_ >= sizeof(uint32_t) * (2 + num_buckets_));
  bucket_table_ = data_ + size_ - sizeof(uint32_t) * (2 + num_buckets_);

  assert(size_ >= map_size_);
  map_start_ = data_ + size_ - map_size_;
  assert(map_start_ <= bucket_table_);
}

bool BlockSuffixIndex::Seek(const Slice& key,
                            std::vector<uint32_t>& bucket) const {
  assert(bucket.size() == 0);
  uint32_t idx = SuffixToBucket(key, num_buckets_);
  uint32_t bucket_off = DecodeFixed32(bucket_table_ + idx * sizeof(uint32_t));
  const char* limit;
  if (idx < num_buckets_ - 1)
    // limited by the start offset of the next bucket
    limit = map_start_ +
            DecodeFixed32(bucket_table_ + (idx + 1) * sizeof(uint32_t));
  else
    // limited by the location of the NUM_BUCK
    limit = map_start_ + (size_ - 2 * sizeof(uint32_t));
  for (const char* p = map_start_ + bucket_off; p < limit;
       p += sizeof(uint32_t)) {
    bucket.push_back(DecodeFixed32(p));
  }
  return false;
}

}  // namespace rocksdb
