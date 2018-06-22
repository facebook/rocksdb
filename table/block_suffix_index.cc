// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/slice.h"
#include "table/block_suffix_index.h"
#include "util/hash.h"

namespace rocksdb {

const uint32_t kSeed = 2018;

inline uint16_t SuffixToBucket(const Slice& s, uint16_t num_buckets) {
  return rocksdb::Hash(s.data(), s.size(), kSeed) % num_buckets;
}

// BlockSuffixIndexBuilder
void BlockSuffixIndexBuilder::Add(const Slice& suffix, const uint16_t& pos) {
  uint16_t idx = SuffixToBucket(suffix, num_buckets_);
  bucket_[idx].push_back(pos);
}

void BlockSuffixIndexBuilder::Finish(std::string& /* suffix_index */) {
  // TODO(fwu)
}


// BlockSuffixIndex
bool BlockSuffixIndex::Seek(const Slice& /*key*/,  uint16_t* /*index*/) const {
// TODO(fwu)
  return false;
}

}  // namespace rocksdb
