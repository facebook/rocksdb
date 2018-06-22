// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef BLOCK_SUFFIX_INDEX_H
#define BLOCK_SUFFIX_INDEX_H

#include <string>
#include <vector>

namespace rocksdb {

// The format of the suffix hash map is as follows:
//
// B B B ... B IDX NUM_BUCK MAP_SIZE
//
// NUM_BUCK: Number of buckets, which is the length of the IDX array.
//
// IDX:      Array of offsets, each pointing to the starting offset (relative to
//           MAP_START) of one hash bucket.
//
// B:        Bucket, an array consisting of a list of restart interval offsets.
//           We do not have to store the length of individual buckets, as they
//           are delimited by the next bucket offset.
//
// MAP_SIZE: the size of the hash map.
//
// The suffix hash map is construct right in-place of the block without any data
// been copied.

class BlockSuffixIndexBuilder {
 public:
  BlockSuffixIndexBuilder(uint32_t n) : num_buckets_(n), buckets_(n) {}
  void Add(const Slice &suffix, const uint32_t &pos);
  std::string Finish();

 private:
  uint32_t num_buckets_;
  std::vector<std::vector<uint32_t>> buckets_;
};

class BlockSuffixIndex {
 public:
  BlockSuffixIndex(std::string &s);
  bool Seek(const Slice &key, std::vector<uint32_t> &bucket) const;

 private:
  std::string suffix_index_;
  const char *data_;
  uint32_t size_;
  uint32_t num_buckets_;
  uint32_t map_size_;
  const char *map_start_;
  const char *bucket_table_;
};

}  // namespace rocksdb
#endif
