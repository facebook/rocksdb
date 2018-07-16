// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

namespace rocksdb {

// The format of the datablock hash map is as follows:
//
// B B B ... B IDX NUM_BUCK MAP_START
//
// NUM_BUCK: Number of buckets, which is the length of the IDX array.
//
// IDX:      Array of offsets of the index hash bucket (relative to MAP_START).
//
// B:        B = bucket, an array consisting of a list of restart index, and the
//           second hash value as tag.
//           We do not have to store the length of individual buckets, as they
//           are delimited by the next bucket offset.
//
// MAP_START: the starting offset of the datablock hash map.
//
// Each bucket B has the following structure:
// [TAG RESTART_INDEX][TAG RESTART_INDEX]...[TAG RESTART_INDEX]
// where TAG is the hash value of the second hash funtion.
//
// The datablock hash map is construct right in-place of the block without any
// data been copied.

class DataBlockHashIndexBuilder {
 public:
  DataBlockHashIndexBuilder(uint16_t n)
      : num_buckets_(n),
        buckets_(n),
        estimate_((n + 2) *
                  sizeof(uint16_t) /* n buckets, 2 num at the end */) {}
  void Add(const Slice& key, const uint16_t& restart_index);
  void Finish(std::string& buffer);
  void Reset();
  inline size_t EstimateSize() { return estimate_; }

 private:
  uint16_t num_buckets_;
  std::vector<std::vector<uint16_t>> buckets_;
  size_t estimate_;
};

class DataBlockHashIndexIterator;

class DataBlockHashIndex {
 public:
  DataBlockHashIndex(Slice  block_content);

  inline uint16_t DataBlockHashMapStart() const {
    return static_cast<uint16_t>(map_start_ - data_);
  }

  DataBlockHashIndexIterator* NewIterator(const Slice& key) const;

 private:
  const char *data_;
  uint16_t size_;
  uint16_t num_buckets_;
  const char *map_start_;    // start of the map
  const char *bucket_table_; // start offset of the bucket index table
};

class DataBlockHashIndexIterator {
 public:
  DataBlockHashIndexIterator(const char* start, const char* end,
                             const uint16_t tag)
      : end_(end), tag_(tag) {
    current_ = start - 2 * sizeof(uint16_t);
    Next();
  }
  bool Valid();
  void Next();
  uint16_t Value();

 private:
  const char* end_; // the end of the bucket
  const uint16_t tag_;  // the fingerprint (2nd hash value) of the searching key
  const char* current_;
};

}  // namespace rocksdb
