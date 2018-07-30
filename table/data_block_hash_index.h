// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

namespace rocksdb {
// This is an experimental feature aiming to reduce the CPU utilization of
// point-lookup within a data-block. It is not used in per-table index-blocks.
// It supports Get(), but not Seek() or Scan(). If the key does not exist,
// the iterator is set to invalid.
//
// A serialized hash index is appended to the data-block. The new block data
// format is as follows:
//
// DATA_BLOCK: [RI RI RI ... RI RI_IDX HASH_IDX FOOTER]
//
// RI:       Restart Interval (the same as the default data-block format)
// RI_IDX:   Restart Interval index (the same as the default data-block format)
// HASH_IDX: The new data-block hash index feature.
// FOOTER:   A 32bit block footer, which is the NUM_RESTARTS with the MSB as
//           the flag indicating if this hash index is in use. Note that
//           given a data block < 32KB, the MSB is never used. So we can
//           borrow the MSB as the hash index flag. Besides, this format is
//           compatible with the legacy data-blocks < 32KB, as the MSB is 0.
//
// If we zoom in the HASH_IDX, the format of the data-block hash index is as
// follows:
//
// HASH_IDX: [B B B ... B IDX NUM_BUCK MAP_START]
//
// B:        B = bucket, an array of pairs <TAG, restart index>.
//           TAG is the second hash value of the string. It is used to flag a
//           matching entry among different keys that are hashed to the same
//           bucket. A similar tagging idea is used in [Lim et. al, SOSP'11].
//           However we have a differnet hash design that is not based on cuckoo
//           hashing as Lim's paper is.
//           We do not have to store the length of individual buckets, as they
//           are delimited by the next bucket offset.
// IDX:      Array of offsets of the index hash bucket (relative to MAP_START)
// NUM_BUCK: Number of buckets, which is the length of the IDX array.
// MAP_START: the starting offset of the data-block hash index.
//
// Each bucket B has the following structure:
// [TAG RESTART_INDEX][TAG RESTART_INDEX]...[TAG RESTART_INDEX]
// where TAG is the hash value of the second hash funtion.
//
// pairs of <key, restart index> are inserted to the hash index. Queries will
// first lookup this hash index to find the restart index, then go to the
// corresponding restart interval to search linearly for the key.
//
// For a point-lookup for a key K:
//
//        Hash1()
// 1) K ===========> bucket_id
//
// 2) Look up this bucket_id in the IDX table to find the offset of the bucket
//
//        Hash2()
// 3) K ============> TAG
// 3) examine the first field (which is TAG) of each entry within this bucket,
//    skip those without a matching TAG.
// 4) for the entries matching the TAG, get the restart interval index from the
//    second field.
//
// (following step are implemented in block.cc)
// 5) lookup the restart index table (refer to the traditional block format),
//    use the restart interval index to find the offset of the restart interval.
// 6) linearly search the restart interval for the key.
//

class DataBlockHashIndexBuilder {
 public:
  explicit DataBlockHashIndexBuilder(uint16_t n)
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
  explicit DataBlockHashIndex(Slice  block_content);

  inline uint16_t DataBlockHashMapStart() const {
    return static_cast<uint16_t>(map_start_ - data_);
  }

  DataBlockHashIndexIterator* NewIterator(const Slice& key) const;

 private:
  const char *data_;
  // To make the serialized hash index compact and to save the space overhead,
  // here all the data fields persisted in the block are in uint16 format.
  // We find that a uint16 is large enough to index every offset of a 64KiB
  // block.
  // So in other words, DataBlockHashIndex does not support block size equal
  // or greater then 64KiB.
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
