// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/filter_policy.h"

namespace rocksdb {

class Slice;

class FullFilterBitsBuilder : public FilterBitsBuilder {
 public:
  explicit FullFilterBitsBuilder(const int bits_per_key, const int num_probes);

  // No Copy allowed
  FullFilterBitsBuilder(const FullFilterBitsBuilder&) = delete;
  void operator=(const FullFilterBitsBuilder&) = delete;

  ~FullFilterBitsBuilder();

  void AddKey(const Slice& key) override;

  // Create a filter that for hashes [0, n-1], the filter is allocated here
  // When creating filter, it is ensured that
  // total_bits = num_lines * CACHE_LINE_SIZE * 8
  // dst len is >= 5, 1 for num_probes, 4 for num_lines
  // Then total_bits = (len - 5) * 8, and cache_line_size could be calculated
  // +----------------------------------------------------------------+
  // |              filter data with length total_bits/8              |
  // +----------------------------------------------------------------+
  // |                                                                |
  // | ...                                                            |
  // |                                                                |
  // +----------------------------------------------------------------+
  // | ...                | num_probes : 1 byte | num_lines : 4 bytes |
  // +----------------------------------------------------------------+
  Slice Finish(std::unique_ptr<const char[]>* buf) override;

  int CalculateNumEntry(const uint32_t bytes) override;

  // Calculate number of bytes needed for a new filter, including
  // metadata. Passing the result to CalculateNumEntry should
  // return >= the num_entry passed in.
  uint32_t CalculateSpace(const int num_entry, uint32_t* total_bits,
                          uint32_t* num_lines);

 private:
  friend class FullFilterBlockTest_DuplicateEntries_Test;
  int bits_per_key_;
  int num_probes_;
  std::vector<uint32_t> hash_entries_;

  // Get totalbits that optimized for cpu cache line
  uint32_t GetTotalBitsForLocality(uint32_t total_bits);

  // Reserve space for new filter
  char* ReserveSpace(const int num_entry, uint32_t* total_bits,
                     uint32_t* num_lines);

  // Assuming single threaded access to this function.
  void AddHash(uint32_t h, char* data, uint32_t num_lines, uint32_t total_bits);
};

// RocksDB built-in filter policy for Bloom or Bloom-like filters
class BloomFilterPolicy : public FilterPolicy {
 public:
  explicit BloomFilterPolicy(int bits_per_key, bool use_block_based_builder);

  ~BloomFilterPolicy() override;

  const char* Name() const override;

  void CreateFilter(const Slice* keys, int n, std::string* dst) const override;

  bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const override;

  FilterBitsBuilder* GetFilterBitsBuilder() const override;

  // Read metadata to determine what kind of FilterBitsReader is needed
  // and return a new one.
  FilterBitsReader* GetFilterBitsReader(const Slice& contents) const override;

 private:
  int bits_per_key_;
  int num_probes_;
  const bool use_block_based_builder_;
};

}  // namespace rocksdb
