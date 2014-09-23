// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "rocksdb/options.h"

namespace rocksdb {

struct MutableCFOptions {
  explicit MutableCFOptions(const Options& options)
    : write_buffer_size(options.write_buffer_size),
      arena_block_size(options.arena_block_size),
      memtable_prefix_bloom_bits(options.memtable_prefix_bloom_bits),
      memtable_prefix_bloom_probes(options.memtable_prefix_bloom_probes),
      memtable_prefix_bloom_huge_page_tlb_size(
          options.memtable_prefix_bloom_huge_page_tlb_size),
      max_successive_merges(options.max_successive_merges),
      filter_deletes(options.filter_deletes) {
  }
  MutableCFOptions()
    : write_buffer_size(0),
      arena_block_size(0),
      memtable_prefix_bloom_bits(0),
      memtable_prefix_bloom_probes(0),
      memtable_prefix_bloom_huge_page_tlb_size(0),
      max_successive_merges(0),
      filter_deletes(false) {}

  size_t write_buffer_size;
  size_t arena_block_size;
  uint32_t memtable_prefix_bloom_bits;
  uint32_t memtable_prefix_bloom_probes;
  size_t memtable_prefix_bloom_huge_page_tlb_size;
  size_t max_successive_merges;
  bool filter_deletes;
};

}  // namespace rocksdb
