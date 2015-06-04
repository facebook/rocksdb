// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#include <stddef.h>
#include <stdint.h>
#include <string>

namespace rocksdb {
struct CompactionJobStats {
  CompactionJobStats() { Reset(); }
  void Reset();

  // the elapsed time in micro of this compaction.
  uint64_t elapsed_micros;

  // the number of compaction input records.
  uint64_t num_input_records;
  // the number of compaction input files.
  size_t num_input_files;
  // the number of compaction input files at the output level.
  size_t num_input_files_at_output_level;

  // the number of compaction output records.
  uint64_t num_output_records;
  // the number of compaction output files.
  size_t num_output_files;

  // true if the compaction is a manual compaction
  bool is_manual_compaction;

  // the size of the compaction input in bytes.
  uint64_t total_input_bytes;
  // the size of the compaction output in bytes.
  uint64_t total_output_bytes;

  // number of records being replaced by newer record associated with same key
  uint64_t num_records_replaced;

  // the sum of the uncompressed input keys in bytes.
  uint64_t total_input_raw_key_bytes;
  // the sum of the uncompressed input values in bytes.
  uint64_t total_input_raw_value_bytes;

  // 0-terminated strings storing the first 8 bytes of the smallest and
  // largest key in the output.
  static const size_t kMaxPrefixLength = 8;

  std::string smallest_output_key_prefix;
  std::string largest_output_key_prefix;
};
}  // namespace rocksdb
