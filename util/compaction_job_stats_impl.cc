// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <cstring>
#include "include/rocksdb/compaction_job_stats.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE

void CompactionJobStats::Reset() {
  elapsed_micros = 0;

  num_input_files = 0;
  num_input_files_at_output_level = 0;
  num_output_files = 0;

  num_input_records = 0;
  num_output_records = 0;

  total_input_bytes = 0;
  total_output_bytes = 0;

  total_input_raw_key_bytes = 0;
  total_input_raw_value_bytes = 0;

  num_records_replaced = 0;

  is_manual_compaction = 0;
}

#else

void CompactionJobStats::Reset() {
}

#endif  // !ROCKSDB_LITE

}  // namespace rocksdb
