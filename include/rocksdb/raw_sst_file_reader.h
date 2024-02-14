// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE

#include <memory>
#include <string>

#include "rocksdb/raw_iterator.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/options.h"



namespace ROCKSDB_NAMESPACE {

class RawSstFileReader {
 public:

  RawSstFileReader(const Options& options, const std::string& file_name,
            size_t readahead_size, bool verify_checksum,
            bool silent = false);
  ~RawSstFileReader();

  RawIterator* newIterator(bool has_from, Slice* from,
                           bool has_to, Slice *to);
  Status getStatus() { return init_result_; }

 private:
  // Get the TableReader implementation for the sst file
  Status GetTableReader(const std::string& file_path);
  Status ReadTableProperties(uint64_t table_magic_number,
                             uint64_t file_size);

  Status SetTableOptionsByMagicNumber(uint64_t table_magic_number);
  Status SetOldTableOptions();

  // Helper function to call the factory with settings specific to the
  // factory implementation
  Status NewTableReader(uint64_t file_size);

  std::string file_name_;
  Temperature file_temp_;

  // less verbose in stdout/stderr
  bool silent_;

  // options_ and internal_comparator_ will also be used in
  // ReadSequential internally (specifically, seek-related operations)
  Options options_;

  Status init_result_;

  struct Rep;
  std::unique_ptr<Rep> rep_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
