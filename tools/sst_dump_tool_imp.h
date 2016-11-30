// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/sst_dump_tool.h"

#include <memory>
#include <string>
#include "db/dbformat.h"
#include "util/cf_options.h"
#include "util/file_reader_writer.h"

namespace rocksdb {

class SstFileReader {
 public:
  explicit SstFileReader(const std::string& file_name, bool verify_checksum,
                         bool output_hex);

  Status ReadSequential(bool print_kv, uint64_t read_num, bool has_from,
                        const std::string& from_key, bool has_to,
                        const std::string& to_key);

  Status ReadTableProperties(
      std::shared_ptr<const TableProperties>* table_properties);
  uint64_t GetReadNumber() { return read_num_; }
  TableProperties* GetInitTableProperties() { return table_properties_.get(); }

  Status DumpTable(const std::string& out_filename);
  Status getStatus() { return init_result_; }

  int ShowAllCompressionSizes(size_t block_size);

 private:
  // Get the TableReader implementation for the sst file
  Status GetTableReader(const std::string& file_path);
  Status ReadTableProperties(uint64_t table_magic_number,
                             RandomAccessFileReader* file, uint64_t file_size);

  uint64_t CalculateCompressedTableSize(const TableBuilderOptions& tb_options,
                                        size_t block_size);

  Status SetTableOptionsByMagicNumber(uint64_t table_magic_number);
  Status SetOldTableOptions();

  // Helper function to call the factory with settings specific to the
  // factory implementation
  Status NewTableReader(const ImmutableCFOptions& ioptions,
                        const EnvOptions& soptions,
                        const InternalKeyComparator& internal_comparator,
                        uint64_t file_size,
                        unique_ptr<TableReader>* table_reader);

  std::string file_name_;
  uint64_t read_num_;
  bool verify_checksum_;
  bool output_hex_;
  EnvOptions soptions_;

  // options_ and internal_comparator_ will also be used in
  // ReadSequential internally (specifically, seek-related operations)
  Options options_;

  Status init_result_;
  unique_ptr<TableReader> table_reader_;
  unique_ptr<RandomAccessFileReader> file_;

  const ImmutableCFOptions ioptions_;
  InternalKeyComparator internal_comparator_;
  unique_ptr<TableProperties> table_properties_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
