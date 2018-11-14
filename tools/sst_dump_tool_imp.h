// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/sst_dump_tool.h"
#include "table/internal_sst_file_reader.h"

#include <string>

namespace rocksdb {

class SstFileDumper : public InternalSstFileReader {
 public:
  explicit SstFileDumper(const std::string& file_name, bool verify_checksum,
                         bool output_hex);

  Status ReadSequential(bool print_kv, uint64_t read_num, bool has_from,
                        const std::string& from_key, bool has_to,
                        const std::string& to_key,
                        bool use_from_as_prefix = false);

  Status ReadTableProperties(
      std::shared_ptr<const TableProperties>* table_properties);
  uint64_t GetReadNumber() { return read_num_; }
  TableProperties* GetInitTableProperties() { return table_properties_.get(); }

  Status DumpTable(const std::string& out_filename);

  virtual ~SstFileDumper() {}

  int ShowAllCompressionSizes(
      size_t block_size,
      const std::vector<std::pair<CompressionType, const char*>>&
          compression_types);

 private:
  virtual Status SetTableOptionsByMagicNumber(
      uint64_t table_magic_number) override;
  virtual Status SetOldTableOptions() override;
  virtual Status ReadTableProperties(uint64_t table_magic_number,
                                     RandomAccessFileReader* file,
                                     uint64_t file_size) override;

  uint64_t CalculateCompressedTableSize(const TableBuilderOptions& tb_options,
                                        size_t block_size);

  bool verify_checksum_;
  uint64_t read_num_;
  bool output_hex_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
