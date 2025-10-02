// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <memory>
#include <string>

#include "db/dbformat.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/advanced_options.h"

namespace ROCKSDB_NAMESPACE {

class SstFileDumper {
 public:
  explicit SstFileDumper(const Options& options, const std::string& file_name,
                         Temperature file_temp, size_t readahead_size,
                         bool verify_checksum, bool output_hex,
                         bool decode_blob_index,
                         const EnvOptions& soptions = EnvOptions(),
                         bool silent = false);

  // read_num_limit limits the total number of keys read. If read_num_limit = 0,
  // then there is no limit. If read_num_limit = 0 or
  // std::numeric_limits<uint64_t>::max(), has_from and has_to are false, then
  // the number of keys read is compared with `num_entries` field in table
  // properties. A Corruption status is returned if they do not match.
  Status ReadSequential(bool print_kv, uint64_t read_num_limit, bool has_from,
                        const std::string& from_key, bool has_to,
                        const std::string& to_key,
                        bool use_from_as_prefix = false);

  Status ReadTableProperties(
      std::shared_ptr<const TableProperties>* table_properties);
  uint64_t GetReadNumber() { return read_num_; }
  TableProperties* GetInitTableProperties() { return table_properties_.get(); }

  Status VerifyChecksum();
  Status DumpTable(const std::string& out_filename);
  Status getStatus() { return init_result_; }

  Status ShowAllCompressionSizes(
      const std::vector<CompressionType>& compression_types,
      int32_t compress_level_from, int32_t compress_level_to);

  Status ShowCompressionSize(CompressionType compress_type,
                             const CompressionOptions& compress_opt);

  BlockContents& GetMetaIndexContents() { return meta_index_contents_; }

 private:
  // Get the TableReader implementation for the sst file
  Status GetTableReader(const std::string& file_path);
  Status ReadTableProperties(uint64_t table_magic_number,
                             RandomAccessFileReader* file, uint64_t file_size,
                             FilePrefetchBuffer* prefetch_buffer);

  Status CalculateCompressedTableSize(const TableBuilderOptions& tb_options,
                                      TableProperties* props,
                                      std::chrono::microseconds* write_time,
                                      std::chrono::microseconds* read_time);

  Status SetTableOptionsByMagicNumber(uint64_t table_magic_number);
  Status SetOldTableOptions();

  // Helper function to call the factory with settings specific to the
  // factory implementation
  Status NewTableReader(const ImmutableOptions& ioptions,
                        const EnvOptions& soptions,
                        const InternalKeyComparator& internal_comparator,
                        uint64_t file_size,
                        std::unique_ptr<TableReader>* table_reader);

  std::string file_name_;
  uint64_t read_num_;
  Temperature file_temp_;
  bool output_hex_;
  bool decode_blob_index_;
  EnvOptions soptions_;
  // less verbose in stdout/stderr
  bool silent_;

  // options_ and internal_comparator_ will also be used in
  // ReadSequential internally (specifically, seek-related operations)
  Options options_;

  Status init_result_;
  std::unique_ptr<TableReader> table_reader_;
  std::unique_ptr<RandomAccessFileReader> file_;

  ImmutableOptions ioptions_;
  const MutableCFOptions moptions_;
  ReadOptions read_options_;
  InternalKeyComparator internal_comparator_;
  std::unique_ptr<TableProperties> table_properties_;
  BlockContents meta_index_contents_;
};

}  // namespace ROCKSDB_NAMESPACE
