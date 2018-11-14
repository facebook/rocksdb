//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <string>
#include "rocksdb/options.h"
#include "table/table_reader.h"

namespace rocksdb {

class InternalSstFileReader {
 public:
  InternalSstFileReader(const std::string& file_name, Options options,
                        const Comparator* comparator);

  virtual ~InternalSstFileReader() {}

  InternalIterator* NewIterator(bool cksum, bool cache,
                                const SliceTransform* prefix_extractor);
  InternalIterator* NewIterator(bool cksum, bool cache);
  InternalIterator* NewIterator(const ReadOptions& read_options,
                                const SliceTransform* prefix_extractor,
                                Arena* arena = nullptr,
                                bool skip_filters = false,
                                bool for_compaction = false);

  Status ReadTableProperties(
      std::shared_ptr<const TableProperties>* table_properties);

  Status getStatus() { return init_result_; }

  Status VerifyChecksum();

 protected:
  enum SstFileFormat {
    BlockBased,
    PlainTable,
    Unsupported,
  };

  InternalSstFileReader(Options options = Options(),
                        const Comparator* comparator = BytewiseComparator());

  Status Open(const std::string& file_name);

  // Get the TableReader implementation for the sst file
  Status GetTableReader(const std::string& file_path);

  // Helper function to call the factory with settings specific to the
  // factory implementation
  Status NewTableReader(const ImmutableCFOptions& ioptions,
                        const EnvOptions& soptions,
                        const InternalKeyComparator& internal_comparator,
                        uint64_t file_size,
                        std::unique_ptr<TableReader>* table_reader);

  virtual Status ReadTableProperties(uint64_t table_magic_number,
                                     RandomAccessFileReader* file,
                                     uint64_t file_size);

  virtual Status SetTableOptionsByMagicNumber(uint64_t table_magic_number);
  void SetBlockBasedTableOptionsByMagicNumber();
  void SetPlainTableOptionsByMagicNumber();
  virtual Status SetOldTableOptions();

  SstFileFormat GetSstFileFormat(uint64_t table_magic_number);

  std::string file_name_;
  EnvOptions soptions_;

  // options_ and internal_comparator_ will also be used in NewIterator,
  // ReadSequential internally (specifically, seek-related operations)
  Options options_;
  const ImmutableCFOptions ioptions_;
  const MutableCFOptions moptions_;

  Status init_result_ = Status::Incomplete();
  std::unique_ptr<TableReader> table_reader_;
  std::unique_ptr<RandomAccessFileReader> file_;

  InternalKeyComparator internal_comparator_;
  std::unique_ptr<TableProperties> table_properties_;
};
}  // namespace rocksdb

#endif  // !ROCKSDB_LITE
