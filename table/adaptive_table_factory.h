// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE

#include <string>
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace rocksdb {

struct Options;
struct EnvOptions;

using std::unique_ptr;
class Status;
class RandomAccessFile;
class WritableFile;
class Table;
class TableBuilder;

class AdaptiveTableFactory : public TableFactory {
 public:
  ~AdaptiveTableFactory() {}

  explicit AdaptiveTableFactory(
      std::shared_ptr<TableFactory> table_factory_to_write,
      std::shared_ptr<TableFactory> block_based_table_factory,
      std::shared_ptr<TableFactory> plain_table_factory,
      std::shared_ptr<TableFactory> cuckoo_table_factory);
  const char* Name() const override { return "AdaptiveTableFactory"; }
  Status NewTableReader(const Options& options, const EnvOptions& soptions,
                        const InternalKeyComparator& internal_comparator,
                        unique_ptr<RandomAccessFile>&& file, uint64_t file_size,
                        unique_ptr<TableReader>* table) const override;
  TableBuilder* NewTableBuilder(const Options& options,
                                const InternalKeyComparator& icomparator,
                                WritableFile* file,
                                CompressionType compression_type) const
      override;

  // Sanitizes the specified DB Options.
  Status SanitizeDBOptions(const DBOptions* db_opts) const override {
    if (db_opts->allow_mmap_reads == false) {
      return Status::NotSupported(
          "AdaptiveTable with allow_mmap_reads == false is not supported.");
    }
    return Status::OK();
  }

  std::string GetPrintableTableOptions() const override;

 private:
  std::shared_ptr<TableFactory> table_factory_to_write_;
  std::shared_ptr<TableFactory> block_based_table_factory_;
  std::shared_ptr<TableFactory> plain_table_factory_;
  std::shared_ptr<TableFactory> cuckoo_table_factory_;
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
