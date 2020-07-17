//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <memory>
#include <string>

#include "db/dbformat.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {
struct ConfigOptions;
struct EnvOptions;

class BlockBasedTableBuilder;

// A class used to track actual bytes written from the tail in the recent SST
// file opens, and provide a suggestion for following open.
class TailPrefetchStats {
 public:
  void RecordEffectiveSize(size_t len);
  // 0 indicates no information to determine.
  size_t GetSuggestedPrefetchSize();

 private:
  const static size_t kNumTracked = 32;
  size_t records_[kNumTracked];
  port::Mutex mutex_;
  size_t next_ = 0;
  size_t num_records_ = 0;
};

class BlockBasedTableFactory : public TableFactory {
 public:
  explicit BlockBasedTableFactory(
      const BlockBasedTableOptions& table_options = BlockBasedTableOptions());

  ~BlockBasedTableFactory() {}

  const char* Name() const override { return kName.c_str(); }

  using TableFactory::NewTableReader;
  Status NewTableReader(
      const ReadOptions& ro, const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache = true) const override;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, WritableFileWriter* file) const override;

  // Sanitizes the specified DB Options.
  Status SanitizeOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

  std::string GetPrintableTableOptions() const override;

  Status GetOptionString(const ConfigOptions& config_options,
                         std::string* opt_string) const override;

  const BlockBasedTableOptions& table_options() const;

  void* GetOptions() override { return &table_options_; }

  bool IsDeleteRangeSupported() const override { return true; }

  TailPrefetchStats* tail_prefetch_stats() { return &tail_prefetch_stats_; }

  static const std::string kName;

 private:
  BlockBasedTableOptions table_options_;
  mutable TailPrefetchStats tail_prefetch_stats_;
};

extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;
extern const std::string kPropTrue;
extern const std::string kPropFalse;

#ifndef ROCKSDB_LITE
extern Status VerifyBlockBasedTableFactory(
    const ConfigOptions& config_options, const BlockBasedTableFactory* base_tf,
    const BlockBasedTableFactory* file_tf);
#endif  // !ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE
