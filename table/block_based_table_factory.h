//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <memory>
#include <string>

#include "rocksdb/flush_block_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "db/dbformat.h"

namespace rocksdb {

struct Options;
struct EnvOptions;

using std::unique_ptr;
class BlockBasedTableBuilder;

class BlockBasedTableFactory : public TableFactory {
 public:
  explicit BlockBasedTableFactory(
      const BlockBasedTableOptions& table_options = BlockBasedTableOptions());

  ~BlockBasedTableFactory() {}

  const char* Name() const override { return "BlockBasedTable"; }

  Status NewTableReader(const Options& options, const EnvOptions& soptions,
                        const InternalKeyComparator& internal_comparator,
                        unique_ptr<RandomAccessFile>&& file, uint64_t file_size,
                        unique_ptr<TableReader>* table_reader) const override;

  TableBuilder* NewTableBuilder(
      const Options& options, const InternalKeyComparator& internal_comparator,
      WritableFile* file, CompressionType compression_type) const override;

  // Sanitizes the specified DB Options.
  Status SanitizeDBOptions(const DBOptions* db_opts) const override {
    return Status::OK();
  }

  std::string GetPrintableTableOptions() const override;

 private:
  BlockBasedTableOptions table_options_;
};

extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;

}  // namespace rocksdb
