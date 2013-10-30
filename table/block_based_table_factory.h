//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <memory>
#include <stdint.h>

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
class BlockBasedTable;
class BlockBasedTableBuilder;

class BlockBasedTableFactory: public TableFactory {
public:
  ~BlockBasedTableFactory() {
  }
  BlockBasedTableFactory() {
  }
  const char* Name() const override {
    return "BlockBasedTable";
  }
  Status GetTableReader(const Options& options, const EnvOptions& soptions,
                        unique_ptr<RandomAccessFile> && file,
                        uint64_t file_size,
                        unique_ptr<TableReader>* table_reader) const override;

  TableBuilder* GetTableBuilder(const Options& options, WritableFile* file,
                                CompressionType compression_type) const
                                    override;
};

}  // namespace rocksdb
