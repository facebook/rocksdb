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
#include "rocksdb/status.h"

namespace rocksdb {

using std::unique_ptr;

class RandomAccessFile;
class TableBuilder;
class TableReader;
class WritableFile;
struct EnvOptions;
struct Options;

// A base class for table factories
class TableFactory {
 public:
  virtual ~TableFactory() {}

  // The type of the table.
  //
  // The client of this package should switch to a new name whenever
  // the table format implementation changes.
  //
  // Names starting with "rocksdb." are reserved and should not be used
  // by any clients of this package.
  virtual const char* Name() const = 0;

  // Returns a Table object table that can fetch data from file specified
  // in parameter file. It's the caller's responsibility to make sure
  // file is in the correct format.
  //
  // NewTableReader() is called in two places:
  // (1) TableCache::FindTable() calls the function when table cache miss
  //     and cache the table object returned.
  // (1) SstFileReader (for SST Dump) opens the table and dump the table
  //     contents using the interator of the table.
  // options and soptions are options. options is the general options.
  // Multiple configured can be accessed from there, including and not
  // limited to block cache and key comparators.
  // file is a file handler to handle the file for the table
  // file_size is the physical file size of the file
  // table_reader is the output table reader
  virtual Status NewTableReader(
      const Options& options, const EnvOptions& soptions,
      unique_ptr<RandomAccessFile>&& file, uint64_t file_size,
      unique_ptr<TableReader>* table_reader) const = 0;

  // Return a table builder to write to a file for this table type.
  //
  // It is called in several places:
  // (1) When flushing memtable to a level-0 output file, it creates a table
  //     builder (In DBImpl::WriteLevel0Table(), by calling BuildTable())
  // (2) During compaction, it gets the builder for writing compaction output
  //     files in DBImpl::OpenCompactionOutputFile().
  // (3) When recovering from transaction logs, it creates a table builder to
  //     write to a level-0 output file (In DBImpl::WriteLevel0TableForRecovery,
  //     by calling BuildTable())
  // (4) When running Repairer, it creates a table builder to convert logs to
  //     SST files (In Repairer::ConvertLogToTable() by calling BuildTable())
  //
  // options is the general options. Multiple configured can be acceseed from
  // there, including and not limited to compression options.
  // file is a handle of a writable file. It is the caller's responsibility to
  // keep the file open and close the file after closing the table builder.
  // compression_type is the compression type to use in this table.
  virtual TableBuilder* NewTableBuilder(
      const Options& options, WritableFile* file,
      CompressionType compression_type) const = 0;
};

}  // namespace rocksdb
