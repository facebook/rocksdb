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
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/table_stats.h"
#include "rocksdb/options.h"

namespace rocksdb {

struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;
class WritableFile;

using std::unique_ptr;

// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.
class TableBuilder {
 public:
  // REQUIRES: Either Finish() or Abandon() has been called.
  virtual ~TableBuilder() {}

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual void Add(const Slice& key, const Slice& value) = 0;

  // Return non-ok iff some error has been detected.
  virtual Status status() const = 0;

  // Finish building the table.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual Status Finish() = 0;

  // Indicate that the contents of this builder should be abandoned.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual void Abandon() = 0;

  // Number of calls to Add() so far.
  virtual uint64_t NumEntries() const = 0;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  virtual uint64_t FileSize() const = 0;
};

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class TableReader {
 public:
  virtual ~TableReader() {}

  // Determine whether there is a chance that the current table file
  // contains the key a key starting with iternal_prefix. The specific
  // table implementation can use bloom filter and/or other heuristic
  // to filter out this table as a whole.
  virtual bool PrefixMayMatch(const Slice& internal_prefix) = 0;

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  virtual Iterator* NewIterator(const ReadOptions&) = 0;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  virtual uint64_t ApproximateOffsetOf(const Slice& key) = 0;

  // Returns true if the block for the specified key is in cache.
  // REQUIRES: key is in this table.
  virtual bool TEST_KeyInCache(const ReadOptions& options,
                               const Slice& key) = 0;

  // Set up the table for Compaction. Might change some parameters with
  // posix_fadvise
  virtual void SetupForCompaction() = 0;

  virtual TableStats& GetTableStats() = 0;

  // Calls (*result_handler)(handle_context, ...) repeatedly, starting with
  // the entry found after a call to Seek(key), until result_handler returns
  // false, where k is the actual internal key for a row found and v as the
  // value of the key. didIO is true if I/O is involved in the operation. May
  // not make such a call if filter policy says that key is not present.
  //
  // mark_key_may_exist_handler needs to be called when it is configured to be
  // memory only and the key is not found in the block cache, with
  // the parameter to be handle_context.
  //
  // readOptions is the options for the read
  // key is the key to search for
  virtual Status Get(
      const ReadOptions& readOptions,
      const Slice& key,
      void* handle_context,
      bool (*result_handler)(void* handle_context, const Slice& k,
                             const Slice& v, bool didIO),
      void (*mark_key_may_exist_handler)(void* handle_context) = nullptr) = 0;
};

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
  // GetTableReader() is called in two places:
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
  virtual Status GetTableReader(
      const Options& options, const EnvOptions& soptions,
      unique_ptr<RandomAccessFile> && file, uint64_t file_size,
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
  virtual TableBuilder* GetTableBuilder(
      const Options& options, WritableFile* file,
      CompressionType compression_type) const = 0;
};
}  // namespace rocksdb
