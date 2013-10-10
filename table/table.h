// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <memory>
#include <stdint.h>
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/table_stats.h"

namespace rocksdb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;

using std::unique_ptr;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class Table {
 public:
  static const std::string kFilterBlockPrefix;
  static const std::string kStatsBlock;

  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to nullptr and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options& options,
                     const EnvOptions& soptions,
                     unique_ptr<RandomAccessFile>&& file,
                     uint64_t file_size,
                     unique_ptr<Table>* table);

  ~Table();

  bool PrefixMayMatch(const Slice& internal_prefix) const;

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  Iterator* NewIterator(const ReadOptions&) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice& key) const;

  // Returns true if the block for the specified key is in cache.
  // REQUIRES: key is in this table.
  bool TEST_KeyInCache(const ReadOptions& options, const Slice& key);

  // Set up the table for Compaction. Might change some parameters with
  // posix_fadvise
  void SetupForCompaction();

  const TableStats& GetTableStats() const;

 private:
  struct Rep;
  Rep* rep_;
  bool compaction_optimized_;

  explicit Table(Rep* rep) : compaction_optimized_(false) { rep_ = rep; }
  static Iterator* BlockReader(void*, const ReadOptions&,
                               const EnvOptions& soptions, const Slice&,
                               bool for_compaction);
  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&,
                               bool* didIO, bool for_compaction = false);

  // Calls (*handle_result)(arg, ...) repeatedly, starting with the entry found
  // after a call to Seek(key), until handle_result returns false.
  // May not make such a call if filter policy says that key is not present.
  friend class TableCache;
  Status InternalGet(
      const ReadOptions&, const Slice& key,
      void* arg,
      bool (*handle_result)(void* arg, const Slice& k, const Slice& v, bool),
      void (*mark_key_may_exist)(void*) = nullptr);


  void ReadMeta(const Footer& footer);
  void ReadFilter(const Slice& filter_handle_value);
  static Status ReadStats(const Slice& handle_value, Rep* rep);

  static void SetupCacheKeyPrefix(Rep* rep);

  // No copying allowed
  Table(const Table&);
  void operator=(const Table&);
};

struct TableStatsNames {
  static const std::string kDataSize;
  static const std::string kIndexSize;
  static const std::string kRawKeySize;
  static const std::string kRawValueSize;
  static const std::string kNumDataBlocks;
  static const std::string kNumEntries;
};

}  // namespace rocksdb
