//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#pragma once
#include <string>
#include <stdint.h>
#include "db/dbformat.h"
#include "rocksdb/env.h"
#include "rocksdb/cache.h"
#include "port/port.h"
#include "rocksdb/table.h"

namespace rocksdb {

class Env;

class TableCache {
 public:
  TableCache(const std::string& dbname, const Options* options,
             const EnvOptions& storage_options, int entries);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-nullptr, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or nullptr if no Table object underlies
  // the returned iterator.  The returned "*tableptr" object is owned by
  // the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options,
                        const EnvOptions& toptions,
                        uint64_t file_number,
                        uint64_t file_size,
                        TableReader** table_reader_ptr = nullptr,
                        bool for_compaction = false);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value) repeatedly until
  // it returns false.
  Status Get(const ReadOptions& options,
             uint64_t file_number,
             uint64_t file_size,
             const Slice& k,
             void* arg,
             bool (*handle_result)(void*, const Slice&, const Slice&, bool),
             bool* table_io,
             void (*mark_key_may_exist)(void*) = nullptr);

  // Determine whether the table may contain the specified prefix.  If
  // the table index of blooms are not in memory, this may cause an I/O
  bool PrefixMayMatch(const ReadOptions& options, uint64_t file_number,
                      uint64_t file_size, const Slice& internal_prefix,
                      bool* table_io);

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

 private:
  Env* const env_;
  const std::string dbname_;
  const Options* options_;
  const EnvOptions& storage_options_;
  std::shared_ptr<Cache> cache_;

  Status FindTable(const EnvOptions& toptions, uint64_t file_number,
                   uint64_t file_size, Cache::Handle**, bool* table_io=nullptr,
                   const bool no_io = false);
};

}  // namespace rocksdb
