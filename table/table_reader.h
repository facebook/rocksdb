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

namespace rocksdb {

class Iterator;
struct ParsedInternalKey;
class Slice;
class Arena;
struct ReadOptions;
struct TableProperties;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class TableReader {
 public:
  virtual ~TableReader() {}

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  // arena: If not null, the arena needs to be used to allocate the Iterator.
  //        When destroying the iterator, the caller will not call "delete"
  //        but Iterator::~Iterator() directly. The destructor needs to destroy
  //        all the states but those allocated in arena.
  virtual Iterator* NewIterator(const ReadOptions&, Arena* arena = nullptr) = 0;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  virtual uint64_t ApproximateOffsetOf(const Slice& key) = 0;

  // Set up the table for Compaction. Might change some parameters with
  // posix_fadvise
  virtual void SetupForCompaction() = 0;

  virtual std::shared_ptr<const TableProperties> GetTableProperties() const = 0;

  // Prepare work that can be done before the real Get()
  virtual void Prepare(const Slice& target) {}

  // Report an approximation of how much memory has been used.
  virtual size_t ApproximateMemoryUsage() const = 0;

  // Calls (*result_handler)(handle_context, ...) repeatedly, starting with
  // the entry found after a call to Seek(key), until result_handler returns
  // false, where k is the actual internal key for a row found and v as the
  // value of the key. May not make such a call if filter policy says that key
  // is not present.
  //
  // mark_key_may_exist_handler needs to be called when it is configured to be
  // memory only and the key is not found in the block cache, with
  // the parameter to be handle_context.
  //
  // readOptions is the options for the read
  // key is the key to search for
  virtual Status Get(
      const ReadOptions& readOptions, const Slice& key, void* handle_context,
      bool (*result_handler)(void* arg, const ParsedInternalKey& k,
                             const Slice& v),
      void (*mark_key_may_exist_handler)(void* handle_context) = nullptr) = 0;
};

}  // namespace rocksdb
