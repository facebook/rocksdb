//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace rocksdb {

class MemTable;

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface.
class WriteBatchInternal {
 public:
  // Return the number of entries in the batch.
  static int Count(const WriteBatch* batch);

  // Set the count for the number of entries in the batch.
  static void SetCount(WriteBatch* batch, int n);

  // Return the seqeunce number for the start of this batch.
  static SequenceNumber Sequence(const WriteBatch* batch);

  // Store the specified number as the seqeunce number for the start of
  // this batch.
  static void SetSequence(WriteBatch* batch, SequenceNumber seq);

  static Slice Contents(const WriteBatch* batch) {
    return Slice(batch->rep_);
  }

  static size_t ByteSize(const WriteBatch* batch) {
    return batch->rep_.size();
  }

  static void SetContents(WriteBatch* batch, const Slice& contents);

  // Inserts batch entries into memtable
  // Drops deletes in batch if filter_del is set to true and
  // db->KeyMayExist returns false
  static Status InsertInto(const WriteBatch* batch, MemTable* memtable,
                           const Options* opts, DB* db = nullptr,
                           const bool filter_del = false);

  static void Append(WriteBatch* dst, const WriteBatch* src);
};

}  // namespace rocksdb
