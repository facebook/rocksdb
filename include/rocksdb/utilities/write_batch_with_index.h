// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A WriteBatchWithIndex with a binary searchable index built for all the keys
// inserted.

#pragma once

#include "rocksdb/status.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"

namespace rocksdb {

class ColumnFamilyHandle;
struct SliceParts;
class Comparator;

enum WriteType { kPutRecord, kMergeRecord, kDeleteRecord, kLogDataRecord };

// an entry for Put, Merge or Delete entry for write batches. Used in
// WBWIIterator.
struct WriteEntry {
  WriteType type;
  Slice key;
  Slice value;
};

// Iterator of one column family out of a WriteBatchWithIndex.
class WBWIIterator {
 public:
  virtual ~WBWIIterator() {}

  virtual bool Valid() const = 0;

  virtual void Seek(const Slice& key) = 0;

  virtual void Next() = 0;

  virtual const WriteEntry& Entry() const = 0;

  virtual Status status() const = 0;
};

// A WriteBatchWithIndex with a binary searchable index built for all the keys
// inserted.
// In Put(), Merge() or Delete(), the same function of the wrapped will be
// called. At the same time, indexes will be built.
// By calling GetWriteBatch(), a user will get the WriteBatch for the data
// they inserted, which can be used for DB::Write().
// A user can call NewIterator() to create an iterator.
class WriteBatchWithIndex {
 public:
  // index_comparator indicates the order when iterating data in the write
  // batch. Technically, it doesn't have to be the same as the one used in
  // the DB.
  // reserved_bytes: reserved bytes in underlying WriteBatch
  explicit WriteBatchWithIndex(const Comparator* index_comparator,
                               size_t reserved_bytes = 0);
  virtual ~WriteBatchWithIndex();

  WriteBatch* GetWriteBatch();

  virtual void Put(ColumnFamilyHandle* column_family, const Slice& key,
                   const Slice& value);

  virtual void Put(const Slice& key, const Slice& value);

  virtual void Merge(ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value);

  virtual void Merge(const Slice& key, const Slice& value);

  virtual void PutLogData(const Slice& blob);

  virtual void Delete(ColumnFamilyHandle* column_family, const Slice& key);
  virtual void Delete(const Slice& key);

  virtual void Delete(ColumnFamilyHandle* column_family, const SliceParts& key);

  virtual void Delete(const SliceParts& key);

  // Create an iterator of a column family. User can call iterator.Seek() to
  // search to the next entry of or after a key. Keys will be iterated in the
  // order given by index_comparator. For multiple updates on the same key,
  // each update will be returned as a separate entry, in the order of update
  // time.
  virtual WBWIIterator* NewIterator(ColumnFamilyHandle* column_family);
  // Create an iterator of the default column family.
  virtual WBWIIterator* NewIterator();

 private:
  struct Rep;
  Rep* rep;
};

}  // namespace rocksdb
