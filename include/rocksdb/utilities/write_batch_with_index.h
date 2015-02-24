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

#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_batch_base.h"

namespace rocksdb {

class ColumnFamilyHandle;
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

  virtual void SeekToFirst() = 0;

  virtual void SeekToLast() = 0;

  virtual void Seek(const Slice& key) = 0;

  virtual void Next() = 0;

  virtual void Prev() = 0;

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
class WriteBatchWithIndex : public WriteBatchBase {
 public:
  // backup_index_comparator: the backup comparator used to compare keys
  // within the same column family, if column family is not given in the
  // interface, or we can't find a column family from the column family handle
  // passed in, backup_index_comparator will be used for the column family.
  // reserved_bytes: reserved bytes in underlying WriteBatch
  // overwrite_key: if true, overwrite the key in the index when inserting
  //                the same key as previously, so iterator will never
  //                show two entries with the same key.
  explicit WriteBatchWithIndex(
      const Comparator* backup_index_comparator = BytewiseComparator(),
      size_t reserved_bytes = 0, bool overwrite_key = false);
  virtual ~WriteBatchWithIndex();

  using WriteBatchBase::Put;
  void Put(ColumnFamilyHandle* column_family, const Slice& key,
           const Slice& value) override;

  void Put(const Slice& key, const Slice& value) override;

  using WriteBatchBase::Merge;
  void Merge(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value) override;

  void Merge(const Slice& key, const Slice& value) override;

  using WriteBatchBase::Delete;
  void Delete(ColumnFamilyHandle* column_family, const Slice& key) override;
  void Delete(const Slice& key) override;

  using WriteBatchBase::PutLogData;
  void PutLogData(const Slice& blob) override;

  using WriteBatchBase::Clear;
  void Clear() override;

  using WriteBatchBase::GetWriteBatch;
  WriteBatch* GetWriteBatch() override;

  // Create an iterator of a column family. User can call iterator.Seek() to
  // search to the next entry of or after a key. Keys will be iterated in the
  // order given by index_comparator. For multiple updates on the same key,
  // each update will be returned as a separate entry, in the order of update
  // time.
  WBWIIterator* NewIterator(ColumnFamilyHandle* column_family);
  // Create an iterator of the default column family.
  WBWIIterator* NewIterator();

  // Will create a new Iterator that will use WBWIIterator as a delta and
  // base_iterator as base
  Iterator* NewIteratorWithBase(ColumnFamilyHandle* column_family,
                                Iterator* base_iterator);
  // default column family
  Iterator* NewIteratorWithBase(Iterator* base_iterator);

 private:
  struct Rep;
  Rep* rep;
};

}  // namespace rocksdb
