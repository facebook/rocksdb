// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.

#ifndef STORAGE_ROCKSDB_INCLUDE_WRITE_BATCH_H_
#define STORAGE_ROCKSDB_INCLUDE_WRITE_BATCH_H_

#include <atomic>
#include <stack>
#include <string>
#include <stdint.h>
#include "rocksdb/status.h"
#include "rocksdb/write_batch_base.h"

namespace rocksdb {

class Slice;
class ColumnFamilyHandle;
struct SavePoints;
struct SliceParts;

struct SavePoint {
  size_t size;  // size of rep_
  int count;    // count of elements in rep_
  uint32_t content_flags;

  SavePoint() : size(0), count(0), content_flags(0) {}

  SavePoint(size_t _size, int _count, uint32_t _flags)
      : size(_size), count(_count), content_flags(_flags) {}

  void clear() {
    size = 0;
    count = 0;
    content_flags = 0;
  }

  bool is_cleared() const { return (size | count | content_flags) == 0; }
};

class WriteBatch : public WriteBatchBase {
 public:
  explicit WriteBatch(size_t reserved_bytes = 0);
  ~WriteBatch();

  using WriteBatchBase::Put;
  // Store the mapping "key->value" in the database.
  void Put(ColumnFamilyHandle* column_family, const Slice& key,
           const Slice& value) override;
  void Put(const Slice& key, const Slice& value) override {
    Put(nullptr, key, value);
  }

  // Variant of Put() that gathers output like writev(2).  The key and value
  // that will be written to the database are concatentations of arrays of
  // slices.
  void Put(ColumnFamilyHandle* column_family, const SliceParts& key,
           const SliceParts& value) override;
  void Put(const SliceParts& key, const SliceParts& value) override {
    Put(nullptr, key, value);
  }

  using WriteBatchBase::Delete;
  // If the database contains a mapping for "key", erase it.  Else do nothing.
  void Delete(ColumnFamilyHandle* column_family, const Slice& key) override;
  void Delete(const Slice& key) override { Delete(nullptr, key); }

  // variant that takes SliceParts
  void Delete(ColumnFamilyHandle* column_family,
              const SliceParts& key) override;
  void Delete(const SliceParts& key) override { Delete(nullptr, key); }

  using WriteBatchBase::SingleDelete;
  // WriteBatch implementation of DB::SingleDelete().  See db.h.
  void SingleDelete(ColumnFamilyHandle* column_family,
                    const Slice& key) override;
  void SingleDelete(const Slice& key) override { SingleDelete(nullptr, key); }

  // variant that takes SliceParts
  void SingleDelete(ColumnFamilyHandle* column_family,
                    const SliceParts& key) override;
  void SingleDelete(const SliceParts& key) override {
    SingleDelete(nullptr, key);
  }

  using WriteBatchBase::DeleteRange;
  // WriteBatch implementation of DB::DeleteRange().  See db.h.
  void DeleteRange(ColumnFamilyHandle* column_family, const Slice& begin_key,
                   const Slice& end_key) override;
  void DeleteRange(const Slice& begin_key, const Slice& end_key) override {
    DeleteRange(nullptr, begin_key, end_key);
  }

  // variant that takes SliceParts
  void DeleteRange(ColumnFamilyHandle* column_family,
                   const SliceParts& begin_key,
                   const SliceParts& end_key) override;
  void DeleteRange(const SliceParts& begin_key,
                   const SliceParts& end_key) override {
    DeleteRange(nullptr, begin_key, end_key);
  }

  using WriteBatchBase::Merge;
  // Merge "value" with the existing value of "key" in the database.
  // "key->merge(existing, value)"
  void Merge(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value) override;
  void Merge(const Slice& key, const Slice& value) override {
    Merge(nullptr, key, value);
  }

  // variant that takes SliceParts
  void Merge(ColumnFamilyHandle* column_family, const SliceParts& key,
             const SliceParts& value) override;
  void Merge(const SliceParts& key, const SliceParts& value) override {
    Merge(nullptr, key, value);
  }

  using WriteBatchBase::PutLogData;
  // Append a blob of arbitrary size to the records in this batch. The blob will
  // be stored in the transaction log but not in any other file. In particular,
  // it will not be persisted to the SST files. When iterating over this
  // WriteBatch, WriteBatch::Handler::LogData will be called with the contents
  // of the blob as it is encountered. Blobs, puts, deletes, and merges will be
  // encountered in the same order in thich they were inserted. The blob will
  // NOT consume sequence number(s) and will NOT increase the count of the batch
  //
  // Example application: add timestamps to the transaction log for use in
  // replication.
  void PutLogData(const Slice& blob) override;

  using WriteBatchBase::Clear;
  // Clear all updates buffered in this batch.
  void Clear() override;

  // Records the state of the batch for future calls to RollbackToSavePoint().
  // May be called multiple times to set multiple save points.
  void SetSavePoint() override;

  // Remove all entries in this batch (Put, Merge, Delete, PutLogData) since the
  // most recent call to SetSavePoint() and removes the most recent save point.
  // If there is no previous call to SetSavePoint(), Status::NotFound()
  // will be returned.
  // Otherwise returns Status::OK().
  Status RollbackToSavePoint() override;

  // Support for iterating over the contents of a batch.
  class Handler {
   public:
    virtual ~Handler();
    // All handler functions in this class provide default implementations so
    // we won't break existing clients of Handler on a source code level when
    // adding a new member function.

    // default implementation will just call Put without column family for
    // backwards compatibility. If the column family is not default,
    // the function is noop
    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) {
      if (column_family_id == 0) {
        // Put() historically doesn't return status. We didn't want to be
        // backwards incompatible so we didn't change the return status
        // (this is a public API). We do an ordinary get and return Status::OK()
        Put(key, value);
        return Status::OK();
      }
      return Status::InvalidArgument(
          "non-default column family and PutCF not implemented");
    }
    virtual void Put(const Slice& /*key*/, const Slice& /*value*/) {}

    virtual Status DeleteCF(uint32_t column_family_id, const Slice& key) {
      if (column_family_id == 0) {
        Delete(key);
        return Status::OK();
      }
      return Status::InvalidArgument(
          "non-default column family and DeleteCF not implemented");
    }
    virtual void Delete(const Slice& /*key*/) {}

    virtual Status SingleDeleteCF(uint32_t column_family_id, const Slice& key) {
      if (column_family_id == 0) {
        SingleDelete(key);
        return Status::OK();
      }
      return Status::InvalidArgument(
          "non-default column family and SingleDeleteCF not implemented");
    }
    virtual void SingleDelete(const Slice& /*key*/) {}

    virtual Status DeleteRangeCF(uint32_t column_family_id,
                                 const Slice& begin_key, const Slice& end_key) {
      return Status::InvalidArgument("DeleteRangeCF not implemented");
    }

    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) {
      if (column_family_id == 0) {
        Merge(key, value);
        return Status::OK();
      }
      return Status::InvalidArgument(
          "non-default column family and MergeCF not implemented");
    }
    virtual void Merge(const Slice& /*key*/, const Slice& /*value*/) {}

    // The default implementation of LogData does nothing.
    virtual void LogData(const Slice& blob);

    virtual Status MarkBeginPrepare() {
      return Status::InvalidArgument("MarkBeginPrepare() handler not defined.");
    }

    virtual Status MarkEndPrepare(const Slice& xid) {
      return Status::InvalidArgument("MarkEndPrepare() handler not defined.");
    }

    virtual Status MarkRollback(const Slice& xid) {
      return Status::InvalidArgument(
          "MarkRollbackPrepare() handler not defined.");
    }

    virtual Status MarkCommit(const Slice& xid) {
      return Status::InvalidArgument("MarkCommit() handler not defined.");
    }

    // Continue is called by WriteBatch::Iterate. If it returns false,
    // iteration is halted. Otherwise, it continues iterating. The default
    // implementation always returns true.
    virtual bool Continue();
  };
  Status Iterate(Handler* handler) const;

  // Retrieve the serialized version of this batch.
  const std::string& Data() const { return rep_; }

  // Retrieve data size of the batch.
  size_t GetDataSize() const { return rep_.size(); }

  // Returns the number of updates in the batch
  int Count() const;

  // Returns true if PutCF will be called during Iterate
  bool HasPut() const;

  // Returns true if DeleteCF will be called during Iterate
  bool HasDelete() const;

  // Returns true if SingleDeleteCF will be called during Iterate
  bool HasSingleDelete() const;

  // Returns true if DeleteRangeCF will be called during Iterate
  bool HasDeleteRange() const;

  // Returns true if MergeCF will be called during Iterate
  bool HasMerge() const;

  // Returns true if MarkBeginPrepare will be called during Iterate
  bool HasBeginPrepare() const;

  // Returns true if MarkEndPrepare will be called during Iterate
  bool HasEndPrepare() const;

  // Returns trie if MarkCommit will be called during Iterate
  bool HasCommit() const;

  // Returns trie if MarkRollback will be called during Iterate
  bool HasRollback() const;

  using WriteBatchBase::GetWriteBatch;
  WriteBatch* GetWriteBatch() override { return this; }

  // Constructor with a serialized string object
  explicit WriteBatch(const std::string& rep);

  WriteBatch(const WriteBatch& src);
  WriteBatch(WriteBatch&& src);
  WriteBatch& operator=(const WriteBatch& src);
  WriteBatch& operator=(WriteBatch&& src);

  // marks this point in the WriteBatch as the last record to
  // be inserted into the WAL, provided the WAL is enabled
  void MarkWalTerminationPoint();
  const SavePoint& GetWalTerminationPoint() const { return wal_term_point_; }

 private:
  friend class WriteBatchInternal;
  SavePoints* save_points_;

  // When sending a WriteBatch through WriteImpl we might want to
  // specify that only the first x records of the batch be written to
  // the WAL.
  SavePoint wal_term_point_;

  // For HasXYZ.  Mutable to allow lazy computation of results
  mutable std::atomic<uint32_t> content_flags_;

  // Performs deferred computation of content_flags if necessary
  uint32_t ComputeContentFlags() const;

 protected:
  std::string rep_;  // See comment in write_batch.cc for the format of rep_

  // Intentionally copyable
};

}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_INCLUDE_WRITE_BATCH_H_
