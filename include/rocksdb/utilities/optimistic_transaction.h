// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#ifndef ROCKSDB_LITE

#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace rocksdb {

class OptimisticTransactionDB;
class WriteBatchWithIndex;

// Provides BEGIN/COMMIT/ROLLBACK transactions for batched writes.
//
// The current implementation provides optimistic concurrency control.
// Transactional reads/writes will not block other operations in the
// db.  At commit time, the batch of writes will only be written if there have
// been no other writes to any keys read or written by this transaction.
// Otherwise, the commit will return an error.
//
// A new optimistic transaction is created by calling
// OptimisticTransactionDB::BeginTransaction().
// Only reads/writes done through this transaction object will be a part of the
// transaction.  Any other reads/writes will not be tracked by this
// transaction.
//
// For example, reading data via OptimisticTransaction::GetForUpdate() will
// prevent the transaction from committing if this key is written to outside of
// this transaction.  Any reads done via DB::Get() will not be checked for
// conflicts at commit time.
//
// It is up to the caller to synchronize access to this object.
//
// See examples/transaction_example.cc for some simple examples.
//
// TODO(agiardullo): Not yet implemented:
//  -Transaction support for iterators
//  -Ensuring memtable holds large enough history to check for conflicts
//  -Support for using Transactions with DBWithTTL

// Options to use when starting an Optimistic Transaction
struct OptimisticTransactionOptions {
  // Setting set_snapshot=true is the same as calling SetSnapshot().
  bool set_snapshot = false;

  // Should be set if the DB has a non-default comparator.
  // See comment in WriteBatchWithIndex constructor.
  const Comparator* cmp = BytewiseComparator();
};

class OptimisticTransaction {
 public:
  virtual ~OptimisticTransaction() {}

  // If SetSnapshot() is not called, all keys read/written through this
  // transaction will only be committed if there have been no writes to
  // these keys outside of this transaction *since the time each key
  // was first read/written* in this transaction.
  //
  // When SetSnapshot() is called, this transaction will create a Snapshot
  // to use for conflict validation of all future operations in the transaction.
  // All future keys read/written will only be committed if there have been
  // no writes to these keys outside of this transaction *since SetSnapshot()
  // was called.* Otherwise, Commit() will not succeed.
  //
  // It is not necessary to call SetSnapshot() if you only care about other
  // writes happening on keys *after* they have first been read/written in this
  // transaction.  However, you should set a snapshot if you are concerned
  // with any other writes happening since a particular time (such as
  // the start of the transaction).
  //
  // SetSnapshot() may be called multiple times if you would like to change
  // the snapshot used for different operations in this transaction.
  //
  // Calling SetSnapshot will not affect the version of Data returned by Get()
  // methods.  See OptimisticTransaction::Get() for more details.
  //
  // TODO(agiardullo): add better documentation here once memtable change are
  // committed
  virtual void SetSnapshot() = 0;

  // Returns the Snapshot created by the last call to SetSnapshot().
  //
  // REQUIRED: The returned Snapshot is only valid up until the next time
  // SetSnapshot() is called or the OptimisticTransaction is deleted.
  virtual const Snapshot* GetSnapshot() const = 0;

  // Write all batched keys to the db atomically if there have not been any
  // other writes performed on the keys read/written by this transaction.
  //
  // Currently, Commit() only checks the memtables to verify that there are no
  // other writes to these keys.  If the memtable's history is not long
  // enough to verify that there are no conflicts, Commit() will return
  // a non-OK status.
  //
  // Returns OK on success, non-OK on failure.
  virtual Status Commit() = 0;

  // Discard all batched writes in this transaction.
  virtual void Rollback() = 0;

  // This function is similar to DB::Get() except it will also read pending
  // changes in this transaction.
  //
  // If read_options.snapshot is not set, the current version of the key will
  // be read.  Calling SetSnapshot() does not affect the version of the data
  // returned.
  //
  // Note that setting read_options.snapshot will affect what is read from the
  // DB but will NOT change which keys are read from this transaction (the keys
  // in this transaction do not yet belong to any snapshot and will be fetched
  // regardless).
  //
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     std::string* value) = 0;

  virtual Status Get(const ReadOptions& options, const Slice& key,
                     std::string* value) = 0;

  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys, std::vector<std::string>* values) = 0;

  virtual std::vector<Status> MultiGet(const ReadOptions& options,
                                       const std::vector<Slice>& keys,
                                       std::vector<std::string>* values) = 0;

  // Read this key and ensure that this transaction will only
  // be able to be committed if this key is not written outside this
  // transaction after it has first been read (or after the snapshot if a
  // snapshot is set in this transaction).

  // This function is similar to OptimisticTransaction::Get() except it will
  // affect whether this transaction will be able to be committed.
  virtual Status GetForUpdate(const ReadOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice& key, std::string* value) = 0;

  virtual Status GetForUpdate(const ReadOptions& options, const Slice& key,
                              std::string* value) = 0;

  virtual std::vector<Status> MultiGetForUpdate(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys, std::vector<std::string>* values) = 0;

  virtual std::vector<Status> MultiGetForUpdate(
      const ReadOptions& options, const std::vector<Slice>& keys,
      std::vector<std::string>* values) = 0;

  // Put, Merge, and Delete behave similarly to their corresponding
  // functions in WriteBatch.  In addition, this transaction will only
  // be able to be committed if these keys are not written outside of this
  // transaction after they have been written by this transaction (or after the
  // snapshot if a snapshot is set in this transaction).
  virtual void Put(ColumnFamilyHandle* column_family, const Slice& key,
                   const Slice& value) = 0;
  virtual void Put(const Slice& key, const Slice& value) = 0;
  virtual void Put(ColumnFamilyHandle* column_family, const SliceParts& key,
                   const SliceParts& value) = 0;
  virtual void Put(const SliceParts& key, const SliceParts& value) = 0;

  virtual void Merge(ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value) = 0;
  virtual void Merge(const Slice& key, const Slice& value) = 0;

  virtual void Delete(ColumnFamilyHandle* column_family, const Slice& key) = 0;
  virtual void Delete(const Slice& key) = 0;
  virtual void Delete(ColumnFamilyHandle* column_family,
                      const SliceParts& key) = 0;
  virtual void Delete(const SliceParts& key) = 0;

  // PutUntracked() will write a Put to the batch of operations to be committed
  // in this transaction.  This write will only happen if this transaction
  // gets committed successfully.  But unlike OptimisticTransaction::Put(),
  // no conflict checking will be done for this key.  So any other writes to
  // this key outside of this transaction will not prevent this transaction from
  // committing.
  virtual void PutUntracked(ColumnFamilyHandle* column_family, const Slice& key,
                            const Slice& value) = 0;
  virtual void PutUntracked(const Slice& key, const Slice& value) = 0;
  virtual void PutUntracked(ColumnFamilyHandle* column_family,
                            const SliceParts& key, const SliceParts& value) = 0;
  virtual void PutUntracked(const SliceParts& key, const SliceParts& value) = 0;

  virtual void MergeUntracked(ColumnFamilyHandle* column_family,
                              const Slice& key, const Slice& value) = 0;
  virtual void MergeUntracked(const Slice& key, const Slice& value) = 0;

  virtual void DeleteUntracked(ColumnFamilyHandle* column_family,
                               const Slice& key) = 0;

  virtual void DeleteUntracked(const Slice& key) = 0;
  virtual void DeleteUntracked(ColumnFamilyHandle* column_family,
                               const SliceParts& key) = 0;
  virtual void DeleteUntracked(const SliceParts& key) = 0;

  // Similar to WriteBatch::PutLogData
  virtual void PutLogData(const Slice& blob) = 0;

  // Fetch the underlying write batch that contains all pending changes to be
  // committed.
  //
  // Note:  You should not write or delete anything from the batch directly and
  // should only use the the functions in the OptimisticTransaction class to
  // write to this transaction.
  virtual WriteBatchWithIndex* GetWriteBatch() = 0;

 protected:
  // To begin a new transaction, see OptimisticTransactionDB::BeginTransaction()
  explicit OptimisticTransaction(const OptimisticTransactionDB* db) {}
  OptimisticTransaction() {}

 private:
  // No copying allowed
  OptimisticTransaction(const OptimisticTransaction&);
  void operator=(const OptimisticTransaction&);
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
