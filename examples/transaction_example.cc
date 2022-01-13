// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Snapshot;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::Transaction;
using ROCKSDB_NAMESPACE::TransactionDB;
using ROCKSDB_NAMESPACE::TransactionDBOptions;
using ROCKSDB_NAMESPACE::TransactionOptions;
using ROCKSDB_NAMESPACE::WriteOptions;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_transaction_example";
#else
std::string kDBPath = "/tmp/rocksdb_transaction_example";
#endif

int main() {
  // open DB
  Options options;
  TransactionDBOptions txn_db_options;
  options.create_if_missing = true;
  TransactionDB* txn_db;

  Status s = TransactionDB::Open(options, txn_db_options, kDBPath, &txn_db);
  assert(s.ok());

  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;

  ////////////////////////////////////////////////////////
  //
  // Simple Transaction Example ("Read Committed")
  //
  ////////////////////////////////////////////////////////

  // Start a transaction
  Transaction* txn = txn_db->BeginTransaction(write_options);
  assert(txn);

  // Read a key in this transaction
  s = txn->Get(read_options, "abc", &value);
  assert(s.IsNotFound());

  // Write a key in this transaction
  s = txn->Put("abc", "def");
  assert(s.ok());

  // Read a key OUTSIDE this transaction. Does not affect txn.
  s = txn_db->Get(read_options, "abc", &value);
  assert(s.IsNotFound());

  // Write a key OUTSIDE of this transaction.
  // Does not affect txn since this is an unrelated key.
  s = txn_db->Put(write_options, "xyz", "zzz");
  assert(s.ok());

  // Write a key OUTSIDE of this transaction.
  // Fail because the key conflicts with the key written in txn.
  s = txn_db->Put(write_options, "abc", "def");
  assert(s.subcode() == Status::kLockTimeout);

  // Value for key "xyz" has been committed, can be read in txn.
  s = txn->Get(read_options, "xyz", &value);
  assert(s.ok());
  assert(value == "zzz");

  // Commit transaction
  s = txn->Commit();
  assert(s.ok());
  delete txn;

  // Value is committed, can be read now.
  s = txn_db->Get(read_options, "abc", &value);
  assert(s.ok());
  assert(value == "def");

  ////////////////////////////////////////////////////////
  //
  // "Repeatable Read" (Snapshot Isolation) Example
  //   -- Using a single Snapshot
  //
  ////////////////////////////////////////////////////////

  // Set a snapshot at start of transaction by setting set_snapshot=true
  txn_options.set_snapshot = true;
  txn = txn_db->BeginTransaction(write_options, txn_options);

  const Snapshot* snapshot = txn->GetSnapshot();

  // Write a key OUTSIDE of transaction
  s = txn_db->Put(write_options, "abc", "xyz");
  assert(s.ok());

  // Read the latest committed value.
  s = txn->Get(read_options, "abc", &value);
  assert(s.ok());
  assert(value == "xyz");

  // Read the snapshotted value.
  read_options.snapshot = snapshot;
  s = txn->Get(read_options, "abc", &value);
  assert(s.ok());
  assert(value == "def");

  // Attempt to read a key using the snapshot.  This will fail since
  // the previous write outside this txn conflicts with this read.
  s = txn->GetForUpdate(read_options, "abc", &value);
  assert(s.IsBusy());

  txn->Rollback();

  // Snapshot will be released upon deleting the transaction.
  delete txn;
  // Clear snapshot from read options since it is no longer valid
  read_options.snapshot = nullptr;
  snapshot = nullptr;

  ////////////////////////////////////////////////////////
  //
  // "Read Committed" (Monotonic Atomic Views) Example
  //   --Using multiple Snapshots
  //
  ////////////////////////////////////////////////////////

  // In this example, we set the snapshot multiple times.  This is probably
  // only necessary if you have very strict isolation requirements to
  // implement.

  // Set a snapshot at start of transaction
  txn_options.set_snapshot = true;
  txn = txn_db->BeginTransaction(write_options, txn_options);

  // Do some reads and writes to key "x"
  read_options.snapshot = txn_db->GetSnapshot();
  s = txn->Get(read_options, "x", &value);
  assert(s.IsNotFound());
  s = txn->Put("x", "x");
  assert(s.ok());

  // Do a write outside of the transaction to key "y"
  s = txn_db->Put(write_options, "y", "y1");
  assert(s.ok());

  // Set a new snapshot in the transaction
  txn->SetSnapshot();
  txn->SetSavePoint();
  read_options.snapshot = txn_db->GetSnapshot();

  // Do some reads and writes to key "y"
  // Since the snapshot was advanced, the write done outside of the
  // transaction does not conflict.
  s = txn->GetForUpdate(read_options, "y", &value);
  assert(s.ok());
  assert(value == "y1");
  s = txn->Put("y", "y2");
  assert(s.ok());

  // Decide we want to revert the last write from this transaction.
  txn->RollbackToSavePoint();

  // Commit.
  s = txn->Commit();
  assert(s.ok());
  delete txn;
  // Clear snapshot from read options since it is no longer valid
  read_options.snapshot = nullptr;

  // db state is at the save point.
  s = txn_db->Get(read_options, "x", &value);
  assert(s.ok());
  assert(value == "x");

  s = txn_db->Get(read_options, "y", &value);
  assert(s.ok());
  assert(value == "y1");

  // Cleanup
  delete txn_db;
  ROCKSDB_NAMESPACE::DestroyDB(kDBPath, options);
  return 0;
}

#endif  // ROCKSDB_LITE
