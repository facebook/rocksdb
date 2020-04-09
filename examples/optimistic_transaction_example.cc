// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"

using namespace ROCKSDB_NAMESPACE;

std::string kDBPath = "/tmp/rocksdb_transaction_example";

int main() {
  // open DB
  Options options;
  options.create_if_missing = true;
  DB* db;
  OptimisticTransactionDB* txn_db;

  Status s = OptimisticTransactionDB::Open(options, kDBPath, &txn_db);
  assert(s.ok());
  db = txn_db->GetBaseDB();

  WriteOptions write_options;
  ReadOptions read_options;
  OptimisticTransactionOptions txn_options;
  std::string value;

  ////////////////////////////////////////////////////////
  //
  // Simple OptimisticTransaction Example ("Read Committed")
  //
  ////////////////////////////////////////////////////////

  // Start a transaction
  Transaction* txn = txn_db->BeginTransaction(write_options);
  assert(txn);

  // Read a key in this transaction
  s = txn->Get(read_options, "abc", &value);
  assert(s.IsNotFound());

  // Write a key in this transaction
  s = txn->Put("abc", "xyz");
  assert(s.ok());

  // Read a key OUTSIDE this transaction. Does not affect txn.
  s = db->Get(read_options, "abc", &value);
  assert(s.IsNotFound());

  // Write a key OUTSIDE of this transaction.
  // Does not affect txn since this is an unrelated key.  If we wrote key 'abc'
  // here, the transaction would fail to commit.
  s = db->Put(write_options, "xyz", "zzz");
  assert(s.ok());
  s = db->Put(write_options, "abc", "def");
  assert(s.ok());

  // Commit transaction
  s = txn->Commit();
  assert(s.IsBusy());
  delete txn;

  s = db->Get(read_options, "xyz", &value);
  assert(s.ok());
  assert(value == "zzz");

  s = db->Get(read_options, "abc", &value);
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
  s = db->Put(write_options, "abc", "xyz");
  assert(s.ok());

  // Read a key using the snapshot
  read_options.snapshot = snapshot;
  s = txn->GetForUpdate(read_options, "abc", &value);
  assert(s.ok());
  assert(value == "def");

  // Attempt to commit transaction
  s = txn->Commit();

  // Transaction could not commit since the write outside of the txn conflicted
  // with the read!
  assert(s.IsBusy());

  delete txn;
  // Clear snapshot from read options since it is no longer valid
  read_options.snapshot = nullptr;
  snapshot = nullptr;

  s = db->Get(read_options, "abc", &value);
  assert(s.ok());
  assert(value == "xyz");

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
  read_options.snapshot = db->GetSnapshot();
  s = txn->Get(read_options, "x", &value);
  assert(s.IsNotFound());
  s = txn->Put("x", "x");
  assert(s.ok());

  // The transaction hasn't committed, so the write is not visible
  // outside of txn.
  s = db->Get(read_options, "x", &value);
  assert(s.IsNotFound());

  // Do a write outside of the transaction to key "y"
  s = db->Put(write_options, "y", "z");
  assert(s.ok());

  // Set a new snapshot in the transaction
  txn->SetSnapshot();
  read_options.snapshot = db->GetSnapshot();

  // Do some reads and writes to key "y"
  s = txn->GetForUpdate(read_options, "y", &value);
  assert(s.ok());
  assert(value == "z");
  txn->Put("y", "y");

  // Commit.  Since the snapshot was advanced, the write done outside of the
  // transaction does not prevent this transaction from Committing.
  s = txn->Commit();
  assert(s.ok());
  delete txn;
  // Clear snapshot from read options since it is no longer valid
  read_options.snapshot = nullptr;

  // txn is committed, read the latest values.
  s = db->Get(read_options, "x", &value);
  assert(s.ok());
  assert(value == "x");

  s = db->Get(read_options, "y", &value);
  assert(s.ok());
  assert(value == "y");

  // Cleanup
  delete txn_db;
  DestroyDB(kDBPath, options);
  return 0;
}

#endif  // ROCKSDB_LITE
