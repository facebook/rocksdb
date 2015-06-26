//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include <string>

#include "rocksdb/db.h"
#include "rocksdb/utilities/optimistic_transaction.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "util/logging.h"
#include "util/testharness.h"

using std::string;

namespace rocksdb {

class OptimisticTransactionTest : public testing::Test {
 public:
  OptimisticTransactionDB* txn_db;
  DB* db;
  string dbname;
  Options options;

  OptimisticTransactionTest() {
    options.create_if_missing = true;
    options.max_write_buffer_number = 2;
    dbname = test::TmpDir() + "/optimistic_transaction_testdb";

    DestroyDB(dbname, options);
    Status s = OptimisticTransactionDB::Open(options, dbname, &txn_db);
    assert(s.ok());
    db = txn_db->GetBaseDB();
  }

  ~OptimisticTransactionTest() {
    delete txn_db;
    DestroyDB(dbname, options);
  }
};

TEST_F(OptimisticTransactionTest, SuccessTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  db->Put(write_options, Slice("foo"), Slice("bar"));
  db->Put(write_options, Slice("foo2"), Slice("bar"));

  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  txn->GetForUpdate(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  txn->Put(Slice("foo"), Slice("bar2"));

  txn->GetForUpdate(read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  s = txn->Commit();
  ASSERT_OK(s);

  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  delete txn;
}

TEST_F(OptimisticTransactionTest, WriteConflictTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  db->Put(write_options, "foo", "bar");
  db->Put(write_options, "foo2", "bar");

  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  txn->Put("foo", "bar2");

  // This Put outside of a transaction will conflict with the previous write
  s = db->Put(write_options, "foo", "barz");
  ASSERT_OK(s);

  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "barz");

  s = txn->Commit();
  ASSERT_NOK(s);  // Txn should not commit

  // Verify that transaction did not write anything
  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "barz");
  db->Get(read_options, "foo2", &value);
  ASSERT_EQ(value, "bar");

  delete txn;
}

TEST_F(OptimisticTransactionTest, WriteConflictTest2) {
  WriteOptions write_options;
  ReadOptions read_options;
  OptimisticTransactionOptions txn_options;
  string value;
  Status s;

  db->Put(write_options, "foo", "bar");
  db->Put(write_options, "foo2", "bar");

  txn_options.set_snapshot = true;
  OptimisticTransaction* txn =
      txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  // This Put outside of a transaction will conflict with a later write
  s = db->Put(write_options, "foo", "barz");
  ASSERT_OK(s);

  txn->Put("foo", "bar2");  // Conflicts with write done after snapshot taken

  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "barz");

  s = txn->Commit();
  ASSERT_NOK(s);  // Txn should not commit

  // Verify that transaction did not write anything
  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "barz");
  db->Get(read_options, "foo2", &value);
  ASSERT_EQ(value, "bar");

  delete txn;
}

TEST_F(OptimisticTransactionTest, ReadConflictTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  OptimisticTransactionOptions txn_options;
  string value;
  Status s;

  db->Put(write_options, "foo", "bar");
  db->Put(write_options, "foo2", "bar");

  txn_options.set_snapshot = true;
  OptimisticTransaction* txn =
      txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn->GetForUpdate(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  // This Put outside of a transaction will conflict with the previous read
  s = db->Put(write_options, "foo", "barz");
  ASSERT_OK(s);

  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "barz");

  s = txn->Commit();
  ASSERT_NOK(s);  // Txn should not commit

  // Verify that transaction did not write anything
  txn->GetForUpdate(read_options, "foo", &value);
  ASSERT_EQ(value, "barz");
  txn->GetForUpdate(read_options, "foo2", &value);
  ASSERT_EQ(value, "bar");

  delete txn;
}

TEST_F(OptimisticTransactionTest, TxnOnlyTest) {
  // Test to make sure transactions work when there are no other writes in an
  // empty db.

  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  txn->Put("x", "y");

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
}

TEST_F(OptimisticTransactionTest, FlushTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  string value;
  Status s;

  db->Put(write_options, Slice("foo"), Slice("bar"));
  db->Put(write_options, Slice("foo2"), Slice("bar"));

  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn->GetForUpdate(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  txn->Put(Slice("foo"), Slice("bar2"));

  txn->GetForUpdate(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  // Put a random key so we have a memtable to flush
  s = db->Put(write_options, "dummy", "dummy");
  ASSERT_OK(s);

  // force a memtable flush
  FlushOptions flush_ops;
  db->Flush(flush_ops);

  s = txn->Commit();
  // txn should commit since the flushed table is still in MemtableList History
  ASSERT_OK(s);

  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  delete txn;
}

TEST_F(OptimisticTransactionTest, FlushTest2) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  string value;
  Status s;

  db->Put(write_options, Slice("foo"), Slice("bar"));
  db->Put(write_options, Slice("foo2"), Slice("bar"));

  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn->GetForUpdate(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  txn->Put(Slice("foo"), Slice("bar2"));

  txn->GetForUpdate(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  // Put a random key so we have a MemTable to flush
  s = db->Put(write_options, "dummy", "dummy");
  ASSERT_OK(s);

  // force a memtable flush
  FlushOptions flush_ops;
  db->Flush(flush_ops);

  // Put a random key so we have a MemTable to flush
  s = db->Put(write_options, "dummy", "dummy2");
  ASSERT_OK(s);

  // force a memtable flush
  db->Flush(flush_ops);

  s = db->Put(write_options, "dummy", "dummy3");
  ASSERT_OK(s);

  // force a memtable flush
  // Since our test db has max_write_buffer_number=2, this flush will cause
  // the first memtable to get purged from the MemtableList history.
  db->Flush(flush_ops);

  s = txn->Commit();
  // txn should not commit since MemTableList History is not large enough
  ASSERT_NOK(s);

  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  delete txn;
}

TEST_F(OptimisticTransactionTest, NoSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  db->Put(write_options, "AAA", "bar");

  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  // Modify key after transaction start
  db->Put(write_options, "AAA", "bar1");

  // Read and write without a snapshot
  txn->GetForUpdate(read_options, "AAA", &value);
  ASSERT_EQ(value, "bar1");
  txn->Put("AAA", "bar2");

  // Should commit since read/write was done after data changed
  s = txn->Commit();
  ASSERT_OK(s);

  txn->GetForUpdate(read_options, "AAA", &value);
  ASSERT_EQ(value, "bar2");

  delete txn;
}

TEST_F(OptimisticTransactionTest, MultipleSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  string value;
  Status s;

  db->Put(write_options, "AAA", "bar");
  db->Put(write_options, "BBB", "bar");
  db->Put(write_options, "CCC", "bar");

  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  db->Put(write_options, "AAA", "bar1");

  // Read and write without a snapshot
  txn->GetForUpdate(read_options, "AAA", &value);
  ASSERT_EQ(value, "bar1");
  txn->Put("AAA", "bar2");

  // Modify BBB before snapshot is taken
  db->Put(write_options, "BBB", "bar1");

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  // Read and write with snapshot
  txn->GetForUpdate(snapshot_read_options, "BBB", &value);
  ASSERT_EQ(value, "bar1");
  txn->Put("BBB", "bar2");

  db->Put(write_options, "CCC", "bar1");

  // Set a new snapshot
  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  // Read and write with snapshot
  txn->GetForUpdate(snapshot_read_options, "CCC", &value);
  ASSERT_EQ(value, "bar1");
  txn->Put("CCC", "bar2");

  s = txn->GetForUpdate(read_options, "AAA", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");
  s = txn->GetForUpdate(read_options, "BBB", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");
  s = txn->GetForUpdate(read_options, "CCC", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");

  s = db->Get(read_options, "AAA", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar1");
  s = db->Get(read_options, "BBB", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar1");
  s = db->Get(read_options, "CCC", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar1");

  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "AAA", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");
  s = db->Get(read_options, "BBB", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");
  s = db->Get(read_options, "CCC", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");

  // verify that we track multiple writes to the same key at different snapshots
  delete txn;
  txn = txn_db->BeginTransaction(write_options);

  // Potentially conflicting writes
  db->Put(write_options, "ZZZ", "zzz");
  db->Put(write_options, "XXX", "xxx");

  txn->SetSnapshot();

  OptimisticTransactionOptions txn_options;
  txn_options.set_snapshot = true;
  OptimisticTransaction* txn2 =
      txn_db->BeginTransaction(write_options, txn_options);
  txn2->SetSnapshot();

  // This should not conflict in txn since the snapshot is later than the
  // previous write (spoiler alert:  it will later conflict with txn2).
  txn->Put("ZZZ", "zzzz");
  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;

  // This will conflict since the snapshot is earlier than another write to ZZZ
  txn2->Put("ZZZ", "xxxxx");

  s = txn2->Commit();
  ASSERT_NOK(s);

  delete txn2;
}

TEST_F(OptimisticTransactionTest, ColumnFamiliesTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  OptimisticTransactionOptions txn_options;
  string value;
  Status s;

  ColumnFamilyHandle *cfa, *cfb;
  ColumnFamilyOptions cf_options;

  // Create 2 new column families
  s = db->CreateColumnFamily(cf_options, "CFA", &cfa);
  ASSERT_OK(s);
  s = db->CreateColumnFamily(cf_options, "CFB", &cfb);
  ASSERT_OK(s);

  delete cfa;
  delete cfb;
  delete txn_db;

  // open DB with three column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
  // open the new column families
  column_families.push_back(
      ColumnFamilyDescriptor("CFA", ColumnFamilyOptions()));
  column_families.push_back(
      ColumnFamilyDescriptor("CFB", ColumnFamilyOptions()));
  std::vector<ColumnFamilyHandle*> handles;
  s = OptimisticTransactionDB::Open(options, dbname, column_families, &handles,
                                    &txn_db);
  ASSERT_OK(s);
  db = txn_db->GetBaseDB();

  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn_options.set_snapshot = true;
  OptimisticTransaction* txn2 =
      txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  // Write some data to the db
  WriteBatch batch;
  batch.Put("foo", "foo");
  batch.Put(handles[1], "AAA", "bar");
  batch.Put(handles[1], "AAAZZZ", "bar");
  s = db->Write(write_options, &batch);
  ASSERT_OK(s);
  db->Delete(write_options, handles[1], "AAAZZZ");

  // These keys do no conflict with existing writes since they're in
  // different column families
  txn->Delete("AAA");
  txn->GetForUpdate(snapshot_read_options, handles[1], "foo", &value);
  Slice key_slice("AAAZZZ");
  Slice value_slices[2] = {Slice("bar"), Slice("bar")};
  txn->Put(handles[2], SliceParts(&key_slice, 1), SliceParts(value_slices, 2));

  // Txn should commit
  s = txn->Commit();
  ASSERT_OK(s);
  s = db->Get(read_options, "AAA", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = db->Get(read_options, handles[2], "AAAZZZ", &value);
  ASSERT_EQ(value, "barbar");

  Slice key_slices[3] = {Slice("AAA"), Slice("ZZ"), Slice("Z")};
  Slice value_slice("barbarbar");
  // This write will cause a conflict with the earlier batch write
  txn2->Put(handles[1], SliceParts(key_slices, 3), SliceParts(&value_slice, 1));

  txn2->Delete(handles[2], "XXX");
  txn2->Delete(handles[1], "XXX");
  s = txn2->GetForUpdate(snapshot_read_options, handles[1], "AAA", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Verify txn did not commit
  s = txn2->Commit();
  ASSERT_NOK(s);
  s = db->Get(read_options, handles[1], "AAAZZZ", &value);
  ASSERT_EQ(value, "barbar");

  delete txn;
  delete txn2;

  txn = txn_db->BeginTransaction(write_options, txn_options);
  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  std::vector<ColumnFamilyHandle*> multiget_cfh = {handles[1], handles[2],
                                                   handles[0], handles[2]};
  std::vector<Slice> multiget_keys = {"AAA", "AAAZZZ", "foo", "foo"};
  std::vector<std::string> values(4);

  std::vector<Status> results = txn->MultiGetForUpdate(
      snapshot_read_options, multiget_cfh, multiget_keys, &values);
  ASSERT_OK(results[0]);
  ASSERT_OK(results[1]);
  ASSERT_OK(results[2]);
  ASSERT_TRUE(results[3].IsNotFound());
  ASSERT_EQ(values[0], "bar");
  ASSERT_EQ(values[1], "barbar");
  ASSERT_EQ(values[2], "foo");

  txn->Delete(handles[2], "ZZZ");
  txn->Put(handles[2], "ZZZ", "YYY");
  txn->Put(handles[2], "ZZZ", "YYYY");
  txn->Delete(handles[2], "ZZZ");
  txn->Put(handles[2], "AAAZZZ", "barbarbar");

  // Txn should commit
  s = txn->Commit();
  ASSERT_OK(s);
  s = db->Get(read_options, handles[2], "ZZZ", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Put a key which will conflict with the next txn using the previous snapshot
  db->Put(write_options, handles[2], "foo", "000");

  results = txn2->MultiGetForUpdate(snapshot_read_options, multiget_cfh,
                                    multiget_keys, &values);
  ASSERT_OK(results[0]);
  ASSERT_OK(results[1]);
  ASSERT_OK(results[2]);
  ASSERT_TRUE(results[3].IsNotFound());
  ASSERT_EQ(values[0], "bar");
  ASSERT_EQ(values[1], "barbar");
  ASSERT_EQ(values[2], "foo");

  // Verify Txn Did not Commit
  s = txn2->Commit();
  ASSERT_NOK(s);

  s = db->DropColumnFamily(handles[1]);
  ASSERT_OK(s);
  s = db->DropColumnFamily(handles[2]);
  ASSERT_OK(s);

  delete txn;
  delete txn2;

  for (auto handle : handles) {
    delete handle;
  }
}

TEST_F(OptimisticTransactionTest, EmptyTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  s = db->Put(write_options, "aaa", "aaa");
  ASSERT_OK(s);

  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  txn = txn_db->BeginTransaction(write_options);
  txn->Rollback();
  delete txn;

  txn = txn_db->BeginTransaction(write_options);
  s = txn->GetForUpdate(read_options, "aaa", &value);
  ASSERT_EQ(value, "aaa");

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  txn = txn_db->BeginTransaction(write_options);
  txn->SetSnapshot();
  s = txn->GetForUpdate(read_options, "aaa", &value);
  ASSERT_EQ(value, "aaa");

  s = db->Put(write_options, "aaa", "xxx");
  s = txn->Commit();
  ASSERT_NOK(s);
  delete txn;
}

TEST_F(OptimisticTransactionTest, PredicateManyPreceders) {
  WriteOptions write_options;
  ReadOptions read_options1, read_options2;
  OptimisticTransactionOptions txn_options;
  string value;
  Status s;

  txn_options.set_snapshot = true;
  OptimisticTransaction* txn1 =
      txn_db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  OptimisticTransaction* txn2 = txn_db->BeginTransaction(write_options);
  txn2->SetSnapshot();
  read_options2.snapshot = txn2->GetSnapshot();

  std::vector<Slice> multiget_keys = {"1", "2", "3"};
  std::vector<std::string> multiget_values;

  std::vector<Status> results =
      txn1->MultiGetForUpdate(read_options1, multiget_keys, &multiget_values);
  ASSERT_TRUE(results[1].IsNotFound());

  txn2->Put("2", "x");

  s = txn2->Commit();
  ASSERT_OK(s);

  multiget_values.clear();
  results =
      txn1->MultiGetForUpdate(read_options1, multiget_keys, &multiget_values);
  ASSERT_TRUE(results[1].IsNotFound());

  // should not commit since txn2 wrote a key txn has read
  s = txn1->Commit();
  ASSERT_NOK(s);

  delete txn1;
  delete txn2;

  txn1 = txn_db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  txn1->Put("4", "x");

  txn2->Delete("4");

  // txn1 can commit since txn2's delete hasn't happened yet (it's just batched)
  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options2, "4", &value);
  ASSERT_TRUE(s.IsNotFound());

  // txn2 cannot commit since txn1 changed "4"
  s = txn2->Commit();
  ASSERT_NOK(s);

  delete txn1;
  delete txn2;
}

TEST_F(OptimisticTransactionTest, LostUpdate) {
  WriteOptions write_options;
  ReadOptions read_options, read_options1, read_options2;
  OptimisticTransactionOptions txn_options;
  string value;
  Status s;

  // Test 2 transactions writing to the same key in multiple orders and
  // with/without snapshots

  OptimisticTransaction* txn1 = txn_db->BeginTransaction(write_options);
  OptimisticTransaction* txn2 = txn_db->BeginTransaction(write_options);

  txn1->Put("1", "1");
  txn2->Put("1", "2");

  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_NOK(s);

  delete txn1;
  delete txn2;

  txn_options.set_snapshot = true;
  txn1 = txn_db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  txn1->Put("1", "3");
  txn2->Put("1", "4");

  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_NOK(s);

  delete txn1;
  delete txn2;

  txn1 = txn_db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  txn1->Put("1", "5");
  s = txn1->Commit();
  ASSERT_OK(s);

  txn2->Put("1", "6");
  s = txn2->Commit();
  ASSERT_NOK(s);

  delete txn1;
  delete txn2;

  txn1 = txn_db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  txn1->Put("1", "5");
  s = txn1->Commit();
  ASSERT_OK(s);

  txn2->SetSnapshot();
  txn2->Put("1", "6");
  s = txn2->Commit();
  ASSERT_OK(s);

  delete txn1;
  delete txn2;

  txn1 = txn_db->BeginTransaction(write_options);
  txn2 = txn_db->BeginTransaction(write_options);

  txn1->Put("1", "7");
  s = txn1->Commit();
  ASSERT_OK(s);

  txn2->Put("1", "8");
  s = txn2->Commit();
  ASSERT_OK(s);

  delete txn1;
  delete txn2;

  s = db->Get(read_options, "1", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "8");
}

TEST_F(OptimisticTransactionTest, UntrackedWrites) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  // Verify transaction rollback works for untracked keys.
  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  txn->PutUntracked("untracked", "0");
  txn->Rollback();
  s = db->Get(read_options, "untracked", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
  txn = txn_db->BeginTransaction(write_options);

  txn->Put("tracked", "1");
  txn->PutUntracked("untracked", "1");
  txn->MergeUntracked("untracked", "2");
  txn->DeleteUntracked("untracked");

  // Write to the untracked key outside of the transaction and verify
  // it doesn't prevent the transaction from committing.
  s = db->Put(write_options, "untracked", "x");
  ASSERT_OK(s);

  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "untracked", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
  txn = txn_db->BeginTransaction(write_options);

  txn->Put("tracked", "10");
  txn->PutUntracked("untracked", "A");

  // Write to tracked key outside of the transaction and verify that the
  // untracked keys are not written when the commit fails.
  s = db->Delete(write_options, "tracked");

  s = txn->Commit();
  ASSERT_NOK(s);

  s = db->Get(read_options, "untracked", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#endif  // ROCKSDB_LITE
