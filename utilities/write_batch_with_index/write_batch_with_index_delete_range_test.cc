//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include <map>
#include <memory>

#include "db/column_family.h"
#include "memtable/skiplist.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "test_util/testharness.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {
class ColumnFamilyHandleImplDummy : public ColumnFamilyHandleImpl {
 public:
  explicit ColumnFamilyHandleImplDummy(int id, const Comparator* comparator)
      : ColumnFamilyHandleImpl(nullptr, nullptr, nullptr),
        id_(id),
        comparator_(comparator) {}
  uint32_t GetID() const override { return id_; }
  const Comparator* GetComparator() const override { return comparator_; }

 private:
  uint32_t id_;
  const Comparator* comparator_;
};
}  // namespace

class WriteBatchWithIndexDeleteRangeTest : public testing::Test {};

class TestDB {
 public:
  DB* db;

 private:
  Options options;
  std::string dbname_;

 public:
  TestDB(std::string dbname) : dbname_(test::PerThreadDBPath(dbname)) {
    options.create_if_missing = true;

    EXPECT_OK(DestroyDB(dbname_, options));
    Status s = DB::Open(options, dbname_, &db);
    EXPECT_OK(s);
  }

 public:
  ColumnFamilyHandle* newCF(std::string familyName) {
    ColumnFamilyHandle* cf;
    EXPECT_OK(db->CreateColumnFamily(ColumnFamilyOptions(), familyName, &cf));
    return cf;
  }
};

static std::vector<std::string> GetValuesFromBatch(
    WriteBatchWithIndex* batch, ColumnFamilyHandle* column_family,
    std::vector<std::string> keys) {
  DBOptions db_options;
  std::vector<std::string> result;
  for (std::string key : keys) {
    std::string value;
    Status s = batch->GetFromBatch(column_family, db_options, key, &value);
    if (s.IsNotFound()) {
      result.push_back("" + key + "={}");
    } else {
      result.push_back("" + key + "=" + value);
    }
  }
  return result;
};

static std::vector<std::string> GetValuesFromBatch(
    WriteBatchWithIndex* batch, std::vector<std::string> keys) {
  return GetValuesFromBatch(batch, nullptr, keys);
};

static void PrintContents(WriteBatchWithIndex* batch,
                          ColumnFamilyHandle* column_family,
                          std::string* result) {
  WBWIIterator* iter;
  if (column_family == nullptr) {
    iter = batch->NewIterator();
  } else {
    iter = batch->NewIterator(column_family);
  }

  iter->SeekToFirst();
  while (iter->Valid()) {
    ASSERT_OK(iter->status());

    WriteEntry e = iter->Entry();

    if (e.type == kPutRecord) {
      result->append("PUT(");
      result->append(e.key.ToString());
      result->append("):");
      result->append(e.value.ToString());
    } else if (e.type == kMergeRecord) {
      result->append("MERGE(");
      result->append(e.key.ToString());
      result->append("):");
      result->append(e.value.ToString());
    } else if (e.type == kSingleDeleteRecord) {
      result->append("SINGLE-DEL(");
      result->append(e.key.ToString());
      result->append(")");
    } else {
      assert(e.type == kDeleteRecord);
      result->append("DEL(");
      result->append(e.key.ToString());
      result->append(")");
    }

    result->append(",");
    iter->Next();
  }

  ASSERT_OK(iter->status());

  delete iter;
}

static std::string PrintContents(WriteBatchWithIndex* batch,
                                 ColumnFamilyHandle* column_family) {
  std::string result;
  PrintContents(batch, column_family, &result);
  return result;
}

void AssertKey(std::string key, WBWIIterator* iter) {
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(key, iter->Entry().key.ToString());
}

void AssertValue(std::string value, WBWIIterator* iter) {
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value, iter->Entry().value.ToString());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest,
       DeleteRangeTestBatchUnsupportedOption) {
  WriteBatchWithIndex batch(BytewiseComparator(), 20, false);
  Status s;
  std::string value;
  DBOptions db_options;

  // Delete range overwrite_key=false
  s = batch.DeleteRange("B", "C");
  ASSERT_TRUE(s.IsNotSupported());

  ColumnFamilyHandleImplDummy cf1(6, BytewiseComparator());
  s = batch.DeleteRange(&cf1, "B", "C");
  ASSERT_TRUE(s.IsNotSupported());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, BatchBadRange) {
  TestDB testDB("batch_bad_range");

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  DB* db = testDB.db;
  Status s;
  std::string value;
  DBOptions db_options;

  s = batch.Put("EE", "e0");
  ASSERT_OK(s);
  s = batch.Put("G", "g0");
  ASSERT_OK(s);

  // Delete range overwrite_key=false
  s = batch.DeleteRange("D", "C");
  ASSERT_TRUE(s.IsInvalidArgument());

  s = batch.DeleteRange("E", "E");
  ASSERT_TRUE(s.IsInvalidArgument());
  s = batch.GetFromBatch(db_options, "EE", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e0", value);

  s = batch.DeleteRange("E", "EEEE");
  ASSERT_OK(s);
  s = batch.GetFromBatch(db_options, "EE", &value);
  ASSERT_TRUE(s.IsNotFound());

  WriteOptions write_options;
  ReadOptions read_options;
  value = "";
  s = db->Write(write_options, batch.GetWriteBatch());
  ASSERT_OK(s);

  s = db->Get(read_options, "EE", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = db->Get(read_options, "G", &value);
  ASSERT_OK(s);
  ASSERT_EQ("g0", value);
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, BatchBadRangeCF) {
  TestDB testDB("batch_bad_range_cf");
  DB* db = testDB.db;
  ColumnFamilyHandle* cf1 = testDB.newCF("First Family");

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  Status s;
  std::string value;
  DBOptions db_options;

  s = batch.Put(cf1, "EE", "e0");
  ASSERT_OK(s);
  s = batch.Put(cf1, "G", "g0");
  ASSERT_OK(s);

  // Delete range overwrite_key=false
  s = batch.DeleteRange(cf1, "D", "C");
  ASSERT_TRUE(s.IsInvalidArgument());

  s = batch.DeleteRange(cf1, "E", "E");
  ASSERT_TRUE(s.IsInvalidArgument());
  s = batch.GetFromBatch(cf1, db_options, "EE", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e0", value);

  s = batch.DeleteRange(cf1, "E", "EEEE");
  ASSERT_OK(s);
  s = batch.GetFromBatch(cf1, db_options, "EE", &value);
  ASSERT_TRUE(s.IsNotFound());

  WriteOptions write_options;
  ReadOptions read_options;
  value = "";
  s = db->Write(write_options, batch.GetWriteBatch());
  ASSERT_OK(s);

  s = db->Get(read_options, cf1, "EE", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = db->Get(read_options, cf1, "G", &value);
  ASSERT_OK(s);
  ASSERT_EQ("g0", value);
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, DeleteSingleRange) {
  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  Status s;
  std::string value;
  DBOptions db_options;

  // Delete range with nothing in the range is OK,
  ASSERT_OK(batch.DeleteRange("B", "C"));

  // Read a bunch of values, ensure it's all not there OK
  s = batch.GetFromBatch(db_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  //
  // Simple range deletion in centre of A-E
  //
  batch.Clear();
  ASSERT_OK(batch.Put("A", "a"));
  ASSERT_OK(batch.Put("B", "b"));
  ASSERT_OK(batch.Put("C", "c"));
  ASSERT_OK(batch.Put("D", "d"));
  ASSERT_OK(batch.Put("E", "e"));
  ASSERT_OK(batch.DeleteRange("B", "D"));

  s = batch.GetFromBatch(db_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);
  s = batch.GetFromBatch(db_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d", value);
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);
  s = batch.GetFromBatch(db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, DeleteSingleRangeCF) {
  TestDB testDB("delete_single_range_cf");
  ColumnFamilyHandle* cf1 = testDB.newCF("First Family");

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  Status s;
  std::string value;
  DBOptions db_options;

  // Delete range with nothing in the range is OK,
  ASSERT_OK(batch.DeleteRange(cf1, "B", "C"));

  // Read a bunch of values, ensure it's all not there OK
  s = batch.GetFromBatch(cf1, db_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  //
  // Simple range deletion in centre of A-E
  //
  batch.Clear();
  ASSERT_OK(batch.Put(cf1, "A", "a"));
  ASSERT_OK(batch.Put(cf1, "B", "b"));
  ASSERT_OK(batch.Put(cf1, "C", "c"));
  ASSERT_OK(batch.Put(cf1, "D", "d"));
  ASSERT_OK(batch.Put(cf1, "E", "e"));
  ASSERT_OK(batch.DeleteRange(cf1, "B", "D"));

  s = batch.GetFromBatch(cf1, db_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);
  s = batch.GetFromBatch(cf1, db_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d", value);
  s = batch.GetFromBatch(cf1, db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);
  s = batch.GetFromBatch(cf1, db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, PutDeleteRangePutAgain) {
  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  Status s;
  std::string value;
  DBOptions db_options;

  //
  // Test for a deleted value being properly set by subsequent Put
  //

  ASSERT_OK(batch.Put("C", "c0"));
  s = batch.GetFromBatch(db_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c0", value);
  ASSERT_OK(batch.DeleteRange("B", "D"));
  s = batch.GetFromBatch(db_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_OK(batch.Put("C", "c1"));
  s = batch.GetFromBatch(db_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c1", value);
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, PutDeleteRangePutAgainCF) {
  TestDB testDB("put_delete_range_put_again_cf");
  ColumnFamilyHandle* cf1 = testDB.newCF("First Family");

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  Status s;
  std::string value;
  DBOptions db_options;

  //
  // Test for a deleted value being properly set by subsequent Put
  //

  ASSERT_OK(batch.Put(cf1, "C", "c0"));
  s = batch.GetFromBatch(cf1, db_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c0", value);
  ASSERT_OK(batch.DeleteRange(cf1, "B", "D"));
  s = batch.GetFromBatch(cf1, db_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_OK(batch.Put(cf1, "C", "c1"));
  s = batch.GetFromBatch(cf1, db_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c1", value);
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, DeleteSingleDeleteRangeInteraction) {
  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  Status s;
  std::string value;
  DBOptions db_options;

  //
  // Test mixing delete and delete range
  //

  std::string contents;
  ASSERT_OK(batch.Put("C", "c0"));
  s = batch.GetFromBatch(db_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c0", value);
  contents = PrintContents(&batch, nullptr);
  ASSERT_OK(batch.DeleteRange("A", "M"));
  contents = PrintContents(&batch, nullptr);
  s = batch.GetFromBatch(db_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_OK(batch.Put("E", "e1"));
  contents = PrintContents(&batch, nullptr);
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e1", value);
  ASSERT_OK(batch.Delete("C"));
  s = batch.GetFromBatch(db_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e1", value);
  ASSERT_OK(batch.Delete("E"));
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_OK(batch.Put("E", "e2"));
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e2", value);
  ASSERT_OK(batch.Put("C", "c2"));
  s = batch.GetFromBatch(db_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c2", value);
}

TEST_F(WriteBatchWithIndexDeleteRangeTest,
       DeleteSingleDeleteRangeInteractionCF) {
  TestDB testDB("delete_single_delete_range_interaction_cf");
  ColumnFamilyHandle* cf1 = testDB.newCF("First Family");

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  Status s;
  std::string value;
  DBOptions db_options;

  //
  // Test mixing delete and delete range
  //

  std::string contents;
  ASSERT_OK(batch.Put(cf1, "C", "c0"));
  s = batch.GetFromBatch(cf1, db_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c0", value);
  contents = PrintContents(&batch, cf1);
  ASSERT_OK(batch.DeleteRange(cf1, "A", "M"));
  contents = PrintContents(&batch, cf1);
  s = batch.GetFromBatch(cf1, db_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_OK(batch.Put(cf1, "E", "e1"));
  contents = PrintContents(&batch, cf1);
  s = batch.GetFromBatch(cf1, db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e1", value);
  ASSERT_OK(batch.Delete(cf1, "C"));
  s = batch.GetFromBatch(cf1, db_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e1", value);
  ASSERT_OK(batch.Delete(cf1, "E"));
  s = batch.GetFromBatch(cf1, db_options, "E", &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_OK(batch.Put(cf1, "E", "e2"));
  s = batch.GetFromBatch(cf1, db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e2", value);
  ASSERT_OK(batch.Put(cf1, "C", "c2"));
  s = batch.GetFromBatch(cf1, db_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c2", value);
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, BatchAndDB) {
  TestDB testDB("batch_and_db");
  DB* db = testDB.db;

  ReadOptions read_options;
  WriteOptions write_options;

  Status s;

  s = db->Put(write_options, "A", "a0");
  ASSERT_OK(s);
  s = db->Put(write_options, "B", "b0");
  ASSERT_OK(s);
  s = db->Put(write_options, "BB", "bb0");
  ASSERT_OK(s);
  s = db->Put(write_options, "C", "c0");
  ASSERT_OK(s);

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  //
  // Range deletion and underlying database
  //
  batch.Clear();
  ASSERT_OK(batch.Put("B", "b"));
  ASSERT_OK(batch.Put("D", "d"));
  ASSERT_OK(batch.Put("E", "e"));
  ASSERT_OK(batch.DeleteRange("B", "D"));
  db->Write(write_options, batch.GetWriteBatch());

  s = batch.GetFromBatchAndDB(db, read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);
  s = batch.GetFromBatch(db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d", value);
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);
  s = batch.GetFromBatch(db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, BatchAndDBCF) {
  TestDB testDB("batch_and_db_cf");
  DB* db = testDB.db;
  ColumnFamilyHandle* cf1 = testDB.newCF("First Family");

  ReadOptions read_options;
  WriteOptions write_options;

  Status s;

  s = db->Put(write_options, cf1, "A", "a0");
  ASSERT_OK(s);
  s = db->Put(write_options, cf1, "B", "b0");
  ASSERT_OK(s);
  s = db->Put(write_options, cf1, "BB", "bb0");
  ASSERT_OK(s);
  s = db->Put(write_options, cf1, "C", "c0");
  ASSERT_OK(s);

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  //
  // Range deletion and underlying database
  //
  batch.Clear();
  ASSERT_OK(batch.Put(cf1, "B", "b"));
  ASSERT_OK(batch.Put(cf1, "D", "d"));
  ASSERT_OK(batch.Put(cf1, "E", "e"));
  ASSERT_OK(batch.DeleteRange(cf1, "B", "D"));
  db->Write(write_options, batch.GetWriteBatch());

  s = batch.GetFromBatchAndDB(db, read_options, cf1, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);
  s = batch.GetFromBatch(cf1, db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d", value);
  s = batch.GetFromBatch(cf1, db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);
  s = batch.GetFromBatch(cf1, db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, DeletedRangeRemembered) {
  TestDB testDB("deleted_range_remembered");
  DB* db = testDB.db;
  Status s;

  ReadOptions read_options;
  WriteOptions write_options;

  //
  // Set up the underlying DB
  //
  s = db->Put(write_options, "A", "a0");
  ASSERT_OK(s);
  s = db->Put(write_options, "B", "b0");
  ASSERT_OK(s);
  s = db->Put(write_options, "C", "c0");
  ASSERT_OK(s);

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  //
  // Range deletion using the batch
  // Check get with batch and underlying database
  //
  batch.Clear();
  ASSERT_OK(batch.Put("B", "b"));
  ASSERT_OK(batch.Put("D", "d"));
  ASSERT_OK(batch.Put("E", "e"));
  ASSERT_OK(batch.DeleteRange("B", "D"));

  s = batch.GetFromBatchAndDB(db, read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  // This checks the range map recording explicit deletion
  // "deletes" the C in the underlying database
  s = batch.GetFromBatchAndDB(db, read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = batch.GetFromBatch(db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d", value);
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);
  s = batch.GetFromBatch(db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, DeletedRangeRememberedCF) {
  TestDB testDB("deleted_range_remembered_cf");
  ColumnFamilyHandle* cf1 = testDB.newCF("First Family");
  DB* db = testDB.db;
  Status s;

  ReadOptions read_options;
  WriteOptions write_options;

  //
  // Set up the underlying DB
  //
  s = db->Put(write_options, cf1, "A", "a0");
  ASSERT_OK(s);
  s = db->Put(write_options, cf1, "B", "b0");
  ASSERT_OK(s);
  s = db->Put(write_options, cf1, "C", "c0");
  ASSERT_OK(s);

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  //
  // Range deletion using the batch
  // Check get with batch and underlying database
  //
  batch.Clear();
  ASSERT_OK(batch.Put(cf1, "B", "b"));
  ASSERT_OK(batch.Put(cf1, "D", "d"));
  ASSERT_OK(batch.Put(cf1, "E", "e"));
  ASSERT_OK(batch.DeleteRange(cf1, "B", "D"));

  s = batch.GetFromBatchAndDB(db, read_options, cf1, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  // This checks the range map recording explicit deletion
  // "deletes" the C in the underlying database
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "C", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = batch.GetFromBatch(cf1, db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d", value);
  s = batch.GetFromBatch(cf1, db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);
  s = batch.GetFromBatch(cf1, db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, RollbackDeleteRange) {
  TestDB testDB("rollback_delete_range");
  DB* db = testDB.db;
  Status s;

  ReadOptions read_options;
  WriteOptions write_options;

  //
  // Set up the underlying DB
  //
  s = db->Put(write_options, "A", "a0");
  ASSERT_OK(s);
  s = db->Put(write_options, "B", "b0");
  ASSERT_OK(s);
  s = db->Put(write_options, "C", "c0");
  ASSERT_OK(s);

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  //
  // Range deletion using the batch
  // Check get with batch and underlying database
  //
  batch.Clear();
  ASSERT_OK(batch.Put("B", "b"));
  ASSERT_OK(batch.Put("D", "d"));
  ASSERT_OK(batch.Put("E", "e"));

  batch.SetSavePoint();
  ASSERT_OK(batch.Put("B", "b2"));
  ASSERT_OK(batch.Put("CC", "cc2"));
  ASSERT_OK(batch.Put("D", "d2"));
  ASSERT_OK(batch.Put("E", "e2"));
  ASSERT_OK(batch.DeleteRange("B", "D"));

  s = batch.GetFromBatchAndDB(db, read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  // This checks the range map recording explicit deletion
  // "deletes" the C in the underlying database
  s = batch.GetFromBatchAndDB(db, read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = batch.GetFromBatchAndDB(db, read_options, "CC", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d2", value);
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e2", value);
  s = batch.GetFromBatch(db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch.RollbackToSavePoint());

  // Check the deleted range [B,D) is no longer deleted
  // along with everything else being rolled back to the SP

  s = batch.GetFromBatchAndDB(db, read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);
  s = batch.GetFromBatchAndDB(db, read_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c0", value);
  s = batch.GetFromBatchAndDB(db, read_options, "CC", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d", value);
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);
  s = batch.GetFromBatch(db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, RollbackDeleteRangeCF) {
  TestDB testDB("rollback_delete_range_cf");
  ColumnFamilyHandle* cf1 = testDB.newCF("First Family");
  DB* db = testDB.db;
  Status s;

  ReadOptions read_options;
  WriteOptions write_options;

  //
  // Set up the underlying DB
  //
  s = db->Put(write_options, cf1, "A", "a0");
  ASSERT_OK(s);
  s = db->Put(write_options, cf1, "B", "b0");
  ASSERT_OK(s);
  s = db->Put(write_options, cf1, "C", "c0");
  ASSERT_OK(s);

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  //
  // Range deletion using the batch
  // Check get with batch and underlying database
  //
  batch.Clear();
  ASSERT_OK(batch.Put(cf1, "B", "b"));
  ASSERT_OK(batch.Put(cf1, "D", "d"));
  ASSERT_OK(batch.Put(cf1, "E", "e"));

  batch.SetSavePoint();
  ASSERT_OK(batch.Put(cf1, "B", "b2"));
  ASSERT_OK(batch.Put(cf1, "CC", "cc2"));
  ASSERT_OK(batch.Put(cf1, "D", "d2"));
  ASSERT_OK(batch.Put(cf1, "E", "e2"));
  ASSERT_OK(batch.DeleteRange(cf1, "B", "D"));

  s = batch.GetFromBatchAndDB(db, read_options, cf1, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  // This checks the range map recording explicit deletion
  // "deletes" the C in the underlying database
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "C", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = batch.GetFromBatchAndDB(db, read_options, cf1, "CC", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d2", value);
  s = batch.GetFromBatch(cf1, db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e2", value);
  s = batch.GetFromBatch(cf1, db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch.RollbackToSavePoint());

  // Check the deleted range [B,D) is no longer deleted
  // along with everything else being rolled back to the SP

  s = batch.GetFromBatchAndDB(db, read_options, cf1, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "CC", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d", value);
  s = batch.GetFromBatch(cf1, db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);
  s = batch.GetFromBatch(cf1, db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, RedoDeleteRange) {
  TestDB testDB("redo_delete_range");
  DB* db = testDB.db;
  Status s;

  ReadOptions read_options;
  WriteOptions write_options;

  //
  // Set up the underlying DB
  //
  s = db->Put(write_options, "A", "a0");
  ASSERT_OK(s);
  s = db->Put(write_options, "B", "b0");
  ASSERT_OK(s);
  s = db->Put(write_options, "C", "c0");
  ASSERT_OK(s);

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  //
  // Range deletion using the batch
  // Check get with batch and underlying database
  //
  batch.Clear();
  ASSERT_OK(batch.Put("B", "b2"));
  ASSERT_OK(batch.Put("CC", "cc2"));
  ASSERT_OK(batch.Put("D", "d2"));
  ASSERT_OK(batch.Put("E", "e2"));
  ASSERT_OK(batch.DeleteRange("B", "D"));
  ASSERT_OK(batch.Put("CCC", "ccc2"));

  s = batch.GetFromBatchAndDB(db, read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  // This checks the range map recording explicit deletion
  // "deletes" the C in the underlying database
  s = batch.GetFromBatchAndDB(db, read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "CC", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d2", value);
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e2", value);
  s = batch.GetFromBatch(db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
  // Check the write *after* the Delete Range is still there
  s = batch.GetFromBatchAndDB(db, read_options, "CCC", &value);
  ASSERT_OK(s);
  ASSERT_EQ("ccc2", value);

  // We check that redo rolls the delete range forward to here
  batch.SetSavePoint();
  ASSERT_OK(batch.Put("CC", "cc3"));

  // Check the deleted range [B,D) is deleted again
  // along with everything else being rolled back to the SP
  ASSERT_OK(batch.RollbackToSavePoint());

  s = batch.GetFromBatchAndDB(db, read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "CC", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d2", value);
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e2", value);
  s = batch.GetFromBatch(db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "CCC", &value);
  ASSERT_OK(s);
  ASSERT_EQ("ccc2", value);
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, RedoDeleteRangeCF) {
  TestDB testDB("redo_delete_range_cf");
  ColumnFamilyHandle* cf1 = testDB.newCF("First Family");
  DB* db = testDB.db;
  Status s;

  ReadOptions read_options;
  WriteOptions write_options;

  //
  // Set up the underlying DB
  //
  s = db->Put(write_options, cf1, "A", "a0");
  ASSERT_OK(s);
  s = db->Put(write_options, cf1, "B", "b0");
  ASSERT_OK(s);
  s = db->Put(write_options, cf1, "C", "c0");
  ASSERT_OK(s);

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  //
  // Range deletion using the batch
  // Check get with batch and underlying database
  //
  batch.Clear();
  ASSERT_OK(batch.Put(cf1, "B", "b2"));
  ASSERT_OK(batch.Put(cf1, "CC", "cc2"));
  ASSERT_OK(batch.Put(cf1, "D", "d2"));
  ASSERT_OK(batch.Put(cf1, "E", "e2"));
  ASSERT_OK(batch.DeleteRange(cf1, "B", "D"));
  ASSERT_OK(batch.Put(cf1, "CCC", "ccc2"));

  s = batch.GetFromBatchAndDB(db, read_options, cf1, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  // This checks the range map recording explicit deletion
  // "deletes" the C in the underlying database
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "CC", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d2", value);
  s = batch.GetFromBatch(cf1, db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e2", value);
  s = batch.GetFromBatch(cf1, db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
  // Check the write *after* the Delete Range is still there
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "CCC", &value);
  ASSERT_OK(s);
  ASSERT_EQ("ccc2", value);

  // We check that redo rolls the delete range forward to here
  batch.SetSavePoint();
  ASSERT_OK(batch.Put(cf1, "CC", "cc3"));

  // Check the deleted range [B,D) is deleted again
  // along with everything else being rolled back to the SP
  ASSERT_OK(batch.RollbackToSavePoint());

  s = batch.GetFromBatchAndDB(db, read_options, cf1, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "CC", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d2", value);
  s = batch.GetFromBatch(cf1, db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e2", value);
  s = batch.GetFromBatch(cf1, db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "CCC", &value);
  ASSERT_OK(s);
  ASSERT_EQ("ccc2", value);
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, MultipleRanges) {
  TestDB testDB("multiple_ranges");
  DB* db = testDB.db;
  Status s;

  ReadOptions read_options;
  WriteOptions write_options;

  //
  // Set up the underlying DB
  //
  s = db->Put(write_options, "D", "d0");
  ASSERT_OK(s);

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  //
  // Range deletion using the batch
  // Check get with batch and underlying database
  //
  batch.Clear();
  ASSERT_OK(batch.DeleteRange("B", "C"));
  ASSERT_OK(batch.DeleteRange("F", "G"));

  s = batch.GetFromBatchAndDB(db, read_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d0", value);

  ASSERT_OK(batch.DeleteRange("A", "H"));

  s = batch.GetFromBatchAndDB(db, read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, MultipleRangesCF) {
  TestDB testDB("multiple_ranges_cf");
  ColumnFamilyHandle* cf1 = testDB.newCF("First Family");
  DB* db = testDB.db;
  Status s;

  ReadOptions read_options;
  WriteOptions write_options;

  //
  // Set up the underlying DB
  //
  s = db->Put(write_options, cf1, "D", "d0");
  ASSERT_OK(s);

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  //
  // Range deletion using the batch
  // Check get with batch and underlying database
  //
  batch.Clear();
  ASSERT_OK(batch.DeleteRange(cf1, "B", "C"));
  ASSERT_OK(batch.DeleteRange(cf1, "F", "G"));

  s = batch.GetFromBatchAndDB(db, read_options, cf1, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d0", value);

  ASSERT_OK(batch.DeleteRange(cf1, "A", "H"));

  s = batch.GetFromBatchAndDB(db, read_options, cf1, "D", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, MoreRanges) {
  ReadOptions read_options;
  WriteOptions write_options;

  //
  // Set up
  //
  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  std::vector<std::string> entries;
  std::vector<std::string> expect;

  std::vector<std::string> all_keys = {"A",  "B",  "BA", "BB", "BC", "BD", "BE",
                                       "C",  "CA", "CB", "CC", "D",  "DA", "DB",
                                       "DC", "DD", "E",  "EA", "EB", "EF", "EG",
                                       "F",  "FA", "FB", "G",  "GA", "GB"};
  expect = {"A=a", "B={}", "C=c"};

  ASSERT_OK(batch.Put("A", "a"));
  ASSERT_OK(batch.Put("B", "b"));
  ASSERT_OK(batch.Put("BA", "ba"));
  ASSERT_OK(batch.Put("BB", "bb"));
  ASSERT_OK(batch.Put("BC", "bc"));
  ASSERT_OK(batch.Put("BD", "bd"));
  ASSERT_OK(batch.Put("BE", "be"));
  ASSERT_OK(batch.Put("C", "c"));
  ASSERT_OK(batch.Put("CA", "ca"));
  ASSERT_OK(batch.Put("CB", "cb"));
  ASSERT_OK(batch.Put("CC", "cc"));
  ASSERT_OK(batch.Put("D", "d"));
  ASSERT_OK(batch.Put("DA", "da"));
  ASSERT_OK(batch.Put("DB", "db"));
  ASSERT_OK(batch.Put("DC", "dc"));
  ASSERT_OK(batch.Put("DD", "dd"));
  ASSERT_OK(batch.Put("E", "e"));
  ASSERT_OK(batch.Put("EA", "ea"));
  ASSERT_OK(batch.Put("EB", "eb"));
  ASSERT_OK(batch.Put("G", "g"));
  ASSERT_OK(batch.Put("GA", "ga"));
  ASSERT_OK(batch.Put("GB", "gb"));

  ASSERT_OK(batch.DeleteRange("B", "BE"));
  ASSERT_OK(batch.DeleteRange("D", "DE"));

  entries = GetValuesFromBatch(&batch, all_keys);
  expect = {"A=a",   "B={}",  "BA={}", "BB={}", "BC={}", "BD={}", "BE=be",
            "C=c",   "CA=ca", "CB=cb", "CC=cc", "D={}",  "DA={}", "DB={}",
            "DC={}", "DD={}", "E=e",   "EA=ea", "EB=eb", "EF={}", "EG={}",
            "F={}",  "FA={}", "FB={}", "G=g",   "GA=ga", "GB=gb"};
  ASSERT_EQ(expect, entries);

  ASSERT_OK(batch.Put("F", "f"));
  ASSERT_OK(batch.Put("FA", "fa"));
  ASSERT_OK(batch.Put("FB", "fb"));

  ASSERT_OK(batch.DeleteRange("BC", "DC"));

  ASSERT_OK(batch.DeleteRange("DA", "F"));

  entries = GetValuesFromBatch(&batch, all_keys);
  expect = {"A=a",   "B={}",  "BA={}", "BB={}", "BC={}", "BD={}", "BE={}",
            "C={}",  "CA={}", "CB={}", "CC={}", "D={}",  "DA={}", "DB={}",
            "DC={}", "DD={}", "E={}",  "EA={}", "EB={}", "EF={}", "EG={}",
            "F=f",   "FA=fa", "FB=fb", "G=g",   "GA=ga", "GB=gb"};
  ASSERT_EQ(expect, entries);

  ASSERT_OK(batch.DeleteRange("BC", "G"));

  entries = GetValuesFromBatch(&batch, all_keys);
  expect = {"A=a",   "B={}",  "BA={}", "BB={}", "BC={}", "BD={}", "BE={}",
            "C={}",  "CA={}", "CB={}", "CC={}", "D={}",  "DA={}", "DB={}",
            "DC={}", "DD={}", "E={}",  "EA={}", "EB={}", "EF={}", "EG={}",
            "F={}",  "FA={}", "FB={}", "G=g",   "GA=ga", "GB=gb"};
  ASSERT_EQ(expect, entries);

  ASSERT_OK(batch.Put("C", "c2"));
  ASSERT_OK(batch.Put("CA", "ca2"));
  ASSERT_OK(batch.Put("CC", "cc2"));
  ASSERT_OK(batch.Put("D", "d2"));
  ASSERT_OK(batch.Put("DA", "da2"));
  ASSERT_OK(batch.Put("DC", "dc2"));
  ASSERT_OK(batch.Put("DD", "dd2"));
  ASSERT_OK(batch.Put("E", "e2"));
  ASSERT_OK(batch.Put("EF", "ef2"));
  ASSERT_OK(batch.Put("EG", "eg2"));
  ASSERT_OK(batch.Put("F", "f2"));
  ASSERT_OK(batch.Put("FB", "fb2"));
  ASSERT_OK(batch.Put("GA", "ga2"));
  ASSERT_OK(batch.Put("GB", "gb2"));

  entries = GetValuesFromBatch(&batch, all_keys);
  expect = {"A=a",    "B={}",   "BA={}",  "BB={}",  "BC={}",  "BD={}",
            "BE={}",  "C=c2",   "CA=ca2", "CB={}",  "CC=cc2", "D=d2",
            "DA=da2", "DB={}",  "DC=dc2", "DD=dd2", "E=e2",   "EA={}",
            "EB={}",  "EF=ef2", "EG=eg2", "F=f2",   "FA={}",  "FB=fb2",
            "G=g",    "GA=ga2", "GB=gb2"};
  ASSERT_EQ(expect, entries);
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, MoreRangesCF) {
  TestDB testDB("more_ranges_cf");
  ColumnFamilyHandle* cf1 = testDB.newCF("First Family");

  ReadOptions read_options;
  WriteOptions write_options;

  //
  // Set up
  //
  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  std::vector<std::string> entries;
  std::vector<std::string> expect;

  std::vector<std::string> all_keys = {"A",  "B",  "BA", "BB", "BC", "BD", "BE",
                                       "C",  "CA", "CB", "CC", "D",  "DA", "DB",
                                       "DC", "DD", "E",  "EA", "EB", "EF", "EG",
                                       "F",  "FA", "FB", "G",  "GA", "GB"};
  expect = {"A=a", "B={}", "C=c"};

  ASSERT_OK(batch.Put(cf1, "A", "a"));
  ASSERT_OK(batch.Put(cf1, "B", "b"));
  ASSERT_OK(batch.Put(cf1, "BA", "ba"));
  ASSERT_OK(batch.Put(cf1, "BB", "bb"));
  ASSERT_OK(batch.Put(cf1, "BC", "bc"));
  ASSERT_OK(batch.Put(cf1, "BD", "bd"));
  ASSERT_OK(batch.Put(cf1, "BE", "be"));
  ASSERT_OK(batch.Put(cf1, "C", "c"));
  ASSERT_OK(batch.Put(cf1, "CA", "ca"));
  ASSERT_OK(batch.Put(cf1, "CB", "cb"));
  ASSERT_OK(batch.Put(cf1, "CC", "cc"));
  ASSERT_OK(batch.Put(cf1, "D", "d"));
  ASSERT_OK(batch.Put(cf1, "DA", "da"));
  ASSERT_OK(batch.Put(cf1, "DB", "db"));
  ASSERT_OK(batch.Put(cf1, "DC", "dc"));
  ASSERT_OK(batch.Put(cf1, "DD", "dd"));
  ASSERT_OK(batch.Put(cf1, "E", "e"));
  ASSERT_OK(batch.Put(cf1, "EA", "ea"));
  ASSERT_OK(batch.Put(cf1, "EB", "eb"));
  ASSERT_OK(batch.Put(cf1, "G", "g"));
  ASSERT_OK(batch.Put(cf1, "GA", "ga"));
  ASSERT_OK(batch.Put(cf1, "GB", "gb"));

  ASSERT_OK(batch.DeleteRange(cf1, "B", "BE"));
  ASSERT_OK(batch.DeleteRange(cf1, "D", "DE"));

  entries = GetValuesFromBatch(&batch, cf1, all_keys);
  expect = {"A=a",   "B={}",  "BA={}", "BB={}", "BC={}", "BD={}", "BE=be",
            "C=c",   "CA=ca", "CB=cb", "CC=cc", "D={}",  "DA={}", "DB={}",
            "DC={}", "DD={}", "E=e",   "EA=ea", "EB=eb", "EF={}", "EG={}",
            "F={}",  "FA={}", "FB={}", "G=g",   "GA=ga", "GB=gb"};
  ASSERT_EQ(expect, entries);

  ASSERT_OK(batch.Put(cf1, "F", "f"));
  ASSERT_OK(batch.Put(cf1, "FA", "fa"));
  ASSERT_OK(batch.Put(cf1, "FB", "fb"));

  ASSERT_OK(batch.DeleteRange(cf1, "BC", "DC"));

  ASSERT_OK(batch.DeleteRange(cf1, "DA", "F"));

  entries = GetValuesFromBatch(&batch, cf1, all_keys);
  expect = {"A=a",   "B={}",  "BA={}", "BB={}", "BC={}", "BD={}", "BE={}",
            "C={}",  "CA={}", "CB={}", "CC={}", "D={}",  "DA={}", "DB={}",
            "DC={}", "DD={}", "E={}",  "EA={}", "EB={}", "EF={}", "EG={}",
            "F=f",   "FA=fa", "FB=fb", "G=g",   "GA=ga", "GB=gb"};
  ASSERT_EQ(expect, entries);

  ASSERT_OK(batch.DeleteRange(cf1, "BC", "G"));

  entries = GetValuesFromBatch(&batch, cf1, all_keys);
  expect = {"A=a",   "B={}",  "BA={}", "BB={}", "BC={}", "BD={}", "BE={}",
            "C={}",  "CA={}", "CB={}", "CC={}", "D={}",  "DA={}", "DB={}",
            "DC={}", "DD={}", "E={}",  "EA={}", "EB={}", "EF={}", "EG={}",
            "F={}",  "FA={}", "FB={}", "G=g",   "GA=ga", "GB=gb"};
  ASSERT_EQ(expect, entries);

  ASSERT_OK(batch.Put(cf1, "C", "c2"));
  ASSERT_OK(batch.Put(cf1, "CA", "ca2"));
  ASSERT_OK(batch.Put(cf1, "CC", "cc2"));
  ASSERT_OK(batch.Put(cf1, "D", "d2"));
  ASSERT_OK(batch.Put(cf1, "DA", "da2"));
  ASSERT_OK(batch.Put(cf1, "DC", "dc2"));
  ASSERT_OK(batch.Put(cf1, "DD", "dd2"));
  ASSERT_OK(batch.Put(cf1, "E", "e2"));
  ASSERT_OK(batch.Put(cf1, "EF", "ef2"));
  ASSERT_OK(batch.Put(cf1, "EG", "eg2"));
  ASSERT_OK(batch.Put(cf1, "F", "f2"));
  ASSERT_OK(batch.Put(cf1, "FB", "fb2"));
  ASSERT_OK(batch.Put(cf1, "GA", "ga2"));
  ASSERT_OK(batch.Put(cf1, "GB", "gb2"));

  entries = GetValuesFromBatch(&batch, cf1, all_keys);
  expect = {"A=a",    "B={}",   "BA={}",  "BB={}",  "BC={}",  "BD={}",
            "BE={}",  "C=c2",   "CA=ca2", "CB={}",  "CC=cc2", "D=d2",
            "DA=da2", "DB={}",  "DC=dc2", "DD=dd2", "E=e2",   "EA={}",
            "EB={}",  "EF=ef2", "EG=eg2", "F=f2",   "FA={}",  "FB=fb2",
            "G=g",    "GA=ga2", "GB=gb2"};
  ASSERT_EQ(expect, entries);
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, BatchFlushDBRead) {
  TestDB testDB("batch_flush_db_read");
  DB* db = testDB.db;
  Status s;

  ReadOptions read_options;
  WriteOptions write_options;

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  batch.Clear();
  ASSERT_OK(batch.Put("A", "a0"));
  ASSERT_OK(batch.Put("B", "b0"));
  ASSERT_OK(batch.Put("C", "c0"));
  ASSERT_OK(batch.DeleteRange("B", "D"));
  db->Write(write_options, batch.GetWriteBatch());

  //
  // Check nothing is in the flushed batch
  //
  batch.Clear();
  s = batch.GetFromBatch(db_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());

  //
  // Check the Put(s) and DeleteRange(s) got into the DB
  //
  s = batch.GetFromBatchAndDB(db, read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());

  //
  // Start a new batch
  // Check GetFromBatchAndDB gets from DB where there's a value there
  //
  batch.Clear();
  ASSERT_OK(batch.Put("B", "b"));
  ASSERT_OK(batch.Put("D", "d"));
  ASSERT_OK(batch.Put("E", "e"));
  ASSERT_OK(batch.DeleteRange("B", "D"));
  db->Write(write_options, batch.GetWriteBatch());

  // Do the same set of checks twice,
  // second time clear the batch (which has already been written).
  //
  s = batch.GetFromBatchAndDB(db, read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d", value);
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);
  s = batch.GetFromBatch(db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());

  batch.Clear();

  s = batch.GetFromBatchAndDB(db, read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, BatchFlushDBReadCF) {
  TestDB testDB("batch_flush_db_read_cf");
  ColumnFamilyHandle* cf1 = testDB.newCF("First Family");
  DB* db = testDB.db;
  Status s;

  ReadOptions read_options;
  WriteOptions write_options;

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  std::string value;
  DBOptions db_options;

  batch.Clear();
  ASSERT_OK(batch.Put(cf1, "A", "a0"));
  ASSERT_OK(batch.Put(cf1, "B", "b0"));
  ASSERT_OK(batch.Put(cf1, "C", "c0"));
  ASSERT_OK(batch.DeleteRange(cf1, "B", "D"));
  db->Write(write_options, batch.GetWriteBatch());

  //
  // Check nothing is in the flushed batch
  //
  batch.Clear();
  s = batch.GetFromBatch(cf1, db_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());

  //
  // Check the Put(s) and DeleteRange(s) got into the DB
  //
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "C", &value);
  ASSERT_TRUE(s.IsNotFound());

  //
  // Start a new batch
  // Check GetFromBatchAndDB gets from DB where there's a value there
  //
  batch.Clear();
  ASSERT_OK(batch.Put(cf1, "B", "b"));
  ASSERT_OK(batch.Put(cf1, "D", "d"));
  ASSERT_OK(batch.Put(cf1, "E", "e"));
  ASSERT_OK(batch.DeleteRange(cf1, "B", "D"));
  db->Write(write_options, batch.GetWriteBatch());

  // Do the same set of checks twice,
  // second time clear the batch (which has already been written).
  //
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d", value);
  s = batch.GetFromBatch(cf1, db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);
  s = batch.GetFromBatch(cf1, db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());

  batch.Clear();

  s = batch.GetFromBatchAndDB(db, read_options, cf1, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "E", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(cf1, db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(WriteBatchWithIndexDeleteRangeTest, MultipleColumnFamilies) {
  TestDB testDB("multiple_column_families");
  DB* db = testDB.db;
  ColumnFamilyHandle* cf1 = testDB.newCF("First Family");
  ColumnFamilyHandle* cf2 = testDB.newCF("Second Family");

  Status s;

  ReadOptions read_options;
  WriteOptions write_options;

  std::string value;
  DBOptions db_options;

  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);

  //
  // Set up the underlying DB
  //
  ASSERT_OK(batch.Put(cf1, "A", "a_cf1_0"));
  ASSERT_OK(batch.Put(cf2, "A", "a_cf2_0"));
  ASSERT_OK(batch.Put("A", "a_cf0_0"));
  ASSERT_OK(batch.Put(cf1, "Z", "z_cf1_0"));
  ASSERT_OK(batch.Put(cf2, "Z", "z_cf2_0"));
  ASSERT_OK(batch.Put("Z", "z_cf0_0"));
  s = db->Write(write_options, batch.GetWriteBatch());
  ASSERT_OK(s);
  batch.Clear();

  s = batch.GetFromBatchAndDB(db, read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a_cf0_0", value);

  ASSERT_OK(batch.Put(cf1, "A", "a_cf1"));
  ASSERT_OK(batch.Put(cf2, "A", "a_cf2"));
  ASSERT_OK(batch.Put("A", "a_cf0"));
  ASSERT_OK(batch.Put(cf1, "B", "b_cf1"));
  ASSERT_OK(batch.Put(cf2, "B", "b_cf2"));
  ASSERT_OK(batch.Put("B", "b_cf0"));

  ASSERT_OK(batch.DeleteRange("A", "M"));
  ASSERT_OK(batch.DeleteRange(cf2, "N", "ZZ"));

  s = batch.GetFromBatchAndDB(db, read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "Z", &value);
  ASSERT_OK(s);
  ASSERT_EQ("z_cf0_0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a_cf1", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b_cf1", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "Z", &value);
  ASSERT_OK(s);
  ASSERT_EQ("z_cf1_0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf2, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a_cf2", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf2, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b_cf2", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf2, "Z", &value);
  ASSERT_TRUE(s.IsNotFound());

  // We will re-check these values when we roll back
  batch.SetSavePoint();

  // Make some changes on top of the savepoint
  ASSERT_OK(batch.Put(cf1, "A", "a_cf1_2"));
  ASSERT_OK(batch.Put(cf2, "A", "a_cf2_2"));
  ASSERT_OK(batch.Put("A", "a_cf0_2"));
  ASSERT_OK(batch.Put(cf1, "B", "b_cf1_2"));
  ASSERT_OK(batch.Put(cf2, "B", "b_cf2_2"));
  ASSERT_OK(batch.Put("B", "b_cf0_2"));

  ASSERT_OK(batch.DeleteRange("A", "M"));
  ASSERT_OK(batch.DeleteRange(cf1, "N", "ZZ"));

  s = batch.GetFromBatchAndDB(db, read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "Z", &value);
  ASSERT_OK(s);
  ASSERT_EQ("z_cf0_0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a_cf1_2", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b_cf1_2", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "Z", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, cf2, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a_cf2_2", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf2, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b_cf2_2", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf2, "Z", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Roll back, and do the original checks
  batch.RollbackToSavePoint();

  s = batch.GetFromBatchAndDB(db, read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatchAndDB(db, read_options, "Z", &value);
  ASSERT_OK(s);
  ASSERT_EQ("z_cf0_0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a_cf1", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b_cf1", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf1, "Z", &value);
  ASSERT_OK(s);
  ASSERT_EQ("z_cf1_0", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf2, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a_cf2", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf2, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b_cf2", value);
  s = batch.GetFromBatchAndDB(db, read_options, cf2, "Z", &value);
  ASSERT_TRUE(s.IsNotFound());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main() {
  fprintf(stderr, "SKIPPED\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
