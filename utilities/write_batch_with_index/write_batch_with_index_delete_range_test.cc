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
#include "port/stack_trace.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "test_util/testharness.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend.h"

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

struct Entry {
  std::string key;
  std::string value;
  WriteType type;
};

struct TestHandler : public WriteBatch::Handler {
  std::map<uint32_t, std::vector<Entry>> seen;
  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override {
    Entry e;
    e.key = key.ToString();
    e.value = value.ToString();
    e.type = kPutRecord;
    seen[column_family_id].push_back(e);
    return Status::OK();
  }
  Status MergeCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override {
    Entry e;
    e.key = key.ToString();
    e.value = value.ToString();
    e.type = kMergeRecord;
    seen[column_family_id].push_back(e);
    return Status::OK();
  }
  void LogData(const Slice& /*blob*/) override {}
  Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
    Entry e;
    e.key = key.ToString();
    e.value = "";
    e.type = kDeleteRecord;
    seen[column_family_id].push_back(e);
    return Status::OK();
  }
};
}  // namespace

class WriteBatchWithIndexTest : public testing::Test {};

namespace {
typedef std::map<std::string, std::string> KVMap;

class KVIter : public Iterator {
 public:
  explicit KVIter(const KVMap* map) : map_(map), iter_(map_->end()) {}
  bool Valid() const override { return iter_ != map_->end(); }
  void SeekToFirst() override { iter_ = map_->begin(); }
  void SeekToLast() override {
    if (map_->empty()) {
      iter_ = map_->end();
    } else {
      iter_ = map_->find(map_->rbegin()->first);
    }
  }
  void Seek(const Slice& k) override {
    iter_ = map_->lower_bound(k.ToString());
  }
  void SeekForPrev(const Slice& k) override {
    iter_ = map_->upper_bound(k.ToString());
    Prev();
  }
  void Next() override { ++iter_; }
  void Prev() override {
    if (iter_ == map_->begin()) {
      iter_ = map_->end();
      return;
    }
    --iter_;
  }

  Slice key() const override { return iter_->first; }
  Slice value() const override { return iter_->second; }
  Status status() const override { return Status::OK(); }

 private:
  const KVMap* const map_;
  KVMap::const_iterator iter_;
};

void AssertIterValue(std::unique_ptr<WBWIIterator>& iter,
                     const std::string& key, const std::string& value) {
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(key, iter->Entry().key.ToString());
  ASSERT_EQ(kPutRecord, iter->Entry().type);
  ASSERT_EQ(value, iter->Entry().value.ToString());
}

void AssertIterType(std::unique_ptr<WBWIIterator>& iter, const std::string& key,
                    WriteType writeType) {
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(key, iter->Entry().key.ToString());
  ASSERT_EQ(writeType, iter->Entry().type);
}

}  // namespace

void AssertKey(std::string key, WBWIIterator* iter) {
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(key, iter->Entry().key.ToString());
}

void AssertValue(std::string value, WBWIIterator* iter) {
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value, iter->Entry().value.ToString());
}

// Tests that we can write to the WBWI while we iterate (from a single thread).
// iteration should see the newest writes
// same thing as above, but testing IteratorWithBase
// stress testing mutations with IteratorWithBase
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
    } else if (e.type == kDeleteRecord) {
      result->append("DEL(");
      result->append(e.key.ToString());
      result->append(")");
    } else {
      assert(e.type == kDeleteRangeRecord);
    }
    result->append("RANGE-DEL(");
    result->append(e.key.ToString());
    result->append(")");

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

TEST_F(WriteBatchWithIndexTest, TestIteraratorOverwriteKeyTrue) {
  ColumnFamilyHandleImplDummy cf1(6, BytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(2, BytewiseComparator());
  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);

  ASSERT_OK(batch.Put(&cf1, "A", "ab"));
  ASSERT_OK(batch.Put(&cf1, "A", "ac"));
  ASSERT_OK(batch.Put(&cf1, "A", "ad"));
  {
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&cf1));

    iter->Seek("A");
    AssertIterValue(iter, "A", "ad");

    iter->SeekToLast();
    AssertIterValue(iter, "A", "ad");
  }

  ASSERT_OK(batch.Delete(&cf1, "A"));
  {
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&cf1));

    iter->Seek("A");
    AssertIterType(iter, "A", kDeleteRecord);

    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_FALSE(iter->Valid());
  }
}

TEST_F(WriteBatchWithIndexTest, TestIteraratorOverwriteKeyFalse) {
  ColumnFamilyHandleImplDummy cf1(6, BytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(2, BytewiseComparator());
  WriteBatchWithIndex batch(BytewiseComparator(), 20, false);

  ASSERT_OK(batch.Put(&cf1, "A", "ab"));
  ASSERT_OK(batch.Put(&cf1, "A", "ac"));
  ASSERT_OK(batch.Put(&cf1, "A", "ad"));
  {
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&cf1));

    iter->Seek("A");
    AssertIterValue(iter, "A", "ab");
    iter->Next();
    AssertIterValue(iter, "A", "ac");
    iter->Next();
    AssertIterValue(iter, "A", "ad");

    iter->Seek("A");
    AssertIterValue(iter, "A", "ab");

    iter->SeekToLast();
    AssertIterValue(iter, "A", "ad");
  }

  ASSERT_OK(batch.Delete(&cf1, "A"));
  {
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&cf1));

    iter->Seek("A");
    AssertIterValue(iter, "A", "ab");
    iter->Next();
    AssertIterValue(iter, "A", "ac");
    iter->Next();
    AssertIterValue(iter, "A", "ad");
    iter->Next();
    AssertIterType(iter, "A", kDeleteRecord);

    iter->SeekToLast();
    AssertIterType(iter, "A", kDeleteRecord);
    iter->Prev();
    AssertIterValue(iter, "A", "ad");
  }
}

TEST_F(WriteBatchWithIndexTest, DeleteRangeTestBatchUnsupportedOption) {
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

TEST_F(WriteBatchWithIndexTest, DeleteRangeTestBatch) {
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

  // TODO - Range deletion doesn't appear in the iterator
  // TODO - so we get ""
  // TODO - iterator just isn't the appropriate answer for overwrite=true DR ?
  // value = PrintContents(&batch, nullptr);
  // ASSERT_EQ("RANGE-DEL(B,C)", value);

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

TEST_F(WriteBatchWithIndexTest, PutDeleteRangePutAgainTest) {
  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);
  Status s;
  std::string value;
  DBOptions db_options;

  ASSERT_EQ(
      "false",
      "Implement this test to check clearing of the in_deleted_range flag");
}

TEST_F(WriteBatchWithIndexTest, DeleteRangeTestBatchAndDB) {
  DB* db;
  Options options;

  options.create_if_missing = true;
  std::string dbname =
      test::PerThreadDBPath("write_batch_with_index_delete_range_test");

  options.merge_operator = MergeOperators::CreateFromStringId("stringappend");

  EXPECT_OK(DestroyDB(dbname, options));
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);

  ReadOptions read_options;
  WriteOptions write_options;

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
  s = batch.GetFromBatch(db_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d", value);
  s = batch.GetFromBatch(db_options, "E", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);
  s = batch.GetFromBatch(db_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(WriteBatchWithIndexTest, DeleteRangeTestDeletedRangeMap) {
  DB* db;
  Options options;

  options.create_if_missing = true;
  std::string dbname =
      test::PerThreadDBPath("write_batch_with_index_deleted_range_map_test");

  options.merge_operator = MergeOperators::CreateFromStringId("stringappend");

  EXPECT_OK(DestroyDB(dbname, options));
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);

  ReadOptions read_options;
  WriteOptions write_options;

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
  // Range deletion and underlying database
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
  s = batch.GetFromBatchAndDB(db, read_options, "C", &value);

  // TODO this fails because the range map is not there yet
  // TODO implement the range map
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

TEST_F(WriteBatchWithIndexTest, DeleteRangeTestBatchX2AndDB) {
  DB* db;
  Options options;

  options.create_if_missing = true;
  std::string dbname =
      test::PerThreadDBPath("write_batch_with_index_delete_range_test_x2");

  options.merge_operator = MergeOperators::CreateFromStringId("stringappend");

  EXPECT_OK(DestroyDB(dbname, options));
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);

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
}

//
// TODO mine this for tests we need to do, then get rid of it
// TODO it is not a systematic part of what we need to test.
//
TEST_F(WriteBatchWithIndexTest, DeleteRangeTestXXX) {
  WriteBatchWithIndex batch;
  Status s;
  std::string value;
  DBOptions db_options;

  value = PrintContents(&batch, nullptr);
  ASSERT_EQ("PUT(A):a,PUT(A):a2,SINGLE-DEL(A),PUT(B):b,", value);

  ASSERT_OK(batch.Put("C", "c"));
  ASSERT_OK(batch.Put("A", "a3"));
  ASSERT_OK(batch.Delete("B"));
  ASSERT_OK(batch.SingleDelete("B"));
  ASSERT_OK(batch.SingleDelete("C"));

  s = batch.GetFromBatch(db_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a3", value);
  s = batch.GetFromBatch(db_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  value = PrintContents(&batch, nullptr);
  ASSERT_EQ(
      "PUT(A):a,PUT(A):a2,SINGLE-DEL(A),PUT(A):a3,PUT(B):b,DEL(B),SINGLE-DEL("
      "B)"
      ",PUT(C):c,SINGLE-DEL(C),",
      value);

  ASSERT_OK(batch.Put("B", "b4"));
  ASSERT_OK(batch.Put("C", "c4"));
  ASSERT_OK(batch.Put("D", "d4"));
  ASSERT_OK(batch.SingleDelete("D"));
  ASSERT_OK(batch.SingleDelete("D"));
  ASSERT_OK(batch.Delete("A"));

  s = batch.GetFromBatch(db_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch.GetFromBatch(db_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b4", value);
  s = batch.GetFromBatch(db_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c4", value);
  s = batch.GetFromBatch(db_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  value = PrintContents(&batch, nullptr);
  ASSERT_EQ(
      "PUT(A):a,PUT(A):a2,SINGLE-DEL(A),PUT(A):a3,DEL(A),PUT(B):b,DEL(B),"
      "SINGLE-DEL(B),PUT(B):b4,PUT(C):c,SINGLE-DEL(C),PUT(C):c4,PUT(D):d4,"
      "SINGLE-DEL(D),SINGLE-DEL(D),",
      value);
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
