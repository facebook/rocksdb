//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/write_batch_with_index.h"

#include <map>
#include <memory>

#include "db/column_family.h"
#include "port/stack_trace.h"
#include "test_util/testharness.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend.h"
#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

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

using KVMap = std::map<std::string, std::string>;

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

static std::string PrintContents(WriteBatchWithIndex* batch,
                                 ColumnFamilyHandle* column_family) {
  std::string result;

  WBWIIterator* iter;
  if (column_family == nullptr) {
    iter = batch->NewIterator();
  } else {
    iter = batch->NewIterator(column_family);
  }

  iter->SeekToFirst();
  while (iter->Valid()) {
    WriteEntry e = iter->Entry();

    if (e.type == kPutRecord) {
      result.append("PUT(");
      result.append(e.key.ToString());
      result.append("):");
      result.append(e.value.ToString());
    } else if (e.type == kMergeRecord) {
      result.append("MERGE(");
      result.append(e.key.ToString());
      result.append("):");
      result.append(e.value.ToString());
    } else if (e.type == kSingleDeleteRecord) {
      result.append("SINGLE-DEL(");
      result.append(e.key.ToString());
      result.append(")");
    } else {
      assert(e.type == kDeleteRecord);
      result.append("DEL(");
      result.append(e.key.ToString());
      result.append(")");
    }

    result.append(",");
    iter->Next();
  }

  delete iter;
  return result;
}

static std::string PrintContents(WriteBatchWithIndex* batch, KVMap* base_map,
                                 ColumnFamilyHandle* column_family) {
  std::string result;

  Iterator* iter;
  if (column_family == nullptr) {
    iter = batch->NewIteratorWithBase(new KVIter(base_map));
  } else {
    iter = batch->NewIteratorWithBase(column_family, new KVIter(base_map));
  }

  iter->SeekToFirst();
  while (iter->Valid()) {
    assert(iter->status().ok());

    Slice key = iter->key();
    Slice value = iter->value();

    result.append(key.ToString());
    result.append(":");
    result.append(value.ToString());
    result.append(",");

    iter->Next();
  }

  delete iter;
  return result;
}

void AssertIter(Iterator* iter, const std::string& key,
                const std::string& value) {
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(key, iter->key().ToString());
  ASSERT_EQ(value, iter->value().ToString());
}

void AssertItersMatch(Iterator* iter1, Iterator* iter2) {
  ASSERT_EQ(iter1->Valid(), iter2->Valid());
  if (iter1->Valid()) {
    ASSERT_EQ(iter1->key().ToString(), iter2->key().ToString());
    ASSERT_EQ(iter1->value().ToString(), iter2->value().ToString());
  }
}

void AssertItersEqual(Iterator* iter1, Iterator* iter2) {
  iter1->SeekToFirst();
  iter2->SeekToFirst();
  while (iter1->Valid()) {
    ASSERT_EQ(iter1->Valid(), iter2->Valid());
    ASSERT_EQ(iter1->key().ToString(), iter2->key().ToString());
    ASSERT_EQ(iter1->value().ToString(), iter2->value().ToString());
    iter1->Next();
    iter2->Next();
  }
  ASSERT_EQ(iter1->Valid(), iter2->Valid());
}

void AssertIterEqual(WBWIIteratorImpl* wbwii,
                     const std::vector<std::string>& keys) {
  wbwii->SeekToFirst();
  for (auto k : keys) {
    ASSERT_TRUE(wbwii->Valid());
    ASSERT_EQ(wbwii->Entry().key, k);
    wbwii->NextKey();
  }
  ASSERT_FALSE(wbwii->Valid());
  wbwii->SeekToLast();
  for (auto kit = keys.rbegin(); kit != keys.rend(); ++kit) {
    ASSERT_TRUE(wbwii->Valid());
    ASSERT_EQ(wbwii->Entry().key, *kit);
    wbwii->PrevKey();
  }
  ASSERT_FALSE(wbwii->Valid());
}
}  // namespace anonymous

class WBWIBaseTest : public testing::Test {
 public:
  explicit WBWIBaseTest(bool overwrite) : db_(nullptr) {
    options_.merge_operator =
        MergeOperators::CreateFromStringId("stringappend");
    options_.create_if_missing = true;
    dbname_ = test::PerThreadDBPath("write_batch_with_index_test");
    DestroyDB(dbname_, options_);
    batch_.reset(new WriteBatchWithIndex(BytewiseComparator(), 20, overwrite));
  }

  virtual ~WBWIBaseTest() {
    if (db_ != nullptr) {
      ReleaseSnapshot();
      delete db_;
      DestroyDB(dbname_, options_);
    }
  }

  std::string AddToBatch(ColumnFamilyHandle* cf, const std::string& key) {
    std::string result;
    for (size_t i = 0; i < key.size(); i++) {
      if (key[i] == 'd') {
        batch_->Delete(cf, key);
        result = "";
      } else if (key[i] == 'p') {
        result = key + ToString(i);
        batch_->Put(cf, key, result);
      } else if (key[i] == 'm') {
        std::string value = key + ToString(i);
        batch_->Merge(cf, key, value);
        if (result.empty()) {
          result = value;
        } else {
          result = result + "," + value;
        }
      }
    }
    return result;
  }

  virtual Status OpenDB() { return DB::Open(options_, dbname_, &db_); }

  void ReleaseSnapshot() {
    if (read_opts_.snapshot != nullptr) {
      EXPECT_NE(db_, nullptr);
      db_->ReleaseSnapshot(read_opts_.snapshot);
      read_opts_.snapshot = nullptr;
    }
  }

 public:
  DB* db_;
  std::string dbname_;
  Options options_;
  WriteOptions write_opts_;
  ReadOptions read_opts_;
  std::unique_ptr<WriteBatchWithIndex> batch_;
};

class WBWIKeepTest : public WBWIBaseTest {
 public:
  WBWIKeepTest() : WBWIBaseTest(false) {}
};

class WBWIOverwriteTest : public WBWIBaseTest {
 public:
  WBWIOverwriteTest() : WBWIBaseTest(true) {}
};
class WriteBatchWithIndexTest : public WBWIBaseTest,
                                public testing::WithParamInterface<bool> {
 public:
  WriteBatchWithIndexTest() : WBWIBaseTest(GetParam()) {}
};

void TestValueAsSecondaryIndexHelper(std::vector<Entry> entries,
                                     WriteBatchWithIndex* batch) {
  // In this test, we insert <key, value> to column family `data`, and
  // <value, key> to column family `index`. Then iterator them in order
  // and seek them by key.

  // Sort entries by key
  std::map<std::string, std::vector<Entry*>> data_map;
  // Sort entries by value
  std::map<std::string, std::vector<Entry*>> index_map;
  for (auto& e : entries) {
    data_map[e.key].push_back(&e);
    index_map[e.value].push_back(&e);
  }

  ColumnFamilyHandleImplDummy data(6, BytewiseComparator());
  ColumnFamilyHandleImplDummy index(8, BytewiseComparator());
  for (auto& e : entries) {
    if (e.type == kPutRecord) {
      ASSERT_OK(batch->Put(&data, e.key, e.value));
      ASSERT_OK(batch->Put(&index, e.value, e.key));
    } else if (e.type == kMergeRecord) {
      ASSERT_OK(batch->Merge(&data, e.key, e.value));
      ASSERT_OK(batch->Put(&index, e.value, e.key));
    } else {
      assert(e.type == kDeleteRecord);
      std::unique_ptr<WBWIIterator> iter(batch->NewIterator(&data));
      iter->Seek(e.key);
      ASSERT_OK(iter->status());
      auto write_entry = iter->Entry();
      ASSERT_EQ(e.key, write_entry.key.ToString());
      ASSERT_EQ(e.value, write_entry.value.ToString());
      ASSERT_OK(batch->Delete(&data, e.key));
      ASSERT_OK(batch->Put(&index, e.value, ""));
    }
  }

  // Iterator all keys
  {
    std::unique_ptr<WBWIIterator> iter(batch->NewIterator(&data));
    for (int seek_to_first : {0, 1}) {
      if (seek_to_first) {
        iter->SeekToFirst();
      } else {
        iter->Seek("");
      }
      for (auto pair : data_map) {
        for (auto v : pair.second) {
          ASSERT_OK(iter->status());
          ASSERT_TRUE(iter->Valid());
          auto write_entry = iter->Entry();
          ASSERT_EQ(pair.first, write_entry.key.ToString());
          ASSERT_EQ(v->type, write_entry.type);
          if (write_entry.type != kDeleteRecord) {
            ASSERT_EQ(v->value, write_entry.value.ToString());
          }
          iter->Next();
        }
      }
      ASSERT_TRUE(!iter->Valid());
    }
    iter->SeekToLast();
    for (auto pair = data_map.rbegin(); pair != data_map.rend(); ++pair) {
      for (auto v = pair->second.rbegin(); v != pair->second.rend(); v++) {
        ASSERT_OK(iter->status());
        ASSERT_TRUE(iter->Valid());
        auto write_entry = iter->Entry();
        ASSERT_EQ(pair->first, write_entry.key.ToString());
        ASSERT_EQ((*v)->type, write_entry.type);
        if (write_entry.type != kDeleteRecord) {
          ASSERT_EQ((*v)->value, write_entry.value.ToString());
        }
        iter->Prev();
      }
    }
    ASSERT_TRUE(!iter->Valid());
  }

  // Iterator all indexes
  {
    std::unique_ptr<WBWIIterator> iter(batch->NewIterator(&index));
    for (int seek_to_first : {0, 1}) {
      if (seek_to_first) {
        iter->SeekToFirst();
      } else {
        iter->Seek("");
      }
      for (auto pair : index_map) {
        for (auto v : pair.second) {
          ASSERT_OK(iter->status());
          ASSERT_TRUE(iter->Valid());
          auto write_entry = iter->Entry();
          ASSERT_EQ(pair.first, write_entry.key.ToString());
          if (v->type != kDeleteRecord) {
            ASSERT_EQ(v->key, write_entry.value.ToString());
            ASSERT_EQ(v->value, write_entry.key.ToString());
          }
          iter->Next();
        }
      }
      ASSERT_TRUE(!iter->Valid());
    }

    iter->SeekToLast();
    for (auto pair = index_map.rbegin(); pair != index_map.rend(); ++pair) {
      for (auto v = pair->second.rbegin(); v != pair->second.rend(); v++) {
        ASSERT_OK(iter->status());
        ASSERT_TRUE(iter->Valid());
        auto write_entry = iter->Entry();
        ASSERT_EQ(pair->first, write_entry.key.ToString());
        if ((*v)->type != kDeleteRecord) {
          ASSERT_EQ((*v)->key, write_entry.value.ToString());
          ASSERT_EQ((*v)->value, write_entry.key.ToString());
        }
        iter->Prev();
      }
    }
    ASSERT_TRUE(!iter->Valid());
  }

  // Seek to every key
  {
    std::unique_ptr<WBWIIterator> iter(batch->NewIterator(&data));

    // Seek the keys one by one in reverse order
    for (auto pair = data_map.rbegin(); pair != data_map.rend(); ++pair) {
      iter->Seek(pair->first);
      ASSERT_OK(iter->status());
      for (auto v : pair->second) {
        ASSERT_TRUE(iter->Valid());
        auto write_entry = iter->Entry();
        ASSERT_EQ(pair->first, write_entry.key.ToString());
        ASSERT_EQ(v->type, write_entry.type);
        if (write_entry.type != kDeleteRecord) {
          ASSERT_EQ(v->value, write_entry.value.ToString());
        }
        iter->Next();
        ASSERT_OK(iter->status());
      }
    }
  }

  // Seek to every index
  {
    std::unique_ptr<WBWIIterator> iter(batch->NewIterator(&index));

    // Seek the keys one by one in reverse order
    for (auto pair = index_map.rbegin(); pair != index_map.rend(); ++pair) {
      iter->Seek(pair->first);
      ASSERT_OK(iter->status());
      for (auto v : pair->second) {
        ASSERT_TRUE(iter->Valid());
        auto write_entry = iter->Entry();
        ASSERT_EQ(pair->first, write_entry.key.ToString());
        ASSERT_EQ(v->value, write_entry.key.ToString());
        if (v->type != kDeleteRecord) {
          ASSERT_EQ(v->key, write_entry.value.ToString());
        }
        iter->Next();
        ASSERT_OK(iter->status());
      }
    }
  }

  // Verify WriteBatch can be iterated
  TestHandler handler;
  ASSERT_OK(batch->GetWriteBatch()->Iterate(&handler));

  // Verify data column family
  {
    ASSERT_EQ(entries.size(), handler.seen[data.GetID()].size());
    size_t i = 0;
    for (auto e : handler.seen[data.GetID()]) {
      auto write_entry = entries[i++];
      ASSERT_EQ(e.type, write_entry.type);
      ASSERT_EQ(e.key, write_entry.key);
      if (e.type != kDeleteRecord) {
        ASSERT_EQ(e.value, write_entry.value);
      }
    }
  }

  // Verify index column family
  {
    ASSERT_EQ(entries.size(), handler.seen[index.GetID()].size());
    size_t i = 0;
    for (auto e : handler.seen[index.GetID()]) {
      auto write_entry = entries[i++];
      ASSERT_EQ(e.key, write_entry.value);
      if (write_entry.type != kDeleteRecord) {
        ASSERT_EQ(e.value, write_entry.key);
      }
    }
  }
}

TEST_F(WBWIKeepTest, TestValueAsSecondaryIndex) {
  Entry entries[] = {
      {"aaa", "0005", kPutRecord},
      {"b", "0002", kPutRecord},
      {"cdd", "0002", kMergeRecord},
      {"aab", "00001", kPutRecord},
      {"cc", "00005", kPutRecord},
      {"cdd", "0002", kPutRecord},
      {"aab", "0003", kPutRecord},
      {"cc", "00005", kDeleteRecord},
  };
  std::vector<Entry> entries_list(entries, entries + 8);

  batch_.reset(new WriteBatchWithIndex(nullptr, 20, false));

  TestValueAsSecondaryIndexHelper(entries_list, batch_.get());

  // Clear batch and re-run test with new values
  batch_->Clear();

  Entry new_entries[] = {
      {"aaa", "0005", kPutRecord},
      {"e", "0002", kPutRecord},
      {"add", "0002", kMergeRecord},
      {"aab", "00001", kPutRecord},
      {"zz", "00005", kPutRecord},
      {"add", "0002", kPutRecord},
      {"aab", "0003", kPutRecord},
      {"zz", "00005", kDeleteRecord},
  };

  entries_list = std::vector<Entry>(new_entries, new_entries + 8);

  TestValueAsSecondaryIndexHelper(entries_list, batch_.get());
}

TEST_P(WriteBatchWithIndexTest, TestComparatorForCF) {
  ColumnFamilyHandleImplDummy cf1(6, nullptr);
  ColumnFamilyHandleImplDummy reverse_cf(66, ReverseBytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(88, BytewiseComparator());

  ASSERT_OK(batch_->Put(&cf1, "ddd", ""));
  ASSERT_OK(batch_->Put(&cf2, "aaa", ""));
  ASSERT_OK(batch_->Put(&cf2, "eee", ""));
  ASSERT_OK(batch_->Put(&cf1, "ccc", ""));
  ASSERT_OK(batch_->Put(&reverse_cf, "a11", ""));
  ASSERT_OK(batch_->Put(&cf1, "bbb", ""));

  Slice key_slices[] = {"a", "3", "3"};
  Slice value_slice = "";
  ASSERT_OK(batch_->Put(&reverse_cf, SliceParts(key_slices, 3),
                        SliceParts(&value_slice, 1)));
  ASSERT_OK(batch_->Put(&reverse_cf, "a22", ""));

  {
    std::unique_ptr<WBWIIterator> iter(batch_->NewIterator(&cf1));
    iter->Seek("");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bbb", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("ccc", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("ddd", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  {
    std::unique_ptr<WBWIIterator> iter(batch_->NewIterator(&cf2));
    iter->Seek("");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("aaa", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("eee", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  {
    std::unique_ptr<WBWIIterator> iter(batch_->NewIterator(&reverse_cf));
    iter->Seek("");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("z");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a33", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a22", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a11", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("a22");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a22", iter->Entry().key.ToString());

    iter->Seek("a13");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a11", iter->Entry().key.ToString());
  }
}

TEST_F(WBWIOverwriteTest, TestOverwriteKey) {
  ColumnFamilyHandleImplDummy cf1(6, nullptr);
  ColumnFamilyHandleImplDummy reverse_cf(66, ReverseBytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(88, BytewiseComparator());

  ASSERT_OK(batch_->Merge(&cf1, "ddd", ""));
  ASSERT_OK(batch_->Put(&cf1, "ddd", ""));
  ASSERT_OK(batch_->Delete(&cf1, "ddd"));
  ASSERT_OK(batch_->Put(&cf2, "aaa", ""));
  ASSERT_OK(batch_->Delete(&cf2, "aaa"));
  ASSERT_OK(batch_->Put(&cf2, "aaa", "aaa"));
  ASSERT_OK(batch_->Put(&cf2, "eee", "eee"));
  ASSERT_OK(batch_->Put(&cf1, "ccc", ""));
  ASSERT_OK(batch_->Put(&reverse_cf, "a11", ""));
  ASSERT_OK(batch_->Delete(&cf1, "ccc"));
  ASSERT_OK(batch_->Put(&reverse_cf, "a33", "a33"));
  ASSERT_OK(batch_->Put(&reverse_cf, "a11", "a11"));
  Slice slices[] = {"a", "3", "3"};
  ASSERT_OK(batch_->Delete(&reverse_cf, SliceParts(slices, 3)));

  {
    std::unique_ptr<WBWIIterator> iter(batch_->NewIterator(&cf1));
    iter->Seek("");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("ccc", iter->Entry().key.ToString());
    ASSERT_TRUE(iter->Entry().type == WriteType::kDeleteRecord);
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("ddd", iter->Entry().key.ToString());
    ASSERT_TRUE(iter->Entry().type == WriteType::kDeleteRecord);
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  {
    std::unique_ptr<WBWIIterator> iter(batch_->NewIterator(&cf2));
    iter->SeekToLast();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("eee", iter->Entry().key.ToString());
    ASSERT_EQ("eee", iter->Entry().value.ToString());
    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("aaa", iter->Entry().key.ToString());
    ASSERT_EQ("aaa", iter->Entry().value.ToString());
    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("aaa", iter->Entry().key.ToString());
    ASSERT_EQ("aaa", iter->Entry().value.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("eee", iter->Entry().key.ToString());
    ASSERT_EQ("eee", iter->Entry().value.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  {
    std::unique_ptr<WBWIIterator> iter(batch_->NewIterator(&reverse_cf));
    iter->Seek("");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("z");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a33", iter->Entry().key.ToString());
    ASSERT_TRUE(iter->Entry().type == WriteType::kDeleteRecord);
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a11", iter->Entry().key.ToString());
    ASSERT_EQ("a11", iter->Entry().value.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a11", iter->Entry().key.ToString());
    ASSERT_EQ("a11", iter->Entry().value.ToString());
    iter->Prev();

    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a33", iter->Entry().key.ToString());
    ASSERT_TRUE(iter->Entry().type == WriteType::kDeleteRecord);
    iter->Prev();
    ASSERT_TRUE(!iter->Valid());
  }
}

TEST_P(WriteBatchWithIndexTest, TestWBWIIterator) {
  ColumnFamilyHandleImplDummy cf1(1, BytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(2, BytewiseComparator());
  ASSERT_OK(batch_->Put(&cf1, "a", "a1"));
  ASSERT_OK(batch_->Put(&cf1, "c", "c1"));
  ASSERT_OK(batch_->Put(&cf1, "c", "c2"));
  ASSERT_OK(batch_->Put(&cf1, "e", "e1"));
  ASSERT_OK(batch_->Put(&cf1, "e", "e2"));
  ASSERT_OK(batch_->Put(&cf1, "e", "e3"));
  std::unique_ptr<WBWIIteratorImpl> iter1(
      static_cast<WBWIIteratorImpl*>(batch_->NewIterator(&cf1)));
  std::unique_ptr<WBWIIteratorImpl> iter2(
      static_cast<WBWIIteratorImpl*>(batch_->NewIterator(&cf2)));
  AssertIterEqual(iter1.get(), {"a", "c", "e"});
  AssertIterEqual(iter2.get(), {});
  ASSERT_OK(batch_->Put(&cf2, "a", "a2"));
  ASSERT_OK(batch_->Merge(&cf2, "b", "b1"));
  ASSERT_OK(batch_->Merge(&cf2, "b", "b2"));
  ASSERT_OK(batch_->Delete(&cf2, "d"));
  ASSERT_OK(batch_->Merge(&cf2, "d", "d2"));
  ASSERT_OK(batch_->Merge(&cf2, "d", "d3"));
  ASSERT_OK(batch_->Delete(&cf2, "f"));
  AssertIterEqual(iter1.get(), {"a", "c", "e"});
  AssertIterEqual(iter2.get(), {"a", "b", "d", "f"});
}

TEST_P(WriteBatchWithIndexTest, TestRandomIteraratorWithBase) {
  std::vector<std::string> source_strings = {"a", "b", "c", "d", "e",
                                             "f", "g", "h", "i", "j"};
  for (int rand_seed = 301; rand_seed < 366; rand_seed++) {
    Random rnd(rand_seed);

    ColumnFamilyHandleImplDummy cf1(6, BytewiseComparator());
    ColumnFamilyHandleImplDummy cf2(2, BytewiseComparator());
    ColumnFamilyHandleImplDummy cf3(8, BytewiseComparator());
    batch_->Clear();

    if (rand_seed % 2 == 0) {
      ASSERT_OK(batch_->Put(&cf2, "zoo", "bar"));
    }
    if (rand_seed % 4 == 1) {
      ASSERT_OK(batch_->Put(&cf3, "zoo", "bar"));
    }

    KVMap map;
    KVMap merged_map;
    for (auto key : source_strings) {
      std::string value = key + key;
      int type = rnd.Uniform(6);
      switch (type) {
        case 0:
          // only base has it
          map[key] = value;
          merged_map[key] = value;
          break;
        case 1:
          // only delta has it
          ASSERT_OK(batch_->Put(&cf1, key, value));
          map[key] = value;
          merged_map[key] = value;
          break;
        case 2:
          // both has it. Delta should win
          ASSERT_OK(batch_->Put(&cf1, key, value));
          map[key] = "wrong_value";
          merged_map[key] = value;
          break;
        case 3:
          // both has it. Delta is delete
          ASSERT_OK(batch_->Delete(&cf1, key));
          map[key] = "wrong_value";
          break;
        case 4:
          // only delta has it. Delta is delete
          ASSERT_OK(batch_->Delete(&cf1, key));
          map[key] = "wrong_value";
          break;
        default:
          // Neither iterator has it.
          break;
      }
    }

    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&map)));
    std::unique_ptr<Iterator> result_iter(new KVIter(&merged_map));

    bool is_valid = false;
    for (int i = 0; i < 128; i++) {
      // Random walk and make sure iter and result_iter returns the
      // same key and value
      int type = rnd.Uniform(6);
      ASSERT_OK(iter->status());
      switch (type) {
        case 0:
          // Seek to First
          iter->SeekToFirst();
          result_iter->SeekToFirst();
          break;
        case 1:
          // Seek to last
          iter->SeekToLast();
          result_iter->SeekToLast();
          break;
        case 2: {
          // Seek to random key
          auto key_idx = rnd.Uniform(static_cast<int>(source_strings.size()));
          auto key = source_strings[key_idx];
          iter->Seek(key);
          result_iter->Seek(key);
          break;
        }
        case 3: {
          // SeekForPrev to random key
          auto key_idx = rnd.Uniform(static_cast<int>(source_strings.size()));
          auto key = source_strings[key_idx];
          iter->SeekForPrev(key);
          result_iter->SeekForPrev(key);
          break;
        }
        case 4:
          // Next
          if (is_valid) {
            iter->Next();
            result_iter->Next();
          } else {
            continue;
          }
          break;
        default:
          assert(type == 5);
          // Prev
          if (is_valid) {
            iter->Prev();
            result_iter->Prev();
          } else {
            continue;
          }
          break;
      }
      AssertItersMatch(iter.get(), result_iter.get());
      is_valid = iter->Valid();
    }

    ASSERT_OK(iter->status());
  }
}

TEST_P(WriteBatchWithIndexTest, TestIteraratorWithBase) {
  ColumnFamilyHandleImplDummy cf1(6, BytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(2, BytewiseComparator());
  {
    KVMap map;
    map["a"] = "aa";
    map["c"] = "cc";
    map["e"] = "ee";
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    AssertIter(iter.get(), "c", "cc");
    iter->Next();
    AssertIter(iter.get(), "e", "ee");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToLast();
    AssertIter(iter.get(), "e", "ee");
    iter->Prev();
    AssertIter(iter.get(), "c", "cc");
    iter->Prev();
    AssertIter(iter.get(), "a", "aa");
    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("b");
    AssertIter(iter.get(), "c", "cc");

    iter->Prev();
    AssertIter(iter.get(), "a", "aa");

    iter->Seek("a");
    AssertIter(iter.get(), "a", "aa");
  }

  // Test the case that there is one element in the write batch
  ASSERT_OK(batch_->Put(&cf2, "zoo", "bar"));
  ASSERT_OK(batch_->Put(&cf1, "a", "aa"));
  {
    KVMap empty_map;
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&empty_map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  ASSERT_OK(batch_->Delete(&cf1, "b"));
  ASSERT_OK(batch_->Put(&cf1, "c", "cc"));
  ASSERT_OK(batch_->Put(&cf1, "d", "dd"));
  ASSERT_OK(batch_->Delete(&cf1, "e"));

  {
    KVMap map;
    map["b"] = "";
    map["cc"] = "cccc";
    map["f"] = "ff";
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    AssertIter(iter.get(), "c", "cc");
    iter->Next();
    AssertIter(iter.get(), "cc", "cccc");
    iter->Next();
    AssertIter(iter.get(), "d", "dd");
    iter->Next();
    AssertIter(iter.get(), "f", "ff");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToLast();
    AssertIter(iter.get(), "f", "ff");
    iter->Prev();
    AssertIter(iter.get(), "d", "dd");
    iter->Prev();
    AssertIter(iter.get(), "cc", "cccc");
    iter->Prev();
    AssertIter(iter.get(), "c", "cc");
    iter->Next();
    AssertIter(iter.get(), "cc", "cccc");
    iter->Prev();
    AssertIter(iter.get(), "c", "cc");
    iter->Prev();
    AssertIter(iter.get(), "a", "aa");
    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("c");
    AssertIter(iter.get(), "c", "cc");

    iter->Seek("cb");
    AssertIter(iter.get(), "cc", "cccc");

    iter->Seek("cc");
    AssertIter(iter.get(), "cc", "cccc");
    iter->Next();
    AssertIter(iter.get(), "d", "dd");

    iter->Seek("e");
    AssertIter(iter.get(), "f", "ff");

    iter->Prev();
    AssertIter(iter.get(), "d", "dd");

    iter->Next();
    AssertIter(iter.get(), "f", "ff");
  }

  {
    KVMap empty_map;
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&empty_map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    AssertIter(iter.get(), "c", "cc");
    iter->Next();
    AssertIter(iter.get(), "d", "dd");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToLast();
    AssertIter(iter.get(), "d", "dd");
    iter->Prev();
    AssertIter(iter.get(), "c", "cc");
    iter->Prev();
    AssertIter(iter.get(), "a", "aa");

    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("aa");
    AssertIter(iter.get(), "c", "cc");
    iter->Next();
    AssertIter(iter.get(), "d", "dd");

    iter->Seek("ca");
    AssertIter(iter.get(), "d", "dd");

    iter->Prev();
    AssertIter(iter.get(), "c", "cc");
  }
}

TEST_P(WriteBatchWithIndexTest, TestIteraratorWithBaseReverseCmp) {
  ColumnFamilyHandleImplDummy cf1(6, ReverseBytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(2, ReverseBytewiseComparator());

  // Test the case that there is one element in the write batch
  ASSERT_OK(batch_->Put(&cf2, "zoo", "bar"));
  ASSERT_OK(batch_->Put(&cf1, "a", "aa"));
  {
    KVMap empty_map;
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&empty_map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  ASSERT_OK(batch_->Put(&cf1, "c", "cc"));
  {
    KVMap map;
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "c", "cc");
    iter->Next();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToLast();
    AssertIter(iter.get(), "a", "aa");
    iter->Prev();
    AssertIter(iter.get(), "c", "cc");
    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("b");
    AssertIter(iter.get(), "a", "aa");

    iter->Prev();
    AssertIter(iter.get(), "c", "cc");

    iter->Seek("a");
    AssertIter(iter.get(), "a", "aa");
  }

  // default column family
  ASSERT_OK(batch_->Put("a", "b"));
  {
    KVMap map;
    map["b"] = "";
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(new KVIter(&map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "b");
    iter->Next();
    AssertIter(iter.get(), "b", "");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToLast();
    AssertIter(iter.get(), "b", "");
    iter->Prev();
    AssertIter(iter.get(), "a", "b");
    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("b");
    AssertIter(iter.get(), "b", "");

    iter->Prev();
    AssertIter(iter.get(), "a", "b");

    iter->Seek("0");
    AssertIter(iter.get(), "a", "b");
  }
}

TEST_P(WriteBatchWithIndexTest, TestGetFromBatch) {
  Options options;
  Status s;
  std::string value;

  s = batch_->GetFromBatch(options_, "b", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->Put("a", "a"));
  ASSERT_OK(batch_->Put("b", "b"));
  ASSERT_OK(batch_->Put("c", "c"));
  ASSERT_OK(batch_->Put("a", "z"));
  ASSERT_OK(batch_->Delete("c"));
  ASSERT_OK(batch_->Delete("d"));
  ASSERT_OK(batch_->Delete("e"));
  ASSERT_OK(batch_->Put("e", "e"));

  s = batch_->GetFromBatch(options_, "b", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);

  s = batch_->GetFromBatch(options_, "a", &value);
  ASSERT_OK(s);
  ASSERT_EQ("z", value);

  s = batch_->GetFromBatch(options_, "c", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = batch_->GetFromBatch(options_, "d", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = batch_->GetFromBatch(options_, "x", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = batch_->GetFromBatch(options_, "e", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);

  ASSERT_OK(batch_->Merge("z", "z"));

  s = batch_->GetFromBatch(options_, "z", &value);
  ASSERT_NOK(s);  // No merge operator specified.

  s = batch_->GetFromBatch(options_, "b", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);
}

TEST_P(WriteBatchWithIndexTest, TestGetFromBatchMerge) {
  Status s = OpenDB();
  ASSERT_OK(s);

  ColumnFamilyHandle* column_family = db_->DefaultColumnFamily();
  std::string value;

  s = batch_->GetFromBatch(options_, "x", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->Put("x", "X"));
  std::string expected = "X";

  for (int i = 0; i < 5; i++) {
    ASSERT_OK(batch_->Merge("x", ToString(i)));
    expected = expected + "," + ToString(i);

    if (i % 2 == 0) {
      ASSERT_OK(batch_->Put("y", ToString(i / 2)));
    }

    ASSERT_OK(batch_->Merge("z", "z"));

    s = batch_->GetFromBatch(column_family, options_, "x", &value);
    ASSERT_OK(s);
    ASSERT_EQ(expected, value);

    s = batch_->GetFromBatch(column_family, options_, "y", &value);
    ASSERT_OK(s);
    ASSERT_EQ(ToString(i / 2), value);

    s = batch_->GetFromBatch(column_family, options_, "z", &value);
    ASSERT_TRUE(s.IsMergeInProgress());
  }
}

TEST_F(WBWIOverwriteTest, TestGetFromBatchMerge2) {
  Status s = OpenDB();
  ASSERT_OK(s);

  ColumnFamilyHandle* column_family = db_->DefaultColumnFamily();
  std::string value;

  s = batch_->GetFromBatch(column_family, options_, "X", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->Put(column_family, "X", "x"));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("x", value);

  ASSERT_OK(batch_->Put(column_family, "X", "x2"));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("x2", value);

  ASSERT_OK(batch_->Merge(column_family, "X", "aaa"));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("x2,aaa", value);

  ASSERT_OK(batch_->Merge(column_family, "X", "bbb"));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("x2,aaa,bbb", value);

  ASSERT_OK(batch_->Put(column_family, "X", "x3"));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("x3", value);

  ASSERT_OK(batch_->Merge(column_family, "X", "ccc"));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("x3,ccc", value);

  ASSERT_OK(batch_->Delete(column_family, "X"));
  s = batch_->GetFromBatch(column_family, options_, "X", &value);
  ASSERT_TRUE(s.IsNotFound());

  batch_->Merge(column_family, "X", "ddd");
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("ddd", value);
}

TEST_P(WriteBatchWithIndexTest, TestGetFromBatchAndDB) {
  ASSERT_OK(OpenDB());

  std::string value;

  ASSERT_OK(db_->Put(write_opts_, "a", "a"));
  ASSERT_OK(db_->Put(write_opts_, "b", "b"));
  ASSERT_OK(db_->Put(write_opts_, "c", "c"));

  ASSERT_OK(batch_->Put("a", "batch_->a"));
  ASSERT_OK(batch_->Delete("b"));

  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "a", &value));
  ASSERT_EQ("batch_->a", value);

  Status s = batch_->GetFromBatchAndDB(db_, read_opts_, "b", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "c", &value));
  ASSERT_EQ("c", value);

  s = batch_->GetFromBatchAndDB(db_, read_opts_, "x", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(db_->Delete(write_opts_, "x"));

  s = batch_->GetFromBatchAndDB(db_, read_opts_, "x", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_P(WriteBatchWithIndexTest, TestGetFromBatchAndDBMerge) {
  Status s = OpenDB();
  ASSERT_OK(s);

  std::string value;

  ASSERT_OK(db_->Put(write_opts_, "a", "a0"));
  ASSERT_OK(db_->Put(write_opts_, "b", "b0"));
  ASSERT_OK(db_->Merge(write_opts_, "b", "b1"));
  ASSERT_OK(db_->Merge(write_opts_, "c", "c0"));
  ASSERT_OK(db_->Merge(write_opts_, "d", "d0"));

  ASSERT_OK(batch_->Merge("a", "a1"));
  ASSERT_OK(batch_->Merge("a", "a2"));
  ASSERT_OK(batch_->Merge("b", "b2"));
  ASSERT_OK(batch_->Merge("d", "d1"));
  ASSERT_OK(batch_->Merge("e", "e0"));

  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "a", &value));
  ASSERT_EQ("a0,a1,a2", value);

  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "b", &value));
  ASSERT_EQ("b0,b1,b2", value);

  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "c", &value));
  ASSERT_EQ("c0", value);

  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "d", &value));
  ASSERT_EQ("d0,d1", value);

  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "e", &value));
  ASSERT_EQ("e0", value);

  ASSERT_OK(db_->Delete(write_opts_, "x"));

  s = batch_->GetFromBatchAndDB(db_, read_opts_, "x", &value);
  ASSERT_TRUE(s.IsNotFound());

  const Snapshot* snapshot = db_->GetSnapshot();
  ReadOptions snapshot_read_options;
  snapshot_read_options.snapshot = snapshot;

  ASSERT_OK(db_->Delete(write_opts_, "a"));

  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "a", &value));
  ASSERT_EQ("a1,a2", value);

  ASSERT_OK(
      s = batch_->GetFromBatchAndDB(db_, snapshot_read_options, "a", &value));
  ASSERT_EQ("a0,a1,a2", value);

  ASSERT_OK(batch_->Delete("a"));

  s = batch_->GetFromBatchAndDB(db_, read_opts_, "a", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = batch_->GetFromBatchAndDB(db_, snapshot_read_options, "a", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(s = db_->Merge(write_opts_, "c", "c1"));

  ASSERT_OK(s = batch_->GetFromBatchAndDB(db_, read_opts_, "c", &value));
  ASSERT_EQ("c0,c1", value);

  ASSERT_OK(
      s = batch_->GetFromBatchAndDB(db_, snapshot_read_options, "c", &value));
  ASSERT_EQ("c0", value);

  ASSERT_OK(db_->Put(write_opts_, "e", "e1"));
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "e", &value));
  ASSERT_EQ("e1,e0", value);

  ASSERT_OK(batch_->GetFromBatchAndDB(db_, snapshot_read_options, "e", &value));
  ASSERT_EQ("e0", value);

  ASSERT_OK(s = db_->Delete(write_opts_, "e"));
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "e", &value));
  ASSERT_EQ("e0", value);

  ASSERT_OK(batch_->GetFromBatchAndDB(db_, snapshot_read_options, "e", &value));
  ASSERT_EQ("e0", value);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(WBWIOverwriteTest, TestGetFromBatchAndDBMerge2) {
  Status s = OpenDB();
  ASSERT_OK(s);

  std::string value;

  s = batch_->GetFromBatchAndDB(db_, read_opts_, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->Merge("A", "xxx"));
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "A", &value));
  ASSERT_EQ(value, "xxx");

  ASSERT_OK(batch_->Merge("A", "yyy"));
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "A", &value));
  ASSERT_EQ(value, "xxx,yyy");

  ASSERT_OK(db_->Put(write_opts_, "A", "a0"));
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "A", &value));
  ASSERT_EQ(value, "a0,xxx,yyy");

  ASSERT_OK(batch_->Delete("A"));

  s = batch_->GetFromBatchAndDB(db_, read_opts_, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_P(WriteBatchWithIndexTest, TestGetFromBatchAndDBMerge3) {
  Status s = OpenDB();
  ASSERT_OK(s);

  FlushOptions flush_options;
  std::string value;

  ASSERT_OK(db_->Put(write_opts_, "A", "1"));
  ASSERT_OK(db_->Flush(flush_options, db_->DefaultColumnFamily()));
  ASSERT_OK(batch_->Merge("A", "2"));

  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "A", &value));
  ASSERT_EQ(value, "1,2");
}

TEST_P(WriteBatchWithIndexTest, TestPinnedGetFromBatchAndDB) {
  Status s = OpenDB();
  ASSERT_OK(s);

  PinnableSlice value;

  ASSERT_OK(db_->Put(write_opts_, "a", "a0"));
  ASSERT_OK(db_->Put(write_opts_, "b", "b0"));
  ASSERT_OK(db_->Merge(write_opts_, "b", "b1"));
  ASSERT_OK(db_->Merge(write_opts_, "c", "c0"));
  ASSERT_OK(db_->Merge(write_opts_, "d", "d0"));
  ASSERT_OK(batch_->Merge("a", "a1"));
  ASSERT_OK(batch_->Merge("a", "a2"));
  ASSERT_OK(batch_->Merge("b", "b2"));
  ASSERT_OK(batch_->Merge("d", "d1"));
  ASSERT_OK(batch_->Merge("e", "e0"));

  for (int i = 0; i < 2; i++) {
    if (i == 1) {
      // Do it again with a flushed DB...
      ASSERT_OK(db_->Flush(FlushOptions(), db_->DefaultColumnFamily()));
    }
    ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "a", &value));
    ASSERT_EQ("a0,a1,a2", value.ToString());

    ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "b", &value));
    ASSERT_EQ("b0,b1,b2", value.ToString());

    ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "c", &value));
    ASSERT_EQ("c0", value.ToString());

    ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "d", &value));
    ASSERT_EQ("d0,d1", value.ToString());

    ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "e", &value));
    ASSERT_EQ("e0", value.ToString());
    ASSERT_OK(db_->Delete(write_opts_, "x"));

    s = batch_->GetFromBatchAndDB(db_, read_opts_, "x", &value);
    ASSERT_TRUE(s.IsNotFound());
  }
}

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
TEST_F(WBWIOverwriteTest, MutateWhileIteratingCorrectnessTest) {
  for (char c = 'a'; c <= 'z'; ++c) {
    ASSERT_OK(batch_->Put(std::string(1, c), std::string(1, c)));
  }

  std::unique_ptr<WBWIIterator> iter(batch_->NewIterator());
  iter->Seek("k");
  AssertKey("k", iter.get());
  iter->Next();
  AssertKey("l", iter.get());
  ASSERT_OK(batch_->Put("ab", "cc"));
  iter->Next();
  AssertKey("m", iter.get());
  ASSERT_OK(batch_->Put("mm", "kk"));
  iter->Next();
  AssertKey("mm", iter.get());
  AssertValue("kk", iter.get());
  ASSERT_OK(batch_->Delete("mm"));

  iter->Next();
  AssertKey("n", iter.get());
  iter->Prev();
  AssertKey("mm", iter.get());
  ASSERT_EQ(kDeleteRecord, iter->Entry().type);

  iter->Seek("ab");
  AssertKey("ab", iter.get());
  ASSERT_OK(batch_->Delete("x"));
  iter->Seek("x");
  AssertKey("x", iter.get());
  ASSERT_EQ(kDeleteRecord, iter->Entry().type);
  iter->Prev();
  AssertKey("w", iter.get());
}

void AssertIterKey(std::string key, Iterator* iter) {
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(key, iter->key().ToString());
}

void AssertIterValue(std::string value, Iterator* iter) {
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value, iter->value().ToString());
}

// same thing as above, but testing IteratorWithBase
TEST_F(WBWIOverwriteTest, MutateWhileIteratingBaseCorrectnessTest) {
  WriteBatchWithIndex batch(BytewiseComparator(), 0, true);
  for (char c = 'a'; c <= 'z'; ++c) {
    ASSERT_OK(batch_->Put(std::string(1, c), std::string(1, c)));
  }

  KVMap map;
  map["aa"] = "aa";
  map["cc"] = "cc";
  map["ee"] = "ee";
  map["em"] = "me";

  std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(new KVIter(&map)));
  iter->Seek("k");
  AssertIterKey("k", iter.get());
  iter->Next();
  AssertIterKey("l", iter.get());
  ASSERT_OK(batch_->Put("ab", "cc"));
  iter->Next();
  AssertIterKey("m", iter.get());
  ASSERT_OK(batch_->Put("mm", "kk"));
  iter->Next();
  AssertIterKey("mm", iter.get());
  AssertIterValue("kk", iter.get());
  ASSERT_OK(batch_->Delete("mm"));
  iter->Next();
  AssertIterKey("n", iter.get());
  iter->Prev();
  // "mm" is deleted, so we're back at "m"
  AssertIterKey("m", iter.get());

  iter->Seek("ab");
  AssertIterKey("ab", iter.get());
  iter->Prev();
  AssertIterKey("aa", iter.get());
  iter->Prev();
  AssertIterKey("a", iter.get());
  ASSERT_OK(batch_->Delete("aa"));
  iter->Next();
  AssertIterKey("ab", iter.get());
  iter->Prev();
  AssertIterKey("a", iter.get());

  ASSERT_OK(batch_->Delete("x"));
  iter->Seek("x");
  AssertIterKey("y", iter.get());
  iter->Next();
  AssertIterKey("z", iter.get());
  iter->Prev();
  iter->Prev();
  AssertIterKey("w", iter.get());

  ASSERT_OK(batch_->Delete("e"));
  iter->Seek("e");
  AssertIterKey("ee", iter.get());
  AssertIterValue("ee", iter.get());
  ASSERT_OK(batch_->Put("ee", "xx"));
  // still the same value
  AssertIterValue("ee", iter.get());
  iter->Next();
  AssertIterKey("em", iter.get());
  iter->Prev();
  // new value
  AssertIterValue("xx", iter.get());

  ASSERT_OK(iter->status());
}

// stress testing mutations with IteratorWithBase
TEST_F(WBWIOverwriteTest, MutateWhileIteratingBaseStressTest) {
  for (char c = 'a'; c <= 'z'; ++c) {
    ASSERT_OK(batch_->Put(std::string(1, c), std::string(1, c)));
  }

  KVMap map;
  for (char c = 'a'; c <= 'z'; ++c) {
    map[std::string(2, c)] = std::string(2, c);
  }

  std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(new KVIter(&map)));

  Random rnd(301);
  for (int i = 0; i < 1000000; ++i) {
    int random = rnd.Uniform(8);
    char c = static_cast<char>(rnd.Uniform(26) + 'a');
    switch (random) {
      case 0:
        ASSERT_OK(batch_->Put(std::string(1, c), "xxx"));
        break;
      case 1:
        ASSERT_OK(batch_->Put(std::string(2, c), "xxx"));
        break;
      case 2:
        ASSERT_OK(batch_->Delete(std::string(1, c)));
        break;
      case 3:
        ASSERT_OK(batch_->Delete(std::string(2, c)));
        break;
      case 4:
        iter->Seek(std::string(1, c));
        break;
      case 5:
        iter->Seek(std::string(2, c));
        break;
      case 6:
        if (iter->Valid()) {
          iter->Next();
        }
        break;
      case 7:
        if (iter->Valid()) {
          iter->Prev();
        }
        break;
      default:
        assert(false);
    }
  }
  ASSERT_OK(iter->status());
}

TEST_P(WriteBatchWithIndexTest, SavePointTest) {
  ColumnFamilyHandleImplDummy cf1(1, BytewiseComparator());
  KVMap empty_map;
  std::unique_ptr<Iterator> cf0_iter(
      batch_->NewIteratorWithBase(new KVIter(&empty_map)));
  std::unique_ptr<Iterator> cf1_iter(
      batch_->NewIteratorWithBase(&cf1, new KVIter(&empty_map)));
  Status s;
  KVMap kvm_cf0_0 = {{"A", "aa"}, {"B", "b"}};
  KVMap kvm_cf1_0 = {{"A", "a1"}, {"C", "c1"}, {"E", "e1"}};
  KVIter kvi_cf0_0(&kvm_cf0_0);
  KVIter kvi_cf1_0(&kvm_cf1_0);

  ASSERT_OK(batch_->Put("A", "a"));
  ASSERT_OK(batch_->Put("B", "b"));
  ASSERT_OK(batch_->Put("A", "aa"));
  ASSERT_OK(batch_->Put(&cf1, "A", "a1"));
  ASSERT_OK(batch_->Delete(&cf1, "B"));
  ASSERT_OK(batch_->Put(&cf1, "C", "c1"));
  ASSERT_OK(batch_->Put(&cf1, "E", "e1"));

  AssertItersEqual(cf0_iter.get(), &kvi_cf0_0);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_0);
  batch_->SetSavePoint();  // 1

  KVMap kvm_cf0_1 = {{"B", "bb"}, {"C", "cc"}};
  KVMap kvm_cf1_1 = {{"B", "b1"}, {"C", "c1"}};
  KVIter kvi_cf0_1(&kvm_cf0_1);
  KVIter kvi_cf1_1(&kvm_cf1_1);

  ASSERT_OK(batch_->Put("C", "cc"));
  ASSERT_OK(batch_->Put("B", "bb"));
  ASSERT_OK(batch_->Delete("A"));
  ASSERT_OK(batch_->Put(&cf1, "B", "b1"));
  ASSERT_OK(batch_->Delete(&cf1, "A"));
  ASSERT_OK(batch_->SingleDelete(&cf1, "E"));
  batch_->SetSavePoint();  // 2
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_1);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_1);

  KVMap kvm_cf0_2 = {{"A", "xxx"}, {"C", "cc"}};
  KVMap kvm_cf1_2 = {{"B", "b2"}};
  KVIter kvi_cf0_2(&kvm_cf0_2);
  KVIter kvi_cf1_2(&kvm_cf1_2);

  ASSERT_OK(batch_->Put("A", "aaa"));
  ASSERT_OK(batch_->Put("A", "xxx"));
  ASSERT_OK(batch_->Delete("B"));
  ASSERT_OK(batch_->Put(&cf1, "B", "b2"));
  ASSERT_OK(batch_->Delete(&cf1, "C"));
  batch_->SetSavePoint();  // 3
  batch_->SetSavePoint();  // 4
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_2);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_2);

  KVMap kvm_cf0_4 = {{"A", "xxx"}, {"C", "cc"}};
  KVMap kvm_cf1_4 = {{"B", "b2"}};
  KVIter kvi_cf0_4(&kvm_cf0_4);
  KVIter kvi_cf1_4(&kvm_cf1_4);
  ASSERT_OK(batch_->SingleDelete("D"));
  ASSERT_OK(batch_->Delete(&cf1, "D"));
  ASSERT_OK(batch_->Delete(&cf1, "E"));
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_4);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_4);

  ASSERT_OK(batch_->RollbackToSavePoint());  // rollback to 4
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_2);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_2);

  ASSERT_OK(batch_->RollbackToSavePoint());  // rollback to 3
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_2);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_2);

  ASSERT_OK(batch_->RollbackToSavePoint());  // rollback to 2
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_1);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_1);

  batch_->SetSavePoint();  // 5
  ASSERT_OK(batch_->Put("X", "x"));

  KVMap kvm_cf0_5 = {{"B", "bb"}, {"C", "cc"}, {"X", "x"}};
  KVIter kvi_cf0_5(&kvm_cf0_5);
  KVIter kvi_cf1_5(&kvm_cf1_1);
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_5);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_5);

  ASSERT_OK(batch_->RollbackToSavePoint());  // rollback to 5
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_1);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_1);

  ASSERT_OK(batch_->RollbackToSavePoint());  // rollback to 1
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_0);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_0);

  s = batch_->RollbackToSavePoint();  // no savepoint found
  ASSERT_TRUE(s.IsNotFound());
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_0);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_0);

  batch_->SetSavePoint();  // 6

  batch_->Clear();
  ASSERT_EQ("", PrintContents(batch_.get(), nullptr));
  ASSERT_EQ("", PrintContents(batch_.get(), &cf1));

  s = batch_->RollbackToSavePoint();  // rollback to 6
  ASSERT_TRUE(s.IsNotFound());
}

TEST_P(WriteBatchWithIndexTest, SingleDeleteTest) {
  Status s;
  std::string value;

  ASSERT_OK(batch_->SingleDelete("A"));

  s = batch_->GetFromBatch(options_, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch_->GetFromBatch(options_, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  batch_->Clear();
  ASSERT_OK(batch_->Put("A", "a"));
  ASSERT_OK(batch_->Put("A", "a2"));
  ASSERT_OK(batch_->Put("B", "b"));
  ASSERT_OK(batch_->SingleDelete("A"));

  s = batch_->GetFromBatch(options_, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch_->GetFromBatch(options_, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);

  ASSERT_OK(batch_->Put("C", "c"));
  ASSERT_OK(batch_->Put("A", "a3"));
  ASSERT_OK(batch_->Delete("B"));
  ASSERT_OK(batch_->SingleDelete("B"));
  ASSERT_OK(batch_->SingleDelete("C"));

  s = batch_->GetFromBatch(options_, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a3", value);
  s = batch_->GetFromBatch(options_, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch_->GetFromBatch(options_, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch_->GetFromBatch(options_, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->Put("B", "b4"));
  ASSERT_OK(batch_->Put("C", "c4"));
  ASSERT_OK(batch_->Put("D", "d4"));
  ASSERT_OK(batch_->SingleDelete("D"));
  ASSERT_OK(batch_->SingleDelete("D"));
  ASSERT_OK(batch_->Delete("A"));

  s = batch_->GetFromBatch(options_, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch_->GetFromBatch(options_, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b4", value);
  s = batch_->GetFromBatch(options_, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c4", value);
  s = batch_->GetFromBatch(options_, "D", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_P(WriteBatchWithIndexTest, SingleDeleteDeltaIterTest) {
  std::string value;
  ASSERT_OK(batch_->Put("A", "a"));
  ASSERT_OK(batch_->Put("A", "a2"));
  ASSERT_OK(batch_->Put("B", "b"));
  ASSERT_OK(batch_->SingleDelete("A"));
  ASSERT_OK(batch_->Delete("B"));

  KVMap map;
  value = PrintContents(batch_.get(), &map, nullptr);
  ASSERT_EQ("", value);

  map["A"] = "aa";
  map["C"] = "cc";
  map["D"] = "dd";

  ASSERT_OK(batch_->SingleDelete("B"));
  ASSERT_OK(batch_->SingleDelete("C"));
  ASSERT_OK(batch_->SingleDelete("Z"));

  value = PrintContents(batch_.get(), &map, nullptr);
  ASSERT_EQ("D:dd,", value);

  ASSERT_OK(batch_->Put("A", "a3"));
  ASSERT_OK(batch_->Put("B", "b3"));
  ASSERT_OK(batch_->SingleDelete("A"));
  ASSERT_OK(batch_->SingleDelete("A"));
  ASSERT_OK(batch_->SingleDelete("D"));
  ASSERT_OK(batch_->SingleDelete("D"));
  ASSERT_OK(batch_->Delete("D"));

  map["E"] = "ee";

  value = PrintContents(batch_.get(), &map, nullptr);
  ASSERT_EQ("B:b3,E:ee,", value);
}

TEST_P(WriteBatchWithIndexTest, MultiGetTest) {
  // MultiGet a lot of keys in order to force std::vector reallocations
  std::vector<std::string> keys;
  for (int i = 0; i < 100; ++i) {
    keys.emplace_back(std::to_string(i));
  }

  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();

  // Write some data to the db for the even numbered keys
  {
    WriteBatch wb;
    for (size_t i = 0; i < keys.size(); i += 2) {
      std::string val = "val" + std::to_string(i);
      ASSERT_OK(wb.Put(cf0, keys[i], val));
    }
    ASSERT_OK(db_->Write(write_opts_, &wb));
    for (size_t i = 0; i < keys.size(); i += 2) {
      std::string value;
      ASSERT_OK(db_->Get(read_opts_, cf0, keys[i], &value));
    }
  }

  // Write some data to the batch
  for (size_t i = 0; i < keys.size(); ++i) {
    if ((i % 5) == 0) {
      ASSERT_OK(batch_->Delete(cf0, keys[i]));
    } else if ((i % 7) == 0) {
      std::string val = "new" + std::to_string(i);
      ASSERT_OK(batch_->Put(cf0, keys[i], val));
    }
    if (i > 0 && (i % 3) == 0) {
      ASSERT_OK(batch_->Merge(cf0, keys[i], "merge"));
    }
  }

  std::vector<Slice> key_slices;
  for (size_t i = 0; i < keys.size(); ++i) {
    key_slices.emplace_back(keys[i]);
  }
  std::vector<PinnableSlice> values(keys.size());
  std::vector<Status> statuses(keys.size());

  batch_->MultiGetFromBatchAndDB(db_, read_opts_, cf0, key_slices.size(),
                                 key_slices.data(), values.data(),
                                 statuses.data(), false);
  for (size_t i = 0; i < keys.size(); ++i) {
    if (i == 0) {
      ASSERT_TRUE(statuses[i].IsNotFound());
    } else if ((i % 3) == 0) {
      ASSERT_OK(statuses[i]);
      if ((i % 5) == 0) {  // Merge after Delete
        ASSERT_EQ(values[i], "merge");
      } else if ((i % 7) == 0) {  // Merge after Put
        std::string val = "new" + std::to_string(i);
        ASSERT_EQ(values[i], val + ",merge");
      } else if ((i % 2) == 0) {
        std::string val = "val" + std::to_string(i);
        ASSERT_EQ(values[i], val + ",merge");
      } else {
        ASSERT_EQ(values[i], "merge");
      }
    } else if ((i % 5) == 0) {
      ASSERT_TRUE(statuses[i].IsNotFound());
    } else if ((i % 7) == 0) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(values[i], "new" + std::to_string(i));
    } else if ((i % 2) == 0) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(values[i], "val" + std::to_string(i));
    } else {
      ASSERT_TRUE(statuses[i].IsNotFound());
    }
  }
}
TEST_P(WriteBatchWithIndexTest, MultiGetTest2) {
  // MultiGet a lot of keys in order to force std::vector reallocations
  const int num_keys = 700;
  const int keys_per_pass = 100;
  std::vector<std::string> keys;
  for (size_t i = 0; i < num_keys; ++i) {
    keys.emplace_back(std::to_string(i));
  }
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();

  // Keys   0- 99 have a PUT in the batch but not DB
  // Keys 100-199 have a PUT in the DB
  // Keys 200-299 Have a PUT/DELETE
  // Keys 300-399 Have a PUT/DELETE/MERGE
  // Keys 400-499 have a PUT/MERGE
  // Keys 500-599 have a MERGE only
  // Keys 600-699 were never written
  {
    WriteBatch wb;
    for (size_t i = 100; i < 500; i++) {
      std::string val = std::to_string(i);
      ASSERT_OK(wb.Put(cf0, keys[i], val));
    }
    ASSERT_OK(db_->Write(write_opts_, &wb));
  }
  ASSERT_OK(db_->Flush(FlushOptions(), cf0));
  for (size_t i = 0; i < 100; i++) {
    ASSERT_OK(batch_->Put(cf0, keys[i], keys[i]));
  }
  for (size_t i = 200; i < 400; i++) {
    ASSERT_OK(batch_->Delete(cf0, keys[i]));
  }
  for (size_t i = 300; i < 600; i++) {
    std::string val = std::to_string(i) + "m";
    ASSERT_OK(batch_->Merge(cf0, keys[i], val));
  }

  Random rnd(301);
  std::vector<PinnableSlice> values(keys_per_pass);
  std::vector<Status> statuses(keys_per_pass);
  for (int pass = 0; pass < 40; pass++) {
    std::vector<Slice> key_slices;
    for (size_t i = 0; i < keys_per_pass; i++) {
      int random = rnd.Uniform(num_keys);
      key_slices.emplace_back(keys[random]);
    }
    batch_->MultiGetFromBatchAndDB(db_, read_opts_, cf0, keys_per_pass,
                                   key_slices.data(), values.data(),
                                   statuses.data(), false);
    for (size_t i = 0; i < keys_per_pass; i++) {
      int key = ParseInt(key_slices[i].ToString());
      switch (key / 100) {
        case 0:  // 0-99 PUT only
          ASSERT_OK(statuses[i]);
          ASSERT_EQ(values[i], key_slices[i].ToString());
          break;
        case 1:  // 100-199 PUT only
          ASSERT_OK(statuses[i]);
          ASSERT_EQ(values[i], key_slices[i].ToString());
          break;
        case 2:  // 200-299 Deleted
          ASSERT_TRUE(statuses[i].IsNotFound());
          break;
        case 3:  // 300-399 Delete+Merge
          ASSERT_OK(statuses[i]);
          ASSERT_EQ(values[i], key_slices[i].ToString() + "m");
          break;
        case 4:  // 400-400 Put+ Merge
          ASSERT_OK(statuses[i]);
          ASSERT_EQ(values[i], key_slices[i].ToString() + "," +
                                   key_slices[i].ToString() + "m");
          break;
        case 5:  // Merge only
          ASSERT_OK(statuses[i]);
          ASSERT_EQ(values[i], key_slices[i].ToString() + "m");
          break;
        case 6:  // Never written
          ASSERT_TRUE(statuses[i].IsNotFound());
          break;
        default:
          assert(false);
      }  // end switch
    }    // End for each key
  }      // end for passes
}

// This test has merges, but the merge does not play into the final result
TEST_P(WriteBatchWithIndexTest, FakeMergeWithIteratorTest) {
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();

  // The map we are starting with
  KVMap input = {
      {"odm", "odm0"},
      {"omd", "omd0"},
      {"omp", "omp0"},
  };
  KVMap result = {
      {"odm", "odm2"},  // Orig, Delete, Merge
      {"mp", "mp1"},    // Merge, Put
      {"omp", "omp2"},  // Origi, Merge, Put
      {"mmp", "mmp2"}   // Merge, Merge, Put
  };

  for (auto& iter : result) {
    EXPECT_EQ(AddToBatch(cf0, iter.first), iter.second);
  }
  AddToBatch(cf0, "md");   // Merge, Delete
  AddToBatch(cf0, "mmd");  // Merge, Merge, Delete
  AddToBatch(cf0, "omd");  // Orig, Merge, Delete

  KVIter kvi(&result);
  // First try just the batch
  std::unique_ptr<Iterator> iter(
      batch_->NewIteratorWithBase(cf0, new KVIter(&input)));
  AssertItersEqual(iter.get(), &kvi);
}

TEST_P(WriteBatchWithIndexTest, IteratorMergeTest) {
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();

  KVMap result = {
      {"m", "m0"},                // Merge
      {"mm", "mm0,mm1"},          // Merge, Merge
      {"dm", "dm1"},              // Delete, Merge
      {"dmm", "dmm1,dmm2"},       // Delete, Merge, Merge
      {"mdm", "mdm2"},            // Merge, Delete, Merge
      {"mpm", "mpm1,mpm2"},       // Merge, Put, Merge
      {"pm", "pm0,pm1"},          // Put, Merge
      {"pmm", "pmm0,pmm1,pmm2"},  // Put, Merge, Merge
  };

  for (auto& iter : result) {
    EXPECT_EQ(AddToBatch(cf0, iter.first), iter.second);
  }

  KVIter kvi(&result);
  // First try just the batch
  KVMap empty_map;
  std::unique_ptr<Iterator> iter(
      batch_->NewIteratorWithBase(cf0, new KVIter(&empty_map)));
  AssertItersEqual(iter.get(), &kvi);
}

TEST_P(WriteBatchWithIndexTest, IteratorMergeTestWithOrig) {
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();
  KVMap original;
  KVMap results = {
      {"m", "om,m0"},             // Merge
      {"mm", "omm,mm0,mm1"},      // Merge, Merge
      {"dm", "dm1"},              // Delete, Merge
      {"dmm", "dmm1,dmm2"},       // Delete, Merge, Merge
      {"mdm", "mdm2"},            // Merge, Delete, Merge
      {"mpm", "mpm1,mpm2"},       // Merge, Put, Merge
      {"pm", "pm0,pm1"},          // Put, Merge
      {"pmm", "pmm0,pmm1,pmm2"},  // Put, Merge, Merge
  };

  for (auto& iter : results) {
    AddToBatch(cf0, iter.first);
    original[iter.first] = "o" + iter.first;
  }

  KVIter kvi(&results);
  // First try just the batch
  std::unique_ptr<Iterator> iter(
      batch_->NewIteratorWithBase(cf0, new KVIter(&original)));
  AssertItersEqual(iter.get(), &kvi);
}

TEST_P(WriteBatchWithIndexTest, GetFromBatchAfterMerge) {
  std::string value;
  Status s;

  ASSERT_OK(OpenDB());
  ASSERT_OK(db_->Put(write_opts_, "o", "aa"));
  batch_->Merge("o", "bb");  // Merging bb under key "o"
  batch_->Merge("m", "cc");  // Merging bc under key "m"
  s = batch_->GetFromBatch(options_, "m", &value);
  ASSERT_EQ(s.code(), Status::Code::kMergeInProgress);
  s = batch_->GetFromBatch(options_, "o", &value);
  ASSERT_EQ(s.code(), Status::Code::kMergeInProgress);

  ASSERT_OK(db_->Write(write_opts_, batch_->GetWriteBatch()));
  ASSERT_OK(db_->Get(read_opts_, "o", &value));
  ASSERT_EQ(value, "aa,bb");
  ASSERT_OK(db_->Get(read_opts_, "m", &value));
  ASSERT_EQ(value, "cc");
}

TEST_P(WriteBatchWithIndexTest, GetFromBatchAndDBAfterMerge) {
  std::string value;

  ASSERT_OK(OpenDB());
  ASSERT_OK(db_->Put(write_opts_, "o", "aa"));
  ASSERT_OK(batch_->Merge("o", "bb"));  // Merging bb under key "o"
  ASSERT_OK(batch_->Merge("m", "cc"));  // Merging bc under key "m"
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "o", &value));
  ASSERT_EQ(value, "aa,bb");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "m", &value));
  ASSERT_EQ(value, "cc");
}

TEST_F(WBWIKeepTest, GetAfterPut) {
  std::string value;
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();

  ASSERT_OK(db_->Put(write_opts_, "key", "orig"));

  ASSERT_OK(batch_->Put("key", "aa"));  // Writing aa under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "aa");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "key", &value));
  ASSERT_EQ(value, "aa");

  ASSERT_OK(batch_->Merge("key", "bb"));  // Merging bb under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "aa,bb");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "key", &value));
  ASSERT_EQ(value, "aa,bb");

  ASSERT_OK(batch_->Merge("key", "cc"));  // Merging cc under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "aa,bb,cc");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "key", &value));
  ASSERT_EQ(value, "aa,bb,cc");
}

TEST_P(WriteBatchWithIndexTest, GetAfterMergePut) {
  std::string value;
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();
  ASSERT_OK(db_->Put(write_opts_, "key", "orig"));

  ASSERT_OK(batch_->Merge("key", "aa"));  // Merging aa under key
  Status s = batch_->GetFromBatch(cf0, options_, "key", &value);
  ASSERT_EQ(s.code(), Status::Code::kMergeInProgress);
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "key", &value));
  ASSERT_EQ(value, "orig,aa");

  ASSERT_OK(batch_->Merge("key", "bb"));  // Merging bb under key
  s = batch_->GetFromBatch(cf0, options_, "key", &value);
  ASSERT_EQ(s.code(), Status::Code::kMergeInProgress);
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "key", &value));
  ASSERT_EQ(value, "orig,aa,bb");

  ASSERT_OK(batch_->Put("key", "cc"));  // Writing cc under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "cc");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "key", &value));
  ASSERT_EQ(value, "cc");

  ASSERT_OK(batch_->Merge("key", "dd"));  // Merging dd under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "cc,dd");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "key", &value));
  ASSERT_EQ(value, "cc,dd");
}

TEST_P(WriteBatchWithIndexTest, GetAfterMergeDelete) {
  std::string value;
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();

  ASSERT_OK(batch_->Merge("key", "aa"));  // Merging aa under key
  Status s = batch_->GetFromBatch(cf0, options_, "key", &value);
  ASSERT_EQ(s.code(), Status::Code::kMergeInProgress);
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "key", &value));
  ASSERT_EQ(value, "aa");

  ASSERT_OK(batch_->Merge("key", "bb"));  // Merging bb under key
  s = batch_->GetFromBatch(cf0, options_, "key", &value);
  ASSERT_EQ(s.code(), Status::Code::kMergeInProgress);
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "key", &value));
  ASSERT_EQ(value, "aa,bb");

  ASSERT_OK(batch_->Delete("key"));  // Delete key from batch
  s = batch_->GetFromBatch(cf0, options_, "key", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch_->GetFromBatchAndDB(db_, read_opts_, "key", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->Merge("key", "cc"));  // Merging cc under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "cc");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "key", &value));
  ASSERT_EQ(value, "cc");
  ASSERT_OK(batch_->Merge("key", "dd"));  // Merging dd under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "cc,dd");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "key", &value));
  ASSERT_EQ(value, "cc,dd");
}

TEST_F(WBWIOverwriteTest, TestBadMergeOperator) {
  class FailingMergeOperator : public MergeOperator {
   public:
    FailingMergeOperator() {}

    bool FullMergeV2(const MergeOperationInput& /*merge_in*/,
                     MergeOperationOutput* /*merge_out*/) const override {
      return false;
    }

    const char* Name() const override { return "Failing"; }
  };
  options_.merge_operator.reset(new FailingMergeOperator());
  ASSERT_OK(OpenDB());

  ColumnFamilyHandle* column_family = db_->DefaultColumnFamily();
  std::string value;

  ASSERT_OK(db_->Put(write_opts_, "a", "a0"));
  ASSERT_OK(batch_->Put("b", "b0"));

  ASSERT_OK(batch_->Merge("a", "a1"));
  ASSERT_NOK(batch_->GetFromBatchAndDB(db_, read_opts_, "a", &value));
  ASSERT_NOK(batch_->GetFromBatch(column_family, options_, "a", &value));
  ASSERT_OK(batch_->GetFromBatchAndDB(db_, read_opts_, "b", &value));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "b", &value));
}

INSTANTIATE_TEST_CASE_P(WBWI, WriteBatchWithIndexTest, testing::Bool());
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
