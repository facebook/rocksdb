//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.


#include <memory>
#include <map>
#include "db/column_family.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/testharness.h"

namespace rocksdb {

namespace {
class ColumnFamilyHandleImplDummy : public ColumnFamilyHandleImpl {
 public:
  explicit ColumnFamilyHandleImplDummy(int id, const Comparator* comparator)
      : ColumnFamilyHandleImpl(nullptr, nullptr, nullptr),
        id_(id),
        comparator_(comparator) {}
  uint32_t GetID() const override { return id_; }
  const Comparator* user_comparator() const override { return comparator_; }

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
  virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                       const Slice& value) {
    Entry e;
    e.key = key.ToString();
    e.value = value.ToString();
    e.type = kPutRecord;
    seen[column_family_id].push_back(e);
    return Status::OK();
  }
  virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) {
    Entry e;
    e.key = key.ToString();
    e.value = value.ToString();
    e.type = kMergeRecord;
    seen[column_family_id].push_back(e);
    return Status::OK();
  }
  virtual void LogData(const Slice& blob) {}
  virtual Status DeleteCF(uint32_t column_family_id, const Slice& key) {
    Entry e;
    e.key = key.ToString();
    e.value = "";
    e.type = kDeleteRecord;
    seen[column_family_id].push_back(e);
    return Status::OK();
  }
};
}  // namespace anonymous

class WriteBatchWithIndexTest : public testing::Test {};

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
      batch->Put(&data, e.key, e.value);
      batch->Put(&index, e.value, e.key);
    } else if (e.type == kMergeRecord) {
      batch->Merge(&data, e.key, e.value);
      batch->Put(&index, e.value, e.key);
    } else {
      assert(e.type == kDeleteRecord);
      std::unique_ptr<WBWIIterator> iter(batch->NewIterator(&data));
      iter->Seek(e.key);
      ASSERT_OK(iter->status());
      auto& write_entry = iter->Entry();
      ASSERT_EQ(e.key, write_entry.key.ToString());
      ASSERT_EQ(e.value, write_entry.value.ToString());
      batch->Delete(&data, e.key);
      batch->Put(&index, e.value, "");
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
          auto& write_entry = iter->Entry();
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
        auto& write_entry = iter->Entry();
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
          auto& write_entry = iter->Entry();
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
        auto& write_entry = iter->Entry();
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
        auto& write_entry = iter->Entry();
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
        auto& write_entry = iter->Entry();
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
  batch->GetWriteBatch()->Iterate(&handler);

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

TEST_F(WriteBatchWithIndexTest, TestValueAsSecondaryIndex) {
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

  WriteBatchWithIndex batch(nullptr, 20);

  TestValueAsSecondaryIndexHelper(entries_list, &batch);

  // Clear batch and re-run test with new values
  batch.Clear();

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

  TestValueAsSecondaryIndexHelper(entries_list, &batch);
}

TEST_F(WriteBatchWithIndexTest, TestComparatorForCF) {
  ColumnFamilyHandleImplDummy cf1(6, nullptr);
  ColumnFamilyHandleImplDummy reverse_cf(66, ReverseBytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(88, BytewiseComparator());
  WriteBatchWithIndex batch(BytewiseComparator(), 20);

  batch.Put(&cf1, "ddd", "");
  batch.Put(&cf2, "aaa", "");
  batch.Put(&cf2, "eee", "");
  batch.Put(&cf1, "ccc", "");
  batch.Put(&reverse_cf, "a11", "");
  batch.Put(&cf1, "bbb", "");

  Slice key_slices[] = {"a", "3", "3"};
  Slice value_slice = "";
  batch.Put(&reverse_cf, SliceParts(key_slices, 3),
            SliceParts(&value_slice, 1));
  batch.Put(&reverse_cf, "a22", "");

  {
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&cf1));
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
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&cf2));
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
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&reverse_cf));
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

TEST_F(WriteBatchWithIndexTest, TestOverwriteKey) {
  ColumnFamilyHandleImplDummy cf1(6, nullptr);
  ColumnFamilyHandleImplDummy reverse_cf(66, ReverseBytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(88, BytewiseComparator());
  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);

  batch.Put(&cf1, "ddd", "");
  batch.Merge(&cf1, "ddd", "");
  batch.Delete(&cf1, "ddd");
  batch.Put(&cf2, "aaa", "");
  batch.Delete(&cf2, "aaa");
  batch.Put(&cf2, "aaa", "aaa");
  batch.Put(&cf2, "eee", "eee");
  batch.Put(&cf1, "ccc", "");
  batch.Put(&reverse_cf, "a11", "");
  batch.Delete(&cf1, "ccc");
  batch.Put(&reverse_cf, "a33", "a33");
  batch.Put(&reverse_cf, "a11", "a11");
  Slice slices[] = {"a", "3", "3"};
  batch.Delete(&reverse_cf, SliceParts(slices, 3));

  {
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&cf1));
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
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&cf2));
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
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&reverse_cf));
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

namespace {
typedef std::map<std::string, std::string> KVMap;

class KVIter : public Iterator {
 public:
  explicit KVIter(const KVMap* map) : map_(map), iter_(map_->end()) {}
  virtual bool Valid() const { return iter_ != map_->end(); }
  virtual void SeekToFirst() { iter_ = map_->begin(); }
  virtual void SeekToLast() {
    if (map_->empty()) {
      iter_ = map_->end();
    } else {
      iter_ = map_->find(map_->rbegin()->first);
    }
  }
  virtual void Seek(const Slice& k) { iter_ = map_->lower_bound(k.ToString()); }
  virtual void Next() { ++iter_; }
  virtual void Prev() {
    if (iter_ == map_->begin()) {
      iter_ = map_->end();
      return;
    }
    --iter_;
  }

  virtual Slice key() const { return iter_->first; }
  virtual Slice value() const { return iter_->second; }
  virtual Status status() const { return Status::OK(); }

 private:
  const KVMap* const map_;
  KVMap::const_iterator iter_;
};

void AssertIter(Iterator* iter, const std::string& key,
                const std::string& value) {
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(key, iter->key().ToString());
  ASSERT_EQ(value, iter->value().ToString());
}

void AssertItersEqual(Iterator* iter1, Iterator* iter2) {
  ASSERT_EQ(iter1->Valid(), iter2->Valid());
  if (iter1->Valid()) {
    ASSERT_EQ(iter1->key().ToString(), iter2->key().ToString());
    ASSERT_EQ(iter1->value().ToString(), iter2->value().ToString());
  }
}
}  // namespace

TEST_F(WriteBatchWithIndexTest, TestRandomIteraratorWithBase) {
  std::vector<std::string> source_strings = {"a", "b", "c", "d", "e",
                                             "f", "g", "h", "i", "j"};
  for (int rand_seed = 301; rand_seed < 366; rand_seed++) {
    Random rnd(rand_seed);

    ColumnFamilyHandleImplDummy cf1(6, BytewiseComparator());
    ColumnFamilyHandleImplDummy cf2(2, BytewiseComparator());
    ColumnFamilyHandleImplDummy cf3(8, BytewiseComparator());

    WriteBatchWithIndex batch(BytewiseComparator(), 20, true);

    if (rand_seed % 2 == 0) {
      batch.Put(&cf2, "zoo", "bar");
    }
    if (rand_seed % 4 == 1) {
      batch.Put(&cf3, "zoo", "bar");
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
          batch.Put(&cf1, key, value);
          map[key] = value;
          merged_map[key] = value;
          break;
        case 2:
          // both has it. Delta should win
          batch.Put(&cf1, key, value);
          map[key] = "wrong_value";
          merged_map[key] = value;
          break;
        case 3:
          // both has it. Delta is delete
          batch.Delete(&cf1, key);
          map[key] = "wrong_value";
          break;
        case 4:
          // only delta has it. Delta is delete
          batch.Delete(&cf1, key);
          map[key] = "wrong_value";
          break;
        default:
          // Neither iterator has it.
          break;
      }
    }

    std::unique_ptr<Iterator> iter(
        batch.NewIteratorWithBase(&cf1, new KVIter(&map)));
    std::unique_ptr<Iterator> result_iter(new KVIter(&merged_map));

    bool is_valid = false;
    for (int i = 0; i < 128; i++) {
      // Random walk and make sure iter and result_iter returns the
      // same key and value
      int type = rnd.Uniform(5);
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
        case 3:
          // Next
          if (is_valid) {
            iter->Next();
            result_iter->Next();
          } else {
            continue;
          }
          break;
        default:
          assert(type == 4);
          // Prev
          if (is_valid) {
            iter->Prev();
            result_iter->Prev();
          } else {
            continue;
          }
          break;
      }
      AssertItersEqual(iter.get(), result_iter.get());
      is_valid = iter->Valid();
    }
  }
}

TEST_F(WriteBatchWithIndexTest, TestIteraratorWithBase) {
  ColumnFamilyHandleImplDummy cf1(6, BytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(2, BytewiseComparator());
  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);

  {
    KVMap map;
    map["a"] = "aa";
    map["c"] = "cc";
    map["e"] = "ee";
    std::unique_ptr<Iterator> iter(
        batch.NewIteratorWithBase(&cf1, new KVIter(&map)));

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
  batch.Put(&cf2, "zoo", "bar");
  batch.Put(&cf1, "a", "aa");
  {
    KVMap empty_map;
    std::unique_ptr<Iterator> iter(
        batch.NewIteratorWithBase(&cf1, new KVIter(&empty_map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  batch.Delete(&cf1, "b");
  batch.Put(&cf1, "c", "cc");
  batch.Put(&cf1, "d", "dd");
  batch.Delete(&cf1, "e");

  {
    KVMap map;
    map["b"] = "";
    map["cc"] = "cccc";
    map["f"] = "ff";
    std::unique_ptr<Iterator> iter(
        batch.NewIteratorWithBase(&cf1, new KVIter(&map)));

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
        batch.NewIteratorWithBase(&cf1, new KVIter(&empty_map)));

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

TEST_F(WriteBatchWithIndexTest, TestIteraratorWithBaseReverseCmp) {
  ColumnFamilyHandleImplDummy cf1(6, ReverseBytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(2, ReverseBytewiseComparator());
  WriteBatchWithIndex batch(BytewiseComparator(), 20, true);

  // Test the case that there is one element in the write batch
  batch.Put(&cf2, "zoo", "bar");
  batch.Put(&cf1, "a", "aa");
  {
    KVMap empty_map;
    std::unique_ptr<Iterator> iter(
        batch.NewIteratorWithBase(&cf1, new KVIter(&empty_map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  batch.Put(&cf1, "c", "cc");
  {
    KVMap map;
    std::unique_ptr<Iterator> iter(
        batch.NewIteratorWithBase(&cf1, new KVIter(&map)));

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
  batch.Put("a", "b");
  {
    KVMap map;
    map["b"] = "";
    std::unique_ptr<Iterator> iter(batch.NewIteratorWithBase(new KVIter(&map)));

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

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
