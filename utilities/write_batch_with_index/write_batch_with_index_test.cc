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
  explicit ColumnFamilyHandleImplDummy(int id)
      : ColumnFamilyHandleImpl(nullptr, nullptr, nullptr), id_(id) {}
  uint32_t GetID() const override { return id_; }

 private:
  uint32_t id_;
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

class WriteBatchWithIndexTest {};

TEST(WriteBatchWithIndexTest, TestValueAsSecondaryIndex) {
  Entry entries[] = {{"aaa", "0005", kPutRecord},
                     {"b", "0002", kPutRecord},
                     {"cdd", "0002", kMergeRecord},
                     {"aab", "00001", kPutRecord},
                     {"cc", "00005", kPutRecord},
                     {"cdd", "0002", kPutRecord},
                     {"aab", "0003", kPutRecord},
                     {"cc", "00005", kDeleteRecord}, };

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

  WriteBatchWithIndex batch(BytewiseComparator(), 20);
  ColumnFamilyHandleImplDummy data(6), index(8);
  for (auto& e : entries) {
    if (e.type == kPutRecord) {
      batch.Put(&data, e.key, e.value);
      batch.Put(&index, e.value, e.key);
    } else if (e.type == kMergeRecord) {
      batch.Merge(&data, e.key, e.value);
      batch.Put(&index, e.value, e.key);
    } else {
      assert(e.type == kDeleteRecord);
      std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&data));
      iter->Seek(e.key);
      ASSERT_OK(iter->status());
      auto& write_entry = iter->Entry();
      ASSERT_EQ(e.key, write_entry.key.ToString());
      ASSERT_EQ(e.value, write_entry.value.ToString());
      batch.Delete(&data, e.key);
      batch.Put(&index, e.value, "");
    }
  }

  // Iterator all keys
  {
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&data));
    iter->Seek("");
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

  // Iterator all indexes
  {
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&index));
    iter->Seek("");
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

  // Seek to every key
  {
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&data));

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
    std::unique_ptr<WBWIIterator> iter(batch.NewIterator(&index));

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
  batch.GetWriteBatch()->Iterate(&handler);

  // Verify data column family
  {
    ASSERT_EQ(sizeof(entries) / sizeof(Entry),
              handler.seen[data.GetID()].size());
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
    ASSERT_EQ(sizeof(entries) / sizeof(Entry),
              handler.seen[index.GetID()].size());
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

}  // namespace

int main(int argc, char** argv) { return rocksdb::test::RunAllTests(); }
