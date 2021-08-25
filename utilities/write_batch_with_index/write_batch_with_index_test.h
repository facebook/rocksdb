//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

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

}  // namespace

class WBWIBaseTest : public testing::Test {
 public:
  explicit WBWIBaseTest(std::string databaseNamePrefix, bool overwrite)
      : db_(nullptr) {
    options_.merge_operator =
        MergeOperators::CreateFromStringId("stringappend");
    options_.create_if_missing = true;
    dbname_ = test::PerThreadDBPath(databaseNamePrefix);
    DestroyDB(dbname_, options_);
    batch_.reset(new WriteBatchWithIndex(BytewiseComparator(), 20, overwrite));
  }

  virtual ~WBWIBaseTest() {
    batch_.reset();
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
  WBWIKeepTest() : WBWIBaseTest("write_batch_with_index_test", false) {}
};

class WBWIOverwriteTest : public WBWIBaseTest {
 public:
  WBWIOverwriteTest() : WBWIBaseTest("write_batch_with_index_test", true) {}
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE
