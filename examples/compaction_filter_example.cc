// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <rocksdb/compaction_filter.h>
#include <rocksdb/db.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>

class MyMerge : public rocksdb::MergeOperator {
 public:
  bool FullMerge(const rocksdb::Slice& key,
                 const rocksdb::Slice* existing_value,
                 const std::deque<std::string>& operand_list,
                 std::string* new_value,
                 rocksdb::Logger* logger) const override {
    new_value->clear();
    if (existing_value != nullptr) {
      new_value->assign(existing_value->data(), existing_value->size());
    }
    for (const std::string& m : operand_list) {
      fprintf(stderr, "Merge(%s)\n", m.c_str());
      assert(m != "bad");  // the compaction filter filters out bad values
      new_value->assign(m);
    }
    return true;
  }

  const char* Name() const override { return "MyMerge"; }
};

class MyFilter : public rocksdb::CompactionFilter {
 public:
  bool Filter(int level, const rocksdb::Slice& key,
              const rocksdb::Slice& existing_value, std::string* new_value,
              bool* value_changed) const override {
    fprintf(stderr, "Filter(%s)\n", key.ToString().c_str());
    ++count_;
    assert(*value_changed == false);
    return false;
  }

  bool FilterMergeOperand(int level, const rocksdb::Slice& key,
                          const rocksdb::Slice& existing_value) const override {
    fprintf(stderr, "FilterMerge(%s)\n", key.ToString().c_str());
    ++merge_count_;
    return existing_value == "bad";
  }

  const char* Name() const override { return "MyFilter"; }

  mutable int count_ = 0;
  mutable int merge_count_ = 0;
};

int main() {
  rocksdb::DB* raw_db;
  rocksdb::Status status;

  MyFilter filter;

  system("rm -rf /tmp/rocksmergetest");
  rocksdb::Options options;
  options.create_if_missing = true;
  options.merge_operator.reset(new MyMerge);
  options.compaction_filter = &filter;
  status = rocksdb::DB::Open(options, "/tmp/rocksmergetest", &raw_db);
  assert(status.ok());
  std::unique_ptr<rocksdb::DB> db(raw_db);

  rocksdb::WriteOptions wopts;
  db->Merge(wopts, "0", "bad");  // This is filtered out
  db->Merge(wopts, "1", "data1");
  db->Merge(wopts, "1", "bad");
  db->Merge(wopts, "1", "data2");
  db->Merge(wopts, "1", "bad");
  db->Merge(wopts, "3", "data3");
  db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
  fprintf(stderr, "filter.count_ = %d\n", filter.count_);
  assert(filter.count_ == 1);
  fprintf(stderr, "filter.merge_count_ = %d\n", filter.merge_count_);
  assert(filter.merge_count_ == 5);
}
