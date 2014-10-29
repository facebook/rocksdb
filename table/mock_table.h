// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once
#include <algorithm>
#include <set>
#include <memory>
#include <atomic>
#include <map>
#include <string>

#include "rocksdb/table.h"
#include "table/table_reader.h"
#include "table/table_builder.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

// NOTE this currently only supports bitwise comparator

struct MockTableFileSystem {
  port::Mutex mutex;
  std::map<uint32_t, std::map<std::string, std::string>> files;
};

class MockTableReader : public TableReader {
 public:
  MockTableReader(const std::map<std::string, std::string>& table)
      : table_(table) {}

  Iterator* NewIterator(const ReadOptions&, Arena* arena) override;

  Status Get(const ReadOptions&, const Slice& key,
             GetContext* get_context) override;

  uint64_t ApproximateOffsetOf(const Slice& key) override { return 0; }

  virtual size_t ApproximateMemoryUsage() const override { return 0; }

  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties> GetTableProperties() const override;

  ~MockTableReader() {}

 private:
  const std::map<std::string, std::string>& table_;
};

class MockTableIterator : public Iterator {
 public:
  explicit MockTableIterator(const std::map<std::string, std::string>& table)
      : table_(table) {
    itr_ = table_.end();
  }

  bool Valid() const { return itr_ == table_.end(); }

  void SeekToFirst() { itr_ = table_.begin(); }

  void SeekToLast() {
    itr_ = table_.end();
    --itr_;
  }

  void Seek(const Slice& target) {
    std::string str_target(target.data(), target.size());
    itr_ = table_.lower_bound(str_target);
  }

  void Next() { ++itr_; }

  void Prev() {
    if (itr_ == table_.begin()) {
      itr_ = table_.end();
    } else {
      --itr_;
    }
  }

  Slice key() const { return Slice(itr_->first); }

  Slice value() const { return Slice(itr_->second); }

  Status status() const { return Status::OK(); }

 private:
  const std::map<std::string, std::string>& table_;
  std::map<std::string, std::string>::const_iterator itr_;
};

class MockTableBuilder : public TableBuilder {
 public:
  MockTableBuilder(uint32_t id, MockTableFileSystem* file_system)
      : id_(id), file_system_(file_system) {}

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~MockTableBuilder() {}

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override {
    table_.insert({key.ToString(), value.ToString()});
  }

  // Return non-ok iff some error has been detected.
  Status status() const override { return Status::OK(); }

  Status Finish() override {
    MutexLock lock_guard(&file_system_->mutex);
    file_system_->files.insert({id_, table_});
    return Status::OK();
  }

  void Abandon() override {}

  uint64_t NumEntries() const override { return table_.size(); }

  uint64_t FileSize() const override { return table_.size(); }

 private:
  uint32_t id_;
  MockTableFileSystem* file_system_;
  std::map<std::string, std::string> table_;
};

class MockTableFactory : public TableFactory {
 public:
  MockTableFactory();
  const char* Name() const override { return "MockTable"; }
  Status NewTableReader(const ImmutableCFOptions& ioptions,
                        const EnvOptions& env_options,
                        const InternalKeyComparator& internal_key,
                        unique_ptr<RandomAccessFile>&& file, uint64_t file_size,
                        unique_ptr<TableReader>* table_reader) const;

  TableBuilder* NewTableBuilder(
      const ImmutableCFOptions& ioptions,
      const InternalKeyComparator& internal_key, WritableFile* file,
      const CompressionType compression_type,
      const CompressionOptions& compression_opts) const;

  virtual Status SanitizeOptions(const DBOptions& db_opts,
                                 const ColumnFamilyOptions& cf_opts) const {
    return Status::OK();
  }

  virtual std::string GetPrintableTableOptions() const override {
    return std::string();
  }

  // This function will assert that only a single file exists and that the
  // contents are equal to file_contents
  void AssertSingleFile(
      const std::map<std::string, std::string>& file_contents);

 private:
  uint32_t GetAndWriteNextID(WritableFile* file) const;
  uint32_t GetIDFromFile(RandomAccessFile* file) const;

  mutable MockTableFileSystem file_system_;
  mutable std::atomic<uint32_t> next_id_;
};

}  // namespace rocksdb
