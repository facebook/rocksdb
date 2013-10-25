// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "rocksdb/db.h"

namespace rocksdb {

// This class contains APIs to stack rocksdb wrappers.Eg. Stack TTL over base d
class StackableDB : public DB {
 public:
  explicit StackableDB(StackableDB* sdb) : sdb_(sdb) {}

  // Returns the DB object that is the lowermost component in the stack of DBs
  virtual DB* GetRawDB() {
    return sdb_->GetRawDB();
  }

  // convert a DB to StackableDB
  // TODO: This function does not work yet. Passing nullptr to StackableDB in
  //       NewStackableDB's constructor will cause segfault on object's usage
  static StackableDB* DBToStackableDB(DB* db) {
    class NewStackableDB : public StackableDB {
     public:
      NewStackableDB(DB* db)
        : StackableDB(nullptr),
          db_(db) {}

      DB* GetRawDB() {
        return db_;
      }

     private:
      DB* db_;
    };
    return new NewStackableDB(db);
  }

  virtual Status Put(const WriteOptions& options,
                     const Slice& key,
                     const Slice& val) override {
    return sdb_->Put(options, key, val);
  }

  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value) override {
    return sdb_->Get(options, key, value);
  }

  virtual std::vector<Status> MultiGet(const ReadOptions& options,
                                       const std::vector<Slice>& keys,
                                       std::vector<std::string>* values)
    override {
      return sdb_->MultiGet(options, keys, values);
  }

  virtual bool KeyMayExist(const ReadOptions& options,
                           const Slice& key,
                           std::string* value,
                           bool* value_found = nullptr) override {
    return sdb_->KeyMayExist(options, key, value, value_found);
  }

  virtual Status Delete(const WriteOptions& wopts, const Slice& key) override {
    return sdb_->Delete(wopts, key);
  }

  virtual Status Merge(const WriteOptions& options,
                       const Slice& key,
                       const Slice& value) override {
    return sdb_->Merge(options, key, value);
  }


  virtual Status Write(const WriteOptions& opts, WriteBatch* updates)
    override {
      return sdb_->Write(opts, updates);
  }

  virtual Iterator* NewIterator(const ReadOptions& opts) override {
    return sdb_->NewIterator(opts);
  }

  virtual const Snapshot* GetSnapshot() override {
    return sdb_->GetSnapshot();
  }

  virtual void ReleaseSnapshot(const Snapshot* snapshot) override {
    return sdb_->ReleaseSnapshot(snapshot);
  }

  virtual bool GetProperty(const Slice& property, std::string* value)
    override {
      return sdb_->GetProperty(property, value);
  }

  virtual void GetApproximateSizes(const Range* r, int n, uint64_t* sizes)
    override {
      return sdb_->GetApproximateSizes(r, n, sizes);
  }

  virtual void CompactRange(const Slice* begin, const Slice* end,
                            bool reduce_level = false,
                            int target_level = -1) override {
    return sdb_->CompactRange(begin, end, reduce_level, target_level);
  }

  virtual int NumberLevels() override {
    return sdb_->NumberLevels();
  }

  virtual int MaxMemCompactionLevel() override {
    return sdb_->MaxMemCompactionLevel();
  }

  virtual int Level0StopWriteTrigger() override {
    return sdb_->Level0StopWriteTrigger();
  }

  virtual Status Flush(const FlushOptions& fopts) override {
    return sdb_->Flush(fopts);
  }

  virtual Status DisableFileDeletions() override {
    return sdb_->DisableFileDeletions();
  }

  virtual Status EnableFileDeletions() override {
    return sdb_->EnableFileDeletions();
  }

  virtual Status GetLiveFiles(std::vector<std::string>& vec, uint64_t* mfs,
                              bool flush_memtable = true) override {
      return sdb_->GetLiveFiles(vec, mfs, flush_memtable);
  }

  virtual SequenceNumber GetLatestSequenceNumber() const override {
    return sdb_->GetLatestSequenceNumber();
  }

  virtual Status GetSortedWalFiles(VectorLogPtr& files) override {
    return sdb_->GetSortedWalFiles(files);
  }

  virtual Status DeleteFile(std::string name) override {
    return sdb_->DeleteFile(name);
  }

  virtual Status GetUpdatesSince(SequenceNumber seq_number,
                                 unique_ptr<TransactionLogIterator>* iter)
    override {
      return sdb_->GetUpdatesSince(seq_number, iter);
  }

 protected:
  StackableDB* sdb_;
};

} //  namespace rocksdb
