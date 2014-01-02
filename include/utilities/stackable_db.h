// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "rocksdb/db.h"

namespace rocksdb {

// This class contains APIs to stack rocksdb wrappers.Eg. Stack TTL over base d
class StackableDB : public DB {
 public:
  // StackableDB is the owner of db now!
  explicit StackableDB(DB* db) : db_(db) {}

  ~StackableDB() {
    delete db_;
  }

  virtual DB* GetBaseDB() {
    return db_;
  }

  using DB::Put;
  virtual Status Put(const WriteOptions& options,
                     const ColumnFamilyHandle& column_family, const Slice& key,
                     const Slice& val) override {
    return db_->Put(options, column_family, key, val);
  }

  using DB::Get;
  virtual Status Get(const ReadOptions& options,
                     const ColumnFamilyHandle& column_family, const Slice& key,
                     std::string* value) override {
    return db_->Get(options, column_family, key, value);
  }

  using DB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override {
    return db_->MultiGet(options, column_family, keys, values);
  }

  using DB::KeyMayExist;
  virtual bool KeyMayExist(const ReadOptions& options,
                           const ColumnFamilyHandle& column_family,
                           const Slice& key, std::string* value,
                           bool* value_found = nullptr) override {
    return db_->KeyMayExist(options, column_family, key, value, value_found);
  }

  using DB::Delete;
  virtual Status Delete(const WriteOptions& wopts,
                        const ColumnFamilyHandle& column_family,
                        const Slice& key) override {
    return db_->Delete(wopts, column_family, key);
  }

  using DB::Merge;
  virtual Status Merge(const WriteOptions& options,
                       const ColumnFamilyHandle& column_family,
                       const Slice& key, const Slice& value) override {
    return db_->Merge(options, column_family, key, value);
  }


  virtual Status Write(const WriteOptions& opts, WriteBatch* updates)
    override {
      return db_->Write(opts, updates);
  }

  using DB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& opts,
                                const ColumnFamilyHandle& column_family)
      override {
    return db_->NewIterator(opts, column_family);
  }

  virtual Status NewIterators(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle>& column_family,
      std::vector<Iterator*>* iterators) {
    return db_->NewIterators(options, column_family, iterators);
  }


  virtual const Snapshot* GetSnapshot() override {
    return db_->GetSnapshot();
  }

  virtual void ReleaseSnapshot(const Snapshot* snapshot) override {
    return db_->ReleaseSnapshot(snapshot);
  }

  using DB::GetProperty;
  virtual bool GetProperty(const ColumnFamilyHandle& column_family,
                           const Slice& property, std::string* value) override {
      return db_->GetProperty(column_family, property, value);
  }

  using DB::GetApproximateSizes;
  virtual void GetApproximateSizes(const ColumnFamilyHandle& column_family,
                                   const Range* r, int n,
                                   uint64_t* sizes) override {
      return db_->GetApproximateSizes(column_family, r, n, sizes);
  }

  using DB::CompactRange;
  virtual void CompactRange(const ColumnFamilyHandle& column_family,
                            const Slice* begin, const Slice* end,
                            bool reduce_level = false,
                            int target_level = -1) override {
    return db_->CompactRange(column_family, begin, end, reduce_level,
                             target_level);
  }

  using DB::NumberLevels;
  virtual int NumberLevels(const ColumnFamilyHandle& column_family) override {
    return db_->NumberLevels(column_family);
  }

  using DB::MaxMemCompactionLevel;
  virtual int MaxMemCompactionLevel(const ColumnFamilyHandle& column_family)
      override {
    return db_->MaxMemCompactionLevel(column_family);
  }

  using DB::Level0StopWriteTrigger;
  virtual int Level0StopWriteTrigger(const ColumnFamilyHandle& column_family)
      override {
    return db_->Level0StopWriteTrigger(column_family);
  }

  virtual const std::string& GetName() const override {
    return db_->GetName();
  }

  virtual Env* GetEnv() const override {
    return db_->GetEnv();
  }

  using DB::GetOptions;
  virtual const Options& GetOptions(const ColumnFamilyHandle& column_family)
      const override {
    return db_->GetOptions(column_family);
  }

  using DB::Flush;
  virtual Status Flush(const FlushOptions& fopts,
                       const ColumnFamilyHandle& column_family) override {
    return db_->Flush(fopts, column_family);
  }

  virtual Status DisableFileDeletions() override {
    return db_->DisableFileDeletions();
  }

  virtual Status EnableFileDeletions(bool force) override {
    return db_->EnableFileDeletions(force);
  }

  virtual Status GetLiveFiles(std::vector<std::string>& vec, uint64_t* mfs,
                              bool flush_memtable = true) override {
      return db_->GetLiveFiles(vec, mfs, flush_memtable);
  }

  virtual SequenceNumber GetLatestSequenceNumber() const override {
    return db_->GetLatestSequenceNumber();
  }

  virtual Status GetSortedWalFiles(VectorLogPtr& files) override {
    return db_->GetSortedWalFiles(files);
  }

  virtual Status DeleteFile(std::string name) override {
    return db_->DeleteFile(name);
  }

  virtual Status GetDbIdentity(std::string& identity) {
    return db_->GetDbIdentity(identity);
  }

  virtual Status GetUpdatesSince(SequenceNumber seq_number,
                                 unique_ptr<TransactionLogIterator>* iter)
    override {
      return db_->GetUpdatesSince(seq_number, iter);
  }

 protected:
  DB* db_;
};

} //  namespace rocksdb
