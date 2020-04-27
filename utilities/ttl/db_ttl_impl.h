// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/db_plugin.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/utilities/utility_db.h"

#ifdef _WIN32
// Windows API macro interference
#undef GetCurrentTime
#endif

namespace ROCKSDB_NAMESPACE {
class ObjectLibrary;

class TtlDBPlugin : public DBPlugin {
 public:
  TtlDBPlugin();
  TtlDBPlugin(const std::vector<int>& ttls);
  const char* Name() const override;
  Status SanitizeCB(OpenMode mode, const std::string& db_name,
                    DBOptions* db_opts,
                    std::vector<ColumnFamilyDescriptor>* cfss) override;
  Status ValidateCB(
      OpenMode mode, const std::string& db_name, const DBOptions& db_opts,
      const std::vector<ColumnFamilyDescriptor>& cfds) const override;
  Status OpenCB(OpenMode mode, DB* db,
                const std::vector<ColumnFamilyHandle*>& handles,
                DB** wrapped) override;
  bool SupportsOpenMode(OpenMode mode) const override {
    return (mode == OpenMode::Normal || mode == OpenMode::ReadOnly);
  }
  Status PrepareTtlOptions(int ttl, Env* env,
                           ColumnFamilyOptions* cf_options) const;

 private:
  std::vector<int> ttls_;
};

class TtlCompactionFilter : public CompactionFilter {
 public:
  TtlCompactionFilter(int32_t ttl, Env* env,
                      const CompactionFilter* user_comp_filter,
                      std::unique_ptr<const CompactionFilter>
                          user_comp_filter_from_factory = nullptr);
  void SetTtl(int32_t ttl) const { ttl_ = ttl; }

  bool Filter(int level, const Slice& key, const Slice& old_val,
              std::string* new_val, bool* value_changed) const override;
  const char* Name() const override;

  Status PrepareOptions(const ConfigOptions& opts) override;

  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

 protected:
  Configurable* Inner() const override {
    auto filter = const_cast<CompactionFilter*>(user_comp_filter_);
    return filter;
  }

 private:
  mutable int32_t ttl_;
  Env* env_;
  const CompactionFilter* user_comp_filter_;
  std::unique_ptr<const CompactionFilter> user_comp_filter_from_factory_;
};

class TtlCompactionFilterFactory : public CompactionFilterFactory {
 public:
  TtlCompactionFilterFactory(
      int32_t ttl, Env* env,
      const std::shared_ptr<CompactionFilterFactory>& comp_filter_factory);

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override;

  void SetTtl(int32_t ttl) { ttl_ = ttl; }

  const char* Name() const override;

  Status PrepareOptions(const ConfigOptions& opts) override;

  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

 protected:
  Configurable* Inner() const override {
    return user_comp_filter_factory_.get();
  }

 private:
  int32_t ttl_;
  Env* env_;
  std::shared_ptr<CompactionFilterFactory> user_comp_filter_factory_;
};

class TtlMergeOperator : public MergeOperator {
 public:
  explicit TtlMergeOperator(const std::shared_ptr<MergeOperator>& merge_op,
                            Env* env);
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override;

  bool PartialMergeMulti(const Slice& key,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value, Logger* logger) const override;
  const char* Name() const override;
  Status PrepareOptions(const ConfigOptions& opts) override;
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

 protected:
  Configurable* Inner() const override { return user_merge_op_.get(); }

 private:
  std::shared_ptr<MergeOperator> user_merge_op_;
  Env* env_;
};

class DBWithTTLImpl : public DBWithTTL {
 public:
  explicit DBWithTTLImpl(DB* db);

  virtual ~DBWithTTLImpl();

  virtual Status Close() override;

  Status CreateColumnFamilyWithTtl(const ColumnFamilyOptions& options,
                                   const std::string& column_family_name,
                                   ColumnFamilyHandle** handle,
                                   int ttl) override;

  Status CreateColumnFamily(const ColumnFamilyOptions& options,
                            const std::string& column_family_name,
                            ColumnFamilyHandle** handle) override;

  using StackableDB::Put;
  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& val) override;

  using StackableDB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value) override;

  using StackableDB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  using StackableDB::KeyMayExist;
  virtual bool KeyMayExist(const ReadOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value,
                           bool* value_found = nullptr) override;

  using StackableDB::Merge;
  virtual Status Merge(const WriteOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) override;

  virtual Status Write(const WriteOptions& opts, WriteBatch* updates) override;

  using StackableDB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& opts,
                                ColumnFamilyHandle* column_family) override;

  virtual DB* GetBaseDB() override { return db_; }

  static bool IsStale(const Slice& value, int32_t ttl, Env* env);

  static Status AppendTS(const Slice& val, std::string* val_with_ts, Env* env);

  static Status SanityCheckTimestamp(const Slice& str);

  static Status StripTS(std::string* str);

  static Status StripTS(PinnableSlice* str);

  static const uint32_t kTSLength = sizeof(int32_t);  // size of timestamp

  static const int32_t kMinTimestamp = 1368146402;  // 05/09/2013:5:40PM GMT-8

  static const int32_t kMaxTimestamp = 2147483647;  // 01/18/2038:7:14PM GMT-8

  void SetTtl(int32_t ttl) override { SetTtl(DefaultColumnFamily(), ttl); }

  void SetTtl(ColumnFamilyHandle *h, int32_t ttl) override;

 private:
  // remember whether the Close completes or not
  bool closed_;
};

class TtlIterator : public Iterator {

 public:
  explicit TtlIterator(Iterator* iter) : iter_(iter) { assert(iter_); }

  ~TtlIterator() { delete iter_; }

  bool Valid() const override { return iter_->Valid(); }

  void SeekToFirst() override { iter_->SeekToFirst(); }

  void SeekToLast() override { iter_->SeekToLast(); }

  void Seek(const Slice& target) override { iter_->Seek(target); }

  void SeekForPrev(const Slice& target) override { iter_->SeekForPrev(target); }

  void Next() override { iter_->Next(); }

  void Prev() override { iter_->Prev(); }

  Slice key() const override { return iter_->key(); }

  int32_t ttl_timestamp() const {
    return DecodeFixed32(iter_->value().data() + iter_->value().size() -
                         DBWithTTLImpl::kTSLength);
  }

  Slice value() const override {
    // TODO: handle timestamp corruption like in general iterator semantics
    assert(DBWithTTLImpl::SanityCheckTimestamp(iter_->value()).ok());
    Slice trimmed_value = iter_->value();
    trimmed_value.size_ -= DBWithTTLImpl::kTSLength;
    return trimmed_value;
  }

  Status status() const override { return iter_->status(); }

 private:
  Iterator* iter_;
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
