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
#include "rocksdb/merge_operator.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/utilities/db_ttl.h"
#include "utilities/compaction_filters/layered_compaction_filter_base.h"

#ifdef _WIN32
// Windows API macro interference
#undef GetCurrentTime
#endif

namespace ROCKSDB_NAMESPACE {
struct ConfigOptions;
class ObjectLibrary;
class ObjectRegistry;
class DBWithTTLImpl : public DBWithTTL {
 public:
  static void SanitizeOptions(int32_t ttl, ColumnFamilyOptions* options,
                              SystemClock* clock);

  static void RegisterTtlClasses();
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

  static bool IsStale(const Slice& value, int32_t ttl, SystemClock* clock);

  static Status AppendTS(const Slice& val, std::string* val_with_ts,
                         SystemClock* clock);

  static Status SanityCheckTimestamp(const Slice& str);

  static Status StripTS(std::string* str);

  static Status StripTS(PinnableSlice* str);

  static const uint32_t kTSLength = sizeof(int32_t);  // size of timestamp

  static const int32_t kMinTimestamp = 1368146402;  // 05/09/2013:5:40PM GMT-8

  static const int32_t kMaxTimestamp = 2147483647;  // 01/18/2038:7:14PM GMT-8

  void SetTtl(int32_t ttl) override { SetTtl(DefaultColumnFamily(), ttl); }

  void SetTtl(ColumnFamilyHandle* h, int32_t ttl) override;

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

class TtlCompactionFilter : public LayeredCompactionFilterBase {
 public:
  TtlCompactionFilter(int32_t ttl, SystemClock* clock,
                      const CompactionFilter* _user_comp_filter,
                      std::unique_ptr<const CompactionFilter>
                          _user_comp_filter_from_factory = nullptr);

  virtual bool Filter(int level, const Slice& key, const Slice& old_val,
                      std::string* new_val, bool* value_changed) const override;

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "TtlCompactionFilter"; }
  bool IsInstanceOf(const std::string& name) const override {
    if (name == "Delete By TTL") {
      return true;
    } else {
      return LayeredCompactionFilterBase::IsInstanceOf(name);
    }
  }

  Status PrepareOptions(const ConfigOptions& config_options) override;
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

 private:
  int32_t ttl_;
  SystemClock* clock_;
};

class TtlCompactionFilterFactory : public CompactionFilterFactory {
 public:
  TtlCompactionFilterFactory(
      int32_t ttl, SystemClock* clock,
      std::shared_ptr<CompactionFilterFactory> comp_filter_factory);

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override;
  void SetTtl(int32_t ttl) { ttl_ = ttl; }

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "TtlCompactionFilterFactory"; }
  Status PrepareOptions(const ConfigOptions& config_options) override;
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;
  const Customizable* Inner() const override {
    return user_comp_filter_factory_.get();
  }

 private:
  int32_t ttl_;
  SystemClock* clock_;
  std::shared_ptr<CompactionFilterFactory> user_comp_filter_factory_;
};

class TtlMergeOperator : public MergeOperator {
 public:
  explicit TtlMergeOperator(const std::shared_ptr<MergeOperator>& merge_op,
                            SystemClock* clock);

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override;

  bool PartialMergeMulti(const Slice& key,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value, Logger* logger) const override;

  static const char* kClassName() { return "TtlMergeOperator"; }

  const char* Name() const override { return kClassName(); }
  bool IsInstanceOf(const std::string& name) const override {
    if (name == "Merge By TTL") {
      return true;
    } else {
      return MergeOperator::IsInstanceOf(name);
    }
  }

  Status PrepareOptions(const ConfigOptions& config_options) override;
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;
  const Customizable* Inner() const override { return user_merge_op_.get(); }

 private:
  std::shared_ptr<MergeOperator> user_merge_op_;
  SystemClock* clock_;
};
extern "C" {
int RegisterTtlObjects(ObjectLibrary& library, const std::string& /*arg*/);
}  // extern "C"

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
