// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef ROCKSDB_LITE

#include "utilities/ttl/db_ttl_impl.h"

#include "db/filename.h"
#include "db/write_batch_internal.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/db_ttl.h"
#include "util/coding.h"

namespace rocksdb {

void DBWithTTLImpl::SanitizeOptions(int32_t ttl, ColumnFamilyOptions* options,
                                    Env* env) {
  if (options->compaction_filter) {
    options->compaction_filter =
        new TtlCompactionFilter(ttl, env, options->compaction_filter);
  } else {
    options->compaction_filter_factory =
        std::shared_ptr<CompactionFilterFactory>(new TtlCompactionFilterFactory(
            ttl, env, options->compaction_filter_factory));
  }

  if (options->merge_operator) {
    options->merge_operator.reset(
        new TtlMergeOperator(options->merge_operator, env));
  }
}

// Open the db inside DBWithTTLImpl because options needs pointer to its ttl
DBWithTTLImpl::DBWithTTLImpl(DB* db) : DBWithTTL(db) {}

DBWithTTLImpl::~DBWithTTLImpl() {
  // Need to stop background compaction before getting rid of the filter
  CancelAllBackgroundWork(db_, /* wait = */ true);
  delete GetOptions().compaction_filter;
}

Status UtilityDB::OpenTtlDB(const Options& options, const std::string& dbname,
                            StackableDB** dbptr, int32_t ttl, bool read_only) {
  DBWithTTL* db;
  Status s = DBWithTTL::Open(options, dbname, &db, ttl, read_only);
  if (s.ok()) {
    *dbptr = db;
  } else {
    *dbptr = nullptr;
  }
  return s;
}

Status DBWithTTL::Open(const Options& options, const std::string& dbname,
                       DBWithTTL** dbptr, int32_t ttl, bool read_only) {

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DBWithTTL::Open(db_options, dbname, column_families, &handles,
                             dbptr, {ttl}, read_only);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }
  return s;
}

Status DBWithTTL::Open(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DBWithTTL** dbptr,
    std::vector<int32_t> ttls, bool read_only) {

  if (ttls.size() != column_families.size()) {
    return Status::InvalidArgument(
        "ttls size has to be the same as number of column families");
  }

  std::vector<ColumnFamilyDescriptor> column_families_sanitized =
      column_families;
  for (size_t i = 0; i < column_families_sanitized.size(); ++i) {
    DBWithTTLImpl::SanitizeOptions(
        ttls[i], &column_families_sanitized[i].options,
        db_options.env == nullptr ? Env::Default() : db_options.env);
  }
  DB* db;

  Status st;
  if (read_only) {
    st = DB::OpenForReadOnly(db_options, dbname, column_families_sanitized,
                             handles, &db);
  } else {
    st = DB::Open(db_options, dbname, column_families_sanitized, handles, &db);
  }
  if (st.ok()) {
    *dbptr = new DBWithTTLImpl(db);
  } else {
    *dbptr = nullptr;
  }
  return st;
}

Status DBWithTTLImpl::CreateColumnFamilyWithTtl(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    ColumnFamilyHandle** handle, int ttl) {
  ColumnFamilyOptions sanitized_options = options;
  DBWithTTLImpl::SanitizeOptions(ttl, &sanitized_options, GetEnv());

  return DBWithTTL::CreateColumnFamily(sanitized_options, column_family_name,
                                       handle);
}

Status DBWithTTLImpl::CreateColumnFamily(const ColumnFamilyOptions& options,
                                         const std::string& column_family_name,
                                         ColumnFamilyHandle** handle) {
  return CreateColumnFamilyWithTtl(options, column_family_name, handle, 0);
}

// Appends the expiration to the string.
// Returns false if no expiration is specified and it could not get the
// current_time, true if append succeeds
Status DBWithTTLImpl::AppendExpiration(const Slice& val,
                                       std::string* val_with_exp,
                                       Env* env, int32_t exptime) {
  val_with_exp->reserve(kTSLength + val.size());
  char exp_string[kTSLength];
  if (0 == exptime) {
    int64_t curtime;
    Status st = env->GetCurrentTime(&curtime);
    if (!st.ok()) {
      return st;
    }
    exptime = (int32_t)curtime;
  }
  EncodeFixed32(exp_string, exptime);
  val_with_exp->append(val.data(), val.size());
  val_with_exp->append(exp_string, kTSLength);
  return Status::OK();
}

// Returns corruption if the length of the string is lesser than timestamp, or
// timestamp refers to a time lesser than ttl-feature release time
Status DBWithTTLImpl::SanityCheckTimestamp(const Slice& str) {
  if (str.size() < kTSLength) {
    return Status::Corruption("Error: value's length less than timestamp's\n");
  }
  // Checks that TS is not lesser than kMinTimestamp
  // Gaurds against corruption & normal database opened incorrectly in ttl mode
  int32_t timestamp_value = DecodeFixed32(str.data() + str.size() - kTSLength);
  if (timestamp_value < kMinTimestamp) {
    return Status::Corruption("Error: Timestamp < ttl feature release time!\n");
  }
  return Status::OK();
}

// Checks if the string is stale or not according to TTl provided
bool DBWithTTLImpl::IsStale(const Slice& value, int32_t ttl, Env* env) {
  if (ttl <= 0) {  // Data is fresh if TTL is non-positive
    return false;
  }
  int64_t curtime;
  if (!env->GetCurrentTime(&curtime).ok()) {
    return false;  // Treat the data as fresh if could not get current time
  }
  int32_t timestamp_value =
      DecodeFixed32(value.data() + value.size() - kTSLength);
  return (timestamp_value + ttl) < curtime;
}

// Strips the TS from the end of the string
Status DBWithTTLImpl::StripTS(std::string* str) {
  Status st;
  if (str->length() < kTSLength) {
    return Status::Corruption("Bad timestamp in key-value");
  }
  // Erasing characters which hold the TS
  str->erase(str->length() - kTSLength, kTSLength);
  return st;
}

Status DBWithTTLImpl::PutWithExpiration(const WriteOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          const Slice& val, int32_t expiration_time) {
  WriteBatch batch;
  batch.Put(column_family, key, val);
  return WriteWithExpiration(options, &batch, expiration_time);
}

Status DBWithTTLImpl::Put(const WriteOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          const Slice& val) {
  return DBWithTTLImpl::PutWithExpiration(options, column_family, key, val, 0);
}

Status DBWithTTLImpl::Get(const ReadOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          std::string* value) {
  Status st = db_->Get(options, column_family, key, value);
  if (!st.ok()) {
    return st;
  }
  st = SanityCheckTimestamp(*value);
  if (!st.ok()) {
    return st;
  }
  return StripTS(value);
}

std::vector<Status> DBWithTTLImpl::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  auto statuses = db_->MultiGet(options, column_family, keys, values);
  for (size_t i = 0; i < keys.size(); ++i) {
    if (!statuses[i].ok()) {
      continue;
    }
    statuses[i] = SanityCheckTimestamp((*values)[i]);
    if (!statuses[i].ok()) {
      continue;
    }
    statuses[i] = StripTS(&(*values)[i]);
  }
  return statuses;
}

bool DBWithTTLImpl::KeyMayExist(const ReadOptions& options,
                                ColumnFamilyHandle* column_family,
                                const Slice& key, std::string* value,
                                bool* value_found) {
  bool ret = db_->KeyMayExist(options, column_family, key, value, value_found);
  if (ret && value != nullptr && value_found != nullptr && *value_found) {
    if (!SanityCheckTimestamp(*value).ok() || !StripTS(value).ok()) {
      return false;
    }
  }
  return ret;
}

Status DBWithTTLImpl::MergeWithExpiration(const WriteOptions& options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            const Slice& value, int32_t expiration_time) {
  WriteBatch batch;
  batch.Merge(column_family, key, value);
  return WriteWithExpiration(options, &batch, expiration_time);
}

Status DBWithTTLImpl::Merge(const WriteOptions& options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            const Slice& value) {
  return DBWithTTLImpl::MergeWithExpiration(options, column_family, key, value, 0);
}

Status DBWithTTLImpl::WriteWithExpiration(const WriteOptions& opts,
                                          WriteBatch* updates,
                                          int32_t expiration_time) {
  class Handler : public WriteBatch::Handler {
   public:
    explicit Handler(Env* env, int32_t exptime) : env_(env), exptime_(exptime) {}
    WriteBatch updates_ttl;
    Status batch_rewrite_status;
    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
      std::string value_with_ts;
      Status st = AppendExpiration(value, &value_with_ts, env_, exptime_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Put(&updates_ttl, column_family_id, key,
                                value_with_ts);
      }
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) override {
      std::string value_with_ts;
      Status st = AppendExpiration(value, &value_with_ts, env_, exptime_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Merge(&updates_ttl, column_family_id, key,
                                  value_with_ts);
      }
      return Status::OK();
    }
    virtual Status DeleteCF(uint32_t column_family_id,
                            const Slice& key) override {
      WriteBatchInternal::Delete(&updates_ttl, column_family_id, key);
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) override {
      updates_ttl.PutLogData(blob);
    }

   private:
    Env* env_;
    int32_t exptime_;
  };
  Handler handler(GetEnv(), expiration_time);
  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Status DBWithTTLImpl::Write(const WriteOptions& opts, WriteBatch* updates) {
  return DBWithTTLImpl::WriteWithExpiration(opts, updates, 0);
}

Iterator* DBWithTTLImpl::NewIterator(const ReadOptions& opts,
                                     ColumnFamilyHandle* column_family) {
  return new TtlIterator(db_->NewIterator(opts, column_family));
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
