// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef ROCKSDB_LITE

#include "utilities/date_tiered/date_tiered_db_impl.h"

#include <limits>

#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/write_batch_internal.h"
#include "monitoring/instrumented_mutex.h"
#include "options/options_helper.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/date_tiered_db.h"
#include "table/merging_iterator.h"
#include "util/coding.h"
#include "util/filename.h"
#include "util/string_util.h"

namespace rocksdb {

// Open the db inside DateTieredDBImpl because options needs pointer to its ttl
DateTieredDBImpl::DateTieredDBImpl(
    DB* db, Options options,
    const std::vector<ColumnFamilyDescriptor>& descriptors,
    const std::vector<ColumnFamilyHandle*>& handles, int64_t ttl,
    int64_t column_family_interval)
    : db_(db),
      cf_options_(ColumnFamilyOptions(options)),
      ioptions_(ImmutableCFOptions(options)),
      ttl_(ttl),
      column_family_interval_(column_family_interval),
      mutex_(options.statistics.get(), db->GetEnv(), DB_MUTEX_WAIT_MICROS,
             options.use_adaptive_mutex) {
  latest_timebound_ = std::numeric_limits<int64_t>::min();
  for (size_t i = 0; i < handles.size(); ++i) {
    const auto& name = descriptors[i].name;
    int64_t timestamp = 0;
    try {
      timestamp = ParseUint64(name);
    } catch (const std::invalid_argument&) {
      // Bypass unrelated column family, e.g. default
      db_->DestroyColumnFamilyHandle(handles[i]);
      continue;
    }
    if (timestamp > latest_timebound_) {
      latest_timebound_ = timestamp;
    }
    handle_map_.insert(std::make_pair(timestamp, handles[i]));
  }
}

DateTieredDBImpl::~DateTieredDBImpl() {
  for (auto handle : handle_map_) {
    db_->DestroyColumnFamilyHandle(handle.second);
  }
  delete db_;
  db_ = nullptr;
}

Status DateTieredDB::Open(const Options& options, const std::string& dbname,
                          DateTieredDB** dbptr, int64_t ttl,
                          int64_t column_family_interval, bool read_only) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> descriptors;
  std::vector<ColumnFamilyHandle*> handles;
  DB* db;
  Status s;

  // Get column families
  std::vector<std::string> column_family_names;
  s = DB::ListColumnFamilies(db_options, dbname, &column_family_names);
  if (!s.ok()) {
    // No column family found. Use default
    s = DB::Open(options, dbname, &db);
    if (!s.ok()) {
      return s;
    }
  } else {
    for (auto name : column_family_names) {
      descriptors.emplace_back(ColumnFamilyDescriptor(name, cf_options));
    }

    // Open database
    if (read_only) {
      s = DB::OpenForReadOnly(db_options, dbname, descriptors, &handles, &db);
    } else {
      s = DB::Open(db_options, dbname, descriptors, &handles, &db);
    }
  }

  if (s.ok()) {
    *dbptr = new DateTieredDBImpl(db, options, descriptors, handles, ttl,
                                  column_family_interval);
  }
  return s;
}

// Checks if the string is stale or not according to TTl provided
bool DateTieredDBImpl::IsStale(int64_t keytime, int64_t ttl, Env* env) {
  if (ttl <= 0) {
    // Data is fresh if TTL is non-positive
    return false;
  }
  int64_t curtime;
  if (!env->GetCurrentTime(&curtime).ok()) {
    // Treat the data as fresh if could not get current time
    return false;
  }
  return curtime >= keytime + ttl;
}

// Drop column family when all data in that column family is expired
// TODO(jhli): Can be made a background job
Status DateTieredDBImpl::DropObsoleteColumnFamilies() {
  int64_t curtime;
  Status s;
  s = db_->GetEnv()->GetCurrentTime(&curtime);
  if (!s.ok()) {
    return s;
  }
  {
    InstrumentedMutexLock l(&mutex_);
    auto iter = handle_map_.begin();
    while (iter != handle_map_.end()) {
      if (iter->first <= curtime - ttl_) {
        s = db_->DropColumnFamily(iter->second);
        if (!s.ok()) {
          return s;
        }
        delete iter->second;
        iter = handle_map_.erase(iter);
      } else {
        break;
      }
    }
  }
  return Status::OK();
}

// Get timestamp from user key
Status DateTieredDBImpl::GetTimestamp(const Slice& key, int64_t* result) {
  if (key.size() < kTSLength) {
    return Status::Corruption("Bad timestamp in key");
  }
  const char* pos = key.data() + key.size() - 8;
  int64_t timestamp = 0;
  if (port::kLittleEndian) {
    int bytes_to_fill = 8;
    for (int i = 0; i < bytes_to_fill; ++i) {
      timestamp |= (static_cast<uint64_t>(static_cast<unsigned char>(pos[i]))
                    << ((bytes_to_fill - i - 1) << 3));
    }
  } else {
    memcpy(&timestamp, pos, sizeof(timestamp));
  }
  *result = timestamp;
  return Status::OK();
}

Status DateTieredDBImpl::CreateColumnFamily(
    ColumnFamilyHandle** column_family) {
  int64_t curtime;
  Status s;
  mutex_.AssertHeld();
  s = db_->GetEnv()->GetCurrentTime(&curtime);
  if (!s.ok()) {
    return s;
  }
  int64_t new_timebound;
  if (handle_map_.empty()) {
    new_timebound = curtime + column_family_interval_;
  } else {
    new_timebound =
        latest_timebound_ +
        ((curtime - latest_timebound_) / column_family_interval_ + 1) *
            column_family_interval_;
  }
  std::string cf_name = ToString(new_timebound);
  latest_timebound_ = new_timebound;
  s = db_->CreateColumnFamily(cf_options_, cf_name, column_family);
  if (s.ok()) {
    handle_map_.insert(std::make_pair(new_timebound, *column_family));
  }
  return s;
}

Status DateTieredDBImpl::FindColumnFamily(int64_t keytime,
                                          ColumnFamilyHandle** column_family,
                                          bool create_if_missing) {
  *column_family = nullptr;
  {
    InstrumentedMutexLock l(&mutex_);
    auto iter = handle_map_.upper_bound(keytime);
    if (iter == handle_map_.end()) {
      if (!create_if_missing) {
        return Status::NotFound();
      } else {
        return CreateColumnFamily(column_family);
      }
    }
    // Move to previous element to get the appropriate time window
    *column_family = iter->second;
  }
  return Status::OK();
}

Status DateTieredDBImpl::Put(const WriteOptions& options, const Slice& key,
                             const Slice& val) {
  int64_t timestamp = 0;
  Status s;
  s = GetTimestamp(key, &timestamp);
  if (!s.ok()) {
    return s;
  }
  DropObsoleteColumnFamilies();

  // Prune request to obsolete data
  if (IsStale(timestamp, ttl_, db_->GetEnv())) {
    return Status::InvalidArgument();
  }

  // Decide column family (i.e. the time window) to put into
  ColumnFamilyHandle* column_family;
  s = FindColumnFamily(timestamp, &column_family, true /*create_if_missing*/);
  if (!s.ok()) {
    return s;
  }

  // Efficiently put with WriteBatch
  WriteBatch batch;
  batch.Put(column_family, key, val);
  return Write(options, &batch);
}

Status DateTieredDBImpl::Get(const ReadOptions& options, const Slice& key,
                             std::string* value) {
  int64_t timestamp = 0;
  Status s;
  s = GetTimestamp(key, &timestamp);
  if (!s.ok()) {
    return s;
  }
  // Prune request to obsolete data
  if (IsStale(timestamp, ttl_, db_->GetEnv())) {
    return Status::NotFound();
  }

  // Decide column family to get from
  ColumnFamilyHandle* column_family;
  s = FindColumnFamily(timestamp, &column_family, false /*create_if_missing*/);
  if (!s.ok()) {
    return s;
  }
  if (column_family == nullptr) {
    // Cannot find column family
    return Status::NotFound();
  }

  // Get value with key
  return db_->Get(options, column_family, key, value);
}

bool DateTieredDBImpl::KeyMayExist(const ReadOptions& options, const Slice& key,
                                   std::string* value, bool* value_found) {
  int64_t timestamp = 0;
  Status s;
  s = GetTimestamp(key, &timestamp);
  if (!s.ok()) {
    // Cannot get current time
    return false;
  }
  // Decide column family to get from
  ColumnFamilyHandle* column_family;
  s = FindColumnFamily(timestamp, &column_family, false /*create_if_missing*/);
  if (!s.ok() || column_family == nullptr) {
    // Cannot find column family
    return false;
  }
  if (IsStale(timestamp, ttl_, db_->GetEnv())) {
    return false;
  }
  return db_->KeyMayExist(options, column_family, key, value, value_found);
}

Status DateTieredDBImpl::Delete(const WriteOptions& options, const Slice& key) {
  int64_t timestamp = 0;
  Status s;
  s = GetTimestamp(key, &timestamp);
  if (!s.ok()) {
    return s;
  }
  DropObsoleteColumnFamilies();
  // Prune request to obsolete data
  if (IsStale(timestamp, ttl_, db_->GetEnv())) {
    return Status::NotFound();
  }

  // Decide column family to get from
  ColumnFamilyHandle* column_family;
  s = FindColumnFamily(timestamp, &column_family, false /*create_if_missing*/);
  if (!s.ok()) {
    return s;
  }
  if (column_family == nullptr) {
    // Cannot find column family
    return Status::NotFound();
  }

  // Get value with key
  return db_->Delete(options, column_family, key);
}

Status DateTieredDBImpl::Merge(const WriteOptions& options, const Slice& key,
                               const Slice& value) {
  // Decide column family to get from
  int64_t timestamp = 0;
  Status s;
  s = GetTimestamp(key, &timestamp);
  if (!s.ok()) {
    // Cannot get current time
    return s;
  }
  ColumnFamilyHandle* column_family;
  s = FindColumnFamily(timestamp, &column_family, true /*create_if_missing*/);
  if (!s.ok()) {
    return s;
  }
  WriteBatch batch;
  batch.Merge(column_family, key, value);
  return Write(options, &batch);
}

Status DateTieredDBImpl::Write(const WriteOptions& opts, WriteBatch* updates) {
  class Handler : public WriteBatch::Handler {
   public:
    explicit Handler() {}
    WriteBatch updates_ttl;
    Status batch_rewrite_status;
    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
      WriteBatchInternal::Put(&updates_ttl, column_family_id, key, value);
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) override {
      WriteBatchInternal::Merge(&updates_ttl, column_family_id, key, value);
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
  };
  Handler handler;
  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Iterator* DateTieredDBImpl::NewIterator(const ReadOptions& opts) {
  if (handle_map_.empty()) {
    return NewEmptyIterator();
  }

  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db_);

  auto db_iter = NewArenaWrappedDbIterator(
      db_impl->GetEnv(), opts, ioptions_, kMaxSequenceNumber,
      cf_options_.max_sequential_skip_in_iterations, 0);

  auto arena = db_iter->GetArena();
  MergeIteratorBuilder builder(cf_options_.comparator, arena);
  for (auto& item : handle_map_) {
    auto handle = item.second;
    builder.AddIterator(db_impl->NewInternalIterator(
        arena, db_iter->GetRangeDelAggregator(), handle));
  }
  auto internal_iter = builder.Finish();
  db_iter->SetIterUnderDBIter(internal_iter);
  return db_iter;
}
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
