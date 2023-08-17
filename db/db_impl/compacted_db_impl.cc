//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "db/db_impl/compacted_db_impl.h"

#include "db/db_impl/db_impl.h"
#include "db/version_set.h"
#include "logging/logging.h"
#include "table/get_context.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

extern void MarkKeyMayExist(void* arg);
extern bool SaveValue(void* arg, const ParsedInternalKey& parsed_key,
                      const Slice& v, bool hit_and_return);

CompactedDBImpl::CompactedDBImpl(const DBOptions& options,
                                 const std::string& dbname)
    : DBImpl(options, dbname, /*seq_per_batch*/ false, +/*batch_per_txn*/ true,
             /*read_only*/ true),
      cfd_(nullptr),
      version_(nullptr),
      user_comparator_(nullptr) {}

CompactedDBImpl::~CompactedDBImpl() {}

size_t CompactedDBImpl::FindFile(const Slice& key) {
  size_t right = files_.num_files - 1;
  auto cmp = [&](const FdWithKeyRange& f, const Slice& k) -> bool {
    return user_comparator_->Compare(ExtractUserKey(f.largest_key), k) < 0;
  };
  return static_cast<size_t>(
      std::lower_bound(files_.files, files_.files + right, key, cmp) -
      files_.files);
}

Status CompactedDBImpl::Get(const ReadOptions& options, ColumnFamilyHandle*,
                            const Slice& key, PinnableSlice* value) {
  return Get(options, /*column_family*/ nullptr, key, value,
             /*timestamp*/ nullptr);
}

Status CompactedDBImpl::Get(const ReadOptions& options, ColumnFamilyHandle*,
                            const Slice& key, PinnableSlice* value,
                            std::string* timestamp) {
  assert(user_comparator_);
  if (options.timestamp) {
    const Status s = FailIfTsMismatchCf(
        DefaultColumnFamily(), *(options.timestamp), /*ts_for_read=*/true);
    if (!s.ok()) {
      return s;
    }
  } else {
    const Status s = FailIfCfHasTs(DefaultColumnFamily());
    if (!s.ok()) {
      return s;
    }
  }

  // Clear the timestamps for returning results so that we can distinguish
  // between tombstone or key that has never been written
  if (timestamp) {
    timestamp->clear();
  }

  GetWithTimestampReadCallback read_cb(kMaxSequenceNumber);
  std::string* ts =
      user_comparator_->timestamp_size() > 0 ? timestamp : nullptr;
  LookupKey lkey(key, kMaxSequenceNumber, options.timestamp);
  GetContext get_context(user_comparator_, nullptr, nullptr, nullptr,
                         GetContext::kNotFound, lkey.user_key(), value,
                         /*columns=*/nullptr, ts, nullptr, nullptr, true,
                         nullptr, nullptr, nullptr, nullptr, &read_cb);

  const FdWithKeyRange& f = files_.files[FindFile(lkey.user_key())];
  if (user_comparator_->CompareWithoutTimestamp(
          key, /*a_has_ts=*/false,
          ExtractUserKeyAndStripTimestamp(f.smallest_key,
                                          user_comparator_->timestamp_size()),
          /*b_has_ts=*/false) < 0) {
    return Status::NotFound();
  }
  Status s = f.fd.table_reader->Get(options, lkey.internal_key(), &get_context,
                                    nullptr);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  if (get_context.State() == GetContext::kFound) {
    return Status::OK();
  }
  return Status::NotFound();
}

std::vector<Status> CompactedDBImpl::MultiGet(
    const ReadOptions& options, const std::vector<ColumnFamilyHandle*>&,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  return MultiGet(options, keys, values, /*timestamps*/ nullptr);
}

std::vector<Status> CompactedDBImpl::MultiGet(
    const ReadOptions& options, const std::vector<ColumnFamilyHandle*>&,
    const std::vector<Slice>& keys, std::vector<std::string>* values,
    std::vector<std::string>* timestamps) {
  assert(user_comparator_);
  size_t num_keys = keys.size();

  if (options.timestamp) {
    Status s = FailIfTsMismatchCf(DefaultColumnFamily(), *(options.timestamp),
                                  /*ts_for_read=*/true);
    if (!s.ok()) {
      return std::vector<Status>(num_keys, s);
    }
  } else {
    Status s = FailIfCfHasTs(DefaultColumnFamily());
    if (!s.ok()) {
      return std::vector<Status>(num_keys, s);
    }
  }

  // Clear the timestamps for returning results so that we can distinguish
  // between tombstone or key that has never been written
  if (timestamps) {
    for (auto& ts : *timestamps) {
      ts.clear();
    }
  }

  GetWithTimestampReadCallback read_cb(kMaxSequenceNumber);
  autovector<TableReader*, 16> reader_list;
  for (const auto& key : keys) {
    LookupKey lkey(key, kMaxSequenceNumber, options.timestamp);
    const FdWithKeyRange& f = files_.files[FindFile(lkey.user_key())];
    if (user_comparator_->CompareWithoutTimestamp(
            key, /*a_has_ts=*/false,
            ExtractUserKeyAndStripTimestamp(f.smallest_key,
                                            user_comparator_->timestamp_size()),
            /*b_has_ts=*/false) < 0) {
      reader_list.push_back(nullptr);
    } else {
      f.fd.table_reader->Prepare(lkey.internal_key());
      reader_list.push_back(f.fd.table_reader);
    }
  }
  std::vector<Status> statuses(num_keys, Status::NotFound());
  values->resize(num_keys);
  if (timestamps) {
    timestamps->resize(num_keys);
  }
  int idx = 0;
  for (auto* r : reader_list) {
    if (r != nullptr) {
      PinnableSlice pinnable_val;
      std::string& value = (*values)[idx];
      LookupKey lkey(keys[idx], kMaxSequenceNumber, options.timestamp);
      std::string* timestamp = timestamps ? &(*timestamps)[idx] : nullptr;
      GetContext get_context(
          user_comparator_, nullptr, nullptr, nullptr, GetContext::kNotFound,
          lkey.user_key(), &pinnable_val, /*columns=*/nullptr,
          user_comparator_->timestamp_size() > 0 ? timestamp : nullptr, nullptr,
          nullptr, true, nullptr, nullptr, nullptr, nullptr, &read_cb);
      Status s = r->Get(options, lkey.internal_key(), &get_context, nullptr);
      assert(static_cast<size_t>(idx) < statuses.size());
      if (!s.ok() && !s.IsNotFound()) {
        statuses[idx] = s;
      } else {
        value.assign(pinnable_val.data(), pinnable_val.size());
        if (get_context.State() == GetContext::kFound) {
          statuses[idx] = Status::OK();
        }
      }
    }
    ++idx;
  }
  return statuses;
}

Status CompactedDBImpl::Init(const Options& options) {
  SuperVersionContext sv_context(/* create_superversion */ true);
  mutex_.Lock();
  ColumnFamilyDescriptor cf(kDefaultColumnFamilyName,
                            ColumnFamilyOptions(options));
  Status s = Recover({cf}, true /* read only */, false, true);
  if (s.ok()) {
    cfd_ = static_cast_with_check<ColumnFamilyHandleImpl>(DefaultColumnFamily())
               ->cfd();
    cfd_->InstallSuperVersion(&sv_context, &mutex_);
  }
  mutex_.Unlock();
  sv_context.Clean();
  if (!s.ok()) {
    return s;
  }
  NewThreadStatusCfInfo(cfd_);
  version_ = cfd_->GetSuperVersion()->current;
  user_comparator_ = cfd_->user_comparator();
  auto* vstorage = version_->storage_info();
  if (vstorage->num_non_empty_levels() == 0) {
    return Status::NotSupported("no file exists");
  }
  const LevelFilesBrief& l0 = vstorage->LevelFilesBrief(0);
  // L0 should not have files
  if (l0.num_files > 1) {
    return Status::NotSupported("L0 contain more than 1 file");
  }
  if (l0.num_files == 1) {
    if (vstorage->num_non_empty_levels() > 1) {
      return Status::NotSupported("Both L0 and other level contain files");
    }
    files_ = l0;
    return Status::OK();
  }

  for (int i = 1; i < vstorage->num_non_empty_levels() - 1; ++i) {
    if (vstorage->LevelFilesBrief(i).num_files > 0) {
      return Status::NotSupported("Other levels also contain files");
    }
  }

  int level = vstorage->num_non_empty_levels() - 1;
  if (vstorage->LevelFilesBrief(level).num_files > 0) {
    files_ = vstorage->LevelFilesBrief(level);
    return Status::OK();
  }
  return Status::NotSupported("no file exists");
}

Status CompactedDBImpl::Open(const Options& options, const std::string& dbname,
                             DB** dbptr) {
  *dbptr = nullptr;

  if (options.max_open_files != -1) {
    return Status::InvalidArgument("require max_open_files = -1");
  }
  if (options.merge_operator.get() != nullptr) {
    return Status::InvalidArgument("merge operator is not supported");
  }
  DBOptions db_options(options);
  std::unique_ptr<CompactedDBImpl> db(new CompactedDBImpl(db_options, dbname));
  Status s = db->Init(options);
  if (s.ok()) {
    s = db->StartPeriodicTaskScheduler();
  }
  if (s.ok()) {
    ROCKS_LOG_INFO(db->immutable_db_options_.info_log,
                   "Opened the db as fully compacted mode");
    LogFlush(db->immutable_db_options_.info_log);
    *dbptr = db.release();
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
