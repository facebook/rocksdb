//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_impl/compacted_db_impl.h"

#include "db/db_impl/db_impl.h"
#include "db/version_set.h"
#include "logging/logging.h"
#include "table/get_context.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

CompactedDBImpl::CompactedDBImpl(const DBOptions& options,
                                 const std::string& dbname)
    : DBImpl(options, dbname, /*seq_per_batch*/ false, +/*batch_per_txn*/ true,
             /*read_only*/ true),
      cfd_(nullptr),
      version_(nullptr),
      user_comparator_(nullptr) {}

CompactedDBImpl::~CompactedDBImpl() = default;

size_t CompactedDBImpl::FindFile(const Slice& key) {
  size_t right = files_.num_files - 1;
  auto cmp = [&](const FdWithKeyRange& f, const Slice& k) -> bool {
    return user_comparator_->Compare(ExtractUserKey(f.largest_key), k) < 0;
  };
  return static_cast<size_t>(
      std::lower_bound(files_.files, files_.files + right, key, cmp) -
      files_.files);
}

Status CompactedDBImpl::Get(const ReadOptions& _read_options,
                            ColumnFamilyHandle*, const Slice& key,
                            PinnableSlice* value, std::string* timestamp) {
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kGet) {
    return Status::InvalidArgument(
        "Can only call Get with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kGet`");
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kGet;
  }

  assert(user_comparator_);
  if (read_options.timestamp) {
    Status s =
        FailIfTsMismatchCf(DefaultColumnFamily(), *(read_options.timestamp));
    if (!s.ok()) {
      return s;
    }
    if (read_options.timestamp->size() > 0) {
      s = FailIfReadCollapsedHistory(cfd_, cfd_->GetSuperVersion(),
                                     *(read_options.timestamp));
      if (!s.ok()) {
        return s;
      }
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
  LookupKey lkey(key, kMaxSequenceNumber, read_options.timestamp);
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
  Status s = f.fd.table_reader->Get(read_options, lkey.internal_key(),
                                    &get_context, nullptr);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  if (get_context.State() == GetContext::kFound) {
    return Status::OK();
  }
  return Status::NotFound();
}

void CompactedDBImpl::MultiGet(const ReadOptions& _read_options,
                               size_t num_keys,
                               ColumnFamilyHandle** /*column_families*/,
                               const Slice* keys, PinnableSlice* values,
                               std::string* timestamps, Status* statuses,
                               const bool /*sorted_input*/) {
  assert(user_comparator_);
  Status s;
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kMultiGet) {
    s = Status::InvalidArgument(
        "Can only call MultiGet with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kMultiGet`");
  }

  ReadOptions read_options(_read_options);
  if (s.ok()) {
    if (read_options.io_activity == Env::IOActivity::kUnknown) {
      read_options.io_activity = Env::IOActivity::kMultiGet;
    }

    if (read_options.timestamp) {
      s = FailIfTsMismatchCf(DefaultColumnFamily(), *(read_options.timestamp));
      if (s.ok()) {
        if (read_options.timestamp->size() > 0) {
          s = FailIfReadCollapsedHistory(cfd_, cfd_->GetSuperVersion(),
                                         *(read_options.timestamp));
        }
      }
    } else {
      s = FailIfCfHasTs(DefaultColumnFamily());
    }
  }

  if (!s.ok()) {
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }
    return;
  }

  // Clear the timestamps for returning results so that we can distinguish
  // between tombstone or key that has never been written
  if (timestamps) {
    for (size_t i = 0; i < num_keys; ++i) {
      timestamps[i].clear();
    }
  }

  GetWithTimestampReadCallback read_cb(kMaxSequenceNumber);
  autovector<TableReader*, 16> reader_list;
  for (size_t i = 0; i < num_keys; ++i) {
    const Slice& key = keys[i];
    LookupKey lkey(key, kMaxSequenceNumber, read_options.timestamp);
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
  for (size_t i = 0; i < num_keys; ++i) {
    statuses[i] = Status::NotFound();
  }
  int idx = 0;
  for (auto* r : reader_list) {
    if (r != nullptr) {
      PinnableSlice& pinnable_val = values[idx];
      LookupKey lkey(keys[idx], kMaxSequenceNumber, read_options.timestamp);
      std::string* timestamp = timestamps ? &timestamps[idx] : nullptr;
      GetContext get_context(
          user_comparator_, nullptr, nullptr, nullptr, GetContext::kNotFound,
          lkey.user_key(), &pinnable_val, /*columns=*/nullptr,
          user_comparator_->timestamp_size() > 0 ? timestamp : nullptr, nullptr,
          nullptr, true, nullptr, nullptr, nullptr, nullptr, &read_cb);
      Status status =
          r->Get(read_options, lkey.internal_key(), &get_context, nullptr);
      assert(static_cast<size_t>(idx) < num_keys);
      if (!status.ok() && !status.IsNotFound()) {
        statuses[idx] = status;
      } else {
        if (get_context.State() == GetContext::kFound) {
          statuses[idx] = Status::OK();
        }
      }
    }
    ++idx;
  }
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
                             std::unique_ptr<DB>* dbptr) {
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
    *dbptr = std::move(db);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
