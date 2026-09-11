//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/coro_utils.h"

#if defined(WITHOUT_COROUTINES) || (USE_COROUTINES && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {

DEFINE_SYNC_AND_ASYNC(Status, WritePreparedTxnDB::Get)
(const ReadOptions& _read_options, ColumnFamilyHandle* column_family,
 const Slice& key, PinnableSlice* value, std::string* timestamp) {
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kGet) {
    CO_RETURN Status::InvalidArgument(
        "Can only call Get with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kGet`");
  }
  if (timestamp) {
    CO_RETURN Status::NotSupported(
        "Get() that returns timestamp is not implemented");
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kGet;
  }

  CO_RETURN CO_AWAIT(GetImpl, read_options, column_family, key, value);
}

DEFINE_SYNC_AND_ASYNC(Status, WritePreparedTxnDB::GetEntity)
(const ReadOptions& options, ColumnFamilyHandle* column_family,
 const Slice& key, PinnableWideColumns* columns) {
  if (!column_family) {
    CO_RETURN Status::InvalidArgument(
        "Cannot call GetEntity without a column family handle");
  }
  if (!columns) {
    CO_RETURN Status::InvalidArgument(
        "Cannot call GetEntity without a PinnableWideColumns object");
  }
  if (options.io_activity != Env::IOActivity::kUnknown &&
      options.io_activity != Env::IOActivity::kGetEntity) {
    CO_RETURN Status::InvalidArgument(
        "Can only call GetEntity with `ReadOptions::io_activity` set to "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kGetEntity`");
  }
  ReadOptions read_options(options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kGetEntity;
  }
  columns->Reset();

  SequenceNumber min_uncommitted;
  SequenceNumber snap_seq;
  const SnapshotBackup backed_by_snapshot =
      AssignMinMaxSeqs(read_options.snapshot, &min_uncommitted, &snap_seq);
  WritePreparedTxnReadCallback callback(this, snap_seq, min_uncommitted,
                                        backed_by_snapshot);
  DBImpl::GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.columns = columns;
  get_impl_options.callback = &callback;
  Status s = CO_AWAIT(db_impl_->GetImpl, read_options, key, get_impl_options);
  if (LIKELY(callback.valid() && ValidateSnapshot(callback.max_visible_seq(),
                                                  backed_by_snapshot))) {
    CO_RETURN s;
  }

  s.PermitUncheckedError();
  WPRecordTick(TXN_GET_TRY_AGAIN);
  CO_RETURN Status::TryAgain();
}

DEFINE_SYNC_AND_ASYNC(Status, WritePreparedTxnDB::GetEntity)
(const ReadOptions& options, const Slice& key,
 PinnableAttributeGroups* result) {
  if (!result) {
    CO_RETURN Status::InvalidArgument(
        "Cannot call GetEntity without PinnableAttributeGroups object");
  }
  Status s;
  const size_t num_column_families = result->size();
  if (options.io_activity != Env::IOActivity::kUnknown &&
      options.io_activity != Env::IOActivity::kGetEntity) {
    s = Status::InvalidArgument(
        "Can only call GetEntity with `ReadOptions::io_activity` set to "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kGetEntity`");
    for (size_t i = 0; i < num_column_families; ++i) {
      (*result)[i].SetStatus(s);
    }
    CO_RETURN s;
  }
  if (num_column_families == 0) {
    CO_RETURN s;
  }
  ReadOptions read_options(options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kGetEntity;
  }

  SequenceNumber min_uncommitted;
  SequenceNumber snap_seq;
  const SnapshotBackup backed_by_snapshot =
      AssignMinMaxSeqs(read_options.snapshot, &min_uncommitted, &snap_seq);

  for (size_t i = 0; i < num_column_families; ++i) {
    if (!(*result)[i].column_family()) {
      s = Status::InvalidArgument(
          "DB failed to query because one or more group(s) have null column "
          "family handle");
      (*result)[i].SetStatus(
          Status::InvalidArgument("Column family handle cannot be null"));
      break;
    }
  }
  if (!s.ok()) {
    for (size_t i = 0; i < num_column_families; ++i) {
      if ((*result)[i].status().ok()) {
        (*result)[i].SetStatus(
            Status::Incomplete("DB not queried due to invalid argument(s) in "
                               "one or more of the attribute groups"));
      }
    }
    CO_RETURN s;
  }

  for (size_t i = 0; i < num_column_families; ++i) {
    (*result)[i].Reset();
    PinnableWideColumns columns;
    WritePreparedTxnReadCallback callback(this, snap_seq, min_uncommitted,
                                          backed_by_snapshot);
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = (*result)[i].column_family();
    get_impl_options.columns = &columns;
    get_impl_options.callback = &callback;
    Status get_s =
        CO_AWAIT(db_impl_->GetImpl, read_options, key, get_impl_options);
    if (UNLIKELY(!callback.valid() ||
                 !ValidateSnapshot(callback.max_visible_seq(),
                                   backed_by_snapshot))) {
      // Snapshot validation failed for this column family. Surface TryAgain
      // for this attribute group only and leave the other column families'
      // results intact, matching the per-key semantics of
      // WritePreparedTxnDB::MultiGet and DBImpl::GetEntity.
      get_s.PermitUncheckedError();
      (*result)[i].SetStatus(Status::TryAgain());
      WPRecordTick(TXN_GET_TRY_AGAIN);
      continue;
    }
    (*result)[i].SetStatus(get_s);
    (*result)[i].SetColumns(std::move(columns));
  }
  CO_RETURN s;
}

DEFINE_SYNC_AND_ASYNC(Status, WritePreparedTxnDB::GetImpl)
(const ReadOptions& options, ColumnFamilyHandle* column_family,
 const Slice& key, PinnableSlice* value) {
  SequenceNumber min_uncommitted, snap_seq;
  const SnapshotBackup backed_by_snapshot =
      AssignMinMaxSeqs(options.snapshot, &min_uncommitted, &snap_seq);
  WritePreparedTxnReadCallback callback(this, snap_seq, min_uncommitted,
                                        backed_by_snapshot);
  bool* dont_care = nullptr;
  DBImpl::GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.value = value;
  get_impl_options.value_found = dont_care;
  get_impl_options.callback = &callback;
  Status res = CO_AWAIT(db_impl_->GetImpl, options, key, get_impl_options);
  if (LIKELY(callback.valid() && ValidateSnapshot(callback.max_visible_seq(),
                                                  backed_by_snapshot))) {
    CO_RETURN res;
  }

  res.PermitUncheckedError();
  WPRecordTick(TXN_GET_TRY_AGAIN);
  CO_RETURN Status::TryAgain();
}

DEFINE_SYNC_AND_ASYNC(void, WritePreparedTxnDB::MultiGet)
(const ReadOptions& _read_options, const size_t num_keys,
 ColumnFamilyHandle** column_families, const Slice* keys, PinnableSlice* values,
 std::string* timestamps, Status* statuses, const bool /*sorted_input*/) {
  assert(values);

  Status status;
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kMultiGet) {
    status = Status::InvalidArgument(
        "Can only call MultiGet with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kMultiGet`");
  }

  if (status.ok() && timestamps) {
    status = Status::NotSupported(
        "MultiGet() returning timestamps not implemented.");
  }

  if (!status.ok()) {
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = status;
    }
    CO_RETURN;
  }

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kMultiGet;
  }

  for (size_t i = 0; i < num_keys; ++i) {
    statuses[i] = CO_AWAIT(GetImpl, read_options, column_families[i], keys[i],
                           &values[i]);
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // WITHOUT_COROUTINES || (USE_COROUTINES && WITH_COROUTINES)
