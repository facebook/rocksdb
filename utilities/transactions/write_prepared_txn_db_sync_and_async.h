//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/coro_utils.h"

#if defined(USE_COROUTINES) && defined(WITH_COROUTINES)
#include "util/coro_stats_util.h"
#endif  // USE_COROUTINES && WITH_COROUTINES

#if defined(WITHOUT_COROUTINES) || (USE_COROUTINES && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {

DEFINE_SYNC_AND_ASYNC(Status, WritePreparedTxnDB::Get)
(const ReadOptions& _read_options, ColumnFamilyHandle* column_family,
 const Slice& key, PinnableSlice* value, std::string* timestamp) {
#ifdef WITH_COROUTINES
  INSTALL_COROUTINE_STATS_CONTEXT_SCOPE(
      db_impl_->GetFileSystem()->GetReadExecutor(), db_impl_->GetEnv());
#endif
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
#ifdef WITH_COROUTINES
  INSTALL_COROUTINE_STATS_CONTEXT_SCOPE(
      db_impl_->GetFileSystem()->GetReadExecutor(), db_impl_->GetEnv());
#endif
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
#ifdef WITH_COROUTINES
  INSTALL_COROUTINE_STATS_CONTEXT_SCOPE(
      db_impl_->GetFileSystem()->GetReadExecutor(), db_impl_->GetEnv());
#endif
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
