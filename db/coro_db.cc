// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/rocksdb_namespace.h"

#if USE_COROUTINES

#include <algorithm>
#include <array>
#include <cassert>
#include <vector>

#include "folly/Executor.h"
#include "folly/coro/Nothrow.h"
#include "folly/executors/IOExecutor.h"
#include "folly/io/async/EventBase.h"
#include "rocksdb/coro_db.h"
#include "rocksdb/file_system.h"
#include "table/multiget_context.h"

namespace ROCKSDB_NAMESPACE {

folly::coro::Task<Status> CoroDB::CoGet(DB* db, const ReadOptions& options,
                                        ColumnFamilyHandle* column_family,
                                        const Slice& key, PinnableSlice* value,
                                        std::string* timestamp) {
  assert(db != nullptr);
  CoroDB* coro_db = db->GetCoroDB();
  auto* file_system = db->GetFileSystem();
  auto* read_executor =
      file_system == nullptr ? nullptr : file_system->GetReadExecutor();
  auto* read_event_base =
      read_executor == nullptr ? nullptr : read_executor->getEventBase();

  Status status;
  if (coro_db == nullptr || read_event_base == nullptr) {
    status = db->Get(options, column_family, key, value, timestamp);
  } else {
    status = co_await folly::coro::co_nothrow(folly::coro::co_withExecutor(
        folly::Executor::getKeepAliveToken(read_event_base),
        coro_db->GetCoroutine(options, column_family, key, value, timestamp)));
  }
  co_return status;
}

folly::coro::Task<Status> CoroDB::CoGet(DB* db, const ReadOptions& options,
                                        ColumnFamilyHandle* column_family,
                                        const Slice& key, std::string* value,
                                        std::string* timestamp) {
  assert(value != nullptr);
  PinnableSlice pinnable_value(value);
  assert(!pinnable_value.IsPinned());
  Status status = co_await folly::coro::co_nothrow(
      CoGet(db, options, column_family, key, &pinnable_value, timestamp));
  if (status.ok() && pinnable_value.IsPinned()) {
    value->assign(pinnable_value.data(), pinnable_value.size());
  }
  co_return status;
}

folly::coro::Task<Status> CoroDB::CoGet(DB* db, const ReadOptions& options,
                                        const Slice& key, std::string* value) {
  assert(db != nullptr);
  return CoGet(db, options, db->DefaultColumnFamily(), key, value);
}

folly::coro::Task<Status> CoroDB::CoGet(DB* db, const ReadOptions& options,
                                        const Slice& key, std::string* value,
                                        std::string* timestamp) {
  assert(db != nullptr);
  return CoGet(db, options, db->DefaultColumnFamily(), key, value, timestamp);
}

folly::coro::Task<Status> CoroDB::CoGetEntity(DB* db,
                                              const ReadOptions& options,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key,
                                              PinnableWideColumns* columns) {
  assert(db != nullptr);
  CoroDB* coro_db = db->GetCoroDB();
  auto* file_system = db->GetFileSystem();
  auto* read_executor =
      file_system == nullptr ? nullptr : file_system->GetReadExecutor();
  auto* read_event_base =
      read_executor == nullptr ? nullptr : read_executor->getEventBase();

  Status status;
  if (coro_db == nullptr || read_event_base == nullptr) {
    status = db->GetEntity(options, column_family, key, columns);
  } else {
    status = co_await folly::coro::co_nothrow(folly::coro::co_withExecutor(
        folly::Executor::getKeepAliveToken(read_event_base),
        coro_db->GetEntityCoroutine(options, column_family, key, columns)));
  }
  co_return status;
}

folly::coro::Task<Status> CoroDB::CoGetEntity(DB* db,
                                              const ReadOptions& options,
                                              const Slice& key,
                                              PinnableWideColumns* columns) {
  assert(db != nullptr);
  return CoGetEntity(db, options, db->DefaultColumnFamily(), key, columns);
}

folly::coro::Task<Status> CoroDB::CoGetEntity(DB* db,
                                              const ReadOptions& options,
                                              const Slice& key,
                                              PinnableAttributeGroups* result) {
  assert(db != nullptr);
  CoroDB* coro_db = db->GetCoroDB();
  auto* file_system = db->GetFileSystem();
  auto* read_executor =
      file_system == nullptr ? nullptr : file_system->GetReadExecutor();
  auto* read_event_base =
      read_executor == nullptr ? nullptr : read_executor->getEventBase();

  Status status;
  if (coro_db == nullptr || read_event_base == nullptr) {
    status = db->GetEntity(options, key, result);
  } else {
    status = co_await folly::coro::co_nothrow(folly::coro::co_withExecutor(
        folly::Executor::getKeepAliveToken(read_event_base),
        coro_db->GetEntityCoroutine(options, key, result)));
  }
  co_return status;
}

folly::coro::Task<std::vector<Status>> CoroDB::CoMultiGet(
    DB* db, const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    const std::vector<Slice>& keys, std::vector<std::string>* values,
    std::vector<std::string>* timestamps) {
  const size_t num_keys = keys.size();
  std::vector<Status> statuses(num_keys);
  std::vector<PinnableSlice> pinnable_values(num_keys);

  values->resize(num_keys);
  if (timestamps != nullptr) {
    timestamps->resize(num_keys);
  }
  co_await folly::coro::co_nothrow(CoMultiGet(
      db, options, num_keys,
      const_cast<ColumnFamilyHandle**>(column_families.data()), keys.data(),
      pinnable_values.data(),
      timestamps == nullptr ? nullptr : timestamps->data(), statuses.data(),
      /*sorted_input=*/false));
  for (size_t i = 0; i < num_keys; ++i) {
    if (statuses[i].ok()) {
      (*values)[i].assign(pinnable_values[i].data(), pinnable_values[i].size());
    }
  }
  co_return statuses;
}

folly::coro::Task<std::vector<Status>> CoroDB::CoMultiGet(
    DB* db, const ReadOptions& options, const std::vector<Slice>& keys,
    std::vector<std::string>* values, std::vector<std::string>* timestamps) {
  assert(db != nullptr);
  std::vector<ColumnFamilyHandle*> column_families(keys.size(),
                                                   db->DefaultColumnFamily());
  co_return co_await folly::coro::co_nothrow(
      CoMultiGet(db, options, column_families, keys, values, timestamps));
}

folly::coro::Task<void> CoroDB::CoMultiGet(
    DB* db, const ReadOptions& options, size_t num_keys,
    ColumnFamilyHandle** column_families, const Slice* keys,
    PinnableSlice* values, std::string* timestamps, Status* statuses,
    bool sorted_input) {
  assert(db != nullptr);
  CoroDB* coro_db = db->GetCoroDB();
  auto* file_system = db->GetFileSystem();
  auto* read_executor =
      file_system == nullptr ? nullptr : file_system->GetReadExecutor();
  auto* read_event_base =
      read_executor == nullptr ? nullptr : read_executor->getEventBase();

  if (coro_db == nullptr || read_event_base == nullptr) {
    db->MultiGet(options, num_keys, column_families, keys, values, timestamps,
                 statuses, sorted_input);
  } else {
    co_await folly::coro::co_nothrow(folly::coro::co_withExecutor(
        folly::Executor::getKeepAliveToken(read_event_base),
        coro_db->MultiGetCoroutine(options, num_keys, column_families, keys,
                                   values, timestamps, statuses,
                                   sorted_input)));
  }
}

folly::coro::Task<void> CoroDB::CoMultiGet(
    DB* db, const ReadOptions& options, ColumnFamilyHandle* column_family,
    size_t num_keys, const Slice* keys, PinnableSlice* values,
    std::string* timestamps, Status* statuses, bool sorted_input) {
  if (num_keys > MultiGetContext::MAX_BATCH_SIZE) {
    std::vector<ColumnFamilyHandle*> column_families(num_keys, column_family);
    co_await folly::coro::co_nothrow(
        CoMultiGet(db, options, num_keys, column_families.data(), keys, values,
                   timestamps, statuses, sorted_input));
  } else {
    std::array<ColumnFamilyHandle*, MultiGetContext::MAX_BATCH_SIZE>
        column_families;
    std::fill(column_families.begin(), column_families.begin() + num_keys,
              column_family);
    co_await folly::coro::co_nothrow(
        CoMultiGet(db, options, num_keys, column_families.data(), keys, values,
                   timestamps, statuses, sorted_input));
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // USE_COROUTINES
