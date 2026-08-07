//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <string>

#include "folly/coro/Task.h"
#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL native coroutine read interface.
//
// These are the coroutine counterparts to DB::GetAsync() and
// DB::MultiGetAsync(). See those APIs for the shared read, input/output
// lifetime, and concurrency contract. Task completion takes the place of
// OnComplete for those requirements.
//
// The returned tasks are lazy: no read begins until a task is awaited or
// started.
//
// IMPORTANT: RocksDB assumes each request runs entirely on one thread. Schedule
// coroutine reads on the read IOExecutor's EventBase and do not migrate them.
//
// STATS:
// Unlike the callback APIs, coroutine stats are returned through TLS. Each task
// publishes its request-local PerfContext and IOStatsContext to TLS on its
// execution thread before completing. Read them after the await resumes on the
// same EventBase:
//
//   auto* coro_db = db->GetCoroDB();
//   auto* read_executor = db->GetFileSystem()->GetReadExecutor();
//   auto* read_event_base =
//       read_executor == nullptr ? nullptr : read_executor->getEventBase();
//   if (coro_db == nullptr || read_event_base == nullptr) {
//     co_return db->Get(options, column_family, key, value, timestamp);
//   }
//
//   auto read = [&]() -> folly::coro::Task<Status> {
//     Status status = co_await coro_db->GetCoroutine(
//         options, column_family, key, value, timestamp);
//     const PerfContext* perf_context = get_perf_context();
//     const IOStatsContext* iostats_context = get_iostats_context();
//     // Consume or copy the stats before leaving this EventBase.
//     co_return status;
//   };
//   co_return co_await folly::coro::co_withExecutor(
//       folly::Executor::getKeepAliveToken(read_event_base), read());
class CoroDB {
 public:
  virtual ~CoroDB() = default;

  // Reads key and completes after populating value and the optional timestamp.
  // The returned Status describes the operation and its outputs.
  virtual folly::coro::Task<Status> GetCoroutine(
      const ReadOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, PinnableSlice* value, std::string* timestamp) = 0;

  // Reads num_keys keys and completes after populating all per-key statuses,
  // values, and optional timestamps. There is no aggregate Status.
  virtual folly::coro::Task<void> MultiGetCoroutine(
      const ReadOptions& options, size_t num_keys,
      ColumnFamilyHandle** column_families, const Slice* keys,
      PinnableSlice* values, std::string* timestamps, Status* statuses,
      bool sorted_input) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
