//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class WriteBufferManager;

class FlushInitiator {
 public:
  explicit FlushInitiator(bool atomic_flush);
  virtual ~FlushInitiator();

  // The stable registration state is published independently of this object,
  // so initiators must not move while registered.
  FlushInitiator(const FlushInitiator&) = delete;
  FlushInitiator& operator=(const FlushInitiator&) = delete;
  FlushInitiator(FlushInitiator&&) = delete;
  FlushInitiator& operator=(FlushInitiator&&) = delete;

  // `memtable_mem` is the memtable's total allocation after adding `mem`.
  void ReserveMem(size_t mem, size_t memtable_mem);

  // Removes a sealed memtable from the mutable-memory total. The owner
  // refreshes the largest-CF counter after installing its replacement.
  void ScheduleFreeMem(size_t mem);

  // Replaces the cached maximum only if no allocation update overlapped the
  // caller's scan.
  bool TrySetLargestMutableCFMem(size_t mem, uint64_t update_seq);

  // Uses conservative total mutable memory for selection until the cached
  // maximum can be rebuilt.
  void InvalidateLargestMutableCFMem();
  void UpdateLargestMutableCFMem(size_t mem);
  uint64_t GetLargestMutableCFUpdateSequence() const;
  size_t GetTotalMutableMem() const;
  size_t GetLargestMutableCFMem() const;
  size_t GetFlushableMemUsage() const;

  // False while counters are being rebuilt after a runtime policy change.
  bool HasAccurateFlushableMemUsage() const;
  void MarkFlushableMemUsageAccurate();
  bool UsesTotalMutableMem() const;
  void SetFlushable(bool flushable);
  void SetHasFlushableCF(bool has_flushable_cf);
  bool IsRegistered() const;

  // Queues one asynchronous flush. Must not reacquire the registry mutex.
  // False lets the coordinator advance to another candidate.
  virtual bool ScheduleFlush() = 0;

  // Tries to rebuild counters without waiting for the owning DB's mutex.
  virtual bool TryRefreshMemoryAccounting() { return false; }

 private:
  friend class WriteBufferManager;

  struct RegistrationState;

  static constexpr size_t kInvalidRegistryIndex =
      std::numeric_limits<size_t>::max();

  std::shared_ptr<RegistrationState> registration_state_;
};

}  // namespace ROCKSDB_NAMESPACE
