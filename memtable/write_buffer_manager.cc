//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"

#include <chrono>
#include <condition_variable>
#include <memory>
#include <vector>

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "db/db_impl/db_impl.h"
#include "port/port.h"
#include "rocksdb/status.h"
#include "util/atomic.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
struct FlushInitiator::RegistrationState {
  RegistrationState(FlushInitiator* owner_arg, bool atomic_flush_arg)
      : initiator(owner_arg), atomic_flush(atomic_flush_arg) {}

  size_t GetFlushableMemUsage() const {
    if (!flushable.load(std::memory_order_relaxed)) {
      return 0;
    }
    if (!atomic_flush &&
        !largest_mutable_cf_mem_accurate.load(std::memory_order_acquire)) {
      return 0;
    }
    return atomic_flush
               ? total_mutable_mem.load(std::memory_order_relaxed)
               : largest_mutable_cf_mem.load(std::memory_order_relaxed);
  }

  FlushInitiator* Pin() {
    callbacks_in_progress.fetch_add(1, std::memory_order_acq_rel);
    FlushInitiator* const result = initiator.load(std::memory_order_acquire);
    if (result == nullptr) {
      Unpin();
    }
    return result;
  }

  void Unpin() {
    const size_t previous =
        callbacks_in_progress.fetch_sub(1, std::memory_order_acq_rel);
    assert(previous > 0);
    if (previous == 1) {
      std::lock_guard<std::mutex> lock(callbacks_mu);
      callbacks_cv.notify_all();
    }
  }

  void Detach() {
    initiator.store(nullptr, std::memory_order_release);
    std::unique_lock<std::mutex> lock(callbacks_mu);
    callbacks_cv.wait(lock, [this] {
      return callbacks_in_progress.load(std::memory_order_acquire) == 0;
    });
  }

  std::atomic<FlushInitiator*> initiator;
  const bool atomic_flush;
  std::atomic<size_t> total_mutable_mem{0};
  std::atomic<size_t> largest_mutable_cf_mem{0};
  std::atomic<uint64_t> largest_mutable_cf_update_seq{0};
  std::atomic<bool> largest_mutable_cf_mem_accurate{true};
  std::atomic<bool> flushable{true};
  std::atomic<size_t> registry_index{kInvalidRegistryIndex};
  std::atomic<size_t> callbacks_in_progress{0};
  std::mutex callbacks_mu;
  std::condition_variable callbacks_cv;
};

struct WriteBufferManager::FlushInitiatorRegistry {
  explicit FlushInitiatorRegistry(WriteBufferManager* owner_arg)
      : owner(owner_arg) {}

  FlushInitiatorRegistry(const FlushInitiatorRegistry&) = delete;
  FlushInitiatorRegistry& operator=(const FlushInitiatorRegistry&) = delete;
  FlushInitiatorRegistry(FlushInitiatorRegistry&&) = delete;
  FlushInitiatorRegistry& operator=(FlushInitiatorRegistry&&) = delete;

  ~FlushInitiatorRegistry() { Stop(); }

  void Register(
      const std::shared_ptr<FlushInitiator::RegistrationState>& state) {
    {
      std::lock_guard<std::mutex> lock(mu);
      assert(state->registry_index.load(std::memory_order_relaxed) ==
             FlushInitiator::kInvalidRegistryIndex);
      state->registry_index.store(active.size(), std::memory_order_release);
      active.push_back(state);
    }
    if (owner->flush_policy() ==
        WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {
      StartSorter();
    }
    RequestRefresh();
  }

  void Deregister(
      const std::shared_ptr<FlushInitiator::RegistrationState>& state) {
    {
      std::lock_guard<std::mutex> lock(mu);
      const size_t index =
          state->registry_index.load(std::memory_order_acquire);
      assert(index < active.size());
      assert(active[index] == state);
      const std::shared_ptr<FlushInitiator::RegistrationState> last =
          active.back();
      active[index] = last;
      last->registry_index.store(index, std::memory_order_release);
      active.pop_back();
      state->registry_index.store(FlushInitiator::kInvalidRegistryIndex,
                                  std::memory_order_release);

      if (AtomicSharedPtrLoad(&largest, std::memory_order_acquire) == state) {
        AtomicSharedPtrStore(
            &largest, std::shared_ptr<FlushInitiator::RegistrationState>{},
            std::memory_order_release);
      }
    }

    state->Detach();
    RequestRefresh();
  }

  void PolicyChanged(WriteBufferFlushPolicy policy) {
    if (policy != WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {
      StopSorter();
      return;
    }
    StartSorter();
    RequestRefresh();
  }

  void RequestRefresh() {
    refresh_requested.store(true, std::memory_order_release);
    cv.notify_one();
  }

  void Refresh() {
    std::lock_guard<std::mutex> refresh_lock(refresh_mu);
    {
      std::lock_guard<std::mutex> lock(mu);
      refresh_snapshot.assign(active.begin(), active.end());
    }

    std::shared_ptr<FlushInitiator::RegistrationState> best;
    size_t best_mem = 0;
    if (owner->flush_policy() ==
        WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {
      for (const auto& candidate : refresh_snapshot) {
        if (candidate->registry_index.load(std::memory_order_acquire) ==
            FlushInitiator::kInvalidRegistryIndex) {
          continue;
        }
        const size_t mem = candidate->GetFlushableMemUsage();
        if (best == nullptr || mem > best_mem) {
          best = candidate;
          best_mem = mem;
        }
      }
    }
    {
      std::lock_guard<std::mutex> lock(mu);
      if (owner->flush_policy() ==
              WriteBufferFlushPolicy::kFlushLargestAcrossDBs &&
          best != nullptr && best_mem > 0 &&
          best->registry_index.load(std::memory_order_acquire) !=
              FlushInitiator::kInvalidRegistryIndex) {
        AtomicSharedPtrStore(&largest, best, std::memory_order_release);
      } else {
        AtomicSharedPtrStore(
            &largest, std::shared_ptr<FlushInitiator::RegistrationState>{},
            std::memory_order_release);
      }
    }
    refresh_snapshot.clear();
  }

  std::shared_ptr<FlushInitiator::RegistrationState> PinLargest(
      FlushInitiator** initiator) {
    std::shared_ptr<FlushInitiator::RegistrationState> state =
        AtomicSharedPtrLoad(&largest, std::memory_order_acquire);
    *initiator = state == nullptr ? nullptr : state->Pin();
    if (*initiator == nullptr) {
      state.reset();
    }
    return state;
  }

  void Stop() {
    StopSorter();
    AtomicSharedPtrStore(&largest,
                         std::shared_ptr<FlushInitiator::RegistrationState>{},
                         std::memory_order_release);
  }

  bool Empty() const {
    std::lock_guard<std::mutex> lock(mu);
    return active.empty();
  }

  size_t TEST_Size() const {
    std::lock_guard<std::mutex> lock(mu);
    return active.size();
  }

  bool TEST_HasSorter() const {
    std::lock_guard<std::mutex> lifecycle_lock(sorter_lifecycle_mu);
    return sorter != nullptr;
  }

 private:
  void StartSorter() {
    std::lock_guard<std::mutex> lifecycle_lock(sorter_lifecycle_mu);
    {
      std::lock_guard<std::mutex> lock(mu);
      if (owner->flush_policy() !=
              WriteBufferFlushPolicy::kFlushLargestAcrossDBs ||
          active.empty() || sorter != nullptr) {
        return;
      }
      stopping = false;
      sorter = std::make_unique<port::Thread>([this] { Run(); });
    }
  }

  void StopSorter() {
    std::lock_guard<std::mutex> lifecycle_lock(sorter_lifecycle_mu);
    std::unique_ptr<port::Thread> sorter_to_join;
    {
      std::lock_guard<std::mutex> lock(mu);
      stopping = true;
      sorter_to_join = std::move(sorter);
      AtomicSharedPtrStore(&largest,
                           std::shared_ptr<FlushInitiator::RegistrationState>{},
                           std::memory_order_release);
    }
    cv.notify_all();
    if (sorter_to_join != nullptr) {
      sorter_to_join->join();
    }
  }

  void Run() {
    constexpr auto kPressureRefreshInterval = std::chrono::milliseconds(10);
    constexpr auto kIdleRefreshInterval = std::chrono::seconds(1);
    for (;;) {
      Refresh();
      std::unique_lock<std::mutex> lock(mu);
      const auto refresh_interval = owner->ShouldFlush()
                                        ? kPressureRefreshInterval
                                        : kIdleRefreshInterval;
      cv.wait_for(lock, refresh_interval, [this] {
        return stopping ||
               refresh_requested.exchange(false, std::memory_order_acq_rel);
      });
      if (stopping) {
        return;
      }
    }
  }

  WriteBufferManager* const owner;
  mutable std::mutex mu;
  std::condition_variable cv;
  bool stopping = false;
  std::atomic<bool> refresh_requested{false};
  mutable std::mutex sorter_lifecycle_mu;
  std::unique_ptr<port::Thread> sorter;
  std::mutex refresh_mu;
  std::vector<std::shared_ptr<FlushInitiator::RegistrationState>>
      refresh_snapshot;
  std::vector<std::shared_ptr<FlushInitiator::RegistrationState>> active;
  std::shared_ptr<FlushInitiator::RegistrationState> largest;
};

WriteBufferManager::WriteBufferManager(size_t _buffer_size,
                                       std::shared_ptr<Cache> cache,
                                       bool allow_stall)
    : WriteBufferManager(_buffer_size, cache, allow_stall,
                         WriteBufferFlushPolicy::kFlushOldest) {}

WriteBufferManager::WriteBufferManager(size_t _buffer_size,
                                       std::shared_ptr<Cache> cache,
                                       bool allow_stall,
                                       WriteBufferFlushPolicy flush_policy)
    : buffer_size_(_buffer_size),
      mutable_limit_(buffer_size_ * 7 / 8),
      memory_used_(0),
      memory_active_(0),
      cache_res_mgr_(nullptr),
      allow_stall_(allow_stall),
      stall_active_(false),
      flush_policy_(flush_policy),
      flush_initiator_registry_(
          std::make_unique<FlushInitiatorRegistry>(this)) {
  if (cache) {
    // Memtable's memory usage tends to fluctuate frequently
    // therefore we set delayed_decrease = true to save some dummy entry
    // insertion on memory increase right after memory decrease
    cache_res_mgr_ = std::make_shared<
        CacheReservationManagerImpl<CacheEntryRole::kWriteBuffer>>(
        cache, true /* delayed_decrease */);
  }
}

WriteBufferManager::~WriteBufferManager() {
  flush_initiator_registry_->Stop();
#ifndef NDEBUG
  {
    std::unique_lock<std::mutex> lock(mu_);
    assert(queue_.empty());
  }
  assert(flush_initiator_registry_->Empty());
#endif
}

void WriteBufferManager::SetFlushPolicy(
    WriteBufferFlushPolicy new_flush_policy) {
  std::lock_guard<std::mutex> lock(flush_policy_mu_);
  flush_policy_.store(new_flush_policy, std::memory_order_relaxed);
  flush_initiator_registry_->PolicyChanged(new_flush_policy);
}

std::size_t WriteBufferManager::dummy_entries_in_cache_usage() const {
  if (cache_res_mgr_ != nullptr) {
    return cache_res_mgr_->GetTotalReservedCacheSize();
  } else {
    return 0;
  }
}

void WriteBufferManager::ReserveMem(size_t mem) {
  if (cache_res_mgr_ != nullptr) {
    ReserveMemWithCache(mem);
  } else if (enabled()) {
    memory_used_.fetch_add(mem, std::memory_order_relaxed);
  }
  if (enabled()) {
    const size_t previous =
        memory_active_.fetch_add(mem, std::memory_order_relaxed);
    const size_t mutable_limit = mutable_limit_.load(std::memory_order_relaxed);
    if (previous <= mutable_limit && previous + mem > mutable_limit) {
      flush_initiator_registry_->RequestRefresh();
    }
  }
}

// Should only be called from write thread
void WriteBufferManager::ReserveMemWithCache(size_t mem) {
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);

  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) + mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s = cache_res_mgr_->UpdateCacheReservation(new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly. Ideallly we should prevent this allocation
  // from happening if this cache charging fails.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
}

void WriteBufferManager::ScheduleFreeMem(size_t mem) {
  if (enabled()) {
    memory_active_.fetch_sub(mem, std::memory_order_relaxed);
  }
}

void WriteBufferManager::FreeMem(size_t mem) {
  if (cache_res_mgr_ != nullptr) {
    FreeMemWithCache(mem);
  } else if (enabled()) {
    memory_used_.fetch_sub(mem, std::memory_order_relaxed);
  }
  // Check if stall is active and can be ended.
  MaybeEndWriteStall();
}

void WriteBufferManager::FreeMemWithCache(size_t mem) {
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);
  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) - mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s = cache_res_mgr_->UpdateCacheReservation(new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
}

void WriteBufferManager::BeginWriteStall(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);

  // Allocate outside of the lock.
  std::list<StallInterface*> new_node = {wbm_stall};

  {
    std::unique_lock<std::mutex> lock(mu_);
    // Verify if the stall conditions are stil active.
    if (ShouldStall()) {
      stall_active_.store(true, std::memory_order_relaxed);
      queue_.splice(queue_.end(), new_node);
    }
  }

  // If the node was not consumed, the stall has ended already and we can signal
  // the caller.
  if (!new_node.empty()) {
    new_node.front()->Signal();
  }
}

// Called when memory is freed in FreeMem or the buffer size has changed.
void WriteBufferManager::MaybeEndWriteStall() {
  // Stall conditions have not been resolved.
  if (allow_stall_.load(std::memory_order_relaxed) &&
      IsStallThresholdExceeded()) {
    return;
  }

  // Perform all deallocations outside of the lock.
  std::list<StallInterface*> cleanup;

  std::unique_lock<std::mutex> lock(mu_);
  if (!stall_active_.load(std::memory_order_relaxed)) {
    return;  // Nothing to do.
  }

  // Unblock new writers.
  stall_active_.store(false, std::memory_order_relaxed);

  // Unblock the writers in the queue.
  for (StallInterface* wbm_stall : queue_) {
    wbm_stall->Signal();
  }
  cleanup = std::move(queue_);
}

void WriteBufferManager::RemoveDBFromQueue(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);

  // Deallocate the removed nodes outside of the lock.
  std::list<StallInterface*> cleanup;

  if (enabled() && allow_stall_.load(std::memory_order_relaxed)) {
    std::unique_lock<std::mutex> lock(mu_);
    for (auto it = queue_.begin(); it != queue_.end();) {
      auto next = std::next(it);
      if (*it == wbm_stall) {
        cleanup.splice(cleanup.end(), queue_, std::move(it));
      }
      it = next;
    }
  }
  wbm_stall->Signal();
}

FlushInitiator::FlushInitiator(bool atomic_flush)
    : registration_state_(
          std::make_shared<RegistrationState>(this, atomic_flush)) {}

FlushInitiator::~FlushInitiator() {
  assert(!IsRegistered());
  registration_state_->Detach();
}

void FlushInitiator::ReserveMem(size_t mem, size_t memtable_mem) {
  registration_state_->total_mutable_mem.fetch_add(mem,
                                                   std::memory_order_relaxed);
  if (!registration_state_->atomic_flush) {
    UpdateLargestMutableCFMem(memtable_mem);
  }
}

void FlushInitiator::SetLargestMutableCFMem(size_t mem) {
  registration_state_->largest_mutable_cf_mem.store(mem,
                                                    std::memory_order_seq_cst);
  registration_state_->largest_mutable_cf_mem_accurate.store(
      true, std::memory_order_release);
}

bool FlushInitiator::TrySetLargestMutableCFMem(size_t mem,
                                               uint64_t update_seq) {
  if (registration_state_->largest_mutable_cf_update_seq.load(
          std::memory_order_seq_cst) != update_seq) {
    return false;
  }

  size_t previous = registration_state_->largest_mutable_cf_mem.load(
      std::memory_order_seq_cst);
  if (!registration_state_->largest_mutable_cf_mem.compare_exchange_strong(
          previous, mem, std::memory_order_seq_cst)) {
    return false;
  }
  if (registration_state_->largest_mutable_cf_update_seq.load(
          std::memory_order_seq_cst) == update_seq) {
    registration_state_->largest_mutable_cf_mem_accurate.store(
        true, std::memory_order_release);
    return true;
  }

  // Preserve the old upper bound when an allocation overlapped the rebuild.
  // The allocator might have skipped its max update based on that value.
  size_t current = registration_state_->largest_mutable_cf_mem.load(
      std::memory_order_seq_cst);
  while (current < previous &&
         !registration_state_->largest_mutable_cf_mem.compare_exchange_weak(
             current, previous, std::memory_order_seq_cst)) {
  }
  return false;
}

void FlushInitiator::InvalidateLargestMutableCFMem() {
  registration_state_->largest_mutable_cf_mem_accurate.store(
      false, std::memory_order_release);
}

void FlushInitiator::UpdateLargestMutableCFMem(size_t mem) {
  registration_state_->largest_mutable_cf_update_seq.fetch_add(
      1, std::memory_order_seq_cst);
  size_t largest = registration_state_->largest_mutable_cf_mem.load(
      std::memory_order_seq_cst);
  while (largest < mem &&
         !registration_state_->largest_mutable_cf_mem.compare_exchange_weak(
             largest, mem, std::memory_order_seq_cst)) {
  }
}

uint64_t FlushInitiator::GetLargestMutableCFUpdateSequence() const {
  return registration_state_->largest_mutable_cf_update_seq.load(
      std::memory_order_seq_cst);
}

void FlushInitiator::ScheduleFreeMem(size_t mem) {
  [[maybe_unused]] const size_t previous =
      registration_state_->total_mutable_mem.fetch_sub(
          mem, std::memory_order_relaxed);
  assert(previous >= mem);
}

size_t FlushInitiator::GetTotalMutableMem() const {
  return registration_state_->total_mutable_mem.load(std::memory_order_relaxed);
}

size_t FlushInitiator::GetLargestMutableCFMem() const {
  return registration_state_->largest_mutable_cf_mem.load(
      std::memory_order_relaxed);
}

size_t FlushInitiator::GetFlushableMemUsage() const {
  return registration_state_->GetFlushableMemUsage();
}

bool FlushInitiator::HasAccurateFlushableMemUsage() const {
  return registration_state_->atomic_flush ||
         registration_state_->largest_mutable_cf_mem_accurate.load(
             std::memory_order_acquire);
}

bool FlushInitiator::UsesTotalMutableMem() const {
  return registration_state_->atomic_flush;
}

void FlushInitiator::SetFlushable(bool flushable) {
  registration_state_->flushable.store(flushable, std::memory_order_relaxed);
}

bool FlushInitiator::IsRegistered() const {
  return registration_state_->registry_index.load(std::memory_order_acquire) !=
         kInvalidRegistryIndex;
}

void WriteBufferManager::RegisterFlushInitiator(FlushInitiator* initiator) {
  assert(initiator != nullptr);
  flush_initiator_registry_->Register(initiator->registration_state_);
}

void WriteBufferManager::DeregisterFlushInitiator(FlushInitiator* initiator) {
  assert(initiator != nullptr);
  flush_initiator_registry_->Deregister(initiator->registration_state_);
}

void WriteBufferManager::NotifyFlushInitiatorChanged() {
  flush_initiator_registry_->RequestRefresh();
}

bool WriteBufferManager::InitiateFlushOnLargestDB(FlushInitiator* self) {
  if (self != nullptr && !self->HasAccurateFlushableMemUsage()) {
    return false;
  }
  FlushInitiator* initiator = nullptr;
  const std::shared_ptr<FlushInitiator::RegistrationState> best =
      flush_initiator_registry_->PinLargest(&initiator);
  if (best == nullptr ||
      (self != nullptr && best.get() == self->registration_state_.get())) {
    if (best != nullptr) {
      best->Unpin();
    }
    return false;
  }
  const size_t best_mem = best->GetFlushableMemUsage();
  if (best_mem == 0 ||
      (self != nullptr && best_mem <= self->GetFlushableMemUsage())) {
    best->Unpin();
    return false;
  }

  const bool scheduled = initiator->ScheduleFlush();
  best->Unpin();
  return scheduled;
}

size_t WriteBufferManager::TEST_GetFlushInitiatorRegistrySize() const {
  return flush_initiator_registry_->TEST_Size();
}

bool WriteBufferManager::TEST_HasFlushInitiatorSorter() const {
  return flush_initiator_registry_->TEST_HasSorter();
}

void WriteBufferManager::TEST_RefreshFlushInitiatorCandidate() {
  flush_initiator_registry_->Refresh();
}

}  // namespace ROCKSDB_NAMESPACE
