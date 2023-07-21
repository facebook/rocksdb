//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/secondary_cache_adapter.h"

#include "monitoring/perf_context_imp.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {
// A distinct pointer value for marking "dummy" cache entries
struct Dummy {
  char val[7] = "kDummy";
};
const Dummy kDummy{};
Cache::ObjectPtr const kDummyObj = const_cast<Dummy*>(&kDummy);
}  // namespace

// When CacheWithSecondaryAdapter is constructed with the distribute_cache_res
// parameter set to true, it manages the entire memory budget across the
// primary and secondary cache. The secondary cache is assumed to be in
// memory, such as the CompressedSecondaryCache. When a placeholder entry
// is inserted by a CacheReservationManager instance to reserve memory,
// the CacheWithSecondaryAdapter ensures that the reservation is distributed
// proportionally across the primary/secondary caches.
//
// The primary block cache is initially sized to the sum of the primary cache
// budget + teh secondary cache budget, as follows -
//   |---------    Primary Cache Configured Capacity  -----------|
//   |---Secondary Cache Budget----|----Primary Cache Budget-----|
//
// A ConcurrentCacheReservationManager member in the CacheWithSecondaryAdapter,
// pri_cache_res_,
// is used to help with tracking the distribution of memory reservations.
// Initially, it accounts for the entire secondary cache budget as a
// reservation against the primary cache. This shrinks the usable capacity of
// the primary cache to the budget that the user originally desired.
//
//   |--Reservation for Sec Cache--|-Pri Cache Usable Capacity---|
//
// When a reservation placeholder is inserted into the adapter, it is inserted
// directly into the primary cache. This means the entire charge of the
// placeholder is counted against the primary cache. To compensate and count
// a portion of it against the secondary cache, the secondary cache Deflate()
// method is called to shrink it. Since the Deflate() causes the secondary
// actual usage to shrink, it is refelcted here by releasing an equal amount
// from the pri_cache_res_ reservation. The Deflate() in the secondary cache
// can be, but is not required to be, implemented using its own cache
// reservation manager.
//
// For example, if the pri/sec ratio is 70/30, and the combined capacity is
// 100MB, the intermediate and final  state after inserting a reservation
// placeholder for 10MB would be as follows -
//
//   |-Reservation for Sec Cache-|-Pri Cache Usable Capacity-|---R---|
// 1. After inserting the placeholder in primary
//   |-------  30MB -------------|------- 60MB -------------|-10MB--|
// 2. After deflating the secondary and adjusting the reservation for
//    secondary against the primary
//   |-------  27MB -------------|------- 63MB -------------|-10MB--|
//
// Likewise, when the user inserted placeholder is released, the secondary
// cache Inflate() method is called to grow it, and the pri_cache_res_
// reservation is increased by an equal amount.
//
// Another way of implementing this would have been to simply split the user
// reservation into primary and seconary components. However, this would
// require allocating a structure to track the associated secondary cache
// reservation, which adds some complexity and overhead.
//
CacheWithSecondaryAdapter::CacheWithSecondaryAdapter(
    std::shared_ptr<Cache> target,
    std::shared_ptr<SecondaryCache> secondary_cache, bool distribute_cache_res)
    : CacheWrapper(std::move(target)),
      secondary_cache_(std::move(secondary_cache)),
      distribute_cache_res_(distribute_cache_res) {
  target_->SetEvictionCallback([this](const Slice& key, Handle* handle) {
    return EvictionHandler(key, handle);
  });
  if (distribute_cache_res_) {
    size_t sec_capacity = 0;
    pri_cache_res_ = std::make_shared<ConcurrentCacheReservationManager>(
        std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
            target_));
    Status s = secondary_cache_->GetCapacity(sec_capacity);
    assert(s.ok());
    // Initially, the primary cache is sized to uncompressed cache budget plsu
    // compressed secondary cache budget. The secondary cache budget is then
    // taken away from the primary cache through cache reservations. Later,
    // when a placeholder entry is inserted by the caller, its inserted
    // into the primary cache and the portion that should be assigned to the
    // secondary cache is freed from the reservation.
    s = pri_cache_res_->UpdateCacheReservation(sec_capacity);
    assert(s.ok());
    sec_cache_res_ratio_ = (double)sec_capacity / target_->GetCapacity();
  }
}

CacheWithSecondaryAdapter::~CacheWithSecondaryAdapter() {
  // `*this` will be destroyed before `*target_`, so we have to prevent
  // use after free
  target_->SetEvictionCallback({});
#ifndef NDEBUG
  if (distribute_cache_res_) {
    size_t sec_capacity = 0;
    Status s = secondary_cache_->GetCapacity(sec_capacity);
    assert(s.ok());
    assert(pri_cache_res_->GetTotalReservedCacheSize() == sec_capacity);
  }
#endif  // NDEBUG
}

bool CacheWithSecondaryAdapter::EvictionHandler(const Slice& key,
                                                Handle* handle) {
  auto helper = GetCacheItemHelper(handle);
  if (helper->IsSecondaryCacheCompatible()) {
    auto obj = target_->Value(handle);
    // Ignore dummy entry
    if (obj != kDummyObj) {
      // Spill into secondary cache.
      secondary_cache_->Insert(key, obj, helper).PermitUncheckedError();
    }
  }
  // Never takes ownership of obj
  return false;
}

bool CacheWithSecondaryAdapter::ProcessDummyResult(Cache::Handle** handle,
                                                   bool erase) {
  if (*handle && target_->Value(*handle) == kDummyObj) {
    target_->Release(*handle, erase);
    *handle = nullptr;
    return true;
  } else {
    return false;
  }
}

void CacheWithSecondaryAdapter::CleanupCacheObject(
    ObjectPtr obj, const CacheItemHelper* helper) {
  if (helper->del_cb) {
    helper->del_cb(obj, memory_allocator());
  }
}

Cache::Handle* CacheWithSecondaryAdapter::Promote(
    std::unique_ptr<SecondaryCacheResultHandle>&& secondary_handle,
    const Slice& key, const CacheItemHelper* helper, Priority priority,
    Statistics* stats, bool found_dummy_entry, bool kept_in_sec_cache) {
  assert(secondary_handle->IsReady());

  ObjectPtr obj = secondary_handle->Value();
  if (!obj) {
    // Nothing found.
    return nullptr;
  }
  // Found something.
  switch (helper->role) {
    case CacheEntryRole::kFilterBlock:
      RecordTick(stats, SECONDARY_CACHE_FILTER_HITS);
      break;
    case CacheEntryRole::kIndexBlock:
      RecordTick(stats, SECONDARY_CACHE_INDEX_HITS);
      break;
    case CacheEntryRole::kDataBlock:
      RecordTick(stats, SECONDARY_CACHE_DATA_HITS);
      break;
    default:
      break;
  }
  PERF_COUNTER_ADD(secondary_cache_hit_count, 1);
  RecordTick(stats, SECONDARY_CACHE_HITS);

  // Note: SecondaryCache::Size() is really charge (from the CreateCallback)
  size_t charge = secondary_handle->Size();
  Handle* result = nullptr;
  // Insert into primary cache, possibly as a standalone+dummy entries.
  if (secondary_cache_->SupportForceErase() && !found_dummy_entry) {
    // Create standalone and insert dummy
    // Allow standalone to be created even if cache is full, to avoid
    // reading the entry from storage.
    result =
        CreateStandalone(key, obj, helper, charge, /*allow_uncharged*/ true);
    assert(result);
    PERF_COUNTER_ADD(block_cache_standalone_handle_count, 1);

    // Insert dummy to record recent use
    // TODO: try to avoid case where inserting this dummy could overwrite a
    // regular entry
    Status s = Insert(key, kDummyObj, &kNoopCacheItemHelper, /*charge=*/0,
                      /*handle=*/nullptr, priority);
    s.PermitUncheckedError();
    // Nothing to do or clean up on dummy insertion failure
  } else {
    // Insert regular entry into primary cache.
    // Don't allow it to spill into secondary cache again if it was kept there.
    Status s = Insert(
        key, obj, kept_in_sec_cache ? helper->without_secondary_compat : helper,
        charge, &result, priority);
    if (s.ok()) {
      assert(result);
      PERF_COUNTER_ADD(block_cache_real_handle_count, 1);
    } else {
      // Create standalone result instead, even if cache is full, to avoid
      // reading the entry from storage.
      result =
          CreateStandalone(key, obj, helper, charge, /*allow_uncharged*/ true);
      assert(result);
      PERF_COUNTER_ADD(block_cache_standalone_handle_count, 1);
    }
  }
  return result;
}

Status CacheWithSecondaryAdapter::Insert(const Slice& key, ObjectPtr value,
                                         const CacheItemHelper* helper,
                                         size_t charge, Handle** handle,
                                         Priority priority) {
  Status s = target_->Insert(key, value, helper, charge, handle, priority);
  if (s.ok() && value == nullptr && distribute_cache_res_) {
    size_t sec_charge = static_cast<size_t>(charge * (sec_cache_res_ratio_));
    s = secondary_cache_->Deflate(sec_charge);
    assert(s.ok());
    s = pri_cache_res_->UpdateCacheReservation(sec_charge, /*increase=*/false);
    assert(s.ok());
  }

  return s;
}

Cache::Handle* CacheWithSecondaryAdapter::Lookup(const Slice& key,
                                                 const CacheItemHelper* helper,
                                                 CreateContext* create_context,
                                                 Priority priority,
                                                 Statistics* stats) {
  // NOTE: we could just StartAsyncLookup() and Wait(), but this should be a bit
  // more efficient
  Handle* result =
      target_->Lookup(key, helper, create_context, priority, stats);
  bool secondary_compatible = helper && helper->IsSecondaryCacheCompatible();
  bool found_dummy_entry =
      ProcessDummyResult(&result, /*erase=*/secondary_compatible);
  if (!result && secondary_compatible) {
    // Try our secondary cache
    bool kept_in_sec_cache = false;
    std::unique_ptr<SecondaryCacheResultHandle> secondary_handle =
        secondary_cache_->Lookup(key, helper, create_context, /*wait*/ true,
                                 found_dummy_entry, /*out*/ kept_in_sec_cache);
    if (secondary_handle) {
      result = Promote(std::move(secondary_handle), key, helper, priority,
                       stats, found_dummy_entry, kept_in_sec_cache);
    }
  }
  return result;
}

bool CacheWithSecondaryAdapter::Release(Handle* handle,
                                        bool erase_if_last_ref) {
  if (erase_if_last_ref) {
    ObjectPtr v = target_->Value(handle);
    if (v == nullptr && distribute_cache_res_) {
      size_t charge = target_->GetCharge(handle);
      size_t sec_charge = static_cast<size_t>(charge * (sec_cache_res_ratio_));
      Status s = secondary_cache_->Inflate(sec_charge);
      assert(s.ok());
      s = pri_cache_res_->UpdateCacheReservation(sec_charge, /*increase=*/true);
      assert(s.ok());
    }
  }
  return target_->Release(handle, erase_if_last_ref);
}

Cache::ObjectPtr CacheWithSecondaryAdapter::Value(Handle* handle) {
  ObjectPtr v = target_->Value(handle);
  // TODO with stacked secondaries: might fail in EvictionHandler
  assert(v != kDummyObj);
  return v;
}

void CacheWithSecondaryAdapter::StartAsyncLookupOnMySecondary(
    AsyncLookupHandle& async_handle) {
  assert(!async_handle.IsPending());
  assert(async_handle.result_handle == nullptr);

  std::unique_ptr<SecondaryCacheResultHandle> secondary_handle =
      secondary_cache_->Lookup(async_handle.key, async_handle.helper,
                               async_handle.create_context, /*wait*/ false,
                               async_handle.found_dummy_entry,
                               /*out*/ async_handle.kept_in_sec_cache);
  if (secondary_handle) {
    // TODO with stacked secondaries: Check & process if already ready?
    async_handle.pending_handle = secondary_handle.release();
    async_handle.pending_cache = secondary_cache_.get();
  }
}

void CacheWithSecondaryAdapter::StartAsyncLookup(
    AsyncLookupHandle& async_handle) {
  target_->StartAsyncLookup(async_handle);
  if (!async_handle.IsPending()) {
    bool secondary_compatible =
        async_handle.helper &&
        async_handle.helper->IsSecondaryCacheCompatible();
    async_handle.found_dummy_entry |= ProcessDummyResult(
        &async_handle.result_handle, /*erase=*/secondary_compatible);

    if (async_handle.Result() == nullptr && secondary_compatible) {
      // Not found and not pending on another secondary cache
      StartAsyncLookupOnMySecondary(async_handle);
    }
  }
}

void CacheWithSecondaryAdapter::WaitAll(AsyncLookupHandle* async_handles,
                                        size_t count) {
  if (count == 0) {
    // Nothing to do
    return;
  }
  // Requests that are pending on *my* secondary cache, at the start of this
  // function
  std::vector<AsyncLookupHandle*> my_pending;
  // Requests that are pending on an "inner" secondary cache (managed somewhere
  // under target_), as of the start of this function
  std::vector<AsyncLookupHandle*> inner_pending;

  // Initial accounting of pending handles, excluding those already handled
  // by "outer" secondary caches. (See cur->pending_cache = nullptr.)
  for (size_t i = 0; i < count; ++i) {
    AsyncLookupHandle* cur = async_handles + i;
    if (cur->pending_cache) {
      assert(cur->IsPending());
      assert(cur->helper);
      assert(cur->helper->IsSecondaryCacheCompatible());
      if (cur->pending_cache == secondary_cache_.get()) {
        my_pending.push_back(cur);
        // Mark as "to be handled by this caller"
        cur->pending_cache = nullptr;
      } else {
        // Remember as potentially needing a lookup in my secondary
        inner_pending.push_back(cur);
      }
    }
  }

  // Wait on inner-most cache lookups first
  // TODO with stacked secondaries: because we are not using proper
  // async/await constructs here yet, there is a false synchronization point
  // here where all the results at one level are needed before initiating
  // any lookups at the next level. Probably not a big deal, but worth noting.
  if (!inner_pending.empty()) {
    target_->WaitAll(async_handles, count);
  }

  // For those that failed to find something, convert to lookup in my
  // secondary cache.
  for (AsyncLookupHandle* cur : inner_pending) {
    if (cur->Result() == nullptr) {
      // Not found, try my secondary
      StartAsyncLookupOnMySecondary(*cur);
      if (cur->IsPending()) {
        assert(cur->pending_cache == secondary_cache_.get());
        my_pending.push_back(cur);
        // Mark as "to be handled by this caller"
        cur->pending_cache = nullptr;
      }
    }
  }

  // Wait on all lookups on my secondary cache
  {
    std::vector<SecondaryCacheResultHandle*> my_secondary_handles;
    for (AsyncLookupHandle* cur : my_pending) {
      my_secondary_handles.push_back(cur->pending_handle);
    }
    secondary_cache_->WaitAll(std::move(my_secondary_handles));
  }

  // Process results
  for (AsyncLookupHandle* cur : my_pending) {
    std::unique_ptr<SecondaryCacheResultHandle> secondary_handle(
        cur->pending_handle);
    cur->pending_handle = nullptr;
    cur->result_handle = Promote(
        std::move(secondary_handle), cur->key, cur->helper, cur->priority,
        cur->stats, cur->found_dummy_entry, cur->kept_in_sec_cache);
    assert(cur->pending_cache == nullptr);
  }
}

std::string CacheWithSecondaryAdapter::GetPrintableOptions() const {
  std::string str = target_->GetPrintableOptions();
  str.append("  secondary_cache:\n");
  str.append(secondary_cache_->GetPrintableOptions());
  return str;
}

const char* CacheWithSecondaryAdapter::Name() const {
  // To the user, at least for now, configure the underlying cache with
  // a secondary cache. So we pretend to be that cache
  return target_->Name();
}

std::shared_ptr<Cache> NewTieredVolatileCache(
    TieredVolatileCacheOptions& opts) {
  if (!opts.cache_opts) {
    return nullptr;
  }

  std::shared_ptr<Cache> cache;
  if (opts.cache_type == PrimaryCacheType::kCacheTypeLRU) {
    LRUCacheOptions cache_opts =
        *(static_cast_with_check<LRUCacheOptions, ShardedCacheOptions>(
            opts.cache_opts));
    cache_opts.capacity += opts.comp_cache_opts.capacity;
    cache = cache_opts.MakeSharedCache();
  } else if (opts.cache_type == PrimaryCacheType::kCacheTypeHCC) {
    HyperClockCacheOptions cache_opts =
        *(static_cast_with_check<HyperClockCacheOptions, ShardedCacheOptions>(
            opts.cache_opts));
    cache = cache_opts.MakeSharedCache();
  } else {
    return nullptr;
  }
  std::shared_ptr<SecondaryCache> sec_cache;
  sec_cache = NewCompressedSecondaryCache(opts.comp_cache_opts);

  return std::make_shared<CacheWithSecondaryAdapter>(cache, sec_cache, true);
}
}  // namespace ROCKSDB_NAMESPACE
