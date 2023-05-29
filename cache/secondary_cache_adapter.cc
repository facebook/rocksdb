//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/secondary_cache_adapter.h"

#include "monitoring/perf_context_imp.h"

namespace ROCKSDB_NAMESPACE {

namespace {
// A distinct pointer value for marking "dummy" cache entries
struct Dummy {
  char val[7] = "kDummy";
};
const Dummy kDummy{};
Cache::ObjectPtr const kDummyObj = const_cast<Dummy*>(&kDummy);
}  // namespace

CacheWithSecondaryAdapter::CacheWithSecondaryAdapter(
    std::shared_ptr<Cache> target,
    std::shared_ptr<SecondaryCache> secondary_cache)
    : CacheWrapper(std::move(target)),
      secondary_cache_(std::move(secondary_cache)) {
  target_->SetEvictionCallback([this](const Slice& key, Handle* handle) {
    return EvictionHandler(key, handle);
  });
}

CacheWithSecondaryAdapter::~CacheWithSecondaryAdapter() {
  // `*this` will be destroyed before `*target_`, so we have to prevent
  // use after free
  target_->SetEvictionCallback({});
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
    secondary_cache_->WaitAll(my_secondary_handles);
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
}  // namespace ROCKSDB_NAMESPACE
