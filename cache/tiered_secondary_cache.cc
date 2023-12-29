//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/tiered_secondary_cache.h"

#include "monitoring/statistics_impl.h"

namespace ROCKSDB_NAMESPACE {

// Creation callback for use in the lookup path. It calls the upper layer
// create_cb to create the object, and optionally calls the compressed
// secondary cache InsertSaved to save the compressed block. If
// advise_erase is set, it means the primary cache wants the block to be
// erased in the secondary cache, so we skip calling InsertSaved.
//
// For the time being, we assume that all blocks in the nvm tier belong to
// the primary block cache (i.e CacheTier::kVolatileTier). That can be changed
// if we implement demotion from the compressed secondary cache to the nvm
// cache in the future.
Status TieredSecondaryCache::MaybeInsertAndCreate(
    const Slice& data, CompressionType type, CacheTier source,
    Cache::CreateContext* ctx, MemoryAllocator* allocator,
    Cache::ObjectPtr* out_obj, size_t* out_charge) {
  TieredSecondaryCache::CreateContext* context =
      static_cast<TieredSecondaryCache::CreateContext*>(ctx);
  assert(source == CacheTier::kVolatileTier);
  if (!context->advise_erase && type != kNoCompression) {
    // Attempt to insert into compressed secondary cache
    // TODO: Don't hardcode the source
    context->comp_sec_cache->InsertSaved(*context->key, data, type, source)
        .PermitUncheckedError();
    RecordTick(context->stats, COMPRESSED_SECONDARY_CACHE_PROMOTIONS);
  } else {
    RecordTick(context->stats, COMPRESSED_SECONDARY_CACHE_PROMOTION_SKIPS);
  }
  // Primary cache will accept the object, so call its helper to create
  // the object
  return context->helper->create_cb(data, type, source, context->inner_ctx,
                                    allocator, out_obj, out_charge);
}

// The lookup first looks up in the compressed secondary cache. If its a miss,
// then the nvm cache lookup is called. The cache item helper and create
// context are wrapped in order to intercept the creation callback to make
// the decision on promoting to the compressed secondary cache.
std::unique_ptr<SecondaryCacheResultHandle> TieredSecondaryCache::Lookup(
    const Slice& key, const Cache::CacheItemHelper* helper,
    Cache::CreateContext* create_context, bool wait, bool advise_erase,
    Statistics* stats, bool& kept_in_sec_cache) {
  bool dummy = false;
  std::unique_ptr<SecondaryCacheResultHandle> result =
      target()->Lookup(key, helper, create_context, wait, advise_erase, stats,
                       /*kept_in_sec_cache=*/dummy);
  // We never want the item to spill back into the secondary cache
  kept_in_sec_cache = true;
  if (result) {
    assert(result->IsReady());
    return result;
  }

  // If wait is true, then we can be a bit more efficient and avoid a memory
  // allocation for the CReateContext.
  const Cache::CacheItemHelper* outer_helper =
      TieredSecondaryCache::GetHelper();
  if (wait) {
    TieredSecondaryCache::CreateContext ctx;
    ctx.key = &key;
    ctx.advise_erase = advise_erase;
    ctx.helper = helper;
    ctx.inner_ctx = create_context;
    ctx.comp_sec_cache = target();
    ctx.stats = stats;

    return nvm_sec_cache_->Lookup(key, outer_helper, &ctx, wait, advise_erase,
                                  stats, kept_in_sec_cache);
  }

  // If wait is false, i.e its an async lookup, we have to allocate a result
  // handle for tracking purposes. Embed the CreateContext inside the handle
  // so we need only allocate memory once instead of twice.
  std::unique_ptr<ResultHandle> handle(new ResultHandle());
  handle->ctx()->key = &key;
  handle->ctx()->advise_erase = advise_erase;
  handle->ctx()->helper = helper;
  handle->ctx()->inner_ctx = create_context;
  handle->ctx()->comp_sec_cache = target();
  handle->ctx()->stats = stats;
  handle->SetInnerHandle(
      nvm_sec_cache_->Lookup(key, outer_helper, handle->ctx(), wait,
                             advise_erase, stats, kept_in_sec_cache));
  if (!handle->inner_handle()) {
    handle.reset();
  } else {
    result.reset(handle.release());
  }

  return result;
}

// Call the nvm cache WaitAll to complete the lookups
void TieredSecondaryCache::WaitAll(
    std::vector<SecondaryCacheResultHandle*> handles) {
  std::vector<SecondaryCacheResultHandle*> nvm_handles;
  std::vector<ResultHandle*> my_handles;
  nvm_handles.reserve(handles.size());
  for (auto handle : handles) {
    // The handle could belong to the compressed secondary cache. Skip if
    // that's the case.
    if (handle->IsReady()) {
      continue;
    }
    ResultHandle* hdl = static_cast<ResultHandle*>(handle);
    nvm_handles.push_back(hdl->inner_handle());
    my_handles.push_back(hdl);
  }
  nvm_sec_cache_->WaitAll(nvm_handles);
  for (auto handle : my_handles) {
    assert(handle->inner_handle()->IsReady());
    handle->Complete();
  }
}

}  // namespace ROCKSDB_NAMESPACE
