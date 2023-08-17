//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/block_cache.h"

namespace ROCKSDB_NAMESPACE {

void BlockCreateContext::Create(std::unique_ptr<Block_kData>* parsed_out,
                                BlockContents&& block) {
  parsed_out->reset(new Block_kData(
      std::move(block), table_options->read_amp_bytes_per_bit, statistics));
}
void BlockCreateContext::Create(std::unique_ptr<Block_kIndex>* parsed_out,
                                BlockContents&& block) {
  parsed_out->reset(new Block_kIndex(std::move(block),
                                     /*read_amp_bytes_per_bit*/ 0, statistics));
}
void BlockCreateContext::Create(
    std::unique_ptr<Block_kFilterPartitionIndex>* parsed_out,
    BlockContents&& block) {
  parsed_out->reset(new Block_kFilterPartitionIndex(
      std::move(block), /*read_amp_bytes_per_bit*/ 0, statistics));
}
void BlockCreateContext::Create(
    std::unique_ptr<Block_kRangeDeletion>* parsed_out, BlockContents&& block) {
  parsed_out->reset(new Block_kRangeDeletion(
      std::move(block), /*read_amp_bytes_per_bit*/ 0, statistics));
}
void BlockCreateContext::Create(std::unique_ptr<Block_kMetaIndex>* parsed_out,
                                BlockContents&& block) {
  parsed_out->reset(new Block_kMetaIndex(
      std::move(block), /*read_amp_bytes_per_bit*/ 0, statistics));
}

void BlockCreateContext::Create(
    std::unique_ptr<ParsedFullFilterBlock>* parsed_out, BlockContents&& block) {
  parsed_out->reset(new ParsedFullFilterBlock(
      table_options->filter_policy.get(), std::move(block)));
}

void BlockCreateContext::Create(std::unique_ptr<UncompressionDict>* parsed_out,
                                BlockContents&& block) {
  parsed_out->reset(new UncompressionDict(
      block.data, std::move(block.allocation), using_zstd));
}

namespace {
// For getting SecondaryCache-compatible helpers from a BlockType. This is
// useful for accessing block cache in untyped contexts, such as for generic
// cache warming in table builder.
constexpr std::array<const Cache::CacheItemHelper*,
                     static_cast<unsigned>(BlockType::kInvalid) + 1>
    kCacheItemFullHelperForBlockType{{
        &BlockCacheInterface<Block_kData>::kFullHelper,
        &BlockCacheInterface<ParsedFullFilterBlock>::kFullHelper,
        &BlockCacheInterface<Block_kFilterPartitionIndex>::kFullHelper,
        nullptr,  // kProperties
        &BlockCacheInterface<UncompressionDict>::kFullHelper,
        &BlockCacheInterface<Block_kRangeDeletion>::kFullHelper,
        nullptr,  // kHashIndexPrefixes
        nullptr,  // kHashIndexMetadata
        nullptr,  // kMetaIndex (not yet stored in block cache)
        &BlockCacheInterface<Block_kIndex>::kFullHelper,
        nullptr,  // kInvalid
    }};

// For getting basic helpers from a BlockType (no SecondaryCache support)
constexpr std::array<const Cache::CacheItemHelper*,
                     static_cast<unsigned>(BlockType::kInvalid) + 1>
    kCacheItemBasicHelperForBlockType{{
        &BlockCacheInterface<Block_kData>::kBasicHelper,
        &BlockCacheInterface<ParsedFullFilterBlock>::kBasicHelper,
        &BlockCacheInterface<Block_kFilterPartitionIndex>::kBasicHelper,
        nullptr,  // kProperties
        &BlockCacheInterface<UncompressionDict>::kBasicHelper,
        &BlockCacheInterface<Block_kRangeDeletion>::kBasicHelper,
        nullptr,  // kHashIndexPrefixes
        nullptr,  // kHashIndexMetadata
        nullptr,  // kMetaIndex (not yet stored in block cache)
        &BlockCacheInterface<Block_kIndex>::kBasicHelper,
        nullptr,  // kInvalid
    }};
}  // namespace

const Cache::CacheItemHelper* GetCacheItemHelper(
    BlockType block_type, CacheTier lowest_used_cache_tier) {
  if (lowest_used_cache_tier == CacheTier::kNonVolatileBlockTier) {
    return kCacheItemFullHelperForBlockType[static_cast<unsigned>(block_type)];
  } else {
    return kCacheItemBasicHelperForBlockType[static_cast<unsigned>(block_type)];
  }
}

}  // namespace ROCKSDB_NAMESPACE
