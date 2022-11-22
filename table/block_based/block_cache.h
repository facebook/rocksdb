//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Code supporting block cache (Cache) access for block-based table, based on
// the convenient APIs in typed_cache.h

#pragma once

#include <type_traits>

#include "cache/typed_cache.h"
#include "port/lang.h"
#include "table/block_based/block.h"
#include "table/block_based/block_type.h"
#include "table/block_based/parsed_full_filter_block.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

// Metaprogramming wrappers for Block, to give each type a single role when
// used with FullTypedCacheInterface.
// (NOTE: previous attempts to create actual derived classes of Block with
// virtual calls resulted in performance regression)

class Block_kData : public Block {
 public:
  using Block::Block;

  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kDataBlock;
  static constexpr BlockType kBlockType = BlockType::kData;
};

class Block_kIndex : public Block {
 public:
  using Block::Block;

  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kIndexBlock;
  static constexpr BlockType kBlockType = BlockType::kIndex;
};

class Block_kFilterPartitionIndex : public Block {
 public:
  using Block::Block;

  static constexpr CacheEntryRole kCacheEntryRole =
      CacheEntryRole::kFilterMetaBlock;
  static constexpr BlockType kBlockType = BlockType::kFilterPartitionIndex;
};

class Block_kRangeDeletion : public Block {
 public:
  using Block::Block;

  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kOtherBlock;
  static constexpr BlockType kBlockType = BlockType::kRangeDeletion;
};

// Useful for creating the Block even though meta index blocks are not
// yet stored in block cache
class Block_kMetaIndex : public Block {
 public:
  using Block::Block;

  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kOtherBlock;
  static constexpr BlockType kBlockType = BlockType::kMetaIndex;
};

struct BlockCreateContext : public Cache::CreateContext {
  BlockCreateContext(){};
  BlockCreateContext(const BlockBasedTableOptions* _table_options,
                     Statistics* _statistics, bool _using_zstd)
      : table_options(_table_options),
        statistics(_statistics),
        using_zstd(_using_zstd) {}

  const BlockBasedTableOptions* table_options = nullptr;
  Statistics* statistics = nullptr;
  bool using_zstd = false;

  // For TypedCacheInterface
  template <typename TBlocklike>
  inline void Create(std::unique_ptr<TBlocklike>* parsed_out,
                     size_t* charge_out, const Slice& data,
                     MemoryAllocator* alloc) {
    Create(parsed_out,
           BlockContents(AllocateAndCopyBlock(data, alloc), data.size()));
    *charge_out = parsed_out->get()->ApproximateMemoryUsage();
  }

  void Create(std::unique_ptr<Block_kData>* parsed_out, BlockContents&& block);
  void Create(std::unique_ptr<Block_kIndex>* parsed_out, BlockContents&& block);
  void Create(std::unique_ptr<Block_kFilterPartitionIndex>* parsed_out,
              BlockContents&& block);
  void Create(std::unique_ptr<Block_kRangeDeletion>* parsed_out,
              BlockContents&& block);
  void Create(std::unique_ptr<Block_kMetaIndex>* parsed_out,
              BlockContents&& block);
  void Create(std::unique_ptr<ParsedFullFilterBlock>* parsed_out,
              BlockContents&& block);
  void Create(std::unique_ptr<UncompressionDict>* parsed_out,
              BlockContents&& block);
};

// Convenient cache interface to use with block_cache_compressed
using CompressedBlockCacheInterface =
    BasicTypedCacheInterface<BlockContents, CacheEntryRole::kOtherBlock>;

// Convenient cache interface to use for block_cache, with support for
// SecondaryCache.
template <typename TBlocklike>
using BlockCacheInterface =
    FullTypedCacheInterface<TBlocklike, BlockCreateContext>;

// Shortcut name for cache handles under BlockCacheInterface
template <typename TBlocklike>
using BlockCacheTypedHandle =
    typename BlockCacheInterface<TBlocklike>::TypedHandle;

// For getting SecondaryCache-compatible helpers from a BlockType. This is
// useful for accessing block cache in untyped contexts, such as for generic
// cache warming in table builder.
static constexpr std::array<const Cache::CacheItemHelper*,
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
static constexpr std::array<const Cache::CacheItemHelper*,
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

// Selects the right helper based on BlockType and CacheTier
const Cache::CacheItemHelper* GetCacheItemHelper(
    BlockType block_type,
    CacheTier lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier);

// For SFINAE check that a type is "blocklike" with a kCacheEntryRole member.
// Can get difficult compiler/linker errors without a good check like this.
template <typename TUse, typename TBlocklike>
using WithBlocklikeCheck = std::enable_if_t<
    TBlocklike::kCacheEntryRole == CacheEntryRole::kMisc || true, TUse>;

}  // namespace ROCKSDB_NAMESPACE
