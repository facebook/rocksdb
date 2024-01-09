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
  BlockCreateContext() {}
  BlockCreateContext(const BlockBasedTableOptions* _table_options,
                     const ImmutableOptions* _ioptions, Statistics* _statistics,
                     bool _using_zstd, uint8_t _protection_bytes_per_key,
                     const Comparator* _raw_ucmp,
                     bool _index_value_is_full = false,
                     bool _index_has_first_key = false)
      : table_options(_table_options),
        ioptions(_ioptions),
        statistics(_statistics),
        raw_ucmp(_raw_ucmp),
        using_zstd(_using_zstd),
        protection_bytes_per_key(_protection_bytes_per_key),
        index_value_is_full(_index_value_is_full),
        index_has_first_key(_index_has_first_key) {}

  const BlockBasedTableOptions* table_options = nullptr;
  const ImmutableOptions* ioptions = nullptr;
  Statistics* statistics = nullptr;
  const Comparator* raw_ucmp = nullptr;
  const UncompressionDict* dict = nullptr;
  uint32_t format_version;
  bool using_zstd = false;
  uint8_t protection_bytes_per_key = 0;
  bool index_value_is_full;
  bool index_has_first_key;

  // For TypedCacheInterface
  template <typename TBlocklike>
  inline void Create(std::unique_ptr<TBlocklike>* parsed_out,
                     size_t* charge_out, const Slice& data,
                     CompressionType type, MemoryAllocator* alloc) {
    BlockContents uncompressed_block_contents;
    if (type != CompressionType::kNoCompression) {
      assert(dict != nullptr);
      UncompressionContext context(type);
      UncompressionInfo info(context, *dict, type);
      Status s = UncompressBlockData(
          info, data.data(), data.size(), &uncompressed_block_contents,
          table_options->format_version, *ioptions, alloc);
      if (!s.ok()) {
        parsed_out->reset();
        return;
      }
    } else {
      uncompressed_block_contents =
          BlockContents(AllocateAndCopyBlock(data, alloc), data.size());
    }
    Create(parsed_out, std::move(uncompressed_block_contents));
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

// Convenient cache interface to use for block_cache, with support for
// SecondaryCache.
template <typename TBlocklike>
using BlockCacheInterface =
    FullTypedCacheInterface<TBlocklike, BlockCreateContext>;

// Shortcut name for cache handles under BlockCacheInterface
template <typename TBlocklike>
using BlockCacheTypedHandle =
    typename BlockCacheInterface<TBlocklike>::TypedHandle;

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
