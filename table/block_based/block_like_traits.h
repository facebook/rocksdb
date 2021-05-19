//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "cache/cache_entry_roles.h"
#include "port/lang.h"
#include "table/block_based/block.h"
#include "table/block_based/block_type.h"
#include "table/block_based/parsed_full_filter_block.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

template <typename TBlocklike>
class BlocklikeTraits;

template <>
class BlocklikeTraits<BlockContents> {
 public:
  static BlockContents* Create(BlockContents&& contents,
                               size_t /* read_amp_bytes_per_bit */,
                               Statistics* /* statistics */,
                               bool /* using_zstd */,
                               const FilterPolicy* /* filter_policy */) {
    return new BlockContents(std::move(contents));
  }

  static uint32_t GetNumRestarts(const BlockContents& /* contents */) {
    return 0;
  }

  static Cache::DeleterFn GetDeleter(BlockType block_type) {
    if (block_type == BlockType::kFilter) {
      return GetCacheEntryDeleterForRole<
          BlockContents, CacheEntryRole::kDeprecatedFilterBlock>();
    } else {
      // E.g. compressed cache
      return GetCacheEntryDeleterForRole<BlockContents,
                                         CacheEntryRole::kOtherBlock>();
    }
  }
};

template <>
class BlocklikeTraits<ParsedFullFilterBlock> {
 public:
  static ParsedFullFilterBlock* Create(BlockContents&& contents,
                                       size_t /* read_amp_bytes_per_bit */,
                                       Statistics* /* statistics */,
                                       bool /* using_zstd */,
                                       const FilterPolicy* filter_policy) {
    return new ParsedFullFilterBlock(filter_policy, std::move(contents));
  }

  static uint32_t GetNumRestarts(const ParsedFullFilterBlock& /* block */) {
    return 0;
  }

  static Cache::DeleterFn GetDeleter(BlockType block_type) {
    (void)block_type;
    assert(block_type == BlockType::kFilter);
    return GetCacheEntryDeleterForRole<ParsedFullFilterBlock,
                                       CacheEntryRole::kFilterBlock>();
  }
};

template <>
class BlocklikeTraits<Block> {
 public:
  static Block* Create(BlockContents&& contents, size_t read_amp_bytes_per_bit,
                       Statistics* statistics, bool /* using_zstd */,
                       const FilterPolicy* /* filter_policy */) {
    return new Block(std::move(contents), read_amp_bytes_per_bit, statistics);
  }

  static uint32_t GetNumRestarts(const Block& block) {
    return block.NumRestarts();
  }

  static Cache::DeleterFn GetDeleter(BlockType block_type) {
    switch (block_type) {
      case BlockType::kData:
        return GetCacheEntryDeleterForRole<Block, CacheEntryRole::kDataBlock>();
      case BlockType::kIndex:
        return GetCacheEntryDeleterForRole<Block,
                                           CacheEntryRole::kIndexBlock>();
      case BlockType::kFilter:
        return GetCacheEntryDeleterForRole<Block,
                                           CacheEntryRole::kFilterMetaBlock>();
      default:
        // Not a recognized combination
        assert(false);
        FALLTHROUGH_INTENDED;
      case BlockType::kRangeDeletion:
        return GetCacheEntryDeleterForRole<Block,
                                           CacheEntryRole::kOtherBlock>();
    }
  }
};

template <>
class BlocklikeTraits<UncompressionDict> {
 public:
  static UncompressionDict* Create(BlockContents&& contents,
                                   size_t /* read_amp_bytes_per_bit */,
                                   Statistics* /* statistics */,
                                   bool using_zstd,
                                   const FilterPolicy* /* filter_policy */) {
    return new UncompressionDict(contents.data, std::move(contents.allocation),
                                 using_zstd);
  }

  static uint32_t GetNumRestarts(const UncompressionDict& /* dict */) {
    return 0;
  }

  static Cache::DeleterFn GetDeleter(BlockType block_type) {
    (void)block_type;
    assert(block_type == BlockType::kCompressionDictionary);
    return GetCacheEntryDeleterForRole<UncompressionDict,
                                       CacheEntryRole::kOtherBlock>();
  }
};

}  // namespace ROCKSDB_NAMESPACE
