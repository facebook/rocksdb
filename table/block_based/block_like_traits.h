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

template <typename T, CacheEntryRole R>
Cache::CacheItemHelper* GetCacheItemHelperForRole();

template <typename TBlocklike>
Cache::CreateCallback GetCreateCallback(size_t read_amp_bytes_per_bit,
                                        Statistics* statistics, bool using_zstd,
                                        const FilterPolicy* filter_policy,
                                        const TBlocklike& /*block*/) {
  return [read_amp_bytes_per_bit, statistics, using_zstd, filter_policy](
             void* buf, size_t size, void** out_obj, size_t* charge) -> Status {
    assert(buf != nullptr);
    std::unique_ptr<char[]> buf_data(new char[size]());
    memcpy(buf_data.get(), buf, size);
    BlockContents bc = BlockContents(std::move(buf_data), size);
    TBlocklike* ucd_ptr = BlocklikeTraits<TBlocklike>::Create(
        std::move(bc), read_amp_bytes_per_bit, statistics, using_zstd,
        filter_policy);
    *out_obj = reinterpret_cast<void*>(ucd_ptr);
    *charge = size;
    return Status::OK();
  };
}

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

  static size_t SizeCallback(void* obj) {
    assert(obj != nullptr);
    BlockContents* ptr = reinterpret_cast<BlockContents*>(obj);
    return ptr->data.size();
  }

  static Status SaveToCallback(void* obj, size_t offset, size_t size,
                               void* out) {
    assert(obj != nullptr);
    BlockContents* ptr = reinterpret_cast<BlockContents*>(obj);
    const char* buf = ptr->data.data();
    assert(size == ptr->data.size());
    assert(offset == 0);
    (void)offset;
    memcpy(out, buf, size);
    return Status::OK();
  }

  static Cache::CacheItemHelper* GetCacheItemHelper(BlockType block_type) {
    if (block_type == BlockType::kFilter) {
      return GetCacheItemHelperForRole<
          BlockContents, CacheEntryRole::kDeprecatedFilterBlock>();
    } else {
      // E.g. compressed cache
      return GetCacheItemHelperForRole<BlockContents,
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

  static size_t SizeCallback(void* obj) {
    assert(obj != nullptr);
    ParsedFullFilterBlock* ptr = reinterpret_cast<ParsedFullFilterBlock*>(obj);
    return ptr->GetBlockContentsData().size();
  }

  static Status SaveToCallback(void* obj, size_t offset, size_t size,
                               void* out) {
    assert(obj != nullptr);
    ParsedFullFilterBlock* ptr = reinterpret_cast<ParsedFullFilterBlock*>(obj);
    const char* buf = ptr->GetBlockContentsData().data();
    assert(size == ptr->GetBlockContentsData().size());
    assert(offset == 0);
    (void)offset;
    memcpy(out, buf, size);
    return Status::OK();
  }

  static Cache::CacheItemHelper* GetCacheItemHelper(BlockType block_type) {
    (void)block_type;
    assert(block_type == BlockType::kFilter);
    return GetCacheItemHelperForRole<ParsedFullFilterBlock,
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

  static size_t SizeCallback(void* obj) {
    assert(obj != nullptr);
    Block* ptr = reinterpret_cast<Block*>(obj);
    return ptr->size();
  }

  static Status SaveToCallback(void* obj, size_t offset, size_t size,
                               void* out) {
    assert(obj != nullptr);
    Block* ptr = reinterpret_cast<Block*>(obj);
    const char* buf = ptr->data();
    assert(size == ptr->size());
    assert(offset == 0);
    (void)offset;
    memcpy(out, buf, size);
    return Status::OK();
  }

  static Cache::CacheItemHelper* GetCacheItemHelper(BlockType block_type) {
    switch (block_type) {
      case BlockType::kData:
        return GetCacheItemHelperForRole<Block, CacheEntryRole::kDataBlock>();
      case BlockType::kIndex:
        return GetCacheItemHelperForRole<Block, CacheEntryRole::kIndexBlock>();
      case BlockType::kFilter:
        return GetCacheItemHelperForRole<Block,
                                         CacheEntryRole::kFilterMetaBlock>();
      default:
        // Not a recognized combination
        assert(false);
        FALLTHROUGH_INTENDED;
      case BlockType::kRangeDeletion:
        return GetCacheItemHelperForRole<Block, CacheEntryRole::kOtherBlock>();
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

  static size_t SizeCallback(void* obj) {
    assert(obj != nullptr);
    UncompressionDict* ptr = reinterpret_cast<UncompressionDict*>(obj);
    return ptr->slice_.size();
  }

  static Status SaveToCallback(void* obj, size_t offset, size_t size,
                               void* out) {
    assert(obj != nullptr);
    UncompressionDict* ptr = reinterpret_cast<UncompressionDict*>(obj);
    const char* buf = ptr->slice_.data();
    assert(size == ptr->slice_.size());
    assert(offset == 0);
    (void)offset;
    memcpy(out, buf, size);
    return Status::OK();
  }

  static Cache::CacheItemHelper* GetCacheItemHelper(BlockType block_type) {
    (void)block_type;
    assert(block_type == BlockType::kCompressionDictionary);
    return GetCacheItemHelperForRole<UncompressionDict,
                                     CacheEntryRole::kOtherBlock>();
  }
};

// Get an CacheItemHelper pointer for value type T and role R.
template <typename T, CacheEntryRole R>
Cache::CacheItemHelper* GetCacheItemHelperForRole() {
  static Cache::CacheItemHelper cache_helper(
      BlocklikeTraits<T>::SizeCallback, BlocklikeTraits<T>::SaveToCallback,
      GetCacheEntryDeleterForRole<T, R>());
  return &cache_helper;
}

}  // namespace ROCKSDB_NAMESPACE
