//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/cachable_entry.h"

#include <cstdio>
#include <unordered_map>
#include <utility>

#include "port/lang.h"
#include "table/block_based/block.h"
#include "table/block_based/block_type.h"
#include "table/block_based/parsed_full_filter_block.h"
#include "table/format.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

// Using a named local namespace to (hopefully) help ensure that different
// instantiations of UniqueDeleter have unique fnptr addresses.
namespace cachable_entry_details {

template <class T, BlockType kBlockType>
void UniqueDeleter(const Slice& /*key*/, void* value) {
  auto entry = reinterpret_cast<T*>(value);
  delete entry;
}

using DeleterMap =
    std::unordered_map<Cache::DeleterFn, std::pair<CacheEntryType, BlockType>>;

template <class T>
void PopulateDeleterMap(DeleterMap* map) {
  const CacheEntryType entry_type = CachableEntry<T>::GetType();

#define HANDLE_CASE(kBlockType)                                  \
  assert(map->find(UniqueDeleter<T, kBlockType>) == map->end()); \
  (*map)[UniqueDeleter<T, kBlockType>] = {entry_type, kBlockType}

  HANDLE_CASE(BlockType::kData);
  HANDLE_CASE(BlockType::kFilter);
  HANDLE_CASE(BlockType::kProperties);
  HANDLE_CASE(BlockType::kCompressionDictionary);
  HANDLE_CASE(BlockType::kRangeDeletion);
  HANDLE_CASE(BlockType::kHashIndexPrefixes);
  HANDLE_CASE(BlockType::kHashIndexMetadata);
  HANDLE_CASE(BlockType::kMetaIndex);
  HANDLE_CASE(BlockType::kIndex);
#undef HANDLE_CASE
}

DeleterMap ComputeDeleterMap() {
  DeleterMap rv;
  PopulateDeleterMap<BlockContents>(&rv);
  PopulateDeleterMap<ParsedFullFilterBlock>(&rv);
  PopulateDeleterMap<Block>(&rv);
  PopulateDeleterMap<UncompressionDict>(&rv);
  return rv;
}

}  // namespace cachable_entry_details

bool ParseTypesFromDeleter(Cache::DeleterFn deleter, CacheEntryType* entry_type,
                           BlockType* block_type) {
  static auto map = cachable_entry_details::ComputeDeleterMap();
  auto it = map.find(deleter);
  if (it == map.end()) {
    return false;
  }
  *entry_type = it->second.first;
  *block_type = it->second.second;
  return true;
}

// Shared implementations
template <class T>
Cache::DeleterFn CachableEntry<T>::GetDeleterForBlockType(
    BlockType block_type) {
#define HANDLE_CASE(kBlockType) \
  case kBlockType:              \
    return cachable_entry_details::UniqueDeleter<T, kBlockType>

  switch (block_type) {
    default:
      FALLTHROUGH_INTENDED;
    case BlockType::kInvalid:
      assert(false);
      FALLTHROUGH_INTENDED;
      HANDLE_CASE(BlockType::kData);
      HANDLE_CASE(BlockType::kFilter);
      HANDLE_CASE(BlockType::kProperties);
      HANDLE_CASE(BlockType::kCompressionDictionary);
      HANDLE_CASE(BlockType::kRangeDeletion);
      HANDLE_CASE(BlockType::kHashIndexPrefixes);
      HANDLE_CASE(BlockType::kHashIndexMetadata);
      HANDLE_CASE(BlockType::kMetaIndex);
      HANDLE_CASE(BlockType::kIndex);
  }
#undef HANDLE_CASE
}

template <class T>
uint32_t CachableEntry<T>::GetNumRestarts() {
  return 0;
}

template <class T>
Status CachableEntry<T>::CastValueMaybeChecked(Cache::Handle* from_handle,
                                               Cache* from_cache,
                                               BlockType expected_block_type,
                                               T** result, bool check) {
  void* value = from_cache->Value(from_handle);
  if (check) {
    *result = nullptr;  // for error cases
    auto deleter = from_cache->GetDeleter(from_handle);
    CacheEntryType entry_type{};
    BlockType block_type{};
    bool success = ParseTypesFromDeleter(deleter, &entry_type, &block_type);
    if (!success) {
      return Status::Corruption(
          "Unrecognized deleter associated with cache key");
    } else if (block_type != expected_block_type) {
      return Status::Corruption("Expected block type " +
                                BlockTypeToString(expected_block_type) +
                                " but found " + BlockTypeToString(block_type));
    } else if (entry_type != GetType()) {
      return Status::Corruption(
          "Expected cache entry type " + CacheEntryTypeToString(GetType()) +
          " but found " + CacheEntryTypeToString(entry_type));
    }
  }
  *result = reinterpret_cast<T*>(value);
  return Status::OK();
}

// BlockContents
template <>
BlockContents* CachableEntry<BlockContents>::CreateValue(
    BlockContents&& contents, size_t /* read_amp_bytes_per_bit */,
    Statistics* /* statistics */, bool /* using_zstd */,
    const FilterPolicy* /* filter_policy */) {
  return new BlockContents(std::move(contents));
}

template <>
CacheEntryType CachableEntry<BlockContents>::GetType() {
  return CacheEntryType::kUncompressedRaw;
}

template class CachableEntry<BlockContents>;

// ParsedFullFilterBlock
template <>
ParsedFullFilterBlock* CachableEntry<ParsedFullFilterBlock>::CreateValue(
    BlockContents&& contents, size_t /* read_amp_bytes_per_bit */,
    Statistics* /* statistics */, bool /* using_zstd */,
    const FilterPolicy* filter_policy) {
  return new ParsedFullFilterBlock(filter_policy, std::move(contents));
}

template <>
CacheEntryType CachableEntry<ParsedFullFilterBlock>::GetType() {
  return CacheEntryType::kParsedFullFilterBlock;
}

template class CachableEntry<ParsedFullFilterBlock>;

// Block
template <>
uint32_t CachableEntry<Block>::GetNumRestarts() {
  assert(GetValue());
  return GetValue()->NumRestarts();
}

template <>
Block* CachableEntry<Block>::CreateValue(
    BlockContents&& contents, size_t read_amp_bytes_per_bit,
    Statistics* statistics, bool /* using_zstd */,
    const FilterPolicy* /* filter_policy */) {
  return new Block(std::move(contents), read_amp_bytes_per_bit, statistics);
}

template <>
CacheEntryType CachableEntry<Block>::GetType() {
  return CacheEntryType::kParsedKvBlock;
}

template class CachableEntry<Block>;

// UncompressionDict
template <>
UncompressionDict* CachableEntry<UncompressionDict>::CreateValue(
    BlockContents&& contents, size_t /* read_amp_bytes_per_bit */,
    Statistics* /* statistics */, bool using_zstd,
    const FilterPolicy* /* filter_policy */) {
  return new UncompressionDict(contents.data, std::move(contents.allocation),
                               using_zstd);
}

template <>
CacheEntryType CachableEntry<UncompressionDict>::GetType() {
  return CacheEntryType::kUncompressionDict;
}

template class CachableEntry<UncompressionDict>;

}  // namespace ROCKSDB_NAMESPACE
