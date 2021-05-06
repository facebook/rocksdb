//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstdint>
#include <string>

#include "port/lang.h"

namespace ROCKSDB_NAMESPACE {

// Represents the types of blocks used in the block based table format.
// See https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format
// for details.

enum class BlockType : uint8_t {
  kData,
  kFilter,
  kProperties,
  kCompressionDictionary,
  kRangeDeletion,
  kHashIndexPrefixes,
  kHashIndexMetadata,
  kMetaIndex,
  kIndex,
  // Note: keep kInvalid the last value when adding new enum values.
  kInvalid
};

inline std::string BlockTypeToString(BlockType type) {
  switch (type) {
    case BlockType::kData:
      return "Data";
    case BlockType::kFilter:
      return "Filter";
    case BlockType::kProperties:
      return "Properties";
    case BlockType::kCompressionDictionary:
      return "CompressionDictionary";
    case BlockType::kRangeDeletion:
      return "RangeDeletion";
    case BlockType::kHashIndexPrefixes:
      return "HashIndexPrefixes";
    case BlockType::kHashIndexMetadata:
      return "HashIndexMetadata";
    case BlockType::kMetaIndex:
      return "MetaIndex";
    case BlockType::kIndex:
      return "Index";
    default:
      assert(false);
      FALLTHROUGH_INTENDED;
    case BlockType::kInvalid:
      return "Invalid";
  }
}

}  // namespace ROCKSDB_NAMESPACE
