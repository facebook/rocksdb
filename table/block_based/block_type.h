//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

namespace rocksdb {

// Represents the types of blocks used in the block based table format.
// See https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format
// for details.

// Note: index/filter partitions act like the main index/filter block in some
// contexts and as pure data blocks in others. Hence, it makes sense to treat
// them as separate types that are distinct from the above.
enum class BlockType : uint8_t {
  kData,
  kFilter,
  kFilterPartition,
  kProperties,
  kCompressionDictionary,
  kRangeDeletion,
  kHashIndexPrefixes,
  kHashIndexMetadata,
  kMetaIndex,
  kIndex,
  kIndexPartition,
  // Note: keep kInvalid the last value when adding new enum values.
  kInvalid
};

}  // namespace rocksdb
