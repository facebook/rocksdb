// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

enum class OptionType {
  kBoolean,
  kInt,
  kInt32T,
  kInt64T,
  kVectorInt,
  kUInt,
  kUInt32T,
  kUInt64T,
  kSizeT,
  kString,
  kDouble,
  kCompactionStyle,
  kCompactionPri,
  kSliceTransform,
  kCompressionType,
  kVectorCompressionType,
  kTableFactory,
  kComparator,
  kCompactionFilter,
  kCompactionFilterFactory,
  kCompactionOptionsFIFO,
  kCompactionOptionsUniversal,
  kCompactionStopStyle,
  kMergeOperator,
  kMemTableRepFactory,
  kBlockBasedTableIndexType,
  kBlockBasedTableDataBlockIndexType,
  kBlockBasedTableIndexShorteningMode,
  kFilterPolicy,
  kFlushBlockPolicyFactory,
  kChecksumType,
  kEncodingType,
  kWALRecoveryMode,
  kAccessHint,
  kInfoLogLevel,
  kLRUCacheOptions,
  kEnv,
  kUnknown,
};

enum class OptionVerificationType {
  kNormal,
  kByName,               // The option is pointer typed so we can only verify
                         // based on it's name.
  kByNameAllowNull,      // Same as kByName, but it also allows the case
                         // where one of them is a nullptr.
  kByNameAllowFromNull,  // Same as kByName, but it also allows the case
                         // where the old option is nullptr.
  kDeprecated            // The option is no longer used in rocksdb. The RocksDB
                         // OptionsParser will still accept this option if it
                         // happen to exists in some Options file.  However,
                         // the parser will not include it in serialization
                         // and verification processes.
};

enum class OptionTypeFlags : uint32_t {
  kNone = 0x00,     // No flags
  kMutable = 0x01,  // Option is mutable
};

inline OptionTypeFlags operator|(const OptionTypeFlags &a,
                                 const OptionTypeFlags &b) {
  return static_cast<OptionTypeFlags>(static_cast<uint32_t>(a) |
                                      static_cast<uint32_t>(b));
}

inline OptionTypeFlags operator&(const OptionTypeFlags &a,
                                 const OptionTypeFlags &b) {
  return static_cast<OptionTypeFlags>(static_cast<uint32_t>(a) &
                                      static_cast<uint32_t>(b));
}

// A struct for storing constant option information such as option name,
// option type, and offset.
class OptionTypeInfo {
 public:
  int offset;
  int mutable_offset;
  OptionType type;

  // A simple "normal", non-mutable Type "_type" at _offset
  OptionTypeInfo(int _offset, OptionType _type)
      : offset(_offset),
        mutable_offset(0),
        type(_type),
        verification(OptionVerificationType::kNormal),
        flags(OptionTypeFlags::kNone) {}

  // A simple "normal", mutable Type "_type" at _offset
  OptionTypeInfo(int _offset, OptionType _type, int _mutable_offset)
      : offset(_offset),
        mutable_offset(_mutable_offset),
        type(_type),
        verification(OptionVerificationType::kNormal),
        flags(OptionTypeFlags::kMutable) {}

  OptionTypeInfo(int _offset, OptionType _type,
                 OptionVerificationType _verification, OptionTypeFlags _flags,
                 int _mutable_offset)
      : offset(_offset),
        mutable_offset(_mutable_offset),
        type(_type),
        verification(_verification),
        flags(_flags) {}

  bool IsEnabled(OptionTypeFlags otf) const { return (flags & otf) == otf; }

  bool IsMutable() const { return IsEnabled(OptionTypeFlags::kMutable); }

  bool IsDeprecated() const {
    return IsEnabled(OptionVerificationType::kDeprecated);
  }

  bool IsEnabled(OptionVerificationType ovf) const {
    return verification == ovf;
  }

  bool IsByName() const {
    return (verification == OptionVerificationType::kByName ||
            verification == OptionVerificationType::kByNameAllowNull ||
            verification == OptionVerificationType::kByNameAllowFromNull);
  }

 protected:
 private:
  OptionVerificationType verification;
  OptionTypeFlags flags;
};
}  // namespace ROCKSDB_NAMESPACE
