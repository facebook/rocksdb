// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <functional>
#include <memory>

#include "rocksdb/status.h"

namespace rocksdb {
struct DBOptions;
enum OptionsSanityCheckLevel : unsigned char {
  // Performs no sanity check at all.
  kSanityLevelNone = 0x00,
  // Performs minimum check to ensure the RocksDB instance can be
  // opened without corrupting / mis-interpreting the data.
  kSanityLevelLooselyCompatible = 0x01,
  // Perform exact match sanity check.
  kSanityLevelExactMatch = 0xFF,
};

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
  kCompressionOpts,
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
  kEnum,
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
  kDeprecated,           // The option is no longer used in rocksdb. The RocksDB
                         // OptionsParser will still accept this option if it
                         // happen to exists in some Options file.  However,
                         // the parser will not include it in serialization
                         // and verification processes.
  kAlias                 // This option represents is a name/shortcut for
                         // another option and should not be written or verified
                         // independently
};

enum OptionTypeFlags {
  kNone = 0x00,     // No flags
  kMutable = 0x01,  // Option is mutable
  kPointer = 0x02,  // The option is stored as a pointer
  kShared = 0x04,   // The option is stored as a shared_ptr
  kUnique = 0x08,   // The option is stored as a unique_ptr
  kEnum = 0x10,     // The option represents an enumerated type
  kMutableEnum = (kEnum | kMutable),  // Mutable enumerated type
};

// A struct for storing constant option information such as option name,
// option type, and offset.
struct OptionTypeInfo {
  int offset;
  OptionType type;
  OptionVerificationType verification;
  OptionTypeFlags flags;  // This is a bitmask of OptionTypeFlag values
  int mutable_offset;

  bool IsMutable() const {
    return (flags & OptionTypeFlags::kMutable) == OptionTypeFlags::kMutable;
  }
  bool IsSharedPtr() const {
    return (flags & OptionTypeFlags::kShared) == OptionTypeFlags::kShared;
  }
  bool IsUniquePtr() const {
    return (flags & OptionTypeFlags::kUnique) == OptionTypeFlags::kUnique;
  }
  bool IsRawPtr() const {
    return (flags & OptionTypeFlags::kPointer) == OptionTypeFlags::kPointer;
  }

  bool IsEnum() const {
    return type == OptionType::kEnum ||
           ((flags & OptionTypeFlags::kEnum) == OptionTypeFlags::kEnum);
  }

  template <typename T>
  const T *GetPointer(const char *addr) const {
    if (addr == nullptr) {
      return nullptr;
    } else if (IsUniquePtr()) {
      const std::unique_ptr<T> *ptr =
          reinterpret_cast<const std::unique_ptr<T> *>(addr);
      return ptr->get();
    } else if (IsSharedPtr()) {
      const std::shared_ptr<T> *ptr =
          reinterpret_cast<const std::shared_ptr<T> *>(addr);
      return ptr->get();
    } else if (IsRawPtr()) {
      const T *const *ptr = reinterpret_cast<const T *const *>(addr);
      return *ptr;
    } else {
      return reinterpret_cast<const T *>(addr);
    }
  }
  template <typename T>
  T *GetPointer(char *addr) const {
    if (addr == nullptr) {
      return nullptr;
    } else if (IsUniquePtr()) {
      std::unique_ptr<T> *ptr = reinterpret_cast<std::unique_ptr<T> *>(addr);
      return ptr->get();
    } else if (IsSharedPtr()) {
      std::shared_ptr<T> *ptr = reinterpret_cast<std::shared_ptr<T> *>(addr);
      return ptr->get();
    } else if (IsRawPtr()) {
      T **ptr = reinterpret_cast<T **>(addr);
      return *ptr;
    } else {
      return reinterpret_cast<T *>(addr);
    }
  }
};

}  // namespace rocksdb
