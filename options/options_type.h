// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <functional>
#include <memory>
#include <unordered_map>

#include "rocksdb/convenience.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

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
  kDeprecated,           // The option is no longer used in rocksdb. The RocksDB
                         // OptionsParser will still accept this option if it
                         // happen to exists in some Options file.  However,
                         // the parser will not include it in serialization
                         // and verification processes.
  kAlias,                // This option represents is a name/shortcut for
                         // another option and should not be written or verified
                         // independently
};

enum class OptionTypeFlags : uint32_t {
  kNone = 0x00,  // No flags
  kCompareDefault = 0x0,
  kCompareNever = ConfigOptions::kSanityLevelNone,
  kCompareLoose = ConfigOptions::kSanityLevelLooselyCompatible,
  kCompareExact = ConfigOptions::kSanityLevelExactMatch,

  kMutable = 0x0100,        // Option is mutable
  kDontSerialize = 0x2000,  // Don't serialize the option
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

// Function for converting a option string value into its underlying
// representation in "addr"
// On success, Status::OK is returned and addr is set to the parsed form
// On failure, a non-OK status is returned
// @param opts  The ConfigOptions controlling how the value is parsed
// @param name  The name of the options being parsed
// @param value The string representation of the option
// @param addr  Pointer to the object
using ParserFunc = std::function<Status(
    const ConfigOptions& /*opts*/, const std::string& /*name*/,
    const std::string& /*value*/, char* /*addr*/)>;

// Function for converting an option "addr" into its string representation.
// On success, Status::OK is returned and value is the serialized form.
// On failure, a non-OK status is returned
// @param opts  The ConfigOptions controlling how the values are serialized
// @param name  The name of the options being serialized
// @param addr  Pointer to the value being serialized
// @param value The result of the serialization.
using StringFunc = std::function<Status(
    const ConfigOptions& /*opts*/, const std::string& /*name*/,
    const char* /*addr*/, std::string* /*value*/)>;

// Function for comparing two option values
// If they are not equal, updates "mismatch" with the name of the bad option
// @param opts  The ConfigOptions controlling how the values are compared
// @param name  The name of the options being compared
// @param addr1 The first address to compare
// @param addr2 The address to compare to
// @param mismatch If the values are not equal, the name of the option that
// first differs
using EqualsFunc = std::function<bool(
    const ConfigOptions& /*opts*/, const std::string& /*name*/,
    const char* /*addr1*/, const char* /*addr2*/, std::string* mismatch)>;

// A struct for storing constant option information such as option name,
// option type, and offset.
class OptionTypeInfo {
 public:
  int offset;
  int mutable_offset;

  // A simple "normal", non-mutable Type "_type" at _offset
  OptionTypeInfo(int _offset, OptionType _type)
      : offset(_offset),
        mutable_offset(0),
        parser_func(nullptr),
        string_func(nullptr),
        equals_func(nullptr),
        type(_type),
        verification(OptionVerificationType::kNormal),
        flags(OptionTypeFlags::kNone) {}

  // A simple "normal", mutable Type "_type" at _offset
  OptionTypeInfo(int _offset, OptionType _type, int _mutable_offset)
      : offset(_offset),
        mutable_offset(_mutable_offset),
        parser_func(nullptr),
        string_func(nullptr),
        equals_func(nullptr),
        type(_type),
        verification(OptionVerificationType::kNormal),
        flags(OptionTypeFlags::kMutable) {}

  OptionTypeInfo(int _offset, OptionType _type,
                 OptionVerificationType _verification, OptionTypeFlags _flags,
                 int _mutable_offset)
      : offset(_offset),
        mutable_offset(_mutable_offset),
        parser_func(nullptr),
        string_func(nullptr),
        equals_func(nullptr),
        type(_type),
        verification(_verification),
        flags(_flags) {}

  OptionTypeInfo(int _offset, OptionType _type,
                 OptionVerificationType _verification, OptionTypeFlags _flags,
                 int _mutable_offset, const ParserFunc& _pfunc)
      : offset(_offset),
        mutable_offset(_mutable_offset),
        parser_func(_pfunc),
        string_func(nullptr),
        equals_func(nullptr),
        type(_type),
        verification(_verification),
        flags(_flags) {}

  OptionTypeInfo(int _offset, OptionType _type,
                 OptionVerificationType _verification, OptionTypeFlags _flags,
                 int _mutable_offset, const ParserFunc& _pfunc,
                 const StringFunc& _sfunc, const EqualsFunc& _efunc)
      : offset(_offset),
        mutable_offset(_mutable_offset),
        parser_func(_pfunc),
        string_func(_sfunc),
        equals_func(_efunc),
        type(_type),
        verification(_verification),
        flags(_flags) {}

  bool IsEnabled(OptionTypeFlags otf) const { return (flags & otf) == otf; }

  bool IsMutable() const { return IsEnabled(OptionTypeFlags::kMutable); }

  bool IsDeprecated() const {
    return IsEnabled(OptionVerificationType::kDeprecated);
  }

  // Returns true if the option is marked as an Alias.
  // Aliases are valid options that are parsed but are not converted to strings
  // or compared.
  bool IsAlias() const { return IsEnabled(OptionVerificationType::kAlias); }

  bool IsEnabled(OptionVerificationType ovf) const {
    return verification == ovf;
  }

  // Returns the sanity level for comparing the option.
  // If the options should not be compared, returns None
  // If the option has a compare flag, returns it.
  // Otherwise, returns "exact"
  ConfigOptions::SanityLevel GetSanityLevel() const {
    if (IsDeprecated() || IsAlias()) {
      return ConfigOptions::SanityLevel::kSanityLevelNone;
    } else {
      auto match = (flags & OptionTypeFlags::kCompareExact);
      if (match == OptionTypeFlags::kCompareDefault) {
        return ConfigOptions::SanityLevel::kSanityLevelExactMatch;
      } else {
        return (ConfigOptions::SanityLevel)match;
      }
    }
  }

  // Returns true if the option should be serialized.
  // Options should be serialized if the are not deprecated, aliases,
  // or marked as "Don't Serialize".
  bool ShouldSerialize() const {
    if (IsDeprecated() || IsAlias()) {
      return false;
    } else if (IsEnabled(OptionTypeFlags::kDontSerialize)) {
      return false;
    } else {
      return true;
    }
  }

  bool IsByName() const {
    return (verification == OptionVerificationType::kByName ||
            verification == OptionVerificationType::kByNameAllowNull ||
            verification == OptionVerificationType::kByNameAllowFromNull);
  }

  // Parses the option in "opt_value" according to the rules of this class
  // and updates the value at "opt_addr".
  // On success, Status::OK() is returned.  On failure:
  // NotFound means the opt_name is not valid for this option
  // NotSupported means we do not know how to parse the value for this option
  // InvalidArgument means the opt_value is not valid for this option.
  Status ParseOption(const ConfigOptions& config_options,
                     const std::string& opt_name, const std::string& opt_value,
                     char* opt_addr) const;

  // Serializes the option in "opt_addr" according to the rules of this class
  // into the value at "opt_value".
  Status SerializeOption(const ConfigOptions& config_options,
                         const std::string& opt_name, const char* opt_addr,
                         std::string* opt_value) const;

  // Compares the "addr1" and "addr2" values according to the rules of this
  // class and returns true if they match.  On a failed match, mismatch is the
  // name of the option that failed to match.
  bool MatchesOption(const ConfigOptions& config_options,
                     const std::string& opt_name, const char* addr1,
                     const char* addr2, std::string* mismatch) const;

  // Used to override the match rules for "ByName" options.
  bool MatchesByName(const ConfigOptions& config_options,
                     const std::string& opt_name, const char* this_offset,
                     const char* that_offset) const;
  bool MatchesByName(const ConfigOptions& config_options,
                     const std::string& opt_name, const char* this_ptr,
                     const std::string& that_value) const;

 private:
  // The optional function to convert a string to its representation
  ParserFunc parser_func;

  // The optional function to convert a value to its string representation
  StringFunc string_func;

  // The optional function to convert a match to option values
  EqualsFunc equals_func;

  OptionType type;
  OptionVerificationType verification;
  OptionTypeFlags flags;
};
}  // namespace ROCKSDB_NAMESPACE
