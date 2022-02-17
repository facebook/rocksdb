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
class OptionTypeInfo;

// The underlying "class/type" of the option.
// This enum is used to determine how the option should
// be converted to/from strings and compared.
enum class OptionType {
  kBoolean,
  kInt,
  kInt32T,
  kInt64T,
  kUInt,
  kUInt8T,
  kUInt32T,
  kUInt64T,
  kSizeT,
  kString,
  kDouble,
  kCompactionStyle,
  kCompactionPri,
  kCompressionType,
  kCompactionStopStyle,
  kChecksumType,
  kEncodingType,
  kEnv,
  kEnum,
  kStruct,
  kVector,
  kConfigurable,
  kCustomizable,
  kEncodedString,
  kTemperature,
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

// A set of modifier flags used to alter how an option is evaluated or
// processed. These flags can be combined together (e.g. kMutable | kShared).
// The kCompare flags can be used to control if/when options are compared.
// If kCompareNever is set, two related options would never be compared (always
// equal) If kCompareExact is set, the options will only be compared if the
// sanity mode
//                  is exact
// kMutable       means the option can be changed after it is prepared
// kShared        means the option is contained in a std::shared_ptr
// kUnique        means the option is contained in a std::uniqued_ptr
// kRawPointer    means the option is a raw pointer value.
// kAllowNull     means that an option is allowed to be null for verification
//                purposes.
// kDontSerialize means this option should not be serialized and included in
//                the string representation.
// kDontPrepare   means do not call PrepareOptions for this pointer value.
enum class OptionTypeFlags : uint32_t {
  kNone = 0x00,  // No flags
  kCompareDefault = 0x0,
  kCompareNever = ConfigOptions::kSanityLevelNone,
  kCompareLoose = ConfigOptions::kSanityLevelLooselyCompatible,
  kCompareExact = ConfigOptions::kSanityLevelExactMatch,

  kMutable = 0x0100,         // Option is mutable
  kRawPointer = 0x0200,      // The option is stored as a raw pointer
  kShared = 0x0400,          // The option is stored as a shared_ptr
  kUnique = 0x0800,          // The option is stored as a unique_ptr
  kAllowNull = 0x1000,       // The option can be null
  kDontSerialize = 0x2000,   // Don't serialize the option
  kDontPrepare = 0x4000,     // Don't prepare or sanitize this option
  kStringNameOnly = 0x8000,  // The option serializes to a name only
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

// Converts an string into its enumerated value.
// @param type_map Mapping between strings and enum values
// @param type The string representation of the enum
// @param value Returns the enum value represented by the string
// @return true if the string was found in the enum map, false otherwise.
template <typename T>
bool ParseEnum(const std::unordered_map<std::string, T>& type_map,
               const std::string& type, T* value) {
  auto iter = type_map.find(type);
  if (iter != type_map.end()) {
    *value = iter->second;
    return true;
  }
  return false;
}

// Converts an enum into its string representation.
// @param type_map Mapping between strings and enum values
// @param type The enum
// @param value Returned as the string representation of the enum
// @return true if the enum was found in the enum map, false otherwise.
template <typename T>
bool SerializeEnum(const std::unordered_map<std::string, T>& type_map,
                   const T& type, std::string* value) {
  for (const auto& pair : type_map) {
    if (pair.second == type) {
      *value = pair.first;
      return true;
    }
  }
  return false;
}

template <typename T>
Status ParseVector(const ConfigOptions& config_options,
                   const OptionTypeInfo& elem_info, char separator,
                   const std::string& name, const std::string& value,
                   std::vector<T>* result);

template <typename T>
Status SerializeVector(const ConfigOptions& config_options,
                       const OptionTypeInfo& elem_info, char separator,
                       const std::string& name, const std::vector<T>& vec,
                       std::string* value);
template <typename T>
bool VectorsAreEqual(const ConfigOptions& config_options,
                     const OptionTypeInfo& elem_info, const std::string& name,
                     const std::vector<T>& vec1, const std::vector<T>& vec2,
                     std::string* mismatch);

// Function for converting a option string value into its underlying
// representation in "addr"
// On success, Status::OK is returned and addr is set to the parsed form
// On failure, a non-OK status is returned
// @param opts  The ConfigOptions controlling how the value is parsed
// @param name  The name of the options being parsed
// @param value The string representation of the option
// @param addr  Pointer to the object
using ParseFunc = std::function<Status(
    const ConfigOptions& /*opts*/, const std::string& /*name*/,
    const std::string& /*value*/, void* /*addr*/)>;

// Function for converting an option "addr" into its string representation.
// On success, Status::OK is returned and value is the serialized form.
// On failure, a non-OK status is returned
// @param opts  The ConfigOptions controlling how the values are serialized
// @param name  The name of the options being serialized
// @param addr  Pointer to the value being serialized
// @param value The result of the serialization.
using SerializeFunc = std::function<Status(
    const ConfigOptions& /*opts*/, const std::string& /*name*/,
    const void* /*addr*/, std::string* /*value*/)>;

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
    const void* /*addr1*/, const void* /*addr2*/, std::string* mismatch)>;

// A struct for storing constant option information such as option name,
// option type, and offset.
class OptionTypeInfo {
 public:
  // A simple "normal", non-mutable Type "type" at offset
  OptionTypeInfo(int offset, OptionType type)
      : offset_(offset),
        parse_func_(nullptr),
        serialize_func_(nullptr),
        equals_func_(nullptr),
        type_(type),
        verification_(OptionVerificationType::kNormal),
        flags_(OptionTypeFlags::kNone) {}

  OptionTypeInfo(int offset, OptionType type,
                 OptionVerificationType verification, OptionTypeFlags flags)
      : offset_(offset),
        parse_func_(nullptr),
        serialize_func_(nullptr),
        equals_func_(nullptr),
        type_(type),
        verification_(verification),
        flags_(flags) {}

  OptionTypeInfo(int offset, OptionType type,
                 OptionVerificationType verification, OptionTypeFlags flags,
                 const ParseFunc& parse_func)
      : offset_(offset),
        parse_func_(parse_func),
        serialize_func_(nullptr),
        equals_func_(nullptr),
        type_(type),
        verification_(verification),
        flags_(flags) {}

  OptionTypeInfo(int offset, OptionType type,
                 OptionVerificationType verification, OptionTypeFlags flags,
                 const ParseFunc& parse_func,
                 const SerializeFunc& serialize_func,
                 const EqualsFunc& equals_func)
      : offset_(offset),
        parse_func_(parse_func),
        serialize_func_(serialize_func),
        equals_func_(equals_func),
        type_(type),
        verification_(verification),
        flags_(flags) {}

  // Creates an OptionTypeInfo for an enum type.  Enums use an additional
  // map to convert the enums to/from their string representation.
  // To create an OptionTypeInfo that is an Enum, one should:
  // - Create a static map of string values to the corresponding enum value
  // - Call this method passing the static map in as a parameter.
  // Note that it is not necessary to add a new OptionType or make any
  // other changes -- the returned object handles parsing, serialization, and
  // comparisons.
  //
  // @param offset The offset in the option object for this enum
  // @param map The string to enum mapping for this enum
  template <typename T>
  static OptionTypeInfo Enum(
      int offset, const std::unordered_map<std::string, T>* const map,
      OptionTypeFlags flags = OptionTypeFlags::kNone) {
    return OptionTypeInfo(
        offset, OptionType::kEnum, OptionVerificationType::kNormal, flags,
        // Uses the map argument to convert the input string into
        // its corresponding enum value.  If value is found in the map,
        // addr is updated to the corresponding map entry.
        // @return OK if the value is found in the map
        // @return InvalidArgument if the value is not found in the map
        [map](const ConfigOptions&, const std::string& name,
              const std::string& value, void* addr) {
          if (map == nullptr) {
            return Status::NotSupported("No enum mapping ", name);
          } else if (ParseEnum<T>(*map, value, static_cast<T*>(addr))) {
            return Status::OK();
          } else {
            return Status::InvalidArgument("No mapping for enum ", name);
          }
        },
        // Uses the map argument to convert the input enum into
        // its corresponding string value.  If enum value is found in the map,
        // value is updated to the corresponding string value in the map.
        // @return OK if the enum is found in the map
        // @return InvalidArgument if the enum is not found in the map
        [map](const ConfigOptions&, const std::string& name, const void* addr,
              std::string* value) {
          if (map == nullptr) {
            return Status::NotSupported("No enum mapping ", name);
          } else if (SerializeEnum<T>(*map, (*static_cast<const T*>(addr)),
                                      value)) {
            return Status::OK();
          } else {
            return Status::InvalidArgument("No mapping for enum ", name);
          }
        },
        // Casts addr1 and addr2 to the enum type and returns true if
        // they are equal, false otherwise.
        [](const ConfigOptions&, const std::string&, const void* addr1,
           const void* addr2, std::string*) {
          return (*static_cast<const T*>(addr1) ==
                  *static_cast<const T*>(addr2));
        });
  }  // End OptionTypeInfo::Enum

  // Creates an OptionTypeInfo for a Struct type.  Structs have a
  // map of string-OptionTypeInfo associated with them that describes how
  // to process the object for parsing, serializing, and matching.
  // Structs also have a struct_name, which is the name of the object
  // as registered in the parent map.
  // When processing a struct, the option name can be specified as:
  //   - <struct_name>       Meaning to process the entire struct.
  //   - <struct_name.field> Meaning to process the single field
  //   - <field>             Process the single fields
  // The CompactionOptionsFIFO, CompactionOptionsUniversal, and LRUCacheOptions
  // are all examples of Struct options.
  //
  // To create an OptionTypeInfo that is a Struct, one should:
  // - Create a static map of string-OptionTypeInfo corresponding to the
  //   properties of the object that can be set via the options.
  // - Call this method passing the name and map in as parameters.
  // Note that it is not necessary to add a new OptionType or make any
  // other changes -- the returned object handles parsing, serialization, and
  // comparisons.
  //
  // @param offset The offset in the option object for this enum
  // @param map The string to enum mapping for this enum
  static OptionTypeInfo Struct(
      const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* struct_map,
      int offset, OptionVerificationType verification, OptionTypeFlags flags) {
    return OptionTypeInfo(
        offset, OptionType::kStruct, verification, flags,
        // Parses the struct and updates the fields at addr
        [struct_name, struct_map](const ConfigOptions& opts,
                                  const std::string& name,
                                  const std::string& value, void* addr) {
          return ParseStruct(opts, struct_name, struct_map, name, value, addr);
        },
        // Serializes the struct options into value
        [struct_name, struct_map](const ConfigOptions& opts,
                                  const std::string& name, const void* addr,
                                  std::string* value) {
          return SerializeStruct(opts, struct_name, struct_map, name, addr,
                                 value);
        },
        // Compares the struct fields of addr1 and addr2 for equality
        [struct_name, struct_map](const ConfigOptions& opts,
                                  const std::string& name, const void* addr1,
                                  const void* addr2, std::string* mismatch) {
          return StructsAreEqual(opts, struct_name, struct_map, name, addr1,
                                 addr2, mismatch);
        });
  }
  static OptionTypeInfo Struct(
      const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* struct_map,
      int offset, OptionVerificationType verification, OptionTypeFlags flags,
      const ParseFunc& parse_func) {
    return OptionTypeInfo(
        offset, OptionType::kStruct, verification, flags, parse_func,
        [struct_name, struct_map](const ConfigOptions& opts,
                                  const std::string& name, const void* addr,
                                  std::string* value) {
          return SerializeStruct(opts, struct_name, struct_map, name, addr,
                                 value);
        },
        [struct_name, struct_map](const ConfigOptions& opts,
                                  const std::string& name, const void* addr1,
                                  const void* addr2, std::string* mismatch) {
          return StructsAreEqual(opts, struct_name, struct_map, name, addr1,
                                 addr2, mismatch);
        });
  }

  template <typename T>
  static OptionTypeInfo Vector(int _offset,
                               OptionVerificationType _verification,
                               OptionTypeFlags _flags,
                               const OptionTypeInfo& elem_info,
                               char separator = ':') {
    return OptionTypeInfo(
        _offset, OptionType::kVector, _verification, _flags,
        [elem_info, separator](const ConfigOptions& opts,
                               const std::string& name,
                               const std::string& value, void* addr) {
          auto result = static_cast<std::vector<T>*>(addr);
          return ParseVector<T>(opts, elem_info, separator, name, value,
                                result);
        },
        [elem_info, separator](const ConfigOptions& opts,
                               const std::string& name, const void* addr,
                               std::string* value) {
          const auto& vec = *(static_cast<const std::vector<T>*>(addr));
          return SerializeVector<T>(opts, elem_info, separator, name, vec,
                                    value);
        },
        [elem_info](const ConfigOptions& opts, const std::string& name,
                    const void* addr1, const void* addr2,
                    std::string* mismatch) {
          const auto& vec1 = *(static_cast<const std::vector<T>*>(addr1));
          const auto& vec2 = *(static_cast<const std::vector<T>*>(addr2));
          return VectorsAreEqual<T>(opts, elem_info, name, vec1, vec2,
                                    mismatch);
        });
  }

  // Create a new std::shared_ptr<Customizable> OptionTypeInfo
  // This function will call the T::CreateFromString method to create a new
  // std::shared_ptr<T> object.
  //
  // @param offset The offset for the Customizable from the base pointer
  // @param ovt How to verify this option
  // @param flags, Extra flags specifying the behavior of this option
  // @param _sfunc Optional function for serializing this option
  // @param _efunc Optional function for comparing this option
  template <typename T>
  static OptionTypeInfo AsCustomSharedPtr(int offset,
                                          OptionVerificationType ovt,
                                          OptionTypeFlags flags) {
    return AsCustomSharedPtr<T>(offset, ovt, flags, nullptr, nullptr);
  }

  template <typename T>
  static OptionTypeInfo AsCustomSharedPtr(int offset,
                                          OptionVerificationType ovt,
                                          OptionTypeFlags flags,
                                          const SerializeFunc& serialize_func,
                                          const EqualsFunc& equals_func) {
    return OptionTypeInfo(
        offset, OptionType::kCustomizable, ovt,
        flags | OptionTypeFlags::kShared,
        [](const ConfigOptions& opts, const std::string& name,
           const std::string& value, void* addr) {
          auto* shared = static_cast<std::shared_ptr<T>*>(addr);
          if (name == kIdPropName() && value.empty()) {
            shared->reset();
            return Status::OK();
          } else {
            return T::CreateFromString(opts, value, shared);
          }
        },
        serialize_func, equals_func);
  }

  // Create a new std::unique_ptr<Customizable> OptionTypeInfo
  // This function will call the T::CreateFromString method to create a new
  // std::unique_ptr<T> object.
  //
  // @param offset The offset for the Customizable from the base pointer
  // @param ovt How to verify this option
  // @param flags, Extra flags specifying the behavior of this option
  // @param _sfunc Optional function for serializing this option
  // @param _efunc Optional function for comparing this option
  template <typename T>
  static OptionTypeInfo AsCustomUniquePtr(int offset,
                                          OptionVerificationType ovt,
                                          OptionTypeFlags flags) {
    return AsCustomUniquePtr<T>(offset, ovt, flags, nullptr, nullptr);
  }

  template <typename T>
  static OptionTypeInfo AsCustomUniquePtr(int offset,
                                          OptionVerificationType ovt,
                                          OptionTypeFlags flags,
                                          const SerializeFunc& serialize_func,
                                          const EqualsFunc& equals_func) {
    return OptionTypeInfo(
        offset, OptionType::kCustomizable, ovt,
        flags | OptionTypeFlags::kUnique,
        [](const ConfigOptions& opts, const std::string& name,
           const std::string& value, void* addr) {
          auto* unique = static_cast<std::unique_ptr<T>*>(addr);
          if (name == kIdPropName() && value.empty()) {
            unique->reset();
            return Status::OK();
          } else {
            return T::CreateFromString(opts, value, unique);
          }
        },
        serialize_func, equals_func);
  }

  // Create a new Customizable* OptionTypeInfo
  // This function will call the T::CreateFromString method to create a new
  // T object.
  //
  // @param _offset The offset for the Customizable from the base pointer
  // @param ovt How to verify this option
  // @param flags, Extra flags specifying the behavior of this option
  // @param _sfunc Optional function for serializing this option
  // @param _efunc Optional function for comparing this option
  template <typename T>
  static OptionTypeInfo AsCustomRawPtr(int offset, OptionVerificationType ovt,
                                       OptionTypeFlags flags) {
    return AsCustomRawPtr<T>(offset, ovt, flags, nullptr, nullptr);
  }

  template <typename T>
  static OptionTypeInfo AsCustomRawPtr(int offset, OptionVerificationType ovt,
                                       OptionTypeFlags flags,
                                       const SerializeFunc& serialize_func,
                                       const EqualsFunc& equals_func) {
    return OptionTypeInfo(
        offset, OptionType::kCustomizable, ovt,
        flags | OptionTypeFlags::kRawPointer,
        [](const ConfigOptions& opts, const std::string& name,
           const std::string& value, void* addr) {
          auto** pointer = static_cast<T**>(addr);
          if (name == kIdPropName() && value.empty()) {
            *pointer = nullptr;
            return Status::OK();
          } else {
            return T::CreateFromString(opts, value, pointer);
          }
        },
        serialize_func, equals_func);
  }

  bool IsEnabled(OptionTypeFlags otf) const { return (flags_ & otf) == otf; }

  bool IsEditable(const ConfigOptions& opts) const {
    if (opts.mutable_options_only) {
      return IsMutable();
    } else {
      return true;
    }
  }
  bool IsMutable() const { return IsEnabled(OptionTypeFlags::kMutable); }

  bool IsDeprecated() const {
    return IsEnabled(OptionVerificationType::kDeprecated);
  }

  // Returns true if the option is marked as an Alias.
  // Aliases are valid options that are parsed but are not converted to strings
  // or compared.
  bool IsAlias() const { return IsEnabled(OptionVerificationType::kAlias); }

  bool IsEnabled(OptionVerificationType ovf) const {
    return verification_ == ovf;
  }

  // Returns the sanity level for comparing the option.
  // If the options should not be compared, returns None
  // If the option has a compare flag, returns it.
  // Otherwise, returns "exact"
  ConfigOptions::SanityLevel GetSanityLevel() const {
    if (IsDeprecated() || IsAlias()) {
      return ConfigOptions::SanityLevel::kSanityLevelNone;
    } else {
      auto match = (flags_ & OptionTypeFlags::kCompareExact);
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

  // Returns true if the option is allowed to be null.
  // Options can be null if the verification type is allow from null
  // or if the flags specify allow null.
  bool CanBeNull() const {
    return (IsEnabled(OptionTypeFlags::kAllowNull) ||
            IsEnabled(OptionVerificationType::kByNameAllowNull) ||
            IsEnabled(OptionVerificationType::kByNameAllowFromNull));
  }

  bool IsSharedPtr() const { return IsEnabled(OptionTypeFlags::kShared); }

  bool IsUniquePtr() const { return IsEnabled(OptionTypeFlags::kUnique); }

  bool IsRawPtr() const { return IsEnabled(OptionTypeFlags::kRawPointer); }

  bool IsByName() const {
    return (verification_ == OptionVerificationType::kByName ||
            verification_ == OptionVerificationType::kByNameAllowNull ||
            verification_ == OptionVerificationType::kByNameAllowFromNull);
  }

  bool IsStruct() const { return (type_ == OptionType::kStruct); }

  bool IsConfigurable() const {
    return (type_ == OptionType::kConfigurable ||
            type_ == OptionType::kCustomizable);
  }

  bool IsCustomizable() const { return (type_ == OptionType::kCustomizable); }

  // Returns the underlying pointer for the type at base_addr
  // The value returned is the underlying "raw" pointer, offset from base.
  template <typename T>
  const T* AsRawPointer(const void* const base_addr) const {
    if (base_addr == nullptr) {
      return nullptr;
    }
    const void* opt_addr = static_cast<const char*>(base_addr) + offset_;
    if (IsUniquePtr()) {
      const std::unique_ptr<T>* ptr =
          static_cast<const std::unique_ptr<T>*>(opt_addr);
      return ptr->get();
    } else if (IsSharedPtr()) {
      const std::shared_ptr<T>* ptr =
          static_cast<const std::shared_ptr<T>*>(opt_addr);
      return ptr->get();
    } else if (IsRawPtr()) {
      const T* const* ptr = static_cast<const T* const*>(opt_addr);
      return *ptr;
    } else {
      return static_cast<const T*>(opt_addr);
    }
  }

  // Returns the underlying pointer for the type at base_addr
  // The value returned is the underlying "raw" pointer, offset from base.
  template <typename T>
  T* AsRawPointer(void* base_addr) const {
    if (base_addr == nullptr) {
      return nullptr;
    }
    void* opt_addr = static_cast<char*>(base_addr) + offset_;
    if (IsUniquePtr()) {
      std::unique_ptr<T>* ptr = static_cast<std::unique_ptr<T>*>(opt_addr);
      return ptr->get();
    } else if (IsSharedPtr()) {
      std::shared_ptr<T>* ptr = static_cast<std::shared_ptr<T>*>(opt_addr);
      return ptr->get();
    } else if (IsRawPtr()) {
      T** ptr = static_cast<T**>(opt_addr);
      return *ptr;
    } else {
      return static_cast<T*>(opt_addr);
    }
  }

  // Parses the option in "opt_value" according to the rules of this class
  // and updates the value at "opt_ptr".
  // On success, Status::OK() is returned.  On failure:
  // NotFound means the opt_name is not valid for this option
  // NotSupported means we do not know how to parse the value for this option
  // InvalidArgument means the opt_value is not valid for this option.
  Status Parse(const ConfigOptions& config_options, const std::string& opt_name,
               const std::string& opt_value, void* const opt_ptr) const;

  // Serializes the option in "opt_addr" according to the rules of this class
  // into the value at "opt_value".
  Status Serialize(const ConfigOptions& config_options,
                   const std::string& opt_name, const void* const opt_ptr,
                   std::string* opt_value) const;

  // Compares the "addr1" and "addr2" values according to the rules of this
  // class and returns true if they match.  On a failed match, mismatch is the
  // name of the option that failed to match.
  bool AreEqual(const ConfigOptions& config_options,
                const std::string& opt_name, const void* const addr1,
                const void* const addr2, std::string* mismatch) const;

  // Used to override the match rules for "ByName" options.
  bool AreEqualByName(const ConfigOptions& config_options,
                      const std::string& opt_name, const void* const this_ptr,
                      const void* const that_ptr) const;
  bool AreEqualByName(const ConfigOptions& config_options,
                      const std::string& opt_name, const void* const this_ptr,
                      const std::string& that_value) const;

  // Parses the input opts_map according to the type_map for the opt_addr
  // For each name-value pair in opts_map, find the corresponding name in
  // type_map If the name is found:
  //    - set the corresponding value in opt_addr, returning the status on
  //    failure;
  // If the name is not found:
  //    - If unused is specified, add the name-value to unused and continue
  //    - If ingore_unknown_options is false, return NotFound
  // Returns OK if all options were either:
  //    - Successfully set
  //    - options were not found and ignore_unknown_options=true
  //    - options were not found and unused was specified
  // Note that this method is much less sophisticated than the comparable
  // Configurable::Configure methods.  For example, on error, there is no
  // attempt to return opt_addr to the initial state.  Additionally, there
  // is no effort to initialize (Configurable::PrepareOptions) the object
  // on success.  This method should typically only be used for simpler,
  // standalone structures and not those that contain shared and embedded
  // objects.
  static Status ParseType(
      const ConfigOptions& config_options, const std::string& opts_str,
      const std::unordered_map<std::string, OptionTypeInfo>& type_map,
      void* opt_addr,
      std::unordered_map<std::string, std::string>* unused = nullptr);
  static Status ParseType(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opts_map,
      const std::unordered_map<std::string, OptionTypeInfo>& type_map,
      void* opt_addr,
      std::unordered_map<std::string, std::string>* unused = nullptr);

  // Parses the input value according to the map for the struct at opt_addr
  // struct_name is the name of the struct option as registered
  // opt_name is the name of the option being evaluated.  This may
  // be the whole struct or a sub-element of it, based on struct_name and
  // opt_name.
  static Status ParseStruct(
      const ConfigOptions& config_options, const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* map,
      const std::string& opt_name, const std::string& value, void* opt_addr);

  // Serializes the values from opt_addr using the rules in type_map.
  // Returns the serialized form in result.
  // Returns OK on success or non-OK if some option could not be serialized.
  static Status SerializeType(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, OptionTypeInfo>& type_map,
      const void* opt_addr, std::string* value);

  // Serializes the input addr according to the map for the struct to value.
  // struct_name is the name of the struct option as registered
  // opt_name is the name of the option being evaluated.  This may
  // be the whole struct or a sub-element of it
  static Status SerializeStruct(
      const ConfigOptions& config_options, const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* map,
      const std::string& opt_name, const void* opt_addr, std::string* value);

  // Compares the values in this_addr and that_addr using the rules in type_map.
  // If the values are equal, returns true
  // If the values are not equal, returns false and sets mismatch to the name
  // of the first value that did not match.
  static bool TypesAreEqual(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, OptionTypeInfo>& map,
      const void* this_addr, const void* that_addr, std::string* mismatch);

  // Compares the input offsets according to the map for the struct and returns
  // true if they are equivalent, false otherwise.
  // struct_name is the name of the struct option as registered
  // opt_name is the name of the option being evaluated.  This may
  // be the whole struct or a sub-element of it
  static bool StructsAreEqual(
      const ConfigOptions& config_options, const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* map,
      const std::string& opt_name, const void* this_offset,
      const void* that_offset, std::string* mismatch);

  // Finds the entry for the opt_name in the opt_map, returning
  // nullptr if not found.
  // If found, elem_name will be the name of option to find.
  // This may be opt_name, or a substring of opt_name.
  // For "simple" options, opt_name will be equal to elem_name.  Given the
  // opt_name "opt", elem_name will equal "opt".
  // For "embedded" options (like structs), elem_name may be opt_name
  // or a field within the opt_name.  For example, given the struct "struct",
  // and opt_name of "struct.field", elem_name will be "field"
  static const OptionTypeInfo* Find(
      const std::string& opt_name,
      const std::unordered_map<std::string, OptionTypeInfo>& opt_map,
      std::string* elem_name);

  // Returns the next token marked by the delimiter from "opts" after start in
  // token and updates end to point to where that token stops. Delimiters inside
  // of braces are ignored. Returns OK if a token is found and an error if the
  // input opts string is mis-formatted.
  // Given "a=AA;b=BB;" start=2 and delimiter=";", token is "AA" and end points
  // to "b" Given "{a=A;b=B}", the token would be "a=A;b=B"
  //
  // @param opts The string in which to find the next token
  // @param delimiter The delimiter between tokens
  // @param start     The position in opts to start looking for the token
  // @param ed        Returns the end position in opts of the token
  // @param token     Returns the token
  // @returns OK if a token was found
  // @return InvalidArgument if the braces mismatch
  //          (e.g. "{a={b=c;}" ) -- missing closing brace
  // @return InvalidArgument if an expected delimiter is not found
  //        e.g. "{a=b}c=d;" -- missing delimiter before "c"
  static Status NextToken(const std::string& opts, char delimiter, size_t start,
                          size_t* end, std::string* token);

  constexpr static const char* kIdPropName() { return "id"; }
  constexpr static const char* kIdPropSuffix() { return ".id"; }

 private:
  int offset_;

  // The optional function to convert a string to its representation
  ParseFunc parse_func_;

  // The optional function to convert a value to its string representation
  SerializeFunc serialize_func_;

  // The optional function to match two option values
  EqualsFunc equals_func_;

  OptionType type_;
  OptionVerificationType verification_;
  OptionTypeFlags flags_;
};

// Parses the input value into elements of the result vector.  This method
// will break the input value into the individual tokens (based on the
// separator), where each of those tokens will be parsed based on the rules of
// elem_info. The result vector will be populated with elements based on the
// input tokens. For example, if the value=1:2:3:4:5 and elem_info parses
// integers, the result vector will contain the integers 1,2,3,4,5
// @param config_options Controls how the option value is parsed.
// @param elem_info Controls how individual tokens in value are parsed
// @param separator Character separating tokens in values (':' in the above
// example)
// @param name      The name associated with this vector option
// @param value     The input string to parse into tokens
// @param result    Returns the results of parsing value into its elements.
// @return OK if the value was successfully parse
// @return InvalidArgument if the value is improperly formed or if the token
//                          could not be parsed
// @return NotFound         If the tokenized value contains unknown options for
// its type
template <typename T>
Status ParseVector(const ConfigOptions& config_options,
                   const OptionTypeInfo& elem_info, char separator,
                   const std::string& name, const std::string& value,
                   std::vector<T>* result) {
  result->clear();
  Status status;

  // Turn off ignore_unknown_objects so we can tell if the returned
  // object is valid or not.
  ConfigOptions copy = config_options;
  copy.ignore_unsupported_options = false;
  for (size_t start = 0, end = 0;
       status.ok() && start < value.size() && end != std::string::npos;
       start = end + 1) {
    std::string token;
    status = OptionTypeInfo::NextToken(value, separator, start, &end, &token);
    if (status.ok()) {
      T elem;
      status = elem_info.Parse(copy, name, token, &elem);
      if (status.ok()) {
        result->emplace_back(elem);
      } else if (config_options.ignore_unsupported_options &&
                 status.IsNotSupported()) {
        // If we were ignoring unsupported options and this one should be
        // ignored, ignore it by setting the status to OK
        status = Status::OK();
      }
    }
  }
  return status;
}

// Serializes the input vector into its output value.  Elements are
// separated by the separator character.  This element will convert all of the
// elements in vec into their serialized form, using elem_info to perform the
// serialization.
// For example, if the vec contains the integers 1,2,3,4,5 and elem_info
// serializes the output would be 1:2:3:4:5 for separator ":".
// @param config_options Controls how the option value is serialized.
// @param elem_info Controls how individual tokens in value are serialized
// @param separator Character separating tokens in value (':' in the above
// example)
// @param name      The name associated with this vector option
// @param vec       The input vector to serialize
// @param value     The output string of serialized options
// @return OK if the value was successfully parse
// @return InvalidArgument if the value is improperly formed or if the token
//                          could not be parsed
// @return NotFound         If the tokenized value contains unknown options for
// its type
template <typename T>
Status SerializeVector(const ConfigOptions& config_options,
                       const OptionTypeInfo& elem_info, char separator,
                       const std::string& name, const std::vector<T>& vec,
                       std::string* value) {
  std::string result;
  ConfigOptions embedded = config_options;
  embedded.delimiter = ";";
  int printed = 0;
  for (const auto& elem : vec) {
    std::string elem_str;
    Status s = elem_info.Serialize(embedded, name, &elem, &elem_str);
    if (!s.ok()) {
      return s;
    } else if (!elem_str.empty()) {
      if (printed++ > 0) {
        result += separator;
      }
      // If the element contains embedded separators, put it inside of brackets
      if (elem_str.find(separator) != std::string::npos) {
        result += "{" + elem_str + "}";
      } else {
        result += elem_str;
      }
    }
  }
  if (result.find("=") != std::string::npos) {
    *value = "{" + result + "}";
  } else if (printed > 1 && result.at(0) == '{') {
    *value = "{" + result + "}";
  } else {
    *value = result;
  }
  return Status::OK();
}

// Compares the input vectors vec1 and vec2 for equality
// If the vectors are the same size, elements of the vectors are compared one by
// one using elem_info to perform the comparison.
//
// @param config_options Controls how the vectors are compared.
// @param elem_info Controls how individual elements in the vectors are compared
// @param name      The name associated with this vector option
// @param vec1,vec2 The vectors to compare.
// @param mismatch  If the vectors are not equivalent, mismatch will point to
// the first
//                  element of the comparison that did not match.
// @return true     If vec1 and vec2 are "equal", false otherwise
template <typename T>
bool VectorsAreEqual(const ConfigOptions& config_options,
                     const OptionTypeInfo& elem_info, const std::string& name,
                     const std::vector<T>& vec1, const std::vector<T>& vec2,
                     std::string* mismatch) {
  if (vec1.size() != vec2.size()) {
    *mismatch = name;
    return false;
  } else {
    for (size_t i = 0; i < vec1.size(); ++i) {
      if (!elem_info.AreEqual(
              config_options, name, reinterpret_cast<const char*>(&vec1[i]),
              reinterpret_cast<const char*>(&vec2[i]), mismatch)) {
        return false;
      }
    }
    return true;
  }
}
}  // namespace ROCKSDB_NAMESPACE
