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

enum class OptionType {
  kBoolean,
  kInt,
  kInt32T,
  kInt64T,
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
  kCompactionStopStyle,
  kChecksumType,
  kEncodingType,
  kEnum,
  kStruct,
  kVector,
  kConfigurable,
  kCustomizable,
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

  kMutable = 0x0100,         // Option is mutable
  kPointer = 0x0200,         // The option is stored as a pointer
  kShared = 0x0400,          // The option is stored as a shared_ptr
  kUnique = 0x0800,          // The option is stored as a unique_ptr
  kAllowNull = 0x1000,       // The option can be null
  kDontSerialize = 0x2000,   // Don't serialize the option
  kStringNameOnly = 0x4000,  // The option serializes to a name only
  kDontPrepare = 0x4000,     // Don't prepare or sanitize this option
};

inline OptionTypeFlags operator|(const OptionTypeFlags& a,
                                 const OptionTypeFlags& b) {
  return static_cast<OptionTypeFlags>(static_cast<uint32_t>(a) |
                                      static_cast<uint32_t>(b));
}

inline OptionTypeFlags operator&(const OptionTypeFlags& a,
                                 const OptionTypeFlags& b) {
  return static_cast<OptionTypeFlags>(static_cast<uint32_t>(a) &
                                      static_cast<uint32_t>(b));
}

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
bool MatchesVector(const ConfigOptions& config_options,
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
  // A simple "normal", non-mutable Type "_type" at _offset
  OptionTypeInfo(int _offset, OptionType _type)
      : offset(_offset),
        parser_func(nullptr),
        string_func(nullptr),
        equals_func(nullptr),
        type(_type),
        verification(OptionVerificationType::kNormal),
        flags(OptionTypeFlags::kNone) {}

  OptionTypeInfo(int _offset, OptionType _type,
                 OptionVerificationType _verification, OptionTypeFlags _flags)
      : offset(_offset),
        parser_func(nullptr),
        string_func(nullptr),
        equals_func(nullptr),
        type(_type),
        verification(_verification),
        flags(_flags) {}

  OptionTypeInfo(int _offset, OptionType _type,
                 OptionVerificationType _verification, OptionTypeFlags _flags,
                 const ParserFunc& _pfunc)
      : offset(_offset),
        parser_func(_pfunc),
        string_func(nullptr),
        equals_func(nullptr),
        type(_type),
        verification(_verification),
        flags(_flags) {}

  OptionTypeInfo(int _offset, OptionType _type,
                 OptionVerificationType _verification, OptionTypeFlags _flags,
                 const ParserFunc& _pfunc, const StringFunc& _sfunc,
                 const EqualsFunc& _efunc)
      : offset(_offset),
        parser_func(_pfunc),
        string_func(_sfunc),
        equals_func(_efunc),
        type(_type),
        verification(_verification),
        flags(_flags) {}

  template <typename T>
  static OptionTypeInfo Enum(
      int _offset, const std::unordered_map<std::string, T>* const map) {
    return OptionTypeInfo(
        _offset, OptionType::kEnum, OptionVerificationType::kNormal,
        OptionTypeFlags::kNone,
        [map](const ConfigOptions&, const std::string& name,
              const std::string& value, char* addr) {
          if (map == nullptr) {
            return Status::NotSupported("No enum mapping ", name);
          } else if (ParseEnum<T>(*map, value, reinterpret_cast<T*>(addr))) {
            return Status::OK();
          } else {
            return Status::InvalidArgument("No mapping for enum ", name);
          }
        },
        [map](const ConfigOptions&, const std::string& name, const char* addr,
              std::string* value) {
          if (map == nullptr) {
            return Status::NotSupported("No enum mapping ", name);
          } else if (SerializeEnum<T>(*map, (*reinterpret_cast<const T*>(addr)),
                                      value)) {
            return Status::OK();
          } else {
            return Status::InvalidArgument("No mapping for enum ", name);
          }
        },
        [](const ConfigOptions&, const std::string&, const char* addr1,
           const char* addr2, std::string*) {
          return (*reinterpret_cast<const T*>(addr1) ==
                  *reinterpret_cast<const T*>(addr2));
        });
  }

  static OptionTypeInfo Struct(
      const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* struct_map,
      int _offset, OptionVerificationType _verification,
      OptionTypeFlags _flags) {
    return OptionTypeInfo(
        _offset, OptionType::kStruct, _verification, _flags,
        [struct_name, struct_map](const ConfigOptions& opts,
                                  const std::string& name,
                                  const std::string& value, char* addr) {
          return ParseStruct(opts, struct_name, struct_map, name, value, addr);
        },
        [struct_name, struct_map](const ConfigOptions& opts,
                                  const std::string& name, const char* addr,
                                  std::string* value) {
          return SerializeStruct(opts, struct_name, struct_map, name, addr,
                                 value);
        },
        [struct_name, struct_map](const ConfigOptions& opts,
                                  const std::string& name, const char* addr1,
                                  const char* addr2, std::string* mismatch) {
          return MatchesStruct(opts, struct_name, struct_map, name, addr1,
                               addr2, mismatch);
        });
  }
  static OptionTypeInfo Struct(
      const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* struct_map,
      int _offset, OptionVerificationType _verification, OptionTypeFlags _flags,
      const ParserFunc& _pfunc) {
    return OptionTypeInfo(
        _offset, OptionType::kStruct, _verification, _flags, _pfunc,
        [struct_name, struct_map](const ConfigOptions& opts,
                                  const std::string& name, const char* addr,
                                  std::string* value) {
          return SerializeStruct(opts, struct_name, struct_map, name, addr,
                                 value);
        },
        [struct_name, struct_map](const ConfigOptions& opts,
                                  const std::string& name, const char* addr1,
                                  const char* addr2, std::string* mismatch) {
          return MatchesStruct(opts, struct_name, struct_map, name, addr1,
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
                               const std::string& value, char* addr) {
          auto result = reinterpret_cast<std::vector<T>*>(addr);
          return ParseVector<T>(opts, elem_info, separator, name, value,
                                result);
        },
        [elem_info, separator](const ConfigOptions& opts,
                               const std::string& name, const char* addr,
                               std::string* value) {
          const auto& vec = *(reinterpret_cast<const std::vector<T>*>(addr));
          return SerializeVector<T>(opts, elem_info, separator, name, vec,
                                    value);
        },
        [elem_info](const ConfigOptions& opts, const std::string& name,
                    const char* addr1, const char* addr2,
                    std::string* mismatch) {
          const auto& vec1 = *(reinterpret_cast<const std::vector<T>*>(addr1));
          const auto& vec2 = *(reinterpret_cast<const std::vector<T>*>(addr2));
          return MatchesVector<T>(opts, elem_info, name, vec1, vec2, mismatch);
        });
  }

  // Create a new std::shared_ptr<Customizable> OptionTypeInfo
  // This function will call the T::CreateFromString method to create a new
  // std::shared_ptr<T> object.
  //
  // @param _offset The offset for the Customizable from the base pointer
  // @param ovt How to verify this option
  // @param flags, Extra flags specifying the behavior of this option
  // @param _sfunc Optional function for serializing this option
  // @param _efunc Optional function for comparing this option
  template <typename T>
  static OptionTypeInfo AsCustomS(int _offset, OptionVerificationType ovt,
                                  OptionTypeFlags flags) {
    return AsCustomS<T>(_offset, ovt, flags, nullptr, nullptr);
  }

  template <typename T>
  static OptionTypeInfo AsCustomS(int _offset, OptionVerificationType ovt,
                                  OptionTypeFlags flags,
                                  const StringFunc& _sfunc,
                                  const EqualsFunc& _efunc) {
    return OptionTypeInfo(
        _offset, OptionType::kCustomizable, ovt,
        flags | OptionTypeFlags::kShared,
        [](const ConfigOptions& opts, const std::string&,
           const std::string& value, char* addr) {
          auto* shared = reinterpret_cast<std::shared_ptr<T>*>(addr);
          return T::CreateFromString(opts, value, shared);
        },
        _sfunc, _efunc);
  }

  // Create a new std::unique_ptr<Customizable> OptionTypeInfo
  // This function will call the T::CreateFromString method to create a new
  // std::unique_ptr<T> object.
  //
  // @param _offset The offset for the Customizable from the base pointer
  // @param ovt How to verify this option
  // @param flags, Extra flags specifying the behavior of this option
  // @param _sfunc Optional function for serializing this option
  // @param _efunc Optional function for comparing this option
  template <typename T>
  static OptionTypeInfo AsCustomU(int _offset, OptionVerificationType ovt,
                                  OptionTypeFlags flags) {
    return AsCustomU<T>(_offset, ovt, flags, nullptr, nullptr);
  }

  template <typename T>
  static OptionTypeInfo AsCustomU(int _offset, OptionVerificationType ovt,
                                  OptionTypeFlags flags,
                                  const StringFunc& _sfunc,
                                  const EqualsFunc& _efunc) {
    return OptionTypeInfo(
        _offset, OptionType::kCustomizable, ovt,
        flags | OptionTypeFlags::kUnique,
        [](const ConfigOptions& opts, const std::string&,
           const std::string& value, char* addr) {
          auto* unique = reinterpret_cast<std::unique_ptr<T>*>(addr);
          return T::CreateFromString(opts, value, unique);
        },
        _sfunc, _efunc);
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
  static OptionTypeInfo AsCustomP(int _offset, OptionVerificationType ovt,
                                  OptionTypeFlags flags) {
    return AsCustomP<T>(_offset, ovt, flags, nullptr, nullptr);
  }

  template <typename T>
  static OptionTypeInfo AsCustomP(int _offset, OptionVerificationType ovt,
                                  OptionTypeFlags flags,
                                  const StringFunc& _sfunc,
                                  const EqualsFunc& _efunc) {
    return OptionTypeInfo(
        _offset, OptionType::kCustomizable, ovt,
        flags | OptionTypeFlags::kPointer,
        [](const ConfigOptions& opts, const std::string&,
           const std::string& value, char* addr) {
          auto** pointer = reinterpret_cast<T**>(addr);
          return T::CreateFromString(opts, value, pointer);
        },
        _sfunc, _efunc);
  }

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

  bool CanBeNull() const {
    return (IsEnabled(OptionTypeFlags::kAllowNull) ||
            IsEnabled(OptionVerificationType::kByNameAllowFromNull));
  }

  bool IsSharedPtr() const { return IsEnabled(OptionTypeFlags::kShared); }

  bool IsUniquePtr() const { return IsEnabled(OptionTypeFlags::kUnique); }

  bool IsRawPtr() const { return IsEnabled(OptionTypeFlags::kPointer); }

  bool IsByName() const {
    return (verification == OptionVerificationType::kByName ||
            verification == OptionVerificationType::kByNameAllowNull ||
            verification == OptionVerificationType::kByNameAllowFromNull);
  }

  bool IsStruct() const { return (type == OptionType::kStruct); }

  bool IsConfigurable() const {
    return (type == OptionType::kConfigurable ||
            type == OptionType::kCustomizable);
  }

  bool IsCustomizable() const { return (type == OptionType::kCustomizable); }

  // Returns the underlying pointer for the type at base_addr
  // The value returned is the underlying "raw" pointer, offset from base.
  template <typename T>
  const T* AsRawPointer(const void* const base_addr) const {
    if (base_addr == nullptr) {
      return nullptr;
    }
    const auto opt_addr = reinterpret_cast<const char*>(base_addr) + offset;
    if (IsUniquePtr()) {
      const std::unique_ptr<T>* ptr =
          reinterpret_cast<const std::unique_ptr<T>*>(opt_addr);
      return ptr->get();
    } else if (IsSharedPtr()) {
      const std::shared_ptr<T>* ptr =
          reinterpret_cast<const std::shared_ptr<T>*>(opt_addr);
      return ptr->get();
    } else if (IsRawPtr()) {
      const T* const* ptr = reinterpret_cast<const T* const*>(opt_addr);
      return *ptr;
    } else {
      return reinterpret_cast<const T*>(opt_addr);
    }
  }

  // Returns the underlying pointer for the type at base_addr
  // The value returned is the underlying "raw" pointer, offset from base.
  template <typename T>
  T* AsRawPointer(void* base_addr) const {
    if (base_addr == nullptr) {
      return nullptr;
    }
    auto opt_addr = reinterpret_cast<char*>(base_addr) + offset;
    if (IsUniquePtr()) {
      std::unique_ptr<T>* ptr = reinterpret_cast<std::unique_ptr<T>*>(opt_addr);
      return ptr->get();
    } else if (IsSharedPtr()) {
      std::shared_ptr<T>* ptr = reinterpret_cast<std::shared_ptr<T>*>(opt_addr);
      return ptr->get();
    } else if (IsRawPtr()) {
      T** ptr = reinterpret_cast<T**>(opt_addr);
      return *ptr;
    } else {
      return reinterpret_cast<T*>(opt_addr);
    }
  }

  // Parses the option in "opt_value" according to the rules of this class
  // and updates the value at "opt_addr".
  // On success, Status::OK() is returned.  On failure:
  // NotFound means the opt_name is not valid for this option
  // NotSupported means we do not know how to parse the value for this option
  // InvalidArgument means the opt_value is not valid for this option.
  Status ParseOption(const ConfigOptions& config_options,
                     const std::string& opt_name, const std::string& opt_value,
                     void* opt_ptr) const;

  // Serializes the option in "opt_ptr" according to the rules of this class
  // into the value at "opt_value".
  Status SerializeOption(const ConfigOptions& config_options,
                         const std::string& opt_name, const void* const opt_ptr,
                         std::string* opt_value) const;

  // Compares the "addr1" and "addr2" values according to the rules of this
  // class and returns true if they match.  On a failed match, mismatch is the
  // name of the option that failed to match.
  bool MatchesOption(const ConfigOptions& config_options,
                     const std::string& opt_name, const void* const addr1,
                     const void* const addr2, std::string* mismatch) const;

  // Used to override the match rules for "ByName" options.
  bool CheckByName(const ConfigOptions& config_options,
                   const std::string& opt_name, const void* const this_ptr,
                   const void* const that_ptr) const;
  bool CheckByName(const ConfigOptions& config_options,
                   const std::string& opt_name, const void* const this_ptr,
                   const std::string& that_value) const;

  // Parses the input value according to the map for the struct at opt_addr
  // struct_name is the name of the struct option as registered
  // opt_name is the name of the option being evaluated.  This may
  // be the whole struct or a sub-element of it
  static Status ParseStruct(
      const ConfigOptions& config_options, const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* map,
      const std::string& opt_name, const std::string& value, char* opt_addr);

  // Serializes the input addr according to the map for the struct to value.
  // struct_name is the name of the struct option as registered
  // opt_name is the name of the option being evaluated.  This may
  // be the whole struct or a sub-element of it
  static Status SerializeStruct(
      const ConfigOptions& config_options, const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* map,
      const std::string& opt_name, const char* opt_addr, std::string* value);

  // Matches the input offsets according to the map for the struct.
  // struct_name is the name of the struct option as registered
  // opt_name is the name of the option being evaluated.  This may
  // be the whole struct or a sub-element of it
  static bool MatchesStruct(
      const ConfigOptions& config_options, const std::string& struct_name,
      const std::unordered_map<std::string, OptionTypeInfo>* map,
      const std::string& opt_name, const char* this_offset,
      const char* that_offset, std::string* mismatch);

  // Finds the entry for the opt_name in the opt_map, returning
  // nullptr if not found.
  // If found, elem_name will be the name of option to find.
  // This may be opt_name, or a substring of opt_name.
  static const OptionTypeInfo* FindOption(
      const std::string& opt_name,
      const std::unordered_map<std::string, OptionTypeInfo>& opt_map,
      std::string* elem_name);

  // Returns the next token marked by the delimiter from "opts" after start in
  // token and updates end to point to where that token stops. Delimiters inside
  // of braces are ignored. Returns OK if a token is found and an error if the
  // input opts string is mis-formated.
  static Status NextToken(const std::string& opts, char delimiter, size_t start,
                          size_t* end, std::string* token);

 private:
  // The offset of this option from the ptr
  int offset;

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

// Parses the input value into elements of the result vector,
// Separator defines the delimiter between vector elements.
// Elem_info specifies how to parse an individual element of the vector.
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
  copy.ignore_unknown_objects = false;

  for (size_t start = 0, end = 0;
       status.ok() && start < value.size() && end != std::string::npos;
       start = end + 1) {
    std::string token;
    status = OptionTypeInfo::NextToken(value, separator, start, &end, &token);
    if (status.ok()) {
      T elem;
      status = elem_info.ParseOption(copy, name, token,
                                     reinterpret_cast<char*>(&elem));
      if (status.ok()) {
        result->emplace_back(elem);
      } else if (config_options.ignore_unknown_objects &&
                 status.IsNotSupported()) {
        // If we were ignoring unknown objects and this one should be
        // ignored, ignore it by setting the status to OK
        status = Status::OK();
      }
    }
  }
  return status;
}

// Serializes the input vector into its output value.  Elements are
// separated by the separator character.
// Elem_info specifies how to serialize an individual element of the vector.
template <typename T>
Status SerializeVector(const ConfigOptions& config_options,
                       const OptionTypeInfo& elem_info, char separator,
                       const std::string& name, const std::vector<T>& vec,
                       std::string* value) {
  std::string result;
  ConfigOptions embedded = config_options.Embedded();
  for (size_t i = 0; i < vec.size(); ++i) {
    std::string elem_str;
    Status s = elem_info.SerializeOption(
        embedded, name, reinterpret_cast<const char*>(&vec[i]), &elem_str);
    if (!s.ok()) {
      return s;
    } else {
      if (i > 0) {
        result += separator;
      }
      result += elem_str;
    }
  }
  if (result.find("=") != std::string::npos) {
    *value = "{" + result + "}";
  } else {
    *value = result;
  }
  return Status::OK();
}

// Compares the input vectors
// Elem_info specifies how to compare an individual element of the vector.
template <typename T>
bool MatchesVector(const ConfigOptions& config_options,
                   const OptionTypeInfo& elem_info, const std::string& name,
                   const std::vector<T>& vec1, const std::vector<T>& vec2,
                   std::string* mismatch) {
  if (vec1.size() != vec2.size()) {
    *mismatch = name;
    return false;
  } else {
    for (size_t i = 0; i < vec1.size(); ++i) {
      if (!elem_info.MatchesOption(
              config_options, name, reinterpret_cast<const char*>(&vec1[i]),
              reinterpret_cast<const char*>(&vec2[i]), mismatch)) {
        return false;
      }
    }
    return true;
  }
}
}  // namespace ROCKSDB_NAMESPACE
