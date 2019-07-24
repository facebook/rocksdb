// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/configurable.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace rocksdb {

const std::string Configurable::kDefaultPrefix = "rocksdb.";
/*********************************************************************************/
/*                                                                               */
/*       Methods for Configuring Options from Strings/Name-Value Pairs/Maps */
/*                                                                               */
/*********************************************************************************/

#ifndef ROCKSDB_LITE
/**
 * Updates the object with the named-value property values, returning OK on
 * succcess. Any names properties that could not be found are returned in
 * used_opts. Returns OK if all of the properties were successfully updated.
 */
Status Configurable::DoConfigureFromMap(
    const DBOptions& db_opts,
    const std::unordered_map<std::string, std::string>& opts_map,
    bool input_strings_escaped, bool ignore_unused_options,
    std::unordered_set<std::string>* unused_opts) {
  Status s, result, invalid;
  bool found_one = false;
  std::unordered_set<std::string> invalid_opts;
  std::string curr_opts;
  if (!ignore_unused_options) {
    // If we are not ignoring unused, get the defaults in case we need to reset
    GetOptionString(&curr_opts, ";");
  }
  // Go through all of the values in the input map and attempt to configure the
  // property.
  for (const auto& o : opts_map) {
    s = ConfigureOption(db_opts, o.first, o.second, input_strings_escaped);
    if (s.ok()) {
      found_one = true;
    } else if (s.IsNotFound()) {
      result = s;
      unused_opts->insert(o.first);
    } else if (s.IsNotSupported()) {
      invalid_opts.insert(o.first);
    } else {
      invalid_opts.insert(o.first);
      invalid = s;
    }
  }
  // While there are unused properties and we processed at least one,
  // go through the remaining unused properties and attempt to configure them.
  while (found_one && !unused_opts->empty()) {
    result = Status::OK();
    found_one = false;
    for (auto it = unused_opts->begin(); it != unused_opts->end();) {
      s = ConfigureOption(db_opts, *it, opts_map.at(*it),
                          input_strings_escaped);
      if (s.ok()) {
        found_one = true;
        it = unused_opts->erase(it);
      } else if (s.IsNotFound()) {
        result = s;
        ++it;
      } else if (s.IsNotSupported()) {
        invalid_opts.insert(*it);
        it = unused_opts->erase(it);
      } else {
        invalid_opts.insert(*it);
        it = unused_opts->erase(it);
        invalid = s;
      }
    }
  }
  if (!invalid_opts.empty()) {
    unused_opts->insert(invalid_opts.begin(), invalid_opts.end());
  }
  if (ignore_unused_options || (invalid.ok() && result.ok())) {
    return Status::OK();
  } else {
    if (!curr_opts.empty()) {
      // There are some options to reset from this current error
      ConfigureFromString(db_opts, curr_opts, input_strings_escaped, true);
    }
    if (!invalid.ok()) {
      return invalid;
    } else {
      return result;
    }
  }
}

Status Configurable::ConfigureFromMap(
    const DBOptions& db_opts,
    const std::unordered_map<std::string, std::string>& opts_map,
    bool input_strings_escaped, bool ignore_unused_options) {
  std::unordered_set<std::string> unused_opts;
  Status s = DoConfigureFromMap(db_opts, opts_map, input_strings_escaped,
                                ignore_unused_options, &unused_opts);
  return s;
}

Status Configurable::ConfigureFromMap(
    const DBOptions& db_opts,
    const std::unordered_map<std::string, std::string>& opts_map,
    bool input_strings_escaped, std::unordered_set<std::string>* unused) {
  return DoConfigureFromMap(db_opts, opts_map, input_strings_escaped, true,
                            unused);
}

Status Configurable::ConfigureFromString(
    const DBOptions& db_opts, const std::string& opt_str,
    bool input_strings_escaped, std::unordered_set<std::string>* unused_opts) {
  std::unordered_map<std::string, std::string> opt_map;
  Status s = StringToMap(opt_str, &opt_map);
  if (s.ok()) {
    s = DoConfigureFromMap(db_opts, opt_map, input_strings_escaped, true,
                           unused_opts);
  }
  return s;
}
#endif  // ROCKSDB_LITE

Status Configurable::ConfigureFromString(const DBOptions& db_opts,
                                         const std::string& opts_str,
                                         bool input_strings_escaped,
                                         bool ignore_unused_options) {
  Status s;
  if (!opts_str.empty()) {
    if (opts_str.find(';') == std::string::npos &&
        opts_str.find('=') == std::string::npos) {
      return ParseStringOptions(db_opts, opts_str, input_strings_escaped,
                                ignore_unused_options);
    } else {
#ifndef ROCKSDB_LITE
      std::unordered_map<std::string, std::string> opt_map;
      std::unordered_set<std::string> unused_opts;

      s = StringToMap(opts_str, &opt_map);
      if (s.ok()) {
        s = DoConfigureFromMap(db_opts, opt_map, input_strings_escaped,
                               ignore_unused_options, &unused_opts);
      }
#else
      s = ParseStringOptions(db_opts, opts_str, input_strings_escaped,
                             ignore_unused_options);
#endif  // ROCKSDB_LITE
    }
  }
  return s;
}

/**
 * Sets the value of the named property to the input value, returning OK on
 * succcess.
 */
Status Configurable::ConfigureOption(const DBOptions& db_opts,
                                     const std::string& name,
                                     const std::string& value,
                                     bool input_strings_escaped) {
  const std::string& option_name = GetOptionName(name);
  const std::string& option_value =
      input_strings_escaped ? UnescapeOptionString(value) : value;
  bool found_it = false;
  Status status;
#ifndef ROCKSDB_LITE
  status = SetOption(db_opts, option_name, option_value, input_strings_escaped,
                     &found_it);
#endif
  if (!found_it) {
    status = SetUnknown(db_opts, option_name, option_value);
  }
  return status;
}

/**
 * Looks for the named option amongst the options for this type and sets
 * the value for it to be the input value.
 * If the name was found, found_option will be set to true and the resulting
 * status should be returned.
 */
#ifndef ROCKSDB_LITE
Status Configurable::SetOption(const DBOptions& db_opts,
                               const std::string& name,
                               const std::string& value,
                               bool input_strings_escaped, bool* found_option) {
  // By the time this method is called, the name is the short name (no prefix)
  // and the value is unescaped.
  Status status;
  *found_option = false;
  // Look for the name in all of the registered option maps until it is found
  for (auto iter : options_) {
    const auto& opt_map = iter.second.second;
    void* opt_ptr = iter.second.first;
    // Look up the value in the map
    auto opt_iter = FindOption(name, opt_map);
    if (opt_iter != opt_map.end()) {
      const auto& opt_info = opt_iter->second;
      *found_option = true;
      if (name == opt_iter->first) {
        status = ParseOption(opt_info, db_opts, opt_ptr, name, value,
                             input_strings_escaped);
      } else {
        status = ParseOption(opt_info, db_opts, opt_ptr,
                             name.substr(opt_iter->first.size() + 1), value,
                             input_strings_escaped);
      }
      if (status.IsNotFound()) {
        *found_option = false;
      }
      return status;
    } else {
      for (auto map_iter : opt_map) {
        if (map_iter.second.IsConfigurable()) {
          char* opt_addr = GetOptAddress(map_iter.second, opt_ptr);
          Configurable* config =
              map_iter.second.GetPointer<Configurable>(opt_addr);
          if (config != nullptr) {
            status = config->ConfigureOption(db_opts, name, value);
            if (!status.IsNotFound()) {
              *found_option = true;
              return status;
            }
          }
        }  // End if config option
      }
    }
  }
  return status;
}

Status Configurable::ParseOption(const OptionTypeInfo& opt_info,
                                 const DBOptions& db_opts, void* opt_ptr,
                                 const std::string& opt_name,
                                 const std::string& opt_value,
                                 bool input_strings_escaped) {
  Status status;
  if (opt_info.verification != OptionVerificationType::kDeprecated) {
    try {
      char* opt_addr = GetOptAddress(opt_info, opt_ptr);
      if (opt_addr == nullptr) {
        if (IsMutable()) {
          return Status::InvalidArgument("Option not changeable: " + opt_name);
        } else {
          status = Status::NotFound("Could not find property:", opt_name);
        }
      } else if (opt_info.pfunc != nullptr) {
        status = opt_info.pfunc(db_opts, opt_name, opt_addr, opt_value);
      } else if (ParseOptionHelper(opt_addr, opt_info, opt_value)) {
        status = Status::OK();
      } else if (opt_info.IsConfigurable()) {
        Configurable* config = opt_info.GetPointer<Configurable>(opt_addr);
        if (!opt_value.empty()) {
          if (config == nullptr) {
            status =
                Status::NotFound("Could not find configurable: ", opt_name);
          } else if (opt_value.find("=") != std::string::npos) {
            status = config->ConfigureFromString(db_opts, opt_value,
                                                 input_strings_escaped);
          } else {
            status = config->ConfigureOption(db_opts, opt_name, opt_value,
                                             input_strings_escaped);
          }
        }
      } else if (opt_info.IsEnum()) {
        status = SetEnum(db_opts, opt_name, opt_value, opt_addr);
      } else if (opt_info.type == OptionType::kUnknown) {
        status = SetUnknown(db_opts, opt_name, opt_value);
      } else if (opt_info.verification == OptionVerificationType::kByName ||
                 opt_info.verification ==
                     OptionVerificationType::kByNameAllowNull ||
                 opt_info.verification ==
                     OptionVerificationType::kByNameAllowFromNull) {
        status = Status::NotSupported("Deserializing the option " + opt_name +
                                      " is not supported");
      } else {
        status = Status::InvalidArgument("Error parsing:", opt_name);
      }
    } catch (std::exception& e) {
      status = Status::InvalidArgument("Error parsing " + opt_name + ":" +
                                       std::string(e.what()));
    }
  }
  return status;
}

#endif  // ROCKSDB_LITE

/*********************************************************************************/
/*                                                                               */
/*       Methods for Converting Options into strings */
/*                                                                               */
/*********************************************************************************/

#ifndef ROCKSDB_LITE
std::string Configurable::ToString(uint32_t mode, const std::string& prefix,
                                   const std::string& delimiter) const {
  std::string result = AsString(mode, prefix, delimiter);
  if (result.empty() || result.find('=') == std::string::npos) {
    return result;
  } else if ((mode & OptionStringMode::kOptionDetached) != 0) {
    return result;
  } else {
    return "{" + result + "}";
  }
}

std::string Configurable::AsString(uint32_t mode, const std::string& header,
                                   const std::string& delimiter) const {
  std::string result;
  Status s = SerializeOptions(mode, &result, header, delimiter);
  assert(s.ok());
  return result;
}

Status Configurable::GetOptionString(uint32_t mode, std::string* result,
                                     const std::string& delimiter) const {
  assert(result);
  result->clear();
  Status s = SerializeOptions(mode, result, "", delimiter);
  return s;
}

Status Configurable::GetOptionNames(
    uint32_t mode, std::unordered_set<std::string>* result) const {
  assert(result);
  return ListOptions(mode, "", result);
}

#endif  // ROCKSDB_LITE

Status Configurable::GetOption(const std::string& name,
                               std::string* value) const {
  assert(value);
  value->clear();
  Status status;
  bool found_it = false;
  const std::string& option_name = GetOptionName(name);

#ifndef ROCKSDB_LITE
  status = SerializeSingleOption(OptionStringMode::kOptionDetached, option_name,
                                 value, &found_it);
#endif
  if (!found_it) {
    status =
        UnknownToString(OptionStringMode::kOptionDetached, option_name, value);
  }
  return status;
}

#ifndef ROCKSDB_LITE
Status Configurable::SerializeOption(const std::string& opt_name,
                                     const OptionTypeInfo& opt_info,
                                     const char* opt_addr, uint32_t mode,
                                     std::string* opt_value,
                                     const std::string& prefix,
                                     const std::string& delimiter) const {
  // If the option is no longer used in rocksdb and marked as deprecated,
  // we skip it in the serialization.
  Status s;
  if (opt_addr != nullptr &&
      opt_info.verification != OptionVerificationType::kDeprecated) {
    if (opt_info.sfunc != nullptr) {
      s = opt_info.sfunc(mode, opt_name, opt_addr, opt_value);
    } else if (SerializeSingleOptionHelper(opt_addr, opt_info, opt_value)) {
      s = Status::OK();
    } else if (opt_info.IsConfigurable()) {
      const Configurable* config = opt_info.GetPointer<Configurable>(opt_addr);
      if (config != nullptr) {
        if ((mode & OptionStringMode::kOptionDetached) != 0) {
          *opt_value = config->ToString(mode, prefix, delimiter);
        } else {
          *opt_value = config->ToString(mode, "", "; ");
        }
      }
    } else if (opt_info.type == OptionType::kUnknown) {
      s = UnknownToString(mode, opt_name, opt_value);
    } else {
      s = Status::InvalidArgument("Cannot serialize option: ", opt_name);
    }
  }
  return s;
}

Status Configurable::SerializeOptions(uint32_t mode, std::string* result,
                                      const std::string& prefix,
                                      const std::string& delimiter) const {
  assert(result);
  auto header = ((mode & OptionStringMode::kOptionPrefix) != 0)
                    ? GetOptionsPrefix()
                    : prefix;
  for (auto const iter : options_) {
    const auto& opt_map = iter.second.second;
    void* opt_ptr = iter.second.first;
    for (auto opt_iter = opt_map.begin(); opt_iter != opt_map.end();
         ++opt_iter) {
      const auto& opt_name = opt_iter->first;
      const auto& opt_info = opt_iter->second;
      const char* opt_addr = GetOptAddress(opt_info, opt_ptr);
      if (opt_addr != nullptr &&
          opt_info.verification != OptionVerificationType::kAlias) {
        std::string value;
        Status s = SerializeOption(
            opt_name, opt_info, opt_addr, mode, &value,
            ((mode & OptionStringMode::kOptionDetached) != 0) ? opt_name + "."
                                                              : header,
            delimiter);
        if (!s.ok()) {
          return s;
        } else if (!opt_info.IsConfigurable()) {
          PrintSingleOption(header, opt_name, value, delimiter, result);
        } else if ((mode & OptionStringMode::kOptionDetached) != 0) {
          result->append(value);
        } else if (!value.empty()) {
          PrintSingleOption(header, opt_name, value, delimiter, result);
        }
      }
    }
  }
  return Status::OK();
}

Status Configurable::ListOptions(
    uint32_t mode, const std::string& prefix,
    std::unordered_set<std::string>* result) const {
  Status status;
  std::string header = ((mode & OptionStringMode::kOptionPrefix) != 0)
                           ? GetOptionsPrefix() + prefix
                           : prefix;
  for (auto const iter : options_) {
    const auto& opt_map = iter.second.second;
    void* opt_ptr = iter.second.first;
    for (auto opt_iter = opt_map.begin(); opt_iter != opt_map.end();
         ++opt_iter) {
      const auto& opt_name = opt_iter->first;
      const auto& opt_info = opt_iter->second;
      // If the option is no longer used in rocksdb and marked as deprecated,
      // we skip it in the serialization.
      const char* opt_addr = GetOptAddress(opt_info, opt_ptr);
      if (opt_addr != nullptr &&
          opt_info.verification != OptionVerificationType::kDeprecated &&
          opt_info.verification != OptionVerificationType::kAlias) {
        if (opt_info.IsConfigurable()) {
          const Configurable* config =
              opt_info.GetPointer<Configurable>(opt_addr);
          if (config != nullptr) {
            status = config->ListOptions(mode | OptionStringMode::kOptionPrefix,
                                         prefix, result);
          }
        } else {
          result->emplace(header + opt_name);
        }
      }
      if (!status.ok()) {
        return status;
      }
    }
  }
  return status;
}

Status Configurable::SerializeSingleOption(uint32_t mode,
                                           const std::string& name,
                                           std::string* value,
                                           bool* found_it) const {
  assert(value);
  *found_it = false;
  // By the time this method is called, the name is the short name (no prefix)
  // and the value is unescaped.
  // Look for the name in all of the registered option maps until it is found
  for (auto iter : options_) {
    const auto& opt_map = iter.second.second;
    void* opt_ptr = iter.second.first;
    // Look up the value in the map
    auto opt_iter = FindOption(name, opt_map);
    if (opt_iter != opt_map.end()) {
      const auto& opt_info = opt_iter->second;
      *found_it = true;
      const char* opt_addr = GetOptAddress(opt_info, opt_ptr);
      if (opt_addr != nullptr) {
        if (name == opt_iter->first) {
          return SerializeOption(name, opt_info, opt_addr, mode, value, "",
                                 ";");
        } else if (opt_info.IsConfigurable()) {
          auto const* config = opt_info.GetPointer<Configurable>(opt_addr);
          if (config != nullptr) {
            return config->GetOption(name.substr(opt_iter->first.size() + 1),
                                     value);
          }
        }
      }
      return Status::NotFound("Cannot find option: ", name);
    } else {
      for (auto const map_iter : opt_map) {
        if (map_iter.second.IsConfigurable()) {
          const char* opt_addr = GetOptAddress(map_iter.second, opt_ptr);
          auto const* config =
              map_iter.second.GetPointer<Configurable>(opt_addr);
          if (config != nullptr) {
            Status status = config->GetOption(name, value);
            if (!status.IsNotFound()) {
              *found_it = true;
              return status;
            }
          }
        }
      }
    }
  }
  return Status::NotFound("Could not find option: ", name);
}
#endif  // ROCKSDB_LITE

#ifndef ROCKSDB_LITE
void Configurable::RegisterOptionsMap(const std::string& name, void* opt_ptr,
                                      const OptionTypeMap& opt_map) {
  options_[name] = std::pair<void*, OptionTypeMap>(opt_ptr, opt_map);
}
#else
void Configurable::RegisterOptionsMap(const std::string& name, void* opt_ptr,
                                      const OptionTypeMap&) {
  options_[name] = opt_ptr;
}
#endif  // ROCKSDB_LITE

void Configurable::PrintSingleOption(const std::string& prefix,
                                     const std::string& name,
                                     const std::string& value,
                                     const std::string& delimiter,
                                     std::string* result) const {
  result->append(prefix);
  result->append(name);  // Add the name
  result->append("=");
  result->append(value);
  result->append(delimiter);
}

/*********************************************************************************/
/*                                                                               */
/*       Methods for Validating and Sanitizing Configurables */
/*                                                                               */
/*********************************************************************************/

Status Configurable::SanitizeOptions() {
  Options opts;
  return Sanitize(opts, opts);
}

Status Configurable::SanitizeOptions(DBOptions& db_opts) {
  ColumnFamilyOptions cf_opts;
  return Sanitize(db_opts, cf_opts);
}

Status Configurable::SanitizeOptions(DBOptions& db_opts,
                                     ColumnFamilyOptions& cf_opts) {
  Status status = Sanitize(db_opts, cf_opts);
  if (status.ok()) {
    status = Validate(db_opts, cf_opts);
  }
  return status;
}

#ifndef ROCKSDB_LITE
Status Configurable::Sanitize(DBOptions& db_opts,
                              ColumnFamilyOptions& cf_opts) {
  Status status;
  for (auto opt_iter : options_) {
    const auto opt_map = opt_iter.second.second;
    for (auto map_iter = opt_map.begin(); map_iter != opt_map.end();
         ++map_iter) {
      auto& opt_info = map_iter->second;
      if (opt_info.verification != OptionVerificationType::kDeprecated &&
          opt_info.verification != OptionVerificationType::kAlias) {
        if (opt_info.IsConfigurable()) {
          char* opt_addr = GetOptAddress(opt_info, opt_iter.second.first);
          Configurable* config = opt_info.GetPointer<Configurable>(opt_addr);
          if (config != nullptr) {
            status = config->Sanitize(db_opts, cf_opts);
          } else if (opt_info.verification !=
                     OptionVerificationType::kByNameAllowFromNull) {
            status = Status::NotFound("Missing configurable object",
                                      map_iter->first);
          }
          if (!status.ok()) {
            return status;
          }
        }
      }
    }
  }
  return status;
}
#else
Status Configurable::Sanitize(DBOptions&, ColumnFamilyOptions&) {
  return Status::OK();
}
#endif  // ROCKSDB_LITE

Status Configurable::ValidateOptions() const {
  Options opts;
  return ValidateOptions(opts, opts);
}

Status Configurable::ValidateOptions(const DBOptions& db_opts) const {
  ColumnFamilyOptions cf_opts;
  return Validate(db_opts, cf_opts);
}

Status Configurable::ValidateOptions(const DBOptions& db_opts,
                                     const ColumnFamilyOptions& cf_opts) const {
  return Validate(db_opts, cf_opts);
}

#ifndef ROCKSDB_LITE
Status Configurable::Validate(const DBOptions& db_opts,
                              const ColumnFamilyOptions& cf_opts) const {
  Status status;
  for (auto opt_iter : options_) {
    const auto opt_map = opt_iter.second.second;
    for (auto map_iter = opt_map.begin(); map_iter != opt_map.end();
         ++map_iter) {
      auto& opt_info = map_iter->second;
      if (opt_info.verification != OptionVerificationType::kDeprecated &&
          opt_info.verification != OptionVerificationType::kAlias) {
        if (opt_info.IsConfigurable()) {
          const char* opt_addr = GetOptAddress(opt_info, opt_iter.second.first);
          const Configurable* config =
              opt_info.GetPointer<Configurable>(opt_addr);
          if (config != nullptr) {
            status = config->Validate(db_opts, cf_opts);
          } else if (opt_info.verification !=
                     OptionVerificationType::kByNameAllowFromNull) {
            status = Status::NotFound("Missing configurable object",
                                      map_iter->first);
          }
          if (!status.ok()) {
            return status;
          }
        }
      }
    }
  }
  return status;
}
#else
Status Configurable::Validate(const DBOptions&,
                              const ColumnFamilyOptions&) const {
  return Status::OK();
}
#endif  // ROCKSDB_LITE

/*********************************************************************************/
/*                                                                               */
/*       Methods for Comparing Configurables */
/*                                                                               */
/*********************************************************************************/

bool Configurable::Matches(const Configurable* other,
                           OptionsSanityCheckLevel level) const {
  std::string name;
  return Matches(other, level, &name);
}

bool Configurable::Matches(const Configurable* other,
                           OptionsSanityCheckLevel level,
                           std::string* name) const {
  assert(name);
  name->clear();
  if (this == other || level == OptionsSanityCheckLevel::kSanityLevelNone) {
    return true;
  } else if (other != nullptr) {
    return MatchesOption(other, level, name);
  } else {
    return false;
  }
}

bool Configurable::MatchesOption(const Configurable* that,
                                 OptionsSanityCheckLevel level,
                                 std::string* name) const {
  assert(name != nullptr);
  if (this == that || level == OptionsSanityCheckLevel::kSanityLevelNone) {
    return true;
  } else {
#ifndef ROCKSDB_LITE
    for (auto const iter : options_) {
      const char* this_option =
          reinterpret_cast<const char*>(iter.second.first);
      const char* that_option = that->GetOptions<const char>(iter.first);
      if (!OptionsAreEqual(iter.second.second,
                           GetOptionsSanityCheckLevel(iter.first), level,
                           this_option, that_option, name)) {
        return false;
      }
    }
    return true;
#endif  // ROCKSDB_LITE
    return false;
  }
}

OptionsSanityCheckLevel Configurable::GetSanityLevelForOption(
    const std::unordered_map<std::string, OptionsSanityCheckLevel>* map,
    const std::string& name) const {
  if (map != nullptr) {
    auto iter = map->find(name);
    if (iter != map->end()) {
      return iter->second;
    }
  }
  return OptionsSanityCheckLevel::kSanityLevelExactMatch;
}

#ifndef ROCKSDB_LITE
bool Configurable::VerifyOptionEqual(const std::string& opt_name,
                                     const OptionTypeInfo& opt_info,
                                     const char* this_offset,
                                     const char* that_offset) const {
  std::string this_value, that_value;

  OptionStringMode mode = OptionStringMode::kOptionNone;
  if (opt_info.verification != OptionVerificationType::kByName &&
      opt_info.verification != OptionVerificationType::kByNameAllowNull &&
      opt_info.verification != OptionVerificationType::kByNameAllowFromNull) {
    return false;
  } else if (opt_info.sfunc != nullptr) {
    if (!opt_info.sfunc(mode, opt_name, this_offset, &this_value).ok() ||
        !opt_info.sfunc(mode, opt_name, that_offset, &that_value).ok()) {
      return false;
    }
  } else if (!SerializeOption(opt_name, opt_info, this_offset, mode,
                              &this_value, "", ";")
                  .ok() ||
             !SerializeOption(opt_name, opt_info, that_offset, mode,
                              &that_value, "", ";")
                  .ok()) {
    return false;
  }
  if (opt_info.verification == OptionVerificationType::kByNameAllowFromNull &&
      that_value == kNullptrString) {
    return true;
  } else if (opt_info.verification ==
                 OptionVerificationType::kByNameAllowNull &&
             this_value == kNullptrString) {
    return true;
  } else if (this_value != that_value) {
    return false;
  } else {
    return true;
  }
}

bool Configurable::IsConfigEqual(const std::string& /* opt_name */,
                                 const OptionTypeInfo& /* opt_info */,
                                 OptionsSanityCheckLevel level,
                                 const Configurable* this_config,
                                 const Configurable* that_config,
                                 std::string* mismatch) const {
  if (this_config == that_config) {
    return true;
  } else if (this_config != nullptr && that_config != nullptr) {
    return this_config->Matches(that_config, level, mismatch);
  } else {
    return false;
  }
}

bool Configurable::OptionIsEqual(const std::string& opt_name,
                                 const OptionTypeInfo& opt_info,
                                 OptionsSanityCheckLevel level,
                                 const char* this_offset,
                                 const char* that_offset,
                                 std::string* bad_name) const {
  if (this_offset == nullptr || that_offset == nullptr) {
    if (this_offset != that_offset) {
      *bad_name = opt_name;
      return false;
    }
  } else if (opt_info.efunc != nullptr) {
    return opt_info.efunc(level, opt_name, this_offset, that_offset);
  } else if (IsOptionEqual(level, opt_info, this_offset, that_offset)) {
    return true;
  } else if (opt_info.type == OptionType::kUnknown) {
    if (!IsUnknownEqual(opt_name, opt_info, level, this_offset, that_offset)) {
      *bad_name = opt_name;
      return false;
    }
  } else if (opt_info.IsEnum()) {
    if (!IsEnumEqual(opt_name, opt_info, level, this_offset, that_offset)) {
      *bad_name = opt_name;
      return false;
    }
  } else if (opt_info.IsConfigurable()) {
    std::string mismatch;
    const auto* this_config = opt_info.GetPointer<Configurable>(this_offset);
    const auto* that_config = opt_info.GetPointer<Configurable>(that_offset);
    if (!IsConfigEqual(opt_name, opt_info, level, this_config, that_config,
                       &mismatch)) {
      *bad_name = opt_name;
      if (!mismatch.empty()) {
        bad_name->append("." + mismatch);
      }
      return false;
    }
  } else {
    *bad_name = opt_name;
    return false;
  }
  return true;
}

bool Configurable::OptionsAreEqual(
    const OptionTypeMap& opt_map,
    const std::unordered_map<std::string, OptionsSanityCheckLevel>* opt_level,
    OptionsSanityCheckLevel sanity_check_level, const char* this_option,
    const char* that_option, std::string* bad_name) const {
  *bad_name = "";
  if (this_option == that_option) {
    return true;
  } else if (this_option == nullptr || that_option == nullptr) {
    return false;
  } else {
    for (auto& pair : opt_map) {
      const auto& opt_info = pair.second;
      // We skip checking deprecated variables as they might
      // contain random values since they might not be initialized
      if (opt_info.verification != OptionVerificationType::kDeprecated &&
          opt_info.verification != OptionVerificationType::kAlias) {
        OptionsSanityCheckLevel level =
            GetSanityLevelForOption(opt_level, pair.first);
        if (level > OptionsSanityCheckLevel::kSanityLevelNone &&
            level <= sanity_check_level) {
          const char* this_offset = GetOptAddress(opt_info, this_option);
          const char* that_offset = GetOptAddress(opt_info, that_option);
          if (!OptionIsEqual(pair.first, opt_info, level, this_offset,
                             that_offset, bad_name) &&
              !VerifyOptionEqual(pair.first, opt_info, this_offset,
                                 that_offset)) {
            return false;
          }
        }
      }
    }
    return true;
  }
}
#endif  // ROCKSDB_LITE

/*********************************************************************************/
/*                                                                               */
/*       Methods for Retrieving Options from Configurables */
/*                                                                               */
/*********************************************************************************/

const void* Configurable::GetOptionsPtr(const std::string& name) const {
  const auto& iter = options_.find(name);
  if (iter != options_.end()) {
#ifndef ROCKSDB_LITE
    return iter->second.first;
#else
    return iter->second;
#endif  // ROCKSDB_LITE
  }
  return nullptr;
}

/*********************************************************************************/
/*                                                                               */
/*       Methods for Logging Configurables */
/*                                                                               */
/*********************************************************************************/

void Configurable::DumpOptions(Logger* logger, const std::string& indent,
#ifndef ROCKSDB_LITE
                               uint32_t mode
#else
                               uint32_t
#endif  // ROCKSDB_LITE
                               ) const {
  std::string value = GetPrintableOptions();
  if (!value.empty()) {
    ROCKS_LOG_HEADER(logger, "%s Options: %s\n", indent.c_str(), value.c_str());
  } else {
#ifndef ROCKSDB_LITE
    for (auto const iter : options_) {
      std::string header = iter.first;
      if ((mode & OptionStringMode::kOptionPrefix) != 0) {
        header.append("::" + GetOptionsPrefix());
      } else {
        header.append(".");
      }
      const auto& opt_map = iter.second.second;
      const void* opt_ptr = iter.second.first;

      for (auto opt_iter = opt_map.begin(); opt_iter != opt_map.end();
           ++opt_iter) {
        auto& opt_name = opt_iter->first;
        auto& opt_info = opt_iter->second;
        // If the option is no longer used in rocksdb and marked as deprecated,
        // we skip it in the serialization.
        const char* opt_addr = GetOptAddress(opt_info, opt_ptr);
        if (opt_addr != nullptr &&
            opt_info.verification != OptionVerificationType::kAlias) {
          if (opt_info.IsConfigurable() &&
              (mode & OptionStringMode::kOptionDetached) != 0) {
            // Nested options on individual lines
            const Configurable* config =
                opt_info.GetPointer<Configurable>(opt_addr);
            if (config != nullptr) {
              ROCKS_LOG_HEADER(logger, "%s%s%s Options:\n", indent.c_str(),
                               header.c_str(), opt_name.c_str());
              config->Dump(logger, indent + "  ", mode);
            }
          } else {
            Status s = SerializeOption(opt_name, opt_info, opt_addr, mode,
                                       &value, "", ";");
            if (s.ok()) {
              ROCKS_LOG_HEADER(logger, "%s%s%s: %s\n", indent.c_str(),
                               header.c_str(), opt_name.c_str(), value.c_str());
            }
          }
        }
      }
    }
#endif  // ROCKSDB_LITE
  }
}

/*********************************************************************************/
/*                                                                               */
/*       Configurable Support and Utility Methods */
/*                                                                               */
/*********************************************************************************/

std::string Configurable::GetOptionName(const std::string& long_name) const {
  auto& prefix = GetOptionsPrefix();
  auto prefix_len = prefix.length();
  if (long_name.compare(0, prefix_len, prefix) == 0) {
    return long_name.substr(prefix_len);
  } else {
    return long_name;
  }
}

#ifndef ROCKSDB_LITE

OptionTypeMap::const_iterator Configurable::FindOption(
    const std::string& option, const OptionTypeMap& options_map) const {
  auto iter = options_map.find(option);  // Look up the value in the map
  if (iter == options_map.end()) {       // Didn't find the option in the map
    auto idx = option.find(".");         // Look for a separator
    if (idx > 0 && idx != std::string::npos) {  // We found a separator
      auto siter =
          options_map.find(option.substr(0, idx));  // Look for the short name
      if (siter != options_map.end() &&             // We found the short name
          siter->second.IsConfigurable()) {  // and the object is a configurable
        return siter;                        // Return the short name value
      }
    }
  }
  return iter;
}
#endif  // ROCKSDB_LITE

}  // namespace rocksdb
