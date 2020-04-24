// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/configurable.h"

#include "logging/logging.h"
#include "options/options_helper.h"
#include "rocksdb/customizable.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

void Configurable::RegisterOptions(
    const std::string& name, void* opt_ptr,
    const std::unordered_map<std::string, OptionTypeInfo>* type_map) {
  RegisteredOptions opts;
  opts.name = name;
#ifndef ROCKSDB_LITE
  opts.type_map = type_map;
#else
  (void)type_map;
#endif  // ROCKSDB_LITE
  opts.opt_ptr = opt_ptr;
  options_.emplace_back(opts);
}

/*********************************************************************************/
/*                                                                               */
/*       Methods for Configuring Options from Strings/Name-Value Pairs/Maps */
/*                                                                               */
/*********************************************************************************/
Status Configurable::PrepareOptions(const ConfigOptions& opts) {
  Status status;
#ifndef ROCKSDB_LITE
  for (auto opt_iter : options_) {
    for (auto map_iter : *(opt_iter.type_map)) {
      auto& opt_info = map_iter.second;
      if (!opt_info.IsDeprecated() && !opt_info.IsAlias() &&
          opt_info.IsConfigurable()) {
        if (!opt_info.IsEnabled(OptionTypeFlags::kDontPrepare)) {
          Configurable* config =
              opt_info.AsRawPointer<Configurable>(opt_iter.opt_ptr);
          if (config != nullptr) {
            status = config->PrepareOptions(opts);
            if (!status.ok()) {
              return status;
            }
          }
        }
      }
    }
  }
#endif  // ROCKSDB_LITE
  if (status.ok()) {
    auto inner = Inner();
    if (inner != nullptr) {
      status = inner->PrepareOptions(opts);
    }
  }
  return status;
}

Status Configurable::ValidateOptions(const DBOptions& db_opts,
                                     const ColumnFamilyOptions& cf_opts) const {
  Status status;
#ifndef ROCKSDB_LITE
  for (auto opt_iter : options_) {
    for (auto map_iter : *(opt_iter.type_map)) {
      auto& opt_info = map_iter.second;
      if (!opt_info.IsDeprecated() && !opt_info.IsAlias()) {
        if (opt_info.IsConfigurable()) {
          const Configurable* config =
              opt_info.AsRawPointer<Configurable>(opt_iter.opt_ptr);
          if (config != nullptr) {
            status = config->ValidateOptions(db_opts, cf_opts);
          } else if (!opt_info.CanBeNull()) {
            status =
                Status::NotFound("Missing configurable object", map_iter.first);
          }
          if (!status.ok()) {
            return status;
          }
        }
      }
    }
  }
#endif  // ROCKSDB_LITE
  if (status.ok()) {
    const auto inner = Inner();
    if (inner != nullptr) {
      status = inner->ValidateOptions(db_opts, cf_opts);
    }
  }
  return status;
}

#ifndef ROCKSDB_LITE
Status Configurable::DoConfigureOption(const ConfigOptions& config_options,
                                       const OptionTypeInfo* opt_info,
                                       const std::string& short_name,
                                       const std::string& opt_name,
                                       const std::string& opt_value,
                                       void* opt_ptr) {
  Status s;
  if (opt_info == nullptr) {
    s = Status::NotFound("Could not find option: ", short_name);
  } else if (opt_name == short_name) {
    s = ParseOption(config_options, *opt_info, opt_name, opt_value, opt_ptr);
  } else if (opt_info->IsCustomizable() &&
             EndsWith(short_name, kIdPropSuffix)) {
    s = ParseOption(config_options, *opt_info, opt_name, opt_value, opt_ptr);
  } else if (opt_info->IsStruct()) {
    s = ParseOption(config_options, *opt_info, opt_name, opt_value, opt_ptr);
  } else if (opt_info->IsCustomizable()) {
    Customizable* custom = opt_info->AsRawPointer<Customizable>(opt_ptr);
    if (opt_value.empty()) {
      s = Status::OK();
    } else if (custom == nullptr ||
               !StartsWith(opt_name, custom->GetId() + ".")) {
      s = Status::NotFound("Could not find customizable option: ", short_name);
    } else if (opt_value.find("=") != std::string::npos) {
      s = custom->ConfigureFromString(config_options, opt_value);
    } else {
      s = custom->ConfigureOption(config_options, opt_name, opt_value);
    }
  } else if (opt_info->IsConfigurable()) {
    s = ParseOption(config_options, *opt_info, opt_name, opt_value, opt_ptr);
  }
  return s;
}

/**
 * Updates the object with the named-value property values, returning OK on
 * succcess. Any properties that were found are removed from the options list;
 * upon return only options that were not found in this opt_map remain.
 * Returns OK if the properties were successfully updated, N
 */
Status Configurable::DoConfigureOptions(
    const ConfigOptions& config_options, const std::string& /*type_name*/,
    const std::unordered_map<std::string, OptionTypeInfo>& type_map,
    std::unordered_map<std::string, std::string>* options, void* opt_ptr) {
  Status s, not_found, invalid;
  int found = 1;
  std::unordered_set<std::string> unsupported;
  // While there are unused properties and we processed at least one,
  // go through the remaining unused properties and attempt to configure them.
  while (found > 0 && !options->empty()) {
    not_found = Status::OK();
    found = 0;
    std::string elem_name;
    for (auto it = options->begin(); it != options->end();) {
      const std::string& opt_name = GetOptionName(it->first);
      const std::string& opt_value = it->second;
      const auto opt_info =
          OptionTypeInfo::FindOption(opt_name, type_map, &elem_name);
      s = DoConfigureOption(config_options, opt_info, opt_name, elem_name,
                            opt_value, opt_ptr);
      if (s.IsNotFound()) {
        not_found = s;
        ++it;
      } else {
        found++;
        if (s.ok()) {
          it = options->erase(it);
        } else if (s.IsNotSupported()) {
          unsupported.insert(it->first);
          found--;  // Saw this one before, don't count it
          ++it;     // Skip it for now
          if (!config_options.ignore_unknown_objects && not_found.ok()) {
            // If we are not ignoring unknown objects and this
            // is the first error, save the status
            not_found = s;
          }
        } else {
          unsupported.insert(it->first);
          it = options->erase(it);
          invalid = s;
        }
      }
    }  // End for all remaining options
  }    // End while found one or options remain

  // Now that we have been through the list, remove any unsupported
  for (auto u : unsupported) {
    auto it = options->find(u);
    if (it != options->end()) {
      options->erase(it);
    }
  }
  if (!invalid.ok()) {
    return invalid;
  } else {
    return not_found;
  }
}

Status Configurable::DoConfigureFromMap(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    std::unordered_map<std::string, std::string>* unused) {
  std::unordered_map<std::string, std::string> remaining = opts_map;
  Status s;
  Status not_found, invalid;
  for (const auto iter : options_) {
    s = DoConfigureOptions(config_options, iter.name, *iter.type_map,
                           &remaining, iter.opt_ptr);
    if (remaining.empty()) {  // Are there more options left?
      break;
    } else if (!config_options.ignore_unknown_options && !s.IsNotFound()) {
      break;
    }
  }
  if (unused != nullptr && !remaining.empty()) {
    unused->insert(remaining.begin(), remaining.end());
  }
  if (config_options.ignore_unknown_options && s.IsNotFound()) {
    s = Status::OK();
  }
  return s;
}

Status Configurable::ConfigureFromString(
    const ConfigOptions& config_options, const std::string& opt_str,
    std::unordered_map<std::string, std::string>* unused_opts) {
  std::unordered_map<std::string, std::string> opt_map;
  Status s = StringToMap(opt_str, &opt_map);
  if (s.ok()) {
    s = ConfigureFromMap(config_options, opt_map, unused_opts);
  }
  return s;
}
#endif  // ROCKSDB_LITE

Status Configurable::ConfigureFromMap(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, std::string>& opts_map) {
  Status s = ConfigureFromMap(config_options, opts_map, nullptr);
  return s;
}

Status Configurable::ConfigureFromMap(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    std::unordered_map<std::string, std::string>* unused) {
  Status s;
  std::string curr_opts;
#ifndef ROCKSDB_LITE
  if (!config_options.ignore_unknown_options) {
    // If we are not ignoring unused, get the defaults in case we need to reset
    GetOptionString(config_options, &curr_opts);
  }
#endif  // ROCKSDB_LITE
  if (!opts_map.empty()) {
#ifndef ROCKSDB_LITE
    s = DoConfigureFromMap(config_options, opts_map, unused);
#else
    if (!config_options.ignore_unknown_options) {
      (void)unused;
      s = Status::NotSupported("ConfigureFromMap not supported in LITE mode");
    }
#endif  // ROCKSDB_LITE
  }
  if (config_options.invoke_prepare_options && s.ok()) {
    s = PrepareOptions(config_options);
  }
#ifndef ROCKSDB_LITE
  if (!s.ok() && !curr_opts.empty()) {
    ConfigOptions reset = config_options;
    reset.ignore_unknown_options = true;
    reset.invoke_prepare_options = true;
    // There are some options to reset from this current error
    ConfigureFromString(reset, curr_opts);
  }
#endif  // ROCKSDB_LITE
  return s;
}

Status Configurable::ParseStringOptions(const ConfigOptions& config_options,
                                        const std::string& opts_str) {
  if (!opts_str.empty()) {
    return Status::InvalidArgument("Cannot parse option: ", opts_str);
  } else if (config_options.invoke_prepare_options) {
    return PrepareOptions(config_options);
  } else {
    return Status::OK();
  }
}
Status Configurable::ConfigureFromString(const ConfigOptions& config_options,
                                         const std::string& opts_str) {
  Status s;
  if (!opts_str.empty()) {
    if (opts_str.find(';') == std::string::npos &&
        opts_str.find('=') == std::string::npos) {
      return ParseStringOptions(config_options, opts_str);
    } else {
#ifndef ROCKSDB_LITE
      std::unordered_map<std::string, std::string> opt_map;

      s = StringToMap(opts_str, &opt_map);
      if (s.ok()) {
        s = ConfigureFromMap(config_options, opt_map, nullptr);
      }
#else
      s = ParseStringOptions(config_options, opts_str);
#endif  // ROCKSDB_LITE
    }
  }
  return s;
}

/**
 * Sets the value of the named property to the input value, returning OK on
 * succcess.
 */
Status Configurable::ConfigureOption(const ConfigOptions& config_options,
                                     const std::string& name,
                                     const std::string& value) {
  const std::string& opt_name = GetOptionName(name);
  std::string elem_name;
#ifndef ROCKSDB_LITE
  Status status;
  // Look for the name in all of the registered option maps until it is found
  for (const auto& iter : options_) {
    const auto opt_info =
        OptionTypeInfo::FindOption(opt_name, *(iter.type_map), &elem_name);
    if (opt_info != nullptr) {
      return DoConfigureOption(config_options, opt_info, opt_name, elem_name,
                               value, iter.opt_ptr);
    }
  }
#endif
  return SetUnknown(config_options, opt_name, value);
}

/**
 * Looks for the named option amongst the options for this type and sets
 * the value for it to be the input value.
 * If the name was found, found_option will be set to true and the resulting
 * status should be returned.
 */
#ifndef ROCKSDB_LITE
Status Configurable::ParseOption(const ConfigOptions& config_options,
                                 const OptionTypeInfo& opt_info,
                                 const std::string& opt_name,
                                 const std::string& opt_value, void* opt_ptr) {
  return opt_info.ParseOption(config_options, opt_name, opt_value, opt_ptr);
}

Status Configurable::SerializeOption(const ConfigOptions& config_options,
                                     const OptionTypeInfo& opt_info,
                                     const std::string& opt_name,
                                     const void* const opt_ptr,
                                     std::string* opt_value) const {
  return opt_info.SerializeOption(config_options, opt_name, opt_ptr, opt_value);
}

bool Configurable::MatchesOption(const ConfigOptions& config_options,
                                 const OptionTypeInfo& opt_info,
                                 const std::string& opt_name,
                                 const void* const this_ptr,
                                 const void* const that_ptr,
                                 std::string* mismatch) const {
  return opt_info.MatchesOption(config_options, opt_name, this_ptr, that_ptr,
                                mismatch);
}

bool Configurable::CheckByName(const ConfigOptions& config_options,
                               const OptionTypeInfo& opt_info,
                               const std::string& opt_name,
                               const void* const this_ptr,
                               const void* const that_ptr) const {
  return opt_info.CheckByName(config_options, opt_name, this_ptr, that_ptr);
}
#endif  // ROCKSDB_LITE

/*********************************************************************************/
/*                                                                               */
/*       Methods for Converting Options into strings */
/*                                                                               */
/*********************************************************************************/

Status Configurable::GetOptionString(const ConfigOptions& config_options,
                                     std::string* result) const {
  assert(result);
  result->clear();
  Status s;
#ifndef ROCKSDB_LITE
  s = DoSerializeOptions(config_options, "", result);
#else
  (void)config_options;
#endif  // ROCKSDB_LITE
  return s;
}

#ifndef ROCKSDB_LITE
std::string Configurable::ToString(const ConfigOptions& config_options,
                                   const std::string& prefix) const {
  std::string result = AsString(config_options, prefix);
  if (result.empty() || result.find('=') == std::string::npos) {
    return result;
  } else {
    return "{" + result + "}";
  }
}

std::string Configurable::AsString(const ConfigOptions& config_options,
                                   const std::string& header) const {
  std::string result;
  Status s = DoSerializeOptions(config_options, header, &result);
  assert(s.ok());
  return result;
}

Status Configurable::GetOptionNames(
    const ConfigOptions& config_options,
    std::unordered_set<std::string>* result) const {
  assert(result);
  return DoListOptions(config_options, "", result);
}

Status Configurable::DoListOptions(
    const ConfigOptions& /*options*/, const std::string& prefix,
    std::unordered_set<std::string>* result) const {
  Status status;
  for (auto const& iter : options_) {
    for (const auto& map_iter : *(iter.type_map)) {
      const auto& opt_name = map_iter.first;
      const auto& opt_info = map_iter.second;
      // If the option is no longer used in rocksdb and marked as deprecated,
      // we skip it in the serialization.
      if (!opt_info.IsDeprecated() && !opt_info.IsAlias()) {
        result->emplace(prefix + opt_name);
      }
    }
  }
  return status;
}

Status Configurable::GetOption(const ConfigOptions& config_options,
                               const std::string& name,
                               std::string* value) const {
  assert(value);
  value->clear();

  return DoGetOption(config_options.Embedded(), GetOptionName(name), value);
}

Status Configurable::DoGetOption(const ConfigOptions& config_options,
                                 const std::string& short_name,
                                 std::string* value) const {
  // Look for option directly
  for (auto iter : options_) {
    // Look up the value in the map
    std::string opt_name;
    const auto opt_info =
        OptionTypeInfo::FindOption(short_name, *(iter.type_map), &opt_name);
    if (opt_info != nullptr) {
      if (short_name == opt_name) {
        return SerializeOption(config_options, *opt_info, opt_name,
                               iter.opt_ptr, value);
      } else if (opt_info->IsStruct()) {
        return SerializeOption(config_options, *opt_info, opt_name,
                               iter.opt_ptr, value);
      } else if (opt_info->IsConfigurable()) {
        auto const* config = opt_info->AsRawPointer<Configurable>(iter.opt_ptr);
        if (config != nullptr) {
          return config->GetOption(config_options, opt_name, value);
        }
      }
    }
  }
  return Status::NotFound("Cannot find option: ", short_name);
}

Status Configurable::SerializeOptions(
    const ConfigOptions& config_options, const std::string& prefix,
    const std::unordered_map<std::string, OptionTypeInfo>& type_map,
    const void* opt_ptr, std::string* result) const {
  for (auto opt_iter : type_map) {
    const auto& opt_name = opt_iter.first;
    const auto& opt_info = opt_iter.second;
    if (opt_info.ShouldSerialize()) {
      std::string value;
      Status s = SerializeOption(config_options, opt_info, prefix + opt_name,
                                 opt_ptr, &value);
      if (!s.ok()) {
        return s;
      } else if (!value.empty()) {
        // <prefix><opt_name>=<value><delimiter>
        result->append(prefix + opt_name + "=" + value +
                       config_options.delimiter);
      }
    }
  }

  return Status::OK();
}

Status Configurable::DoSerializeOptions(const ConfigOptions& config_options,
                                        const std::string& prefix,
                                        std::string* result) const {
  assert(result);
  for (auto const iter : options_) {
    Status s = SerializeOptions(config_options, prefix, *(iter.type_map),
                                iter.opt_ptr, result);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

#endif  // ROCKSDB_LITE

/*********************************************************************************/
/*                                                                               */
/*       Methods for Comparing Configurables */
/*                                                                               */
/*********************************************************************************/

bool Configurable::Matches(const ConfigOptions& config_options,
                           const Configurable* other) const {
  std::string name;
  return Matches(config_options, other, &name);
}

bool Configurable::Matches(const ConfigOptions& config_options,
                           const Configurable* other, std::string* name) const {
  assert(name);
  name->clear();
  if (this == other || config_options.IsCheckDisabled()) {
    return true;
  } else if (other != nullptr) {
    return DoMatchesOptions(config_options, other, name);
  } else {
    return false;
  }
}

bool Configurable::DoMatchesOptions(const ConfigOptions& config_options,
                                    const Configurable* that,
                                    std::string* mismatch) const {
  assert(mismatch != nullptr);
  assert(that);
#ifndef ROCKSDB_LITE
  for (auto const o : options_) {
    const auto this_offset = this->GetOptionsPtr(o.name);
    const auto that_offset = that->GetOptionsPtr(o.name);
    if (this_offset == nullptr || that_offset == nullptr) {
    } else if (this_offset != that_offset) {
      for (const auto map_iter : *(o.type_map)) {
        if (config_options.IsCheckEnabled(map_iter.second.GetSanityLevel())) {
          if (!MatchesOption(config_options, map_iter.second, map_iter.first,
                             this_offset, that_offset, mismatch) &&
              !CheckByName(config_options, map_iter.second, map_iter.first,
                           this_offset, that_offset)) {
            return false;
          }
        }
      }
    }
  }
#else
  (void)config_options;
#endif  // ROCKSDB_LITE
  return true;
}

/*********************************************************************************/
/*                                                                               */
/*       Methods for Retrieving Options from Configurables */
/*                                                                               */
/*********************************************************************************/

const void* Configurable::GetOptionsPtr(const std::string& name) const {
  for (auto o : options_) {
    if (o.name == name) {
      return o.opt_ptr;
    }
  }
  auto inner = Inner();
  if (inner != nullptr) {
    return inner->GetOptionsPtr(name);
  } else {
    return nullptr;
  }
}

std::string Configurable::GetOptionName(const std::string& opt_name) const {
  return opt_name;
}
}  // namespace ROCKSDB_NAMESPACE
