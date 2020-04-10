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
const std::string Configurable::kIdPropName = "id";
const std::string Configurable::kIdPropSuffix = "." + kIdPropName;

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
Status Configurable::DoConfigureOption(const OptionTypeInfo* opt_info,
                                       const std::string& short_name,
                                       const std::string& opt_name,
                                       const std::string& opt_value,
                                       const ConfigOptions& cfg,
                                       void* opt_ptr) {
  Status s;
  if (opt_info == nullptr) {
    s = Status::NotFound("Could not find option: ", opt_name);
  } else if (opt_name == short_name) {
    s = ParseOption(*opt_info, opt_name, opt_value, cfg, opt_ptr);
  } else if (opt_info->IsCustomizable() &&
             EndsWith(short_name, kIdPropSuffix)) {
    s = ParseOption(*opt_info, opt_name, opt_value, cfg, opt_ptr);
  } else if (opt_info->IsStruct()) {
    s = ParseOption(*opt_info, opt_name, opt_value, cfg, opt_ptr);
  } else if (opt_info->IsCustomizable()) {
    Customizable* custom = opt_info->AsRawPointer<Customizable>(opt_ptr);
    if (opt_value.empty()) {
      s = Status::OK();
    } else if (custom == nullptr ||
               !StartsWith(opt_name, custom->GetId() + ".")) {
      s = Status::NotFound("Could not find customizable option: ", short_name);
    } else if (opt_value.find("=") != std::string::npos) {
      s = custom->ConfigureFromString(opt_value, cfg);
    } else {
      s = custom->ConfigureOption(opt_name, opt_value, cfg);
    }
  } else if (opt_info->IsConfigurable()) {
    s = ParseOption(*opt_info, opt_name, opt_value, cfg, opt_ptr);
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
    std::unordered_map<std::string, std::string>* options,
    const std::unordered_map<std::string, OptionTypeInfo>& type_map,
    const std::string& /*type_name*/, void* opt_ptr, const ConfigOptions& cfg) {
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
      s = DoConfigureOption(opt_info, opt_name, elem_name, opt_value, cfg,
                            opt_ptr);
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
          if (!cfg.ignore_unknown_objects && not_found.ok()) {
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
    const std::unordered_map<std::string, std::string>& opts_map,
    const ConfigOptions& options,
    std::unordered_map<std::string, std::string>* unused) {
  std::unordered_map<std::string, std::string> remaining = opts_map;
  Status s;
  Status not_found, invalid;
  for (const auto iter : options_) {
    s = DoConfigureOptions(&remaining, *iter.type_map, iter.name, iter.opt_ptr,
                           options);
    if (remaining.empty()) {  // Are there more options left?
      break;
    } else if (!options.ignore_unknown_options && !s.IsNotFound()) {
      break;
    }
  }
  if (unused != nullptr && !remaining.empty()) {
    unused->insert(remaining.begin(), remaining.end());
  }
  if (options.ignore_unknown_options && s.IsNotFound()) {
    s = Status::OK();
  }
  return s;
}

Status Configurable::ConfigureFromString(
    const std::string& opt_str, const ConfigOptions& options,
    std::unordered_map<std::string, std::string>* unused_opts) {
  std::unordered_map<std::string, std::string> opt_map;
  Status s = StringToMap(opt_str, &opt_map);
  if (s.ok()) {
    s = ConfigureFromMap(opt_map, options, unused_opts);
  }
  return s;
}
#endif  // ROCKSDB_LITE

Status Configurable::ConfigureFromMap(
    const std::unordered_map<std::string, std::string>& opts_map,
    const ConfigOptions& options) {
  Status s = ConfigureFromMap(opts_map, options, nullptr);
  return s;
}

Status Configurable::ConfigureFromMap(
    const std::unordered_map<std::string, std::string>& opts_map,
    const ConfigOptions& options,
    std::unordered_map<std::string, std::string>* unused) {
  Status s;
  std::string curr_opts;
#ifndef ROCKSDB_LITE
  if (!options.ignore_unknown_options) {
    // If we are not ignoring unused, get the defaults in case we need to reset
    GetOptionString(options, &curr_opts);
  }
#endif  // ROCKSDB_LITE
  if (!opts_map.empty()) {
#ifndef ROCKSDB_LITE
    s = DoConfigureFromMap(opts_map, options, unused);
#else
    if (!options.ignore_unknown_options) {
      (void)unused;
      s = Status::NotSupported("ConfigureFromMap not supported in LITE mode");
    }
#endif  // ROCKSDB_LITE
  }
  if (options.invoke_prepare_options && s.ok()) {
    s = PrepareOptions(options);
  }
#ifndef ROCKSDB_LITE
  if (!s.ok() && !curr_opts.empty()) {
    ConfigOptions reset = options;
    reset.ignore_unknown_options = true;
    reset.invoke_prepare_options = true;
    // There are some options to reset from this current error
    ConfigureFromString(curr_opts, reset);
  }
#endif  // ROCKSDB_LITE
  return s;
}

Status Configurable::ParseStringOptions(const std::string& opts_str,
                                        const ConfigOptions& opts) {
  if (!opts_str.empty()) {
    return Status::InvalidArgument("Cannot parse option: ", opts_str);
  } else if (opts.invoke_prepare_options) {
    return PrepareOptions(opts);
  } else {
    return Status::OK();
  }
}
Status Configurable::ConfigureFromString(const std::string& opts_str,
                                         const ConfigOptions& options) {
  Status s;
  if (!opts_str.empty()) {
    if (opts_str.find(';') == std::string::npos &&
        opts_str.find('=') == std::string::npos) {
      return ParseStringOptions(opts_str, options);
    } else {
#ifndef ROCKSDB_LITE
      std::unordered_map<std::string, std::string> opt_map;

      s = StringToMap(opts_str, &opt_map);
      if (s.ok()) {
        s = ConfigureFromMap(opt_map, options, nullptr);
      }
#else
      s = ParseStringOptions(opts_str, options);
#endif  // ROCKSDB_LITE
    }
  }
  return s;
}

/**
 * Sets the value of the named property to the input value, returning OK on
 * succcess.
 */
Status Configurable::ConfigureOption(const std::string& name,
                                     const std::string& value,
                                     const ConfigOptions& options) {
  const std::string& opt_name = GetOptionName(name);
  std::string elem_name;
#ifndef ROCKSDB_LITE
  Status status;
  // Look for the name in all of the registered option maps until it is found
  for (const auto& iter : options_) {
    const auto opt_info =
        OptionTypeInfo::FindOption(opt_name, *(iter.type_map), &elem_name);
    if (opt_info != nullptr) {
      return DoConfigureOption(opt_info, opt_name, elem_name, value, options,
                               iter.opt_ptr);
    }
  }
#endif
  return SetUnknown(opt_name, value, options);
}

/**
 * Looks for the named option amongst the options for this type and sets
 * the value for it to be the input value.
 * If the name was found, found_option will be set to true and the resulting
 * status should be returned.
 */
#ifndef ROCKSDB_LITE
Status Configurable::ParseOption(const OptionTypeInfo& opt_info,
                                 const std::string& opt_name,
                                 const std::string& opt_value,
                                 const ConfigOptions& cfg, void* opt_ptr) {
  Status s = opt_info.ParseOption(opt_name, opt_value, cfg, opt_ptr);
  return s;
}

Status Configurable::SerializeOption(const OptionTypeInfo& opt_info,
                                     const std::string& opt_name,
                                     const void* const opt_ptr,
                                     const ConfigOptions& options,
                                     std::string* opt_value) const {
  Status s = opt_info.SerializeOption(opt_name, opt_ptr, options, opt_value);
  return s;
}

bool Configurable::MatchesOption(const OptionTypeInfo& opt_info,
                                 const std::string& opt_name,
                                 const void* const this_ptr,
                                 const void* const that_ptr,
                                 const ConfigOptions& options,
                                 std::string* mismatch) const {
  bool matches =
      opt_info.MatchesOption(opt_name, this_ptr, that_ptr, options, mismatch);
  return matches;
}

bool Configurable::CheckByName(const OptionTypeInfo& opt_info,
                               const std::string& opt_name,
                               const void* const this_ptr,
                               const void* const that_ptr,
                               const ConfigOptions& options) const {
  return opt_info.CheckByName(opt_name, this_ptr, that_ptr, options);
}
#endif  // ROCKSDB_LITE

/*********************************************************************************/
/*                                                                               */
/*       Methods for Converting Options into strings */
/*                                                                               */
/*********************************************************************************/

Status Configurable::GetOptionString(const ConfigOptions& options,
                                     std::string* result) const {
  assert(result);
  result->clear();
  Status s;
#ifndef ROCKSDB_LITE
  s = DoSerializeOptions("", options, result);
#else
  (void)options;
#endif  // ROCKSDB_LITE
  return s;
}

#ifndef ROCKSDB_LITE
std::string Configurable::ToString(const std::string& prefix,
                                   const ConfigOptions& options) const {
  std::string result = AsString(prefix, options);
  if (result.empty() || result.find('=') == std::string::npos) {
    return result;
  } else {
    return "{" + result + "}";
  }
}

std::string Configurable::AsString(const std::string& header,
                                   const ConfigOptions& options) const {
  std::string result;
  Status s = DoSerializeOptions(header, options, &result);
  assert(s.ok());
  return result;
}

Status Configurable::GetOptionNames(
    const ConfigOptions& options,
    std::unordered_set<std::string>* result) const {
  assert(result);
  return DoListOptions("", options, result);
}

Status Configurable::DoListOptions(
    const std::string& prefix, const ConfigOptions& /*options*/,
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

Status Configurable::GetOption(const std::string& name,
                               const ConfigOptions& options,
                               std::string* value) const {
  assert(value);
  value->clear();

  return DoGetOption(GetOptionName(name), options.Embedded(), value);
}

Status Configurable::DoGetOption(const std::string& short_name,
                                 const ConfigOptions& options,
                                 std::string* value) const {
  // Look for option directly
  for (auto iter : options_) {
    // Look up the value in the map
    std::string opt_name;
    const auto opt_info =
        OptionTypeInfo::FindOption(short_name, *(iter.type_map), &opt_name);
    if (opt_info != nullptr) {
      if (short_name == opt_name) {
        return SerializeOption(*opt_info, opt_name, iter.opt_ptr, options,
                               value);
      } else if (opt_info->IsStruct()) {
        return SerializeOption(*opt_info, opt_name, iter.opt_ptr, options,
                               value);
      } else if (opt_info->IsConfigurable()) {
        auto const* config = opt_info->AsRawPointer<Configurable>(iter.opt_ptr);
        if (config != nullptr) {
          return config->GetOption(opt_name, options, value);
        }
      }
    }
  }
  return Status::NotFound("Cannot find option: ", short_name);
}

Status Configurable::SerializeOptions(
    const std::string& prefix,
    const std::unordered_map<std::string, OptionTypeInfo>& type_map,
    const void* opt_ptr, const ConfigOptions& options,
    std::string* result) const {
  for (auto opt_iter : type_map) {
    const auto& opt_name = opt_iter.first;
    const auto& opt_info = opt_iter.second;
    if (opt_info.ShouldSerialize()) {
      std::string value;
      Status s = SerializeOption(opt_info, prefix + opt_name, opt_ptr, options,
                                 &value);
      if (!s.ok()) {
        return s;
      } else if (!value.empty()) {
        // <prefix><opt_name>=<value><delimiter>
        result->append(prefix + opt_name + "=" + value + options.delimiter);
      }
    }
  }

  return Status::OK();
}

Status Configurable::DoSerializeOptions(const std::string& prefix,
                                        const ConfigOptions& options,
                                        std::string* result) const {
  assert(result);
  for (auto const iter : options_) {
    Status s = SerializeOptions(prefix, *(iter.type_map), iter.opt_ptr, options,
                                result);
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

bool Configurable::Matches(const Configurable* other,
                           const ConfigOptions& options) const {
  std::string name;
  return Matches(other, options, &name);
}

bool Configurable::Matches(const Configurable* other,
                           const ConfigOptions& options,
                           std::string* name) const {
  assert(name);
  name->clear();
  if (this == other || options.IsCheckDisabled()) {
    return true;
  } else if (other != nullptr) {
    return DoMatchesOptions(other, options, name);
  } else {
    return false;
  }
}

bool Configurable::DoMatchesOptions(const Configurable* that,
                                    const ConfigOptions& cfg,
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
        if (cfg.IsCheckEnabled(map_iter.second.GetSanityLevel())) {
          if (!MatchesOption(map_iter.second, map_iter.first, this_offset,
                             that_offset, cfg, mismatch) &&
              !CheckByName(map_iter.second, map_iter.first, this_offset,
                           that_offset, cfg)) {
            return false;
          }
        }
      }
    }
  }
#else
  (void)cfg;
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
