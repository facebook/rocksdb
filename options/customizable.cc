// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/customizable.h"

#include <sstream>

#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

std::string Customizable::GetOptionName(const std::string& long_name) const {
  const std::string& name = Name();
  size_t name_len = name.size();
  if (long_name.size() > name_len + 1 &&
      long_name.compare(0, name_len, name) == 0 &&
      long_name.at(name_len) == '.') {
    return long_name.substr(name_len + 1);
  } else {
    return Configurable::GetOptionName(long_name);
  }
}

std::string Customizable::GenerateIndividualId() const {
  std::ostringstream ostr;
  ostr << Name() << "@" << static_cast<const void*>(this) << "#"
       << port::GetProcessID();
  return ostr.str();
}

#ifndef ROCKSDB_LITE
Status Customizable::GetOption(const ConfigOptions& config_options,
                               const std::string& opt_name,
                               std::string* value) const {
  if (opt_name == OptionTypeInfo::kIdPropName()) {
    *value = GetId();
    return Status::OK();
  } else {
    return Configurable::GetOption(config_options, opt_name, value);
  }
}

std::string Customizable::SerializeOptions(const ConfigOptions& config_options,
                                           const std::string& prefix) const {
  std::string result;
  std::string parent;
  std::string id = GetId();
  if (!config_options.IsShallow() && !id.empty()) {
    parent = Configurable::SerializeOptions(config_options, "");
  }
  if (parent.empty()) {
    result = id;
  } else {
    result.append(prefix);
    result.append(OptionTypeInfo::kIdPropName());
    result.append("=");
    result.append(id);
    result.append(config_options.delimiter);
    result.append(parent);
  }
  return result;
}

#endif  // ROCKSDB_LITE

bool Customizable::AreEquivalent(const ConfigOptions& config_options,
                                 const Configurable* other,
                                 std::string* mismatch) const {
  if (config_options.sanity_level > ConfigOptions::kSanityLevelNone &&
      this != other) {
    const Customizable* custom = reinterpret_cast<const Customizable*>(other);
    if (custom == nullptr) {  // Cast failed
      return false;
    } else if (GetId() != custom->GetId()) {
      *mismatch = OptionTypeInfo::kIdPropName();
      return false;
    } else if (config_options.sanity_level >
               ConfigOptions::kSanityLevelLooselyCompatible) {
      bool matches =
          Configurable::AreEquivalent(config_options, other, mismatch);
      return matches;
    }
  }
  return true;
}

Status Customizable::GetOptionsMap(
    const ConfigOptions& config_options, const Customizable* customizable,
    const std::string& value, std::string* id,
    std::unordered_map<std::string, std::string>* props) {
  Status status;
  if (value.empty() || value == kNullptrString) {
    *id = "";
    props->clear();
  } else if (customizable != nullptr) {
    status =
        Configurable::GetOptionsMap(value, customizable->GetId(), id, props);
#ifdef ROCKSDB_LITE
    (void)config_options;
#else
    if (status.ok() && customizable->IsInstanceOf(*id)) {
      // The new ID and the old ID match, so the objects are the same type.
      // Try to get the existing options, ignoring any errors
      ConfigOptions embedded = config_options;
      embedded.delimiter = ";";
      std::string curr_opts;
      if (customizable->GetOptionString(embedded, &curr_opts).ok()) {
        std::unordered_map<std::string, std::string> curr_props;
        if (StringToMap(curr_opts, &curr_props).ok()) {
          props->insert(curr_props.begin(), curr_props.end());
        }
      }
    }
#endif  // ROCKSDB_LITE
  } else {
    status = Configurable::GetOptionsMap(value, "", id, props);
  }
  return status;
}

Status Customizable::ConfigureNewObject(
    const ConfigOptions& config_options, Customizable* object,
    const std::unordered_map<std::string, std::string>& opt_map) {
  Status status;
  if (object != nullptr) {
    status = object->ConfigureFromMap(config_options, opt_map);
  } else if (!opt_map.empty()) {
    status = Status::InvalidArgument("Cannot configure null object ");
  }
  return status;
}
}  // namespace ROCKSDB_NAMESPACE
