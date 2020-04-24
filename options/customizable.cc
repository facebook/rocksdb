// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/customizable.h"

#include "rocksdb/convenience.h"
#include "rocksdb/status.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

const Customizable* Customizable::FindInstance(const std::string& name) const {
  if (name == Name()) {
    return this;
  } else {
    auto const inner = static_cast<Customizable*>(Inner());
    if (inner != nullptr) {
      return inner->FindInstance(name);
    } else {
      return nullptr;
    }
  }
}

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

#ifndef ROCKSDB_LITE
std::string Customizable::AsString(const ConfigOptions& config_options,
                                   const std::string& prefix) const {
  std::string result;
  std::string parent;
  if (!config_options.IsShallow()) {
    parent = Configurable::AsString(config_options, "");
  }
  if (parent.empty()) {
    result = GetId();
  } else {
    result.append(prefix + kIdPropName + "=" + GetId() +
                  config_options.delimiter);
    result.append(parent);
  }
  return result;
}

Status Customizable::DoGetOption(const ConfigOptions& config_options,
                                 const std::string& opt_name,
                                 std::string* value) const {
  if (opt_name == kIdPropName) {
    *value = GetId();
    return Status::OK();
  } else {
    return Configurable::DoGetOption(config_options, opt_name, value);
  }
}

#endif  // ROCKSDB_LITE

bool Customizable::DoMatchesOptions(const ConfigOptions& config_options,
                                    const Configurable* other,
                                    std::string* name) const {
  if (config_options.sanity_level > ConfigOptions::kSanityLevelNone &&
      this != other) {
    const Customizable* custom = reinterpret_cast<const Customizable*>(other);
    if (GetId() != custom->GetId()) {
      *name = kIdPropName;
      return false;
    } else if (config_options.sanity_level >
               ConfigOptions::kSanityLevelLooselyCompatible) {
      bool matches =
          Configurable::DoMatchesOptions(config_options, other, name);
      return matches;
    }
  }
  return true;
}

Status Customizable::ConfigureNewObject(
    const ConfigOptions& config_options, Customizable* object,
    const std::string& id, const std::string& base_opts,
    const std::unordered_map<std::string, std::string>& opts) {
  if (object != nullptr) {
    if (!base_opts.empty()) {
#ifndef ROCKSDB_LITE
      // Don't run prepare options on the base, as we would do that on the
      // overlay opts instead
      ConfigOptions copy = config_options;
      copy.invoke_prepare_options = false;
      Status status = object->ConfigureFromString(copy, base_opts);
      if (!status.ok()) {
        return status;
      }
#endif  // ROCKSDB_LITE
    }
    return object->ConfigureFromMap(config_options, opts);
  } else if (opts.empty()) {  // No object but no map.  This is OK
    return Status::OK();
  } else {  // We have no object but a map to use to configure it.  This is bad
    return Status::InvalidArgument("Cannot configure null object ", id);
  }
}

Status Customizable::GetOptionsMap(
    const std::string& value, std::string* id,
    std::unordered_map<std::string, std::string>* props) {
  return GetOptionsMap(value, "", id, props);
}

Status Customizable::GetOptionsMap(
    const std::string& value, const std::string& default_id, std::string* id,
    std::unordered_map<std::string, std::string>* props) {
  assert(id);
  assert(props);
  Status status;
  if (value.empty() || value == kNullptrString) {
    *id = default_id;
  } else if (value.find('=') == std::string::npos) {
    *id = value;
#ifndef ROCKSDB_LITE
  } else {
    status = StringToMap(value, props);
    if (status.ok()) {
      auto iter = props->find(kIdPropName);
      if (iter != props->end()) {
        *id = iter->second;
        props->erase(iter);
      } else if (default_id.empty()) {  // Should this be an error??
        status = Status::InvalidArgument("Name property is missing");
      } else {
        *id = default_id;
      }
    }
#else
  } else {
    *id = value;
    props->clear();
#endif
  }
  return status;
}
}  // namespace ROCKSDB_NAMESPACE
