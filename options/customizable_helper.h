// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <functional>
#include <memory>
#include <unordered_map>

#include "options/options_helper.h"
#include "rocksdb/convenience.h"
#include "rocksdb/customizable.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"

namespace ROCKSDB_NAMESPACE {
template <typename T>
using SharedFactoryFunc =
    std::function<bool(const std::string&, std::shared_ptr<T>*)>;

template <typename T>
using UniqueFactoryFunc =
    std::function<bool(const std::string&, std::unique_ptr<T>*)>;

template <typename T>
using StaticFactoryFunc = std::function<bool(const std::string&, T**)>;

template <typename T>
static Status LoadSharedObject(const std::string& value,
                               const SharedFactoryFunc<T>& func,
                               const ConfigOptions& cfg_opts,
                               std::shared_ptr<T>* result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  }
  std::string curr_opts;
#ifndef ROCKSDB_LITE
  if (result->get() != nullptr && result->get()->GetId() == id) {
    status = result->get()->GetOptionString(cfg_opts.Embedded(), &curr_opts);
  }
#endif
  if (func == nullptr || !func(id, result)) {  // No factory, or it failed
    if (id.empty() && opt_map.empty()) {
      // No Id and no options.  Clear the object
      result->reset();
      return Status::OK();
    } else if (id.empty()) {  // We have no Id but have options.  Not good
      return Status::NotSupported("Cannot reset object ", id);
    } else {
#ifndef ROCKSDB_LITE
      status = cfg_opts.registry->NewSharedObject(id, result);
#else
      status = Status::NotSupported("Cannot load object in LITE mode ", id);
#endif
      if (!status.ok()) {
        if (cfg_opts.ignore_unknown_objects && status.IsNotSupported()) {
          return Status::OK();
        } else {
          return status;
        }
      }
    }
  }
  return Customizable::ConfigureNewObject(result->get(), id, curr_opts, opt_map,
                                          cfg_opts);
}

template <typename T>
static Status LoadUniqueObject(const std::string& value,
                               const UniqueFactoryFunc<T>& func,
                               const ConfigOptions& cfg_opts,
                               std::unique_ptr<T>* result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  }
  std::string curr_opts;
#ifndef ROCKSDB_LITE
  if (result->get() != nullptr && result->get()->GetId() == id) {
    status = result->get()->GetOptionString(cfg_opts.Embedded(), &curr_opts);
  }
#endif
  if (func == nullptr || !func(id, result)) {  // No factory, or it failed
    if (id.empty() && opt_map.empty()) {
      // No Id and no options.  Clear the object
      result->reset();
      return Status::OK();
    } else if (id.empty()) {  // We have no Id but have options.  Not good
      return Status::NotSupported("Cannot reset object ", id);
    } else {
#ifndef ROCKSDB_LITE
      status = cfg_opts.registry->NewUniqueObject(id, result);
#else
      status = Status::NotSupported("Cannot load object in LITE mode ", id);
#endif  // ROCKSDB_LITE
      if (!status.ok()) {
        if (cfg_opts.ignore_unknown_objects && status.IsNotSupported()) {
          return Status::OK();
        } else {
          return status;
        }
      }
    }
  }
  return Customizable::ConfigureNewObject(result->get(), id, curr_opts, opt_map,
                                          cfg_opts);
}
template <typename T>
static Status LoadStaticObject(const std::string& value,
                               const StaticFactoryFunc<T>& func,
                               const ConfigOptions& cfg_opts, T** result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  }
  std::string curr_opts;
#ifndef ROCKSDB_LITE
  if (*result != nullptr && (*result)->GetId() == id) {
    status = (*result)->GetOptionString(cfg_opts.Embedded(), &curr_opts);
  }
#endif
  if (func == nullptr || !func(id, result)) {  // No factory, or it failed
    if (id.empty() && opt_map.empty()) {
      // No Id and no options.  Clear the object
      *result = nullptr;
      return Status::OK();
    } else if (id.empty()) {  // We have no Id but have options.  Not good
      return Status::NotSupported("Cannot reset object ", id);
    } else {
#ifndef ROCKSDB_LITE
      status = cfg_opts.registry->NewStaticObject(id, result);
#else
      status = Status::NotSupported("Cannot load object in LITE mode ", id);
#endif  // ROCKSDB_LITE
      if (!status.ok()) {
        if (cfg_opts.ignore_unknown_objects && status.IsNotSupported()) {
          return Status::OK();
        } else {
          return status;
        }
      }
    }
  }
#ifndef ROCKSDB_LITE
  if (!curr_opts.empty()) {
    status = (*result)->ConfigureFromString(curr_opts, cfg_opts);
    if (!status.ok()) {
      return status;
    }
  }
#endif  // ROCKSDB_LITE
  return Customizable::ConfigureNewObject(*result, id, curr_opts, opt_map,
                                          cfg_opts);
}
}  // namespace ROCKSDB_NAMESPACE
