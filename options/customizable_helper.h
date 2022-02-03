// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <functional>
#include <memory>
#include <unordered_map>

#include "options/configurable_helper.h"
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

// Creates a new shared Customizable object based on the input parameters.
// This method parses the input value to determine the type of instance to
// create. If there is an existing instance (in result) and it is the same type
// as the object being created, the existing configuration is stored and used as
// the default for the new object.
//
// The value parameter specified the instance class of the object to create.
// If it is a simple string (e.g. BlockBasedTable), then the instance will be
// created using the default settings.  If the value is a set of name-value
// pairs, then the "id" value is used to determine the instance to create and
// the remaining parameters are used to configure the object.  Id name-value
// pairs are specified, there must be an "id=value" pairing or an error will
// result.
//
// The config_options parameter controls the process and how errors are
// returned. If ignore_unknown_options=true, unknown values are ignored during
// the configuration If ignore_unsupported_options=true, unknown instance types
// are ignored If invoke_prepare_options=true, the resulting instance wll be
// initialized (via PrepareOptions
//
// @param config_options Controls how the instance is created and errors are
// handled
// @param value Either the simple name of the instance to create, or a set of
// name-value pairs to
//              create and initailzie the object
// @param func  Optional function to call to attempt to create an instance
// @param result The newly created instance.
template <typename T>
static Status LoadSharedObject(const ConfigOptions& config_options,
                               const std::string& value,
                               const SharedFactoryFunc<T>& func,
                               std::shared_ptr<T>* result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status =
      ConfigurableHelper::GetOptionsMap(value, result->get(), &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  }
  std::string curr_opts;
#ifndef ROCKSDB_LITE
  if (result->get() != nullptr && result->get()->GetId() == id) {
    // Try to get the existing options, ignoring any errors
    ConfigOptions embedded = config_options;
    embedded.delimiter = ";";
    result->get()->GetOptionString(embedded, &curr_opts).PermitUncheckedError();
  }
#endif
  if (func == nullptr || !func(id, result)) {  // No factory, or it failed
    if (value.empty()) {
      // No Id and no options.  Clear the object
      result->reset();
      return Status::OK();
    } else if (id.empty()) {  // We have no Id but have options.  Not good
      return Status::NotSupported("Cannot reset object ", id);
    } else {
#ifndef ROCKSDB_LITE
      status = ObjectRegistry::NewInstance()->NewSharedObject(id, result);
#else
      status = Status::NotSupported("Cannot load object in LITE mode ", id);
#endif
      if (!status.ok()) {
        if (config_options.ignore_unsupported_options) {
          return Status::OK();
        } else {
          return status;
        }
      }
    }
  }
  return ConfigurableHelper::ConfigureNewObject(config_options, result->get(),
                                                id, curr_opts, opt_map);
}

// Creates a new unique customizable instance object based on the input
// parameters.
// @see LoadSharedObject for more information on the inner workings of this
// method.
//
// @param config_options Controls how the instance is created and errors are
// handled
// @param value Either the simple name of the instance to create, or a set of
// name-value pairs to
//              create and initailzie the object
// @param func  Optional function to call to attempt to create an instance
// @param result The newly created instance.
template <typename T>
static Status LoadUniqueObject(const ConfigOptions& config_options,
                               const std::string& value,
                               const UniqueFactoryFunc<T>& func,
                               std::unique_ptr<T>* result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status =
      ConfigurableHelper::GetOptionsMap(value, result->get(), &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  }
  std::string curr_opts;
#ifndef ROCKSDB_LITE
  if (result->get() != nullptr && result->get()->GetId() == id) {
    // Try to get the existing options, ignoring any errors
    ConfigOptions embedded = config_options;
    embedded.delimiter = ";";
    result->get()->GetOptionString(embedded, &curr_opts).PermitUncheckedError();
  }
#endif
  if (func == nullptr || !func(id, result)) {  // No factory, or it failed
    if (value.empty()) {
      // No Id and no options.  Clear the object
      result->reset();
      return Status::OK();
    } else if (id.empty()) {  // We have no Id but have options.  Not good
      return Status::NotSupported("Cannot reset object ", id);
    } else {
#ifndef ROCKSDB_LITE
      status = ObjectRegistry::NewInstance()->NewUniqueObject(id, result);
#else
      status = Status::NotSupported("Cannot load object in LITE mode ", id);
#endif  // ROCKSDB_LITE
      if (!status.ok()) {
        if (config_options.ignore_unsupported_options) {
          return Status::OK();
        } else {
          return status;
        }
      }
    }
  }
  return ConfigurableHelper::ConfigureNewObject(config_options, result->get(),
                                                id, curr_opts, opt_map);
}
// Creates a new static (raw pointer) customizable instance object based on the
// input parameters.
// @see LoadSharedObject for more information on the inner workings of this
// method.
//
// @param config_options Controls how the instance is created and errors are
// handled
// @param value Either the simple name of the instance to create, or a set of
// name-value pairs to
//              create and initailzie the object
// @param func  Optional function to call to attempt to create an instance
// @param result The newly created instance.
template <typename T>
static Status LoadStaticObject(const ConfigOptions& config_options,
                               const std::string& value,
                               const StaticFactoryFunc<T>& func, T** result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status =
      ConfigurableHelper::GetOptionsMap(value, *result, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  }
  std::string curr_opts;
#ifndef ROCKSDB_LITE
  if (*result != nullptr && (*result)->GetId() == id) {
    // Try to get the existing options, ignoring any errors
    ConfigOptions embedded = config_options;
    embedded.delimiter = ";";
    (*result)->GetOptionString(embedded, &curr_opts).PermitUncheckedError();
  }
#endif
  if (func == nullptr || !func(id, result)) {  // No factory, or it failed
    if (value.empty()) {
      // No Id and no options.  Clear the object
      *result = nullptr;
      return Status::OK();
    } else if (id.empty()) {  // We have no Id but have options.  Not good
      return Status::NotSupported("Cannot reset object ", id);
    } else {
#ifndef ROCKSDB_LITE
      status = ObjectRegistry::NewInstance()->NewStaticObject(id, result);
#else
      status = Status::NotSupported("Cannot load object in LITE mode ", id);
#endif  // ROCKSDB_LITE
      if (!status.ok()) {
        if (config_options.ignore_unsupported_options) {
          return Status::OK();
        } else {
          return status;
        }
      }
    }
  }
  return ConfigurableHelper::ConfigureNewObject(config_options, *result, id,
                                                curr_opts, opt_map);
}
}  // namespace ROCKSDB_NAMESPACE
