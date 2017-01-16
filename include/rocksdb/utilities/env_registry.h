// Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#ifndef ROCKSDB_LITE

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/env.h"

namespace rocksdb {

// Creates a new T using the registered factory function corresponding to the
// provided string.
//
// If no registered functions match, returns nullptr. If multiple functions
// match, the factory function used is unspecified.
//
// Populates res_guard with result pointer if caller is granted ownership.
template <typename T>
T* NewCustomObject(const std::string& text, std::unique_ptr<T>* res_guard);

// Returns a new T when called with a string. Populates the unique_ptr argument
// if granting ownership to caller.
template <typename T>
using FactoryFunc = std::function<T*(const std::string&, std::unique_ptr<T>*)>;

namespace {  // TODO: remove this once templated

// To register an Env factory function, initialize an EnvRegistrar object with
// static storage duration. For example:
//
//   static EnvRegistrar hdfs_reg("hdfs://", &CreateHdfsEnv);
//
// Then, calling NewCustomObject<Env>("hdfs://some_path", ...) will use
// CreateHdfsEnv to make a new Env.
class EnvRegistrar {
 public:
  explicit EnvRegistrar(std::string uri_prefix, FactoryFunc<Env> env_factory);
};
}

// Implementation details follow.

namespace internal {

template <typename T>
struct RegistryEntry {
  std::string prefix;
  FactoryFunc<T> env_factory;
};

template <typename T>
struct Registry {
  static Registry* Get() {
    static Registry<T> instance;
    return &instance;
  }
  std::vector<RegistryEntry<T>> entries;

 private:
  Registry() = default;
};

}  // namespace internal

template <typename T>
T* NewCustomObject(const std::string& text, std::unique_ptr<T>* res_guard) {
  res_guard->reset();
  for (const auto& entry : internal::Registry<T>::Get()->entries) {
    if (text.compare(0, entry.prefix.size(), entry.prefix) == 0) {
      return entry.env_factory(text, res_guard);
    }
  }
  return nullptr;
}

namespace {  // TODO: remove this once templated

EnvRegistrar::EnvRegistrar(std::string uri_prefix,
                           FactoryFunc<Env> env_factory) {
  internal::Registry<Env>::Get()->entries.emplace_back(
      internal::RegistryEntry<Env>{std::move(uri_prefix),
                                   std::move(env_factory)});
}
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
