// Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/env_registry.h"

#include <map>
#include <string>

namespace rocksdb {

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

Env* NewEnvFromUri(const std::string& uri, std::unique_ptr<Env>* env_guard) {
  env_guard->reset();
  for (const auto& entry : Registry<Env>::Get()->entries) {
    if (uri.compare(0, entry.prefix.size(), entry.prefix) == 0) {
      return entry.env_factory(uri, env_guard);
    }
  }
  return nullptr;
}

EnvRegistrar::EnvRegistrar(std::string uri_prefix,
                           FactoryFunc<Env> env_factory) {
  Registry<Env>::Get()->entries.emplace_back(
      RegistryEntry<Env>{std::move(uri_prefix), std::move(env_factory)});
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
