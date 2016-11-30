// Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/env_registry.h"

#include <map>
#include <string>

namespace rocksdb {

struct EnvRegistryEntry {
  std::string prefix;
  EnvFactoryFunc env_factory;
};

struct EnvRegistry {
  static EnvRegistry* Get() {
    static EnvRegistry instance;
    return &instance;
  }
  std::vector<EnvRegistryEntry> entries;

 private:
  EnvRegistry() = default;
};

Env* NewEnvFromUri(const std::string& uri, std::unique_ptr<Env>* env_guard) {
  env_guard->reset();
  for (const auto& entry : EnvRegistry::Get()->entries) {
    if (uri.compare(0, entry.prefix.size(), entry.prefix) == 0) {
      return entry.env_factory(uri, env_guard);
    }
  }
  return nullptr;
}

EnvRegistrar::EnvRegistrar(std::string uri_prefix, EnvFactoryFunc env_factory) {
  EnvRegistry::Get()->entries.emplace_back(
      EnvRegistryEntry{std::move(uri_prefix), std::move(env_factory)});
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
