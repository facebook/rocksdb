// Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE
#include <functional>
#include <memory>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/version.h"

namespace ROCKSDB_NAMESPACE {

struct PluginProperties {
  // The version of this structure.  If the structure is changed, the version
  // should be incremented
  static constexpr int kVersion() { return 1; }

  // The RocksDB version this plugin was built against
  RocksVersion rocksdb_version;

  // The name of this plugin
  std::string name;

  // Function to use to register the factories for this plugin
  std::function<int(ObjectLibrary&, const std::string&)> registrar;
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
