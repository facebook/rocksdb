//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/user_value_checksum.h"

#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"

namespace ROCKSDB_NAMESPACE {

Status UserValueChecksum::CreateFromString(
    const ConfigOptions& config_options, const std::string& name,
    std::shared_ptr<UserValueChecksum>* result) {
  return LoadSharedObject<UserValueChecksum>(config_options, name, result);
}

}  // namespace ROCKSDB_NAMESPACE
