//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "db_stress_compression_manager.h"

#include "rocksdb/utilities/object_registry.h"

namespace ROCKSDB_NAMESPACE {
void DbStressCustomCompressionManager::Register() {
  // We must register any compression managers with a custom
  // CompatibilityName() so that if it was used in a past invocation but not
  // the current invocation, we can still read the SST files requiring it.
  static std::once_flag loaded;
  std::call_once(loaded, [&]() {
    TEST_AllowUnsupportedFormatVersion() = true;
    auto& library = *ObjectLibrary::Default();
    library.AddFactory<CompressionManager>(
        DbStressCustomCompressionManager().CompatibilityName(),
        [](const std::string& /*uri*/,
           std::unique_ptr<CompressionManager>* guard,
           std::string* /*errmsg*/) {
          *guard = std::make_unique<DbStressCustomCompressionManager>();
          return guard->get();
        });
  });
}
}  // namespace ROCKSDB_NAMESPACE
