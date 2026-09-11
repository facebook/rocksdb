//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "table/multiget_context.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

inline std::string* GetOutputTimestamp(OutputMetadata* output_metadata) {
  return output_metadata != nullptr && output_metadata->timestamp.has_value()
             ? &*output_metadata->timestamp
             : nullptr;
}

inline std::vector<std::string>* GetOutputTimestamps(
    MultiGetOutputMetadata* output_metadata) {
  return output_metadata != nullptr && output_metadata->timestamps.has_value()
             ? &*output_metadata->timestamps
             : nullptr;
}

inline bool* GetOutputNewerVersionPresent(OutputMetadata* output_metadata) {
  return output_metadata != nullptr &&
                 output_metadata->newer_version_present.has_value()
             ? &*output_metadata->newer_version_present
             : nullptr;
}

inline std::vector<uint8_t>* GetOutputNewerVersionPresent(
    MultiGetOutputMetadata* output_metadata) {
  return output_metadata != nullptr &&
                 output_metadata->newer_version_present.has_value()
             ? &*output_metadata->newer_version_present
             : nullptr;
}

inline ColumnFamilyHandle** MakeMutableCfHandles(
    ColumnFamilyHandle* const* column_families, size_t num_keys,
    autovector<ColumnFamilyHandle*, MultiGetContext::MAX_BATCH_SIZE>*
        stack_column_families,
    std::vector<ColumnFamilyHandle*>* heap_column_families) {
  ColumnFamilyHandle** mutable_column_families;
  if (num_keys <= MultiGetContext::MAX_BATCH_SIZE) {
    stack_column_families->resize(num_keys);
    mutable_column_families =
        num_keys == 0 ? nullptr : &(*stack_column_families)[0];
  } else {
    heap_column_families->resize(num_keys);
    mutable_column_families = heap_column_families->data();
  }
  for (size_t i = 0; i < num_keys; ++i) {
    mutable_column_families[i] = column_families[i];
  }
  return mutable_column_families;
}

}  // namespace ROCKSDB_NAMESPACE
