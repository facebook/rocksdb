//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

// Shared test utilities for Wide Column + Blob integration tests.
// These utilities are used across multiple test files to reduce code
// duplication.

namespace wide_column_test_util {

// Generate a large value that will typically be stored as a blob when
// enable_blob_files is true and the size exceeds min_blob_size.
// Default size is 100 bytes.
inline std::string GenerateLargeValue(size_t size = 100, char fill_char = 'v') {
  return std::string(size, fill_char);
}

// Generate a small value that will typically be stored inline (not as a blob).
inline std::string GenerateSmallValue() { return "small"; }

// Get default options configured for blob file testing.
// Sets enable_blob_files=true, a small min_blob_size threshold (10 bytes),
// and disables auto compactions to allow manual control of flush/compaction.
inline Options GetOptionsForBlobTest() {
  Options options;
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 10;  // Small threshold to force blob extraction
  options.disable_auto_compactions = true;
  return options;
}

}  // namespace wide_column_test_util

}  // namespace ROCKSDB_NAMESPACE
