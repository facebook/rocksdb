//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>

#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {

// Standard blob files are append-only and are not expected to be read until
// close. FileSystems may use this contract to optimize away concurrent reader
// visibility while the file is active.
inline void SetBlobFileAppendOnlyNoReadersOpenContract(
    FileOptions* file_options) {
  assert(file_options != nullptr);
  file_options->open_contract = FileOpenContract::kAppendOnlyNoReaders;
}

inline bool IsBlobFileAppendOnlyNoReadersOpenContract(
    const FileOptions& file_options) {
  return file_options.open_contract == FileOpenContract::kAppendOnlyNoReaders;
}

}  // namespace ROCKSDB_NAMESPACE
