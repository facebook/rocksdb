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

// FileSystems can use this open-time contract to distinguish active blob
// direct-write files from sealed blob files and choose an implementation that
// exposes concurrent append visibility to readers.
inline constexpr char kBlobFileOpenModeProperty[] =
    "rocksdb.blob_file_open_mode";
inline constexpr char kBlobFileOpenModeActiveDirectWrite[] =
    "active_direct_write";

inline void SetBlobFileActiveDirectWriteOpenMode(FileOptions* file_options) {
  assert(file_options != nullptr);
  file_options->io_options.property_bag[kBlobFileOpenModeProperty] =
      kBlobFileOpenModeActiveDirectWrite;
}

inline bool IsBlobFileActiveDirectWriteOpenMode(
    const FileOptions& file_options) {
  const auto entry =
      file_options.io_options.property_bag.find(kBlobFileOpenModeProperty);
  return entry != file_options.io_options.property_bag.end() &&
         entry->second == kBlobFileOpenModeActiveDirectWrite;
}

}  // namespace ROCKSDB_NAMESPACE
