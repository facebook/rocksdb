//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// WARNING: This value is == kCurrentFileBlobIndexFileNumber.
// Use this name only where file number zero means "no valid blob file" or
// "current file" is not understood/supported.
constexpr uint64_t kInvalidBlobFileNumber = 0;

// WARNING: This value is == kInvalidBlobFileNumber.
// Use this name only for BlobIndex references to the same physical file as what
// is currently being read; generic blob-file metadata must treat zero as
// invalid. (Using a distinct value like 1 was found to be more problematic,
// e.g. because of legacy "stackable" blob implementation.)
constexpr uint64_t kCurrentFileBlobIndexFileNumber = kInvalidBlobFileNumber;
static_assert(kCurrentFileBlobIndexFileNumber == kInvalidBlobFileNumber);

}  // namespace ROCKSDB_NAMESPACE
