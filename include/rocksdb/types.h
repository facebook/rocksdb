// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

// Define all public custom types here.

using ColumnFamilyId = uint32_t;

// Represents a sequence number in a WAL file.
using SequenceNumber = uint64_t;

const SequenceNumber kMinUnCommittedSeq = 1;  // 0 is always committed

enum class TableFileCreationReason {
  kFlush,
  kCompaction,
  kRecovery,
  kMisc,
};

enum class BlobFileCreationReason {
  kFlush,
  kCompaction,
  kRecovery,
};

// The types of files RocksDB uses in a DB directory. (Available for
// advanced options.)
enum FileType {
  kWalFile,
  kDBLockFile,
  kTableFile,
  kDescriptorFile,
  kCurrentFile,
  kTempFile,
  kInfoLogFile,  // Either the current one, or an old one
  kMetaDatabase,
  kIdentityFile,
  kOptionsFile,
  kBlobFile
};

// User-oriented representation of internal key types.
// Ordering of this enum entries should not change.
enum EntryType {
  kEntryPut,
  kEntryDelete,
  kEntrySingleDelete,
  kEntryMerge,
  kEntryRangeDeletion,
  kEntryBlobIndex,
  kEntryDeleteWithTimestamp,
  kEntryOther,
};

}  // namespace ROCKSDB_NAMESPACE
