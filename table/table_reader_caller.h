//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

namespace rocksdb {
// A list of callers for a table reader. It is used to trace the caller that
// accesses on a block. This is only used for block cache analysis.
enum TableReaderCaller : char {
  kUserGet = 1,
  kUserMGet = 2,
  kUserIterator = 3,
  kUserApproximateSize = 4,
  kUserVerifyChecksum = 5,
  kSSTDumpTool = 6,
  kExternalSSTIngestion = 7,
  kRepair = 8,

  kPrefetch = 9,
  kCompaction = 10,
  // A compaction job may refill the block cache with blocks in the new SST
  // files if paranoid_file_checks is true.
  kCompactionRefill = 11,

  // Table reader benchmark.
  kTableReaderBench = 12,
  // Unit tests that call a table reader.
  kTest = 13,
  // After building a table, it may load all its blocks into the block cache if
  // paranoid_file_checks is true.
  kBuildTable = 14,
  // sst_file_reader.
  kUnknown = 15,
  // All callers should be added before kMaxBlockCacheLookupCaller.
  kMaxBlockCacheLookupCaller
};
}  // namespace rocksdb
