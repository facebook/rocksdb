//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "rocksdb/listener.h"

namespace ROCKSDB_NAMESPACE {

Status GetStringFromCompactionReason(std::string* compaction_str,
                                     CompactionReason compaction_reason) {
  switch (compaction_reason) {
    case CompactionReason::kUnknown:
      *compaction_str = "Unknown";
      return Status::OK();
    case CompactionReason::kLevelL0FilesNum:
      *compaction_str = "LevelL0FilesNum";
      return Status::OK();
    case CompactionReason::kLevelMaxLevelSize:
      *compaction_str = "LevelMaxLevelSize";
      return Status::OK();
    case CompactionReason::kUniversalSizeAmplification:
      *compaction_str = "UniversalSizeAmplification";
      return Status::OK();
    case CompactionReason::kUniversalSizeRatio:
      *compaction_str = "UniversalSizeRatio";
      return Status::OK();
    case CompactionReason::kUniversalSortedRunNum:
      *compaction_str = "UniversalSortedRunNum";
      return Status::OK();
    case CompactionReason::kFIFOMaxSize:
      *compaction_str = "FIFOMaxSize";
      return Status::OK();
    case CompactionReason::kFIFOReduceNumFiles:
      *compaction_str = "FIFOReduceNumFiles";
      return Status::OK();
    case CompactionReason::kFIFOTtl:
      *compaction_str = "FIFOTtl";
      return Status::OK();
    case CompactionReason::kManualCompaction:
      *compaction_str = "ManualCompaction";
      return Status::OK();
    case CompactionReason::kFilesMarkedForCompaction:
      *compaction_str = "FilesMarkedForCompaction";
      return Status::OK();
    case CompactionReason::kBottommostFiles:
      *compaction_str = "BottommostFiles";
      return Status::OK();
    case CompactionReason::kTtl:
      *compaction_str = "Ttl";
      return Status::OK();
    case CompactionReason::kFlush:
      *compaction_str = "Flush";
      return Status::OK();
    case CompactionReason::kExternalSstIngestion:
      *compaction_str = "ExternalSstIngestion";
      return Status::OK();
    case CompactionReason::kPeriodicCompaction:
      *compaction_str = "PeriodicCompaction";
      return Status::OK();
    case CompactionReason::kNumOfReasons:
      // fall through
    default:
        return Status::InvalidArgument("Invalid compaction reason");
  }
}

}
