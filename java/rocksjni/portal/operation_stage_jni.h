// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#pragma once

#include <jni.h>

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksjni/portal/common.h"

namespace ROCKSDB_NAMESPACE {
// The portal class for org.rocksdb.OperationStage
class OperationStageJni {
 public:
  // Returns the equivalent org.rocksdb.OperationStage for the provided
  // C++ ROCKSDB_NAMESPACE::ThreadStatus::OperationStage enum
  static jbyte toJavaOperationStage(
      const ROCKSDB_NAMESPACE::ThreadStatus::OperationStage& operation_stage) {
    switch (operation_stage) {
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::STAGE_UNKNOWN:
        return 0x0;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::STAGE_FLUSH_RUN:
        return 0x1;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_FLUSH_WRITE_L0:
        return 0x2;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_COMPACTION_PREPARE:
        return 0x3;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_COMPACTION_RUN:
        return 0x4;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_COMPACTION_PROCESS_KV:
        return 0x5;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_COMPACTION_INSTALL:
        return 0x6;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_COMPACTION_SYNC_FILE:
        return 0x7;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_PICK_MEMTABLES_TO_FLUSH:
        return 0x8;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_MEMTABLE_ROLLBACK:
        return 0x9;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
          STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS:
        return 0xA;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ThreadStatus::OperationStage
  // enum for the provided Java org.rocksdb.OperationStage
  static ROCKSDB_NAMESPACE::ThreadStatus::OperationStage toCppOperationStage(
      jbyte joperation_stage) {
    switch (joperation_stage) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::STAGE_UNKNOWN;
      case 0x1:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::STAGE_FLUSH_RUN;
      case 0x2:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_FLUSH_WRITE_L0;
      case 0x3:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_COMPACTION_PREPARE;
      case 0x4:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_COMPACTION_RUN;
      case 0x5:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_COMPACTION_PROCESS_KV;
      case 0x6:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_COMPACTION_INSTALL;
      case 0x7:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_COMPACTION_SYNC_FILE;
      case 0x8:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_PICK_MEMTABLES_TO_FLUSH;
      case 0x9:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_MEMTABLE_ROLLBACK;
      case 0xA:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::
            STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationStage::STAGE_UNKNOWN;
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE
