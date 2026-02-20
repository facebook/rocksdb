//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <vector>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "logging/logging.h"
#include "rocksdb/status.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

Status DBImpl::SuggestCompactRange(ColumnFamilyHandle* column_family,
                                   const Slice* begin, const Slice* end) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  InternalKey start_key, end_key;
  if (begin != nullptr) {
    start_key.SetMinPossibleForUserKey(*begin);
  }
  if (end != nullptr) {
    end_key.SetMaxPossibleForUserKey(*end);
  }
  {
    InstrumentedMutexLock l(&mutex_);
    auto vstorage = cfd->current()->storage_info();
    for (int level = 0; level < vstorage->num_non_empty_levels() - 1; ++level) {
      std::vector<FileMetaData*> inputs;
      vstorage->GetOverlappingInputs(
          level, begin == nullptr ? nullptr : &start_key,
          end == nullptr ? nullptr : &end_key, &inputs);
      for (auto f : inputs) {
        f->marked_for_compaction = true;
      }
    }
    // Since we have some more files to compact, we should also recompute
    // compaction score
    vstorage->ComputeCompactionScore(cfd->ioptions(),
                                     cfd->GetLatestMutableCFOptions(),
                                     cfd->GetFullHistoryTsLow());
    EnqueuePendingCompaction(cfd);
    MaybeScheduleFlushOrCompaction();
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
