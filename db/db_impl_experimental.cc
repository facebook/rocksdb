//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <vector>

#include "db/column_family.h"
#include "db/version_set.h"
#include "rocksdb/status.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
Status DBImpl::SuggestCompactRange(ColumnFamilyHandle* column_family,
                                   const Slice* begin, const Slice* end) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  InternalKey start_key, end_key;
  if (begin != nullptr) {
    start_key = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
  }
  if (end != nullptr) {
    end_key = InternalKey(*end, 0, static_cast<ValueType>(0));
  }
  {
    InstrumentedMutexLock l(&mutex_);
    auto vstorage = cfd->current()->storage_info();
    for (int level = 0; level < vstorage->num_non_empty_levels(); ++level) {
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
    vstorage->ComputeCompactionScore(*cfd->GetLatestMutableCFOptions(),
                                     CompactionOptionsFIFO());
    SchedulePendingCompaction(cfd);
    MaybeScheduleFlushOrCompaction();
  }
  return Status::OK();
}
#endif  // ROCKSDB_LITE

}  // namespace rocksdb
