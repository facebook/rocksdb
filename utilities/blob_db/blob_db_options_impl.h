//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_db.h"

namespace rocksdb {

namespace blob_db {

struct BlobDBOptionsImpl : public BlobDBOptions {
  // deletions check period
  uint32_t deletion_check_period_millisecs;

  // gc percentage each check period
  uint32_t gc_file_pct;

  // gc period
  uint32_t gc_check_period_millisecs;

  // sanity check task
  uint32_t sanity_check_period_millisecs;

  // how many random access open files can we tolerate
  uint32_t open_files_trigger;

  // how many periods of stats do we keep.
  uint32_t wa_num_stats_periods;

  // what is the length of any period
  uint32_t wa_stats_period_millisecs;

  // we will garbage collect blob files in
  // which entire files have expired. However if the
  // ttl_range of files is very large say a day, we
  // would have to wait for the entire day, before we
  // recover most of the space.
  uint32_t partial_expiration_gc_range_secs;

  // this should be based on allowed Write Amplification
  // if 50% of the space of a blob file has been deleted/expired,
  uint32_t partial_expiration_pct;

  // how often should we schedule a job to fsync open files
  uint32_t fsync_files_period_millisecs;

  // how often to schedule reclaim open files.
  uint32_t reclaim_of_period_millisecs;

  // how often to schedule delete obs files periods
  uint32_t delete_obsf_period_millisecs;

  // how often to schedule check seq files period
  uint32_t check_seqf_period_millisecs;

  // default constructor
  BlobDBOptionsImpl();

  explicit BlobDBOptionsImpl(const BlobDBOptions& in);

  BlobDBOptionsImpl& operator=(const BlobDBOptionsImpl& in);
};

}  // namespace blob_db

}  // namespace rocksdb

#endif  // endif ROCKSDB
