//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_db_options_impl.h"

namespace rocksdb {

namespace blob_db {

BlobDBOptionsImpl::BlobDBOptionsImpl(const BlobDBOptions& in)
    : BlobDBOptions(in),
      deletion_check_period_millisecs(2 * 1000),
      gc_file_pct(20),
      gc_check_period_millisecs(60 * 1000),
      sanity_check_period_millisecs(20 * 60 * 1000),
      open_files_trigger(100),
      wa_num_stats_periods(24),
      wa_stats_period_millisecs(3600 * 1000),
      partial_expiration_gc_range_secs(4 * 3600),
      partial_expiration_pct(75),
      fsync_files_period_millisecs(10 * 1000),
      reclaim_of_period_millisecs(1 * 1000),
      delete_obsf_period_millisecs(10 * 1000),
      check_seqf_period_millisecs(10 * 1000),
      disable_background_tasks(false) {}

BlobDBOptionsImpl::BlobDBOptionsImpl()
    : deletion_check_period_millisecs(2 * 1000),
      gc_file_pct(20),
      gc_check_period_millisecs(60 * 1000),
      sanity_check_period_millisecs(20 * 60 * 1000),
      open_files_trigger(100),
      wa_num_stats_periods(24),
      wa_stats_period_millisecs(3600 * 1000),
      partial_expiration_gc_range_secs(4 * 3600),
      partial_expiration_pct(75),
      fsync_files_period_millisecs(10 * 1000),
      reclaim_of_period_millisecs(1 * 1000),
      delete_obsf_period_millisecs(10 * 1000),
      check_seqf_period_millisecs(10 * 1000),
      disable_background_tasks(false) {}

BlobDBOptionsImpl& BlobDBOptionsImpl::operator=(const BlobDBOptionsImpl& in) {
  BlobDBOptions::operator=(in);
  if (this != &in) {
    deletion_check_period_millisecs = in.deletion_check_period_millisecs;
    gc_file_pct = in.gc_file_pct;
    gc_check_period_millisecs = in.gc_check_period_millisecs;
    sanity_check_period_millisecs = in.sanity_check_period_millisecs;
    open_files_trigger = in.open_files_trigger;
    wa_num_stats_periods = in.wa_num_stats_periods;
    wa_stats_period_millisecs = in.wa_stats_period_millisecs;
    partial_expiration_gc_range_secs = in.partial_expiration_gc_range_secs;
    partial_expiration_pct = in.partial_expiration_pct;
    fsync_files_period_millisecs = in.fsync_files_period_millisecs;
    reclaim_of_period_millisecs = in.reclaim_of_period_millisecs;
    delete_obsf_period_millisecs = in.delete_obsf_period_millisecs;
    check_seqf_period_millisecs = in.check_seqf_period_millisecs;
    disable_background_tasks = in.disable_background_tasks;
  }
  return *this;
}

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
