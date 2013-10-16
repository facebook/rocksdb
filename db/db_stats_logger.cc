//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"
#include <string>
#include <stdint.h>
#include <stdio.h>
#include "db/version_set.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "port/port.h"
#include "util/mutexlock.h"

namespace rocksdb {

void DBImpl::MaybeScheduleLogDBDeployStats() {

  // There is a lock in the actual logger.
  if (!logger_ || options_.db_stats_log_interval < 0
      || host_name_.empty()) {
    return;
  }

  if(bg_logstats_scheduled_ || shutting_down_.Acquire_Load()) {
    // Already scheduled
  } else {
    int64_t current_ts = 0;
    Status st = env_->GetCurrentTime(&current_ts);
    if (!st.ok()) {
      return;
    }
    if ((current_ts - last_log_ts) < options_.db_stats_log_interval) {
      return;
    }
    last_log_ts = current_ts;
    bg_logstats_scheduled_ = true;
    env_->Schedule(&DBImpl::BGLogDBDeployStats, this);
  }
}

void DBImpl::BGLogDBDeployStats(void* db) {
  DBImpl* db_inst = reinterpret_cast<DBImpl*>(db);
  db_inst->LogDBDeployStats();
}

void DBImpl::LogDBDeployStats() {
  mutex_.Lock();

  if (shutting_down_.Acquire_Load()) {
    bg_logstats_scheduled_ = false;
    bg_cv_.SignalAll();
    mutex_.Unlock();
    return;
  }

  char tmp_ver[100];
  sprintf(tmp_ver, "%d.%d", kMajorVersion, kMinorVersion);
  std::string version_info(tmp_ver);

  uint64_t file_total_size = 0;
  uint32_t file_total_num = 0;
  for (int i = 0; i < versions_->NumberLevels(); i++) {
    file_total_num += versions_->NumLevelFiles(i);
    file_total_size += versions_->NumLevelBytes(i);
  }

  VersionSet::LevelSummaryStorage scratch;
  const char* file_num_summary = versions_->LevelSummary(&scratch);
  std::string file_num_per_level(file_num_summary);
  std::string data_size_per_level(file_num_summary);

  mutex_.Unlock();

  int64_t unix_ts;
  env_->GetCurrentTime(&unix_ts);

  logger_->Log_Deploy_Stats(version_info, host_name_,
      db_absolute_path_, file_total_size, file_total_num, file_num_per_level,
      data_size_per_level, unix_ts);

  mutex_.Lock();
  bg_logstats_scheduled_ = false;
  bg_cv_.SignalAll();
  mutex_.Unlock();
}

}
