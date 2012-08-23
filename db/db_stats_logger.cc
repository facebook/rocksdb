// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"
#include <string>
#include <stdint.h>
#include "db/version_set.h"
#include "leveldb/db.h"
#include "leveldb/env.h"

namespace leveldb {

void DBImpl::MaybeScheduleLogDBDeployStats() {

  // There is a lock in the actual logger.
  if (!logger_ || options_.db_stats_log_interval < 0
      || host_name_.empty()) {
    return;
  }
  if (shutting_down_.Acquire_Load()) {
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
    env_->Schedule(&DBImpl::LogDBDeployStats, this);
  }
}

void DBImpl::LogDBDeployStats(void* db) {
  DBImpl* db_inst = reinterpret_cast<DBImpl*>(db);

  if (db_inst->shutting_down_.Acquire_Load()) {
    return;
  }

  std::string version_info;
  version_info += boost::lexical_cast<std::string>(kMajorVersion);
  version_info += ".";
  version_info += boost::lexical_cast<std::string>(kMinorVersion);
  std::string data_dir;
  db_inst->env_->GetAbsolutePath(db_inst->dbname_, &data_dir);

  uint64_t file_total_size = 0;
  uint32_t file_total_num = 0;
  for (int i = 0; i < db_inst->versions_->NumberLevels(); i++) {
    file_total_num += db_inst->versions_->NumLevelFiles(i);
    file_total_size += db_inst->versions_->NumLevelBytes(i);
  }

  VersionSet::LevelSummaryStorage scratch;
  const char* file_num_summary = db_inst->versions_->LevelSummary(&scratch);
  std::string file_num_per_level(file_num_summary);
  const char* file_size_summary = db_inst->versions_->LevelDataSizeSummary(
      &scratch);
  std::string data_size_per_level(file_num_summary);
  int64_t unix_ts;
  db_inst->env_->GetCurrentTime(&unix_ts);

  db_inst->logger_->Log_Deploy_Stats(version_info, db_inst->host_name_,
      data_dir, file_total_size, file_total_num, file_num_per_level,
      data_size_per_level, unix_ts);
}

}
