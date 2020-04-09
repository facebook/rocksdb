//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <limits>
#include <set>
#include <utility>
#include <vector>
#include <string>
#include <memory>

#include "db/version_set.h"
#include "file/file_util.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

#ifndef ROCKSDB_LITE

// WAL manager provides the abstraction for reading the WAL files as a single
// unit. Internally, it opens and reads the files using Reader or Writer
// abstraction.
class WalManager {
 public:
  WalManager(const ImmutableDBOptions& db_options,
             const FileOptions& file_options, const bool seq_per_batch = false)
      : db_options_(db_options),
        file_options_(file_options),
        env_(db_options.env),
        fs_(db_options.fs.get()),
        purge_wal_files_last_run_(0),
        seq_per_batch_(seq_per_batch),
        wal_in_db_path_(IsWalDirSameAsDBPath(&db_options)) {}

  Status GetSortedWalFiles(VectorLogPtr& files);

  // Allow user to tail transaction log to find all recent changes to the
  // database that are newer than `seq_number`.
  Status GetUpdatesSince(
      SequenceNumber seq_number, std::unique_ptr<TransactionLogIterator>* iter,
      const TransactionLogIterator::ReadOptions& read_options,
      VersionSet* version_set);

  void PurgeObsoleteWALFiles();

  void ArchiveWALFile(const std::string& fname, uint64_t number);

  Status DeleteFile(const std::string& fname, uint64_t number);

  Status GetLiveWalFile(uint64_t number, std::unique_ptr<LogFile>* log_file);

  Status TEST_ReadFirstRecord(const WalFileType type, const uint64_t number,
                              SequenceNumber* sequence) {
    return ReadFirstRecord(type, number, sequence);
  }

  Status TEST_ReadFirstLine(const std::string& fname, const uint64_t number,
                            SequenceNumber* sequence) {
    return ReadFirstLine(fname, number, sequence);
  }

 private:
  Status GetSortedWalsOfType(const std::string& path, VectorLogPtr& log_files,
                             WalFileType type);
  // Requires: all_logs should be sorted with earliest log file first
  // Retains all log files in all_logs which contain updates with seq no.
  // Greater Than or Equal to the requested SequenceNumber.
  Status RetainProbableWalFiles(VectorLogPtr& all_logs,
                                const SequenceNumber target);

  Status ReadFirstRecord(const WalFileType type, const uint64_t number,
                         SequenceNumber* sequence);

  Status ReadFirstLine(const std::string& fname, const uint64_t number,
                       SequenceNumber* sequence);

  // ------- state from DBImpl ------
  const ImmutableDBOptions& db_options_;
  const FileOptions file_options_;
  Env* env_;
  FileSystem* fs_;

  // ------- WalManager state -------
  // cache for ReadFirstRecord() calls
  std::unordered_map<uint64_t, SequenceNumber> read_first_record_cache_;
  port::Mutex read_first_record_cache_mutex_;

  // last time when PurgeObsoleteWALFiles ran.
  uint64_t purge_wal_files_last_run_;

  bool seq_per_batch_;

  bool wal_in_db_path_;

  // obsolete files will be deleted every this seconds if ttl deletion is
  // enabled and archive size_limit is disabled.
  static const uint64_t kDefaultIntervalToDeleteObsoleteWAL = 600;
};

#endif  // ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE
