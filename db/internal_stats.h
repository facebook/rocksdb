//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#pragma once
#include "rocksdb/statistics.h"
#include "util/statistics.h"
#include "db/version_set.h"

#include <vector>
#include <string>

namespace rocksdb {

class MemTableList;
class DBImpl;

enum DBPropertyType {
  kNumFilesAtLevel,  // Number of files at a specific level
  kLevelStats,       // Return number of files and total sizes of each level
  kStats,            // Return general statitistics of DB
  kSsTables,         // Return a human readable string of current SST files
  kNumImmutableMemTable,  // Return number of immutable mem tables
  kMemtableFlushPending,  // Return 1 if mem table flushing is pending,
                          // otherwise
                          // 0.
  kCompactionPending,     // Return 1 if a compaction is pending. Otherwise 0.
  kBackgroundErrors,      // Return accumulated background errors encountered.
  kCurSizeActiveMemTable,  // Return current size of the active memtable
  kUnknown,
};

extern DBPropertyType GetPropertyType(const Slice& property);

class InternalStats {
 public:
  enum WriteStallType {
    LEVEL0_SLOWDOWN,
    MEMTABLE_COMPACTION,
    LEVEL0_NUM_FILES,
    WRITE_STALLS_ENUM_MAX,
  };

  InternalStats(int num_levels, Env* env, Statistics* statistics)
      : compaction_stats_(num_levels),
        stall_micros_(WRITE_STALLS_ENUM_MAX, 0),
        stall_counts_(WRITE_STALLS_ENUM_MAX, 0),
        stall_leveln_slowdown_(num_levels, 0),
        stall_leveln_slowdown_count_(num_levels, 0),
        bg_error_count_(0),
        number_levels_(num_levels),
        statistics_(statistics),
        env_(env),
        started_at_(env->NowMicros()) {}

  // Per level compaction stats.  compaction_stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    uint64_t micros;

    // Bytes read from level N during compaction between levels N and N+1
    int64_t bytes_readn;

    // Bytes read from level N+1 during compaction between levels N and N+1
    int64_t bytes_readnp1;

    // Total bytes written during compaction between levels N and N+1
    int64_t bytes_written;

    // Files read from level N during compaction between levels N and N+1
    int files_in_leveln;

    // Files read from level N+1 during compaction between levels N and N+1
    int files_in_levelnp1;

    // Files written during compaction between levels N and N+1
    int files_out_levelnp1;

    // Number of compactions done
    int count;

    CompactionStats()
        : micros(0),
          bytes_readn(0),
          bytes_readnp1(0),
          bytes_written(0),
          files_in_leveln(0),
          files_in_levelnp1(0),
          files_out_levelnp1(0),
          count(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_readn += c.bytes_readn;
      this->bytes_readnp1 += c.bytes_readnp1;
      this->bytes_written += c.bytes_written;
      this->files_in_leveln += c.files_in_leveln;
      this->files_in_levelnp1 += c.files_in_levelnp1;
      this->files_out_levelnp1 += c.files_out_levelnp1;
      this->count += 1;
    }
  };

  void AddCompactionStats(int level, const CompactionStats& stats) {
    compaction_stats_[level].Add(stats);
  }

  void RecordWriteStall(WriteStallType write_stall_type, uint64_t micros) {
    stall_micros_[write_stall_type] += micros;
    stall_counts_[write_stall_type]++;
  }

  void RecordLevelNSlowdown(int level, uint64_t micros) {
    stall_leveln_slowdown_[level] += micros;
    stall_leveln_slowdown_count_[level] += micros;
  }

  uint64_t GetBackgroundErrorCount() const { return bg_error_count_; }

  uint64_t BumpAndGetBackgroundErrorCount() { return ++bg_error_count_; }

  bool GetProperty(DBPropertyType property_type, const Slice& property,
                   std::string* value, DBImpl* db);

 private:
  std::vector<CompactionStats> compaction_stats_;

  // Used to compute per-interval statistics
  struct StatsSnapshot {
    uint64_t compaction_bytes_read_;     // Bytes read by compaction
    uint64_t compaction_bytes_written_;  // Bytes written by compaction
    uint64_t ingest_bytes_;              // Bytes written by user
    uint64_t wal_bytes_;                 // Bytes written to WAL
    uint64_t wal_synced_;                // Number of times WAL is synced
    uint64_t write_with_wal_;            // Number of writes that request WAL
    // These count the number of writes processed by the calling thread or
    // another thread.
    uint64_t write_other_;
    uint64_t write_self_;
    double seconds_up_;

    StatsSnapshot()
        : compaction_bytes_read_(0),
          compaction_bytes_written_(0),
          ingest_bytes_(0),
          wal_bytes_(0),
          wal_synced_(0),
          write_with_wal_(0),
          write_other_(0),
          write_self_(0),
          seconds_up_(0) {}
  };

  // Counters from the previous time per-interval stats were computed
  StatsSnapshot last_stats_;

  // These count the number of microseconds for which MakeRoomForWrite stalls.
  std::vector<uint64_t> stall_micros_;
  std::vector<uint64_t> stall_counts_;
  std::vector<uint64_t> stall_leveln_slowdown_;
  std::vector<uint64_t> stall_leveln_slowdown_count_;

  // Total number of background errors encountered. Every time a flush task
  // or compaction task fails, this counter is incremented. The failure can
  // be caused by any possible reason, including file system errors, out of
  // resources, or input file corruption. Failing when retrying the same flush
  // or compaction will cause the counter to increase too.
  uint64_t bg_error_count_;

  int number_levels_;
  Statistics* statistics_;
  Env* env_;
  uint64_t started_at_;
};

}  // namespace rocksdb
