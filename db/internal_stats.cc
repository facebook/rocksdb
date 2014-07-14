//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/internal_stats.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <vector>

#include "db/column_family.h"

namespace rocksdb {

namespace {
const double kMB = 1048576.0;
const double kGB = kMB * 1024;

void PrintLevelStatsHeader(char* buf, size_t len) {
  snprintf(
      buf, len,
      "\n** Compaction Stats **\n"
      "Level Files Size(MB) Score Read(GB)  Rn(GB) Rnp1(GB) "
      "Write(GB) Wnew(GB) RW-Amp W-Amp Rd(MB/s) Wr(MB/s)  Rn(cnt) "
      "Rnp1(cnt) Wnp1(cnt) Wnew(cnt)  Comp(sec) Comp(cnt) Avg(sec) "
      "Stall(sec) Stall(cnt) Avg(ms)\n"
      "--------------------------------------------------------------------"
      "--------------------------------------------------------------------"
      "--------------------------------------------------------------------\n");
}

void PrintLevelStats(char* buf, size_t len, const std::string& name,
    int num_files, double total_file_size, double score,
    double stall_us, uint64_t stalls,
    const InternalStats::CompactionStats& stats) {
  int64_t bytes_read = stats.bytes_readn + stats.bytes_readnp1;
  int64_t bytes_new = stats.bytes_written - stats.bytes_readnp1;
  double rw_amp = (stats.bytes_readn== 0) ? 0.0
          : (stats.bytes_written + bytes_read) / (double)stats.bytes_readn;
  double w_amp = (stats.bytes_readn == 0) ? 0.0
          : stats.bytes_written / (double)stats.bytes_readn;
  double elapsed = (stats.micros + 1) / 1000000.0;

  snprintf(buf, len,
    "%4s %5d %8.0f %5.1f " /* Level, Files, Size(MB), Score */
    "%8.1f " /* Read(GB) */
    "%7.1f " /* Rn(GB) */
    "%8.1f " /* Rnp1(GB) */
    "%9.1f " /* Write(GB) */
    "%8.1f " /* Wnew(GB) */
    "%6.1f " /* RW-Amp */
    "%5.1f " /* W-Amp */
    "%8.1f " /* Rd(MB/s) */
    "%8.1f " /* Wr(MB/s) */
    "%8d " /* Rn(cnt) */
    "%9d " /* Rnp1(cnt) */
    "%9d " /* Wnp1(cnt) */
    "%9d " /* Wnew(cnt) */
    "%10.0f " /* Comp(sec) */
    "%9d " /* Comp(cnt) */
    "%8.3f " /* Avg(sec) */
    "%10.2f " /* Sta(sec) */
    "%10" PRIu64 " " /* Sta(cnt) */
    "%7.2f\n" /* Avg(ms) */,
    name.c_str(), num_files, total_file_size / kMB, score,
    bytes_read / kGB,
    stats.bytes_readn / kGB,
    stats.bytes_readnp1 / kGB,
    stats.bytes_written / kGB,
    bytes_new / kGB,
    rw_amp,
    w_amp,
    bytes_read / kMB / elapsed,
    stats.bytes_written / kMB / elapsed,
    stats.files_in_leveln,
    stats.files_in_levelnp1,
    stats.files_out_levelnp1,
    stats.files_out_levelnp1 - stats.files_in_levelnp1,
    stats.micros / 1000000.0,
    stats.count,
    stats.count == 0 ? 0 : stats.micros / 1000000.0 / stats.count,
    stall_us / 1000000.0,
    stalls,
    stalls == 0 ? 0 : stall_us / 1000.0 / stalls);
}

}

DBPropertyType GetPropertyType(const Slice& property) {
  Slice in = property;
  Slice prefix("rocksdb.");
  if (!in.starts_with(prefix)) return kUnknown;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    return kNumFilesAtLevel;
  } else if (in == "levelstats") {
    return kLevelStats;
  } else if (in == "stats") {
    return kStats;
  } else if (in == "sstables") {
    return kSsTables;
  } else if (in == "num-immutable-mem-table") {
    return kNumImmutableMemTable;
  } else if (in == "mem-table-flush-pending") {
    return kMemtableFlushPending;
  } else if (in == "compaction-pending") {
    return kCompactionPending;
  } else if (in == "background-errors") {
    return kBackgroundErrors;
  } else if (in == "cur-size-active-mem-table") {
    return kCurSizeActiveMemTable;
  } else if (in == "num-entries-active-mem-table") {
    return kNumEntriesInMutableMemtable;
  } else if (in == "num-entries-imm-mem-tables") {
    return kNumEntriesInImmutableMemtable;
  }
  return kUnknown;
}

bool InternalStats::GetProperty(DBPropertyType property_type,
                                const Slice& property, std::string* value,
                                ColumnFamilyData* cfd) {
  Version* current = cfd->current();
  Slice in = property;

  switch (property_type) {
    case kNumFilesAtLevel: {
      in.remove_prefix(strlen("rocksdb.num-files-at-level"));
      uint64_t level;
      bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
      if (!ok || (int)level >= number_levels_) {
        return false;
      } else {
        char buf[100];
        snprintf(buf, sizeof(buf), "%d",
                 current->NumLevelFiles(static_cast<int>(level)));
        *value = buf;
        return true;
      }
    }
    case kLevelStats: {
      char buf[1000];
      snprintf(buf, sizeof(buf),
               "Level Files Size(MB)\n"
               "--------------------\n");
      value->append(buf);

      for (int level = 0; level < number_levels_; level++) {
        snprintf(buf, sizeof(buf), "%3d %8d %8.0f\n", level,
                 current->NumLevelFiles(level),
                 current->NumLevelBytes(level) / kMB);
        value->append(buf);
      }
      return true;
    }
    case kStats: {
      char buf[1000];

      uint64_t wal_bytes = 0;
      uint64_t wal_synced = 0;
      uint64_t user_bytes_written = 0;
      uint64_t write_other = 0;
      uint64_t write_self = 0;
      uint64_t write_with_wal = 0;
      uint64_t micros_up = env_->NowMicros() - started_at_;
      // Add "+1" to make sure seconds_up is > 0 and avoid NaN later
      double seconds_up = (micros_up + 1) / 1000000.0;
      uint64_t total_slowdown = 0;
      uint64_t total_slowdown_count = 0;
      uint64_t interval_bytes_written = 0;
      uint64_t interval_bytes_read = 0;
      uint64_t interval_bytes_new = 0;
      double interval_seconds_up = 0;

      if (statistics_) {
        wal_bytes = statistics_->getTickerCount(WAL_FILE_BYTES);
        wal_synced = statistics_->getTickerCount(WAL_FILE_SYNCED);
        user_bytes_written = statistics_->getTickerCount(BYTES_WRITTEN);
        write_other = statistics_->getTickerCount(WRITE_DONE_BY_OTHER);
        write_self = statistics_->getTickerCount(WRITE_DONE_BY_SELF);
        write_with_wal = statistics_->getTickerCount(WRITE_WITH_WAL);
      }

      PrintLevelStatsHeader(buf, sizeof(buf));
      value->append(buf);

      CompactionStats stats_total(0);
      int total_files = 0;
      double total_file_size = 0;
      uint64_t total_stalls = 0;
      double total_stall_us = 0;
      int level_printed = 0;
      for (int level = 0; level < number_levels_; level++) {
        int files = current->NumLevelFiles(level);
        total_files += files;
        if (stats_[level].micros > 0 || files > 0) {
          ++level_printed;
          uint64_t stalls = level == 0 ? (stall_counts_[LEVEL0_SLOWDOWN] +
                                          stall_counts_[LEVEL0_NUM_FILES] +
                                          stall_counts_[MEMTABLE_COMPACTION])
                                       : stall_leveln_slowdown_count_[level];

          double stall_us = level == 0 ? (stall_micros_[LEVEL0_SLOWDOWN] +
                                          stall_micros_[LEVEL0_NUM_FILES] +
                                          stall_micros_[MEMTABLE_COMPACTION])
                                       : stall_leveln_slowdown_[level];

          stats_total.Add(stats_[level]);
          total_file_size += current->NumLevelBytes(level);
          total_stall_us += stall_us;
          total_stalls += stalls;
          total_slowdown += stall_leveln_slowdown_[level];
          total_slowdown_count += stall_leveln_slowdown_count_[level];
          double score = current->NumLevelBytes(level) /
                cfd->compaction_picker()->MaxBytesForLevel(level);
          PrintLevelStats(buf, sizeof(buf), "L" + std::to_string(level), files,
              current->NumLevelBytes(level), score, stall_us, stalls,
              stats_[level]);
          value->append(buf);
        }
      }
      // Stats summary across levels
      if (level_printed > 1) {
        PrintLevelStats(buf, sizeof(buf), "Sum", total_files, total_file_size,
            0, total_stall_us, total_stalls, stats_total);
        value->append(buf);
      }

      uint64_t total_bytes_read =
          stats_total.bytes_readn + stats_total.bytes_readnp1;
      uint64_t total_bytes_written = stats_total.bytes_written;

      interval_bytes_new = user_bytes_written - last_stats_.ingest_bytes_;
      interval_bytes_read =
          total_bytes_read - last_stats_.compaction_bytes_read_;
      interval_bytes_written =
          total_bytes_written - last_stats_.compaction_bytes_written_;
      interval_seconds_up = seconds_up - last_stats_.seconds_up_;

      snprintf(buf, sizeof(buf), "\nUptime(secs): %.1f total, %.1f interval\n",
               seconds_up, interval_seconds_up);
      value->append(buf);

      snprintf(buf, sizeof(buf),
               "Writes cumulative: %llu total, %llu batches, "
               "%.1f per batch, %.2f ingest GB\n",
               (unsigned long long)(write_other + write_self),
               (unsigned long long)write_self,
               (write_other + write_self) / (double)(write_self + 1),
               user_bytes_written / kGB);
      value->append(buf);

      snprintf(buf, sizeof(buf),
               "WAL cumulative: %llu WAL writes, %llu WAL syncs, "
               "%.2f writes per sync, %.2f GB written\n",
               (unsigned long long)write_with_wal,
               (unsigned long long)wal_synced,
               write_with_wal / (double)(wal_synced + 1),
               wal_bytes / kGB);
      value->append(buf);

      snprintf(buf, sizeof(buf),
               "Compaction IO cumulative (GB): "
               "%.2f new, %.2f read, %.2f write, %.2f read+write\n",
               user_bytes_written / kGB,
               total_bytes_read / kGB,
               total_bytes_written / kGB,
               (total_bytes_read + total_bytes_written) / kGB);
      value->append(buf);

      snprintf(
          buf, sizeof(buf),
          "Compaction IO cumulative (MB/sec): "
          "%.1f new, %.1f read, %.1f write, %.1f read+write\n",
          user_bytes_written / kMB / seconds_up,
          total_bytes_read / kMB / seconds_up,
          total_bytes_written / kMB / seconds_up,
          (total_bytes_read + total_bytes_written) / kMB / seconds_up);
      value->append(buf);

      // +1 to avoid divide by 0 and NaN
      snprintf(
          buf, sizeof(buf),
          "Amplification cumulative: %.1f write, %.1f compaction\n",
          (double)(total_bytes_written + wal_bytes) / (user_bytes_written + 1),
          (double)(total_bytes_written + total_bytes_read + wal_bytes) /
              (user_bytes_written + 1));
      value->append(buf);

      uint64_t interval_write_other = write_other - last_stats_.write_other_;
      uint64_t interval_write_self = write_self - last_stats_.write_self_;

      snprintf(buf, sizeof(buf),
               "Writes interval: %llu total, %llu batches, "
               "%.1f per batch, %.1f ingest MB\n",
               (unsigned long long)(interval_write_other + interval_write_self),
               (unsigned long long)interval_write_self,
               (double)(interval_write_other + interval_write_self) /
                   (interval_write_self + 1),
               (user_bytes_written - last_stats_.ingest_bytes_) / kMB);
      value->append(buf);

      uint64_t interval_write_with_wal =
          write_with_wal - last_stats_.write_with_wal_;

      uint64_t interval_wal_synced = wal_synced - last_stats_.wal_synced_;
      uint64_t interval_wal_bytes = wal_bytes - last_stats_.wal_bytes_;

      snprintf(buf, sizeof(buf),
               "WAL interval: %llu WAL writes, %llu WAL syncs, "
               "%.2f writes per sync, %.2f MB written\n",
               (unsigned long long)interval_write_with_wal,
               (unsigned long long)interval_wal_synced,
               interval_write_with_wal / (double)(interval_wal_synced + 1),
               interval_wal_bytes / kGB);
      value->append(buf);

      snprintf(buf, sizeof(buf),
               "Compaction IO interval (MB): "
               "%.2f new, %.2f read, %.2f write, %.2f read+write\n",
               interval_bytes_new / kMB, interval_bytes_read / kMB,
               interval_bytes_written / kMB,
               (interval_bytes_read + interval_bytes_written) / kMB);
      value->append(buf);

      snprintf(buf, sizeof(buf),
               "Compaction IO interval (MB/sec): "
               "%.1f new, %.1f read, %.1f write, %.1f read+write\n",
               interval_bytes_new / kMB / interval_seconds_up,
               interval_bytes_read / kMB / interval_seconds_up,
               interval_bytes_written / kMB / interval_seconds_up,
               (interval_bytes_read + interval_bytes_written) / kMB /
                   interval_seconds_up);
      value->append(buf);

      // +1 to avoid divide by 0 and NaN
      snprintf(
          buf, sizeof(buf),
          "Amplification interval: %.1f write, %.1f compaction\n",
          (double)(interval_bytes_written + interval_wal_bytes) /
              (interval_bytes_new + 1),
          (double)(interval_bytes_written + interval_bytes_read + interval_wal_bytes) /
              (interval_bytes_new + 1));
      value->append(buf);

      snprintf(buf, sizeof(buf),
               "Stalls(secs): %.3f level0_slowdown, %.3f level0_numfiles, "
               "%.3f memtable_compaction, %.3f leveln_slowdown\n",
               stall_micros_[LEVEL0_SLOWDOWN] / 1000000.0,
               stall_micros_[LEVEL0_NUM_FILES] / 1000000.0,
               stall_micros_[MEMTABLE_COMPACTION] / 1000000.0,
               total_slowdown / 1000000.0);
      value->append(buf);

      snprintf(buf, sizeof(buf),
               "Stalls(count): %lu level0_slowdown, %lu level0_numfiles, "
               "%lu memtable_compaction, %lu leveln_slowdown\n",
               (unsigned long)stall_counts_[LEVEL0_SLOWDOWN],
               (unsigned long)stall_counts_[LEVEL0_NUM_FILES],
               (unsigned long)stall_counts_[MEMTABLE_COMPACTION],
               (unsigned long)total_slowdown_count);
      value->append(buf);

      last_stats_.compaction_bytes_read_ = total_bytes_read;
      last_stats_.compaction_bytes_written_ = total_bytes_written;
      last_stats_.ingest_bytes_ = user_bytes_written;
      last_stats_.seconds_up_ = seconds_up;
      last_stats_.wal_bytes_ = wal_bytes;
      last_stats_.wal_synced_ = wal_synced;
      last_stats_.write_with_wal_ = write_with_wal;
      last_stats_.write_other_ = write_other;
      last_stats_.write_self_ = write_self;

      return true;
    }
    case kSsTables:
      *value = current->DebugString();
      return true;
    case kNumImmutableMemTable:
      *value = std::to_string(cfd->imm()->size());
      return true;
    case kMemtableFlushPending:
      // Return number of mem tables that are ready to flush (made immutable)
      *value = std::to_string(cfd->imm()->IsFlushPending() ? 1 : 0);
      return true;
    case kCompactionPending:
      // 1 if the system already determines at least one compacdtion is needed.
      // 0 otherwise,
      *value = std::to_string(current->NeedsCompaction() ? 1 : 0);
      return true;
    case kBackgroundErrors:
      // Accumulated number of  errors in background flushes or compactions.
      *value = std::to_string(GetBackgroundErrorCount());
      return true;
    case kCurSizeActiveMemTable:
      // Current size of the active memtable
      *value = std::to_string(cfd->mem()->ApproximateMemoryUsage());
      return true;
    case kNumEntriesInMutableMemtable:
      // Current size of the active memtable
      *value = std::to_string(cfd->mem()->GetNumEntries());
      return true;
    case kNumEntriesInImmutableMemtable:
      // Current size of the active memtable
      *value = std::to_string(cfd->imm()->current()->GetTotalNumEntries());
      return true;
    default:
      return false;
  }
}

}  // namespace rocksdb
