//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/internal_stats.h"
#include "db/column_family.h"

#include <vector>

namespace rocksdb {

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
                 current->NumLevelBytes(level) / 1048576.0);
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
      uint64_t total_bytes_written = 0;
      uint64_t total_bytes_read = 0;
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

      snprintf(
          buf, sizeof(buf),
          "                               Compactions\n"
          "Level  Files Size(MB) Score Time(sec)  Read(MB) Write(MB)    Rn(MB) "
          " "
          "Rnp1(MB)  Wnew(MB) RW-Amplify Read(MB/s) Write(MB/s)      Rn     "
          "Rnp1 "
          "    Wnp1     NewW    Count   msComp   msStall  Ln-stall Stall-cnt\n"
          "--------------------------------------------------------------------"
          "--"
          "--------------------------------------------------------------------"
          "--"
          "----------------------------------------------------------------\n");
      value->append(buf);
      for (int level = 0; level < number_levels_; level++) {
        int files = current->NumLevelFiles(level);
        if (compaction_stats_[level].micros > 0 || files > 0) {
          int64_t bytes_read = compaction_stats_[level].bytes_readn +
                               compaction_stats_[level].bytes_readnp1;
          int64_t bytes_new = compaction_stats_[level].bytes_written -
                              compaction_stats_[level].bytes_readnp1;
          double amplify =
              (compaction_stats_[level].bytes_readn == 0)
                  ? 0.0
                  : (compaction_stats_[level].bytes_written +
                     compaction_stats_[level].bytes_readnp1 +
                     compaction_stats_[level].bytes_readn) /
                        (double)compaction_stats_[level].bytes_readn;

          total_bytes_read += bytes_read;
          total_bytes_written += compaction_stats_[level].bytes_written;

          uint64_t stalls = level == 0 ? (stall_counts_[LEVEL0_SLOWDOWN] +
                                          stall_counts_[LEVEL0_NUM_FILES] +
                                          stall_counts_[MEMTABLE_COMPACTION])
                                       : stall_leveln_slowdown_count_[level];

          double stall_us = level == 0 ? (stall_micros_[LEVEL0_SLOWDOWN] +
                                          stall_micros_[LEVEL0_NUM_FILES] +
                                          stall_micros_[MEMTABLE_COMPACTION])
                                       : stall_leveln_slowdown_[level];

          snprintf(buf, sizeof(buf),
                   "%3d %8d %8.0f %5.1f %9.0f %9.0f %9.0f %9.0f %9.0f %9.0f "
                   "%10.1f %9.1f %11.1f %8d %8d %8d %8d %8d %8d %9.1f %9.1f "
                   "%9lu\n",
                   level, files, current->NumLevelBytes(level) / 1048576.0,
                   current->NumLevelBytes(level) /
                       cfd->compaction_picker()->MaxBytesForLevel(level),
                   compaction_stats_[level].micros / 1e6,
                   bytes_read / 1048576.0,
                   compaction_stats_[level].bytes_written / 1048576.0,
                   compaction_stats_[level].bytes_readn / 1048576.0,
                   compaction_stats_[level].bytes_readnp1 / 1048576.0,
                   bytes_new / 1048576.0, amplify,
                   // +1 to avoid division by 0
                   (bytes_read / 1048576.0) /
                       ((compaction_stats_[level].micros + 1) / 1000000.0),
                   (compaction_stats_[level].bytes_written / 1048576.0) /
                       ((compaction_stats_[level].micros + 1) / 1000000.0),
                   compaction_stats_[level].files_in_leveln,
                   compaction_stats_[level].files_in_levelnp1,
                   compaction_stats_[level].files_out_levelnp1,
                   compaction_stats_[level].files_out_levelnp1 -
                       compaction_stats_[level].files_in_levelnp1,
                   compaction_stats_[level].count,
                   (int)((double)compaction_stats_[level].micros / 1000.0 /
                         (compaction_stats_[level].count + 1)),
                   (double)stall_us / 1000.0 / (stalls + 1),
                   stall_us / 1000000.0, (unsigned long)stalls);
          total_slowdown += stall_leveln_slowdown_[level];
          total_slowdown_count += stall_leveln_slowdown_count_[level];
          value->append(buf);
        }
      }

      interval_bytes_new = user_bytes_written - last_stats_.ingest_bytes_;
      interval_bytes_read =
          total_bytes_read - last_stats_.compaction_bytes_read_;
      interval_bytes_written =
          total_bytes_written - last_stats_.compaction_bytes_written_;
      interval_seconds_up = seconds_up - last_stats_.seconds_up_;

      snprintf(buf, sizeof(buf), "Uptime(secs): %.1f total, %.1f interval\n",
               seconds_up, interval_seconds_up);
      value->append(buf);

      snprintf(buf, sizeof(buf),
               "Writes cumulative: %llu total, %llu batches, "
               "%.1f per batch, %.2f ingest GB\n",
               (unsigned long long)(write_other + write_self),
               (unsigned long long)write_self,
               (write_other + write_self) / (double)(write_self + 1),
               user_bytes_written / (1048576.0 * 1024));
      value->append(buf);

      snprintf(buf, sizeof(buf),
               "WAL cumulative: %llu WAL writes, %llu WAL syncs, "
               "%.2f writes per sync, %.2f GB written\n",
               (unsigned long long)write_with_wal,
               (unsigned long long)wal_synced,
               write_with_wal / (double)(wal_synced + 1),
               wal_bytes / (1048576.0 * 1024));
      value->append(buf);

      snprintf(buf, sizeof(buf),
               "Compaction IO cumulative (GB): "
               "%.2f new, %.2f read, %.2f write, %.2f read+write\n",
               user_bytes_written / (1048576.0 * 1024),
               total_bytes_read / (1048576.0 * 1024),
               total_bytes_written / (1048576.0 * 1024),
               (total_bytes_read + total_bytes_written) / (1048576.0 * 1024));
      value->append(buf);

      snprintf(
          buf, sizeof(buf),
          "Compaction IO cumulative (MB/sec): "
          "%.1f new, %.1f read, %.1f write, %.1f read+write\n",
          user_bytes_written / 1048576.0 / seconds_up,
          total_bytes_read / 1048576.0 / seconds_up,
          total_bytes_written / 1048576.0 / seconds_up,
          (total_bytes_read + total_bytes_written) / 1048576.0 / seconds_up);
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
               (user_bytes_written - last_stats_.ingest_bytes_) / 1048576.0);
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
               interval_wal_bytes / (1048576.0 * 1024));
      value->append(buf);

      snprintf(buf, sizeof(buf),
               "Compaction IO interval (MB): "
               "%.2f new, %.2f read, %.2f write, %.2f read+write\n",
               interval_bytes_new / 1048576.0, interval_bytes_read / 1048576.0,
               interval_bytes_written / 1048576.0,
               (interval_bytes_read + interval_bytes_written) / 1048576.0);
      value->append(buf);

      snprintf(buf, sizeof(buf),
               "Compaction IO interval (MB/sec): "
               "%.1f new, %.1f read, %.1f write, %.1f read+write\n",
               interval_bytes_new / 1048576.0 / interval_seconds_up,
               interval_bytes_read / 1048576.0 / interval_seconds_up,
               interval_bytes_written / 1048576.0 / interval_seconds_up,
               (interval_bytes_read + interval_bytes_written) / 1048576.0 /
                   interval_seconds_up);
      value->append(buf);

      // +1 to avoid divide by 0 and NaN
      snprintf(
          buf, sizeof(buf),
          "Amplification interval: %.1f write, %.1f compaction\n",
          (double)(interval_bytes_written + wal_bytes) /
              (interval_bytes_new + 1),
          (double)(interval_bytes_written + interval_bytes_read + wal_bytes) /
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
    /////////////
    case kBackgroundErrors:
      // Accumulated number of  errors in background flushes or compactions.
      *value = std::to_string(GetBackgroundErrorCount());
      return true;
    /////////
    default:
      return false;
  }
}

}  // namespace rocksdb
