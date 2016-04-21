//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/internal_stats.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <string>
#include <algorithm>
#include <utility>
#include <vector>
#include "db/column_family.h"

#include "db/db_impl.h"
#include "util/string_util.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
namespace {
const double kMB = 1048576.0;
const double kGB = kMB * 1024;
const double kMicrosInSec = 1000000.0;

void PrintLevelStatsHeader(char* buf, size_t len, const std::string& cf_name) {
  snprintf(
      buf, len,
      "\n** Compaction Stats [%s] **\n"
      "Level    Files   Size(MB) Score Read(GB)  Rn(GB) Rnp1(GB) "
      "Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) "
      "Comp(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop\n"
      "--------------------------------------------------------------------"
      "-----------------------------------------------------------"
      "--------------------------------------\n",
      cf_name.c_str());
}

void PrintLevelStats(char* buf, size_t len, const std::string& name,
  int num_files, int being_compacted, double total_file_size,
  double score, double w_amp,
  const InternalStats::CompactionStats& stats) {
  uint64_t bytes_read =
      stats.bytes_read_non_output_levels + stats.bytes_read_output_level;
  int64_t bytes_new =
      stats.bytes_written - stats.bytes_read_output_level;
  double elapsed = (stats.micros + 1) / kMicrosInSec;
  std::string num_input_records = NumberToHumanString(stats.num_input_records);
  std::string num_dropped_records =
      NumberToHumanString(stats.num_dropped_records);

  snprintf(buf, len,
           "%4s %6d/%-3d %8.2f %5.1f " /* Level, Files, Size(MB), Score */
           "%8.1f "                    /* Read(GB) */
           "%7.1f "                    /* Rn(GB) */
           "%8.1f "                    /* Rnp1(GB) */
           "%9.1f "                    /* Write(GB) */
           "%8.1f "                    /* Wnew(GB) */
           "%9.1f "                    /* Moved(GB) */
           "%5.1f "                    /* W-Amp */
           "%8.1f "                    /* Rd(MB/s) */
           "%8.1f "                    /* Wr(MB/s) */
           "%9.0f "                    /* Comp(sec) */
           "%9d "                      /* Comp(cnt) */
           "%8.3f "                    /* Avg(sec) */
           "%7s "   /* KeyIn */
           "%6s\n", /* KeyDrop */
           name.c_str(),
           num_files, being_compacted, total_file_size / kMB, score,
           bytes_read / kGB, stats.bytes_read_non_output_levels / kGB,
           stats.bytes_read_output_level / kGB, stats.bytes_written / kGB,
           bytes_new / kGB, stats.bytes_moved / kGB, w_amp,
           bytes_read / kMB / elapsed, stats.bytes_written / kMB / elapsed,
           stats.micros / kMicrosInSec, stats.count,
           stats.count == 0 ? 0 : stats.micros / kMicrosInSec / stats.count,
           num_input_records.c_str(), num_dropped_records.c_str());
}

// Assumes that trailing numbers represent an optional argument. This requires
// property names to not end with numbers.
std::pair<Slice, Slice> GetPropertyNameAndArg(const Slice& property) {
  Slice name = property, arg = property;
  size_t sfx_len = 0;
  while (sfx_len < property.size() &&
         isdigit(property[property.size() - sfx_len - 1])) {
    ++sfx_len;
  }
  name.remove_suffix(sfx_len);
  arg.remove_prefix(property.size() - sfx_len);
  return {name, arg};
}
}  // anonymous namespace

static const std::string rocksdb_prefix = "rocksdb.";

static const std::string num_files_at_level_prefix = "num-files-at-level";
static const std::string compression_ratio_at_level_prefix =
    "compression-ratio-at-level";
static const std::string allstats = "stats";
static const std::string sstables = "sstables";
static const std::string cfstats = "cfstats";
static const std::string dbstats = "dbstats";
static const std::string levelstats = "levelstats";
static const std::string num_immutable_mem_table = "num-immutable-mem-table";
static const std::string num_immutable_mem_table_flushed =
    "num-immutable-mem-table-flushed";
static const std::string mem_table_flush_pending = "mem-table-flush-pending";
static const std::string compaction_pending = "compaction-pending";
static const std::string background_errors = "background-errors";
static const std::string cur_size_active_mem_table =
                          "cur-size-active-mem-table";
static const std::string cur_size_all_mem_tables = "cur-size-all-mem-tables";
static const std::string size_all_mem_tables = "size-all-mem-tables";
static const std::string num_entries_active_mem_table =
                          "num-entries-active-mem-table";
static const std::string num_entries_imm_mem_tables =
                          "num-entries-imm-mem-tables";
static const std::string num_deletes_active_mem_table =
                          "num-deletes-active-mem-table";
static const std::string num_deletes_imm_mem_tables =
                          "num-deletes-imm-mem-tables";
static const std::string estimate_num_keys = "estimate-num-keys";
static const std::string estimate_table_readers_mem =
                          "estimate-table-readers-mem";
static const std::string is_file_deletions_enabled =
                          "is-file-deletions-enabled";
static const std::string num_snapshots = "num-snapshots";
static const std::string oldest_snapshot_time = "oldest-snapshot-time";
static const std::string num_live_versions = "num-live-versions";
static const std::string current_version_number =
    "current-super-version-number";
static const std::string estimate_live_data_size = "estimate-live-data-size";
static const std::string base_level = "base-level";
static const std::string total_sst_files_size = "total-sst-files-size";
static const std::string estimate_pending_comp_bytes =
    "estimate-pending-compaction-bytes";
static const std::string aggregated_table_properties =
    "aggregated-table-properties";
static const std::string aggregated_table_properties_at_level =
    aggregated_table_properties + "-at-level";
static const std::string num_running_compactions = "num-running-compactions";
static const std::string num_running_flushes = "num-running-flushes";

const std::string DB::Properties::kNumFilesAtLevelPrefix =
                      rocksdb_prefix + num_files_at_level_prefix;
const std::string DB::Properties::kCompressionRatioAtLevelPrefix =
                      rocksdb_prefix + compression_ratio_at_level_prefix;
const std::string DB::Properties::kStats = rocksdb_prefix + allstats;
const std::string DB::Properties::kSSTables = rocksdb_prefix + sstables;
const std::string DB::Properties::kCFStats = rocksdb_prefix + cfstats;
const std::string DB::Properties::kDBStats = rocksdb_prefix + dbstats;
const std::string DB::Properties::kLevelStats = rocksdb_prefix + levelstats;
const std::string DB::Properties::kNumImmutableMemTable =
                      rocksdb_prefix + num_immutable_mem_table;
const std::string DB::Properties::kNumImmutableMemTableFlushed =
    rocksdb_prefix + num_immutable_mem_table_flushed;
const std::string DB::Properties::kMemTableFlushPending =
                      rocksdb_prefix + mem_table_flush_pending;
const std::string DB::Properties::kCompactionPending =
                      rocksdb_prefix + compaction_pending;
const std::string DB::Properties::kNumRunningCompactions =
    rocksdb_prefix + num_running_compactions;
const std::string DB::Properties::kNumRunningFlushes =
    rocksdb_prefix + num_running_flushes;
const std::string DB::Properties::kBackgroundErrors =
                      rocksdb_prefix + background_errors;
const std::string DB::Properties::kCurSizeActiveMemTable =
                      rocksdb_prefix + cur_size_active_mem_table;
const std::string DB::Properties::kCurSizeAllMemTables =
    rocksdb_prefix + cur_size_all_mem_tables;
const std::string DB::Properties::kSizeAllMemTables =
    rocksdb_prefix + size_all_mem_tables;
const std::string DB::Properties::kNumEntriesActiveMemTable =
                      rocksdb_prefix + num_entries_active_mem_table;
const std::string DB::Properties::kNumEntriesImmMemTables =
                      rocksdb_prefix + num_entries_imm_mem_tables;
const std::string DB::Properties::kNumDeletesActiveMemTable =
                      rocksdb_prefix + num_deletes_active_mem_table;
const std::string DB::Properties::kNumDeletesImmMemTables =
                      rocksdb_prefix + num_deletes_imm_mem_tables;
const std::string DB::Properties::kEstimateNumKeys =
                      rocksdb_prefix + estimate_num_keys;
const std::string DB::Properties::kEstimateTableReadersMem =
                      rocksdb_prefix + estimate_table_readers_mem;
const std::string DB::Properties::kIsFileDeletionsEnabled =
                      rocksdb_prefix + is_file_deletions_enabled;
const std::string DB::Properties::kNumSnapshots =
                      rocksdb_prefix + num_snapshots;
const std::string DB::Properties::kOldestSnapshotTime =
                      rocksdb_prefix + oldest_snapshot_time;
const std::string DB::Properties::kNumLiveVersions =
                      rocksdb_prefix + num_live_versions;
const std::string DB::Properties::kCurrentSuperVersionNumber =
    rocksdb_prefix + current_version_number;
const std::string DB::Properties::kEstimateLiveDataSize =
                      rocksdb_prefix + estimate_live_data_size;
const std::string DB::Properties::kTotalSstFilesSize =
                      rocksdb_prefix + total_sst_files_size;
const std::string DB::Properties::kBaseLevel = rocksdb_prefix + base_level;
const std::string DB::Properties::kEstimatePendingCompactionBytes =
    rocksdb_prefix + estimate_pending_comp_bytes;
const std::string DB::Properties::kAggregatedTableProperties =
    rocksdb_prefix + aggregated_table_properties;
const std::string DB::Properties::kAggregatedTablePropertiesAtLevel =
    rocksdb_prefix + aggregated_table_properties_at_level;

const std::unordered_map<std::string,
                         DBPropertyInfo> InternalStats::ppt_name_to_info = {
    {DB::Properties::kNumFilesAtLevelPrefix,
     {false, &InternalStats::HandleNumFilesAtLevel, nullptr}},
    {DB::Properties::kCompressionRatioAtLevelPrefix,
     {false, &InternalStats::HandleCompressionRatioAtLevelPrefix, nullptr}},
    {DB::Properties::kLevelStats,
     {false, &InternalStats::HandleLevelStats, nullptr}},
    {DB::Properties::kStats, {false, &InternalStats::HandleStats, nullptr}},
    {DB::Properties::kCFStats, {false, &InternalStats::HandleCFStats, nullptr}},
    {DB::Properties::kDBStats, {false, &InternalStats::HandleDBStats, nullptr}},
    {DB::Properties::kSSTables,
     {false, &InternalStats::HandleSsTables, nullptr}},
    {DB::Properties::kAggregatedTableProperties,
     {false, &InternalStats::HandleAggregatedTableProperties, nullptr}},
    {DB::Properties::kAggregatedTablePropertiesAtLevel,
     {false, &InternalStats::HandleAggregatedTablePropertiesAtLevel, nullptr}},
    {DB::Properties::kNumImmutableMemTable,
     {false, nullptr, &InternalStats::HandleNumImmutableMemTable}},
    {DB::Properties::kNumImmutableMemTableFlushed,
     {false, nullptr, &InternalStats::HandleNumImmutableMemTableFlushed}},
    {DB::Properties::kMemTableFlushPending,
     {false, nullptr, &InternalStats::HandleMemTableFlushPending}},
    {DB::Properties::kCompactionPending,
     {false, nullptr, &InternalStats::HandleCompactionPending}},
    {DB::Properties::kBackgroundErrors,
     {false, nullptr, &InternalStats::HandleBackgroundErrors}},
    {DB::Properties::kCurSizeActiveMemTable,
     {false, nullptr, &InternalStats::HandleCurSizeActiveMemTable}},
    {DB::Properties::kCurSizeAllMemTables,
     {false, nullptr, &InternalStats::HandleCurSizeAllMemTables}},
    {DB::Properties::kSizeAllMemTables,
     {false, nullptr, &InternalStats::HandleSizeAllMemTables}},
    {DB::Properties::kNumEntriesActiveMemTable,
     {false, nullptr, &InternalStats::HandleNumEntriesActiveMemTable}},
    {DB::Properties::kNumEntriesImmMemTables,
     {false, nullptr, &InternalStats::HandleNumEntriesImmMemTables}},
    {DB::Properties::kNumDeletesActiveMemTable,
     {false, nullptr, &InternalStats::HandleNumDeletesActiveMemTable}},
    {DB::Properties::kNumDeletesImmMemTables,
     {false, nullptr, &InternalStats::HandleNumDeletesImmMemTables}},
    {DB::Properties::kEstimateNumKeys,
     {false, nullptr, &InternalStats::HandleEstimateNumKeys}},
    {DB::Properties::kEstimateTableReadersMem,
     {true, nullptr, &InternalStats::HandleEstimateTableReadersMem}},
    {DB::Properties::kIsFileDeletionsEnabled,
     {false, nullptr, &InternalStats::HandleIsFileDeletionsEnabled}},
    {DB::Properties::kNumSnapshots,
     {false, nullptr, &InternalStats::HandleNumSnapshots}},
    {DB::Properties::kOldestSnapshotTime,
     {false, nullptr, &InternalStats::HandleOldestSnapshotTime}},
    {DB::Properties::kNumLiveVersions,
     {false, nullptr, &InternalStats::HandleNumLiveVersions}},
    {DB::Properties::kCurrentSuperVersionNumber,
     {false, nullptr, &InternalStats::HandleCurrentSuperVersionNumber}},
    {DB::Properties::kEstimateLiveDataSize,
     {true, nullptr, &InternalStats::HandleEstimateLiveDataSize}},
    {DB::Properties::kBaseLevel,
     {false, nullptr, &InternalStats::HandleBaseLevel}},
    {DB::Properties::kTotalSstFilesSize,
     {false, nullptr, &InternalStats::HandleTotalSstFilesSize}},
    {DB::Properties::kEstimatePendingCompactionBytes,
     {false, nullptr, &InternalStats::HandleEstimatePendingCompactionBytes}},
    {DB::Properties::kNumRunningFlushes,
     {false, nullptr, &InternalStats::HandleNumRunningFlushes}},
    {DB::Properties::kNumRunningCompactions,
     {false, nullptr, &InternalStats::HandleNumRunningCompactions}},
};

const DBPropertyInfo* GetPropertyInfo(const Slice& property) {
  std::string ppt_name = GetPropertyNameAndArg(property).first.ToString();
  auto ppt_info_iter = InternalStats::ppt_name_to_info.find(ppt_name);
  if (ppt_info_iter == InternalStats::ppt_name_to_info.end()) {
    return nullptr;
  }
  return &ppt_info_iter->second;
}

bool InternalStats::GetStringProperty(const DBPropertyInfo& property_info,
                                      const Slice& property,
                                      std::string* value) {
  assert(value != nullptr);
  assert(property_info.handle_string != nullptr);
  Slice arg = GetPropertyNameAndArg(property).second;
  return (this->*(property_info.handle_string))(value, arg);
}

bool InternalStats::GetIntProperty(const DBPropertyInfo& property_info,
                                   uint64_t* value, DBImpl* db) {
  assert(value != nullptr);
  assert(property_info.handle_int != nullptr &&
         !property_info.need_out_of_mutex);
  db->mutex_.AssertHeld();
  return (this->*(property_info.handle_int))(value, db, nullptr /* version */);
}

bool InternalStats::GetIntPropertyOutOfMutex(
    const DBPropertyInfo& property_info, Version* version, uint64_t* value) {
  assert(value != nullptr);
  assert(property_info.handle_int != nullptr &&
         property_info.need_out_of_mutex);
  return (this->*(property_info.handle_int))(value, nullptr /* db */, version);
}

bool InternalStats::HandleNumFilesAtLevel(std::string* value, Slice suffix) {
  uint64_t level;
  const auto* vstorage = cfd_->current()->storage_info();
  bool ok = ConsumeDecimalNumber(&suffix, &level) && suffix.empty();
  if (!ok || static_cast<int>(level) >= number_levels_) {
    return false;
  } else {
    char buf[100];
    snprintf(buf, sizeof(buf), "%d",
             vstorage->NumLevelFiles(static_cast<int>(level)));
    *value = buf;
    return true;
  }
}

bool InternalStats::HandleCompressionRatioAtLevelPrefix(std::string* value,
                                                        Slice suffix) {
  uint64_t level;
  const auto* vstorage = cfd_->current()->storage_info();
  bool ok = ConsumeDecimalNumber(&suffix, &level) && suffix.empty();
  if (!ok || level >= static_cast<uint64_t>(number_levels_)) {
    return false;
  }
  *value = ToString(
      vstorage->GetEstimatedCompressionRatioAtLevel(static_cast<int>(level)));
  return true;
}

bool InternalStats::HandleLevelStats(std::string* value, Slice suffix) {
  char buf[1000];
  const auto* vstorage = cfd_->current()->storage_info();
  snprintf(buf, sizeof(buf),
           "Level Files Size(MB)\n"
           "--------------------\n");
  value->append(buf);

  for (int level = 0; level < number_levels_; level++) {
    snprintf(buf, sizeof(buf), "%3d %8d %8.0f\n", level,
             vstorage->NumLevelFiles(level),
             vstorage->NumLevelBytes(level) / kMB);
    value->append(buf);
  }
  return true;
}

bool InternalStats::HandleStats(std::string* value, Slice suffix) {
  if (!HandleCFStats(value, suffix)) {
    return false;
  }
  if (!HandleDBStats(value, suffix)) {
    return false;
  }
  return true;
}

bool InternalStats::HandleCFStats(std::string* value, Slice suffix) {
  DumpCFStats(value);
  return true;
}

bool InternalStats::HandleDBStats(std::string* value, Slice suffix) {
  DumpDBStats(value);
  return true;
}

bool InternalStats::HandleSsTables(std::string* value, Slice suffix) {
  auto* current = cfd_->current();
  *value = current->DebugString();
  return true;
}

bool InternalStats::HandleAggregatedTableProperties(std::string* value,
                                                    Slice suffix) {
  std::shared_ptr<const TableProperties> tp;
  auto s = cfd_->current()->GetAggregatedTableProperties(&tp);
  if (!s.ok()) {
    return false;
  }
  *value = tp->ToString();
  return true;
}

bool InternalStats::HandleAggregatedTablePropertiesAtLevel(std::string* value,
                                                           Slice suffix) {
  uint64_t level;
  bool ok = ConsumeDecimalNumber(&suffix, &level) && suffix.empty();
  if (!ok || static_cast<int>(level) >= number_levels_) {
    return false;
  }
  std::shared_ptr<const TableProperties> tp;
  auto s = cfd_->current()->GetAggregatedTableProperties(
      &tp, static_cast<int>(level));
  if (!s.ok()) {
    return false;
  }
  *value = tp->ToString();
  return true;
}

bool InternalStats::HandleNumImmutableMemTable(uint64_t* value, DBImpl* db,
                                               Version* version) {
  *value = cfd_->imm()->NumNotFlushed();
  return true;
}

bool InternalStats::HandleNumImmutableMemTableFlushed(uint64_t* value,
                                                      DBImpl* db,
                                                      Version* version) {
  *value = cfd_->imm()->NumFlushed();
  return true;
}

bool InternalStats::HandleMemTableFlushPending(uint64_t* value, DBImpl* db,
                                               Version* version) {
  // Return number of mem tables that are ready to flush (made immutable)
  *value = (cfd_->imm()->IsFlushPending() ? 1 : 0);
  return true;
}

bool InternalStats::HandleNumRunningFlushes(uint64_t* value, DBImpl* db,
                                            Version* version) {
  *value = db->num_running_flushes();
  return true;
}

bool InternalStats::HandleCompactionPending(uint64_t* value, DBImpl* db,
                                            Version* version) {
  // 1 if the system already determines at least one compaction is needed.
  // 0 otherwise,
  const auto* vstorage = cfd_->current()->storage_info();
  *value = (cfd_->compaction_picker()->NeedsCompaction(vstorage) ? 1 : 0);
  return true;
}

bool InternalStats::HandleNumRunningCompactions(uint64_t* value, DBImpl* db,
                                                Version* version) {
  *value = db->num_running_compactions_;
  return true;
}

bool InternalStats::HandleBackgroundErrors(uint64_t* value, DBImpl* db,
                                           Version* version) {
  // Accumulated number of  errors in background flushes or compactions.
  *value = GetBackgroundErrorCount();
  return true;
}

bool InternalStats::HandleCurSizeActiveMemTable(uint64_t* value, DBImpl* db,
                                                Version* version) {
  // Current size of the active memtable
  *value = cfd_->mem()->ApproximateMemoryUsage();
  return true;
}

bool InternalStats::HandleCurSizeAllMemTables(uint64_t* value, DBImpl* db,
                                              Version* version) {
  // Current size of the active memtable + immutable memtables
  *value = cfd_->mem()->ApproximateMemoryUsage() +
           cfd_->imm()->ApproximateUnflushedMemTablesMemoryUsage();
  return true;
}

bool InternalStats::HandleSizeAllMemTables(uint64_t* value, DBImpl* db,
                                           Version* version) {
  *value = cfd_->mem()->ApproximateMemoryUsage() +
           cfd_->imm()->ApproximateMemoryUsage();
  return true;
}

bool InternalStats::HandleNumEntriesActiveMemTable(uint64_t* value, DBImpl* db,
                                                   Version* version) {
  // Current number of entires in the active memtable
  *value = cfd_->mem()->num_entries();
  return true;
}

bool InternalStats::HandleNumEntriesImmMemTables(uint64_t* value, DBImpl* db,
                                                 Version* version) {
  // Current number of entries in the immutable memtables
  *value = cfd_->imm()->current()->GetTotalNumEntries();
  return true;
}

bool InternalStats::HandleNumDeletesActiveMemTable(uint64_t* value, DBImpl* db,
                                                   Version* version) {
  // Current number of entires in the active memtable
  *value = cfd_->mem()->num_deletes();
  return true;
}

bool InternalStats::HandleNumDeletesImmMemTables(uint64_t* value, DBImpl* db,
                                                 Version* version) {
  // Current number of entries in the immutable memtables
  *value = cfd_->imm()->current()->GetTotalNumDeletes();
  return true;
}

bool InternalStats::HandleEstimateNumKeys(uint64_t* value, DBImpl* db,
                                          Version* version) {
  // Estimate number of entries in the column family:
  // Use estimated entries in tables + total entries in memtables.
  const auto* vstorage = cfd_->current()->storage_info();
  *value = cfd_->mem()->num_entries() +
           cfd_->imm()->current()->GetTotalNumEntries() -
           (cfd_->mem()->num_deletes() +
            cfd_->imm()->current()->GetTotalNumDeletes()) *
               2 +
           vstorage->GetEstimatedActiveKeys();
  return true;
}

bool InternalStats::HandleNumSnapshots(uint64_t* value, DBImpl* db,
                                       Version* version) {
  *value = db->snapshots().count();
  return true;
}

bool InternalStats::HandleOldestSnapshotTime(uint64_t* value, DBImpl* db,
                                             Version* version) {
  *value = static_cast<uint64_t>(db->snapshots().GetOldestSnapshotTime());
  return true;
}

bool InternalStats::HandleNumLiveVersions(uint64_t* value, DBImpl* db,
                                          Version* version) {
  *value = cfd_->GetNumLiveVersions();
  return true;
}

bool InternalStats::HandleCurrentSuperVersionNumber(uint64_t* value, DBImpl* db,
                                                    Version* version) {
  *value = cfd_->GetSuperVersionNumber();
  return true;
}

bool InternalStats::HandleIsFileDeletionsEnabled(uint64_t* value, DBImpl* db,
                                                 Version* version) {
  *value = db->IsFileDeletionsEnabled();
  return true;
}

bool InternalStats::HandleBaseLevel(uint64_t* value, DBImpl* db,
                                    Version* version) {
  const auto* vstorage = cfd_->current()->storage_info();
  *value = vstorage->base_level();
  return true;
}

bool InternalStats::HandleTotalSstFilesSize(uint64_t* value, DBImpl* db,
                                            Version* version) {
  *value = cfd_->GetTotalSstFilesSize();
  return true;
}

bool InternalStats::HandleEstimatePendingCompactionBytes(uint64_t* value,
                                                         DBImpl* db,
                                                         Version* version) {
  const auto* vstorage = cfd_->current()->storage_info();
  *value = vstorage->estimated_compaction_needed_bytes();
  return true;
}

bool InternalStats::HandleEstimateTableReadersMem(uint64_t* value, DBImpl* db,
                                                  Version* version) {
  *value = (version == nullptr) ? 0 : version->GetMemoryUsageByTableReaders();
  return true;
}

bool InternalStats::HandleEstimateLiveDataSize(uint64_t* value, DBImpl* db,
                                               Version* version) {
  const auto* vstorage = cfd_->current()->storage_info();
  *value = vstorage->EstimateLiveDataSize();
  return true;
}

void InternalStats::DumpDBStats(std::string* value) {
  char buf[1000];
  // DB-level stats, only available from default column family
  double seconds_up = (env_->NowMicros() - started_at_ + 1) / kMicrosInSec;
  double interval_seconds_up = seconds_up - db_stats_snapshot_.seconds_up;
  snprintf(buf, sizeof(buf),
           "\n** DB Stats **\nUptime(secs): %.1f total, %.1f interval\n",
           seconds_up, interval_seconds_up);
  value->append(buf);
  // Cumulative
  uint64_t user_bytes_written = GetDBStats(InternalStats::BYTES_WRITTEN);
  uint64_t num_keys_written = GetDBStats(InternalStats::NUMBER_KEYS_WRITTEN);
  uint64_t write_other = GetDBStats(InternalStats::WRITE_DONE_BY_OTHER);
  uint64_t write_self = GetDBStats(InternalStats::WRITE_DONE_BY_SELF);
  uint64_t wal_bytes = GetDBStats(InternalStats::WAL_FILE_BYTES);
  uint64_t wal_synced = GetDBStats(InternalStats::WAL_FILE_SYNCED);
  uint64_t write_with_wal = GetDBStats(InternalStats::WRITE_WITH_WAL);
  uint64_t write_stall_micros = GetDBStats(InternalStats::WRITE_STALL_MICROS);

  const int kHumanMicrosLen = 32;
  char human_micros[kHumanMicrosLen];

  // Data
  // writes: total number of write requests.
  // keys: total number of key updates issued by all the write requests
  // batches: number of group commits issued to the DB. Each group can contain
  //          one or more writes.
  // so writes/keys is the average number of put in multi-put or put
  // writes/batches is the average group commit size.
  //
  // The format is the same for interval stats.
  snprintf(buf, sizeof(buf),
           "Cumulative writes: %s writes, %s keys, %s batches, "
           "%.1f writes per batch, ingest: %.2f GB, %.2f MB/s\n",
           NumberToHumanString(write_other + write_self).c_str(),
           NumberToHumanString(num_keys_written).c_str(),
           NumberToHumanString(write_self).c_str(),
           (write_other + write_self) / static_cast<double>(write_self + 1),
           user_bytes_written / kGB, user_bytes_written / kMB / seconds_up);
  value->append(buf);
  // WAL
  snprintf(buf, sizeof(buf),
           "Cumulative WAL: %s writes, %s syncs, "
           "%.2f writes per sync, written: %.2f GB, %.2f MB/s\n",
           NumberToHumanString(write_with_wal).c_str(),
           NumberToHumanString(wal_synced).c_str(),
           write_with_wal / static_cast<double>(wal_synced + 1),
           wal_bytes / kGB, wal_bytes / kMB / seconds_up);
  value->append(buf);
  // Stall
  AppendHumanMicros(write_stall_micros, human_micros, kHumanMicrosLen, true);
  snprintf(buf, sizeof(buf),
           "Cumulative stall: %s, %.1f percent\n",
           human_micros,
           // 10000 = divide by 1M to get secs, then multiply by 100 for pct
           write_stall_micros / 10000.0 / std::max(seconds_up, 0.001));
  value->append(buf);

  // Interval
  uint64_t interval_write_other = write_other - db_stats_snapshot_.write_other;
  uint64_t interval_write_self = write_self - db_stats_snapshot_.write_self;
  uint64_t interval_num_keys_written =
      num_keys_written - db_stats_snapshot_.num_keys_written;
  snprintf(buf, sizeof(buf),
           "Interval writes: %s writes, %s keys, %s batches, "
           "%.1f writes per batch, ingest: %.2f MB, %.2f MB/s\n",
           NumberToHumanString(
               interval_write_other + interval_write_self).c_str(),
           NumberToHumanString(interval_num_keys_written).c_str(),
           NumberToHumanString(interval_write_self).c_str(),
           static_cast<double>(interval_write_other + interval_write_self) /
               (interval_write_self + 1),
           (user_bytes_written - db_stats_snapshot_.ingest_bytes) / kMB,
           (user_bytes_written - db_stats_snapshot_.ingest_bytes) / kMB /
               std::max(interval_seconds_up, 0.001)),
  value->append(buf);

  uint64_t interval_write_with_wal =
      write_with_wal - db_stats_snapshot_.write_with_wal;
  uint64_t interval_wal_synced = wal_synced - db_stats_snapshot_.wal_synced;
  uint64_t interval_wal_bytes = wal_bytes - db_stats_snapshot_.wal_bytes;

  snprintf(buf, sizeof(buf),
           "Interval WAL: %s writes, %s syncs, "
           "%.2f writes per sync, written: %.2f MB, %.2f MB/s\n",
           NumberToHumanString(interval_write_with_wal).c_str(),
           NumberToHumanString(interval_wal_synced).c_str(),
           interval_write_with_wal /
              static_cast<double>(interval_wal_synced + 1),
           interval_wal_bytes / kGB,
           interval_wal_bytes / kMB / std::max(interval_seconds_up, 0.001));
  value->append(buf);

  // Stall
  AppendHumanMicros(
      write_stall_micros - db_stats_snapshot_.write_stall_micros,
      human_micros, kHumanMicrosLen, true);
  snprintf(buf, sizeof(buf),
           "Interval stall: %s, %.1f percent\n",
           human_micros,
           // 10000 = divide by 1M to get secs, then multiply by 100 for pct
           (write_stall_micros - db_stats_snapshot_.write_stall_micros) /
               10000.0 / std::max(interval_seconds_up, 0.001));
  value->append(buf);

  for (int level = 0; level < number_levels_; level++) {
    if (!file_read_latency_[level].Empty()) {
      char buf2[5000];
      snprintf(buf2, sizeof(buf2),
               "** Level %d read latency histogram (micros):\n%s\n", level,
               file_read_latency_[level].ToString().c_str());
      value->append(buf2);
    }
  }

  db_stats_snapshot_.seconds_up = seconds_up;
  db_stats_snapshot_.ingest_bytes = user_bytes_written;
  db_stats_snapshot_.write_other = write_other;
  db_stats_snapshot_.write_self = write_self;
  db_stats_snapshot_.num_keys_written = num_keys_written;
  db_stats_snapshot_.wal_bytes = wal_bytes;
  db_stats_snapshot_.wal_synced = wal_synced;
  db_stats_snapshot_.write_with_wal = write_with_wal;
  db_stats_snapshot_.write_stall_micros = write_stall_micros;
}

void InternalStats::DumpCFStats(std::string* value) {
  const VersionStorageInfo* vstorage = cfd_->current()->storage_info();

  int num_levels_to_check =
      (cfd_->ioptions()->compaction_style != kCompactionStyleFIFO)
          ? vstorage->num_levels() - 1
          : 1;

  // Compaction scores are sorted base on its value. Restore them to the
  // level order
  std::vector<double> compaction_score(number_levels_, 0);
  for (int i = 0; i < num_levels_to_check; ++i) {
    compaction_score[vstorage->CompactionScoreLevel(i)] =
        vstorage->CompactionScore(i);
  }
  // Count # of files being compacted for each level
  std::vector<int> files_being_compacted(number_levels_, 0);
  for (int level = 0; level < number_levels_; ++level) {
    for (auto* f : vstorage->LevelFiles(level)) {
      if (f->being_compacted) {
        ++files_being_compacted[level];
      }
    }
  }

  char buf[1000];
  // Per-ColumnFamily stats
  PrintLevelStatsHeader(buf, sizeof(buf), cfd_->GetName());
  value->append(buf);

  CompactionStats stats_sum(0);
  int total_files = 0;
  int total_files_being_compacted = 0;
  double total_file_size = 0;
  for (int level = 0; level < number_levels_; level++) {
    int files = vstorage->NumLevelFiles(level);
    total_files += files;
    total_files_being_compacted += files_being_compacted[level];
    if (comp_stats_[level].micros > 0 || files > 0) {
      stats_sum.Add(comp_stats_[level]);
      total_file_size += vstorage->NumLevelBytes(level);
      double w_amp =
          (comp_stats_[level].bytes_read_non_output_levels == 0) ? 0.0
          : static_cast<double>(comp_stats_[level].bytes_written) /
            comp_stats_[level].bytes_read_non_output_levels;
      PrintLevelStats(buf, sizeof(buf), "L" + ToString(level), files,
                      files_being_compacted[level],
                      static_cast<double>(vstorage->NumLevelBytes(level)),
                      compaction_score[level],
                      w_amp, comp_stats_[level]);
      value->append(buf);
    }
  }

  uint64_t curr_ingest = cf_stats_value_[BYTES_FLUSHED];
  // Cumulative summary
  double w_amp = stats_sum.bytes_written / static_cast<double>(curr_ingest + 1);
  uint64_t total_stall_count =
      cf_stats_count_[LEVEL0_SLOWDOWN_TOTAL] +
      cf_stats_count_[LEVEL0_NUM_FILES_TOTAL] +
      cf_stats_count_[SOFT_PENDING_COMPACTION_BYTES_LIMIT] +
      cf_stats_count_[HARD_PENDING_COMPACTION_BYTES_LIMIT] +
      cf_stats_count_[MEMTABLE_COMPACTION] + cf_stats_count_[MEMTABLE_SLOWDOWN];
  // Stats summary across levels
  PrintLevelStats(buf, sizeof(buf), "Sum", total_files,
                  total_files_being_compacted, total_file_size, 0, w_amp,
                  stats_sum);
  value->append(buf);
  // Interval summary
  uint64_t interval_ingest =
      curr_ingest - cf_stats_snapshot_.ingest_bytes + 1;
  CompactionStats interval_stats(stats_sum);
  interval_stats.Subtract(cf_stats_snapshot_.comp_stats);
  w_amp = interval_stats.bytes_written / static_cast<double>(interval_ingest);
  PrintLevelStats(buf, sizeof(buf), "Int", 0, 0, 0, 0, w_amp, interval_stats);
  value->append(buf);

  double seconds_up = (env_->NowMicros() - started_at_ + 1) / kMicrosInSec;
  double interval_seconds_up = seconds_up - cf_stats_snapshot_.seconds_up;
  snprintf(buf, sizeof(buf), "Uptime(secs): %.1f total, %.1f interval\n",
           seconds_up, interval_seconds_up);
  value->append(buf);

  snprintf(buf, sizeof(buf),
           "Flush(GB): cumulative %.3f, interval %.3f\n",
           curr_ingest / kGB, interval_ingest / kGB);
  value->append(buf);

  // Compact
  uint64_t compact_bytes_read = 0;
  uint64_t compact_bytes_write = 0;
  uint64_t compact_micros = 0;
  for (int level = 0; level < number_levels_; level++) {
    compact_bytes_read += comp_stats_[level].bytes_read_output_level +
                          comp_stats_[level].bytes_read_non_output_levels;
    compact_bytes_write += comp_stats_[level].bytes_written;
    compact_micros += comp_stats_[level].micros;
  }

  snprintf(buf, sizeof(buf),
           "Cumulative compaction: %.2f GB write, %.2f MB/s write, "
           "%.2f GB read, %.2f MB/s read, %.1f seconds\n",
           compact_bytes_write / kGB, compact_bytes_write / kMB / seconds_up,
           compact_bytes_read / kGB, compact_bytes_read / kMB / seconds_up,
           compact_micros / kMicrosInSec);
  value->append(buf);

  // Compaction interval
  uint64_t interval_compact_bytes_write =
      compact_bytes_write - cf_stats_snapshot_.compact_bytes_write;
  uint64_t interval_compact_bytes_read =
      compact_bytes_read - cf_stats_snapshot_.compact_bytes_read;
  uint64_t interval_compact_micros =
      compact_micros - cf_stats_snapshot_.compact_micros;

  snprintf(
      buf, sizeof(buf),
      "Interval compaction: %.2f GB write, %.2f MB/s write, "
      "%.2f GB read, %.2f MB/s read, %.1f seconds\n",
      interval_compact_bytes_write / kGB,
      interval_compact_bytes_write / kMB / std::max(interval_seconds_up, 0.001),
      interval_compact_bytes_read / kGB,
      interval_compact_bytes_read / kMB / std::max(interval_seconds_up, 0.001),
      interval_compact_micros / kMicrosInSec);
  value->append(buf);
  cf_stats_snapshot_.compact_bytes_write = compact_bytes_write;
  cf_stats_snapshot_.compact_bytes_read = compact_bytes_read;
  cf_stats_snapshot_.compact_micros = compact_micros;

  snprintf(buf, sizeof(buf), "Stalls(count): %" PRIu64
                             " level0_slowdown, "
                             "%" PRIu64
                             " level0_slowdown_with_compaction, "
                             "%" PRIu64
                             " level0_numfiles, "
                             "%" PRIu64
                             " level0_numfiles_with_compaction, "
                             "%" PRIu64
                             " stop for pending_compaction_bytes, "
                             "%" PRIu64
                             " slowdown for pending_compaction_bytes, "
                             "%" PRIu64
                             " memtable_compaction, "
                             "%" PRIu64
                             " memtable_slowdown, "
                             "interval %" PRIu64 " total count\n",
           cf_stats_count_[LEVEL0_SLOWDOWN_TOTAL],
           cf_stats_count_[LEVEL0_SLOWDOWN_WITH_COMPACTION],
           cf_stats_count_[LEVEL0_NUM_FILES_TOTAL],
           cf_stats_count_[LEVEL0_NUM_FILES_WITH_COMPACTION],
           cf_stats_count_[HARD_PENDING_COMPACTION_BYTES_LIMIT],
           cf_stats_count_[SOFT_PENDING_COMPACTION_BYTES_LIMIT],
           cf_stats_count_[MEMTABLE_COMPACTION],
           cf_stats_count_[MEMTABLE_SLOWDOWN],
           total_stall_count - cf_stats_snapshot_.stall_count);
  value->append(buf);

  cf_stats_snapshot_.ingest_bytes = curr_ingest;
  cf_stats_snapshot_.comp_stats = stats_sum;
  cf_stats_snapshot_.stall_count = total_stall_count;
}


#else

const DBPropertyInfo* GetPropertyInfo(const Slice& property) { return nullptr; }

#endif  // !ROCKSDB_LITE

}  // namespace rocksdb
