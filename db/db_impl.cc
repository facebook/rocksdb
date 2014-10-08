//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <algorithm>
#include <climits>
#include <cstdio>
#include <set>
#include <stdexcept>
#include <stdint.h>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/table_cache.h"
#include "db/table_properties_collector.h"
#include "db/forward_iterator.h"
#include "db/transaction_log_impl.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "port/likely.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/block.h"
#include "table/block_based_table_factory.h"
#include "table/merger.h"
#include "table/table_builder.h"
#include "table/two_level_iterator.h"
#include "util/auto_roll_logger.h"
#include "util/autovector.h"
#include "util/build_version.h"
#include "util/coding.h"
#include "util/hash_skiplist_rep.h"
#include "util/hash_linklist_rep.h"
#include "util/logging.h"
#include "util/log_buffer.h"
#include "util/mutexlock.h"
#include "util/perf_context_imp.h"
#include "util/iostats_context_imp.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"

namespace rocksdb {

const std::string kDefaultColumnFamilyName("default");

void DumpLeveldbBuildVersion(Logger * log);

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool disableWAL;
  bool in_batch_group;
  bool done;
  uint64_t timeout_hint_us;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::WriteContext {
  autovector<SuperVersion*> superversions_to_free_;
  autovector<log::Writer*> logs_to_free_;

  ~WriteContext() {
    for (auto& sv : superversions_to_free_) {
      delete sv;
    }
    for (auto& log : logs_to_free_) {
      delete log;
    }
  }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // If there were two snapshots with seq numbers s1 and
  // s2 and s1 < s2, and if we find two instances of a key k1 then lies
  // entirely within s1 and s2, then the earlier version of k1 can be safely
  // deleted because that version is not visible in any snapshot.
  std::vector<SequenceNumber> existing_snapshots;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint32_t path_id;
    uint64_t file_size;
    InternalKey smallest, largest;
    SequenceNumber smallest_seqno, largest_seqno;
  };
  std::vector<Output> outputs;
  std::list<uint64_t> allocated_file_numbers;

  // State kept for output being generated
  unique_ptr<WritableFile> outfile;
  unique_ptr<TableBuilder> builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        total_bytes(0) {
  }

  // Create a client visible context of this compaction
  CompactionFilter::Context GetFilterContextV1() {
    CompactionFilter::Context context;
    context.is_full_compaction = compaction->IsFullCompaction();
    context.is_manual_compaction = compaction->IsManualCompaction();
    return context;
  }

  // Create a client visible context of this compaction
  CompactionFilterContext GetFilterContext() {
    CompactionFilterContext context;
    context.is_full_compaction = compaction->IsFullCompaction();
    context.is_manual_compaction = compaction->IsManualCompaction();
    return context;
  }

  std::vector<std::string> key_str_buf_;
  std::vector<std::string> existing_value_str_buf_;
  // new_value_buf_ will only be appended if a value changes
  std::vector<std::string> new_value_buf_;
  // if values_changed_buf_[i] is true
  // new_value_buf_ will add a new entry with the changed value
  std::vector<bool> value_changed_buf_;
  // to_delete_buf_[i] is true iff key_buf_[i] is deleted
  std::vector<bool> to_delete_buf_;

  std::vector<std::string> other_key_str_buf_;
  std::vector<std::string> other_value_str_buf_;

  std::vector<Slice> combined_key_buf_;
  std::vector<Slice> combined_value_buf_;

  std::string cur_prefix_;

  // Buffers the kv-pair that will be run through compaction filter V2
  // in the future.
  void BufferKeyValueSlices(const Slice& key, const Slice& value) {
    key_str_buf_.emplace_back(key.ToString());
    existing_value_str_buf_.emplace_back(value.ToString());
  }

  // Buffers the kv-pair that will not be run through compaction filter V2
  // in the future.
  void BufferOtherKeyValueSlices(const Slice& key, const Slice& value) {
    other_key_str_buf_.emplace_back(key.ToString());
    other_value_str_buf_.emplace_back(value.ToString());
  }

  // Add a kv-pair to the combined buffer
  void AddToCombinedKeyValueSlices(const Slice& key, const Slice& value) {
    // The real strings are stored in the batch buffers
    combined_key_buf_.emplace_back(key);
    combined_value_buf_.emplace_back(value);
  }

  // Merging the two buffers
  void MergeKeyValueSliceBuffer(const InternalKeyComparator* comparator) {
    size_t i = 0;
    size_t j = 0;
    size_t total_size = key_str_buf_.size() + other_key_str_buf_.size();
    combined_key_buf_.reserve(total_size);
    combined_value_buf_.reserve(total_size);

    while (i + j < total_size) {
      int comp_res = 0;
      if (i < key_str_buf_.size() && j < other_key_str_buf_.size()) {
        comp_res = comparator->Compare(key_str_buf_[i], other_key_str_buf_[j]);
      } else if (i >= key_str_buf_.size() && j < other_key_str_buf_.size()) {
        comp_res = 1;
      } else if (j >= other_key_str_buf_.size() && i < key_str_buf_.size()) {
        comp_res = -1;
      }
      if (comp_res > 0) {
        AddToCombinedKeyValueSlices(other_key_str_buf_[j], other_value_str_buf_[j]);
        j++;
      } else if (comp_res < 0) {
        AddToCombinedKeyValueSlices(key_str_buf_[i], existing_value_str_buf_[i]);
        i++;
      }
    }
  }

  void CleanupBatchBuffer() {
    to_delete_buf_.clear();
    key_str_buf_.clear();
    existing_value_str_buf_.clear();
    new_value_buf_.clear();
    value_changed_buf_.clear();

    to_delete_buf_.shrink_to_fit();
    key_str_buf_.shrink_to_fit();
    existing_value_str_buf_.shrink_to_fit();
    new_value_buf_.shrink_to_fit();
    value_changed_buf_.shrink_to_fit();

    other_key_str_buf_.clear();
    other_value_str_buf_.clear();
    other_key_str_buf_.shrink_to_fit();
    other_value_str_buf_.shrink_to_fit();
  }

  void CleanupMergedBuffer() {
    combined_key_buf_.clear();
    combined_value_buf_.clear();
    combined_key_buf_.shrink_to_fit();
    combined_value_buf_.shrink_to_fit();
  }
};

Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const Options& src) {
  auto db_options = SanitizeOptions(dbname, DBOptions(src));
  auto cf_options = SanitizeOptions(icmp, ColumnFamilyOptions(src));
  return Options(db_options, cf_options);
}

DBOptions SanitizeOptions(const std::string& dbname, const DBOptions& src) {
  DBOptions result = src;

  // result.max_open_files means an "infinite" open files.
  if (result.max_open_files != -1) {
    ClipToRange(&result.max_open_files, 20, 1000000);
  }

  if (result.info_log == nullptr) {
    Status s = CreateLoggerFromOptions(dbname, result.db_log_dir, src.env,
                                       result, &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }

  if (!result.rate_limiter) {
    if (result.bytes_per_sync == 0) {
      result.bytes_per_sync = 1024 * 1024;
    }
  }

  if (result.wal_dir.empty()) {
    // Use dbname as default
    result.wal_dir = dbname;
  }
  if (result.wal_dir.back() == '/') {
    result.wal_dir = result.wal_dir.substr(0, result.wal_dir.size() - 1);
  }

  if (result.db_paths.size() == 0) {
    result.db_paths.emplace_back(dbname, std::numeric_limits<uint64_t>::max());
  }

  return result;
}

namespace {

Status SanitizeDBOptionsByCFOptions(
    const DBOptions* db_opts,
    const std::vector<ColumnFamilyDescriptor>& column_families) {
  Status s;
  for (auto cf : column_families) {
    s = cf.options.table_factory->SanitizeDBOptions(db_opts);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

CompressionType GetCompressionFlush(const Options& options) {
  // Compressing memtable flushes might not help unless the sequential load
  // optimization is used for leveled compaction. Otherwise the CPU and
  // latency overhead is not offset by saving much space.

  bool can_compress;

  if (options.compaction_style == kCompactionStyleUniversal) {
    can_compress =
        (options.compaction_options_universal.compression_size_percent < 0);
  } else {
    // For leveled compress when min_level_to_compress == 0.
    can_compress = options.compression_per_level.empty() ||
                   options.compression_per_level[0] != kNoCompression;
  }

  if (can_compress) {
    return options.compression;
  } else {
    return kNoCompression;
  }
}
}  // namespace

DBImpl::DBImpl(const DBOptions& options, const std::string& dbname)
    : env_(options.env),
      dbname_(dbname),
      options_(SanitizeOptions(dbname, options)),
      stats_(options_.statistics.get()),
      db_lock_(nullptr),
      mutex_(options.use_adaptive_mutex),
      shutting_down_(nullptr),
      bg_cv_(&mutex_),
      logfile_number_(0),
      log_empty_(true),
      default_cf_handle_(nullptr),
      total_log_size_(0),
      max_total_in_memory_state_(0),
      tmp_batch_(),
      bg_schedule_needed_(false),
      bg_compaction_scheduled_(0),
      bg_manual_only_(0),
      bg_flush_scheduled_(0),
      manual_compaction_(nullptr),
      disable_delete_obsolete_files_(0),
      delete_obsolete_files_last_run_(options.env->NowMicros()),
      purge_wal_files_last_run_(0),
      last_stats_dump_time_microsec_(0),
      default_interval_to_delete_obsolete_WAL_(600),
      flush_on_destroy_(false),
      delayed_writes_(0),
      storage_options_(options),
      bg_work_gate_closed_(false),
      refitting_level_(false),
      opened_successfully_(false) {
  env_->GetAbsolutePath(dbname, &db_absolute_path_);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  // Give a large number for setting of "infinite" open files.
  const int table_cache_size =
      (options_.max_open_files == -1) ? 4194304 : options_.max_open_files - 10;
  // Reserve ten files or so for other uses and give the rest to TableCache.
  table_cache_ =
      NewLRUCache(table_cache_size, options_.table_cache_numshardbits,
                  options_.table_cache_remove_scan_count_limit);

  versions_.reset(
      new VersionSet(dbname_, &options_, storage_options_, table_cache_.get()));
  column_family_memtables_.reset(
      new ColumnFamilyMemTablesImpl(versions_->GetColumnFamilySet()));

  DumpLeveldbBuildVersion(options_.info_log.get());
  DumpDBFileSummary(options_, dbname_);
  options_.Dump(options_.info_log.get());

  LogFlush(options_.info_log);
}

DBImpl::~DBImpl() {
  mutex_.Lock();
  if (flush_on_destroy_) {
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (cfd->mem()->GetFirstSequenceNumber() != 0) {
        cfd->Ref();
        mutex_.Unlock();
        FlushMemTable(cfd, FlushOptions());
        mutex_.Lock();
        cfd->Unref();
      }
    }
    versions_->GetColumnFamilySet()->FreeDeadColumnFamilies();
  }

  // Wait for background work to finish
  shutting_down_.Release_Store(this);  // Any non-nullptr value is ok
  while (bg_compaction_scheduled_ || bg_flush_scheduled_) {
    bg_cv_.Wait();
  }

  if (default_cf_handle_ != nullptr) {
    // we need to delete handle outside of lock because it does its own locking
    mutex_.Unlock();
    delete default_cf_handle_;
    mutex_.Lock();
  }

  if (options_.allow_thread_local) {
    // Clean up obsolete files due to SuperVersion release.
    // (1) Need to delete to obsolete files before closing because RepairDB()
    // scans all existing files in the file system and builds manifest file.
    // Keeping obsolete files confuses the repair process.
    // (2) Need to check if we Open()/Recover() the DB successfully before
    // deleting because if VersionSet recover fails (may be due to corrupted
    // manifest file), it is not able to identify live files correctly. As a
    // result, all "live" files can get deleted by accident. However, corrupted
    // manifest is recoverable by RepairDB().
    if (opened_successfully_) {
      DeletionState deletion_state;
      FindObsoleteFiles(deletion_state, true);
      // manifest number starting from 2
      deletion_state.manifest_file_number = 1;
      if (deletion_state.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(deletion_state);
      }
    }
  }

  // versions need to be destroyed before table_cache since it can hold
  // references to table_cache.
  versions_.reset();
  mutex_.Unlock();
  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  LogFlush(options_.info_log);
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  Log(options_.info_log, "Creating manifest 1 \n");
  const std::string manifest = DescriptorFileName(dbname_, 1);
  unique_ptr<WritableFile> file;
  Status s = env_->NewWritableFile(
      manifest, &file, env_->OptimizeForManifestWrite(storage_options_));
  if (!s.ok()) {
    return s;
  }
  file->SetPreallocationBlockSize(options_.manifest_preallocation_size);
  {
    log::Writer log(std::move(file));
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
  }
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1, db_directory_.get());
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

const Status DBImpl::CreateArchivalDirectory() {
  if (options_.WAL_ttl_seconds > 0 || options_.WAL_size_limit_MB > 0) {
    std::string archivalPath = ArchivalDirectory(options_.wal_dir);
    return env_->CreateDirIfMissing(archivalPath);
  }
  return Status::OK();
}

void DBImpl::PrintStatistics() {
  auto dbstats = options_.statistics.get();
  if (dbstats) {
    Log(options_.info_log,
        "STATISTCS:\n %s",
        dbstats->ToString().c_str());
  }
}

void DBImpl::MaybeDumpStats() {
  if (options_.stats_dump_period_sec == 0) return;

  const uint64_t now_micros = env_->NowMicros();

  if (last_stats_dump_time_microsec_ +
      options_.stats_dump_period_sec * 1000000
      <= now_micros) {
    // Multiple threads could race in here simultaneously.
    // However, the last one will update last_stats_dump_time_microsec_
    // atomically. We could see more than one dump during one dump
    // period in rare cases.
    last_stats_dump_time_microsec_ = now_micros;

    bool tmp1 = false;
    bool tmp2 = false;
    DBPropertyType cf_property_type =
        GetPropertyType("rocksdb.cfstats", &tmp1, &tmp2);
    DBPropertyType db_property_type =
        GetPropertyType("rocksdb.dbstats", &tmp1, &tmp2);
    std::string stats;
    {
      MutexLock l(&mutex_);
      for (auto cfd : *versions_->GetColumnFamilySet()) {
        cfd->internal_stats()->GetStringProperty(cf_property_type,
                                                 "rocksdb.cfstats", &stats);
      }
      default_cf_internal_stats_->GetStringProperty(db_property_type,
                                                    "rocksdb.dbstats", &stats);
    }
    Log(options_.info_log, "------- DUMPING STATS -------");
    Log(options_.info_log, "%s", stats.c_str());

    PrintStatistics();
  }
}

// Returns the list of live files in 'sst_live' and the list
// of all files in the filesystem in 'candidate_files'.
// no_full_scan = true -- never do the full scan using GetChildren()
// force = false -- don't force the full scan, except every
//  options_.delete_obsolete_files_period_micros
// force = true -- force the full scan
void DBImpl::FindObsoleteFiles(DeletionState& deletion_state,
                               bool force,
                               bool no_full_scan) {
  mutex_.AssertHeld();

  // if deletion is disabled, do nothing
  if (disable_delete_obsolete_files_ > 0) {
    return;
  }

  bool doing_the_full_scan = false;

  // logic for figurint out if we're doing the full scan
  if (no_full_scan) {
    doing_the_full_scan = false;
  } else if (force || options_.delete_obsolete_files_period_micros == 0) {
    doing_the_full_scan = true;
  } else {
    const uint64_t now_micros = env_->NowMicros();
    if (delete_obsolete_files_last_run_ +
        options_.delete_obsolete_files_period_micros < now_micros) {
      doing_the_full_scan = true;
      delete_obsolete_files_last_run_ = now_micros;
    }
  }

  // get obsolete files
  versions_->GetObsoleteFiles(&deletion_state.sst_delete_files);

  // store the current filenum, lognum, etc
  deletion_state.manifest_file_number = versions_->ManifestFileNumber();
  deletion_state.pending_manifest_file_number =
      versions_->PendingManifestFileNumber();
  deletion_state.log_number = versions_->MinLogNumber();
  deletion_state.prev_log_number = versions_->PrevLogNumber();

  if (!doing_the_full_scan && !deletion_state.HaveSomethingToDelete()) {
    // avoid filling up sst_live if we're sure that we
    // are not going to do the full scan and that we don't have
    // anything to delete at the moment
    return;
  }

  // don't delete live files
  for (auto pair : pending_outputs_) {
    deletion_state.sst_live.emplace_back(pair.first, pair.second, 0);
  }
  /*  deletion_state.sst_live.insert(pending_outputs_.begin(),
                                   pending_outputs_.end());*/
  versions_->AddLiveFiles(&deletion_state.sst_live);

  if (doing_the_full_scan) {
    for (uint32_t path_id = 0; path_id < options_.db_paths.size(); path_id++) {
      // set of all files in the directory. We'll exclude files that are still
      // alive in the subsequent processings.
      std::vector<std::string> files;
      env_->GetChildren(options_.db_paths[path_id].path,
                        &files);  // Ignore errors
      for (std::string file : files) {
        deletion_state.candidate_files.emplace_back(file, path_id);
      }
    }

    //Add log files in wal_dir
    if (options_.wal_dir != dbname_) {
      std::vector<std::string> log_files;
      env_->GetChildren(options_.wal_dir, &log_files); // Ignore errors
      for (std::string log_file : log_files) {
        deletion_state.candidate_files.emplace_back(log_file, 0);
      }
    }
    // Add info log files in db_log_dir
    if (!options_.db_log_dir.empty() && options_.db_log_dir != dbname_) {
      std::vector<std::string> info_log_files;
      env_->GetChildren(options_.db_log_dir, &info_log_files);  // Ignore errors
      for (std::string log_file : info_log_files) {
        deletion_state.candidate_files.emplace_back(log_file, 0);
      }
    }
  }
}

namespace {
bool CompareCandidateFile(const rocksdb::DBImpl::CandidateFileInfo& first,
                          const rocksdb::DBImpl::CandidateFileInfo& second) {
  if (first.file_name > second.file_name) {
    return true;
  } else if (first.file_name < second.file_name) {
    return false;
  } else {
    return (first.path_id > second.path_id);
  }
}
};  // namespace

// Diffs the files listed in filenames and those that do not
// belong to live files are posibly removed. Also, removes all the
// files in sst_delete_files and log_delete_files.
// It is not necessary to hold the mutex when invoking this method.
void DBImpl::PurgeObsoleteFiles(DeletionState& state) {
  // we'd better have sth to delete
  assert(state.HaveSomethingToDelete());

  // this checks if FindObsoleteFiles() was run before. If not, don't do
  // PurgeObsoleteFiles(). If FindObsoleteFiles() was run, we need to also
  // run PurgeObsoleteFiles(), even if disable_delete_obsolete_files_ is true
  if (state.manifest_file_number == 0) {
    return;
  }

  // Now, convert live list to an unordered map, WITHOUT mutex held;
  // set is slow.
  std::unordered_map<uint64_t, const FileDescriptor*> sst_live_map;
  for (FileDescriptor& fd : state.sst_live) {
    sst_live_map[fd.GetNumber()] = &fd;
  }

  auto& candidate_files = state.candidate_files;
  candidate_files.reserve(
      candidate_files.size() +
      state.sst_delete_files.size() +
      state.log_delete_files.size());
  // We may ignore the dbname when generating the file names.
  const char* kDumbDbName = "";
  for (auto file : state.sst_delete_files) {
    candidate_files.emplace_back(
        MakeTableFileName(kDumbDbName, file->fd.GetNumber()),
        file->fd.GetPathId());
    delete file;
  }

  for (auto file_num : state.log_delete_files) {
    if (file_num > 0) {
      candidate_files.emplace_back(LogFileName(kDumbDbName, file_num).substr(1),
                                   0);
    }
  }

  // dedup state.candidate_files so we don't try to delete the same
  // file twice
  sort(candidate_files.begin(), candidate_files.end(), CompareCandidateFile);
  candidate_files.erase(unique(candidate_files.begin(), candidate_files.end()),
                        candidate_files.end());

  std::vector<std::string> old_info_log_files;
  InfoLogPrefix info_log_prefix(!options_.db_log_dir.empty(), dbname_);
  for (const auto& candidate_file : candidate_files) {
    std::string to_delete = candidate_file.file_name;
    uint32_t path_id = candidate_file.path_id;
    uint64_t number;
    FileType type;
    // Ignore file if we cannot recognize it.
    if (!ParseFileName(to_delete, &number, info_log_prefix.prefix, &type)) {
      continue;
    }

    bool keep = true;
    switch (type) {
      case kLogFile:
        keep = ((number >= state.log_number) ||
                (number == state.prev_log_number));
        break;
      case kDescriptorFile:
        // Keep my manifest file, and any newer incarnations'
        // (can happen during manifest roll)
        keep = (number >= state.manifest_file_number);
        break;
      case kTableFile:
        keep = (sst_live_map.find(number) != sst_live_map.end());
        break;
      case kTempFile:
        // Any temp files that are currently being written to must
        // be recorded in pending_outputs_, which is inserted into "live".
        // Also, SetCurrentFile creates a temp file when writing out new
        // manifest, which is equal to state.pending_manifest_file_number. We
        // should not delete that file
        keep = (sst_live_map.find(number) != sst_live_map.end()) ||
               (number == state.pending_manifest_file_number);
        break;
      case kInfoLogFile:
        keep = true;
        if (number != 0) {
          old_info_log_files.push_back(to_delete);
        }
        break;
      case kCurrentFile:
      case kDBLockFile:
      case kIdentityFile:
      case kMetaDatabase:
        keep = true;
        break;
    }

    if (keep) {
      continue;
    }

    std::string fname;
    if (type == kTableFile) {
      // evict from cache
      TableCache::Evict(table_cache_.get(), number);
      fname = TableFileName(options_.db_paths, number, path_id);
    } else {
      fname =
          ((type == kLogFile) ? options_.wal_dir : dbname_) + "/" + to_delete;
    }

    if (type == kLogFile &&
        (options_.WAL_ttl_seconds > 0 || options_.WAL_size_limit_MB > 0)) {
      auto archived_log_name = ArchivedLogFileName(options_.wal_dir, number);
      // The sync point below is used in (DBTest,TransactionLogIteratorRace)
      TEST_SYNC_POINT("DBImpl::PurgeObsoleteFiles:1");
      Status s = env_->RenameFile(fname, archived_log_name);
      // The sync point below is used in (DBTest,TransactionLogIteratorRace)
      TEST_SYNC_POINT("DBImpl::PurgeObsoleteFiles:2");
      Log(options_.info_log,
          "Move log file %s to %s -- %s\n",
          fname.c_str(), archived_log_name.c_str(), s.ToString().c_str());
    } else {
      Status s = env_->DeleteFile(fname);
      Log(options_.info_log, "Delete %s type=%d #%" PRIu64 " -- %s\n",
          fname.c_str(), type, number, s.ToString().c_str());
    }
  }

  // Delete old info log files.
  size_t old_info_log_file_count = old_info_log_files.size();
  if (old_info_log_file_count >= options_.keep_log_file_num) {
    std::sort(old_info_log_files.begin(), old_info_log_files.end());
    size_t end = old_info_log_file_count - options_.keep_log_file_num;
    for (unsigned int i = 0; i <= end; i++) {
      std::string& to_delete = old_info_log_files.at(i);
      std::string full_path_to_delete =
          (options_.db_log_dir.empty() ? dbname_ : options_.db_log_dir) + "/" +
          to_delete;
      Log(options_.info_log, "Delete info log file %s\n",
          full_path_to_delete.c_str());
      Status s = env_->DeleteFile(full_path_to_delete);
      if (!s.ok()) {
        Log(options_.info_log, "Delete info log file %s FAILED -- %s\n",
            to_delete.c_str(), s.ToString().c_str());
      }
    }
  }
  PurgeObsoleteWALFiles();
  LogFlush(options_.info_log);
}

void DBImpl::DeleteObsoleteFiles() {
  mutex_.AssertHeld();
  DeletionState deletion_state;
  FindObsoleteFiles(deletion_state, true);
  if (deletion_state.HaveSomethingToDelete()) {
    PurgeObsoleteFiles(deletion_state);
  }
}

#ifndef ROCKSDB_LITE
// 1. Go through all archived files and
//    a. if ttl is enabled, delete outdated files
//    b. if archive size limit is enabled, delete empty files,
//        compute file number and size.
// 2. If size limit is enabled:
//    a. compute how many files should be deleted
//    b. get sorted non-empty archived logs
//    c. delete what should be deleted
void DBImpl::PurgeObsoleteWALFiles() {
  bool const ttl_enabled = options_.WAL_ttl_seconds > 0;
  bool const size_limit_enabled =  options_.WAL_size_limit_MB > 0;
  if (!ttl_enabled && !size_limit_enabled) {
    return;
  }

  int64_t current_time;
  Status s = env_->GetCurrentTime(&current_time);
  if (!s.ok()) {
    Log(options_.info_log, "Can't get current time: %s", s.ToString().c_str());
    assert(false);
    return;
  }
  uint64_t const now_seconds = static_cast<uint64_t>(current_time);
  uint64_t const time_to_check = (ttl_enabled && !size_limit_enabled) ?
    options_.WAL_ttl_seconds / 2 : default_interval_to_delete_obsolete_WAL_;

  if (purge_wal_files_last_run_ + time_to_check > now_seconds) {
    return;
  }

  purge_wal_files_last_run_ = now_seconds;

  std::string archival_dir = ArchivalDirectory(options_.wal_dir);
  std::vector<std::string> files;
  s = env_->GetChildren(archival_dir, &files);
  if (!s.ok()) {
    Log(options_.info_log, "Can't get archive files: %s", s.ToString().c_str());
    assert(false);
    return;
  }

  size_t log_files_num = 0;
  uint64_t log_file_size = 0;

  for (auto& f : files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kLogFile) {
      std::string const file_path = archival_dir + "/" + f;
      if (ttl_enabled) {
        uint64_t file_m_time;
        Status const s = env_->GetFileModificationTime(file_path,
          &file_m_time);
        if (!s.ok()) {
          Log(options_.info_log, "Can't get file mod time: %s: %s",
              file_path.c_str(), s.ToString().c_str());
          continue;
        }
        if (now_seconds - file_m_time > options_.WAL_ttl_seconds) {
          Status const s = env_->DeleteFile(file_path);
          if (!s.ok()) {
            Log(options_.info_log, "Can't delete file: %s: %s",
                file_path.c_str(), s.ToString().c_str());
            continue;
          } else {
            MutexLock l(&read_first_record_cache_mutex_);
            read_first_record_cache_.erase(number);
          }
          continue;
        }
      }

      if (size_limit_enabled) {
        uint64_t file_size;
        Status const s = env_->GetFileSize(file_path, &file_size);
        if (!s.ok()) {
          Log(options_.info_log, "Can't get file size: %s: %s",
              file_path.c_str(), s.ToString().c_str());
          return;
        } else {
          if (file_size > 0) {
            log_file_size = std::max(log_file_size, file_size);
            ++log_files_num;
          } else {
            Status s = env_->DeleteFile(file_path);
            if (!s.ok()) {
              Log(options_.info_log, "Can't delete file: %s: %s",
                  file_path.c_str(), s.ToString().c_str());
              continue;
            } else {
              MutexLock l(&read_first_record_cache_mutex_);
              read_first_record_cache_.erase(number);
            }
          }
        }
      }
    }
  }

  if (0 == log_files_num || !size_limit_enabled) {
    return;
  }

  size_t const files_keep_num = options_.WAL_size_limit_MB *
    1024 * 1024 / log_file_size;
  if (log_files_num <= files_keep_num) {
    return;
  }

  size_t files_del_num = log_files_num - files_keep_num;
  VectorLogPtr archived_logs;
  GetSortedWalsOfType(archival_dir, archived_logs, kArchivedLogFile);

  if (files_del_num > archived_logs.size()) {
    Log(options_.info_log, "Trying to delete more archived log files than "
        "exist. Deleting all");
    files_del_num = archived_logs.size();
  }

  for (size_t i = 0; i < files_del_num; ++i) {
    std::string const file_path = archived_logs[i]->PathName();
    Status const s = DeleteFile(file_path);
    if (!s.ok()) {
      Log(options_.info_log, "Can't delete file: %s: %s",
          file_path.c_str(), s.ToString().c_str());
      continue;
    } else {
      MutexLock l(&read_first_record_cache_mutex_);
      read_first_record_cache_.erase(archived_logs[i]->LogNumber());
    }
  }
}

namespace {
struct CompareLogByPointer {
  bool operator()(const unique_ptr<LogFile>& a, const unique_ptr<LogFile>& b) {
    LogFileImpl* a_impl = dynamic_cast<LogFileImpl*>(a.get());
    LogFileImpl* b_impl = dynamic_cast<LogFileImpl*>(b.get());
    return *a_impl < *b_impl;
  }
};
}

Status DBImpl::GetSortedWalsOfType(const std::string& path,
                                   VectorLogPtr& log_files,
                                   WalFileType log_type) {
  std::vector<std::string> all_files;
  const Status status = env_->GetChildren(path, &all_files);
  if (!status.ok()) {
    return status;
  }
  log_files.reserve(all_files.size());
  for (const auto& f : all_files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kLogFile) {
      SequenceNumber sequence;
      Status s = ReadFirstRecord(log_type, number, &sequence);
      if (!s.ok()) {
        return s;
      }
      if (sequence == 0) {
        // empty file
        continue;
      }

      // Reproduce the race condition where a log file is moved
      // to archived dir, between these two sync points, used in
      // (DBTest,TransactionLogIteratorRace)
      TEST_SYNC_POINT("DBImpl::GetSortedWalsOfType:1");
      TEST_SYNC_POINT("DBImpl::GetSortedWalsOfType:2");

      uint64_t size_bytes;
      s = env_->GetFileSize(LogFileName(path, number), &size_bytes);
      // re-try in case the alive log file has been moved to archive.
      if (!s.ok() && log_type == kAliveLogFile &&
          env_->FileExists(ArchivedLogFileName(path, number))) {
        s = env_->GetFileSize(ArchivedLogFileName(path, number), &size_bytes);
      }
      if (!s.ok()) {
        return s;
      }

      log_files.push_back(std::move(unique_ptr<LogFile>(
          new LogFileImpl(number, log_type, sequence, size_bytes))));
    }
  }
  CompareLogByPointer compare_log_files;
  std::sort(log_files.begin(), log_files.end(), compare_log_files);
  return status;
}

Status DBImpl::RetainProbableWalFiles(VectorLogPtr& all_logs,
                                      const SequenceNumber target) {
  int64_t start = 0;  // signed to avoid overflow when target is < first file.
  int64_t end = static_cast<int64_t>(all_logs.size()) - 1;
  // Binary Search. avoid opening all files.
  while (end >= start) {
    int64_t mid = start + (end - start) / 2;  // Avoid overflow.
    SequenceNumber current_seq_num = all_logs.at(mid)->StartSequence();
    if (current_seq_num == target) {
      end = mid;
      break;
    } else if (current_seq_num < target) {
      start = mid + 1;
    } else {
      end = mid - 1;
    }
  }
  // end could be -ve.
  size_t start_index = std::max(static_cast<int64_t>(0), end);
  // The last wal file is always included
  all_logs.erase(all_logs.begin(), all_logs.begin() + start_index);
  return Status::OK();
}

Status DBImpl::ReadFirstRecord(const WalFileType type, const uint64_t number,
                               SequenceNumber* sequence) {
  if (type != kAliveLogFile && type != kArchivedLogFile) {
    return Status::NotSupported("File Type Not Known " + std::to_string(type));
  }
  {
    MutexLock l(&read_first_record_cache_mutex_);
    auto itr = read_first_record_cache_.find(number);
    if (itr != read_first_record_cache_.end()) {
      *sequence = itr->second;
      return Status::OK();
    }
  }
  Status s;
  if (type == kAliveLogFile) {
    std::string fname = LogFileName(options_.wal_dir, number);
    s = ReadFirstLine(fname, sequence);
    if (env_->FileExists(fname) && !s.ok()) {
      // return any error that is not caused by non-existing file
      return s;
    }
  }

  if (type == kArchivedLogFile || !s.ok()) {
    //  check if the file got moved to archive.
    std::string archived_file = ArchivedLogFileName(options_.wal_dir, number);
    s = ReadFirstLine(archived_file, sequence);
  }

  if (s.ok() && *sequence != 0) {
    MutexLock l(&read_first_record_cache_mutex_);
    read_first_record_cache_.insert({number, *sequence});
  }
  return s;
}

// the function returns status.ok() and sequence == 0 if the file exists, but is
// empty
Status DBImpl::ReadFirstLine(const std::string& fname,
                             SequenceNumber* sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;

    Status* status;
    bool ignore_error;  // true if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->ignore_error ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status->ok()) {
        // only keep the first error
        *this->status = s;
      }
    }
  };

  unique_ptr<SequentialFile> file;
  Status status = env_->NewSequentialFile(fname, &file, storage_options_);

  if (!status.ok()) {
    return status;
  }

  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log.get();
  reporter.fname = fname.c_str();
  reporter.status = &status;
  reporter.ignore_error = !options_.paranoid_checks;
  log::Reader reader(std::move(file), &reporter, true /*checksum*/,
                     0 /*initial_offset*/);
  std::string scratch;
  Slice record;

  if (reader.ReadRecord(&record, &scratch) &&
      (status.ok() || !options_.paranoid_checks)) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      // TODO read record's till the first no corrupt entry?
    } else {
      WriteBatch batch;
      WriteBatchInternal::SetContents(&batch, record);
      *sequence = WriteBatchInternal::Sequence(&batch);
      return Status::OK();
    }
  }

  // ReadRecord returns false on EOF, which means that the log file is empty. we
  // return status.ok() in that case and set sequence number to 0
  *sequence = 0;
  return status;
}

#endif  // ROCKSDB_LITE

Status DBImpl::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families, bool read_only,
    bool error_if_log_file_exist) {
  mutex_.AssertHeld();

  bool is_new_db = false;
  assert(db_lock_ == nullptr);
  if (!read_only) {
    // We call CreateDirIfMissing() as the directory may already exist (if we
    // are reopening a DB), when this happens we don't want creating the
    // directory to cause an error. However, we need to check if creating the
    // directory fails or else we may get an obscure message about the lock
    // file not existing. One real-world example of this occurring is if
    // env->CreateDirIfMissing() doesn't create intermediate directories, e.g.
    // when dbname_ is "dir/db" but when "dir" doesn't exist.
    Status s = env_->CreateDirIfMissing(dbname_);
    if (!s.ok()) {
      return s;
    }

    for (auto& db_path : options_.db_paths) {
      s = env_->CreateDirIfMissing(db_path.path);
      if (!s.ok()) {
        return s;
      }
    }

    s = env_->NewDirectory(dbname_, &db_directory_);
    if (!s.ok()) {
      return s;
    }

    s = env_->LockFile(LockFileName(dbname_), &db_lock_);
    if (!s.ok()) {
      return s;
    }

    if (!env_->FileExists(CurrentFileName(dbname_))) {
      if (options_.create_if_missing) {
        s = NewDB();
        is_new_db = true;
        if (!s.ok()) {
          return s;
        }
      } else {
        return Status::InvalidArgument(
            dbname_, "does not exist (create_if_missing is false)");
      }
    } else {
      if (options_.error_if_exists) {
        return Status::InvalidArgument(
            dbname_, "exists (error_if_exists is true)");
      }
    }
    // Check for the IDENTITY file and create it if not there
    if (!env_->FileExists(IdentityFileName(dbname_))) {
      s = SetIdentityFile(env_, dbname_);
      if (!s.ok()) {
        return s;
      }
    }
  }

  Status s = versions_->Recover(column_families, read_only);
  if (options_.paranoid_checks && s.ok()) {
    s = CheckConsistency();
  }
  if (s.ok()) {
    SequenceNumber max_sequence(0);
    default_cf_handle_ = new ColumnFamilyHandleImpl(
        versions_->GetColumnFamilySet()->GetDefault(), this, &mutex_);
    default_cf_internal_stats_ = default_cf_handle_->cfd()->internal_stats();
    single_column_family_mode_ =
        versions_->GetColumnFamilySet()->NumberOfColumnFamilies() == 1;

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of rocksdb.
    const uint64_t min_log = versions_->MinLogNumber();
    const uint64_t prev_log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    s = env_->GetChildren(options_.wal_dir, &filenames);
    if (!s.ok()) {
      return s;
    }

    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
      uint64_t number;
      FileType type;
      if (ParseFileName(filenames[i], &number, &type) && type == kLogFile) {
        if (is_new_db) {
          return Status::Corruption(
              "While creating a new Db, wal_dir contains "
              "existing log file: ",
              filenames[i]);
        } else if ((number >= min_log) || (number == prev_log)) {
          logs.push_back(number);
        }
      }
    }

    if (logs.size() > 0 && error_if_log_file_exist) {
      return Status::Corruption(""
          "The db was opened in readonly mode with error_if_log_file_exist"
          "flag but a log file already exists");
    }

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    for (const auto& log : logs) {
      // The previous incarnation may not have written any MANIFEST
      // records after allocating this log number.  So we manually
      // update the file number allocation counter in VersionSet.
      versions_->MarkFileNumberUsed(log);
      s = RecoverLogFile(log, &max_sequence, read_only);
    }
    SetTickerCount(stats_, SEQUENCE_NUMBER, versions_->LastSequence());
  }

  for (auto cfd : *versions_->GetColumnFamilySet()) {
    max_total_in_memory_state_ += cfd->options()->write_buffer_size *
                                  cfd->options()->max_write_buffer_number;
  }

  return s;
}

Status DBImpl::RecoverLogFile(uint64_t log_number, SequenceNumber* max_sequence,
                              bool read_only) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // nullptr if options_.paranoid_checks==false or
                     //            options_.skip_log_error_on_recovery==true
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  std::unordered_map<int, VersionEdit> version_edits;
  // no need to refcount because iteration is under mutex
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());
    version_edits.insert({cfd->GetID(), edit});
  }

  // Open the log file
  std::string fname = LogFileName(options_.wal_dir, log_number);
  unique_ptr<SequentialFile> file;
  Status status = env_->NewSequentialFile(fname, &file, storage_options_);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log.get();
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks &&
                     !options_.skip_log_error_on_recovery ? &status : nullptr);
  // We intentially make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(std::move(file), &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%" PRIu64 "", log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  while (reader.ReadRecord(&record, &scratch)) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    // If column family was not found, it might mean that the WAL write
    // batch references to the column family that was dropped after the
    // insert. We don't want to fail the whole write batch in that case -- we
    // just ignore the update. That's why we set ignore missing column families
    // to true
    status = WriteBatchInternal::InsertInto(
        &batch, column_family_memtables_.get(),
        true /* ignore missing column families */, log_number);

    MaybeIgnoreError(&status);
    if (!status.ok()) {
      return status;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (!read_only) {
      // no need to refcount since client still doesn't have access
      // to the DB and can not drop column families while we iterate
      for (auto cfd : *versions_->GetColumnFamilySet()) {
        if (cfd->mem()->ShouldFlush()) {
          // If this asserts, it means that InsertInto failed in
          // filtering updates to already-flushed column families
          assert(cfd->GetLogNumber() <= log_number);
          auto iter = version_edits.find(cfd->GetID());
          assert(iter != version_edits.end());
          VersionEdit* edit = &iter->second;
          status = WriteLevel0TableForRecovery(cfd, cfd->mem(), edit);
          // we still want to clear the memtable, even if the recovery failed
          cfd->CreateNewMemtable();
          if (!status.ok()) {
            // Reflect errors immediately so that conditions like full
            // file-systems cause the DB::Open() to fail.
            return status;
          }
        }
      }
    }
  }

  if (versions_->LastSequence() < *max_sequence) {
    versions_->SetLastSequence(*max_sequence);
  }

  if (!read_only) {
    // no need to refcount since client still doesn't have access
    // to the DB and can not drop column families while we iterate
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      auto iter = version_edits.find(cfd->GetID());
      assert(iter != version_edits.end());
      VersionEdit* edit = &iter->second;

      if (cfd->GetLogNumber() > log_number) {
        // Column family cfd has already flushed the data
        // from log_number. Memtable has to be empty because
        // we filter the updates based on log_number
        // (in WriteBatch::InsertInto)
        assert(cfd->mem()->GetFirstSequenceNumber() == 0);
        assert(edit->NumEntries() == 0);
        continue;
      }

      // flush the final memtable (if non-empty)
      if (cfd->mem()->GetFirstSequenceNumber() != 0) {
        status = WriteLevel0TableForRecovery(cfd, cfd->mem(), edit);
      }
      // we still want to clear the memtable, even if the recovery failed
      cfd->CreateNewMemtable();
      if (!status.ok()) {
        return status;
      }

      // write MANIFEST with update
      // writing log number in the manifest means that any log file
      // with number strongly less than (log_number + 1) is already
      // recovered and should be ignored on next reincarnation.
      // Since we already recovered log_number, we want all logs
      // with numbers `<= log_number` (includes this one) to be ignored
      edit->SetLogNumber(log_number + 1);
      // we must mark the next log number as used, even though it's
      // not actually used. that is because VersionSet assumes
      // VersionSet::next_file_number_ always to be strictly greater than any
      // log number
      versions_->MarkFileNumberUsed(log_number + 1);
      status = versions_->LogAndApply(cfd, edit, &mutex_);
      if (!status.ok()) {
        return status;
      }
    }
  }

  return status;
}

Status DBImpl::WriteLevel0TableForRecovery(ColumnFamilyData* cfd, MemTable* mem,
                                           VersionEdit* edit) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.fd = FileDescriptor(versions_->NewFileNumber(), 0, 0);
  pending_outputs_[meta.fd.GetNumber()] = 0;  // path 0 for level 0 file.
  ReadOptions ro;
  ro.total_order_seek = true;
  Iterator* iter = mem->NewIterator(ro);
  const SequenceNumber newest_snapshot = snapshots_.GetNewest();
  const SequenceNumber earliest_seqno_in_memtable =
    mem->GetFirstSequenceNumber();
  Log(options_.info_log, "[%s] Level-0 table #%" PRIu64 ": started",
      cfd->GetName().c_str(), meta.fd.GetNumber());

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, *cfd->options(), storage_options_,
                   cfd->table_cache(), iter, &meta, cfd->internal_comparator(),
                   newest_snapshot, earliest_seqno_in_memtable,
                   GetCompressionFlush(*cfd->options()), Env::IO_HIGH);
    LogFlush(options_.info_log);
    mutex_.Lock();
  }

  Log(options_.info_log,
      "[%s] Level-0 table #%" PRIu64 ": %" PRIu64 " bytes %s",
      cfd->GetName().c_str(), meta.fd.GetNumber(), meta.fd.GetFileSize(),
      s.ToString().c_str());
  delete iter;

  pending_outputs_.erase(meta.fd.GetNumber());

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.fd.GetFileSize() > 0) {
    edit->AddFile(level, meta.fd.GetNumber(), meta.fd.GetPathId(),
                  meta.fd.GetFileSize(), meta.smallest, meta.largest,
                  meta.smallest_seqno, meta.largest_seqno);
  }

  InternalStats::CompactionStats stats(1);
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.fd.GetFileSize();
  stats.files_out_levelnp1 = 1;
  cfd->internal_stats()->AddCompactionStats(level, stats);
  cfd->internal_stats()->AddCFStats(
      InternalStats::BYTES_FLUSHED, meta.fd.GetFileSize());
  RecordTick(stats_, COMPACT_WRITE_BYTES, meta.fd.GetFileSize());
  return s;
}

Status DBImpl::WriteLevel0Table(ColumnFamilyData* cfd,
                                autovector<MemTable*>& mems, VersionEdit* edit,
                                uint64_t* filenumber, LogBuffer* log_buffer) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;

  meta.fd = FileDescriptor(versions_->NewFileNumber(), 0, 0);
  *filenumber = meta.fd.GetNumber();
  pending_outputs_[meta.fd.GetNumber()] = 0;  // path 0 for level 0 file.

  const SequenceNumber newest_snapshot = snapshots_.GetNewest();
  const SequenceNumber earliest_seqno_in_memtable =
    mems[0]->GetFirstSequenceNumber();
  Version* base = cfd->current();
  base->Ref();          // it is likely that we do not need this reference
  Status s;
  {
    mutex_.Unlock();
    log_buffer->FlushBufferToLog();
    std::vector<Iterator*> memtables;
    ReadOptions ro;
    ro.total_order_seek = true;
    for (MemTable* m : mems) {
      Log(options_.info_log,
          "[%s] Flushing memtable with next log file: %" PRIu64 "\n",
          cfd->GetName().c_str(), m->GetNextLogNumber());
      memtables.push_back(m->NewIterator(ro));
    }
    Iterator* iter = NewMergingIterator(&cfd->internal_comparator(),
                                        &memtables[0], memtables.size());
    Log(options_.info_log, "[%s] Level-0 flush table #%" PRIu64 ": started",
        cfd->GetName().c_str(), meta.fd.GetNumber());

    s = BuildTable(dbname_, env_, *cfd->options(), storage_options_,
                   cfd->table_cache(), iter, &meta, cfd->internal_comparator(),
                   newest_snapshot, earliest_seqno_in_memtable,
                   GetCompressionFlush(*cfd->options()), Env::IO_HIGH);
    LogFlush(options_.info_log);
    delete iter;
    Log(options_.info_log,
        "[%s] Level-0 flush table #%" PRIu64 ": %" PRIu64 " bytes %s",
        cfd->GetName().c_str(), meta.fd.GetNumber(), meta.fd.GetFileSize(),
        s.ToString().c_str());

    if (!options_.disableDataSync) {
      db_directory_->Fsync();
    }
    mutex_.Lock();
  }
  base->Unref();

  // re-acquire the most current version
  base = cfd->current();

  // There could be multiple threads writing to its own level-0 file.
  // The pending_outputs cannot be cleared here, otherwise this newly
  // created file might not be considered as a live-file by another
  // compaction thread that is concurrently deleting obselete files.
  // The pending_outputs can be cleared only after the new version is
  // committed so that other threads can recognize this file as a
  // valid one.
  // pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.fd.GetFileSize() > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    // if we have more than 1 background thread, then we cannot
    // insert files directly into higher levels because some other
    // threads could be concurrently producing compacted files for
    // that key range.
    if (base != nullptr && options_.max_background_compactions <= 1 &&
        cfd->options()->compaction_style == kCompactionStyleLevel) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.fd.GetNumber(), meta.fd.GetPathId(),
                  meta.fd.GetFileSize(), meta.smallest, meta.largest,
                  meta.smallest_seqno, meta.largest_seqno);
  }

  InternalStats::CompactionStats stats(1);
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.fd.GetFileSize();
  cfd->internal_stats()->AddCompactionStats(level, stats);
  cfd->internal_stats()->AddCFStats(
      InternalStats::BYTES_FLUSHED, meta.fd.GetFileSize());
  RecordTick(stats_, COMPACT_WRITE_BYTES, meta.fd.GetFileSize());
  return s;
}

Status DBImpl::FlushMemTableToOutputFile(ColumnFamilyData* cfd,
                                         bool* madeProgress,
                                         DeletionState& deletion_state,
                                         LogBuffer* log_buffer) {
  mutex_.AssertHeld();
  assert(cfd->imm()->size() != 0);
  assert(cfd->imm()->IsFlushPending());

  // Save the contents of the earliest memtable as a new Table
  uint64_t file_number;
  autovector<MemTable*> mems;
  cfd->imm()->PickMemtablesToFlush(&mems);
  if (mems.empty()) {
    LogToBuffer(log_buffer, "[%s] Nothing in memtable to flush",
                cfd->GetName().c_str());
    return Status::OK();
  }

  // record the logfile_number_ before we release the mutex
  // entries mems are (implicitly) sorted in ascending order by their created
  // time. We will use the first memtable's `edit` to keep the meta info for
  // this flush.
  MemTable* m = mems[0];
  VersionEdit* edit = m->GetEdits();
  edit->SetPrevLogNumber(0);
  // SetLogNumber(log_num) indicates logs with number smaller than log_num
  // will no longer be picked up for recovery.
  edit->SetLogNumber(mems.back()->GetNextLogNumber());
  edit->SetColumnFamily(cfd->GetID());

  // This will release and re-acquire the mutex.
  Status s = WriteLevel0Table(cfd, mems, edit, &file_number, log_buffer);

  if (s.ok() && shutting_down_.Acquire_Load() && cfd->IsDropped()) {
    s = Status::ShutdownInProgress(
        "Database shutdown or Column family drop during flush");
  }

  if (!s.ok()) {
    cfd->imm()->RollbackMemtableFlush(mems, file_number, &pending_outputs_);
  } else {
    // Replace immutable memtable with the generated Table
    s = cfd->imm()->InstallMemtableFlushResults(
        cfd, mems, versions_.get(), &mutex_, options_.info_log.get(),
        file_number, &pending_outputs_, &deletion_state.memtables_to_free,
        db_directory_.get(), log_buffer);
  }

  if (s.ok()) {
    InstallSuperVersion(cfd, deletion_state);
    if (madeProgress) {
      *madeProgress = 1;
    }
    Version::LevelSummaryStorage tmp;
    LogToBuffer(log_buffer, "[%s] Level summary: %s\n", cfd->GetName().c_str(),
                cfd->current()->LevelSummary(&tmp));

    if (disable_delete_obsolete_files_ == 0) {
      // add to deletion state
      while (alive_log_files_.size() &&
             alive_log_files_.begin()->number < versions_->MinLogNumber()) {
        const auto& earliest = *alive_log_files_.begin();
        deletion_state.log_delete_files.push_back(earliest.number);
        total_log_size_ -= earliest.size;
        alive_log_files_.pop_front();
      }
    }
  }

  if (!s.ok() && !s.IsShutdownInProgress() && options_.paranoid_checks &&
      bg_error_.ok()) {
    // if a bad error happened (not ShutdownInProgress) and paranoid_checks is
    // true, mark DB read-only
    bg_error_ = s;
  }
  RecordFlushIOStats();
  return s;
}

Status DBImpl::CompactRange(ColumnFamilyHandle* column_family,
                            const Slice* begin, const Slice* end,
                            bool reduce_level, int target_level,
                            uint32_t target_path_id) {
  if (target_path_id >= options_.db_paths.size()) {
    return Status::InvalidArgument("Invalid target path ID");
  }

  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  Status s = FlushMemTable(cfd, FlushOptions());
  if (!s.ok()) {
    LogFlush(options_.info_log);
    return s;
  }

  int max_level_with_files = 0;
  {
    MutexLock l(&mutex_);
    Version* base = cfd->current();
    for (int level = 1; level < cfd->NumberLevels(); level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  for (int level = 0; level <= max_level_with_files; level++) {
    // in case the compaction is unversal or if we're compacting the
    // bottom-most level, the output level will be the same as input one.
    // level 0 can never be the bottommost level (i.e. if all files are in level
    // 0, we will compact to level 1)
    if (cfd->options()->compaction_style == kCompactionStyleUniversal ||
        cfd->options()->compaction_style == kCompactionStyleFIFO ||
        (level == max_level_with_files && level > 0)) {
      s = RunManualCompaction(cfd, level, level, target_path_id, begin, end);
    } else {
      s = RunManualCompaction(cfd, level, level + 1, target_path_id, begin,
                              end);
    }
    if (!s.ok()) {
      LogFlush(options_.info_log);
      return s;
    }
  }

  if (reduce_level) {
    s = ReFitLevel(cfd, max_level_with_files, target_level);
  }
  LogFlush(options_.info_log);

  {
    MutexLock l(&mutex_);
    // an automatic compaction that has been scheduled might have been
    // preempted by the manual compactions. Need to schedule it back.
    MaybeScheduleFlushOrCompaction();
  }

  return s;
}

// return the same level if it cannot be moved
int DBImpl::FindMinimumEmptyLevelFitting(ColumnFamilyData* cfd, int level) {
  mutex_.AssertHeld();
  Version* current = cfd->current();
  int minimum_level = level;
  for (int i = level - 1; i > 0; --i) {
    // stop if level i is not empty
    if (current->NumLevelFiles(i) > 0) break;
    // stop if level i is too small (cannot fit the level files)
    if (cfd->compaction_picker()->MaxBytesForLevel(i) <
        current->NumLevelBytes(level)) {
      break;
    }

    minimum_level = i;
  }
  return minimum_level;
}

Status DBImpl::ReFitLevel(ColumnFamilyData* cfd, int level, int target_level) {
  assert(level < cfd->NumberLevels());

  SuperVersion* superversion_to_free = nullptr;
  SuperVersion* new_superversion = new SuperVersion();

  mutex_.Lock();

  // only allow one thread refitting
  if (refitting_level_) {
    mutex_.Unlock();
    Log(options_.info_log, "ReFitLevel: another thread is refitting");
    delete new_superversion;
    return Status::NotSupported("another thread is refitting");
  }
  refitting_level_ = true;

  // wait for all background threads to stop
  bg_work_gate_closed_ = true;
  while (bg_compaction_scheduled_ > 0 || bg_flush_scheduled_) {
    Log(options_.info_log,
        "RefitLevel: waiting for background threads to stop: %d %d",
        bg_compaction_scheduled_, bg_flush_scheduled_);
    bg_cv_.Wait();
  }

  // move to a smaller level
  int to_level = target_level;
  if (target_level < 0) {
    to_level = FindMinimumEmptyLevelFitting(cfd, level);
  }

  assert(to_level <= level);

  Status status;
  if (to_level < level) {
    Log(options_.info_log, "[%s] Before refitting:\n%s", cfd->GetName().c_str(),
        cfd->current()->DebugString().data());

    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());
    for (const auto& f : cfd->current()->files_[level]) {
      edit.DeleteFile(level, f->fd.GetNumber());
      edit.AddFile(to_level, f->fd.GetNumber(), f->fd.GetPathId(),
                   f->fd.GetFileSize(), f->smallest, f->largest,
                   f->smallest_seqno, f->largest_seqno);
    }
    Log(options_.info_log, "[%s] Apply version edit:\n%s",
        cfd->GetName().c_str(), edit.DebugString().data());

    status = versions_->LogAndApply(cfd, &edit, &mutex_, db_directory_.get());
    superversion_to_free = cfd->InstallSuperVersion(new_superversion, &mutex_);
    new_superversion = nullptr;

    Log(options_.info_log, "[%s] LogAndApply: %s\n", cfd->GetName().c_str(),
        status.ToString().data());

    if (status.ok()) {
      Log(options_.info_log, "[%s] After refitting:\n%s",
          cfd->GetName().c_str(), cfd->current()->DebugString().data());
    }
  }

  refitting_level_ = false;
  bg_work_gate_closed_ = false;

  mutex_.Unlock();
  delete superversion_to_free;
  delete new_superversion;
  return status;
}

int DBImpl::NumberLevels(ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  return cfh->cfd()->NumberLevels();
}

int DBImpl::MaxMemCompactionLevel(ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  return cfh->cfd()->options()->max_mem_compaction_level;
}

int DBImpl::Level0StopWriteTrigger(ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  return cfh->cfd()->options()->level0_stop_writes_trigger;
}

Status DBImpl::Flush(const FlushOptions& options,
                     ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  return FlushMemTable(cfh->cfd(), options);
}

SequenceNumber DBImpl::GetLatestSequenceNumber() const {
  return versions_->LastSequence();
}

Status DBImpl::RunManualCompaction(ColumnFamilyData* cfd, int input_level,
                                   int output_level, uint32_t output_path_id,
                                   const Slice* begin, const Slice* end) {
  assert(input_level >= 0);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.cfd = cfd;
  manual.input_level = input_level;
  manual.output_level = output_level;
  manual.output_path_id = output_path_id;
  manual.done = false;
  manual.in_progress = false;
  // For universal compaction, we enforce every manual compaction to compact
  // all files.
  if (begin == nullptr ||
      cfd->options()->compaction_style == kCompactionStyleUniversal ||
      cfd->options()->compaction_style == kCompactionStyleFIFO) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr ||
      cfd->options()->compaction_style == kCompactionStyleUniversal ||
      cfd->options()->compaction_style == kCompactionStyleFIFO) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);

  // When a manual compaction arrives, temporarily disable scheduling of
  // non-manual compactions and wait until the number of scheduled compaction
  // jobs drops to zero. This is needed to ensure that this manual compaction
  // can compact any range of keys/files.
  //
  // bg_manual_only_ is non-zero when at least one thread is inside
  // RunManualCompaction(), i.e. during that time no other compaction will
  // get scheduled (see MaybeScheduleFlushOrCompaction).
  //
  // Note that the following loop doesn't stop more that one thread calling
  // RunManualCompaction() from getting to the second while loop below.
  // However, only one of them will actually schedule compaction, while
  // others will wait on a condition variable until it completes.

  ++bg_manual_only_;
  while (bg_compaction_scheduled_ > 0) {
    Log(options_.info_log,
        "[%s] Manual compaction waiting for all other scheduled background "
        "compactions to finish",
        cfd->GetName().c_str());
    bg_cv_.Wait();
  }

  Log(options_.info_log, "[%s] Manual compaction starting",
      cfd->GetName().c_str());

  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    assert(bg_manual_only_ > 0);
    if (manual_compaction_ != nullptr) {
      // Running either this or some other manual compaction
      bg_cv_.Wait();
    } else {
      manual_compaction_ = &manual;
      assert(bg_compaction_scheduled_ == 0);
      bg_compaction_scheduled_++;
      env_->Schedule(&DBImpl::BGWorkCompaction, this, Env::Priority::LOW);
    }
  }

  assert(!manual.in_progress);
  assert(bg_manual_only_ > 0);
  --bg_manual_only_;
  return manual.status;
}

Status DBImpl::FlushMemTable(ColumnFamilyData* cfd,
                             const FlushOptions& options) {
  Writer w(&mutex_);
  w.batch = nullptr;
  w.sync = false;
  w.disableWAL = false;
  w.in_batch_group = false;
  w.done = false;
  w.timeout_hint_us = kNoTimeOut;

  Status s;
  {
    WriteContext context;
    MutexLock guard_lock(&mutex_);
    s = BeginWrite(&w, 0);
    assert(s.ok() && !w.done);  // No timeout and nobody should do our job

    // SetNewMemtableAndNewLogFile() will release and reacquire mutex
    // during execution
    s = SetNewMemtableAndNewLogFile(cfd, &context);
    cfd->imm()->FlushRequested();
    MaybeScheduleFlushOrCompaction();

    assert(!writers_.empty());
    assert(writers_.front() == &w);
    EndWrite(&w, &w, s);
  }


  if (s.ok() && options.wait) {
    // Wait until the compaction completes
    s = WaitForFlushMemTable(cfd);
  }
  return s;
}

Status DBImpl::WaitForFlushMemTable(ColumnFamilyData* cfd) {
  Status s;
  // Wait until the compaction completes
  MutexLock l(&mutex_);
  while (cfd->imm()->size() > 0 && bg_error_.ok()) {
    bg_cv_.Wait();
  }
  if (!bg_error_.ok()) {
    s = bg_error_;
  }
  return s;
}

void DBImpl::MaybeScheduleFlushOrCompaction() {
  mutex_.AssertHeld();
  bg_schedule_needed_ = false;
  if (bg_work_gate_closed_) {
    // gate closed for backgrond work
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else {
    bool is_flush_pending = false;
    // no need to refcount since we're under a mutex
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (cfd->imm()->IsFlushPending()) {
        is_flush_pending = true;
      }
    }
    if (is_flush_pending) {
      // memtable flush needed
      if (bg_flush_scheduled_ < options_.max_background_flushes) {
        bg_flush_scheduled_++;
        env_->Schedule(&DBImpl::BGWorkFlush, this, Env::Priority::HIGH);
      } else if (options_.max_background_flushes > 0) {
        bg_schedule_needed_ = true;
      }
    }
    bool is_compaction_needed = false;
    // no need to refcount since we're under a mutex
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (cfd->current()->NeedsCompaction()) {
        is_compaction_needed = true;
        break;
      }
    }

    // Schedule BGWorkCompaction if there's a compaction pending (or a memtable
    // flush, but the HIGH pool is not enabled)
    // Do it only if max_background_compactions hasn't been reached and
    // bg_manual_only_ == 0
    if (!bg_manual_only_ &&
        (is_compaction_needed ||
         (is_flush_pending && options_.max_background_flushes == 0))) {
      if (bg_compaction_scheduled_ < options_.max_background_compactions) {
        bg_compaction_scheduled_++;
        env_->Schedule(&DBImpl::BGWorkCompaction, this, Env::Priority::LOW);
      } else {
        bg_schedule_needed_ = true;
      }
    }
  }
}

void DBImpl::RecordFlushIOStats() {
  RecordTick(stats_, FLUSH_WRITE_BYTES, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}

void DBImpl::RecordCompactionIOStats() {
  RecordTick(stats_, COMPACT_READ_BYTES, IOSTATS(bytes_read));
  IOSTATS_RESET(bytes_read);
  RecordTick(stats_, COMPACT_WRITE_BYTES, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}

void DBImpl::BGWorkFlush(void* db) {
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::HIGH);
  reinterpret_cast<DBImpl*>(db)->BackgroundCallFlush();
}

void DBImpl::BGWorkCompaction(void* db) {
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::LOW);
  reinterpret_cast<DBImpl*>(db)->BackgroundCallCompaction();
}

Status DBImpl::BackgroundFlush(bool* madeProgress,
                               DeletionState& deletion_state,
                               LogBuffer* log_buffer) {
  mutex_.AssertHeld();
  // call_status is failure if at least one flush was a failure. even if
  // flushing one column family reports a failure, we will continue flushing
  // other column families. however, call_status will be a failure in that case.
  Status call_status;
  // refcounting in iteration
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    cfd->Ref();
    Status flush_status;
    while (flush_status.ok() && cfd->imm()->IsFlushPending()) {
      LogToBuffer(
          log_buffer,
          "BackgroundCallFlush doing FlushMemTableToOutputFile with column "
          "family [%s], flush slots available %d",
          cfd->GetName().c_str(),
          options_.max_background_flushes - bg_flush_scheduled_);
      flush_status = FlushMemTableToOutputFile(cfd, madeProgress,
                                               deletion_state, log_buffer);
    }
    if (call_status.ok() && !flush_status.ok()) {
      call_status = flush_status;
    }
    cfd->Unref();
  }
  versions_->GetColumnFamilySet()->FreeDeadColumnFamilies();
  return call_status;
}

void DBImpl::BackgroundCallFlush() {
  bool madeProgress = false;
  DeletionState deletion_state(true);
  assert(bg_flush_scheduled_);

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, options_.info_log.get());
  {
    MutexLock l(&mutex_);

    Status s;
    if (!shutting_down_.Acquire_Load()) {
      s = BackgroundFlush(&madeProgress, deletion_state, &log_buffer);
      if (!s.ok()) {
        // Wait a little bit before retrying background compaction in
        // case this is an environmental problem and we do not want to
        // chew up resources for failed compactions for the duration of
        // the problem.
        uint64_t error_cnt =
          default_cf_internal_stats_->BumpAndGetBackgroundErrorCount();
        bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
        mutex_.Unlock();
        Log(options_.info_log,
            "Waiting after background flush error: %s"
            "Accumulated background error counts: %" PRIu64,
            s.ToString().c_str(), error_cnt);
        log_buffer.FlushBufferToLog();
        LogFlush(options_.info_log);
        env_->SleepForMicroseconds(1000000);
        mutex_.Lock();
      }
    }

    // If !s.ok(), this means that Flush failed. In that case, we want
    // to delete all obsolete files and we force FindObsoleteFiles()
    FindObsoleteFiles(deletion_state, !s.ok());
    // delete unnecessary files if any, this is done outside the mutex
    if (deletion_state.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock();
      // Have to flush the info logs before bg_flush_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      log_buffer.FlushBufferToLog();
      if (deletion_state.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(deletion_state);
      }
      mutex_.Lock();
    }

    bg_flush_scheduled_--;
    // Any time the mutex is released After finding the work to do, another
    // thread might execute MaybeScheduleFlushOrCompaction(). It is possible
    // that there is a pending job but it is not scheduled because of the
    // max thread limit.
    if (madeProgress || bg_schedule_needed_) {
      MaybeScheduleFlushOrCompaction();
    }
    RecordFlushIOStats();
    bg_cv_.SignalAll();
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}

void DBImpl::BackgroundCallCompaction() {
  bool madeProgress = false;
  DeletionState deletion_state(true);

  MaybeDumpStats();
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, options_.info_log.get());
  {
    MutexLock l(&mutex_);
    assert(bg_compaction_scheduled_);
    Status s;
    if (!shutting_down_.Acquire_Load()) {
      s = BackgroundCompaction(&madeProgress, deletion_state, &log_buffer);
      if (!s.ok()) {
        // Wait a little bit before retrying background compaction in
        // case this is an environmental problem and we do not want to
        // chew up resources for failed compactions for the duration of
        // the problem.
        uint64_t error_cnt =
          default_cf_internal_stats_->BumpAndGetBackgroundErrorCount();
        bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
        mutex_.Unlock();
        log_buffer.FlushBufferToLog();
        Log(options_.info_log,
            "Waiting after background compaction error: %s, "
            "Accumulated background error counts: %" PRIu64,
            s.ToString().c_str(), error_cnt);
        LogFlush(options_.info_log);
        env_->SleepForMicroseconds(1000000);
        mutex_.Lock();
      }
    }

    // If !s.ok(), this means that Compaction failed. In that case, we want
    // to delete all obsolete files we might have created and we force
    // FindObsoleteFiles(). This is because deletion_state does not catch
    // all created files if compaction failed.
    FindObsoleteFiles(deletion_state, !s.ok());

    // delete unnecessary files if any, this is done outside the mutex
    if (deletion_state.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock();
      // Have to flush the info logs before bg_compaction_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      log_buffer.FlushBufferToLog();
      if (deletion_state.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(deletion_state);
      }
      mutex_.Lock();
    }

    bg_compaction_scheduled_--;

    versions_->GetColumnFamilySet()->FreeDeadColumnFamilies();

    // Previous compaction may have produced too many files in a level,
    // So reschedule another compaction if we made progress in the
    // last compaction.
    //
    // Also, any time the mutex is released After finding the work to do,
    // another thread might execute MaybeScheduleFlushOrCompaction(). It is
    // possible  that there is a pending job but it is not scheduled because of
    // the max thread limit.
    if (madeProgress || bg_schedule_needed_) {
      MaybeScheduleFlushOrCompaction();
    }
    if (madeProgress || bg_compaction_scheduled_ == 0 || bg_manual_only_ > 0) {
      // signal if
      // * madeProgress -- need to wakeup MakeRoomForWrite
      // * bg_compaction_scheduled_ == 0 -- need to wakeup ~DBImpl
      // * bg_manual_only_ > 0 -- need to wakeup RunManualCompaction
      // If none of this is true, there is no need to signal since nobody is
      // waiting for it
      bg_cv_.SignalAll();
    }
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}

Status DBImpl::BackgroundCompaction(bool* madeProgress,
                                    DeletionState& deletion_state,
                                    LogBuffer* log_buffer) {
  *madeProgress = false;
  mutex_.AssertHeld();

  bool is_manual = (manual_compaction_ != nullptr) &&
                   (manual_compaction_->in_progress == false);

  if (is_manual) {
    // another thread cannot pick up the same work
    manual_compaction_->in_progress = true;
  } else if (manual_compaction_ != nullptr) {
    // there should be no automatic compactions running when manual compaction
    // is running
    return Status::OK();
  }

  // FLUSH preempts compaction
  Status flush_stat;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    while (cfd->imm()->IsFlushPending()) {
      LogToBuffer(
          log_buffer,
          "BackgroundCompaction doing FlushMemTableToOutputFile, "
          "compaction slots available %d",
          options_.max_background_compactions - bg_compaction_scheduled_);
      cfd->Ref();
      flush_stat = FlushMemTableToOutputFile(cfd, madeProgress, deletion_state,
                                             log_buffer);
      cfd->Unref();
      if (!flush_stat.ok()) {
        if (is_manual) {
          manual_compaction_->status = flush_stat;
          manual_compaction_->done = true;
          manual_compaction_->in_progress = false;
          manual_compaction_ = nullptr;
        }
        return flush_stat;
      }
    }
  }

  unique_ptr<Compaction> c;
  InternalKey manual_end_storage;
  InternalKey* manual_end = &manual_end_storage;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    assert(m->in_progress);
    c.reset(m->cfd->CompactRange(m->input_level, m->output_level,
                                 m->output_path_id, m->begin, m->end,
                                 &manual_end));
    if (!c) {
      m->done = true;
    }
    LogToBuffer(log_buffer,
                "[%s] Manual compaction from level-%d to level-%d from %s .. "
                "%s; will stop at %s\n",
                m->cfd->GetName().c_str(), m->input_level, m->output_level,
                (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
                (m->end ? m->end->DebugString().c_str() : "(end)"),
                ((m->done || manual_end == nullptr)
                     ? "(end)"
                     : manual_end->DebugString().c_str()));
  } else {
    // no need to refcount in iteration since it's always under a mutex
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (!cfd->options()->disable_auto_compactions) {
        c.reset(cfd->PickCompaction(log_buffer));
        if (c != nullptr) {
          // update statistics
          MeasureTime(stats_, NUM_FILES_IN_SINGLE_COMPACTION,
                      c->inputs(0)->size());
          break;
        }
      }
    }
  }

  Status status;
  if (!c) {
    // Nothing to do
    LogToBuffer(log_buffer, "Compaction nothing to do");
  } else if (c->IsDeletionCompaction()) {
    // TODO(icanadi) Do we want to honor snapshots here? i.e. not delete old
    // file if there is alive snapshot pointing to it
    assert(c->num_input_files(1) == 0);
    assert(c->level() == 0);
    assert(c->column_family_data()->options()->compaction_style ==
           kCompactionStyleFIFO);
    for (const auto& f : *c->inputs(0)) {
      c->edit()->DeleteFile(c->level(), f->fd.GetNumber());
    }
    status = versions_->LogAndApply(c->column_family_data(), c->edit(), &mutex_,
                                    db_directory_.get());
    InstallSuperVersion(c->column_family_data(), deletion_state);
    LogToBuffer(log_buffer, "[%s] Deleted %d files\n",
                c->column_family_data()->GetName().c_str(),
                c->num_input_files(0));
    c->ReleaseCompactionFiles(status);
    *madeProgress = true;
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->fd.GetNumber());
    c->edit()->AddFile(c->level() + 1, f->fd.GetNumber(), f->fd.GetPathId(),
                       f->fd.GetFileSize(), f->smallest, f->largest,
                       f->smallest_seqno, f->largest_seqno);
    status = versions_->LogAndApply(c->column_family_data(), c->edit(), &mutex_,
                                    db_directory_.get());
    InstallSuperVersion(c->column_family_data(), deletion_state);

    Version::LevelSummaryStorage tmp;
    LogToBuffer(
        log_buffer, "[%s] Moved #%lld to level-%d %lld bytes %s: %s\n",
        c->column_family_data()->GetName().c_str(),
        static_cast<unsigned long long>(f->fd.GetNumber()), c->level() + 1,
        static_cast<unsigned long long>(f->fd.GetFileSize()),
        status.ToString().c_str(), c->input_version()->LevelSummary(&tmp));
    c->ReleaseCompactionFiles(status);
    *madeProgress = true;
  } else {
    MaybeScheduleFlushOrCompaction(); // do more compaction work in parallel.
    CompactionState* compact = new CompactionState(c.get());
    status = DoCompactionWork(compact, deletion_state, log_buffer);
    CleanupCompaction(compact, status);
    c->ReleaseCompactionFiles(status);
    c->ReleaseInputs();
    *madeProgress = true;
  }
  c.reset();

  if (status.ok()) {
    // Done
  } else if (status.IsShutdownInProgress()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log, "Compaction error: %s",
        status.ToString().c_str());
    if (options_.paranoid_checks && bg_error_.ok()) {
      bg_error_ = status;
    }
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->status = status;
      m->done = true;
    }
    // For universal compaction:
    //   Because universal compaction always happens at level 0, so one
    //   compaction will pick up all overlapped files. No files will be
    //   filtered out due to size limit and left for a successive compaction.
    //   So we can safely conclude the current compaction.
    //
    //   Also note that, if we don't stop here, then the current compaction
    //   writes a new file back to level 0, which will be used in successive
    //   compaction. Hence the manual compaction will never finish.
    //
    // Stop the compaction if manual_end points to nullptr -- this means
    // that we compacted the whole range. manual_end should always point
    // to nullptr in case of universal compaction
    if (manual_end == nullptr) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      // Universal and FIFO compactions should always compact the whole range
      assert(m->cfd->options()->compaction_style != kCompactionStyleUniversal);
      assert(m->cfd->options()->compaction_style != kCompactionStyleFIFO);
      m->tmp_storage = *manual_end;
      m->begin = &m->tmp_storage;
    }
    m->in_progress = false; // not being processed anymore
    manual_compaction_ = nullptr;
  }
  return status;
}

void DBImpl::CleanupCompaction(CompactionState* compact, Status status) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    compact->builder.reset();
  } else {
    assert(compact->outfile == nullptr);
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);

    // If this file was inserted into the table cache then remove
    // them here because this compaction was not committed.
    if (!status.ok()) {
      TableCache::Evict(table_cache_.get(), out.number);
    }
  }
  delete compact;
}

// Allocate the file numbers for the output file. We allocate as
// many output file numbers as there are files in level+1 (at least one)
// Insert them into pending_outputs so that they do not get deleted.
void DBImpl::AllocateCompactionOutputFileNumbers(CompactionState* compact) {
  mutex_.AssertHeld();
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  int filesNeeded = compact->compaction->num_input_files(1);
  for (int i = 0; i < std::max(filesNeeded, 1); i++) {
    uint64_t file_number = versions_->NewFileNumber();
    pending_outputs_[file_number] = compact->compaction->GetOutputPathId();
    compact->allocated_file_numbers.push_back(file_number);
  }
}

// Frees up unused file number.
void DBImpl::ReleaseCompactionUnusedFileNumbers(CompactionState* compact) {
  mutex_.AssertHeld();
  for (const auto file_number : compact->allocated_file_numbers) {
    pending_outputs_.erase(file_number);
  }
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  // If we have not yet exhausted the pre-allocated file numbers,
  // then use the one from the front. Otherwise, we have to acquire
  // the heavyweight lock and allocate a new file number.
  if (!compact->allocated_file_numbers.empty()) {
    file_number = compact->allocated_file_numbers.front();
    compact->allocated_file_numbers.pop_front();
  } else {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_[file_number] = compact->compaction->GetOutputPathId();
    mutex_.Unlock();
  }
  CompactionState::Output out;
  out.number = file_number;
  out.path_id = compact->compaction->GetOutputPathId();
  out.smallest.Clear();
  out.largest.Clear();
  out.smallest_seqno = out.largest_seqno = 0;
  compact->outputs.push_back(out);

  // Make the output file
  std::string fname = TableFileName(options_.db_paths, file_number,
                                    compact->compaction->GetOutputPathId());
  Status s = env_->NewWritableFile(fname, &compact->outfile, storage_options_);

  if (s.ok()) {
    compact->outfile->SetIOPriority(Env::IO_LOW);
    compact->outfile->SetPreallocationBlockSize(
        compact->compaction->OutputFilePreallocationSize());

    ColumnFamilyData* cfd = compact->compaction->column_family_data();
    compact->builder.reset(NewTableBuilder(
        *cfd->options(), cfd->internal_comparator(), compact->outfile.get(),
        compact->compaction->OutputCompressionType()));
  }
  LogFlush(options_.info_log);
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  const uint32_t output_path_id = compact->current_output()->path_id;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  compact->builder.reset();

  // Finish and check for file errors
  if (s.ok() && !options_.disableDataSync) {
    if (options_.use_fsync) {
      StopWatch sw(env_, stats_, COMPACTION_OUTFILE_SYNC_MICROS);
      s = compact->outfile->Fsync();
    } else {
      StopWatch sw(env_, stats_, COMPACTION_OUTFILE_SYNC_MICROS);
      s = compact->outfile->Sync();
    }
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  compact->outfile.reset();

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    ColumnFamilyData* cfd = compact->compaction->column_family_data();
    FileDescriptor fd(output_number, output_path_id, current_bytes);
    Iterator* iter = cfd->table_cache()->NewIterator(
        ReadOptions(), storage_options_, cfd->internal_comparator(), fd);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "[%s] Generated table #%" PRIu64 ": %" PRIu64
                             " keys, %" PRIu64 " bytes",
          cfd->GetName().c_str(), output_number, current_entries,
          current_bytes);
    }
  }
  return s;
}


Status DBImpl::InstallCompactionResults(CompactionState* compact,
                                        LogBuffer* log_buffer) {
  mutex_.AssertHeld();

  // paranoia: verify that the files that we started with
  // still exist in the current version and in the same original level.
  // This ensures that a concurrent compaction did not erroneously
  // pick the same files to compact.
  if (!versions_->VerifyCompactionFileConsistency(compact->compaction)) {
    Log(options_.info_log, "[%s] Compaction %d@%d + %d@%d files aborted",
        compact->compaction->column_family_data()->GetName().c_str(),
        compact->compaction->num_input_files(0), compact->compaction->level(),
        compact->compaction->num_input_files(1),
        compact->compaction->output_level());
    return Status::Corruption("Compaction input files inconsistent");
  }

  LogToBuffer(log_buffer, "[%s] Compacted %d@%d + %d@%d files => %lld bytes",
              compact->compaction->column_family_data()->GetName().c_str(),
              compact->compaction->num_input_files(0),
              compact->compaction->level(),
              compact->compaction->num_input_files(1),
              compact->compaction->output_level(),
              static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(compact->compaction->output_level(),
                                         out.number, out.path_id, out.file_size,
                                         out.smallest, out.largest,
                                         out.smallest_seqno, out.largest_seqno);
  }
  return versions_->LogAndApply(compact->compaction->column_family_data(),
                                compact->compaction->edit(), &mutex_,
                                db_directory_.get());
}

// Given a sequence number, return the sequence number of the
// earliest snapshot that this sequence number is visible in.
// The snapshots themselves are arranged in ascending order of
// sequence numbers.
// Employ a sequential search because the total number of
// snapshots are typically small.
inline SequenceNumber DBImpl::findEarliestVisibleSnapshot(
  SequenceNumber in, std::vector<SequenceNumber>& snapshots,
  SequenceNumber* prev_snapshot) {
  SequenceNumber prev __attribute__((unused)) = 0;
  for (const auto cur : snapshots) {
    assert(prev <= cur);
    if (cur >= in) {
      *prev_snapshot = prev;
      return cur;
    }
    prev = cur; // assignment
    assert(prev);
  }
  Log(options_.info_log,
      "Looking for seqid %" PRIu64 " but maxseqid is %" PRIu64 "", in,
      snapshots[snapshots.size() - 1]);
  assert(0);
  return 0;
}

uint64_t DBImpl::CallFlushDuringCompaction(ColumnFamilyData* cfd,
                                           DeletionState& deletion_state,
                                           LogBuffer* log_buffer) {
  if (options_.max_background_flushes > 0) {
    // flush thread will take care of this
    return 0;
  }
  if (cfd->imm()->imm_flush_needed.NoBarrier_Load() != nullptr) {
    const uint64_t imm_start = env_->NowMicros();
    mutex_.Lock();
    if (cfd->imm()->IsFlushPending()) {
      cfd->Ref();
      FlushMemTableToOutputFile(cfd, nullptr, deletion_state, log_buffer);
      cfd->Unref();
      bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
    }
    mutex_.Unlock();
    log_buffer->FlushBufferToLog();
    return env_->NowMicros() - imm_start;
  }
  return 0;
}

Status DBImpl::ProcessKeyValueCompaction(
    bool is_snapshot_supported,
    SequenceNumber visible_at_tip,
    SequenceNumber earliest_snapshot,
    SequenceNumber latest_snapshot,
    DeletionState& deletion_state,
    bool bottommost_level,
    int64_t& imm_micros,
    Iterator* input,
    CompactionState* compact,
    bool is_compaction_v2,
    LogBuffer* log_buffer) {
  size_t combined_idx = 0;
  Status status;
  std::string compaction_filter_value;
  ParsedInternalKey ikey;
  IterKey current_user_key;
  bool has_current_user_key = false;
  IterKey delete_key;
  SequenceNumber last_sequence_for_key __attribute__((unused)) =
    kMaxSequenceNumber;
  SequenceNumber visible_in_snapshot = kMaxSequenceNumber;
  ColumnFamilyData* cfd = compact->compaction->column_family_data();
  MergeHelper merge(
      cfd->user_comparator(), cfd->options()->merge_operator.get(),
      options_.info_log.get(), cfd->options()->min_partial_merge_operands,
      false /* internal key corruption is expected */);
  auto compaction_filter = cfd->options()->compaction_filter;
  std::unique_ptr<CompactionFilter> compaction_filter_from_factory = nullptr;
  if (!compaction_filter) {
    auto context = compact->GetFilterContextV1();
    compaction_filter_from_factory =
        cfd->options()->compaction_filter_factory->CreateCompactionFilter(
            context);
    compaction_filter = compaction_filter_from_factory.get();
  }

  int64_t key_drop_user = 0;
  int64_t key_drop_newer_entry = 0;
  int64_t key_drop_obsolete = 0;
  int64_t loop_cnt = 0;
  while (input->Valid() && !shutting_down_.Acquire_Load() &&
         !cfd->IsDropped()) {
    if (++loop_cnt > 1000) {
      if (key_drop_user > 0) {
        RecordTick(stats_, COMPACTION_KEY_DROP_USER, key_drop_user);
        key_drop_user = 0;
      }
      if (key_drop_newer_entry > 0) {
        RecordTick(stats_, COMPACTION_KEY_DROP_NEWER_ENTRY,
                   key_drop_newer_entry);
        key_drop_newer_entry = 0;
      }
      if (key_drop_obsolete > 0) {
        RecordTick(stats_, COMPACTION_KEY_DROP_OBSOLETE, key_drop_obsolete);
        key_drop_obsolete = 0;
      }
      RecordCompactionIOStats();
      loop_cnt = 0;
    }
    // FLUSH preempts compaction
    // TODO(icanadi) this currently only checks if flush is necessary on
    // compacting column family. we should also check if flush is necessary on
    // other column families, too
    imm_micros += CallFlushDuringCompaction(cfd, deletion_state, log_buffer);

    Slice key;
    Slice value;
    // If is_compaction_v2 is on, kv-pairs are reset to the prefix batch.
    // This prefix batch should contain results after calling
    // compaction_filter_v2.
    //
    // If is_compaction_v2 is off, this function will go through all the
    // kv-pairs in input.
    if (!is_compaction_v2) {
      key = input->key();
      value = input->value();
    } else {
      if (combined_idx >= compact->combined_key_buf_.size()) {
        break;
      }
      assert(combined_idx < compact->combined_key_buf_.size());
      key = compact->combined_key_buf_[combined_idx];
      value = compact->combined_value_buf_[combined_idx];

      ++combined_idx;
    }

    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    bool current_entry_is_merging = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      // TODO: error key stays in db forever? Figure out the intention/rationale
      // v10 error v8 : we cannot hide v8 even though it's pretty obvious.
      current_user_key.Clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
      visible_in_snapshot = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          cfd->user_comparator()->Compare(ikey.user_key,
                                          current_user_key.GetKey()) != 0) {
        // First occurrence of this user key
        current_user_key.SetKey(ikey.user_key);
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
        visible_in_snapshot = kMaxSequenceNumber;
        // apply the compaction filter to the first occurrence of the user key
        if (compaction_filter && !is_compaction_v2 &&
            ikey.type == kTypeValue &&
            (visible_at_tip || ikey.sequence > latest_snapshot)) {
          // If the user has specified a compaction filter and the sequence
          // number is greater than any external snapshot, then invoke the
          // filter.
          // If the return value of the compaction filter is true, replace
          // the entry with a delete marker.
          bool value_changed = false;
          compaction_filter_value.clear();
          bool to_delete = compaction_filter->Filter(
              compact->compaction->level(), ikey.user_key, value,
              &compaction_filter_value, &value_changed);
          if (to_delete) {
            // make a copy of the original key and convert it to a delete
            delete_key.SetInternalKey(ExtractUserKey(key), ikey.sequence,
                                      kTypeDeletion);
            // anchor the key again
            key = delete_key.GetKey();
            // needed because ikey is backed by key
            ParseInternalKey(key, &ikey);
            // no value associated with delete
            value.clear();
            ++key_drop_user;
          } else if (value_changed) {
            value = compaction_filter_value;
          }
        }
      }

      // If there are no snapshots, then this kv affect visibility at tip.
      // Otherwise, search though all existing snapshots to find
      // the earlist snapshot that is affected by this kv.
      SequenceNumber prev_snapshot = 0; // 0 means no previous snapshot
      SequenceNumber visible = visible_at_tip ? visible_at_tip :
        is_snapshot_supported ?  findEarliestVisibleSnapshot(ikey.sequence,
                                  compact->existing_snapshots, &prev_snapshot)
                              : 0;

      if (visible_in_snapshot == visible) {
        // If the earliest snapshot is which this key is visible in
        // is the same as the visibily of a previous instance of the
        // same key, then this kv is not visible in any snapshot.
        // Hidden by an newer entry for same user key
        // TODO: why not > ?
        assert(last_sequence_for_key >= ikey.sequence);
        drop = true;    // (A)
        ++key_drop_newer_entry;
      } else if (ikey.type == kTypeDeletion &&
          ikey.sequence <= earliest_snapshot &&
          compact->compaction->KeyNotExistsBeyondOutputLevel(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
        ++key_drop_obsolete;
      } else if (ikey.type == kTypeMerge) {
        if (!merge.HasOperator()) {
          LogToBuffer(log_buffer, "Options::merge_operator is null.");
          status = Status::InvalidArgument(
              "merge_operator is not properly initialized.");
          break;
        }
        // We know the merge type entry is not hidden, otherwise we would
        // have hit (A)
        // We encapsulate the merge related state machine in a different
        // object to minimize change to the existing flow. Turn out this
        // logic could also be nicely re-used for memtable flush purge
        // optimization in BuildTable.
        int steps = 0;
        merge.MergeUntil(input, prev_snapshot, bottommost_level,
            options_.statistics.get(), &steps);
        // Skip the Merge ops
        combined_idx = combined_idx - 1 + steps;

        current_entry_is_merging = true;
        if (merge.IsSuccess()) {
          // Successfully found Put/Delete/(end-of-key-range) while merging
          // Get the merge result
          key = merge.key();
          ParseInternalKey(key, &ikey);
          value = merge.value();
        } else {
          // Did not find a Put/Delete/(end-of-key-range) while merging
          // We now have some stack of merge operands to write out.
          // NOTE: key,value, and ikey are now referring to old entries.
          //       These will be correctly set below.
          assert(!merge.keys().empty());
          assert(merge.keys().size() == merge.values().size());

          // Hack to make sure last_sequence_for_key is correct
          ParseInternalKey(merge.keys().front(), &ikey);
        }
      }

      last_sequence_for_key = ikey.sequence;
      visible_in_snapshot = visible;
    }

    if (!drop) {
      // We may write a single key (e.g.: for Put/Delete or successful merge).
      // Or we may instead have to write a sequence/list of keys.
      // We have to write a sequence iff we have an unsuccessful merge
      bool has_merge_list = current_entry_is_merging && !merge.IsSuccess();
      const std::deque<std::string>* keys = nullptr;
      const std::deque<std::string>* values = nullptr;
      std::deque<std::string>::const_reverse_iterator key_iter;
      std::deque<std::string>::const_reverse_iterator value_iter;
      if (has_merge_list) {
        keys = &merge.keys();
        values = &merge.values();
        key_iter = keys->rbegin();    // The back (*rbegin()) is the first key
        value_iter = values->rbegin();

        key = Slice(*key_iter);
        value = Slice(*value_iter);
      }

      // If we have a list of keys to write, traverse the list.
      // If we have a single key to write, simply write that key.
      while (true) {
        // Invariant: key,value,ikey will always be the next entry to write
        char* kptr = (char*)key.data();
        std::string kstr;

        // Zeroing out the sequence number leads to better compression.
        // If this is the bottommost level (no files in lower levels)
        // and the earliest snapshot is larger than this seqno
        // then we can squash the seqno to zero.
        if (bottommost_level && ikey.sequence < earliest_snapshot &&
            ikey.type != kTypeMerge) {
          assert(ikey.type != kTypeDeletion);
          // make a copy because updating in place would cause problems
          // with the priority queue that is managing the input key iterator
          kstr.assign(key.data(), key.size());
          kptr = (char *)kstr.c_str();
          UpdateInternalKey(kptr, key.size(), (uint64_t)0, ikey.type);
        }

        Slice newkey(kptr, key.size());
        assert((key.clear(), 1)); // we do not need 'key' anymore

        // Open output file if necessary
        if (compact->builder == nullptr) {
          status = OpenCompactionOutputFile(compact);
          if (!status.ok()) {
            break;
          }
        }

        SequenceNumber seqno = GetInternalKeySeqno(newkey);
        if (compact->builder->NumEntries() == 0) {
          compact->current_output()->smallest.DecodeFrom(newkey);
          compact->current_output()->smallest_seqno = seqno;
        } else {
          compact->current_output()->smallest_seqno =
            std::min(compact->current_output()->smallest_seqno, seqno);
        }
        compact->current_output()->largest.DecodeFrom(newkey);
        compact->builder->Add(newkey, value);
        compact->current_output()->largest_seqno =
          std::max(compact->current_output()->largest_seqno, seqno);

        // Close output file if it is big enough
        if (compact->builder->FileSize() >=
            compact->compaction->MaxOutputFileSize()) {
          status = FinishCompactionOutputFile(compact, input);
          if (!status.ok()) {
            break;
          }
        }

        // If we have a list of entries, move to next element
        // If we only had one entry, then break the loop.
        if (has_merge_list) {
          ++key_iter;
          ++value_iter;

          // If at end of list
          if (key_iter == keys->rend() || value_iter == values->rend()) {
            // Sanity Check: if one ends, then both end
            assert(key_iter == keys->rend() && value_iter == values->rend());
            break;
          }

          // Otherwise not at end of list. Update key, value, and ikey.
          key = Slice(*key_iter);
          value = Slice(*value_iter);
          ParseInternalKey(key, &ikey);

        } else{
          // Only had one item to begin with (Put/Delete)
          break;
        }
      }
    }

    // MergeUntil has moved input to the next entry
    if (!current_entry_is_merging) {
      input->Next();
    }
  }
  if (key_drop_user > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_USER, key_drop_user);
  }
  if (key_drop_newer_entry > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_NEWER_ENTRY, key_drop_newer_entry);
  }
  if (key_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_OBSOLETE, key_drop_obsolete);
  }
  RecordCompactionIOStats();

  return status;
}

void DBImpl::CallCompactionFilterV2(CompactionState* compact,
  CompactionFilterV2* compaction_filter_v2) {
  if (compact == nullptr || compaction_filter_v2 == nullptr) {
    return;
  }

  // Assemble slice vectors for user keys and existing values.
  // We also keep track of our parsed internal key structs because
  // we may need to access the sequence number in the event that
  // keys are garbage collected during the filter process.
  std::vector<ParsedInternalKey> ikey_buf;
  std::vector<Slice> user_key_buf;
  std::vector<Slice> existing_value_buf;

  for (const auto& key : compact->key_str_buf_) {
    ParsedInternalKey ikey;
    ParseInternalKey(Slice(key), &ikey);
    ikey_buf.emplace_back(ikey);
    user_key_buf.emplace_back(ikey.user_key);
  }
  for (const auto& value : compact->existing_value_str_buf_) {
    existing_value_buf.emplace_back(Slice(value));
  }

  // If the user has specified a compaction filter and the sequence
  // number is greater than any external snapshot, then invoke the
  // filter.
  // If the return value of the compaction filter is true, replace
  // the entry with a delete marker.
  compact->to_delete_buf_ = compaction_filter_v2->Filter(
      compact->compaction->level(),
      user_key_buf, existing_value_buf,
      &compact->new_value_buf_,
      &compact->value_changed_buf_);

  // new_value_buf_.size() <= to_delete__buf_.size(). "=" iff all
  // kv-pairs in this compaction run needs to be deleted.
  assert(compact->to_delete_buf_.size() ==
      compact->key_str_buf_.size());
  assert(compact->to_delete_buf_.size() ==
      compact->existing_value_str_buf_.size());
  assert(compact->to_delete_buf_.size() ==
      compact->value_changed_buf_.size());

  int new_value_idx = 0;
  for (unsigned int i = 0; i < compact->to_delete_buf_.size(); ++i) {
    if (compact->to_delete_buf_[i]) {
      // update the string buffer directly
      // the Slice buffer points to the updated buffer
      UpdateInternalKey(&compact->key_str_buf_[i][0],
                        compact->key_str_buf_[i].size(),
                        ikey_buf[i].sequence,
                        kTypeDeletion);

      // no value associated with delete
      compact->existing_value_str_buf_[i].clear();
      RecordTick(stats_, COMPACTION_KEY_DROP_USER);
    } else if (compact->value_changed_buf_[i]) {
      compact->existing_value_str_buf_[i] =
          compact->new_value_buf_[new_value_idx++];
    }
  }  // for
}

Status DBImpl::DoCompactionWork(CompactionState* compact,
                                DeletionState& deletion_state,
                                LogBuffer* log_buffer) {
  assert(compact);
  compact->CleanupBatchBuffer();
  compact->CleanupMergedBuffer();
  bool prefix_initialized = false;

  // Generate file_levels_ for compaction berfore making Iterator
  compact->compaction->GenerateFileLevels();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions
  ColumnFamilyData* cfd = compact->compaction->column_family_data();
  LogToBuffer(
      log_buffer,
      "[%s] Compacting %d@%d + %d@%d files, score %.2f slots available %d",
      cfd->GetName().c_str(), compact->compaction->num_input_files(0),
      compact->compaction->level(), compact->compaction->num_input_files(1),
      compact->compaction->output_level(), compact->compaction->score(),
      options_.max_background_compactions - bg_compaction_scheduled_);
  char scratch[2345];
  compact->compaction->Summary(scratch, sizeof(scratch));
  LogToBuffer(log_buffer, "[%s] Compaction start summary: %s\n",
              cfd->GetName().c_str(), scratch);

  assert(cfd->current()->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(!compact->outfile);

  SequenceNumber visible_at_tip = 0;
  SequenceNumber earliest_snapshot;
  SequenceNumber latest_snapshot = 0;
  snapshots_.getAll(compact->existing_snapshots);
  if (compact->existing_snapshots.size() == 0) {
    // optimize for fast path if there are no snapshots
    visible_at_tip = versions_->LastSequence();
    earliest_snapshot = visible_at_tip;
  } else {
    latest_snapshot = compact->existing_snapshots.back();
    // Add the current seqno as the 'latest' virtual
    // snapshot to the end of this list.
    compact->existing_snapshots.push_back(versions_->LastSequence());
    earliest_snapshot = compact->existing_snapshots[0];
  }

  // Is this compaction producing files at the bottommost level?
  bool bottommost_level = compact->compaction->BottomMostLevel();

  // Allocate the output file numbers before we release the lock
  AllocateCompactionOutputFileNumbers(compact);

  bool is_snapshot_supported = IsSnapshotSupported();
  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();
  log_buffer->FlushBufferToLog();

  const uint64_t start_micros = env_->NowMicros();
  unique_ptr<Iterator> input(versions_->MakeInputIterator(compact->compaction));
  input->SeekToFirst();
  shared_ptr<Iterator> backup_input(
      versions_->MakeInputIterator(compact->compaction));
  backup_input->SeekToFirst();

  Status status;
  ParsedInternalKey ikey;
  std::unique_ptr<CompactionFilterV2> compaction_filter_from_factory_v2
    = nullptr;
  auto context = compact->GetFilterContext();
  compaction_filter_from_factory_v2 =
      cfd->options()->compaction_filter_factory_v2->CreateCompactionFilterV2(
          context);
  auto compaction_filter_v2 =
    compaction_filter_from_factory_v2.get();

  // temp_backup_input always point to the start of the current buffer
  // temp_backup_input = backup_input;
  // iterate through input,
  // 1) buffer ineligible keys and value keys into 2 separate buffers;
  // 2) send value_buffer to compaction filter and alternate the values;
  // 3) merge value_buffer with ineligible_value_buffer;
  // 4) run the modified "compaction" using the old for loop.
  if (compaction_filter_v2) {
    while (backup_input->Valid() && !shutting_down_.Acquire_Load() &&
           !cfd->IsDropped()) {
      // FLUSH preempts compaction
      // TODO(icanadi) this currently only checks if flush is necessary on
      // compacting column family. we should also check if flush is necessary on
      // other column families, too
      imm_micros += CallFlushDuringCompaction(cfd, deletion_state, log_buffer);

      Slice key = backup_input->key();
      Slice value = backup_input->value();

      if (!ParseInternalKey(key, &ikey)) {
        // log error
        Log(options_.info_log, "[%s] Failed to parse key: %s",
            cfd->GetName().c_str(), key.ToString().c_str());
        continue;
      } else {
        const SliceTransform* transformer =
            cfd->options()->compaction_filter_factory_v2->GetPrefixExtractor();
        const auto key_prefix = transformer->Transform(ikey.user_key);
        if (!prefix_initialized) {
          compact->cur_prefix_ = key_prefix.ToString();
          prefix_initialized = true;
        }
        // If the prefix remains the same, keep buffering
        if (key_prefix.compare(Slice(compact->cur_prefix_)) == 0) {
          // Apply the compaction filter V2 to all the kv pairs sharing
          // the same prefix
          if (ikey.type == kTypeValue &&
              (visible_at_tip || ikey.sequence > latest_snapshot)) {
            // Buffer all keys sharing the same prefix for CompactionFilterV2
            // Iterate through keys to check prefix
            compact->BufferKeyValueSlices(key, value);
          } else {
            // buffer ineligible keys
            compact->BufferOtherKeyValueSlices(key, value);
          }
          backup_input->Next();
          continue;
          // finish changing values for eligible keys
        } else {
          // Now prefix changes, this batch is done.
          // Call compaction filter on the buffered values to change the value
          if (compact->key_str_buf_.size() > 0) {
            CallCompactionFilterV2(compact, compaction_filter_v2);
          }
          compact->cur_prefix_ = key_prefix.ToString();
        }
      }

      // Merge this batch of data (values + ineligible keys)
      compact->MergeKeyValueSliceBuffer(&cfd->internal_comparator());

      // Done buffering for the current prefix. Spit it out to disk
      // Now just iterate through all the kv-pairs
      status = ProcessKeyValueCompaction(
          is_snapshot_supported,
          visible_at_tip,
          earliest_snapshot,
          latest_snapshot,
          deletion_state,
          bottommost_level,
          imm_micros,
          input.get(),
          compact,
          true,
          log_buffer);

      if (!status.ok()) {
        break;
      }

      // After writing the kv-pairs, we can safely remove the reference
      // to the string buffer and clean them up
      compact->CleanupBatchBuffer();
      compact->CleanupMergedBuffer();
      // Buffer the key that triggers the mismatch in prefix
      if (ikey.type == kTypeValue &&
        (visible_at_tip || ikey.sequence > latest_snapshot)) {
        compact->BufferKeyValueSlices(key, value);
      } else {
        compact->BufferOtherKeyValueSlices(key, value);
      }
      backup_input->Next();
      if (!backup_input->Valid()) {
        // If this is the single last value, we need to merge it.
        if (compact->key_str_buf_.size() > 0) {
          CallCompactionFilterV2(compact, compaction_filter_v2);
        }
        compact->MergeKeyValueSliceBuffer(&cfd->internal_comparator());

        status = ProcessKeyValueCompaction(
            is_snapshot_supported,
            visible_at_tip,
            earliest_snapshot,
            latest_snapshot,
            deletion_state,
            bottommost_level,
            imm_micros,
            input.get(),
            compact,
            true,
            log_buffer);

        compact->CleanupBatchBuffer();
        compact->CleanupMergedBuffer();
      }
    }  // done processing all prefix batches
    // finish the last batch
    if (compact->key_str_buf_.size() > 0) {
      CallCompactionFilterV2(compact, compaction_filter_v2);
    }
    compact->MergeKeyValueSliceBuffer(&cfd->internal_comparator());
    status = ProcessKeyValueCompaction(
        is_snapshot_supported,
        visible_at_tip,
        earliest_snapshot,
        latest_snapshot,
        deletion_state,
        bottommost_level,
        imm_micros,
        input.get(),
        compact,
        true,
        log_buffer);
  }  // checking for compaction filter v2

  if (!compaction_filter_v2) {
    status = ProcessKeyValueCompaction(
      is_snapshot_supported,
      visible_at_tip,
      earliest_snapshot,
      latest_snapshot,
      deletion_state,
      bottommost_level,
      imm_micros,
      input.get(),
      compact,
      false,
      log_buffer);
  }

  if (status.ok() && (shutting_down_.Acquire_Load() || cfd->IsDropped())) {
    status = Status::ShutdownInProgress(
        "Database shutdown or Column family drop during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input.get());
  }
  if (status.ok()) {
    status = input->status();
  }
  input.reset();

  if (!options_.disableDataSync) {
    db_directory_->Fsync();
  }

  InternalStats::CompactionStats stats(1);
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  stats.files_in_leveln = compact->compaction->num_input_files(0);
  stats.files_in_levelnp1 = compact->compaction->num_input_files(1);
  MeasureTime(stats_, COMPACTION_TIME, stats.micros);

  int num_output_files = compact->outputs.size();
  if (compact->builder != nullptr) {
    // An error occurred so ignore the last output.
    assert(num_output_files > 0);
    --num_output_files;
  }
  stats.files_out_levelnp1 = num_output_files;

  for (int i = 0; i < compact->compaction->num_input_files(0); i++) {
    stats.bytes_readn += compact->compaction->input(0, i)->fd.GetFileSize();
  }

  for (int i = 0; i < compact->compaction->num_input_files(1); i++) {
    stats.bytes_readnp1 += compact->compaction->input(1, i)->fd.GetFileSize();
  }

  for (int i = 0; i < num_output_files; i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  RecordCompactionIOStats();

  LogFlush(options_.info_log);
  mutex_.Lock();
  cfd->internal_stats()->AddCompactionStats(
      compact->compaction->output_level(), stats);

  // if there were any unused file number (mostly in case of
  // compaction error), free up the entry from pending_putputs
  ReleaseCompactionUnusedFileNumbers(compact);

  if (status.ok()) {
    status = InstallCompactionResults(compact, log_buffer);
    InstallSuperVersion(cfd, deletion_state);
  }
  Version::LevelSummaryStorage tmp;
  LogToBuffer(
      log_buffer,
      "[%s] compacted to: %s, %.1f MB/sec, level %d, files in(%d, %d) out(%d) "
      "MB in(%.1f, %.1f) out(%.1f), read-write-amplify(%.1f) "
      "write-amplify(%.1f) %s\n",
      cfd->GetName().c_str(), cfd->current()->LevelSummary(&tmp),
      (stats.bytes_readn + stats.bytes_readnp1 + stats.bytes_written) /
          (double)stats.micros,
      compact->compaction->output_level(), stats.files_in_leveln,
      stats.files_in_levelnp1, stats.files_out_levelnp1,
      stats.bytes_readn / 1048576.0, stats.bytes_readnp1 / 1048576.0,
      stats.bytes_written / 1048576.0,
      (stats.bytes_written + stats.bytes_readnp1 + stats.bytes_readn) /
          (double)stats.bytes_readn,
      stats.bytes_written / (double)stats.bytes_readn,
      status.ToString().c_str());

  return status;
}

namespace {
struct IterState {
  IterState(DBImpl* db, port::Mutex* mu, SuperVersion* super_version)
      : db(db), mu(mu), super_version(super_version) {}

  DBImpl* db;
  port::Mutex* mu;
  SuperVersion* super_version;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);

  if (state->super_version->Unref()) {
    DBImpl::DeletionState deletion_state;

    state->mu->Lock();
    state->super_version->Cleanup();
    state->db->FindObsoleteFiles(deletion_state, false, true);
    state->mu->Unlock();

    delete state->super_version;
    if (deletion_state.HaveSomethingToDelete()) {
      state->db->PurgeObsoleteFiles(deletion_state);
    }
  }

  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      ColumnFamilyData* cfd,
                                      SuperVersion* super_version,
                                      Arena* arena) {
  Iterator* internal_iter;
  if (arena != nullptr) {
    // Need to create internal iterator from the arena.
    MergeIteratorBuilder merge_iter_builder(&cfd->internal_comparator(), arena);
    // Collect iterator for mutable mem
    merge_iter_builder.AddIterator(
        super_version->mem->NewIterator(options, arena));
    // Collect all needed child iterators for immutable memtables
    super_version->imm->AddIterators(options, &merge_iter_builder);
    // Collect iterators for files in L0 - Ln
    super_version->current->AddIterators(options, storage_options_,
                                         &merge_iter_builder);
    internal_iter = merge_iter_builder.Finish();
  } else {
    // Need to create internal iterator using malloc.
    std::vector<Iterator*> iterator_list;
    // Collect iterator for mutable mem
    iterator_list.push_back(super_version->mem->NewIterator(options));
    // Collect all needed child iterators for immutable memtables
    super_version->imm->AddIterators(options, &iterator_list);
    // Collect iterators for files in L0 - Ln
    super_version->current->AddIterators(options, storage_options_,
                                         &iterator_list);
    internal_iter = NewMergingIterator(&cfd->internal_comparator(),
                                       &iterator_list[0], iterator_list.size());
  }
  IterState* cleanup = new IterState(this, &mutex_, super_version);
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  return internal_iter;
}

ColumnFamilyHandle* DBImpl::DefaultColumnFamily() const {
  return default_cf_handle_;
}

Status DBImpl::Get(const ReadOptions& options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   std::string* value) {
  return GetImpl(options, column_family, key, value);
}

// DeletionState gets created and destructed outside of the lock -- we
// use this convinently to:
// * malloc one SuperVersion() outside of the lock -- new_superversion
// * delete SuperVersion()s outside of the lock -- superversions_to_free
//
// However, if InstallSuperVersion() gets called twice with the same,
// deletion_state, we can't reuse the SuperVersion() that got malloced because
// first call already used it. In that rare case, we take a hit and create a
// new SuperVersion() inside of the mutex. We do similar thing
// for superversion_to_free
void DBImpl::InstallSuperVersion(ColumnFamilyData* cfd,
                                 DeletionState& deletion_state) {
  mutex_.AssertHeld();
  // if new_superversion == nullptr, it means somebody already used it
  SuperVersion* new_superversion =
    (deletion_state.new_superversion != nullptr) ?
    deletion_state.new_superversion : new SuperVersion();
  SuperVersion* old_superversion =
      cfd->InstallSuperVersion(new_superversion, &mutex_);
  deletion_state.new_superversion = nullptr;
  deletion_state.superversions_to_free.push_back(old_superversion);
}

Status DBImpl::GetImpl(const ReadOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       std::string* value, bool* value_found) {
  StopWatch sw(env_, stats_, DB_GET);
  PERF_TIMER_GUARD(get_snapshot_time);

  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  // Acquire SuperVersion
  SuperVersion* sv = GetAndRefSuperVersion(cfd);

  // Prepare to store a list of merge operations if merge occurs.
  MergeContext merge_context;

  Status s;
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  LookupKey lkey(key, snapshot);
  PERF_TIMER_STOP(get_snapshot_time);

  if (sv->mem->Get(lkey, value, &s, merge_context, *cfd->options())) {
    // Done
    RecordTick(stats_, MEMTABLE_HIT);
  } else if (sv->imm->Get(lkey, value, &s, merge_context, *cfd->options())) {
    // Done
    RecordTick(stats_, MEMTABLE_HIT);
  } else {
    PERF_TIMER_GUARD(get_from_output_files_time);
    sv->current->Get(options, lkey, value, &s, &merge_context, value_found);
    RecordTick(stats_, MEMTABLE_MISS);
  }

  {
    PERF_TIMER_GUARD(get_post_process_time);

    ReturnAndCleanupSuperVersion(cfd, sv);

    RecordTick(stats_, NUMBER_KEYS_READ);
    RecordTick(stats_, BYTES_READ, value->size());
  }
  return s;
}

std::vector<Status> DBImpl::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {

  StopWatch sw(env_, stats_, DB_MULTIGET);
  PERF_TIMER_GUARD(get_snapshot_time);

  SequenceNumber snapshot;

  struct MultiGetColumnFamilyData {
    ColumnFamilyData* cfd;
    SuperVersion* super_version;
  };
  std::unordered_map<uint32_t, MultiGetColumnFamilyData*> multiget_cf_data;
  // fill up and allocate outside of mutex
  for (auto cf : column_family) {
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(cf);
    auto cfd = cfh->cfd();
    if (multiget_cf_data.find(cfd->GetID()) == multiget_cf_data.end()) {
      auto mgcfd = new MultiGetColumnFamilyData();
      mgcfd->cfd = cfd;
      multiget_cf_data.insert({cfd->GetID(), mgcfd});
    }
  }

  mutex_.Lock();
  if (options.snapshot != nullptr) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }
  for (auto mgd_iter : multiget_cf_data) {
    mgd_iter.second->super_version =
        mgd_iter.second->cfd->GetSuperVersion()->Ref();
  }
  mutex_.Unlock();

  // Contain a list of merge operations if merge occurs.
  MergeContext merge_context;

  // Note: this always resizes the values array
  size_t num_keys = keys.size();
  std::vector<Status> stat_list(num_keys);
  values->resize(num_keys);

  // Keep track of bytes that we read for statistics-recording later
  uint64_t bytes_read = 0;
  PERF_TIMER_STOP(get_snapshot_time);

  // For each of the given keys, apply the entire "get" process as follows:
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  for (size_t i = 0; i < num_keys; ++i) {
    merge_context.Clear();
    Status& s = stat_list[i];
    std::string* value = &(*values)[i];

    LookupKey lkey(keys[i], snapshot);
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family[i]);
    auto mgd_iter = multiget_cf_data.find(cfh->cfd()->GetID());
    assert(mgd_iter != multiget_cf_data.end());
    auto mgd = mgd_iter->second;
    auto super_version = mgd->super_version;
    auto cfd = mgd->cfd;
    if (super_version->mem->Get(lkey, value, &s, merge_context,
                                *cfd->options())) {
      // Done
    } else if (super_version->imm->Get(lkey, value, &s, merge_context,
                                       *cfd->options())) {
      // Done
    } else {
      super_version->current->Get(options, lkey, value, &s, &merge_context);
    }

    if (s.ok()) {
      bytes_read += value->size();
    }
  }

  // Post processing (decrement reference counts and record statistics)
  PERF_TIMER_GUARD(get_post_process_time);
  autovector<SuperVersion*> superversions_to_delete;

  // TODO(icanadi) do we need lock here or just around Cleanup()?
  mutex_.Lock();
  for (auto mgd_iter : multiget_cf_data) {
    auto mgd = mgd_iter.second;
    if (mgd->super_version->Unref()) {
      mgd->super_version->Cleanup();
      superversions_to_delete.push_back(mgd->super_version);
    }
  }
  mutex_.Unlock();

  for (auto td : superversions_to_delete) {
    delete td;
  }
  for (auto mgd : multiget_cf_data) {
    delete mgd.second;
  }

  RecordTick(stats_, NUMBER_MULTIGET_CALLS);
  RecordTick(stats_, NUMBER_MULTIGET_KEYS_READ, num_keys);
  RecordTick(stats_, NUMBER_MULTIGET_BYTES_READ, bytes_read);
  PERF_TIMER_STOP(get_post_process_time);

  return stat_list;
}

Status DBImpl::CreateColumnFamily(const ColumnFamilyOptions& options,
                                  const std::string& column_family_name,
                                  ColumnFamilyHandle** handle) {
  *handle = nullptr;
  MutexLock l(&mutex_);

  if (versions_->GetColumnFamilySet()->GetColumnFamily(column_family_name) !=
      nullptr) {
    return Status::InvalidArgument("Column family already exists");
  }
  VersionEdit edit;
  edit.AddColumnFamily(column_family_name);
  uint32_t new_id = versions_->GetColumnFamilySet()->GetNextColumnFamilyID();
  edit.SetColumnFamily(new_id);
  edit.SetLogNumber(logfile_number_);
  edit.SetComparatorName(options.comparator->Name());

  // LogAndApply will both write the creation in MANIFEST and create
  // ColumnFamilyData object
  Status s = versions_->LogAndApply(nullptr, &edit, &mutex_,
                                    db_directory_.get(), false, &options);
  if (s.ok()) {
    single_column_family_mode_ = false;
    auto cfd =
        versions_->GetColumnFamilySet()->GetColumnFamily(column_family_name);
    assert(cfd != nullptr);
    delete cfd->InstallSuperVersion(new SuperVersion(), &mutex_);
    *handle = new ColumnFamilyHandleImpl(cfd, this, &mutex_);
    Log(options_.info_log, "Created column family [%s] (ID %u)",
        column_family_name.c_str(), (unsigned)cfd->GetID());
    max_total_in_memory_state_ += cfd->options()->write_buffer_size *
                                  cfd->options()->max_write_buffer_number;
  } else {
    Log(options_.info_log, "Creating column family [%s] FAILED -- %s",
        column_family_name.c_str(), s.ToString().c_str());
  }
  return s;
}

Status DBImpl::DropColumnFamily(ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  if (cfd->GetID() == 0) {
    return Status::InvalidArgument("Can't drop default column family");
  }

  VersionEdit edit;
  edit.DropColumnFamily();
  edit.SetColumnFamily(cfd->GetID());

  Status s;
  {
    MutexLock l(&mutex_);
    if (cfd->IsDropped()) {
      s = Status::InvalidArgument("Column family already dropped!\n");
    }
    if (s.ok()) {
      s = versions_->LogAndApply(cfd, &edit, &mutex_);
    }
  }

  if (s.ok()) {
    assert(cfd->IsDropped());
    max_total_in_memory_state_ -= cfd->options()->write_buffer_size *
                                  cfd->options()->max_write_buffer_number;
    Log(options_.info_log, "Dropped column family with id %u\n", cfd->GetID());
  } else {
    Log(options_.info_log, "Dropping column family with id %u FAILED -- %s\n",
        cfd->GetID(), s.ToString().c_str());
  }

  return s;
}

bool DBImpl::KeyMayExist(const ReadOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         std::string* value, bool* value_found) {
  if (value_found != nullptr) {
    // falsify later if key-may-exist but can't fetch value
    *value_found = true;
  }
  ReadOptions roptions = options;
  roptions.read_tier = kBlockCacheTier; // read from block cache only
  auto s = GetImpl(roptions, column_family, key, value, value_found);

  // If block_cache is enabled and the index block of the table didn't
  // not present in block_cache, the return value will be Status::Incomplete.
  // In this case, key may still exist in the table.
  return s.ok() || s.IsIncomplete();
}

Iterator* DBImpl::NewIterator(const ReadOptions& options,
                              ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  if (options.tailing) {
#ifdef ROCKSDB_LITE
    // not supported in lite version
    return nullptr;
#else
    // TODO(ljin): remove tailing iterator
    auto iter = new ForwardIterator(this, options, cfd);
    return NewDBIterator(env_, *cfd->options(), cfd->user_comparator(), iter,
                         kMaxSequenceNumber);
// return new TailingIterator(env_, this, options, cfd);
#endif
  } else {
    SequenceNumber latest_snapshot = versions_->LastSequence();
    SuperVersion* sv = nullptr;
    sv = cfd->GetReferencedSuperVersion(&mutex_);

    auto snapshot =
        options.snapshot != nullptr
            ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
            : latest_snapshot;

    // Try to generate a DB iterator tree in continuous memory area to be
    // cache friendly. Here is an example of result:
    // +-------------------------------+
    // |                               |
    // | ArenaWrappedDBIter            |
    // |  +                            |
    // |  +---> Inner Iterator   ------------+
    // |  |                            |     |
    // |  |    +-- -- -- -- -- -- -- --+     |
    // |  +--- | Arena                 |     |
    // |       |                       |     |
    // |          Allocated Memory:    |     |
    // |       |   +-------------------+     |
    // |       |   | DBIter            | <---+
    // |           |  +                |
    // |       |   |  +-> iter_  ------------+
    // |       |   |                   |     |
    // |       |   +-------------------+     |
    // |       |   | MergingIterator   | <---+
    // |           |  +                |
    // |       |   |  +->child iter1  ------------+
    // |       |   |  |                |          |
    // |           |  +->child iter2  ----------+ |
    // |       |   |  |                |        | |
    // |       |   |  +->child iter3  --------+ | |
    // |           |                   |      | | |
    // |       |   +-------------------+      | | |
    // |       |   | Iterator1         | <--------+
    // |       |   +-------------------+      | |
    // |       |   | Iterator2         | <------+
    // |       |   +-------------------+      |
    // |       |   | Iterator3         | <----+
    // |       |   +-------------------+
    // |       |                       |
    // +-------+-----------------------+
    //
    // ArenaWrappedDBIter inlines an arena area where all the iterartor in the
    // the iterator tree is allocated in the order of being accessed when
    // querying.
    // Laying out the iterators in the order of being accessed makes it more
    // likely that any iterator pointer is close to the iterator it points to so
    // that they are likely to be in the same cache line and/or page.
    ArenaWrappedDBIter* db_iter = NewArenaWrappedDbIterator(
        env_, *cfd->options(), cfd->user_comparator(), snapshot);
    Iterator* internal_iter =
        NewInternalIterator(options, cfd, sv, db_iter->GetArena());
    db_iter->SetIterUnderDBIter(internal_iter);

    return db_iter;
  }
}

Status DBImpl::NewIterators(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  iterators->clear();
  iterators->reserve(column_families.size());
  SequenceNumber latest_snapshot = 0;
  std::vector<SuperVersion*> super_versions;
  super_versions.reserve(column_families.size());

  if (!options.tailing) {
    mutex_.Lock();
    latest_snapshot = versions_->LastSequence();
    for (auto cfh : column_families) {
      auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh)->cfd();
      super_versions.push_back(cfd->GetSuperVersion()->Ref());
    }
    mutex_.Unlock();
  }

  if (options.tailing) {
#ifdef ROCKSDB_LITE
    return Status::InvalidArgument(
        "Tailing interator not supported in RocksDB lite");
#else
    for (auto cfh : column_families) {
      auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh)->cfd();
      auto iter = new ForwardIterator(this, options, cfd);
      iterators->push_back(
          NewDBIterator(env_, *cfd->options(), cfd->user_comparator(), iter,
                        kMaxSequenceNumber));
    }
#endif
  } else {
    for (size_t i = 0; i < column_families.size(); ++i) {
      auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_families[i]);
      auto cfd = cfh->cfd();

      auto snapshot =
          options.snapshot != nullptr
              ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
              : latest_snapshot;

      auto iter = NewInternalIterator(options, cfd, super_versions[i]);
      iter = NewDBIterator(env_, *cfd->options(),
                           cfd->user_comparator(), iter, snapshot);
      iterators->push_back(iter);
    }
  }

  return Status::OK();
}

bool DBImpl::IsSnapshotSupported() const {
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (!cfd->mem()->IsSnapshotSupported()) {
      return false;
    }
  }
  return true;
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  // returns null if the underlying memtable does not support snapshot.
  if (!IsSnapshotSupported()) return nullptr;
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, ColumnFamilyHandle* column_family,
                   const Slice& key, const Slice& val) {
  return DB::Put(o, column_family, key, val);
}

Status DBImpl::Merge(const WriteOptions& o, ColumnFamilyHandle* column_family,
                     const Slice& key, const Slice& val) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  if (!cfh->cfd()->options()->merge_operator) {
    return Status::NotSupported("Provide a merge_operator when opening DB");
  } else {
    return DB::Merge(o, column_family, key, val);
  }
}

Status DBImpl::Delete(const WriteOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key) {
  return DB::Delete(options, column_family, key);
}

// REQUIRES: mutex_ is held
Status DBImpl::BeginWrite(Writer* w, uint64_t expiration_time) {
  // the following code block pushes the current writer "w" into the writer
  // queue "writers_" and wait until one of the following conditions met:
  // 1. the job of "w" has been done by some other writers.
  // 2. "w" becomes the first writer in "writers_"
  // 3. "w" timed-out.
  mutex_.AssertHeld();
  writers_.push_back(w);

  bool timed_out = false;
  while (!w->done && w != writers_.front()) {
    if (expiration_time == 0) {
      w->cv.Wait();
    } else if (w->cv.TimedWait(expiration_time)) {
      if (w->in_batch_group) {
        // then it means the front writer is currently doing the
        // write on behalf of this "timed-out" writer.  Then it
        // should wait until the write completes.
        expiration_time = 0;
      } else {
        timed_out = true;
        break;
      }
    }
  }

  if (timed_out) {
#ifndef NDEBUG
    bool found = false;
#endif
    for (auto iter = writers_.begin(); iter != writers_.end(); iter++) {
      if (*iter == w) {
        writers_.erase(iter);
#ifndef NDEBUG
        found = true;
#endif
        break;
      }
    }
#ifndef NDEBUG
    assert(found);
#endif
    // writers_.front() might still be in cond_wait without a time-out.
    // As a result, we need to signal it to wake it up.  Otherwise no
    // one else will wake him up, and RocksDB will hang.
    if (!writers_.empty()) {
      writers_.front()->cv.Signal();
    }
    return Status::TimedOut();
  }
  return Status::OK();
}

// REQUIRES: mutex_ is held
void DBImpl::EndWrite(Writer* w, Writer* last_writer, Status status) {
  // Pop out the current writer and all writers being pushed before the
  // current writer from the writer queue.
  mutex_.AssertHeld();
  while (!writers_.empty()) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  if (my_batch == nullptr) {
    return Status::Corruption("Batch is nullptr!");
  }
  PERF_TIMER_GUARD(write_pre_and_post_process_time);
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.disableWAL = options.disableWAL;
  w.in_batch_group = false;
  w.done = false;
  w.timeout_hint_us = options.timeout_hint_us;

  uint64_t expiration_time = 0;
  if (w.timeout_hint_us == 0) {
    w.timeout_hint_us = kNoTimeOut;
  } else {
    expiration_time = env_->NowMicros() + w.timeout_hint_us;
  }

  if (!options.disableWAL) {
    RecordTick(stats_, WRITE_WITH_WAL);
    default_cf_internal_stats_->AddDBStats(InternalStats::WRITE_WITH_WAL, 1);
  }

  WriteContext context;
  mutex_.Lock();
  Status status = BeginWrite(&w, expiration_time);
  assert(status.ok() || status.IsTimedOut());
  if (status.IsTimedOut()) {
    mutex_.Unlock();
    RecordTick(stats_, WRITE_TIMEDOUT);
    return Status::TimedOut();
  }
  if (w.done) {  // write was done by someone else
    default_cf_internal_stats_->AddDBStats(InternalStats::WRITE_DONE_BY_OTHER,
                                           1);
    mutex_.Unlock();
    RecordTick(stats_, WRITE_DONE_BY_OTHER);
    return w.status;
  }

  RecordTick(stats_, WRITE_DONE_BY_SELF);
  default_cf_internal_stats_->AddDBStats(InternalStats::WRITE_DONE_BY_SELF, 1);

  // Once reaches this point, the current writer "w" will try to do its write
  // job.  It may also pick up some of the remaining writers in the "writers_"
  // when it finds suitable, and finish them in the same write batch.
  // This is how a write job could be done by the other writer.
  assert(!single_column_family_mode_ ||
         versions_->GetColumnFamilySet()->NumberOfColumnFamilies() == 1);

  uint64_t flush_column_family_if_log_file = 0;
  uint64_t max_total_wal_size = (options_.max_total_wal_size == 0)
                                    ? 4 * max_total_in_memory_state_
                                    : options_.max_total_wal_size;
  if (UNLIKELY(!single_column_family_mode_) &&
      alive_log_files_.begin()->getting_flushed == false &&
      total_log_size_ > max_total_wal_size) {
    flush_column_family_if_log_file = alive_log_files_.begin()->number;
    alive_log_files_.begin()->getting_flushed = true;
    Log(options_.info_log,
        "Flushing all column families with data in WAL number %" PRIu64
        ". Total log size is %" PRIu64 " while max_total_wal_size is %" PRIu64,
        flush_column_family_if_log_file, total_log_size_, max_total_wal_size);
  }

  if (LIKELY(single_column_family_mode_)) {
    // fast path
    status = MakeRoomForWrite(default_cf_handle_->cfd(),
                              &context, expiration_time);
  } else {
    // refcounting cfd in iteration
    bool dead_cfd = false;
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      cfd->Ref();
      if (flush_column_family_if_log_file != 0 &&
          cfd->GetLogNumber() <= flush_column_family_if_log_file) {
        // log size excedded limit and we need to do flush
        // SetNewMemtableAndNewLogFie may temporarily unlock and wait
        status = SetNewMemtableAndNewLogFile(cfd, &context);
        cfd->imm()->FlushRequested();
        MaybeScheduleFlushOrCompaction();
      } else {
        // May temporarily unlock and wait.
        status = MakeRoomForWrite(cfd, &context, expiration_time);
      }

      if (cfd->Unref()) {
        dead_cfd = true;
      }
      if (!status.ok()) {
        break;
      }
    }
    if (dead_cfd) {
      versions_->GetColumnFamilySet()->FreeDeadColumnFamilies();
    }
  }

  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok()) {
    autovector<WriteBatch*> write_batch_group;
    BuildBatchGroup(&last_writer, &write_batch_group);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into memtables
    {
      mutex_.Unlock();
      WriteBatch* updates = nullptr;
      if (write_batch_group.size() == 1) {
        updates = write_batch_group[0];
      } else {
        updates = &tmp_batch_;
        for (size_t i = 0; i < write_batch_group.size(); ++i) {
          WriteBatchInternal::Append(updates, write_batch_group[i]);
        }
      }

      const SequenceNumber current_sequence = last_sequence + 1;
      WriteBatchInternal::SetSequence(updates, current_sequence);
      int my_batch_count = WriteBatchInternal::Count(updates);
      last_sequence += my_batch_count;
      const uint64_t batch_size = WriteBatchInternal::ByteSize(updates);
      // Record statistics
      RecordTick(stats_, NUMBER_KEYS_WRITTEN, my_batch_count);
      RecordTick(stats_, BYTES_WRITTEN, WriteBatchInternal::ByteSize(updates));
      if (options.disableWAL) {
        flush_on_destroy_ = true;
      }
      PERF_TIMER_STOP(write_pre_and_post_process_time);

      uint64_t log_size = 0;
      if (!options.disableWAL) {
        PERF_TIMER_GUARD(write_wal_time);
        Slice log_entry = WriteBatchInternal::Contents(updates);
        status = log_->AddRecord(log_entry);
        total_log_size_ += log_entry.size();
        alive_log_files_.back().AddSize(log_entry.size());
        log_empty_ = false;
        log_size = log_entry.size();
        RecordTick(stats_, WAL_FILE_SYNCED);
        RecordTick(stats_, WAL_FILE_BYTES, log_size);
        if (status.ok() && options.sync) {
          if (options_.use_fsync) {
            StopWatch(env_, stats_, WAL_FILE_SYNC_MICROS);
            status = log_->file()->Fsync();
          } else {
            StopWatch(env_, stats_, WAL_FILE_SYNC_MICROS);
            status = log_->file()->Sync();
          }
        }
      }
      if (status.ok()) {
        PERF_TIMER_GUARD(write_memtable_time);

        status = WriteBatchInternal::InsertInto(
            updates, column_family_memtables_.get(),
            options.ignore_missing_column_families, 0, this, false);
        // A non-OK status here indicates iteration failure (either in-memory
        // writebatch corruption (very bad), or the client specified invalid
        // column family).  This will later on trigger bg_error_.
        //
        // Note that existing logic was not sound. Any partial failure writing
        // into the memtable would result in a state that some write ops might
        // have succeeded in memtable but Status reports error for all writes.

        SetTickerCount(stats_, SEQUENCE_NUMBER, last_sequence);
      }
      PERF_TIMER_START(write_pre_and_post_process_time);
      if (updates == &tmp_batch_) {
        tmp_batch_.Clear();
      }
      mutex_.Lock();
      // internal stats
      default_cf_internal_stats_->AddDBStats(
          InternalStats::BYTES_WRITTEN, batch_size);
      if (!options.disableWAL) {
        default_cf_internal_stats_->AddDBStats(
            InternalStats::WAL_FILE_SYNCED, 1);
        default_cf_internal_stats_->AddDBStats(
            InternalStats::WAL_FILE_BYTES, log_size);
      }
      if (status.ok()) {
        versions_->SetLastSequence(last_sequence);
      }
    }
  }
  if (options_.paranoid_checks && !status.ok() &&
      !status.IsTimedOut() && bg_error_.ok()) {
    bg_error_ = status; // stop compaction & fail any further writes
  }

  EndWrite(&w, last_writer, status);
  mutex_.Unlock();

  if (status.IsTimedOut()) {
    RecordTick(stats_, WRITE_TIMEDOUT);
  }

  return status;
}

// This function will be called only when the first writer succeeds.
// All writers in the to-be-built batch group will be processed.
//
// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-nullptr batch
void DBImpl::BuildBatchGroup(Writer** last_writer,
                             autovector<WriteBatch*>* write_batch_group) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  assert(first->batch != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);
  write_batch_group->push_back(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (!w->disableWAL && first->disableWAL) {
      // Do not include a write that needs WAL into a batch that has
      // WAL disabled.
      break;
    }

    if (w->timeout_hint_us < first->timeout_hint_us) {
      // Do not include those writes with shorter timeout.  Otherwise, we might
      // execute a write that should instead be aborted because of timeout.
      break;
    }

    if (w->batch == nullptr) {
      // Do not include those writes with nullptr batch. Those are not writes,
      // those are something else. They want to be alone
      break;
    }

    size += WriteBatchInternal::ByteSize(w->batch);
    if (size > max_size) {
      // Do not make batch too big
      break;
    }

    write_batch_group->push_back(w->batch);
    w->in_batch_group = true;
    *last_writer = w;
  }
}

// This function computes the amount of time in microseconds by which a write
// should be delayed based on the number of level-0 files according to the
// following formula:
// if n < bottom, return 0;
// if n >= top, return 1000;
// otherwise, let r = (n - bottom) /
//                    (top - bottom)
//  and return r^2 * 1000.
// The goal of this formula is to gradually increase the rate at which writes
// are slowed. We also tried linear delay (r * 1000), but it seemed to do
// slightly worse. There is no other particular reason for choosing quadratic.
uint64_t DBImpl::SlowdownAmount(int n, double bottom, double top) {
  uint64_t delay;
  if (n >= top) {
    delay = 1000;
  }
  else if (n < bottom) {
    delay = 0;
  }
  else {
    // If we are here, we know that:
    //   level0_start_slowdown <= n < level0_slowdown
    // since the previous two conditions are false.
    double how_much =
      (double) (n - bottom) /
              (top - bottom);
    delay = std::max(how_much * how_much * 1000, 100.0);
  }
  assert(delay <= 1000);
  return delay;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(ColumnFamilyData* cfd,
                                WriteContext* context,
                                uint64_t expiration_time) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = true;
  bool allow_hard_rate_limit_delay = true;
  bool allow_soft_rate_limit_delay = true;
  uint64_t rate_limit_delay_millis = 0;
  Status s;
  double score;
  // Once we schedule background work, we shouldn't schedule it again, since it
  // might generate a tight feedback loop, constantly scheduling more background
  // work, even if additional background work is not needed
  bool schedule_background_work = true;
  bool has_timeout = (expiration_time > 0);

  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (has_timeout && env_->NowMicros() > expiration_time) {
      s = Status::TimedOut();
      break;
    } else if (allow_delay && cfd->NeedSlowdownForNumLevel0Files()) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 0-1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      uint64_t slowdown =
          SlowdownAmount(cfd->current()->NumLevelFiles(0),
                         cfd->options()->level0_slowdown_writes_trigger,
                         cfd->options()->level0_stop_writes_trigger);
      mutex_.Unlock();
      uint64_t delayed;
      {
        StopWatch sw(env_, stats_, STALL_L0_SLOWDOWN_COUNT, &delayed);
        env_->SleepForMicroseconds(slowdown);
      }
      RecordTick(stats_, STALL_L0_SLOWDOWN_MICROS, delayed);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
      cfd->internal_stats()->AddCFStats(
          InternalStats::LEVEL0_SLOWDOWN, delayed);
      delayed_writes_++;
    } else if (!cfd->mem()->ShouldFlush()) {
      // There is room in current memtable
      if (allow_delay) {
        DelayLoggingAndReset();
      }
      break;
    } else if (cfd->NeedWaitForNumMemtables()) {
      // We have filled up the current memtable, but the previous
      // ones are still being flushed, so we wait.
      DelayLoggingAndReset();
      Log(options_.info_log, "[%s] wait for memtable flush...\n",
          cfd->GetName().c_str());
      if (schedule_background_work) {
        MaybeScheduleFlushOrCompaction();
        schedule_background_work = false;
      }
      uint64_t stall;
      {
        StopWatch sw(env_, stats_, STALL_MEMTABLE_COMPACTION_COUNT, &stall);
        if (!has_timeout) {
          bg_cv_.Wait();
        } else {
          bg_cv_.TimedWait(expiration_time);
        }
      }
      RecordTick(stats_, STALL_MEMTABLE_COMPACTION_MICROS, stall);
      cfd->internal_stats()->AddCFStats(
          InternalStats::MEMTABLE_COMPACTION, stall);
    } else if (cfd->NeedWaitForNumLevel0Files()) {
      DelayLoggingAndReset();
      Log(options_.info_log, "[%s] wait for fewer level0 files...\n",
          cfd->GetName().c_str());
      uint64_t stall;
      {
        StopWatch sw(env_, stats_, STALL_L0_NUM_FILES_COUNT, &stall);
        if (!has_timeout) {
          bg_cv_.Wait();
        } else {
          bg_cv_.TimedWait(expiration_time);
        }
      }
      RecordTick(stats_, STALL_L0_NUM_FILES_MICROS, stall);
      cfd->internal_stats()->AddCFStats(
          InternalStats::LEVEL0_NUM_FILES, stall);
    } else if (allow_hard_rate_limit_delay && cfd->ExceedsHardRateLimit()) {
      // Delay a write when the compaction score for any level is too large.
      const int max_level = cfd->current()->MaxCompactionScoreLevel();
      score = cfd->current()->MaxCompactionScore();
      mutex_.Unlock();
      uint64_t delayed;
      {
        StopWatch sw(env_, stats_, HARD_RATE_LIMIT_DELAY_COUNT, &delayed);
        env_->SleepForMicroseconds(1000);
      }
      // Make sure the following value doesn't round to zero.
      uint64_t rate_limit = std::max((delayed / 1000), (uint64_t) 1);
      rate_limit_delay_millis += rate_limit;
      RecordTick(stats_, RATE_LIMIT_DELAY_MILLIS, rate_limit);
      if (cfd->options()->rate_limit_delay_max_milliseconds > 0 &&
          rate_limit_delay_millis >=
              (unsigned)cfd->options()->rate_limit_delay_max_milliseconds) {
        allow_hard_rate_limit_delay = false;
      }
      mutex_.Lock();
      cfd->internal_stats()->RecordLevelNSlowdown(max_level, delayed, false);
    } else if (allow_soft_rate_limit_delay && cfd->ExceedsSoftRateLimit()) {
      const int max_level = cfd->current()->MaxCompactionScoreLevel();
      score = cfd->current()->MaxCompactionScore();
      // Delay a write when the compaction score for any level is too large.
      // TODO: add statistics
      uint64_t slowdown = SlowdownAmount(score, cfd->options()->soft_rate_limit,
                                         cfd->options()->hard_rate_limit);
      uint64_t elapsed = 0;
      mutex_.Unlock();
      {
        StopWatch sw(env_, stats_, SOFT_RATE_LIMIT_DELAY_COUNT, &elapsed);
        env_->SleepForMicroseconds(slowdown);
        rate_limit_delay_millis += slowdown;
      }
      allow_soft_rate_limit_delay = false;
      mutex_.Lock();
      cfd->internal_stats()->RecordLevelNSlowdown(max_level, elapsed, true);
    } else {
      s = SetNewMemtableAndNewLogFile(cfd, context);
      if (!s.ok()) {
        break;
      }
      MaybeScheduleFlushOrCompaction();
    }
  }
  return s;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::SetNewMemtableAndNewLogFile(ColumnFamilyData* cfd,
                                           WriteContext* context) {
  mutex_.AssertHeld();
  unique_ptr<WritableFile> lfile;
  log::Writer* new_log = nullptr;
  MemTable* new_mem = nullptr;

  // Attempt to switch to a new memtable and trigger flush of old.
  // Do this without holding the dbmutex lock.
  assert(versions_->PrevLogNumber() == 0);
  bool creating_new_log = !log_empty_;
  uint64_t new_log_number =
      creating_new_log ? versions_->NewFileNumber() : logfile_number_;
  SuperVersion* new_superversion = nullptr;
  mutex_.Unlock();
  Status s;
  {
    DelayLoggingAndReset();
    if (creating_new_log) {
      s = env_->NewWritableFile(LogFileName(options_.wal_dir, new_log_number),
                                &lfile,
                                env_->OptimizeForLogWrite(storage_options_));
      if (s.ok()) {
        // Our final size should be less than write_buffer_size
        // (compression, etc) but err on the side of caution.
        lfile->SetPreallocationBlockSize(1.1 *
                                         cfd->options()->write_buffer_size);
        new_log = new log::Writer(std::move(lfile));
      }
    }

    if (s.ok()) {
      new_mem = new MemTable(cfd->internal_comparator(), *cfd->options());
      new_superversion = new SuperVersion();
    }
  }
  mutex_.Lock();
  if (!s.ok()) {
    // how do we fail if we're not creating new log?
    assert(creating_new_log);
    // Avoid chewing through file number space in a tight loop.
    versions_->ReuseLogFileNumber(new_log_number);
    assert(!new_mem);
    assert(!new_log);
    return s;
  }
  if (creating_new_log) {
    logfile_number_ = new_log_number;
    assert(new_log != nullptr);
    context->logs_to_free_.push_back(log_.release());
    log_.reset(new_log);
    log_empty_ = true;
    alive_log_files_.push_back(LogFileNumberSize(logfile_number_));
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      // all this is just optimization to delete logs that
      // are no longer needed -- if CF is empty, that means it
      // doesn't need that particular log to stay alive, so we just
      // advance the log number. no need to persist this in the manifest
      if (cfd->mem()->GetFirstSequenceNumber() == 0 &&
          cfd->imm()->size() == 0) {
        cfd->SetLogNumber(logfile_number_);
      }
    }
  }
  cfd->mem()->SetNextLogNumber(logfile_number_);
  cfd->imm()->Add(cfd->mem());
  new_mem->Ref();
  cfd->SetMemtable(new_mem);
  Log(options_.info_log,
      "[%s] New memtable created with log file: #%" PRIu64 "\n",
      cfd->GetName().c_str(), logfile_number_);
  context->superversions_to_free_.push_back(
      cfd->InstallSuperVersion(new_superversion, &mutex_));
  return s;
}

#ifndef ROCKSDB_LITE
Status DBImpl::GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                        TablePropertiesCollection* props) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  // Increment the ref count
  mutex_.Lock();
  auto version = cfd->current();
  version->Ref();
  mutex_.Unlock();

  auto s = version->GetPropertiesOfAllTables(props);

  // Decrement the ref count
  mutex_.Lock();
  version->Unref();
  mutex_.Unlock();

  return s;
}
#endif  // ROCKSDB_LITE

const std::string& DBImpl::GetName() const {
  return dbname_;
}

Env* DBImpl::GetEnv() const {
  return env_;
}

const Options& DBImpl::GetOptions(ColumnFamilyHandle* column_family) const {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  return *cfh->cfd()->options();
}

bool DBImpl::GetProperty(ColumnFamilyHandle* column_family,
                         const Slice& property, std::string* value) {
  bool is_int_property = false;
  bool need_out_of_mutex = false;
  DBPropertyType property_type =
      GetPropertyType(property, &is_int_property, &need_out_of_mutex);

  value->clear();
  if (is_int_property) {
    uint64_t int_value;
    bool ret_value = GetIntPropertyInternal(column_family, property_type,
                                            need_out_of_mutex, &int_value);
    if (ret_value) {
      *value = std::to_string(int_value);
    }
    return ret_value;
  } else {
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
    auto cfd = cfh->cfd();
    MutexLock l(&mutex_);
    return cfd->internal_stats()->GetStringProperty(property_type, property,
                                                    value);
  }
}

bool DBImpl::GetIntProperty(ColumnFamilyHandle* column_family,
                            const Slice& property, uint64_t* value) {
  bool is_int_property = false;
  bool need_out_of_mutex = false;
  DBPropertyType property_type =
      GetPropertyType(property, &is_int_property, &need_out_of_mutex);
  if (!is_int_property) {
    return false;
  }
  return GetIntPropertyInternal(column_family, property_type, need_out_of_mutex,
                                value);
}

bool DBImpl::GetIntPropertyInternal(ColumnFamilyHandle* column_family,
                                    DBPropertyType property_type,
                                    bool need_out_of_mutex, uint64_t* value) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  if (!need_out_of_mutex) {
    MutexLock l(&mutex_);
    return cfd->internal_stats()->GetIntProperty(property_type, value, this);
  } else {
    SuperVersion* sv = GetAndRefSuperVersion(cfd);

    bool ret = cfd->internal_stats()->GetIntPropertyOutOfMutex(
        property_type, sv->current, value);

    ReturnAndCleanupSuperVersion(cfd, sv);

    return ret;
  }
}

SuperVersion* DBImpl::GetAndRefSuperVersion(ColumnFamilyData* cfd) {
  // TODO(ljin): consider using GetReferencedSuperVersion() directly
  if (LIKELY(options_.allow_thread_local)) {
    return cfd->GetThreadLocalSuperVersion(&mutex_);
  } else {
    MutexLock l(&mutex_);
    return cfd->GetSuperVersion()->Ref();
  }
}

void DBImpl::ReturnAndCleanupSuperVersion(ColumnFamilyData* cfd,
                                          SuperVersion* sv) {
  bool unref_sv = true;
  if (LIKELY(options_.allow_thread_local)) {
    unref_sv = !cfd->ReturnThreadLocalSuperVersion(sv);
  }

  if (unref_sv) {
    // Release SuperVersion
    if (sv->Unref()) {
      {
        MutexLock l(&mutex_);
        sv->Cleanup();
      }
      delete sv;
      RecordTick(stats_, NUMBER_SUPERVERSION_CLEANUPS);
    }
    RecordTick(stats_, NUMBER_SUPERVERSION_RELEASES);
  }
}

void DBImpl::GetApproximateSizes(ColumnFamilyHandle* column_family,
                                 const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  {
    MutexLock l(&mutex_);
    v = cfd->current();
    v->Ref();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

inline void DBImpl::DelayLoggingAndReset() {
  if (delayed_writes_ > 0) {
    Log(options_.info_log, "delayed %d write...\n", delayed_writes_ );
    delayed_writes_ = 0;
  }
}

#ifndef ROCKSDB_LITE
Status DBImpl::GetUpdatesSince(
    SequenceNumber seq, unique_ptr<TransactionLogIterator>* iter,
    const TransactionLogIterator::ReadOptions& read_options) {

  RecordTick(stats_, GET_UPDATES_SINCE_CALLS);
  if (seq > versions_->LastSequence()) {
    return Status::NotFound("Requested sequence not yet written in the db");
  }
  //  Get all sorted Wal Files.
  //  Do binary search and open files and find the seq number.

  std::unique_ptr<VectorLogPtr> wal_files(new VectorLogPtr);
  Status s = GetSortedWalFiles(*wal_files);
  if (!s.ok()) {
    return s;
  }

  s = RetainProbableWalFiles(*wal_files, seq);
  if (!s.ok()) {
    return s;
  }
  iter->reset(new TransactionLogIteratorImpl(options_.wal_dir, &options_,
                                             read_options, storage_options_,
                                             seq, std::move(wal_files), this));
  return (*iter)->status();
}

Status DBImpl::DeleteFile(std::string name) {
  uint64_t number;
  FileType type;
  WalFileType log_type;
  if (!ParseFileName(name, &number, &type, &log_type) ||
      (type != kTableFile && type != kLogFile)) {
    Log(options_.info_log, "DeleteFile %s failed.\n", name.c_str());
    return Status::InvalidArgument("Invalid file name");
  }

  Status status;
  if (type == kLogFile) {
    // Only allow deleting archived log files
    if (log_type != kArchivedLogFile) {
      Log(options_.info_log, "DeleteFile %s failed - not archived log.\n",
          name.c_str());
      return Status::NotSupported("Delete only supported for archived logs");
    }
    status = env_->DeleteFile(options_.wal_dir + "/" + name.c_str());
    if (!status.ok()) {
      Log(options_.info_log, "DeleteFile %s failed -- %s.\n",
          name.c_str(), status.ToString().c_str());
    }
    return status;
  }

  int level;
  FileMetaData* metadata;
  ColumnFamilyData* cfd;
  VersionEdit edit;
  DeletionState deletion_state(true);
  {
    MutexLock l(&mutex_);
    status = versions_->GetMetadataForFile(number, &level, &metadata, &cfd);
    if (!status.ok()) {
      Log(options_.info_log, "DeleteFile %s failed. File not found\n",
                             name.c_str());
      return Status::InvalidArgument("File not found");
    }
    assert((level > 0) && (level < cfd->NumberLevels()));

    // If the file is being compacted no need to delete.
    if (metadata->being_compacted) {
      Log(options_.info_log,
          "DeleteFile %s Skipped. File about to be compacted\n", name.c_str());
      return Status::OK();
    }

    // Only the files in the last level can be deleted externally.
    // This is to make sure that any deletion tombstones are not
    // lost. Check that the level passed is the last level.
    for (int i = level + 1; i < cfd->NumberLevels(); i++) {
      if (cfd->current()->NumLevelFiles(i) != 0) {
        Log(options_.info_log,
            "DeleteFile %s FAILED. File not in last level\n", name.c_str());
        return Status::InvalidArgument("File not in last level");
      }
    }
    edit.DeleteFile(level, number);
    status = versions_->LogAndApply(cfd, &edit, &mutex_, db_directory_.get());
    if (status.ok()) {
      InstallSuperVersion(cfd, deletion_state);
    }
    FindObsoleteFiles(deletion_state, false);
  } // lock released here
  LogFlush(options_.info_log);
  // remove files outside the db-lock
  if (deletion_state.HaveSomethingToDelete()) {
    PurgeObsoleteFiles(deletion_state);
  }
  {
    MutexLock l(&mutex_);
    // schedule flush if file deletion means we freed the space for flushes to
    // continue
    MaybeScheduleFlushOrCompaction();
  }
  return status;
}

void DBImpl::GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) {
  MutexLock l(&mutex_);
  versions_->GetLiveFilesMetaData(metadata);
}
#endif  // ROCKSDB_LITE

Status DBImpl::CheckConsistency() {
  mutex_.AssertHeld();
  std::vector<LiveFileMetaData> metadata;
  versions_->GetLiveFilesMetaData(&metadata);

  std::string corruption_messages;
  for (const auto& md : metadata) {
    std::string file_path = md.db_path + "/" + md.name;

    uint64_t fsize = 0;
    Status s = env_->GetFileSize(file_path, &fsize);
    if (!s.ok()) {
      corruption_messages +=
          "Can't access " + md.name + ": " + s.ToString() + "\n";
    } else if (fsize != md.size) {
      corruption_messages += "Sst file size mismatch: " + file_path +
                             ". Size recorded in manifest " +
                             std::to_string(md.size) + ", actual size " +
                             std::to_string(fsize) + "\n";
    }
  }
  if (corruption_messages.size() == 0) {
    return Status::OK();
  } else {
    return Status::Corruption(corruption_messages);
  }
}

Status DBImpl::GetDbIdentity(std::string& identity) {
  std::string idfilename = IdentityFileName(dbname_);
  unique_ptr<SequentialFile> idfile;
  const EnvOptions soptions;
  Status s = env_->NewSequentialFile(idfilename, &idfile, soptions);
  if (!s.ok()) {
    return s;
  }
  uint64_t file_size;
  s = env_->GetFileSize(idfilename, &file_size);
  if (!s.ok()) {
    return s;
  }
  char buffer[file_size];
  Slice id;
  s = idfile->Read(file_size, &id, buffer);
  if (!s.ok()) {
    return s;
  }
  identity.assign(id.ToString());
  // If last character is '\n' remove it from identity
  if (identity.size() > 0 && identity.back() == '\n') {
    identity.pop_back();
  }
  return s;
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, ColumnFamilyHandle* column_family,
               const Slice& key, const Slice& value) {
  // Pre-allocate size of write batch conservatively.
  // 8 bytes are taken by header, 4 bytes for count, 1 byte for type,
  // and we allocate 11 extra bytes for key length, as well as value length.
  WriteBatch batch(key.size() + value.size() + 24);
  batch.Put(column_family, key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, ColumnFamilyHandle* column_family,
                  const Slice& key) {
  WriteBatch batch;
  batch.Delete(column_family, key);
  return Write(opt, &batch);
}

Status DB::Merge(const WriteOptions& opt, ColumnFamilyHandle* column_family,
                 const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Merge(column_family, key, value);
  return Write(opt, &batch);
}

// Default implementation -- returns not supported status
Status DB::CreateColumnFamily(const ColumnFamilyOptions& options,
                              const std::string& column_family_name,
                              ColumnFamilyHandle** handle) {
  return Status::NotSupported("");
}
Status DB::DropColumnFamily(ColumnFamilyHandle* column_family) {
  return Status::NotSupported("");
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DB::Open(db_options, dbname, column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }
  return s;
}

Status DB::Open(const DBOptions& db_options, const std::string& dbname,
                const std::vector<ColumnFamilyDescriptor>& column_families,
                std::vector<ColumnFamilyHandle*>* handles, DB** dbptr) {
  Status s = SanitizeDBOptionsByCFOptions(&db_options, column_families);
  if (!s.ok()) {
    return s;
  }
  if (db_options.db_paths.size() > 1) {
    for (auto& cfd : column_families) {
      if (cfd.options.compaction_style != kCompactionStyleUniversal) {
        return Status::NotSupported(
            "More than one DB paths are only supported in "
            "universal compaction style. ");
      }
    }

    if (db_options.db_paths.size() > 4) {
      return Status::NotSupported(
        "More than four DB paths are not supported yet. ");
    }
  }

  *dbptr = nullptr;
  handles->clear();

  size_t max_write_buffer_size = 0;
  for (auto cf : column_families) {
    max_write_buffer_size =
        std::max(max_write_buffer_size, cf.options.write_buffer_size);
  }

  DBImpl* impl = new DBImpl(db_options, dbname);
  s = impl->env_->CreateDirIfMissing(impl->options_.wal_dir);
  if (s.ok()) {
    for (auto db_path : impl->options_.db_paths) {
      s = impl->env_->CreateDirIfMissing(db_path.path);
      if (!s.ok()) {
        break;
      }
    }
  }

  if (!s.ok()) {
    delete impl;
    return s;
  }

  s = impl->CreateArchivalDirectory();
  if (!s.ok()) {
    delete impl;
    return s;
  }
  impl->mutex_.Lock();
  // Handles create_if_missing, error_if_exists
  s = impl->Recover(column_families);
  if (s.ok()) {
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    unique_ptr<WritableFile> lfile;
    EnvOptions soptions(db_options);
    s = impl->options_.env->NewWritableFile(
        LogFileName(impl->options_.wal_dir, new_log_number), &lfile,
        impl->options_.env->OptimizeForLogWrite(soptions));
    if (s.ok()) {
      lfile->SetPreallocationBlockSize(1.1 * max_write_buffer_size);
      impl->logfile_number_ = new_log_number;
      impl->log_.reset(new log::Writer(std::move(lfile)));

      // set column family handles
      for (auto cf : column_families) {
        auto cfd =
            impl->versions_->GetColumnFamilySet()->GetColumnFamily(cf.name);
        if (cfd != nullptr) {
          handles->push_back(
              new ColumnFamilyHandleImpl(cfd, impl, &impl->mutex_));
        } else {
          if (db_options.create_missing_column_families) {
            // missing column family, create it
            ColumnFamilyHandle* handle;
            impl->mutex_.Unlock();
            s = impl->CreateColumnFamily(cf.options, cf.name, &handle);
            impl->mutex_.Lock();
            if (s.ok()) {
              handles->push_back(handle);
            } else {
              break;
            }
          } else {
            s = Status::InvalidArgument("Column family not found: ", cf.name);
            break;
          }
        }
      }
    }
    if (s.ok()) {
      for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
        delete cfd->InstallSuperVersion(new SuperVersion(), &impl->mutex_);
      }
      impl->alive_log_files_.push_back(
          DBImpl::LogFileNumberSize(impl->logfile_number_));
      impl->DeleteObsoleteFiles();
      impl->MaybeScheduleFlushOrCompaction();
      s = impl->db_directory_->Fsync();
    }
  }

  if (s.ok()) {
    for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
      if (cfd->options()->compaction_style == kCompactionStyleUniversal ||
          cfd->options()->compaction_style == kCompactionStyleFIFO) {
        Version* current = cfd->current();
        for (int i = 1; i < current->NumberLevels(); ++i) {
          int num_files = current->NumLevelFiles(i);
          if (num_files > 0) {
            s = Status::InvalidArgument(
                "Not all files are at level 0. Cannot "
                "open with universal or FIFO compaction style.");
            break;
          }
        }
      }
      if (cfd->options()->merge_operator != nullptr &&
          !cfd->mem()->IsMergeOperatorSupported()) {
        s = Status::InvalidArgument(
            "The memtable of column family %s does not support merge operator "
            "its options.merge_operator is non-null", cfd->GetName().c_str());
      }
      if (!s.ok()) {
        break;
      }
    }
  }

  impl->mutex_.Unlock();

  if (s.ok()) {
    impl->opened_successfully_ = true;
    *dbptr = impl;
  } else {
    for (auto h : *handles) {
      delete h;
    }
    handles->clear();
    delete impl;
  }
  return s;
}

Status DB::ListColumnFamilies(const DBOptions& db_options,
                              const std::string& name,
                              std::vector<std::string>* column_families) {
  return VersionSet::ListColumnFamilies(column_families, name, db_options.env);
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  const InternalKeyComparator comparator(options.comparator);
  const Options& soptions(SanitizeOptions(dbname, &comparator, options));
  Env* env = soptions.env;
  std::vector<std::string> filenames;
  std::vector<std::string> archiveFiles;

  std::string archivedir = ArchivalDirectory(dbname);
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);

  if (dbname != soptions.wal_dir) {
    std::vector<std::string> logfilenames;
    env->GetChildren(soptions.wal_dir, &logfilenames);
    filenames.insert(filenames.end(), logfilenames.begin(), logfilenames.end());
    archivedir = ArchivalDirectory(soptions.wal_dir);
  }

  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    InfoLogPrefix info_log_prefix(!options.db_log_dir.empty(), dbname);
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, info_log_prefix.prefix, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del;
        if (type == kMetaDatabase) {
          del = DestroyDB(dbname + "/" + filenames[i], options);
        } else if (type == kLogFile) {
          del = env->DeleteFile(soptions.wal_dir + "/" + filenames[i]);
        } else {
          del = env->DeleteFile(dbname + "/" + filenames[i]);
        }
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }

    for (auto& db_path : options.db_paths) {
      env->GetChildren(db_path.path, &filenames);
      uint64_t number;
      FileType type;
      for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type) &&
            type == kTableFile) {  // Lock file will be deleted at end
          Status del = env->DeleteFile(db_path.path + "/" + filenames[i]);
          if (result.ok() && !del.ok()) {
            result = del;
          }
        }
      }
    }

    env->GetChildren(archivedir, &archiveFiles);
    // Delete archival files.
    for (size_t i = 0; i < archiveFiles.size(); ++i) {
      if (ParseFileName(archiveFiles[i], &number, &type) &&
          type == kLogFile) {
        Status del = env->DeleteFile(archivedir + "/" + archiveFiles[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    // ignore case where no archival directory is present.
    env->DeleteDir(archivedir);

    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
    env->DeleteDir(soptions.wal_dir);
  }
  return result;
}

//
// A global method that can dump out the build version
void DumpLeveldbBuildVersion(Logger * log) {
#if !defined(IOS_CROSS_COMPILE)
  // if we compile with Xcode, we don't run build_detect_vesion, so we don't generate util/build_version.cc
  Log(log, "Git sha %s", rocksdb_build_git_sha);
  Log(log, "Compile time %s %s",
      rocksdb_build_compile_time, rocksdb_build_compile_date);
#endif
}

}  // namespace rocksdb
