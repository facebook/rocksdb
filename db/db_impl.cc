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
#include "db/prefix_filter_iterator.h"
#include "db/table_cache.h"
#include "db/table_properties_collector.h"
#include "db/tailing_iter.h"
#include "db/transaction_log_impl.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "port/port.h"
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
#include "util/stop_watch.h"

namespace rocksdb {

int DBImpl::SuperVersion::dummy = 0;
void* const DBImpl::SuperVersion::kSVInUse = &DBImpl::SuperVersion::dummy;
void* const DBImpl::SuperVersion::kSVObsolete = nullptr;

void DumpLeveldbBuildVersion(Logger * log);

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool disableWAL;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
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
  CompactionFilterContext GetFilterContext() {
    CompactionFilterContext context;
    context.is_full_compaction = compaction->IsFullCompaction();
    context.is_manual_compaction = compaction->IsManualCompaction();
    return context;
  }

  std::vector<Slice> key_buf_;
  std::vector<Slice> existing_value_buf_;
  std::vector<std::string> key_str_buf_;
  std::vector<std::string> existing_value_str_buf_;
  // new_value_buf_ will only be appended if a value changes
  std::vector<std::string> new_value_buf_;
  // if values_changed_buf_[i] is true
  // new_value_buf_ will add a new entry with the changed value
  std::vector<bool> value_changed_buf_;
  // to_delete_buf_[i] is true iff key_buf_[i] is deleted
  std::vector<bool> to_delete_buf_;
  // buffer for the parsed internal keys, the string buffer is backed
  // by key_str_buf_
  std::vector<ParsedInternalKey> ikey_buf_;

  std::vector<Slice> other_key_buf_;
  std::vector<Slice> other_value_buf_;
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
    key_buf_.emplace_back(Slice(key_str_buf_.back()));
    existing_value_buf_.emplace_back(Slice(existing_value_str_buf_.back()));

    ParsedInternalKey ikey;
    ParseInternalKey(key_buf_.back(), &ikey);
    ikey_buf_.emplace_back(ikey);
  }

  // Buffers the kv-pair that will not be run through compaction filter V2
  // in the future.
  void BufferOtherKeyValueSlices(const Slice& key, const Slice& value) {
    other_key_str_buf_.emplace_back(key.ToString());
    other_value_str_buf_.emplace_back(value.ToString());
    other_key_buf_.emplace_back(Slice(other_key_str_buf_.back()));
    other_value_buf_.emplace_back(Slice(other_value_str_buf_.back()));
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
    size_t total_size = key_buf_.size() + other_key_buf_.size();
    combined_key_buf_.reserve(total_size);
    combined_value_buf_.reserve(total_size);

    while (i + j < total_size) {
      int comp_res = 0;
      if (i < key_buf_.size() && j < other_key_buf_.size()) {
        comp_res = comparator->Compare(key_buf_[i], other_key_buf_[j]);
      } else if (i >= key_buf_.size() && j < other_key_buf_.size()) {
        comp_res = 1;
      } else if (j >= other_key_buf_.size() && i < key_buf_.size()) {
        comp_res = -1;
      }
      if (comp_res > 0) {
        AddToCombinedKeyValueSlices(other_key_buf_[j], other_value_buf_[j]);
        j++;
      } else if (comp_res < 0) {
        AddToCombinedKeyValueSlices(key_buf_[i], existing_value_buf_[i]);
        i++;
      }
    }
  }

  void CleanupBatchBuffer() {
    to_delete_buf_.clear();
    key_buf_.clear();
    existing_value_buf_.clear();
    key_str_buf_.clear();
    existing_value_str_buf_.clear();
    new_value_buf_.clear();
    value_changed_buf_.clear();
    ikey_buf_.clear();

    to_delete_buf_.shrink_to_fit();
    key_buf_.shrink_to_fit();
    existing_value_buf_.shrink_to_fit();
    key_str_buf_.shrink_to_fit();
    existing_value_str_buf_.shrink_to_fit();
    new_value_buf_.shrink_to_fit();
    value_changed_buf_.shrink_to_fit();
    ikey_buf_.shrink_to_fit();

    other_key_buf_.clear();
    other_value_buf_.clear();
    other_key_str_buf_.clear();
    other_value_str_buf_.clear();
    other_key_buf_.shrink_to_fit();
    other_value_buf_.shrink_to_fit();
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

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  // result.max_open_files means an "infinite" open files.
  if (result.max_open_files != -1) {
    ClipToRange(&result.max_open_files,            20,     1000000);
  }
  ClipToRange(&result.write_buffer_size,         ((size_t)64)<<10,
                                                 ((size_t)64)<<30);
  ClipToRange(&result.block_size,                1<<10,  4<<20);

  // if user sets arena_block_size, we trust user to use this value. Otherwise,
  // calculate a proper value from writer_buffer_size;
  if (result.arena_block_size <= 0) {
    result.arena_block_size = result.write_buffer_size / 10;
  }

  result.min_write_buffer_number_to_merge = std::min(
    result.min_write_buffer_number_to_merge, result.max_write_buffer_number-1);
  if (result.info_log == nullptr) {
    Status s = CreateLoggerFromOptions(dbname, result.db_log_dir, src.env,
                                       result, &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr && !result.no_block_cache) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  result.compression_per_level = src.compression_per_level;
  if (result.block_size_deviation < 0 || result.block_size_deviation > 100) {
    result.block_size_deviation = 0;
  }
  if (result.max_mem_compaction_level >= result.num_levels) {
    result.max_mem_compaction_level = result.num_levels - 1;
  }
  if (result.soft_rate_limit > result.hard_rate_limit) {
    result.soft_rate_limit = result.hard_rate_limit;
  }
  if (result.compaction_filter) {
    Log(result.info_log, "Compaction filter specified, ignore factory");
  }
  if (result.prefix_extractor) {
    Log(result.info_log, "prefix extractor %s in use.",
        result.prefix_extractor->Name());
  } else {
    assert(result.memtable_factory);
    Slice name = result.memtable_factory->Name();
    if (name.compare("HashSkipListRepFactory") == 0 ||
        name.compare("HashLinkListRepFactory") == 0) {
      Log(result.info_log, "prefix extractor is not provided while using %s. "
          "fallback to skiplist", name.ToString().c_str());
      result.memtable_factory = std::make_shared<SkipListFactory>();
    }
  }

  if (result.wal_dir.empty()) {
    // Use dbname as default
    result.wal_dir = dbname;
  }

  // -- Sanitize the table properties collector
  // All user defined properties collectors will be wrapped by
  // UserKeyTablePropertiesCollector since for them they only have the
  // knowledge of the user keys; internal keys are invisible to them.
  auto& collectors = result.table_properties_collectors;
  for (size_t i = 0; i < result.table_properties_collectors.size(); ++i) {
    assert(collectors[i]);
    collectors[i] =
      std::make_shared<UserKeyTablePropertiesCollector>(collectors[i]);
  }

  // Add collector to collect internal key statistics
  collectors.push_back(
      std::make_shared<InternalKeyPropertiesCollector>()
  );

  return result;
}

CompressionType GetCompressionType(const Options& options, int level,
                                   const bool enable_compression) {
  if (!enable_compression) {
    // disable compression
    return kNoCompression;
  }
  // If the use has specified a different compression level for each level,
  // then pick the compresison for that level.
  if (!options.compression_per_level.empty()) {
    const int n = options.compression_per_level.size() - 1;
    // It is possible for level_ to be -1; in that case, we use level
    // 0's compression.  This occurs mostly in backwards compatibility
    // situations when the builder doesn't know what level the file
    // belongs to.  Likewise, if level_ is beyond the end of the
    // specified compression levels, use the last value.
    return options.compression_per_level[std::max(0, std::min(level, n))];
  } else {
    return options.compression;
  }
}

CompressionType GetCompressionFlush(const Options& options) {
  // Compressing memtable flushes might not help unless the sequential load
  // optimization is used for leveled compaction. Otherwise the CPU and
  // latency overhead is not offset by saving much space.

  bool can_compress;

  if  (options.compaction_style == kCompactionStyleUniversal) {
    can_compress =
        (options.compaction_options_universal.compression_size_percent < 0);
  } else {
    // For leveled compress when min_level_to_compress == 0.
    can_compress = (GetCompressionType(options, 0, true) != kNoCompression);
  }

  if (can_compress) {
    return options.compression;
  } else {
    return kNoCompression;
  }
}

DBImpl::DBImpl(const Options& options, const std::string& dbname)
    : env_(options.env),
      dbname_(dbname),
      internal_comparator_(options.comparator),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, options)),
      internal_filter_policy_(options.filter_policy),
      owns_info_log_(options_.info_log != options.info_log),
      db_lock_(nullptr),
      mutex_(options.use_adaptive_mutex),
      shutting_down_(nullptr),
      bg_cv_(&mutex_),
      mem_(new MemTable(internal_comparator_, options_)),
      imm_(options_.min_write_buffer_number_to_merge),
      logfile_number_(0),
      super_version_(nullptr),
      super_version_number_(0),
      local_sv_(new ThreadLocalPtr(&SuperVersionUnrefHandle)),
      tmp_batch_(),
      bg_schedule_needed_(false),
      bg_compaction_scheduled_(0),
      bg_manual_only_(0),
      bg_flush_scheduled_(0),
      bg_logstats_scheduled_(false),
      manual_compaction_(nullptr),
      logger_(nullptr),
      disable_delete_obsolete_files_(0),
      delete_obsolete_files_last_run_(options.env->NowMicros()),
      purge_wal_files_last_run_(0),
      last_stats_dump_time_microsec_(0),
      default_interval_to_delete_obsolete_WAL_(600),
      flush_on_destroy_(false),
      internal_stats_(options.num_levels, options.env,
                      options.statistics.get()),
      delayed_writes_(0),
      storage_options_(options),
      bg_work_gate_closed_(false),
      refitting_level_(false),
      opened_successfully_(false) {
  mem_->Ref();
  env_->GetAbsolutePath(dbname, &db_absolute_path_);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  // Give a large number for setting of "infinite" open files.
  const int table_cache_size =
      (options_.max_open_files == -1) ?
          4194304 : options_.max_open_files - 10;
  table_cache_.reset(new TableCache(dbname_, &options_,
                                    storage_options_, table_cache_size));
  versions_.reset(new VersionSet(dbname_, &options_, storage_options_,
                                 table_cache_.get(), &internal_comparator_));

  DumpLeveldbBuildVersion(options_.info_log.get());
  options_.Dump(options_.info_log.get());

  char name[100];
  Status s = env_->GetHostName(name, 100L);
  if (s.ok()) {
    host_name_ = name;
  } else {
    Log(options_.info_log, "Can't get hostname, use localhost as host name.");
    host_name_ = "localhost";
  }
  last_log_ts = 0;

  LogFlush(options_.info_log);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  if (flush_on_destroy_ && mem_->GetFirstSequenceNumber() != 0) {
    FlushMemTable(FlushOptions());
  }

  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-nullptr value is ok
  while (bg_compaction_scheduled_ ||
         bg_flush_scheduled_ ||
         bg_logstats_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  // Release SuperVersion reference kept in ThreadLocalPtr.
  // This must be done outside of mutex_ since unref handler can lock mutex.
  // It also needs to be done after FlushMemTable, which can trigger local_sv_
  // access.
  delete local_sv_;

  mutex_.Lock();
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

  if (super_version_ != nullptr) {
    bool is_last_reference __attribute__((unused));
    is_last_reference = super_version_->Unref();
    assert(is_last_reference);
    super_version_->Cleanup();
    delete super_version_;
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  if (mem_ != nullptr) {
    delete mem_->Unref();
  }

  autovector<MemTable*> to_delete;
  imm_.current()->Unref(&to_delete);
  for (MemTable* m: to_delete) {
    delete m;
  }
  // versions need to be destroyed before table_cache since it can holds
  // references to table_cache.
  versions_.reset();
  LogFlush(options_.info_log);
}

// Do not flush and close database elegantly. Simulate a crash.
void DBImpl::TEST_Destroy_DBImpl() {
  // ensure that no new memtable flushes can occur
  flush_on_destroy_ = false;

  // wait till all background compactions are done.
  mutex_.Lock();
  while (bg_compaction_scheduled_ ||
         bg_flush_scheduled_ ||
         bg_logstats_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  // Release SuperVersion reference kept in ThreadLocalPtr.
  // This must be done outside of mutex_ since unref handler can lock mutex.
  // It also needs to be done after FlushMemTable, which can trigger local_sv_
  // access.
  delete local_sv_;

  mutex_.Lock();
  if (super_version_ != nullptr) {
    bool is_last_reference __attribute__((unused));
    is_last_reference = super_version_->Unref();
    assert(is_last_reference);
    super_version_->Cleanup();
    delete super_version_;
  }

  // Prevent new compactions from occuring.
  bg_work_gate_closed_ = true;
  const int LargeNumber = 10000000;
  bg_compaction_scheduled_ += LargeNumber;

  mutex_.Unlock();
  LogFlush(options_.info_log);

  // force release the lock file.
  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  log_.reset();
  versions_.reset();
  table_cache_.reset();
}

uint64_t DBImpl::TEST_Current_Manifest_FileNo() {
  return versions_->ManifestFileNumber();
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

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
    s = SetCurrentFile(env_, dbname_, 1);
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
    std::string stats;
    GetProperty("rocksdb.stats", &stats);
    Log(options_.info_log, "%s", stats.c_str());
    PrintStatistics();
  }
}

// DBImpl::SuperVersion methods
DBImpl::SuperVersion::~SuperVersion() {
  for (auto td : to_delete) {
    delete td;
  }
}

DBImpl::SuperVersion* DBImpl::SuperVersion::Ref() {
  refs.fetch_add(1, std::memory_order_relaxed);
  return this;
}

bool DBImpl::SuperVersion::Unref() {
  assert(refs > 0);
  // fetch_sub returns the previous value of yoeref
  return refs.fetch_sub(1, std::memory_order_relaxed) == 1;
}

void DBImpl::SuperVersion::Cleanup() {
  db->mutex_.AssertHeld();
  assert(refs.load(std::memory_order_relaxed) == 0);
  imm->Unref(&to_delete);
  MemTable* m = mem->Unref();
  if (m != nullptr) {
    to_delete.push_back(m);
  }
  current->Unref();
}

void DBImpl::SuperVersion::Init(MemTable* new_mem, MemTableListVersion* new_imm,
                                Version* new_current) {
  db->mutex_.AssertHeld();
  mem = new_mem;
  imm = new_imm;
  current = new_current;
  mem->Ref();
  imm->Ref();
  current->Ref();
  refs.store(1, std::memory_order_relaxed);
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
  deletion_state.log_number = versions_->LogNumber();
  deletion_state.prev_log_number = versions_->PrevLogNumber();

  if (!doing_the_full_scan && !deletion_state.HaveSomethingToDelete()) {
    // avoid filling up sst_live if we're sure that we
    // are not going to do the full scan and that we don't have
    // anything to delete at the moment
    return;
  }

  // don't delete live files
  deletion_state.sst_live.assign(pending_outputs_.begin(),
                                 pending_outputs_.end());
  versions_->AddLiveFiles(&deletion_state.sst_live);

  if (doing_the_full_scan) {
    // set of all files in the directory. We'll exclude files that are still
    // alive in the subsequent processings.
    env_->GetChildren(
        dbname_, &deletion_state.candidate_files
    ); // Ignore errors

    //Add log files in wal_dir
    if (options_.wal_dir != dbname_) {
      std::vector<std::string> log_files;
      env_->GetChildren(options_.wal_dir, &log_files); // Ignore errors
      deletion_state.candidate_files.insert(
        deletion_state.candidate_files.end(),
        log_files.begin(),
        log_files.end()
      );
    }
  }
}

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

  // Now, convert live list to an unordered set, WITHOUT mutex held;
  // set is slow.
  std::unordered_set<uint64_t> sst_live(state.sst_live.begin(),
                                        state.sst_live.end());

  auto& candidate_files = state.candidate_files;
  candidate_files.reserve(
      candidate_files.size() +
      state.sst_delete_files.size() +
      state.log_delete_files.size());
  // We may ignore the dbname when generating the file names.
  const char* kDumbDbName = "";
  for (auto file : state.sst_delete_files) {
    candidate_files.push_back(
        TableFileName(kDumbDbName, file->number).substr(1)
    );
    delete file;
  }

  for (auto file_num : state.log_delete_files) {
    if (file_num > 0) {
      candidate_files.push_back(LogFileName(kDumbDbName, file_num).substr(1));
    }
  }

  // dedup state.candidate_files so we don't try to delete the same
  // file twice
  sort(candidate_files.begin(), candidate_files.end());
  candidate_files.erase(unique(candidate_files.begin(), candidate_files.end()),
                        candidate_files.end());

  std::vector<std::string> old_info_log_files;

  for (const auto& to_delete : candidate_files) {
    uint64_t number;
    FileType type;
    // Ignore file if we cannot recognize it.
    if (!ParseFileName(to_delete, &number, &type)) {
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
        keep = (sst_live.find(number) != sst_live.end());
        break;
      case kTempFile:
        // Any temp files that are currently being written to must
        // be recorded in pending_outputs_, which is inserted into "live".
        // Also, SetCurrentFile creates a temp file when writing out new
        // manifest, which is equal to state.pending_manifest_file_number. We
        // should not delete that file
        keep = (sst_live.find(number) != sst_live.end()) ||
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

    if (type == kTableFile) {
      // evict from cache
      table_cache_->Evict(number);
    }

    std::string fname = ((type == kLogFile) ? options_.wal_dir : dbname_) +
        "/" + to_delete;
    if (type == kLogFile &&
        (options_.WAL_ttl_seconds > 0 || options_.WAL_size_limit_MB > 0)) {
      auto archived_log_name = ArchivedLogFileName(options_.wal_dir, number);
      Status s = env_->RenameFile(fname, archived_log_name);
      Log(options_.info_log,
          "Move log file %s to %s -- %s\n",
          fname.c_str(), archived_log_name.c_str(), s.ToString().c_str());
    } else {
      Status s = env_->DeleteFile(fname);
      Log(options_.info_log, "Delete %s type=%d #%lu -- %s\n",
          fname.c_str(), type, (unsigned long)number,
          s.ToString().c_str());
    }
  }

  // Delete old info log files.
  size_t old_info_log_file_count = old_info_log_files.size();
  // NOTE: Currently we only support log purge when options_.db_log_dir is
  // located in `dbname` directory.
  if (old_info_log_file_count >= options_.keep_log_file_num &&
      options_.db_log_dir.empty()) {
    std::sort(old_info_log_files.begin(), old_info_log_files.end());
    size_t end = old_info_log_file_count - options_.keep_log_file_num;
    for (unsigned int i = 0; i <= end; i++) {
      std::string& to_delete = old_info_log_files.at(i);
      Log(options_.info_log, "Delete info log file %s\n", to_delete.c_str());
      Status s = env_->DeleteFile(dbname_ + "/" + to_delete);
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
  AppendSortedWalsOfType(archival_dir, archived_logs, kArchivedLogFile);

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
    }
  }
}

Status DBImpl::Recover(bool read_only, bool error_if_log_file_exist) {
  mutex_.AssertHeld();

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
        // TODO: add merge_operator name check
        s = NewDB();
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

  Status s = versions_->Recover();
  if (options_.paranoid_checks && s.ok()) {
    s = CheckConsistency();
  }
  if (s.ok()) {
    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of rocksdb.
    const uint64_t min_log = versions_->LogNumber();
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
      if (ParseFileName(filenames[i], &number, &type)
          && type == kLogFile
          && ((number >= min_log) || (number == prev_log))) {
        logs.push_back(number);
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

    if (s.ok()) {
      if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
      }
      SetTickerCount(options_.statistics.get(), SEQUENCE_NUMBER,
                     versions_->LastSequence());
    }
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

  VersionEdit edit;

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
  Log(options_.info_log, "Recovering log #%lu",
      (unsigned long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  bool memtable_empty = true;
  while (reader.ReadRecord(&record, &scratch)) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    status = WriteBatchInternal::InsertInto(&batch, mem_, &options_);
    memtable_empty = false;
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

    if (!read_only && mem_->ShouldFlush()) {
      status = WriteLevel0TableForRecovery(mem_, &edit);
      // we still want to clear memtable, even if the recovery failed
      delete mem_->Unref();
      mem_ = new MemTable(internal_comparator_, options_);
      mem_->Ref();
      memtable_empty = true;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        return status;
      }
    }
  }

  if (!memtable_empty && !read_only) {
    status = WriteLevel0TableForRecovery(mem_, &edit);
    delete mem_->Unref();
    mem_ = new MemTable(internal_comparator_, options_);
    mem_->Ref();
    if (!status.ok()) {
      return status;
    }
  }

  if (edit.NumEntries() > 0) {
    // if read_only, NumEntries() will be 0
    assert(!read_only);
    // writing log number in the manifest means that any log file
    // with number strongly less than (log_number + 1) is already
    // recovered and should be ignored on next reincarnation.
    // Since we already recovered log_number, we want all logs
    // with numbers `<= log_number` (includes this one) to be ignored
    edit.SetLogNumber(log_number + 1);
    // we must mark the next log number as used, even though it's
    // not actually used. that is because VersionSet assumes
    // VersionSet::next_file_number_ always to be strictly greater than any log
    // number
    versions_->MarkFileNumberUsed(log_number + 1);
    status = versions_->LogAndApply(&edit, &mutex_);
  }

  return status;
}

Status DBImpl::WriteLevel0TableForRecovery(MemTable* mem, VersionEdit* edit) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  const SequenceNumber newest_snapshot = snapshots_.GetNewest();
  const SequenceNumber earliest_seqno_in_memtable =
    mem->GetFirstSequenceNumber();
  Log(options_.info_log, "Level-0 table #%lu: started",
      (unsigned long) meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, storage_options_,
                   table_cache_.get(), iter, &meta, internal_comparator_,
                   newest_snapshot, earliest_seqno_in_memtable,
                   GetCompressionFlush(options_));
    LogFlush(options_.info_log);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%lu: %lu bytes %s",
      (unsigned long) meta.number,
      (unsigned long) meta.file_size,
      s.ToString().c_str());
  delete iter;

  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest,
                  meta.smallest_seqno, meta.largest_seqno);
  }

  InternalStats::CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats.files_out_levelnp1 = 1;
  internal_stats_.AddCompactionStats(level, stats);
  RecordTick(options_.statistics.get(), COMPACT_WRITE_BYTES, meta.file_size);
  return s;
}


Status DBImpl::WriteLevel0Table(autovector<MemTable*>& mems, VersionEdit* edit,
                                uint64_t* filenumber,
                                LogBuffer* log_buffer) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  *filenumber = meta.number;
  pending_outputs_.insert(meta.number);

  const SequenceNumber newest_snapshot = snapshots_.GetNewest();
  const SequenceNumber earliest_seqno_in_memtable =
    mems[0]->GetFirstSequenceNumber();
  Version* base = versions_->current();
  base->Ref();          // it is likely that we do not need this reference
  Status s;
  {
    mutex_.Unlock();
    log_buffer->FlushBufferToLog();
    std::vector<Iterator*> memtables;
    for (MemTable* m : mems) {
      Log(options_.info_log,
          "Flushing memtable with log file: %lu\n",
          (unsigned long)m->GetLogNumber());
      memtables.push_back(m->NewIterator());
    }
    Iterator* iter = NewMergingIterator(
        env_, &internal_comparator_, &memtables[0], memtables.size());
    Log(options_.info_log,
        "Level-0 flush table #%lu: started",
        (unsigned long)meta.number);

    s = BuildTable(dbname_, env_, options_, storage_options_,
                   table_cache_.get(), iter, &meta, internal_comparator_,
                   newest_snapshot, earliest_seqno_in_memtable,
                   GetCompressionFlush(options_));
    LogFlush(options_.info_log);
    delete iter;
    Log(options_.info_log, "Level-0 flush table #%lu: %lu bytes %s",
        (unsigned long) meta.number,
        (unsigned long) meta.file_size,
        s.ToString().c_str());
    if (!options_.disableDataSync) {
      db_directory_->Fsync();
    }
    mutex_.Lock();
  }
  base->Unref();

  // re-acquire the most current version
  base = versions_->current();

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
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    // if we have more than 1 background thread, then we cannot
    // insert files directly into higher levels because some other
    // threads could be concurrently producing compacted files for
    // that key range.
    if (base != nullptr && options_.max_background_compactions <= 1 &&
        options_.compaction_style == kCompactionStyleLevel) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest,
                  meta.smallest_seqno, meta.largest_seqno);
  }

  InternalStats::CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  internal_stats_.AddCompactionStats(level, stats);
  RecordTick(options_.statistics.get(), COMPACT_WRITE_BYTES, meta.file_size);
  return s;
}

Status DBImpl::FlushMemTableToOutputFile(bool* madeProgress,
                                         DeletionState& deletion_state,
                                         LogBuffer* log_buffer) {
  mutex_.AssertHeld();
  assert(imm_.size() != 0);
  assert(imm_.IsFlushPending());

  // Save the contents of the earliest memtable as a new Table
  uint64_t file_number;
  autovector<MemTable*> mems;
  imm_.PickMemtablesToFlush(&mems);
  if (mems.empty()) {
    LogToBuffer(log_buffer, "Nothing in memstore to flush");
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

  std::vector<uint64_t> logs_to_delete;
  for (auto mem : mems) {
    logs_to_delete.push_back(mem->GetLogNumber());
  }

  // This will release and re-acquire the mutex.
  Status s = WriteLevel0Table(mems, edit, &file_number, log_buffer);

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::ShutdownInProgress(
        "Database shutdown started during memtable compaction");
  }

  if (!s.ok()) {
    imm_.RollbackMemtableFlush(mems, file_number, &pending_outputs_);
  } else {
    // Replace immutable memtable with the generated Table
    s = imm_.InstallMemtableFlushResults(
        mems, versions_.get(), &mutex_, options_.info_log.get(), file_number,
        pending_outputs_, &deletion_state.memtables_to_free,
        db_directory_.get());
  }

  if (s.ok()) {
    InstallSuperVersion(deletion_state);
    if (madeProgress) {
      *madeProgress = 1;
    }

    MaybeScheduleLogDBDeployStats();

    if (disable_delete_obsolete_files_ == 0) {
      // add to deletion state
      deletion_state.log_delete_files.insert(
          deletion_state.log_delete_files.end(),
          logs_to_delete.begin(),
          logs_to_delete.end());
    }
  }

  if (!s.ok() && !s.IsShutdownInProgress() && options_.paranoid_checks &&
      bg_error_.ok()) {
    // if a bad error happened (not ShutdownInProgress) and paranoid_checks is
    // true, mark DB read-only
    bg_error_ = s;
  }
  return s;
}

Status DBImpl::CompactRange(const Slice* begin,
                            const Slice* end,
                            bool reduce_level,
                            int target_level) {
  Status s = FlushMemTable(FlushOptions());
  if (!s.ok()) {
    LogFlush(options_.info_log);
    return s;
  }

  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < NumberLevels(); level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  for (int level = 0; level <= max_level_with_files; level++) {
    // in case the compaction is unversal or if we're compacting the
    // bottom-most level, the output level will be the same as input one
    if (options_.compaction_style == kCompactionStyleUniversal ||
        level == max_level_with_files) {
      s = RunManualCompaction(level, level, begin, end);
    } else {
      s = RunManualCompaction(level, level + 1, begin, end);
    }
    if (!s.ok()) {
      LogFlush(options_.info_log);
      return s;
    }
  }

  if (reduce_level) {
    s = ReFitLevel(max_level_with_files, target_level);
  }
  LogFlush(options_.info_log);

  return s;
}

// return the same level if it cannot be moved
int DBImpl::FindMinimumEmptyLevelFitting(int level) {
  mutex_.AssertHeld();
  Version* current = versions_->current();
  int minimum_level = level;
  for (int i = level - 1; i > 0; --i) {
    // stop if level i is not empty
    if (current->NumLevelFiles(i) > 0) break;
    // stop if level i is too small (cannot fit the level files)
    if (versions_->MaxBytesForLevel(i) < current->NumLevelBytes(level)) break;

    minimum_level = i;
  }
  return minimum_level;
}

Status DBImpl::ReFitLevel(int level, int target_level) {
  assert(level < NumberLevels());

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
    to_level = FindMinimumEmptyLevelFitting(level);
  }

  assert(to_level <= level);

  Status status;
  if (to_level < level) {
    Log(options_.info_log, "Before refitting:\n%s",
        versions_->current()->DebugString().data());

    VersionEdit edit;
    for (const auto& f : versions_->current()->files_[level]) {
      edit.DeleteFile(level, f->number);
      edit.AddFile(to_level, f->number, f->file_size, f->smallest, f->largest,
                   f->smallest_seqno, f->largest_seqno);
    }
    Log(options_.info_log, "Apply version edit:\n%s",
        edit.DebugString().data());

    status = versions_->LogAndApply(&edit, &mutex_, db_directory_.get());
    superversion_to_free = InstallSuperVersion(new_superversion);
    new_superversion = nullptr;

    Log(options_.info_log, "LogAndApply: %s\n", status.ToString().data());

    if (status.ok()) {
      Log(options_.info_log, "After refitting:\n%s",
          versions_->current()->DebugString().data());
    }
  }

  refitting_level_ = false;
  bg_work_gate_closed_ = false;

  mutex_.Unlock();
  delete superversion_to_free;
  delete new_superversion;
  return status;
}

int DBImpl::NumberLevels() {
  return options_.num_levels;
}

int DBImpl::MaxMemCompactionLevel() {
  return options_.max_mem_compaction_level;
}

int DBImpl::Level0StopWriteTrigger() {
  return options_.level0_stop_writes_trigger;
}

uint64_t DBImpl::CurrentVersionNumber() const {
  return super_version_number_.load();
}

Status DBImpl::Flush(const FlushOptions& options) {
  return FlushMemTable(options);
}

SequenceNumber DBImpl::GetLatestSequenceNumber() const {
  return versions_->LastSequence();
}

Status DBImpl::GetUpdatesSince(
    SequenceNumber seq, unique_ptr<TransactionLogIterator>* iter,
    const TransactionLogIterator::ReadOptions& read_options) {

  RecordTick(options_.statistics.get(), GET_UPDATES_SINCE_CALLS);
  if (seq > versions_->LastSequence()) {
    return Status::NotFound(
        "Requested sequence not yet written in the db");
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

Status DBImpl::RetainProbableWalFiles(VectorLogPtr& all_logs,
                                      const SequenceNumber target) {
  long start = 0; // signed to avoid overflow when target is < first file.
  long end = static_cast<long>(all_logs.size()) - 1;
  // Binary Search. avoid opening all files.
  while (end >= start) {
    long mid = start + (end - start) / 2;  // Avoid overflow.
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
  size_t start_index = std::max(0l, end); // end could be -ve.
  // The last wal file is always included
  all_logs.erase(all_logs.begin(), all_logs.begin() + start_index);
  return Status::OK();
}

bool DBImpl::CheckWalFileExistsAndEmpty(const WalFileType type,
                                        const uint64_t number) {
  const std::string fname = (type == kAliveLogFile) ?
    LogFileName(options_.wal_dir, number) :
    ArchivedLogFileName(options_.wal_dir, number);
  uint64_t file_size;
  Status s = env_->GetFileSize(fname, &file_size);
  return (s.ok() && (file_size == 0));
}

Status DBImpl::ReadFirstRecord(const WalFileType type, const uint64_t number,
                               WriteBatch* const result) {

  if (type == kAliveLogFile) {
    std::string fname = LogFileName(options_.wal_dir, number);
    Status status = ReadFirstLine(fname, result);
    if (status.ok() || env_->FileExists(fname)) {
      // return OK or any error that is not caused non-existing file
      return status;
    }

    //  check if the file got moved to archive.
    std::string archived_file =
      ArchivedLogFileName(options_.wal_dir, number);
    Status s = ReadFirstLine(archived_file, result);
    if (s.ok() || env_->FileExists(archived_file)) {
      return s;
    }
    return Status::NotFound("Log File has been deleted: " + archived_file);
  } else if (type == kArchivedLogFile) {
    std::string fname = ArchivedLogFileName(options_.wal_dir, number);
    Status status = ReadFirstLine(fname, result);
    return status;
  }
  return Status::NotSupported("File Type Not Known: " + std::to_string(type));
}

Status DBImpl::ReadFirstLine(const std::string& fname,
                             WriteBatch* const batch) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;

    Status* status;
    bool ignore_error;  // true if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->ignore_error ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
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
  log::Reader reader(std::move(file), &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  std::string scratch;
  Slice record;

  if (reader.ReadRecord(&record, &scratch) &&
      (status.ok() || !options_.paranoid_checks)) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      //  TODO read record's till the first no corrupt entry?
    } else {
      WriteBatchInternal::SetContents(batch, record);
      return Status::OK();
    }
  }

  // ReadRecord returns false on EOF, which is deemed as OK() by Reader
  if (status.ok()) {
    status = Status::Corruption("eof reached");
  }
  return status;
}

struct CompareLogByPointer {
  bool operator() (const unique_ptr<LogFile>& a,
                   const unique_ptr<LogFile>& b) {
    LogFileImpl* a_impl = dynamic_cast<LogFileImpl*>(a.get());
    LogFileImpl* b_impl = dynamic_cast<LogFileImpl*>(b.get());
    return *a_impl < *b_impl;
  }
};

Status DBImpl::AppendSortedWalsOfType(const std::string& path,
    VectorLogPtr& log_files, WalFileType log_type) {
  std::vector<std::string> all_files;
  const Status status = env_->GetChildren(path, &all_files);
  if (!status.ok()) {
    return status;
  }
  log_files.reserve(log_files.size() + all_files.size());
  VectorLogPtr::iterator pos_start;
  if (!log_files.empty()) {
    pos_start = log_files.end() - 1;
  } else {
    pos_start = log_files.begin();
  }
  for (const auto& f : all_files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kLogFile){

      WriteBatch batch;
      Status s = ReadFirstRecord(log_type, number, &batch);
      if (!s.ok()) {
        if (CheckWalFileExistsAndEmpty(log_type, number)) {
          continue;
        }
        return s;
      }

      uint64_t size_bytes;
      s = env_->GetFileSize(LogFileName(path, number), &size_bytes);
      if (!s.ok()) {
        return s;
      }

      log_files.push_back(std::move(unique_ptr<LogFile>(new LogFileImpl(
        number, log_type, WriteBatchInternal::Sequence(&batch), size_bytes))));
    }
  }
  CompareLogByPointer compare_log_files;
  std::sort(pos_start, log_files.end(), compare_log_files);
  return status;
}

Status DBImpl::RunManualCompaction(int input_level,
                                   int output_level,
                                   const Slice* begin,
                                   const Slice* end) {
  assert(input_level >= 0);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.input_level = input_level;
  manual.output_level = output_level;
  manual.done = false;
  manual.in_progress = false;
  // For universal compaction, we enforce every manual compaction to compact
  // all files.
  if (begin == nullptr ||
      options_.compaction_style == kCompactionStyleUniversal) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr ||
      options_.compaction_style == kCompactionStyleUniversal) {
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
        "Manual compaction waiting for all other scheduled background "
        "compactions to finish");
    bg_cv_.Wait();
  }

  Log(options_.info_log, "Manual compaction starting");

  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    assert(bg_manual_only_ > 0);
    if (manual_compaction_ != nullptr) {
      // Running either this or some other manual compaction
      bg_cv_.Wait();
    } else {
      manual_compaction_ = &manual;
      MaybeScheduleFlushOrCompaction();
    }
  }

  assert(!manual.in_progress);
  assert(bg_manual_only_ > 0);
  --bg_manual_only_;
  return manual.status;
}

Status DBImpl::TEST_CompactRange(int level,
                                 const Slice* begin,
                                 const Slice* end) {
  int output_level = (options_.compaction_style == kCompactionStyleUniversal)
                         ? level
                         : level + 1;
  return RunManualCompaction(level, output_level, begin, end);
}

Status DBImpl::FlushMemTable(const FlushOptions& options) {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok() && options.wait) {
    // Wait until the compaction completes
    s = WaitForFlushMemTable();
  }
  return s;
}

Status DBImpl::WaitForFlushMemTable() {
  Status s;
  // Wait until the compaction completes
  MutexLock l(&mutex_);
  while (imm_.size() > 0 && bg_error_.ok()) {
    bg_cv_.Wait();
  }
  if (imm_.size() != 0) {
    s = bg_error_;
  }
  return s;
}

Status DBImpl::TEST_FlushMemTable(bool wait) {
  FlushOptions fo;
  fo.wait = wait;
  return FlushMemTable(fo);
}

Status DBImpl::TEST_WaitForFlushMemTable() {
  return WaitForFlushMemTable();
}

Status DBImpl::TEST_WaitForCompact() {
  // Wait until the compaction completes

  // TODO: a bug here. This function actually does not necessarily
  // wait for compact. It actually waits for scheduled compaction
  // OR flush to finish.

  MutexLock l(&mutex_);
  while ((bg_compaction_scheduled_ || bg_flush_scheduled_) &&
         bg_error_.ok()) {
    bg_cv_.Wait();
  }
  return bg_error_;
}

void DBImpl::MaybeScheduleFlushOrCompaction() {
  mutex_.AssertHeld();
  bg_schedule_needed_ = false;
  if (bg_work_gate_closed_) {
    // gate closed for backgrond work
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else {
    bool is_flush_pending = imm_.IsFlushPending();
    if (is_flush_pending) {
      if (bg_flush_scheduled_ < options_.max_background_flushes) {
        // memtable flush needed
        bg_flush_scheduled_++;
        env_->Schedule(&DBImpl::BGWorkFlush, this, Env::Priority::HIGH);
      } else if (options_.max_background_flushes > 0) {
        bg_schedule_needed_ = true;
      }
    }

    // Schedule BGWorkCompaction if there's a compaction pending (or a memtable
    // flush, but the HIGH pool is not enabled). Do it only if
    // max_background_compactions hasn't been reached and, in case
    // bg_manual_only_ > 0, if it's a manual compaction.
    if ((manual_compaction_ ||
         versions_->current()->NeedsCompaction() ||
         (is_flush_pending && (options_.max_background_flushes <= 0))) &&
        (!bg_manual_only_ || manual_compaction_)) {
      if (bg_compaction_scheduled_ < options_.max_background_compactions) {
        bg_compaction_scheduled_++;
        env_->Schedule(&DBImpl::BGWorkCompaction, this, Env::Priority::LOW);
      } else {
        bg_schedule_needed_ = true;
      }
    }
  }
}

void DBImpl::BGWorkFlush(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCallFlush();
}

void DBImpl::BGWorkCompaction(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCallCompaction();
}

Status DBImpl::BackgroundFlush(bool* madeProgress,
                               DeletionState& deletion_state,
                               LogBuffer* log_buffer) {
  Status stat;
  while (stat.ok() && imm_.IsFlushPending()) {
    Log(options_.info_log,
        "BackgroundCallFlush doing FlushMemTableToOutputFile, flush slots available %d",
        options_.max_background_flushes - bg_flush_scheduled_);
    stat = FlushMemTableToOutputFile(madeProgress, deletion_state, log_buffer);
  }
  return stat;
}

void DBImpl::BackgroundCallFlush() {
  bool madeProgress = false;
  DeletionState deletion_state(true);
  assert(bg_flush_scheduled_);

  LogBuffer log_buffer(INFO, options_.info_log.get());
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
        uint64_t error_cnt = internal_stats_.BumpAndGetBackgroundErrorCount();
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
    bg_cv_.SignalAll();
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}


void DBImpl::TEST_PurgeObsoleteteWAL() {
  PurgeObsoleteWALFiles();
}

uint64_t DBImpl::TEST_GetLevel0TotalSize() {
  MutexLock l(&mutex_);
  return versions_->current()->NumLevelBytes(0);
}

void DBImpl::BackgroundCallCompaction() {
  bool madeProgress = false;
  DeletionState deletion_state(true);

  MaybeDumpStats();
  LogBuffer log_buffer(INFO, options_.info_log.get());
  {
    MutexLock l(&mutex_);
    // Log(options_.info_log, "XXX BG Thread %llx process new work item",
    //     pthread_self());
    assert(bg_compaction_scheduled_);
    Status s;
    if (!shutting_down_.Acquire_Load()) {
      s = BackgroundCompaction(&madeProgress, deletion_state, &log_buffer);
      if (!s.ok()) {
        // Wait a little bit before retrying background compaction in
        // case this is an environmental problem and we do not want to
        // chew up resources for failed compactions for the duration of
        // the problem.
        uint64_t error_cnt = internal_stats_.BumpAndGetBackgroundErrorCount();
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

    MaybeScheduleLogDBDeployStats();

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
    bg_cv_.SignalAll();
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
  }

  // TODO: remove memtable flush from formal compaction
  while (imm_.IsFlushPending()) {
    LogToBuffer(log_buffer,
                "BackgroundCompaction doing FlushMemTableToOutputFile, "
                "compaction slots "
                "available %d",
                options_.max_background_compactions - bg_compaction_scheduled_);
    Status stat = FlushMemTableToOutputFile(madeProgress, deletion_state,
                                            log_buffer);
    if (!stat.ok()) {
      if (is_manual) {
        manual_compaction_->status = stat;
        manual_compaction_->done = true;
        manual_compaction_->in_progress = false;
        manual_compaction_ = nullptr;
      }
      return stat;
    }
  }

  unique_ptr<Compaction> c;
  InternalKey manual_end_storage;
  InternalKey* manual_end = &manual_end_storage;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    assert(m->in_progress);
    c.reset(versions_->CompactRange(
        m->input_level, m->output_level, m->begin, m->end, &manual_end));
    if (!c) {
      m->done = true;
    }
    LogToBuffer(
        log_buffer,
        "Manual compaction from level-%d to level-%d from %s .. %s; will stop "
        "at %s\n",
        m->input_level, m->output_level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        ((m->done || manual_end == nullptr)
             ? "(end)"
             : manual_end->DebugString().c_str()));
  } else if (!options_.disable_auto_compactions) {
    c.reset(versions_->PickCompaction(log_buffer));
  }

  Status status;
  if (!c) {
    // Nothing to do
    LogToBuffer(log_buffer, "Compaction nothing to do");
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest,
                       f->smallest_seqno, f->largest_seqno);
    status = versions_->LogAndApply(c->edit(), &mutex_, db_directory_.get());
    InstallSuperVersion(deletion_state);
    Version::LevelSummaryStorage tmp;
    LogToBuffer(log_buffer, "Moved #%lld to level-%d %lld bytes %s: %s\n",
                static_cast<unsigned long long>(f->number), c->level() + 1,
                static_cast<unsigned long long>(f->file_size),
                status.ToString().c_str(),
                versions_->current()->LevelSummary(&tmp));
    versions_->ReleaseCompactionFiles(c.get(), status);
    *madeProgress = true;
  } else {
    MaybeScheduleFlushOrCompaction(); // do more compaction work in parallel.
    CompactionState* compact = new CompactionState(c.get());
    status = DoCompactionWork(compact, deletion_state, log_buffer);
    CleanupCompaction(compact, status);
    versions_->ReleaseCompactionFiles(c.get(), status);
    c->ReleaseInputs();
    *madeProgress = true;
  }
  c.reset();

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(WARN, options_.info_log, "Compaction error: %s",
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
      // Universal compaction should always compact the whole range
      assert(options_.compaction_style != kCompactionStyleUniversal);
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
      table_cache_->Evict(out.number);
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
    pending_outputs_.insert(file_number);
    compact->allocated_file_numbers.push_back(file_number);
  }
}

// Frees up unused file number.
void DBImpl::ReleaseCompactionUnusedFileNumbers(CompactionState* compact) {
  mutex_.AssertHeld();
  for (const auto file_number : compact->allocated_file_numbers) {
    pending_outputs_.erase(file_number);
    // Log(options_.info_log, "XXX releasing unused file num %d", file_number);
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
    pending_outputs_.insert(file_number);
    mutex_.Unlock();
  }
  CompactionState::Output out;
  out.number = file_number;
  out.smallest.Clear();
  out.largest.Clear();
  out.smallest_seqno = out.largest_seqno = 0;
  compact->outputs.push_back(out);

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile, storage_options_);

  if (s.ok()) {
    // Over-estimate slightly so we don't end up just barely crossing
    // the threshold.
    compact->outfile->SetPreallocationBlockSize(
      1.1 * versions_->MaxFileSizeForLevel(compact->compaction->output_level()));

    CompressionType compression_type = GetCompressionType(
        options_, compact->compaction->output_level(),
        compact->compaction->enable_compression());

    compact->builder.reset(NewTableBuilder(options_, internal_comparator_,
                                           compact->outfile.get(),
                                           compression_type));
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
      StopWatch sw(env_, options_.statistics.get(),
                   COMPACTION_OUTFILE_SYNC_MICROS, false);
      s = compact->outfile->Fsync();
    } else {
      StopWatch sw(env_, options_.statistics.get(),
                   COMPACTION_OUTFILE_SYNC_MICROS, false);
      s = compact->outfile->Sync();
    }
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  compact->outfile.reset();

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    FileMetaData meta(output_number, current_bytes);
    Iterator* iter = table_cache_->NewIterator(ReadOptions(), storage_options_,
                                               internal_comparator_, meta);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%lu: %lu keys, %lu bytes",
          (unsigned long) output_number,
          (unsigned long) current_entries,
          (unsigned long) current_bytes);
    }
  }
  return s;
}


Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();

  // paranoia: verify that the files that we started with
  // still exist in the current version and in the same original level.
  // This ensures that a concurrent compaction did not erroneously
  // pick the same files to compact.
  if (!versions_->VerifyCompactionFileConsistency(compact->compaction)) {
    Log(options_.info_log,  "Compaction %d@%d + %d@%d files aborted",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);
    return Status::Corruption("Compaction input files inconsistent");
  }

  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        compact->compaction->output_level(), out.number, out.file_size,
        out.smallest, out.largest, out.smallest_seqno, out.largest_seqno);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_,
                                db_directory_.get());
}

//
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
      "Looking for seqid %lu but maxseqid is %lu",
      (unsigned long)in,
      (unsigned long)snapshots[snapshots.size()-1]);
  assert(0);
  return 0;
}

Status DBImpl::ProcessKeyValueCompaction(
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
  std::string current_user_key;
  bool has_current_user_key = false;
  std::vector<char> delete_key;   // for compaction filter
  SequenceNumber last_sequence_for_key __attribute__((unused)) =
    kMaxSequenceNumber;
  SequenceNumber visible_in_snapshot = kMaxSequenceNumber;
  MergeHelper merge(user_comparator(), options_.merge_operator.get(),
                    options_.info_log.get(),
                    options_.min_partial_merge_operands,
                    false /* internal key corruption is expected */);
  auto compaction_filter = options_.compaction_filter;
  std::unique_ptr<CompactionFilter> compaction_filter_from_factory = nullptr;
  if (!compaction_filter) {
    auto context = compact->GetFilterContext();
    compaction_filter_from_factory =
      options_.compaction_filter_factory->CreateCompactionFilter(context);
    compaction_filter = compaction_filter_from_factory.get();
  }

  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    // TODO: remove memtable flush from normal compaction work
    if (imm_.imm_flush_needed.NoBarrier_Load() != nullptr) {
      const uint64_t imm_start = env_->NowMicros();
      LogFlush(options_.info_log);
      mutex_.Lock();
      if (imm_.IsFlushPending()) {
        FlushMemTableToOutputFile(nullptr, deletion_state, log_buffer);
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
      log_buffer->FlushBufferToLog();
      imm_micros += (env_->NowMicros() - imm_start);
    }

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
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
      visible_in_snapshot = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
            Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
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
          bool to_delete =
            compaction_filter->Filter(compact->compaction->level(),
                ikey.user_key, value,
                &compaction_filter_value,
                &value_changed);
          if (to_delete) {
            // make a copy of the original key
            delete_key.assign(key.data(), key.data() + key.size());
            // convert it to a delete
            UpdateInternalKey(&delete_key[0], delete_key.size(),
                ikey.sequence, kTypeDeletion);
            // anchor the key again
            key = Slice(&delete_key[0], delete_key.size());
            // needed because ikey is backed by key
            ParseInternalKey(key, &ikey);
            // no value associated with delete
            value.clear();
            RecordTick(options_.statistics.get(), COMPACTION_KEY_DROP_USER);
          } else if (value_changed) {
            value = compaction_filter_value;
          }
        }

      }

      // If there are no snapshots, then this kv affect visibility at tip.
      // Otherwise, search though all existing snapshots to find
      // the earlist snapshot that is affected by this kv.
      SequenceNumber prev_snapshot = 0; // 0 means no previous snapshot
      SequenceNumber visible = visible_at_tip ?
        visible_at_tip :
        findEarliestVisibleSnapshot(ikey.sequence,
            compact->existing_snapshots,
            &prev_snapshot);

      if (visible_in_snapshot == visible) {
        // If the earliest snapshot is which this key is visible in
        // is the same as the visibily of a previous instance of the
        // same key, then this kv is not visible in any snapshot.
        // Hidden by an newer entry for same user key
        // TODO: why not > ?
        assert(last_sequence_for_key >= ikey.sequence);
        drop = true;    // (A)
        RecordTick(options_.statistics.get(), COMPACTION_KEY_DROP_NEWER_ENTRY);
      } else if (ikey.type == kTypeDeletion &&
          ikey.sequence <= earliest_snapshot &&
          compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
        RecordTick(options_.statistics.get(), COMPACTION_KEY_DROP_OBSOLETE);
      } else if (ikey.type == kTypeMerge) {
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
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d level: %d bottommost %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)earliest_snapshot,
        compact->compaction->level(), bottommost_level);
#endif

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

  return status;
}

void DBImpl::CallCompactionFilterV2(CompactionState* compact,
  CompactionFilterV2* compaction_filter_v2) {
  if (compact == nullptr || compaction_filter_v2 == nullptr) {
    return;
  }

  std::vector<Slice> user_key_buf;
  for (const auto& key : compact->ikey_buf_) {
    user_key_buf.emplace_back(key.user_key);
  }

  // If the user has specified a compaction filter and the sequence
  // number is greater than any external snapshot, then invoke the
  // filter.
  // If the return value of the compaction filter is true, replace
  // the entry with a delete marker.
  compact->to_delete_buf_ = compaction_filter_v2->Filter(
      compact->compaction->level(),
      user_key_buf, compact->existing_value_buf_,
      &compact->new_value_buf_,
      &compact->value_changed_buf_);

  // new_value_buf_.size() <= to_delete__buf_.size(). "=" iff all
  // kv-pairs in this compaction run needs to be deleted.
  assert(compact->to_delete_buf_.size() ==
      compact->key_buf_.size());
  assert(compact->to_delete_buf_.size() ==
      compact->existing_value_buf_.size());
  assert(compact->to_delete_buf_.size() ==
      compact->value_changed_buf_.size());

  int new_value_idx = 0;
  for (unsigned int i = 0; i < compact->to_delete_buf_.size(); ++i) {
    if (compact->to_delete_buf_[i]) {
      // update the string buffer directly
      // the Slice buffer points to the updated buffer
      UpdateInternalKey(&compact->key_str_buf_[i][0],
          compact->key_str_buf_[i].size(),
          compact->ikey_buf_[i].sequence,
          kTypeDeletion);

      // no value associated with delete
      compact->existing_value_buf_[i].clear();
      RecordTick(options_.statistics.get(), COMPACTION_KEY_DROP_USER);
    } else if (compact->value_changed_buf_[i]) {
      compact->existing_value_buf_[i] =
        Slice(compact->new_value_buf_[new_value_idx++]);
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

  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions
  Log(options_.info_log,
      "Compacting %d@%d + %d@%d files, score %.2f slots available %d",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->output_level(),
      compact->compaction->score(),
      options_.max_background_compactions - bg_compaction_scheduled_);
  char scratch[2345];
  compact->compaction->Summary(scratch, sizeof(scratch));
  Log(options_.info_log, "Compaction start summary: %s\n", scratch);

  assert(versions_->current()->NumLevelFiles(compact->compaction->level()) > 0);
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

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

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
    options_.compaction_filter_factory_v2->CreateCompactionFilterV2(context);
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
    for (; backup_input->Valid() && !shutting_down_.Acquire_Load(); ) {
      // Prioritize immutable compaction work
      if (imm_.imm_flush_needed.NoBarrier_Load() != nullptr) {
        const uint64_t imm_start = env_->NowMicros();
        LogFlush(options_.info_log);
        mutex_.Lock();
        if (imm_.IsFlushPending()) {
          FlushMemTableToOutputFile(nullptr, deletion_state, log_buffer);
          bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
        }
        mutex_.Unlock();
        imm_micros += (env_->NowMicros() - imm_start);
      }

      Slice key = backup_input->key();
      Slice value = backup_input->value();

      const SliceTransform* transformer =
        options_.compaction_filter_factory_v2->GetPrefixExtractor();
      std::string key_prefix = transformer->Transform(key).ToString();
      if (!prefix_initialized) {
        compact->cur_prefix_ = key_prefix;
        prefix_initialized = true;
      }
      if (!ParseInternalKey(key, &ikey)) {
        // log error
        Log(options_.info_log, "Failed to parse key: %s",
            key.ToString().c_str());
        continue;
      } else {
        // If the prefix remains the same, keep buffering
        if (key_prefix == compact->cur_prefix_) {
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
          if (compact->key_buf_.size() > 0) {
            CallCompactionFilterV2(compact, compaction_filter_v2);
          }
          compact->cur_prefix_ = key_prefix;
        }
      }

      // Merge this batch of data (values + ineligible keys)
      compact->MergeKeyValueSliceBuffer(&internal_comparator_);

      // Done buffering for the current prefix. Spit it out to disk
      // Now just iterate through all the kv-pairs
      status = ProcessKeyValueCompaction(
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
        if (compact->key_buf_.size() > 0) {
          CallCompactionFilterV2(compact, compaction_filter_v2);
        }
        compact->MergeKeyValueSliceBuffer(&internal_comparator_);

        status = ProcessKeyValueCompaction(
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
    if (compact->key_buf_.size() > 0) {
      CallCompactionFilterV2(compact, compaction_filter_v2);
    }
    compact->MergeKeyValueSliceBuffer(&internal_comparator_);
    status = ProcessKeyValueCompaction(
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

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::ShutdownInProgress(
        "Database shutdown started during compaction");
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

  InternalStats::CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  MeasureTime(options_.statistics.get(), COMPACTION_TIME, stats.micros);
  stats.files_in_leveln = compact->compaction->num_input_files(0);
  stats.files_in_levelnp1 = compact->compaction->num_input_files(1);

  int num_output_files = compact->outputs.size();
  if (compact->builder != nullptr) {
    // An error occurred so ignore the last output.
    assert(num_output_files > 0);
    --num_output_files;
  }
  stats.files_out_levelnp1 = num_output_files;

  for (int i = 0; i < compact->compaction->num_input_files(0); i++) {
    stats.bytes_readn += compact->compaction->input(0, i)->file_size;
    RecordTick(options_.statistics.get(), COMPACT_READ_BYTES,
               compact->compaction->input(0, i)->file_size);
  }

  for (int i = 0; i < compact->compaction->num_input_files(1); i++) {
    stats.bytes_readnp1 += compact->compaction->input(1, i)->file_size;
    RecordTick(options_.statistics.get(), COMPACT_READ_BYTES,
               compact->compaction->input(1, i)->file_size);
  }

  for (int i = 0; i < num_output_files; i++) {
    stats.bytes_written += compact->outputs[i].file_size;
    RecordTick(options_.statistics.get(), COMPACT_WRITE_BYTES,
               compact->outputs[i].file_size);
  }

  LogFlush(options_.info_log);
  mutex_.Lock();
  internal_stats_.AddCompactionStats(compact->compaction->output_level(),
                                     stats);

  // if there were any unused file number (mostly in case of
  // compaction error), free up the entry from pending_putputs
  ReleaseCompactionUnusedFileNumbers(compact);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
    InstallSuperVersion(deletion_state);
  }
  Version::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s, %.1f MB/sec, level %d, files in(%d, %d) out(%d) "
      "MB in(%.1f, %.1f) out(%.1f), read-write-amplify(%.1f) "
      "write-amplify(%.1f) %s\n",
      versions_->current()->LevelSummary(&tmp),
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
  IterState(DBImpl* db, port::Mutex* mu, DBImpl::SuperVersion* super_version)
    : db(db), mu(mu), super_version(super_version) {}

  DBImpl* db;
  port::Mutex* mu;
  DBImpl::SuperVersion* super_version;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);

  bool need_cleanup = state->super_version->Unref();
  if (need_cleanup) {
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
                                      SequenceNumber* latest_snapshot) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();
  SuperVersion* super_version = super_version_->Ref();
  mutex_.Unlock();

  std::vector<Iterator*> iterator_list;
  // Collect iterator for mutable mem
  iterator_list.push_back(super_version->mem->NewIterator(options));
  // Collect all needed child iterators for immutable memtables
  super_version->imm->AddIterators(options, &iterator_list);
  // Collect iterators for files in L0 - Ln
  super_version->current->AddIterators(options, storage_options_,
                                       &iterator_list);
  Iterator* internal_iter = NewMergingIterator(
      env_, &internal_comparator_, &iterator_list[0], iterator_list.size());

  IterState* cleanup = new IterState(this, &mutex_, super_version);
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  ReadOptions read_options;
  // Use prefix_seek to make the test function more useful.
  read_options.prefix_seek = true;
  return NewInternalIterator(read_options, &ignored);
}

std::pair<Iterator*, Iterator*> DBImpl::GetTailingIteratorPair(
    const ReadOptions& options,
    uint64_t* superversion_number) {

  mutex_.Lock();
  SuperVersion* super_version = super_version_->Ref();
  if (superversion_number != nullptr) {
    *superversion_number = CurrentVersionNumber();
  }
  mutex_.Unlock();

  Iterator* mutable_iter = super_version->mem->NewIterator(options);
  // create a DBIter that only uses memtable content; see NewIterator()
  mutable_iter = NewDBIterator(&dbname_, env_, options_, user_comparator(),
                               mutable_iter, kMaxSequenceNumber);

  std::vector<Iterator*> list;
  super_version->imm->AddIterators(options, &list);
  super_version->current->AddIterators(options, storage_options_, &list);
  Iterator* immutable_iter =
    NewMergingIterator(env_, &internal_comparator_, &list[0], list.size());

  // create a DBIter that only uses memtable content; see NewIterator()
  immutable_iter = NewDBIterator(&dbname_, env_, options_, user_comparator(),
                                 immutable_iter, kMaxSequenceNumber);

  // register cleanups
  mutable_iter->RegisterCleanup(CleanupIteratorState,
    new IterState(this, &mutex_, super_version), nullptr);

  // bump the ref one more time since it will be Unref'ed twice
  immutable_iter->RegisterCleanup(CleanupIteratorState,
    new IterState(this, &mutex_, super_version->Ref()), nullptr);

  return std::make_pair(mutable_iter, immutable_iter);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->current()->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  return GetImpl(options, key, value);
}

// DeletionState gets created and destructed outside of the lock -- we
// use this convinently to:
// * malloc one SuperVersion() outside of the lock -- new_superversion
// * delete SuperVersion()s outside of the lock -- superversions_to_free
//
// However, if InstallSuperVersion() gets called twice with the same,
// deletion_state, we can't reuse the SuperVersion() that got malloced because
// first call already used it. In that rare case, we take a hit and create a
// new SuperVersion() inside of the mutex.
void DBImpl::InstallSuperVersion(DeletionState& deletion_state) {
  mutex_.AssertHeld();
  // if new_superversion == nullptr, it means somebody already used it
  SuperVersion* new_superversion =
    (deletion_state.new_superversion != nullptr) ?
    deletion_state.new_superversion : new SuperVersion();
  SuperVersion* old_superversion = InstallSuperVersion(new_superversion);
  deletion_state.new_superversion = nullptr;
  deletion_state.superversions_to_free.push_back(old_superversion);
  // Reset SuperVersions cached in thread local storage
  if (options_.allow_thread_local) {
    ResetThreadLocalSuperVersions(&deletion_state);
  }
}

DBImpl::SuperVersion* DBImpl::InstallSuperVersion(
    SuperVersion* new_superversion) {
  mutex_.AssertHeld();
  new_superversion->db = this;
  new_superversion->Init(mem_, imm_.current(), versions_->current());
  SuperVersion* old_superversion = super_version_;
  super_version_ = new_superversion;
  ++super_version_number_;
  super_version_->version_number = super_version_number_;

  if (old_superversion != nullptr && old_superversion->Unref()) {
    old_superversion->Cleanup();
    return old_superversion; // will let caller delete outside of mutex
  }
  return nullptr;
}

void DBImpl::ResetThreadLocalSuperVersions(DeletionState* deletion_state) {
  mutex_.AssertHeld();
  autovector<void*> sv_ptrs;
  local_sv_->Scrape(&sv_ptrs, SuperVersion::kSVObsolete);
  for (auto ptr : sv_ptrs) {
    assert(ptr);
    if (ptr == SuperVersion::kSVInUse) {
      continue;
    }
    auto sv = static_cast<SuperVersion*>(ptr);
    if (static_cast<SuperVersion*>(ptr)->Unref()) {
      sv->Cleanup();
      deletion_state->superversions_to_free.push_back(sv);
    }
  }
}

Status DBImpl::GetImpl(const ReadOptions& options,
                       const Slice& key,
                       std::string* value,
                       bool* value_found) {
  Status s;

  StopWatch sw(env_, options_.statistics.get(), DB_GET, false);
  StopWatchNano snapshot_timer(env_, false);
  StartPerfTimer(&snapshot_timer);
  SequenceNumber snapshot;

  if (options.snapshot != nullptr) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  // Acquire SuperVersion
  SuperVersion* sv = nullptr;
  if (LIKELY(options_.allow_thread_local)) {
    // The SuperVersion is cached in thread local storage to avoid acquiring
    // mutex when SuperVersion does not change since the last use. When a new
    // SuperVersion is installed, the compaction or flush thread cleans up
    // cached SuperVersion in all existing thread local storage. To avoid
    // acquiring mutex for this operation, we use atomic Swap() on the thread
    // local pointer to guarantee exclusive access. If the thread local pointer
    // is being used while a new SuperVersion is installed, the cached
    // SuperVersion can become stale. In that case, the background thread would
    // have swapped in kSVObsolete. We re-check the value at the end of
    // Get, with an atomic compare and swap. The superversion will be released
    // if detected to be stale.
    void* ptr = local_sv_->Swap(SuperVersion::kSVInUse);
    // Invariant:
    // (1) Scrape (always) installs kSVObsolete in ThreadLocal storage
    // (2) the Swap above (always) installs kSVInUse, ThreadLocal storage
    // should only keep kSVInUse during a GetImpl.
    assert(ptr != SuperVersion::kSVInUse);
    sv = static_cast<SuperVersion*>(ptr);
    if (sv == SuperVersion::kSVObsolete ||
        sv->version_number != super_version_number_.load(
          std::memory_order_relaxed)) {
      RecordTick(options_.statistics.get(), NUMBER_SUPERVERSION_ACQUIRES);
      SuperVersion* sv_to_delete = nullptr;

      if (sv && sv->Unref()) {
        RecordTick(options_.statistics.get(), NUMBER_SUPERVERSION_CLEANUPS);
        mutex_.Lock();
        // TODO underlying resources held by superversion (sst files) might
        // not be released until the next background job.
        sv->Cleanup();
        sv_to_delete = sv;
      } else {
        mutex_.Lock();
      }
      sv = super_version_->Ref();
      mutex_.Unlock();

      delete sv_to_delete;
    }
  } else {
    mutex_.Lock();
    sv = super_version_->Ref();
    mutex_.Unlock();
  }

  bool have_stat_update = false;
  Version::GetStats stats;

  // Prepare to store a list of merge operations if merge occurs.
  MergeContext merge_context;

  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  LookupKey lkey(key, snapshot);
  BumpPerfTime(&perf_context.get_snapshot_time, &snapshot_timer);
  if (sv->mem->Get(lkey, value, &s, merge_context, options_)) {
    // Done
    RecordTick(options_.statistics.get(), MEMTABLE_HIT);
  } else if (sv->imm->Get(lkey, value, &s, merge_context, options_)) {
    // Done
    RecordTick(options_.statistics.get(), MEMTABLE_HIT);
  } else {
    StopWatchNano from_files_timer(env_, false);
    StartPerfTimer(&from_files_timer);

    sv->current->Get(options, lkey, value, &s, &merge_context, &stats,
                     options_, value_found);
    have_stat_update = true;
    BumpPerfTime(&perf_context.get_from_output_files_time, &from_files_timer);
    RecordTick(options_.statistics.get(), MEMTABLE_MISS);
  }

  StopWatchNano post_process_timer(env_, false);
  StartPerfTimer(&post_process_timer);

  if (!options_.disable_seek_compaction && have_stat_update) {
    mutex_.Lock();
    if (sv->current->UpdateStats(stats)) {
      MaybeScheduleFlushOrCompaction();
    }
    mutex_.Unlock();
  }

  bool unref_sv = true;
  if (LIKELY(options_.allow_thread_local)) {
    // Put the SuperVersion back
    void* expected = SuperVersion::kSVInUse;
    if (local_sv_->CompareAndSwap(static_cast<void*>(sv), expected)) {
      // When we see kSVInUse in the ThreadLocal, we are sure ThreadLocal
      // storage has not been altered and no Scrape has happend. The
      // SuperVersion is still current.
      unref_sv = false;
    } else {
      // ThreadLocal scrape happened in the process of this GetImpl call (after
      // thread local Swap() at the beginning and before CompareAndSwap()).
      // This means the SuperVersion it holds is obsolete.
      assert(expected == SuperVersion::kSVObsolete);
    }
  }

  if (unref_sv) {
    // Release SuperVersion
    if (sv->Unref()) {
      mutex_.Lock();
      sv->Cleanup();
      mutex_.Unlock();
      delete sv;
      RecordTick(options_.statistics.get(), NUMBER_SUPERVERSION_CLEANUPS);
    }
    RecordTick(options_.statistics.get(), NUMBER_SUPERVERSION_RELEASES);
  }

  // Note, tickers are atomic now - no lock protection needed any more.
  RecordTick(options_.statistics.get(), NUMBER_KEYS_READ);
  RecordTick(options_.statistics.get(), BYTES_READ, value->size());
  BumpPerfTime(&perf_context.get_post_process_time, &post_process_timer);
  return s;
}

std::vector<Status> DBImpl::MultiGet(const ReadOptions& options,
                                     const std::vector<Slice>& keys,
                                     std::vector<std::string>* values) {
  StopWatch sw(env_, options_.statistics.get(), DB_MULTIGET, false);
  StopWatchNano snapshot_timer(env_, false);
  StartPerfTimer(&snapshot_timer);

  SequenceNumber snapshot;

  mutex_.Lock();
  if (options.snapshot != nullptr) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  SuperVersion* get_version = super_version_->Ref();
  mutex_.Unlock();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Contain a list of merge operations if merge occurs.
  MergeContext merge_context;

  // Note: this always resizes the values array
  int numKeys = keys.size();
  std::vector<Status> statList(numKeys);
  values->resize(numKeys);

  // Keep track of bytes that we read for statistics-recording later
  uint64_t bytesRead = 0;
  BumpPerfTime(&perf_context.get_snapshot_time, &snapshot_timer);

  // For each of the given keys, apply the entire "get" process as follows:
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  for (int i=0; i<numKeys; ++i) {
    merge_context.Clear();
    Status& s = statList[i];
    std::string* value = &(*values)[i];

    LookupKey lkey(keys[i], snapshot);
    if (get_version->mem->Get(lkey, value, &s, merge_context, options_)) {
      // Done
    } else if (get_version->imm->Get(lkey, value, &s, merge_context,
                                     options_)) {
      // Done
    } else {
      get_version->current->Get(options, lkey, value, &s, &merge_context,
                                &stats, options_);
      have_stat_update = true;
    }

    if (s.ok()) {
      bytesRead += value->size();
    }
  }

  // Post processing (decrement reference counts and record statistics)
  StopWatchNano post_process_timer(env_, false);
  StartPerfTimer(&post_process_timer);
  bool delete_get_version = false;
  if (!options_.disable_seek_compaction && have_stat_update) {
    mutex_.Lock();
    if (get_version->current->UpdateStats(stats)) {
      MaybeScheduleFlushOrCompaction();
    }
    if (get_version->Unref()) {
      get_version->Cleanup();
      delete_get_version = true;
    }
    mutex_.Unlock();
  } else {
    if (get_version->Unref()) {
      mutex_.Lock();
      get_version->Cleanup();
      mutex_.Unlock();
      delete_get_version = true;
    }
  }
  if (delete_get_version) {
    delete get_version;
  }

  RecordTick(options_.statistics.get(), NUMBER_MULTIGET_CALLS);
  RecordTick(options_.statistics.get(), NUMBER_MULTIGET_KEYS_READ, numKeys);
  RecordTick(options_.statistics.get(), NUMBER_MULTIGET_BYTES_READ, bytesRead);
  BumpPerfTime(&perf_context.get_post_process_time, &post_process_timer);

  return statList;
}

bool DBImpl::KeyMayExist(const ReadOptions& options,
                         const Slice& key,
                         std::string* value,
                         bool* value_found) {
  if (value_found != nullptr) {
    // falsify later if key-may-exist but can't fetch value
    *value_found = true;
  }
  ReadOptions roptions = options;
  roptions.read_tier = kBlockCacheTier; // read from block cache only
  auto s = GetImpl(roptions, key, value, value_found);

  // If options.block_cache != nullptr and the index block of the table didn't
  // not present in block_cache, the return value will be Status::Incomplete.
  // In this case, key may still exist in the table.
  return s.ok() || s.IsIncomplete();
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  Iterator* iter;

  if (options.tailing) {
    iter = new TailingIterator(this, options, user_comparator());
  } else {
    SequenceNumber latest_snapshot;
    iter = NewInternalIterator(options, &latest_snapshot);

    iter = NewDBIterator(
      &dbname_, env_, options_, user_comparator(), iter,
      (options.snapshot != nullptr
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot));
  }

  if (options.prefix) {
    // use extra wrapper to exclude any keys from the results which
    // don't begin with the prefix
    iter = new PrefixFilterIterator(iter, *options.prefix,
                                    options_.prefix_extractor.get());
  }
  return iter;
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Merge(const WriteOptions& o, const Slice& key,
                     const Slice& val) {
  if (!options_.merge_operator) {
    return Status::NotSupported("Provide a merge_operator when opening DB");
  } else {
    return DB::Merge(o, key, val);
  }
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  StopWatchNano pre_post_process_timer(env_, false);
  StartPerfTimer(&pre_post_process_timer);
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.disableWAL = options.disableWAL;
  w.done = false;

  StopWatch sw(env_, options_.statistics.get(), DB_WRITE, false);
  mutex_.Lock();
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }

  if (!options.disableWAL) {
    RecordTick(options_.statistics.get(), WRITE_WITH_WAL, 1);
  }

  if (w.done) {
    mutex_.Unlock();
    RecordTick(options_.statistics.get(), WRITE_DONE_BY_OTHER, 1);
    return w.status;
  } else {
    RecordTick(options_.statistics.get(), WRITE_DONE_BY_SELF, 1);
  }

  // May temporarily unlock and wait.
  SuperVersion* superversion_to_free = nullptr;
  Status status = MakeRoomForWrite(my_batch == nullptr, &superversion_to_free);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != nullptr) {  // nullptr batch is for compactions
    autovector<WriteBatch*> write_batch_group;
    BuildBatchGroup(&last_writer, &write_batch_group);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
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
      // Record statistics
      RecordTick(options_.statistics.get(),
                 NUMBER_KEYS_WRITTEN, my_batch_count);
      RecordTick(options_.statistics.get(),
                 BYTES_WRITTEN,
                 WriteBatchInternal::ByteSize(updates));
      if (options.disableWAL) {
        flush_on_destroy_ = true;
      }
      BumpPerfTime(&perf_context.write_pre_and_post_process_time,
                   &pre_post_process_timer);

      if (!options.disableWAL) {
        StopWatchNano timer(env_);
        StartPerfTimer(&timer);
        Slice log_entry = WriteBatchInternal::Contents(updates);
        status = log_->AddRecord(log_entry);
        RecordTick(options_.statistics.get(), WAL_FILE_SYNCED, 1);
        RecordTick(options_.statistics.get(), WAL_FILE_BYTES, log_entry.size());
        if (status.ok() && options.sync) {
          if (options_.use_fsync) {
            StopWatch(env_, options_.statistics.get(), WAL_FILE_SYNC_MICROS);
            status = log_->file()->Fsync();
          } else {
            StopWatch(env_, options_.statistics.get(), WAL_FILE_SYNC_MICROS);
            status = log_->file()->Sync();
          }
        }
        BumpPerfTime(&perf_context.write_wal_time, &timer);
      }
      if (status.ok()) {
        StopWatchNano write_memtable_timer(env_, false);
        StartPerfTimer(&write_memtable_timer);
        status = WriteBatchInternal::InsertInto(updates, mem_, &options_, this,
                                                options_.filter_deletes);
        BumpPerfTime(&perf_context.write_memtable_time, &write_memtable_timer);
        if (!status.ok()) {
          // Panic for in-memory corruptions
          // Note that existing logic was not sound. Any partial failure writing
          // into the memtable would result in a state that some write ops might
          // have succeeded in memtable but Status reports error for all writes.
          throw std::runtime_error("In memory WriteBatch corruption!");
        }
        SetTickerCount(options_.statistics.get(), SEQUENCE_NUMBER,
                       last_sequence);
      }
      StartPerfTimer(&pre_post_process_timer);
      if (updates == &tmp_batch_) tmp_batch_.Clear();
      mutex_.Lock();
      if (status.ok()) {
        versions_->SetLastSequence(last_sequence);
      }
    }
  }
  if (options_.paranoid_checks && !status.ok() && bg_error_.ok()) {
    bg_error_ = status; // stop compaction & fail any further writes
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
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
  mutex_.Unlock();
  delete superversion_to_free;
  BumpPerfTime(&perf_context.write_pre_and_post_process_time,
               &pre_post_process_timer);
  return status;
}

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

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      write_batch_group->push_back(w->batch);
    }
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
Status DBImpl::MakeRoomForWrite(bool force,
                                SuperVersion** superversion_to_free) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  bool allow_hard_rate_limit_delay = !force;
  bool allow_soft_rate_limit_delay = !force;
  uint64_t rate_limit_delay_millis = 0;
  Status s;
  double score;
  *superversion_to_free = nullptr;

  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NeedSlowdownForNumLevel0Files()) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 0-1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      uint64_t slowdown =
          SlowdownAmount(versions_->current()->NumLevelFiles(0),
                         options_.level0_slowdown_writes_trigger,
                         options_.level0_stop_writes_trigger);
      mutex_.Unlock();
      uint64_t delayed;
      {
        StopWatch sw(env_, options_.statistics.get(), STALL_L0_SLOWDOWN_COUNT);
        env_->SleepForMicroseconds(slowdown);
        delayed = sw.ElapsedMicros();
      }
      RecordTick(options_.statistics.get(), STALL_L0_SLOWDOWN_MICROS, delayed);
      internal_stats_.RecordWriteStall(InternalStats::LEVEL0_SLOWDOWN, delayed);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
      delayed_writes_++;
    } else if (!force && !mem_->ShouldFlush()) {
      // There is room in current memtable
      if (allow_delay) {
        DelayLoggingAndReset();
      }
      break;
    } else if (imm_.size() == options_.max_write_buffer_number - 1) {
      // We have filled up the current memtable, but the previous
      // ones are still being flushed, so we wait.
      DelayLoggingAndReset();
      Log(options_.info_log, "wait for memtable flush...\n");
      MaybeScheduleFlushOrCompaction();
      uint64_t stall;
      {
        StopWatch sw(env_, options_.statistics.get(),
          STALL_MEMTABLE_COMPACTION_COUNT);
        bg_cv_.Wait();
        stall = sw.ElapsedMicros();
      }
      RecordTick(options_.statistics.get(),
                 STALL_MEMTABLE_COMPACTION_MICROS, stall);
      internal_stats_.RecordWriteStall(InternalStats::MEMTABLE_COMPACTION,
                                       stall);
    } else if (versions_->current()->NumLevelFiles(0) >=
               options_.level0_stop_writes_trigger) {
      // There are too many level-0 files.
      DelayLoggingAndReset();
      Log(options_.info_log, "wait for fewer level0 files...\n");
      uint64_t stall;
      {
        StopWatch sw(env_, options_.statistics.get(),
                     STALL_L0_NUM_FILES_COUNT);
        bg_cv_.Wait();
        stall = sw.ElapsedMicros();
      }
      RecordTick(options_.statistics.get(), STALL_L0_NUM_FILES_MICROS, stall);
      internal_stats_.RecordWriteStall(InternalStats::LEVEL0_NUM_FILES, stall);
    } else if (allow_hard_rate_limit_delay && options_.hard_rate_limit > 1.0 &&
               (score = versions_->current()->MaxCompactionScore()) >
                   options_.hard_rate_limit) {
      // Delay a write when the compaction score for any level is too large.
      int max_level = versions_->current()->MaxCompactionScoreLevel();
      mutex_.Unlock();
      uint64_t delayed;
      {
        StopWatch sw(env_, options_.statistics.get(),
                     HARD_RATE_LIMIT_DELAY_COUNT);
        env_->SleepForMicroseconds(1000);
        delayed = sw.ElapsedMicros();
      }
      internal_stats_.RecordLevelNSlowdown(max_level, delayed);
      // Make sure the following value doesn't round to zero.
      uint64_t rate_limit = std::max((delayed / 1000), (uint64_t) 1);
      rate_limit_delay_millis += rate_limit;
      RecordTick(options_.statistics.get(),
                 RATE_LIMIT_DELAY_MILLIS, rate_limit);
      if (options_.rate_limit_delay_max_milliseconds > 0 &&
          rate_limit_delay_millis >=
          (unsigned)options_.rate_limit_delay_max_milliseconds) {
        allow_hard_rate_limit_delay = false;
      }
      mutex_.Lock();
    } else if (allow_soft_rate_limit_delay && options_.soft_rate_limit > 0.0 &&
               (score = versions_->current()->MaxCompactionScore()) >
                   options_.soft_rate_limit) {
      // Delay a write when the compaction score for any level is too large.
      // TODO: add statistics
      mutex_.Unlock();
      {
        StopWatch sw(env_, options_.statistics.get(),
                     SOFT_RATE_LIMIT_DELAY_COUNT);
        env_->SleepForMicroseconds(SlowdownAmount(
          score,
          options_.soft_rate_limit,
          options_.hard_rate_limit)
        );
        rate_limit_delay_millis += sw.ElapsedMicros();
      }
      allow_soft_rate_limit_delay = false;
      mutex_.Lock();

    } else {
      unique_ptr<WritableFile> lfile;
      MemTable* new_mem = nullptr;

      // Attempt to switch to a new memtable and trigger flush of old.
      // Do this without holding the dbmutex lock.
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      SuperVersion* new_superversion = nullptr;
      mutex_.Unlock();
      {
        DelayLoggingAndReset();
        s = env_->NewWritableFile(LogFileName(options_.wal_dir, new_log_number),
                                  &lfile,
                                  env_->OptimizeForLogWrite(storage_options_));
        if (s.ok()) {
          // Our final size should be less than write_buffer_size
          // (compression, etc) but err on the side of caution.
          lfile->SetPreallocationBlockSize(1.1 * options_.write_buffer_size);
          new_mem = new MemTable(internal_comparator_, options_);
          new_superversion = new SuperVersion();
        }
      }
      mutex_.Lock();
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        assert (!new_mem);
        break;
      }
      logfile_number_ = new_log_number;
      log_.reset(new log::Writer(std::move(lfile)));
      mem_->SetNextLogNumber(logfile_number_);
      imm_.Add(mem_);
      if (force) {
        imm_.FlushRequested();
      }
      mem_ = new_mem;
      mem_->Ref();
      Log(options_.info_log,
          "New memtable created with log file: #%lu\n",
          (unsigned long)logfile_number_);
      mem_->SetLogNumber(logfile_number_);
      force = false;   // Do not force another compaction if have room
      MaybeScheduleFlushOrCompaction();
      *superversion_to_free = InstallSuperVersion(new_superversion);
    }
  }
  return s;
}

Status DBImpl::GetPropertiesOfAllTables(TablePropertiesCollection* props) {
  // Increment the ref count
  mutex_.Lock();
  auto version = versions_->current();
  version->Ref();
  mutex_.Unlock();

  auto s = version->GetPropertiesOfAllTables(props);

  // Decrement the ref count
  mutex_.Lock();
  version->Unref();
  mutex_.Unlock();

  return s;
}

const std::string& DBImpl::GetName() const {
  return dbname_;
}

Env* DBImpl::GetEnv() const {
  return env_;
}

const Options& DBImpl::GetOptions() const {
  return options_;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();
  DBPropertyType property_type = GetPropertyType(property);
  MutexLock l(&mutex_);
  return internal_stats_.GetProperty(property_type, property, value, this);
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
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
  int maxlevel = NumberLevels();
  VersionEdit edit;
  DeletionState deletion_state(true);
  {
    MutexLock l(&mutex_);
    status = versions_->GetMetadataForFile(number, &level, &metadata);
    if (!status.ok()) {
      Log(options_.info_log, "DeleteFile %s failed. File not found\n",
                             name.c_str());
      return Status::InvalidArgument("File not found");
    }
    assert((level > 0) && (level < maxlevel));

    // If the file is being compacted no need to delete.
    if (metadata->being_compacted) {
      Log(options_.info_log,
          "DeleteFile %s Skipped. File about to be compacted\n", name.c_str());
      return Status::OK();
    }

    // Only the files in the last level can be deleted externally.
    // This is to make sure that any deletion tombstones are not
    // lost. Check that the level passed is the last level.
    for (int i = level + 1; i < maxlevel; i++) {
      if (versions_->current()->NumLevelFiles(i) != 0) {
        Log(options_.info_log,
            "DeleteFile %s FAILED. File not in last level\n", name.c_str());
        return Status::InvalidArgument("File not in last level");
      }
    }
    edit.DeleteFile(level, number);
    status = versions_->LogAndApply(&edit, &mutex_, db_directory_.get());
    if (status.ok()) {
      InstallSuperVersion(deletion_state);
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
  return versions_->GetLiveFilesMetaData(metadata);
}

Status DBImpl::CheckConsistency() {
  mutex_.AssertHeld();
  std::vector<LiveFileMetaData> metadata;
  versions_->GetLiveFilesMetaData(&metadata);

  std::string corruption_messages;
  for (const auto& md : metadata) {
    std::string file_path = dbname_ + md.name;
    uint64_t fsize = 0;
    Status s = env_->GetFileSize(file_path, &fsize);
    if (!s.ok()) {
      corruption_messages +=
          "Can't access " + md.name + ": " + s.ToString() + "\n";
    } else if (fsize != md.size) {
      corruption_messages += "Sst file size mismatch: " + md.name +
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

void DBImpl::TEST_GetFilesMetaData(
    std::vector<std::vector<FileMetaData>>* metadata) {
  MutexLock l(&mutex_);
  metadata->resize(NumberLevels());
  for (int level = 0; level < NumberLevels(); level++) {
    const std::vector<FileMetaData*>& files =
      versions_->current()->files_[level];

    (*metadata)[level].clear();
    for (const auto& f : files) {
      (*metadata)[level].push_back(*f);
    }
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
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  // Pre-allocate size of write batch conservatively.
  // 8 bytes are taken by header, 4 bytes for count, 1 byte for type,
  // and we allocate 11 extra bytes for key length, as well as value length.
  WriteBatch batch(key.size() + value.size() + 24);
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

Status DB::Merge(const WriteOptions& opt, const Slice& key,
                 const Slice& value) {
  WriteBatch batch;
  batch.Merge(key, value);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  if (options.block_cache != nullptr && options.no_block_cache) {
    return Status::InvalidArgument(
        "no_block_cache is true while block_cache is not nullptr");
  }

  DBImpl* impl = new DBImpl(options, dbname);
  Status s = impl->env_->CreateDirIfMissing(impl->options_.wal_dir);
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
  s = impl->Recover(); // Handles create_if_missing, error_if_exists
  if (s.ok()) {
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    unique_ptr<WritableFile> lfile;
    EnvOptions soptions(options);
    s = impl->options_.env->NewWritableFile(
        LogFileName(impl->options_.wal_dir, new_log_number), &lfile,
        impl->options_.env->OptimizeForLogWrite(soptions));
    if (s.ok()) {
      lfile->SetPreallocationBlockSize(1.1 * impl->options_.write_buffer_size);
      VersionEdit edit;
      edit.SetLogNumber(new_log_number);
      impl->logfile_number_ = new_log_number;
      impl->log_.reset(new log::Writer(std::move(lfile)));
      s = impl->versions_->LogAndApply(&edit, &impl->mutex_,
                                       impl->db_directory_.get());
    }
    if (s.ok()) {
      delete impl->InstallSuperVersion(new DBImpl::SuperVersion());
      impl->mem_->SetLogNumber(impl->logfile_number_);
      impl->DeleteObsoleteFiles();
      impl->MaybeScheduleFlushOrCompaction();
      impl->MaybeScheduleLogDBDeployStats();
      s = impl->db_directory_->Fsync();
    }
  }

  if (s.ok() && impl->options_.compaction_style == kCompactionStyleUniversal) {
    Version* current = impl->versions_->current();
    for (int i = 1; i < impl->NumberLevels(); i++) {
      int num_files = current->NumLevelFiles(i);
      if (num_files > 0) {
        s = Status::InvalidArgument("Not all files are at level 0. Cannot "
          "open with universal compaction style.");
        break;
      }
    }
  }

  impl->mutex_.Unlock();

  if (s.ok()) {
    impl->opened_successfully_ = true;
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  const InternalKeyComparator comparator(options.comparator);
  const InternalFilterPolicy filter_policy(options.filter_policy);
  const Options& soptions(SanitizeOptions(
    dbname, &comparator, &filter_policy, options));
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
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
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
  Log(log, "Git sha %s", rocksdb_build_git_sha);
  Log(log, "Compile time %s %s",
      rocksdb_build_compile_time, rocksdb_build_compile_date);
}

}  // namespace rocksdb
