//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of table files per level, as well as a
// set of blob files. The entire set of versions is maintained in a
// VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#pragma once
#include <atomic>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cache/cache_helpers.h"
#include "db/blob/blob_file_meta.h"
#include "db/blob/blob_index.h"
#include "db/column_family.h"
#include "db/compaction/compaction.h"
#include "db/compaction/compaction_picker.h"
#include "db/dbformat.h"
#include "db/error_handler.h"
#include "db/file_indexer.h"
#include "db/log_reader.h"
#include "db/range_del_aggregator.h"
#include "db/read_callback.h"
#include "db/table_cache.h"
#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "env/file_system_tracer.h"
#if USE_COROUTINES
#include "folly/coro/BlockingWait.h"
#include "folly/coro/Collect.h"
#endif
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "options/offpeak_time_info.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "table/get_context.h"
#include "table/multiget_context.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/autovector.h"
#include "util/coro_utils.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

namespace log {
class Writer;
}

class BlobIndex;
class Compaction;
class LogBuffer;
class LookupKey;
class MemTable;
class Version;
class VersionSet;
class WriteBufferManager;
class MergeContext;
class ColumnFamilySet;
class MergeIteratorBuilder;
class SystemClock;
class ManifestTailer;
class FilePickerMultiGet;

// VersionEdit is always supposed to be valid and it is used to point at
// entries in Manifest. Ideally it should not be used as a container to
// carry around few of its fields as function params because it can cause
// readers to think it's a valid entry from Manifest. To avoid that confusion
// introducing VersionEditParams to simply carry around multiple VersionEdit
// params. It need not point to a valid record in Manifest.
using VersionEditParams = VersionEdit;

// Return the smallest index i such that file_level.files[i]->largest >= key.
// Return file_level.num_files if there is no such file.
// REQUIRES: "file_level.files" contains a sorted list of
// non-overlapping files.
int FindFile(const InternalKeyComparator& icmp,
             const LevelFilesBrief& file_level, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, file_level.files[]
// contains disjoint ranges in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const LevelFilesBrief& file_level,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

// Generate LevelFilesBrief from vector<FdWithKeyRange*>
// Would copy smallest_key and largest_key data to sequential memory
// arena: Arena used to allocate the memory
void DoGenerateLevelFilesBrief(LevelFilesBrief* file_level,
                               const std::vector<FileMetaData*>& files,
                               Arena* arena);
enum EpochNumberRequirement {
  kMightMissing,
  kMustPresent,
};

// Information of the storage associated with each Version, including number of
// levels of LSM tree, files information at each level, files marked for
// compaction, blob files, etc.
class VersionStorageInfo {
 public:
  VersionStorageInfo(const InternalKeyComparator* internal_comparator,
                     const Comparator* user_comparator, int num_levels,
                     CompactionStyle compaction_style,
                     VersionStorageInfo* src_vstorage,
                     bool _force_consistency_checks,
                     EpochNumberRequirement epoch_number_requirement,
                     SystemClock* clock,
                     uint32_t bottommost_file_compaction_delay,
                     OffpeakTimeOption offpeak_time_option);
  // No copying allowed
  VersionStorageInfo(const VersionStorageInfo&) = delete;
  void operator=(const VersionStorageInfo&) = delete;
  ~VersionStorageInfo();

  void Reserve(int level, size_t size) { files_[level].reserve(size); }

  void AddFile(int level, FileMetaData* f);

  // Resize/Initialize the space for compact_cursor_
  void ResizeCompactCursors(int level) {
    compact_cursor_.resize(level, InternalKey());
  }

  const std::vector<InternalKey>& GetCompactCursors() const {
    return compact_cursor_;
  }

  // REQUIRES: ResizeCompactCursors has been called
  void AddCursorForOneLevel(int level,
                            const InternalKey& smallest_uncompacted_key) {
    compact_cursor_[level] = smallest_uncompacted_key;
  }

  // REQUIRES: lock is held
  // Update the compact cursor and advance the file index using increment
  // so that it can point to the next cursor (increment means the number of
  // input files in this level of the last compaction)
  const InternalKey& GetNextCompactCursor(int level, size_t increment) {
    int cmp_idx = next_file_to_compact_by_size_[level] + (int)increment;
    assert(cmp_idx <= (int)files_by_compaction_pri_[level].size());
    // TODO(zichen): may need to update next_file_to_compact_by_size_
    // for parallel compaction.
    InternalKey new_cursor;
    if (cmp_idx >= (int)files_by_compaction_pri_[level].size()) {
      cmp_idx = 0;
    }
    // TODO(zichen): rethink if this strategy gives us some good guarantee
    return files_[level][files_by_compaction_pri_[level][cmp_idx]]->smallest;
  }

  void ReserveBlob(size_t size) { blob_files_.reserve(size); }

  void AddBlobFile(std::shared_ptr<BlobFileMetaData> blob_file_meta);

  void PrepareForVersionAppend(const ImmutableOptions& immutable_options,
                               const MutableCFOptions& mutable_cf_options);

  // REQUIRES: PrepareForVersionAppend has been called
  void SetFinalized();

  // Update the accumulated stats from a file-meta.
  void UpdateAccumulatedStats(FileMetaData* file_meta);

  // Decrease the current stat from a to-be-deleted file-meta
  void RemoveCurrentStats(FileMetaData* file_meta);

  // Updates internal structures that keep track of compaction scores
  // We use compaction scores to figure out which compaction to do next
  // REQUIRES: db_mutex held!!
  // TODO find a better way to pass compaction_options_fifo.
  void ComputeCompactionScore(const ImmutableOptions& immutable_options,
                              const MutableCFOptions& mutable_cf_options);

  // Estimate est_comp_needed_bytes_
  void EstimateCompactionBytesNeeded(
      const MutableCFOptions& mutable_cf_options);

  // This computes files_marked_for_compaction_ and is called by
  // ComputeCompactionScore()
  void ComputeFilesMarkedForCompaction(int last_level);

  // This computes ttl_expired_files_ and is called by
  // ComputeCompactionScore()
  void ComputeExpiredTtlFiles(const ImmutableOptions& ioptions,
                              const uint64_t ttl);

  // This computes files_marked_for_periodic_compaction_ and is called by
  // ComputeCompactionScore()
  void ComputeFilesMarkedForPeriodicCompaction(
      const ImmutableOptions& ioptions,
      const uint64_t periodic_compaction_seconds, int last_level);

  // This computes bottommost_files_marked_for_compaction_ and is called by
  // ComputeCompactionScore() or UpdateOldestSnapshot().
  //
  // Among bottommost files (assumes they've already been computed), marks the
  // ones that have keys that would be eliminated if recompacted, according to
  // the seqnum of the oldest existing snapshot. Must be called every time
  // oldest snapshot changes as that is when bottom-level files can become
  // eligible for compaction.
  //
  // REQUIRES: DB mutex held
  void ComputeBottommostFilesMarkedForCompaction(bool allow_ingest_behind);

  // This computes files_marked_for_forced_blob_gc_ and is called by
  // ComputeCompactionScore()
  //
  // REQUIRES: DB mutex held
  void ComputeFilesMarkedForForcedBlobGC(
      double blob_garbage_collection_age_cutoff,
      double blob_garbage_collection_force_threshold,
      bool enable_blob_garbage_collection);

  bool level0_non_overlapping() const { return level0_non_overlapping_; }

  // Updates the oldest snapshot and related internal state, like the bottommost
  // files marked for compaction.
  // REQUIRES: DB mutex held
  void UpdateOldestSnapshot(SequenceNumber oldest_snapshot_seqnum,
                            bool allow_ingest_behind);

  int MaxInputLevel() const;
  int MaxOutputLevel(bool allow_ingest_behind) const;

  // Return level number that has idx'th highest score
  int CompactionScoreLevel(int idx) const { return compaction_level_[idx]; }

  // Return idx'th highest score
  double CompactionScore(int idx) const { return compaction_score_[idx]; }

  void GetOverlappingInputs(
      int level, const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,               // nullptr means after all keys
      std::vector<FileMetaData*>* inputs,
      int hint_index = -1,        // index of overlap file
      int* file_index = nullptr,  // return index of overlap file
      bool expand_range = true,   // if set, returns files which overlap the
                                  // range and overlap each other. If false,
                                  // then just files intersecting the range
      InternalKey** next_smallest = nullptr)  // if non-null, returns the
      const;  // smallest key of next file not included
  void GetCleanInputsWithinInterval(
      int level, const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,               // nullptr means after all keys
      std::vector<FileMetaData*>* inputs,
      int hint_index = -1,        // index of overlap file
      int* file_index = nullptr)  // return index of overlap file
      const;

  void GetOverlappingInputsRangeBinarySearch(
      int level,                 // level > 0
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs,
      int hint_index,                // index of overlap file
      int* file_index,               // return index of overlap file
      bool within_interval = false,  // if set, force the inputs within interval
      InternalKey** next_smallest = nullptr)  // if non-null, returns the
      const;  // smallest key of next file not included

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==NULL represents a key smaller than all keys in the DB.
  // largest_user_key==NULL represents a key largest than all keys in the DB.
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Returns true iff the first or last file in inputs contains
  // an overlapping user key to the file "just outside" of it (i.e.
  // just after the last file, or just before the first file)
  // REQUIRES: "*inputs" is a sorted list of non-overlapping files
  bool HasOverlappingUserKey(const std::vector<FileMetaData*>* inputs,
                             int level);

  int num_levels() const { return num_levels_; }

  // REQUIRES: PrepareForVersionAppend has been called
  int num_non_empty_levels() const {
    assert(finalized_);
    return num_non_empty_levels_;
  }

  // REQUIRES: PrepareForVersionAppend has been called
  // This may or may not return number of level files. It is to keep backward
  // compatible behavior in universal compaction.
  int l0_delay_trigger_count() const { return l0_delay_trigger_count_; }

  void set_l0_delay_trigger_count(int v) { l0_delay_trigger_count_ = v; }

  // REQUIRES: This version has been saved (see VersionBuilder::SaveTo)
  int NumLevelFiles(int level) const {
    assert(finalized_);
    return static_cast<int>(files_[level].size());
  }

  // Return the combined file size of all files at the specified level.
  uint64_t NumLevelBytes(int level) const;

  // REQUIRES: This version has been saved (see VersionBuilder::SaveTo)
  const std::vector<FileMetaData*>& LevelFiles(int level) const {
    return files_[level];
  }

  bool HasMissingEpochNumber() const;
  uint64_t GetMaxEpochNumberOfFiles() const;
  EpochNumberRequirement GetEpochNumberRequirement() const {
    return epoch_number_requirement_;
  }
  void SetEpochNumberRequirement(
      EpochNumberRequirement epoch_number_requirement) {
    epoch_number_requirement_ = epoch_number_requirement;
  }
  // Ensure all files have epoch number set.
  // If there is a file missing epoch number, all files' epoch number will be
  // reset according to CF's epoch number. Otherwise, the CF will be updated
  // with the max epoch number of the files.
  //
  // @param restart_epoch This CF's epoch number will be reset to start from 0.
  // @param force Force resetting all files' epoch number.
  void RecoverEpochNumbers(ColumnFamilyData* cfd, bool restart_epoch = true,
                           bool force = false);

  class FileLocation {
   public:
    FileLocation() = default;
    FileLocation(int level, size_t position)
        : level_(level), position_(position) {}

    int GetLevel() const { return level_; }
    size_t GetPosition() const { return position_; }

    bool IsValid() const { return level_ >= 0; }

    bool operator==(const FileLocation& rhs) const {
      return level_ == rhs.level_ && position_ == rhs.position_;
    }

    bool operator!=(const FileLocation& rhs) const { return !(*this == rhs); }

    static FileLocation Invalid() { return FileLocation(); }

   private:
    int level_ = -1;
    size_t position_ = 0;
  };

  // REQUIRES: PrepareForVersionAppend has been called
  FileLocation GetFileLocation(uint64_t file_number) const {
    const auto it = file_locations_.find(file_number);

    if (it == file_locations_.end()) {
      return FileLocation::Invalid();
    }

    assert(it->second.GetLevel() < num_levels_);
    assert(it->second.GetPosition() < files_[it->second.GetLevel()].size());
    assert(files_[it->second.GetLevel()][it->second.GetPosition()]);
    assert(files_[it->second.GetLevel()][it->second.GetPosition()]
               ->fd.GetNumber() == file_number);

    return it->second;
  }

  // REQUIRES: PrepareForVersionAppend has been called
  FileMetaData* GetFileMetaDataByNumber(uint64_t file_number) const {
    auto location = GetFileLocation(file_number);

    if (!location.IsValid()) {
      return nullptr;
    }

    return files_[location.GetLevel()][location.GetPosition()];
  }

  // REQUIRES: This version has been saved (see VersionBuilder::SaveTo)
  using BlobFiles = std::vector<std::shared_ptr<BlobFileMetaData>>;
  const BlobFiles& GetBlobFiles() const { return blob_files_; }

  // REQUIRES: This version has been saved (see VersionBuilder::SaveTo)
  BlobFiles::const_iterator GetBlobFileMetaDataLB(
      uint64_t blob_file_number) const;

  // REQUIRES: This version has been saved (see VersionBuilder::SaveTo)
  std::shared_ptr<BlobFileMetaData> GetBlobFileMetaData(
      uint64_t blob_file_number) const {
    const auto it = GetBlobFileMetaDataLB(blob_file_number);

    assert(it == blob_files_.end() || *it);

    if (it != blob_files_.end() &&
        (*it)->GetBlobFileNumber() == blob_file_number) {
      return *it;
    }

    return std::shared_ptr<BlobFileMetaData>();
  }

  // REQUIRES: This version has been saved (see VersionBuilder::SaveTo)
  struct BlobStats {
    uint64_t total_file_size = 0;
    uint64_t total_garbage_size = 0;
    double space_amp = 0.0;
  };

  BlobStats GetBlobStats() const {
    uint64_t total_file_size = 0;
    uint64_t total_garbage_size = 0;

    for (const auto& meta : blob_files_) {
      assert(meta);

      total_file_size += meta->GetBlobFileSize();
      total_garbage_size += meta->GetGarbageBlobBytes();
    }

    double space_amp = 0.0;
    if (total_file_size > total_garbage_size) {
      space_amp = static_cast<double>(total_file_size) /
                  (total_file_size - total_garbage_size);
    }

    return BlobStats{total_file_size, total_garbage_size, space_amp};
  }

  const ROCKSDB_NAMESPACE::LevelFilesBrief& LevelFilesBrief(int level) const {
    assert(level < static_cast<int>(level_files_brief_.size()));
    return level_files_brief_[level];
  }

  // REQUIRES: PrepareForVersionAppend has been called
  const std::vector<int>& FilesByCompactionPri(int level) const {
    assert(finalized_);
    return files_by_compaction_pri_[level];
  }

  // REQUIRES: ComputeCompactionScore has been called
  // REQUIRES: DB mutex held during access
  const autovector<std::pair<int, FileMetaData*>>& FilesMarkedForCompaction()
      const {
    assert(finalized_);
    return files_marked_for_compaction_;
  }

  void TEST_AddFileMarkedForCompaction(int level, FileMetaData* f) {
    f->marked_for_compaction = true;
    files_marked_for_compaction_.emplace_back(level, f);
  }

  // REQUIRES: ComputeCompactionScore has been called
  // REQUIRES: DB mutex held during access
  // Used by Leveled Compaction only.
  const autovector<std::pair<int, FileMetaData*>>& ExpiredTtlFiles() const {
    assert(finalized_);
    return expired_ttl_files_;
  }

  // REQUIRES: ComputeCompactionScore has been called
  // REQUIRES: DB mutex held during access
  // Used by Leveled and Universal Compaction.
  const autovector<std::pair<int, FileMetaData*>>&
  FilesMarkedForPeriodicCompaction() const {
    assert(finalized_);
    return files_marked_for_periodic_compaction_;
  }

  void TEST_AddFileMarkedForPeriodicCompaction(int level, FileMetaData* f) {
    files_marked_for_periodic_compaction_.emplace_back(level, f);
  }

  // REQUIRES: PrepareForVersionAppend has been called
  const autovector<std::pair<int, FileMetaData*>>& BottommostFiles() const {
    assert(finalized_);
    return bottommost_files_;
  }

  // REQUIRES: ComputeCompactionScore has been called
  // REQUIRES: DB mutex held during access
  const autovector<std::pair<int, FileMetaData*>>&
  BottommostFilesMarkedForCompaction() const {
    assert(finalized_);
    return bottommost_files_marked_for_compaction_;
  }

  // REQUIRES: ComputeCompactionScore has been called
  // REQUIRES: DB mutex held during access
  const autovector<std::pair<int, FileMetaData*>>& FilesMarkedForForcedBlobGC()
      const {
    assert(finalized_);
    return files_marked_for_forced_blob_gc_;
  }

  int base_level() const { return base_level_; }
  double level_multiplier() const { return level_multiplier_; }

  // REQUIRES: lock is held
  // Set the index that is used to offset into files_by_compaction_pri_ to find
  // the next compaction candidate file.
  void SetNextCompactionIndex(int level, int index) {
    next_file_to_compact_by_size_[level] = index;
  }

  // REQUIRES: lock is held
  int NextCompactionIndex(int level) const {
    return next_file_to_compact_by_size_[level];
  }

  // REQUIRES: PrepareForVersionAppend has been called
  const FileIndexer& file_indexer() const {
    assert(finalized_);
    return file_indexer_;
  }

  // Only the first few entries of files_by_compaction_pri_ are sorted.
  // There is no need to sort all the files because it is likely
  // that on a running system, we need to look at only the first
  // few largest files because a new version is created every few
  // seconds/minutes (because of concurrent compactions).
  static const size_t kNumberFilesToSort = 50;

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[1000];
  };
  struct FileSummaryStorage {
    char buffer[3000];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;
  // Return a human-readable short (single-line) summary of files
  // in a specified level.  Uses *scratch as backing store.
  const char* LevelFileSummary(FileSummaryStorage* scratch, int level) const;

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  uint64_t MaxNextLevelOverlappingBytes();

  // Return a human readable string that describes this version's contents.
  std::string DebugString(bool hex = false) const;

  uint64_t GetAverageValueSize() const {
    if (accumulated_num_non_deletions_ == 0) {
      return 0;
    }
    assert(accumulated_raw_key_size_ + accumulated_raw_value_size_ > 0);
    assert(accumulated_file_size_ > 0);
    return accumulated_raw_value_size_ / accumulated_num_non_deletions_ *
           accumulated_file_size_ /
           (accumulated_raw_key_size_ + accumulated_raw_value_size_);
  }

  uint64_t GetEstimatedActiveKeys() const;

  double GetEstimatedCompressionRatioAtLevel(int level) const;

  // re-initializes the index that is used to offset into
  // files_by_compaction_pri_
  // to find the next compaction candidate file.
  void ResetNextCompactionIndex(int level) {
    next_file_to_compact_by_size_[level] = 0;
  }

  const InternalKeyComparator* InternalComparator() const {
    return internal_comparator_;
  }

  // Returns maximum total bytes of data on a given level.
  uint64_t MaxBytesForLevel(int level) const;

  // Returns an estimate of the amount of live data in bytes.
  uint64_t EstimateLiveDataSize() const;

  uint64_t estimated_compaction_needed_bytes() const {
    return estimated_compaction_needed_bytes_;
  }

  void TEST_set_estimated_compaction_needed_bytes(uint64_t v,
                                                  InstrumentedMutex* mu) {
    InstrumentedMutexLock l(mu);
    estimated_compaction_needed_bytes_ = v;
  }

  bool force_consistency_checks() const { return force_consistency_checks_; }

  SequenceNumber bottommost_files_mark_threshold() const {
    return bottommost_files_mark_threshold_;
  }

  SequenceNumber standalone_range_tombstone_files_mark_threshold() const {
    return standalone_range_tombstone_files_mark_threshold_;
  }

  // Returns whether any key in [`smallest_key`, `largest_key`] could appear in
  // an older L0 file than `last_l0_idx` or in a greater level than `last_level`
  //
  // @param last_level Level after which we check for overlap
  // @param last_l0_idx If `last_level == 0`, index of L0 file after which we
  //    check for overlap; otherwise, must be -1
  bool RangeMightExistAfterSortedRun(const Slice& smallest_user_key,
                                     const Slice& largest_user_key,
                                     int last_level, int last_l0_idx);

  Env::WriteLifeTimeHint CalculateSSTWriteHint(int level) const;

  const Comparator* user_comparator() const { return user_comparator_; }

 private:
  void ComputeCompensatedSizes();
  void UpdateNumNonEmptyLevels();
  void CalculateBaseBytes(const ImmutableOptions& ioptions,
                          const MutableCFOptions& options);
  void UpdateFilesByCompactionPri(const ImmutableOptions& immutable_options,
                                  const MutableCFOptions& mutable_cf_options);

  void GenerateFileIndexer() {
    file_indexer_.UpdateIndex(&arena_, num_non_empty_levels_, files_);
  }

  void GenerateLevelFilesBrief();
  void GenerateLevel0NonOverlapping();
  void GenerateBottommostFiles();
  void GenerateFileLocationIndex();

  const InternalKeyComparator* internal_comparator_;
  const Comparator* user_comparator_;
  int num_levels_;            // Number of levels
  int num_non_empty_levels_;  // Number of levels. Any level larger than it
                              // is guaranteed to be empty.
  // Per-level max bytes
  std::vector<uint64_t> level_max_bytes_;

  // A short brief metadata of files per level
  autovector<ROCKSDB_NAMESPACE::LevelFilesBrief> level_files_brief_;
  FileIndexer file_indexer_;
  Arena arena_;  // Used to allocate space for file_levels_

  CompactionStyle compaction_style_;

  // List of files per level, files in each level are arranged
  // in increasing order of keys
  std::vector<FileMetaData*>* files_;

  // Map of all table files in version. Maps file number to (level, position on
  // level).
  using FileLocations = UnorderedMap<uint64_t, FileLocation>;
  FileLocations file_locations_;

  // Vector of blob files in version sorted by blob file number.
  BlobFiles blob_files_;

  // Level that L0 data should be compacted to. All levels < base_level_ should
  // be empty. -1 if it is not level-compaction so it's not applicable.
  int base_level_;

  // Applies to level compaction when
  // `level_compaction_dynamic_level_bytes=true`. All non-empty levels <=
  // lowest_unnecessary_level_ are not needed and will be drained automatically.
  // -1 if there is no unnecessary level,
  int lowest_unnecessary_level_;

  double level_multiplier_;

  // A list for the same set of files that are stored in files_,
  // but files in each level are now sorted based on file
  // size. The file with the largest size is at the front.
  // This vector stores the index of the file from files_.
  std::vector<std::vector<int>> files_by_compaction_pri_;

  // If true, means that files in L0 have keys with non overlapping ranges
  bool level0_non_overlapping_;

  // An index into files_by_compaction_pri_ that specifies the first
  // file that is not yet compacted
  std::vector<int> next_file_to_compact_by_size_;

  // Only the first few entries of files_by_compaction_pri_ are sorted.
  // There is no need to sort all the files because it is likely
  // that on a running system, we need to look at only the first
  // few largest files because a new version is created every few
  // seconds/minutes (because of concurrent compactions).
  static const size_t number_of_files_to_sort_ = 50;

  // This vector contains list of files marked for compaction and also not
  // currently being compacted. It is protected by DB mutex. It is calculated in
  // ComputeCompactionScore(). Used by Leveled and Universal Compaction.
  autovector<std::pair<int, FileMetaData*>> files_marked_for_compaction_;

  autovector<std::pair<int, FileMetaData*>> expired_ttl_files_;

  autovector<std::pair<int, FileMetaData*>>
      files_marked_for_periodic_compaction_;

  // These files are considered bottommost because none of their keys can exist
  // at lower levels. They are not necessarily all in the same level. The marked
  // ones are eligible for compaction because they contain duplicate key
  // versions that are no longer protected by snapshot. These variables are
  // protected by DB mutex and are calculated in `GenerateBottommostFiles()` and
  // `ComputeBottommostFilesMarkedForCompaction()`.
  autovector<std::pair<int, FileMetaData*>> bottommost_files_;
  autovector<std::pair<int, FileMetaData*>>
      bottommost_files_marked_for_compaction_;

  autovector<std::pair<int, FileMetaData*>> files_marked_for_forced_blob_gc_;

  // Threshold for needing to mark another bottommost file. Maintain it so we
  // can quickly check when releasing a snapshot whether more bottommost files
  // became eligible for compaction. It's defined as the min of the max nonzero
  // seqnums of unmarked bottommost files.
  SequenceNumber bottommost_files_mark_threshold_ = kMaxSequenceNumber;

  // The minimum sequence number among all the standalone range tombstone files
  // that are marked for compaction. A standalone range tombstone file is one
  // with just one range tombstone.
  SequenceNumber standalone_range_tombstone_files_mark_threshold_ =
      kMaxSequenceNumber;

  // Monotonically increases as we release old snapshots. Zero indicates no
  // snapshots have been released yet. When no snapshots remain we set it to the
  // current seqnum, which needs to be protected as a snapshot can still be
  // created that references it.
  SequenceNumber oldest_snapshot_seqnum_ = 0;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by ComputeCompactionScore.
  // The most critical level to be compacted is listed first
  // These are used to pick the best compaction level
  std::vector<double> compaction_score_;
  std::vector<int> compaction_level_;
  int l0_delay_trigger_count_ = 0;  // Count used to trigger slow down and stop
                                    // for number of L0 files.

  // Compact cursors for round-robin compactions in each level
  std::vector<InternalKey> compact_cursor_;

  // the following are the sampled temporary stats.
  // the current accumulated size of sampled files.
  uint64_t accumulated_file_size_;
  // the current accumulated size of all raw keys based on the sampled files.
  uint64_t accumulated_raw_key_size_;
  // the current accumulated size of all raw keys based on the sampled files.
  uint64_t accumulated_raw_value_size_;
  // total number of non-deletion entries
  uint64_t accumulated_num_non_deletions_;
  // total number of deletion entries
  uint64_t accumulated_num_deletions_;
  // current number of non_deletion entries
  uint64_t current_num_non_deletions_;
  // current number of deletion entries
  uint64_t current_num_deletions_;
  // current number of file samples
  uint64_t current_num_samples_;
  // Estimated bytes needed to be compacted until all levels' size is down to
  // target sizes.
  uint64_t estimated_compaction_needed_bytes_;

  // Used for computing bottommost files marked for compaction and checking for
  // offpeak time.
  SystemClock* clock_;
  uint32_t bottommost_file_compaction_delay_;

  bool finalized_;

  // If set to true, we will run consistency checks even if RocksDB
  // is compiled in release mode
  bool force_consistency_checks_;

  EpochNumberRequirement epoch_number_requirement_;

  OffpeakTimeOption offpeak_time_option_;

  friend class Version;
  friend class VersionSet;
};

struct ObsoleteFileInfo {
  FileMetaData* metadata;
  std::string path;
  // If true, the FileMataData should be destroyed but the file should
  // not be deleted. This is because another FileMetaData still references
  // the file, usually because the file is trivial moved so two FileMetadata
  // is managing the file.
  bool only_delete_metadata = false;
  // To apply to this file
  uint32_t uncache_aggressiveness = 0;

  ObsoleteFileInfo() noexcept
      : metadata(nullptr), only_delete_metadata(false) {}
  ObsoleteFileInfo(FileMetaData* f, const std::string& file_path,
                   uint32_t _uncache_aggressiveness,
                   std::shared_ptr<CacheReservationManager>
                       file_metadata_cache_res_mgr_arg = nullptr)
      : metadata(f),
        path(file_path),
        uncache_aggressiveness(_uncache_aggressiveness),
        file_metadata_cache_res_mgr(
            std::move(file_metadata_cache_res_mgr_arg)) {}

  ObsoleteFileInfo(const ObsoleteFileInfo&) = delete;
  ObsoleteFileInfo& operator=(const ObsoleteFileInfo&) = delete;

  ObsoleteFileInfo(ObsoleteFileInfo&& rhs) noexcept : ObsoleteFileInfo() {
    *this = std::move(rhs);
  }

  ObsoleteFileInfo& operator=(ObsoleteFileInfo&& rhs) noexcept {
    metadata = rhs.metadata;
    rhs.metadata = nullptr;
    path = std::move(rhs.path);
    only_delete_metadata = rhs.only_delete_metadata;
    rhs.only_delete_metadata = false;
    uncache_aggressiveness = rhs.uncache_aggressiveness;
    rhs.uncache_aggressiveness = 0;
    file_metadata_cache_res_mgr = rhs.file_metadata_cache_res_mgr;
    rhs.file_metadata_cache_res_mgr = nullptr;

    return *this;
  }
  void DeleteMetadata() {
    if (file_metadata_cache_res_mgr) {
      Status s = file_metadata_cache_res_mgr->UpdateCacheReservation(
          metadata->ApproximateMemoryUsage(), false /* increase */);
      s.PermitUncheckedError();
    }
    delete metadata;
    metadata = nullptr;
  }

 private:
  std::shared_ptr<CacheReservationManager> file_metadata_cache_res_mgr;
};

class ObsoleteBlobFileInfo {
 public:
  ObsoleteBlobFileInfo(uint64_t blob_file_number, std::string path)
      : blob_file_number_(blob_file_number), path_(std::move(path)) {}

  uint64_t GetBlobFileNumber() const { return blob_file_number_; }
  const std::string& GetPath() const { return path_; }

 private:
  uint64_t blob_file_number_;
  std::string path_;
};

using MultiGetRange = MultiGetContext::Range;
// A column family's version consists of the table and blob files owned by
// the column family at a certain point in time.
class Version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // @param read_options Must outlive any iterator built by
  // `merger_iter_builder`.
  void AddIterators(const ReadOptions& read_options,
                    const FileOptions& soptions,
                    MergeIteratorBuilder* merger_iter_builder,
                    bool allow_unprepared_value);

  // @param read_options Must outlive any iterator built by
  // `merger_iter_builder`.
  void AddIteratorsForLevel(const ReadOptions& read_options,
                            const FileOptions& soptions,
                            MergeIteratorBuilder* merger_iter_builder,
                            int level, bool allow_unprepared_value);

  Status OverlapWithLevelIterator(const ReadOptions&, const FileOptions&,
                                  const Slice& smallest_user_key,
                                  const Slice& largest_user_key, int level,
                                  bool* overlap);

  // Lookup the value for key or get all merge operands for key.
  // If do_merge = true (default) then lookup value for key.
  // Behavior if do_merge = true:
  //    If found, store it in *value and
  //    return OK.  Else return a non-OK status.
  //    Uses *operands to store merge_operator operations to apply later.
  //
  //    If the ReadOptions.read_tier is set to do a read-only fetch, then
  //    *value_found will be set to false if it cannot be determined whether
  //    this value exists without doing IO.
  //
  //    If the key is Deleted, *status will be set to NotFound and
  //                        *key_exists will be set to true.
  //    If no key was found, *status will be set to NotFound and
  //                      *key_exists will be set to false.
  //    If seq is non-null, *seq will be set to the sequence number found
  //    for the key if a key was found.
  // Behavior if do_merge = false
  //    If the key has any merge operands then store them in
  //    merge_context.operands_list and don't merge the operands
  // REQUIRES: lock is not held
  // REQUIRES: pinned_iters_mgr != nullptr
  void Get(const ReadOptions&, const LookupKey& key, PinnableSlice* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* status,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq,
           PinnedIteratorsManager* pinned_iters_mgr,
           bool* value_found = nullptr, bool* key_exists = nullptr,
           SequenceNumber* seq = nullptr, ReadCallback* callback = nullptr,
           bool* is_blob = nullptr, bool do_merge = true);

  void MultiGet(const ReadOptions&, MultiGetRange* range,
                ReadCallback* callback = nullptr);

  // Interprets blob_index_slice as a blob reference, and (assuming the
  // corresponding blob file is part of this Version) retrieves the blob and
  // saves it in *value.
  // REQUIRES: blob_index_slice stores an encoded blob reference
  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 const Slice& blob_index_slice,
                 FilePrefetchBuffer* prefetch_buffer, PinnableSlice* value,
                 uint64_t* bytes_read) const;

  // Retrieves a blob using a blob reference and saves it in *value,
  // assuming the corresponding blob file is part of this Version.
  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 const BlobIndex& blob_index,
                 FilePrefetchBuffer* prefetch_buffer, PinnableSlice* value,
                 uint64_t* bytes_read) const;

  struct BlobReadContext {
    BlobReadContext(const BlobIndex& blob_idx, const KeyContext* key_ctx)
        : blob_index(blob_idx), key_context(key_ctx) {}

    BlobIndex blob_index;
    const KeyContext* key_context;
    PinnableSlice result;
  };

  using BlobReadContexts = std::vector<BlobReadContext>;
  void MultiGetBlob(const ReadOptions& read_options, MultiGetRange& range,
                    std::unordered_map<uint64_t, BlobReadContexts>& blob_ctxs);

  // Loads some stats information from files (if update_stats is set) and
  // populates derived data structures. Call without mutex held. It needs to be
  // called before appending the version to the version set.
  void PrepareAppend(const MutableCFOptions& mutable_cf_options,
                     const ReadOptions& read_options, bool update_stats);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  // Decrease reference count. Delete the object if no reference left
  // and return true. Otherwise, return false.
  bool Unref();

  // Add all files listed in the current version to *live_table_files and
  // *live_blob_files.
  void AddLiveFiles(std::vector<uint64_t>* live_table_files,
                    std::vector<uint64_t>* live_blob_files) const;

  // Remove live files that are in the delete candidate lists.
  void RemoveLiveFiles(
      std::vector<ObsoleteFileInfo>& sst_delete_candidates,
      std::vector<ObsoleteBlobFileInfo>& blob_delete_candidates) const;

  // Return a human readable string that describes this version's contents.
  std::string DebugString(bool hex = false, bool print_stats = false) const;

  // Returns the version number of this version
  uint64_t GetVersionNumber() const { return version_number_; }

  // REQUIRES: lock is held
  // On success, "tp" will contains the table properties of the file
  // specified in "file_meta".  If the file name of "file_meta" is
  // known ahead, passing it by a non-null "fname" can save a
  // file-name conversion.
  Status GetTableProperties(const ReadOptions& read_options,
                            std::shared_ptr<const TableProperties>* tp,
                            const FileMetaData* file_meta,
                            const std::string* fname = nullptr) const;

  // REQUIRES: lock is held
  // On success, *props will be populated with all SSTables' table properties.
  // The keys of `props` are the sst file name, the values of `props` are the
  // tables' properties, represented as std::shared_ptr.
  Status GetPropertiesOfAllTables(const ReadOptions& read_options,
                                  TablePropertiesCollection* props);
  Status GetPropertiesOfAllTables(const ReadOptions& read_options,
                                  TablePropertiesCollection* props, int level);
  Status GetPropertiesOfTablesInRange(const ReadOptions& read_options,
                                      const autovector<UserKeyRange>& ranges,
                                      TablePropertiesCollection* props) const;

  // Print summary of range delete tombstones in SST files into out_str,
  // with maximum max_entries_to_print entries printed out.
  Status TablesRangeTombstoneSummary(int max_entries_to_print,
                                     std::string* out_str);

  // REQUIRES: lock is held
  // On success, "tp" will contains the aggregated table property among
  // the table properties of all sst files in this version.
  Status GetAggregatedTableProperties(
      const ReadOptions& read_options,
      std::shared_ptr<const TableProperties>* tp, int level = -1);

  uint64_t GetEstimatedActiveKeys() {
    return storage_info_.GetEstimatedActiveKeys();
  }

  size_t GetMemoryUsageByTableReaders(const ReadOptions& read_options);

  ColumnFamilyData* cfd() const { return cfd_; }

  // Return the next Version in the linked list.
  Version* Next() const { return next_; }

  int TEST_refs() const { return refs_; }

  VersionStorageInfo* storage_info() { return &storage_info_; }
  const VersionStorageInfo* storage_info() const { return &storage_info_; }

  VersionSet* version_set() { return vset_; }

  void GetColumnFamilyMetaData(ColumnFamilyMetaData* cf_meta);

  void GetSstFilesBoundaryKeys(Slice* smallest_user_key,
                               Slice* largest_user_key);

  uint64_t GetSstFilesSize();

  // Retrieves the file_creation_time of the oldest file in the DB.
  // Prerequisite for this API is max_open_files = -1
  void GetCreationTimeOfOldestFile(uint64_t* creation_time);

  const MutableCFOptions& GetMutableCFOptions() { return mutable_cf_options_; }

  InternalIterator* TEST_GetLevelIterator(
      const ReadOptions& read_options, MergeIteratorBuilder* merge_iter_builder,
      int level, bool allow_unprepared_value);

 private:
  Env* env_;
  SystemClock* clock_;

  friend class ReactiveVersionSet;
  friend class VersionSet;
  friend class VersionEditHandler;
  friend class VersionEditHandlerPointInTime;

  const InternalKeyComparator* internal_comparator() const {
    return storage_info_.internal_comparator_;
  }
  const Comparator* user_comparator() const {
    return storage_info_.user_comparator_;
  }

  // Returns true if the filter blocks in the specified level will not be
  // checked during read operations. In certain cases (trivial move or preload),
  // the filter block may already be cached, but we still do not access it such
  // that it eventually expires from the cache.
  bool IsFilterSkipped(int level, bool is_file_last_in_level = false);

  // The helper function of UpdateAccumulatedStats, which may fill the missing
  // fields of file_meta from its associated TableProperties.
  // Returns true if it does initialize FileMetaData.
  bool MaybeInitializeFileMetaData(const ReadOptions& read_options,
                                   FileMetaData* file_meta);

  // Update the accumulated stats associated with the current version.
  // This accumulated stats will be used in compaction.
  void UpdateAccumulatedStats(const ReadOptions& read_options);

  DECLARE_SYNC_AND_ASYNC(
      /* ret_type */ Status, /* func_name */ MultiGetFromSST,
      const ReadOptions& read_options, MultiGetRange file_range,
      int hit_file_level, bool skip_filters, bool skip_range_deletions,
      FdWithKeyRange* f,
      std::unordered_map<uint64_t, BlobReadContexts>& blob_ctxs,
      TableCache::TypedHandle* table_handle, uint64_t& num_filter_read,
      uint64_t& num_index_read, uint64_t& num_sst_read);

#ifdef USE_COROUTINES
  // MultiGet using async IO to read data blocks from SST files in parallel
  // within and across levels
  Status MultiGetAsync(
      const ReadOptions& options, MultiGetRange* range,
      std::unordered_map<uint64_t, BlobReadContexts>* blob_ctxs);

  // A helper function to lookup a batch of keys in a single level. It will
  // queue coroutine tasks to mget_tasks. It may also split the input batch
  // by creating a new batch with keys definitely not in this level and
  // enqueuing it to to_process.
  Status ProcessBatch(
      const ReadOptions& read_options, FilePickerMultiGet* batch,
      std::vector<folly::coro::Task<Status>>& mget_tasks,
      std::unordered_map<uint64_t, BlobReadContexts>* blob_ctxs,
      autovector<FilePickerMultiGet, 4>& batches, std::deque<size_t>& waiting,
      std::deque<size_t>& to_process, unsigned int& num_tasks_queued,
      std::unordered_map<int, std::tuple<uint64_t, uint64_t, uint64_t>>&
          mget_stats);
#endif

  ColumnFamilyData* cfd_;  // ColumnFamilyData to which this Version belongs
  Logger* info_log_;
  Statistics* db_statistics_;
  TableCache* table_cache_;
  BlobSource* blob_source_;
  const MergeOperator* merge_operator_;

  VersionStorageInfo storage_info_;
  VersionSet* vset_;  // VersionSet to which this Version belongs
  Version* next_;     // Next version in linked list
  Version* prev_;     // Previous version in linked list
  int refs_;          // Number of live refs to this version
  const FileOptions file_options_;
  const MutableCFOptions mutable_cf_options_;
  // Cached value to avoid recomputing it on every read.
  const size_t max_file_size_for_l0_meta_pin_;

  // A version number that uniquely represents this version. This is
  // used for debugging and logging purposes only.
  uint64_t version_number_;
  std::shared_ptr<IOTracer> io_tracer_;
  bool use_async_io_;

  Version(ColumnFamilyData* cfd, VersionSet* vset, const FileOptions& file_opt,
          MutableCFOptions mutable_cf_options,
          const std::shared_ptr<IOTracer>& io_tracer,
          uint64_t version_number = 0,
          EpochNumberRequirement epoch_number_requirement =
              EpochNumberRequirement::kMustPresent);

  ~Version();

  // No copying allowed
  Version(const Version&) = delete;
  void operator=(const Version&) = delete;
};

class BaseReferencedVersionBuilder;

class AtomicGroupReadBuffer {
 public:
  AtomicGroupReadBuffer() = default;
  Status AddEdit(VersionEdit* edit);
  void Clear();
  bool IsFull() const;
  bool IsEmpty() const;

  uint64_t TEST_read_edits_in_atomic_group() const {
    return read_edits_in_atomic_group_;
  }
  std::vector<VersionEdit>& replay_buffer() { return replay_buffer_; }

 private:
  uint64_t read_edits_in_atomic_group_ = 0;
  std::vector<VersionEdit> replay_buffer_;
};

// VersionSet is the collection of versions of all the column families of the
// database. Each database owns one VersionSet. A VersionSet has access to all
// column families via ColumnFamilySet, i.e. set of the column families.
class VersionSet {
 public:
  VersionSet(const std::string& dbname, const ImmutableDBOptions* db_options,
             const FileOptions& file_options, Cache* table_cache,
             WriteBufferManager* write_buffer_manager,
             WriteController* write_controller,
             BlockCacheTracer* const block_cache_tracer,
             const std::shared_ptr<IOTracer>& io_tracer,
             const std::string& db_id, const std::string& db_session_id,
             const std::string& daily_offpeak_time_utc,
             ErrorHandler* const error_handler, const bool read_only);
  // No copying allowed
  VersionSet(const VersionSet&) = delete;
  void operator=(const VersionSet&) = delete;

  virtual ~VersionSet();

  virtual Status Close(FSDirectory* db_dir, InstrumentedMutex* mu);

  Status LogAndApplyToDefaultColumnFamily(
      const ReadOptions& read_options, const WriteOptions& write_options,
      VersionEdit* edit, InstrumentedMutex* mu,
      FSDirectory* dir_contains_current_file, bool new_descriptor_log = false,
      const ColumnFamilyOptions* column_family_options = nullptr) {
    ColumnFamilyData* default_cf = GetColumnFamilySet()->GetDefault();
    const MutableCFOptions* cf_options =
        default_cf->GetLatestMutableCFOptions();
    return LogAndApply(default_cf, *cf_options, read_options, write_options,
                       edit, mu, dir_contains_current_file, new_descriptor_log,
                       column_family_options);
  }

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // column_family_options has to be set if edit is column family add
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(
      ColumnFamilyData* column_family_data,
      const MutableCFOptions& mutable_cf_options,
      const ReadOptions& read_options, const WriteOptions& write_options,
      VersionEdit* edit, InstrumentedMutex* mu,
      FSDirectory* dir_contains_current_file, bool new_descriptor_log = false,
      const ColumnFamilyOptions* column_family_options = nullptr,
      const std::function<void(const Status&)>& manifest_wcb = {}) {
    autovector<ColumnFamilyData*> cfds;
    cfds.emplace_back(column_family_data);
    autovector<const MutableCFOptions*> mutable_cf_options_list;
    mutable_cf_options_list.emplace_back(&mutable_cf_options);
    autovector<autovector<VersionEdit*>> edit_lists;
    autovector<VersionEdit*> edit_list;
    edit_list.emplace_back(edit);
    edit_lists.emplace_back(edit_list);
    return LogAndApply(cfds, mutable_cf_options_list, read_options,
                       write_options, edit_lists, mu, dir_contains_current_file,
                       new_descriptor_log, column_family_options,
                       {manifest_wcb});
  }
  // The batch version. If edit_list.size() > 1, caller must ensure that
  // no edit in the list column family add or drop
  Status LogAndApply(
      ColumnFamilyData* column_family_data,
      const MutableCFOptions& mutable_cf_options,
      const ReadOptions& read_options, const WriteOptions& write_options,
      const autovector<VersionEdit*>& edit_list, InstrumentedMutex* mu,
      FSDirectory* dir_contains_current_file, bool new_descriptor_log = false,
      const ColumnFamilyOptions* column_family_options = nullptr,
      const std::function<void(const Status&)>& manifest_wcb = {}) {
    autovector<ColumnFamilyData*> cfds;
    cfds.emplace_back(column_family_data);
    autovector<const MutableCFOptions*> mutable_cf_options_list;
    mutable_cf_options_list.emplace_back(&mutable_cf_options);
    autovector<autovector<VersionEdit*>> edit_lists;
    edit_lists.emplace_back(edit_list);
    return LogAndApply(cfds, mutable_cf_options_list, read_options,
                       write_options, edit_lists, mu, dir_contains_current_file,
                       new_descriptor_log, column_family_options,
                       {manifest_wcb});
  }

  // The across-multi-cf batch version. If edit_lists contain more than
  // 1 version edits, caller must ensure that no edit in the []list is column
  // family manipulation.
  virtual Status LogAndApply(
      const autovector<ColumnFamilyData*>& cfds,
      const autovector<const MutableCFOptions*>& mutable_cf_options_list,
      const ReadOptions& read_options, const WriteOptions& write_options,
      const autovector<autovector<VersionEdit*>>& edit_lists,
      InstrumentedMutex* mu, FSDirectory* dir_contains_current_file,
      bool new_descriptor_log = false,
      const ColumnFamilyOptions* new_cf_options = nullptr,
      const std::vector<std::function<void(const Status&)>>& manifest_wcbs =
          {});

  void WakeUpWaitingManifestWriters();

  // Recover the last saved descriptor (MANIFEST) from persistent storage.
  // If read_only == true, Recover() will not complain if some column families
  // are not opened
  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families,
                 bool read_only = false, std::string* db_id = nullptr,
                 bool no_error_if_files_missing = false, bool is_retry = false,
                 Status* log_status = nullptr);

  // Do a best-efforts recovery (Options.best_efforts_recovery=true) from all
  // available MANIFEST files. Similar to `Recover` with these differences:
  // 1) not only the latest MANIFEST can be used, if it's not available or
  //    no successful recovery can be achieved with it, this function also tries
  //    to recover from previous MANIFEST files, in reverse chronological order
  //    until a successful recovery can be achieved.
  // 2) this function doesn't just aim to recover to the latest version, if that
  //    is not available, the most recent point in time version will be saved in
  //    memory. Check doc for `VersionEditHandlerPointInTime` for more details.
  Status TryRecover(const std::vector<ColumnFamilyDescriptor>& column_families,
                    bool read_only,
                    const std::vector<std::string>& files_in_dbname,
                    std::string* db_id, bool* has_missing_table_file);

  // Try to recover the version set to the most recent consistent state
  // recorded in the specified manifest.
  Status TryRecoverFromOneManifest(
      const std::string& manifest_path,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      bool read_only, std::string* db_id, bool* has_missing_table_file);

  // Recover the next epoch number of each CFs and epoch number
  // of their files (if missing)
  void RecoverEpochNumbers();

  // Reads a manifest file and returns a list of column families in
  // column_families.
  static Status ListColumnFamilies(std::vector<std::string>* column_families,
                                   const std::string& dbname, FileSystem* fs);
  static Status ListColumnFamiliesFromManifest(
      const std::string& manifest_path, FileSystem* fs,
      std::vector<std::string>* column_families);

  // Try to reduce the number of levels. This call is valid when
  // only one level from the new max level to the old
  // max level containing files.
  // The call is static, since number of levels is immutable during
  // the lifetime of a RocksDB instance. It reduces number of levels
  // in a DB by applying changes to manifest.
  // For example, a db currently has 7 levels [0-6], and a call to
  // to reduce to 5 [0-4] can only be executed when only one level
  // among [4-6] contains files.
  static Status ReduceNumberOfLevels(const std::string& dbname,
                                     const Options* options,
                                     const FileOptions& file_options,
                                     int new_levels);

  // Get the checksum information of all live files
  Status GetLiveFilesChecksumInfo(FileChecksumList* checksum_list);

  // printf contents (for debugging)
  Status DumpManifest(Options& options, std::string& manifestFileName,
                      bool verbose, bool hex = false, bool json = false,
                      const std::vector<ColumnFamilyDescriptor>& cf_descs = {});

  const std::string& DbSessionId() const { return db_session_id_; }

  // Return the current manifest file number
  uint64_t manifest_file_number() const { return manifest_file_number_; }

  uint64_t options_file_number() const { return options_file_number_; }

  uint64_t pending_manifest_file_number() const {
    return pending_manifest_file_number_;
  }

  uint64_t current_next_file_number() const { return next_file_number_.load(); }

  uint64_t min_log_number_to_keep() const {
    return min_log_number_to_keep_.load();
  }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_.fetch_add(1); }

  // Fetch And Add n new file number
  uint64_t FetchAddFileNumber(uint64_t n) {
    return next_file_number_.fetch_add(n);
  }

  // Return the last sequence number.
  uint64_t LastSequence() const {
    return last_sequence_.load(std::memory_order_acquire);
  }

  // Note: memory_order_acquire must be sufficient.
  uint64_t LastAllocatedSequence() const {
    return last_allocated_sequence_.load(std::memory_order_seq_cst);
  }

  // Note: memory_order_acquire must be sufficient.
  uint64_t LastPublishedSequence() const {
    return last_published_sequence_.load(std::memory_order_seq_cst);
  }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    // Last visible sequence must always be less than last written seq
    assert(!db_options_->two_write_queues || s <= last_allocated_sequence_);
    last_sequence_.store(s, std::memory_order_release);
  }

  // Note: memory_order_release must be sufficient
  void SetLastPublishedSequence(uint64_t s) {
    assert(s >= last_published_sequence_);
    last_published_sequence_.store(s, std::memory_order_seq_cst);
  }

  // Note: memory_order_release must be sufficient
  void SetLastAllocatedSequence(uint64_t s) {
    assert(s >= last_allocated_sequence_);
    last_allocated_sequence_.store(s, std::memory_order_seq_cst);
  }

  // Note: memory_order_release must be sufficient
  uint64_t FetchAddLastAllocatedSequence(uint64_t s) {
    return last_allocated_sequence_.fetch_add(s, std::memory_order_seq_cst);
  }

  // Mark the specified file number as used.
  // REQUIRED: this is only called during single-threaded recovery or repair.
  void MarkFileNumberUsed(uint64_t number);

  // Mark the specified log number as deleted
  // REQUIRED: this is only called during single-threaded recovery or repair, or
  // from ::LogAndApply where the global mutex is held.
  void MarkMinLogNumberToKeep(uint64_t number);

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t prev_log_number() const { return prev_log_number_; }

  // Returns the minimum log number which still has data not flushed to any SST
  // file.
  // In non-2PC mode, all the log numbers smaller than this number can be safely
  // deleted, although we still use `min_log_number_to_keep_` to determine when
  // to delete a WAL file.
  uint64_t MinLogNumberWithUnflushedData() const {
    return PreComputeMinLogNumberWithUnflushedData(nullptr);
  }

  // Returns the minimum log number which still has data not flushed to any SST
  // file.
  // Empty column families' log number is considered to be
  // new_log_number_for_empty_cf.
  uint64_t PreComputeMinLogNumberWithUnflushedData(
      uint64_t new_log_number_for_empty_cf) const {
    uint64_t min_log_num = std::numeric_limits<uint64_t>::max();
    for (auto cfd : *column_family_set_) {
      // It's safe to ignore dropped column families here:
      // cfd->IsDropped() becomes true after the drop is persisted in MANIFEST.
      uint64_t num =
          cfd->IsEmpty() ? new_log_number_for_empty_cf : cfd->GetLogNumber();
      if (min_log_num > num && !cfd->IsDropped()) {
        min_log_num = num;
      }
    }
    return min_log_num;
  }
  // Returns the minimum log number which still has data not flushed to any SST
  // file, except data from `cfd_to_skip`.
  uint64_t PreComputeMinLogNumberWithUnflushedData(
      const ColumnFamilyData* cfd_to_skip) const {
    uint64_t min_log_num = std::numeric_limits<uint64_t>::max();
    for (auto cfd : *column_family_set_) {
      if (cfd == cfd_to_skip) {
        continue;
      }
      // It's safe to ignore dropped column families here:
      // cfd->IsDropped() becomes true after the drop is persisted in MANIFEST.
      if (min_log_num > cfd->GetLogNumber() && !cfd->IsDropped()) {
        min_log_num = cfd->GetLogNumber();
      }
    }
    return min_log_num;
  }
  // Returns the minimum log number which still has data not flushed to any SST
  // file, except data from `cfds_to_skip`.
  uint64_t PreComputeMinLogNumberWithUnflushedData(
      const std::unordered_set<const ColumnFamilyData*>& cfds_to_skip) const {
    uint64_t min_log_num = std::numeric_limits<uint64_t>::max();
    for (auto cfd : *column_family_set_) {
      if (cfds_to_skip.count(cfd)) {
        continue;
      }
      // It's safe to ignore dropped column families here:
      // cfd->IsDropped() becomes true after the drop is persisted in MANIFEST.
      if (min_log_num > cfd->GetLogNumber() && !cfd->IsDropped()) {
        min_log_num = cfd->GetLogNumber();
      }
    }
    return min_log_num;
  }

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  // @param read_options Must outlive the returned iterator.
  // @param start, end indicates compaction range
  InternalIterator* MakeInputIterator(
      const ReadOptions& read_options, const Compaction* c,
      RangeDelAggregator* range_del_agg,
      const FileOptions& file_options_compactions,
      const std::optional<const Slice>& start,
      const std::optional<const Slice>& end);

  // Add all files listed in any live version to *live_table_files and
  // *live_blob_files. Note that these lists may contain duplicates.
  void AddLiveFiles(std::vector<uint64_t>* live_table_files,
                    std::vector<uint64_t>* live_blob_files) const;

  // Remove live files that are in the delete candidate lists.
  void RemoveLiveFiles(
      std::vector<ObsoleteFileInfo>& sst_delete_candidates,
      std::vector<ObsoleteBlobFileInfo>& blob_delete_candidates) const;

  // Return the approximate size of data to be scanned for range [start, end)
  // in levels [start_level, end_level). If end_level == -1 it will search
  // through all non-empty levels
  uint64_t ApproximateSize(const SizeApproximationOptions& options,
                           const ReadOptions& read_options, Version* v,
                           const Slice& start, const Slice& end,
                           int start_level, int end_level,
                           TableReaderCaller caller);

  // Return the size of the current manifest file
  uint64_t manifest_file_size() const { return manifest_file_size_; }

  Status GetMetadataForFile(uint64_t number, int* filelevel,
                            FileMetaData** metadata, ColumnFamilyData** cfd);

  // This function doesn't support leveldb SST filenames
  void GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata);

  void AddObsoleteBlobFile(uint64_t blob_file_number, std::string path) {
    obsolete_blob_files_.emplace_back(blob_file_number, std::move(path));
  }

  void GetObsoleteFiles(std::vector<ObsoleteFileInfo>* files,
                        std::vector<ObsoleteBlobFileInfo>* blob_files,
                        std::vector<std::string>* manifest_filenames,
                        uint64_t min_pending_output);

  // REQUIRES: DB mutex held
  uint64_t GetObsoleteSstFilesSize() const;

  ColumnFamilySet* GetColumnFamilySet() { return column_family_set_.get(); }

  const UnorderedMap<uint32_t, size_t>& GetRunningColumnFamiliesTimestampSize()
      const {
    return column_family_set_->GetRunningColumnFamiliesTimestampSize();
  }

  const UnorderedMap<uint32_t, size_t>&
  GetColumnFamiliesTimestampSizeForRecord() const {
    return column_family_set_->GetColumnFamiliesTimestampSizeForRecord();
  }

  RefedColumnFamilySet GetRefedColumnFamilySet() {
    return RefedColumnFamilySet(GetColumnFamilySet());
  }

  const FileOptions& file_options() { return file_options_; }
  void ChangeFileOptions(const MutableDBOptions& new_options) {
    file_options_.writable_file_max_buffer_size =
        new_options.writable_file_max_buffer_size;
  }

  // TODO - Consider updating together when file options change in SetDBOptions
  const OffpeakTimeOption& offpeak_time_option() {
    return offpeak_time_option_;
  }
  void ChangeOffpeakTimeOption(const std::string& daily_offpeak_time_utc) {
    offpeak_time_option_.SetFromOffpeakTimeString(daily_offpeak_time_utc);
  }

  const ImmutableDBOptions* db_options() const { return db_options_; }

  static uint64_t GetNumLiveVersions(Version* dummy_versions);

  static uint64_t GetTotalSstFilesSize(Version* dummy_versions);

  static uint64_t GetTotalBlobFileSize(Version* dummy_versions);

  // Get the IO Status returned by written Manifest.
  const IOStatus& io_status() const { return io_status_; }

  // The returned WalSet needs to be accessed with DB mutex held.
  const WalSet& GetWalSet() const { return wals_; }

  void TEST_CreateAndAppendVersion(ColumnFamilyData* cfd) {
    assert(cfd);

    const auto& mutable_cf_options = *cfd->GetLatestMutableCFOptions();
    Version* const version =
        new Version(cfd, this, file_options_, mutable_cf_options, io_tracer_);

    constexpr bool update_stats = false;
    // TODO: plumb Env::IOActivity, Env::IOPriority
    const ReadOptions read_options;
    version->PrepareAppend(mutable_cf_options, read_options, update_stats);
    AppendVersion(cfd, version);
  }

 protected:
  struct ManifestWriter;

  friend class Version;
  friend class VersionEditHandler;
  friend class VersionEditHandlerPointInTime;
  friend class DumpManifestHandler;
  friend class DBImpl;
  friend class DBImplReadOnly;

  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t /*bytes*/, const Status& s) override {
      if (status->ok()) {
        *status = s;
      }
    }
  };

  void Reset();

  // Returns approximated offset of a key in a file for a given version.
  uint64_t ApproximateOffsetOf(const ReadOptions& read_options, Version* v,
                               const FdWithKeyRange& f, const Slice& key,
                               TableReaderCaller caller);

  // Returns approximated data size between start and end keys in a file
  // for a given version.
  uint64_t ApproximateSize(const ReadOptions& read_options, Version* v,
                           const FdWithKeyRange& f, const Slice& start,
                           const Slice& end, TableReaderCaller caller);

  struct MutableCFState {
    uint64_t log_number;
    std::string full_history_ts_low;

    explicit MutableCFState() = default;
    explicit MutableCFState(uint64_t _log_number, std::string ts_low)
        : log_number(_log_number), full_history_ts_low(std::move(ts_low)) {}
  };

  // Save current contents to *log
  Status WriteCurrentStateToManifest(
      const WriteOptions& write_options,
      const std::unordered_map<uint32_t, MutableCFState>& curr_state,
      const VersionEdit& wal_additions, log::Writer* log, IOStatus& io_s);

  void AppendVersion(ColumnFamilyData* column_family_data, Version* v);

  ColumnFamilyData* CreateColumnFamily(const ColumnFamilyOptions& cf_options,
                                       const ReadOptions& read_options,
                                       const VersionEdit* edit);

  Status VerifyFileMetadata(const ReadOptions& read_options,
                            ColumnFamilyData* cfd, const std::string& fpath,
                            int level, const FileMetaData& meta);

  // Protected by DB mutex.
  WalSet wals_;

  std::unique_ptr<ColumnFamilySet> column_family_set_;
  Cache* table_cache_;
  Env* const env_;
  FileSystemPtr const fs_;
  SystemClock* const clock_;
  const std::string dbname_;
  std::string db_id_;
  const ImmutableDBOptions* const db_options_;
  std::atomic<uint64_t> next_file_number_;
  // Any WAL number smaller than this should be ignored during recovery,
  // and is qualified for being deleted.
  std::atomic<uint64_t> min_log_number_to_keep_ = {0};
  uint64_t manifest_file_number_;
  uint64_t options_file_number_;
  uint64_t options_file_size_;
  uint64_t pending_manifest_file_number_;
  // The last seq visible to reads. It normally indicates the last sequence in
  // the memtable but when using two write queues it could also indicate the
  // last sequence in the WAL visible to reads.
  std::atomic<uint64_t> last_sequence_;
  // The last sequence number of data committed to the descriptor (manifest
  // file).
  SequenceNumber descriptor_last_sequence_ = 0;
  // The last seq that is already allocated. It is applicable only when we have
  // two write queues. In that case seq might or might not have appreated in
  // memtable but it is expected to appear in the WAL.
  // We have last_sequence <= last_allocated_sequence_
  std::atomic<uint64_t> last_allocated_sequence_;
  // The last allocated sequence that is also published to the readers. This is
  // applicable only when last_seq_same_as_publish_seq_ is not set. Otherwise
  // last_sequence_ also indicates the last published seq.
  // We have last_sequence <= last_published_sequence_ <=
  // last_allocated_sequence_
  std::atomic<uint64_t> last_published_sequence_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  std::unique_ptr<log::Writer> descriptor_log_;

  // generates a increasing version number for every new version
  uint64_t current_version_number_;

  // Queue of writers to the manifest file
  std::deque<ManifestWriter*> manifest_writers_;

  // Current size of manifest file
  uint64_t manifest_file_size_;

  // Obsolete files, or during DB shutdown any files not referenced by what's
  // left of the in-memory LSM state.
  std::vector<ObsoleteFileInfo> obsolete_files_;
  std::vector<ObsoleteBlobFileInfo> obsolete_blob_files_;
  std::vector<std::string> obsolete_manifests_;

  // env options for all reads and writes except compactions
  FileOptions file_options_;

  BlockCacheTracer* const block_cache_tracer_;

  // Store the IO status when Manifest is written
  IOStatus io_status_;

  std::shared_ptr<IOTracer> io_tracer_;

  std::string db_session_id_;

  // Off-peak time option used for compaction scoring
  OffpeakTimeOption offpeak_time_option_;

  // Pointer to the DB's ErrorHandler.
  ErrorHandler* const error_handler_;

 private:
  // REQUIRES db mutex at beginning. may release and re-acquire db mutex
  Status ProcessManifestWrites(std::deque<ManifestWriter>& writers,
                               InstrumentedMutex* mu,
                               FSDirectory* dir_contains_current_file,
                               bool new_descriptor_log,
                               const ColumnFamilyOptions* new_cf_options,
                               const ReadOptions& read_options,
                               const WriteOptions& write_options);

  void LogAndApplyCFHelper(VersionEdit* edit,
                           SequenceNumber* max_last_sequence);
  Status LogAndApplyHelper(ColumnFamilyData* cfd, VersionBuilder* b,
                           VersionEdit* edit, SequenceNumber* max_last_sequence,
                           InstrumentedMutex* mu);

  const bool read_only_;
  bool closed_;
};

// ReactiveVersionSet represents a collection of versions of the column
// families of the database. Users of ReactiveVersionSet, e.g. DBImplSecondary,
// need to replay the MANIFEST (description log in older terms) in order to
// reconstruct and install versions.
class ReactiveVersionSet : public VersionSet {
 public:
  ReactiveVersionSet(const std::string& dbname,
                     const ImmutableDBOptions* _db_options,
                     const FileOptions& _file_options, Cache* table_cache,
                     WriteBufferManager* write_buffer_manager,
                     WriteController* write_controller,
                     const std::shared_ptr<IOTracer>& io_tracer);

  ~ReactiveVersionSet() override;

  Status Close(FSDirectory* /*db_dir*/, InstrumentedMutex* /*mu*/) override {
    return Status::OK();
  }

  Status ReadAndApply(
      InstrumentedMutex* mu,
      std::unique_ptr<log::FragmentBufferedReader>* manifest_reader,
      Status* manifest_read_status,
      std::unordered_set<ColumnFamilyData*>* cfds_changed,
      std::vector<std::string>* files_to_delete);

  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families,
                 std::unique_ptr<log::FragmentBufferedReader>* manifest_reader,
                 std::unique_ptr<log::Reader::Reporter>* manifest_reporter,
                 std::unique_ptr<Status>* manifest_reader_status);
#ifndef NDEBUG
  uint64_t TEST_read_edits_in_atomic_group() const;
#endif  //! NDEBUG

  std::vector<VersionEdit>& replay_buffer();

 protected:
  // REQUIRES db mutex
  Status ApplyOneVersionEditToBuilder(
      VersionEdit& edit, std::unordered_set<ColumnFamilyData*>* cfds_changed,
      VersionEdit* version_edit);

  Status MaybeSwitchManifest(
      log::Reader::Reporter* reporter,
      std::unique_ptr<log::FragmentBufferedReader>* manifest_reader);

 private:
  std::unique_ptr<ManifestTailer> manifest_tailer_;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options_;
  using VersionSet::LogAndApply;
  using VersionSet::Recover;

  Status LogAndApply(
      const autovector<ColumnFamilyData*>& /*cfds*/,
      const autovector<const MutableCFOptions*>& /*mutable_cf_options_list*/,
      const ReadOptions& /* read_options */,
      const WriteOptions& /* write_options */,
      const autovector<autovector<VersionEdit*>>& /*edit_lists*/,
      InstrumentedMutex* /*mu*/, FSDirectory* /*dir_contains_current_file*/,
      bool /*new_descriptor_log*/, const ColumnFamilyOptions* /*new_cf_option*/,
      const std::vector<std::function<void(const Status&)>>& /*manifest_wcbs*/)
      override {
    return Status::NotSupported("not supported in reactive mode");
  }

  // No copy allowed
  ReactiveVersionSet(const ReactiveVersionSet&);
  ReactiveVersionSet& operator=(const ReactiveVersionSet&);
};

}  // namespace ROCKSDB_NAMESPACE
