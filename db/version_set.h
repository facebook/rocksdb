//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#pragma once
#include <map>
#include <memory>
#include <set>
#include <vector>
#include <deque>
#include <atomic>
#include <limits>
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "db/table_cache.h"
#include "db/compaction.h"
#include "db/compaction_picker.h"
#include "db/column_family.h"
#include "db/log_reader.h"
#include "db/file_indexer.h"

namespace rocksdb {

namespace log { class Writer; }

class Compaction;
class CompactionPicker;
class Iterator;
class LogBuffer;
class LookupKey;
class MemTable;
class Version;
class VersionSet;
class MergeContext;
class ColumnFamilyData;
class ColumnFamilySet;
class TableCache;
class MergeIteratorBuilder;

// Return the smallest index i such that file_level.files[i]->largest >= key.
// Return file_level.num_files if there is no such file.
// REQUIRES: "file_level.files" contains a sorted list of
// non-overlapping files.
extern int FindFile(const InternalKeyComparator& icmp,
                    const FileLevel& file_level,
                    const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, file_level.files[]
// contains disjoint ranges in sorted order.
extern bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const FileLevel& file_level,
    const Slice* smallest_user_key,
    const Slice* largest_user_key);

// Generate FileLevel from vector<FdWithKeyRange*>
// Would copy smallest_key and largest_key data to sequential memory
// arena: Arena used to allocate the memory
extern void DoGenerateFileLevel(FileLevel* file_level,
        const std::vector<FileMetaData*>& files,
        Arena* arena);

class Version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&, const EnvOptions& soptions,
                    std::vector<Iterator*>* iters);

  void AddIterators(const ReadOptions&, const EnvOptions& soptions,
                    MergeIteratorBuilder* merger_iter_builder);

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.
  // Uses *operands to store merge_operator operations to apply later
  // REQUIRES: lock is not held
  void Get(const ReadOptions&, const LookupKey& key, std::string* val,
           Status* status, MergeContext* merge_context,
           bool* value_found = nullptr);

  // Updates internal structures that keep track of compaction scores
  // We use compaction scores to figure out which compaction to do next
  // REQUIRES: If Version is not yet saved to current_, it can be called without
  // a lock. Once a version is saved to current_, call only with mutex held
  void ComputeCompactionScore(std::vector<uint64_t>& size_being_compacted);

  // Generate file_levels_ from files_
  void GenerateFileLevels();

  // Update scores, pre-calculated variables. It needs to be called before
  // applying the version to the version set.
  void PrepareApply(std::vector<uint64_t>& size_being_compacted);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  // Decrease reference count. Delete the object if no reference left
  // and return true. Otherwise, return false.
  bool Unref();

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const;

  // Returns the maxmimum compaction score for levels 1 to max
  double MaxCompactionScore() const { return max_compaction_score_; }

  // See field declaration
  int MaxCompactionScoreLevel() const { return max_compaction_score_level_; }

  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,         // nullptr means before all keys
      const InternalKey* end,           // nullptr means after all keys
      std::vector<FileMetaData*>* inputs,
      int hint_index = -1,              // index of overlap file
      int* file_index = nullptr);          // return index of overlap file

  void GetOverlappingInputsBinarySearch(
      int level,
      const Slice& begin,         // nullptr means before all keys
      const Slice& end,           // nullptr means after all keys
      std::vector<FileMetaData*>* inputs,
      int hint_index,             // index of overlap file
      int* file_index);           // return index of overlap file

  void ExtendOverlappingInputs(
      int level,
      const Slice& begin,         // nullptr means before all keys
      const Slice& end,           // nullptr means after all keys
      std::vector<FileMetaData*>* inputs,
      unsigned int index);                 // start extending from this index

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==NULL represents a key smaller than all keys in the DB.
  // largest_user_key==NULL represents a key largest than all keys in the DB.
  bool OverlapInLevel(int level,
                      const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Returns true iff the first or last file in inputs contains
  // an overlapping user key to the file "just outside" of it (i.e.
  // just after the last file, or just before the first file)
  // REQUIRES: "*inputs" is a sorted list of non-overlapping files
  bool HasOverlappingUserKey(const std::vector<FileMetaData*>* inputs,
                             int level);


  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  int NumberLevels() const { return num_levels_; }

  // REQUIRES: lock is held
  int NumLevelFiles(int level) const { return files_[level].size(); }

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  struct FileSummaryStorage {
    char buffer[1000];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;
  // Return a human-readable short (single-line) summary of files
  // in a specified level.  Uses *scratch as backing store.
  const char* LevelFileSummary(FileSummaryStorage* scratch, int level) const;

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Add all files listed in the current version to *live.
  void AddLiveFiles(std::vector<FileDescriptor>* live);

  // Return a human readable string that describes this version's contents.
  std::string DebugString(bool hex = false) const;

  // Returns the version nuber of this version
  uint64_t GetVersionNumber() const { return version_number_; }

  uint64_t GetAverageValueSize() const {
    if (num_non_deletions_ == 0) {
      return 0;
    }
    assert(total_raw_key_size_ + total_raw_value_size_ > 0);
    assert(total_file_size_ > 0);
    return total_raw_value_size_ / num_non_deletions_ * total_file_size_ /
           (total_raw_key_size_ + total_raw_value_size_);
  }

  // REQUIRES: lock is held
  // On success, "tp" will contains the table properties of the file
  // specified in "file_meta".  If the file name of "file_meta" is
  // known ahread, passing it by a non-null "fname" can save a
  // file-name conversion.
  Status GetTableProperties(std::shared_ptr<const TableProperties>* tp,
                            const FileMetaData* file_meta,
                            const std::string* fname = nullptr);

  // REQUIRES: lock is held
  // On success, *props will be populated with all SSTables' table properties.
  // The keys of `props` are the sst file name, the values of `props` are the
  // tables' propertis, represented as shared_ptr.
  Status GetPropertiesOfAllTables(TablePropertiesCollection* props);

  uint64_t GetEstimatedActiveKeys();

  size_t GetMemoryUsageByTableReaders();

  // used to sort files by size
  struct Fsize {
    int index;
    FileMetaData* file;
  };

 private:
  friend class Compaction;
  friend class VersionSet;
  friend class DBImpl;
  friend class ColumnFamilyData;
  friend class CompactionPicker;
  friend class LevelCompactionPicker;
  friend class UniversalCompactionPicker;
  friend class FIFOCompactionPicker;
  friend class ForwardIterator;
  friend class InternalStats;

  class LevelFileNumIterator;
  class LevelFileIteratorState;

  bool PrefixMayMatch(const ReadOptions& options, Iterator* level_iter,
                      const Slice& internal_prefix) const;

  // Update num_non_empty_levels_.
  void UpdateNumNonEmptyLevels();

  // The helper function of UpdateTemporaryStats, which may fill the missing
  // fields of file_mata from its associated TableProperties.
  // Returns true if it does initialize FileMetaData.
  bool MaybeInitializeFileMetaData(FileMetaData* file_meta);

  // Update the temporary stats associated with the current version.
  // This temporary stats will be used in compaction.
  void UpdateTemporaryStats();

  // Sort all files for this version based on their file size and
  // record results in files_by_size_. The largest files are listed first.
  void UpdateFilesBySize();

  ColumnFamilyData* cfd_;  // ColumnFamilyData to which this Version belongs
  const InternalKeyComparator* internal_comparator_;
  const Comparator* user_comparator_;
  TableCache* table_cache_;
  const MergeOperator* merge_operator_;

  autovector<FileLevel> file_levels_;   // A copy of list of files per level
  Logger* info_log_;
  Statistics* db_statistics_;
  int num_levels_;              // Number of levels
  int num_non_empty_levels_;    // Number of levels. Any level larger than it
                                // is guaranteed to be empty.
  FileIndexer file_indexer_;
  VersionSet* vset_;            // VersionSet to which this Version belongs
  Arena arena_;                 // Used to allocate space for file_levels_
  Version* next_;               // Next version in linked list
  Version* prev_;               // Previous version in linked list
  int refs_;                    // Number of live refs to this version

  // List of files per level, files in each level are arranged
  // in increasing order of keys
  std::vector<FileMetaData*>* files_;

  // A list for the same set of files that are stored in files_,
  // but files in each level are now sorted based on file
  // size. The file with the largest size is at the front.
  // This vector stores the index of the file from files_.
  std::vector<std::vector<int>> files_by_size_;

  // An index into files_by_size_ that specifies the first
  // file that is not yet compacted
  std::vector<int> next_file_to_compact_by_size_;

  // Only the first few entries of files_by_size_ are sorted.
  // There is no need to sort all the files because it is likely
  // that on a running system, we need to look at only the first
  // few largest files because a new version is created every few
  // seconds/minutes (because of concurrent compactions).
  static const size_t number_of_files_to_sort_ = 50;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  // The most critical level to be compacted is listed first
  // These are used to pick the best compaction level
  std::vector<double> compaction_score_;
  std::vector<int> compaction_level_;
  double max_compaction_score_; // max score in l1 to ln-1
  int max_compaction_score_level_; // level on which max score occurs

  // A version number that uniquely represents this version. This is
  // used for debugging and logging purposes only.
  uint64_t version_number_;

  Version(ColumnFamilyData* cfd, VersionSet* vset, uint64_t version_number = 0);

  // total file size
  uint64_t total_file_size_;
  // the total size of all raw keys.
  uint64_t total_raw_key_size_;
  // the total size of all raw values.
  uint64_t total_raw_value_size_;
  // total number of non-deletion entries
  uint64_t num_non_deletions_;
  // total number of deletion entries
  uint64_t num_deletions_;

  ~Version();

  // re-initializes the index that is used to offset into files_by_size_
  // to find the next compaction candidate file.
  void ResetNextCompactionIndex(int level) {
    next_file_to_compact_by_size_[level] = 0;
  }

  // No copying allowed
  Version(const Version&);
  void operator=(const Version&);
};

class VersionSet {
 public:
  VersionSet(const std::string& dbname, const DBOptions* options,
             const EnvOptions& storage_options, Cache* table_cache);
  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // column_family_options has to be set if edit is column family add
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(ColumnFamilyData* column_family_data, VersionEdit* edit,
                     port::Mutex* mu, Directory* db_directory = nullptr,
                     bool new_descriptor_log = false,
                     const ColumnFamilyOptions* column_family_options =
                         nullptr);

  // Recover the last saved descriptor from persistent storage.
  // If read_only == true, Recover() will not complain if some column families
  // are not opened
  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families,
                 bool read_only = false);

  // Reads a manifest file and returns a list of column families in
  // column_families.
  static Status ListColumnFamilies(std::vector<std::string>* column_families,
                                   const std::string& dbname, Env* env);

#ifndef ROCKSDB_LITE
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
                                     const EnvOptions& storage_options,
                                     int new_levels);

  // printf contents (for debugging)
  Status DumpManifest(Options& options, std::string& manifestFileName,
                      bool verbose, bool hex = false);

#endif  // ROCKSDB_LITE

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  uint64_t PendingManifestFileNumber() const {
    return pending_manifest_file_number_;
  }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseLogFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the last sequence number.
  uint64_t LastSequence() const {
    return last_sequence_.load(std::memory_order_acquire);
  }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_.store(s, std::memory_order_release);
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Returns the minimum log number such that all
  // log numbers less than or equal to it can be deleted
  uint64_t MinLogNumber() const {
    uint64_t min_log_num = std::numeric_limits<uint64_t>::max();
    for (auto cfd : *column_family_set_) {
      if (min_log_num > cfd->GetLogNumber()) {
        min_log_num = cfd->GetLogNumber();
      }
    }
    return min_log_num;
  }

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Add all files listed in any live version to *live.
  void AddLiveFiles(std::vector<FileDescriptor>* live_list);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return the size of the current manifest file
  uint64_t ManifestFileSize() const { return manifest_file_size_; }

  // verify that the files that we started with for a compaction
  // still exist in the current version and in the same original level.
  // This ensures that a concurrent compaction did not erroneously
  // pick the same files to compact.
  bool VerifyCompactionFileConsistency(Compaction* c);

  Status GetMetadataForFile(uint64_t number, int* filelevel,
                            FileMetaData** metadata, ColumnFamilyData** cfd);

  void GetLiveFilesMetaData(
    std::vector<LiveFileMetaData> *metadata);

  void GetObsoleteFiles(std::vector<FileMetaData*>* files);

  ColumnFamilySet* GetColumnFamilySet() { return column_family_set_.get(); }

 private:
  class Builder;
  struct ManifestWriter;

  friend class Version;

  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(ColumnFamilyData* column_family_data, Version* v);

  bool ManifestContains(uint64_t manifest_file_number,
                        const std::string& record) const;

  ColumnFamilyData* CreateColumnFamily(const ColumnFamilyOptions& options,
                                       VersionEdit* edit);

  std::unique_ptr<ColumnFamilySet> column_family_set_;

  Env* const env_;
  const std::string dbname_;
  const DBOptions* const options_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t pending_manifest_file_number_;
  std::atomic<uint64_t> last_sequence_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  unique_ptr<log::Writer> descriptor_log_;

  // generates a increasing version number for every new version
  uint64_t current_version_number_;

  // Queue of writers to the manifest file
  std::deque<ManifestWriter*> manifest_writers_;

  // Current size of manifest file
  uint64_t manifest_file_size_;

  std::vector<FileMetaData*> obsolete_files_;

  // storage options for all reads and writes except compactions
  const EnvOptions& storage_options_;

  // storage options used for compactions. This is a copy of
  // storage_options_ but with readaheads set to readahead_compactions_.
  const EnvOptions storage_options_compactions_;

  // No copying allowed
  VersionSet(const VersionSet&);
  void operator=(const VersionSet&);

  void LogAndApplyCFHelper(VersionEdit* edit);
  void LogAndApplyHelper(ColumnFamilyData* cfd, Builder* b, Version* v,
                         VersionEdit* edit, port::Mutex* mu);
};

}  // namespace rocksdb
