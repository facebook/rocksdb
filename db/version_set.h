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
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "db/table_cache.h"

namespace rocksdb {

namespace log { class Writer; }

class Compaction;
class Iterator;
class MemTable;
class TableCache;
class Version;
class VersionSet;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
extern int FindFile(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
extern bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key);

class Version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&, const EnvOptions& soptions,
                    std::vector<Iterator*>* iters);

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // Uses *operands to store merge_operator operations to apply later
  // REQUIRES: lock is not held
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };
  void Get(const ReadOptions&, const LookupKey& key, std::string* val,
           Status* status, std::deque<std::string>* operands, GetStats* stats,
           const Options& db_option,
           bool* value_found = nullptr);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  bool UpdateStats(const GetStats& stats);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  void Unref();

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

  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  std::string DebugString(bool hex = false) const;

  // Returns the version nuber of this version
  uint64_t GetVersionNumber() {
    return version_number_;
  }

 private:
  friend class Compaction;
  friend class VersionSet;
  friend class DBImpl;

  class LevelFileNumIterator;
  Iterator* NewConcatenatingIterator(const ReadOptions&,
                                     const EnvOptions& soptions,
                                     int level) const;
  bool PrefixMayMatch(const ReadOptions& options, const EnvOptions& soptions,
                      const Slice& internal_prefix, Iterator* level_iter) const;

  VersionSet* vset_;            // VersionSet to which this Version belongs
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
  std::vector< std::vector<int> > files_by_size_;

  // An index into files_by_size_ that specifies the first
  // file that is not yet compacted
  std::vector<int> next_file_to_compact_by_size_;

  // Only the first few entries of files_by_size_ are sorted.
  // There is no need to sort all the files because it is likely
  // that on a running system, we need to look at only the first
  // few largest files because a new version is created every few
  // seconds/minutes (because of concurrent compactions).
  static const int number_of_files_to_sort_ = 50;

  // Next file to compact based on seek stats.
  FileMetaData* file_to_compact_;
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  // The most critical level to be compacted is listed first
  // These are used to pick the best compaction level
  std::vector<double> compaction_score_;
  std::vector<int> compaction_level_;
  double max_compaction_score_; // max score in l1 to ln-1
  int max_compaction_score_level_; // level on which max score occurs

  // The offset in the manifest file where this version is stored.
  uint64_t offset_manifest_file_;

  // A version number that uniquely represents this version. This is
  // used for debugging and logging purposes only.
  uint64_t version_number_;

  explicit Version(VersionSet* vset, uint64_t version_number = 0);

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
  VersionSet(const std::string& dbname,
             const Options* options,
             const EnvOptions& storage_options,
             TableCache* table_cache,
             const InternalKeyComparator*);
  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu,
      bool new_descriptor_log = false);

  // Recover the last saved descriptor from persistent storage.
  Status Recover();

  // Try to reduce the number of levels. This call is valid when
  // only one level from the new max level to the old
  // max level containing files.
  // For example, a db currently has 7 levels [0-6], and a call to
  // to reduce to 5 [0-4] can only be executed when only one level
  // among [4-6] contains files.
  Status ReduceNumberOfLevels(int new_levels, port::Mutex* mu);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  int NumberLevels() const { return num_levels_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(
      int level,
      const InternalKey* begin,
      const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction because it has
  // exceeded its target size.
  bool NeedsSizeCompaction() const {
    // In universal compaction case, this check doesn't really
    // check the compaction condition, but checks num of files threshold
    // only. We are not going to miss any compaction opportunity
    // but it's likely that more compactions are scheduled but
    // ending up with nothing to do. We can improve it later.
    // TODO: improve this function to be accurate for universal
    //       compactions.
    int num_levels_to_check =
        (options_->compaction_style != kCompactionStyleUniversal) ?
            NumberLevels() - 1 : 1;
    for (int i = 0; i < num_levels_to_check; i++) {
      if (current_->compaction_score_[i] >= 1) {
        return true;
      }
    }
    return false;
  }
  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    return ((current_->file_to_compact_ != nullptr) ||
            NeedsSizeCompaction());
  }

  // Returns the maxmimum compaction score for levels 1 to max
  double MaxCompactionScore() const {
    return current_->max_compaction_score_;
  }

  // See field declaration
  int MaxCompactionScoreLevel() const {
    return current_->max_compaction_score_level_;
  }

  // Add all files listed in any live version to *live.
  void AddLiveFiles(std::vector<uint64_t>* live_list);

  // Add all files listed in the current version to *live.
  void AddLiveFilesCurrentVersion(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  struct FileSummaryStorage {
    char buffer[1000];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

  // printf contents (for debugging)
  Status DumpManifest(Options& options, std::string& manifestFileName,
                      bool verbose, bool hex = false);

  // Return a human-readable short (single-line) summary of the data size
  // of files per level.  Uses *scratch as backing store.
  const char* LevelDataSizeSummary(LevelSummaryStorage* scratch) const;

  // Return a human-readable short (single-line) summary of files
  // in a specified level.  Uses *scratch as backing store.
  const char* LevelFileSummary(FileSummaryStorage* scratch, int level) const;

  // Return the size of the current manifest file
  const uint64_t ManifestFileSize() { return current_->offset_manifest_file_; }

  // For the specfied level, pick a compaction.
  // Returns nullptr if there is no compaction to be done.
  // If level is 0 and there is already a compaction on that level, this
  // function will return nullptr.
  Compaction* PickCompactionBySize(int level, double score);

  // Pick files to compact in Universal mode
  Compaction* PickCompactionUniversal(int level, double score);

  // Pick Universal compaction to limit read amplification
  Compaction* PickCompactionUniversalReadAmp(int level, double score,
                unsigned int ratio, unsigned int num_files);

  // Pick Universal compaction to limit space amplification.
  Compaction* PickCompactionUniversalSizeAmp(int level, double score);

  // Free up the files that were participated in a compaction
  void ReleaseCompactionFiles(Compaction* c, Status status);

  // verify that the files that we started with for a compaction
  // still exist in the current version and in the same original level.
  // This ensures that a concurrent compaction did not erroneously
  // pick the same files to compact.
  bool VerifyCompactionFileConsistency(Compaction* c);

  // used to sort files by size
  typedef struct fsize {
    int index;
    FileMetaData* file;
  } Fsize;

  // Sort all files for this version based on their file size and
  // record results in files_by_size_. The largest files are listed first.
  void UpdateFilesBySize(Version *v);

  // Get the max file size in a given level.
  uint64_t MaxFileSizeForLevel(int level);

  double MaxBytesForLevel(int level);

  Status GetMetadataForFile(
    uint64_t number, int *filelevel, FileMetaData *metadata);

  void GetLiveFilesMetaData(
    std::vector<LiveFileMetaData> *metadata);

  void GetObsoleteFiles(std::vector<FileMetaData*>* files);

 private:
  class Builder;
  struct ManifestWriter;

  friend class Compaction;
  friend class Version;

  void Init(int num_levels);

  void Finalize(Version* v, std::vector<uint64_t>&);

  void GetRange(const std::vector<FileMetaData*>& inputs,
                InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest,
                 InternalKey* largest);

  void ExpandWhileOverlapping(Compaction* c);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);

  bool ManifestContains(const std::string& record) const;

  uint64_t ExpandedCompactionByteSizeLimit(int level);

  uint64_t MaxGrandParentOverlapBytes(int level);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  int num_levels_;

  // Opened lazily
  unique_ptr<log::Writer> descriptor_log_;
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::string* compact_pointer_;

  // Per-level target file size.
  uint64_t* max_file_size_;

  // Per-level max bytes
  uint64_t* level_max_bytes_;

  // record all the ongoing compactions for all levels
  std::vector<std::set<Compaction*> > compactions_in_progress_;

  // generates a increasing version number for every new version
  uint64_t current_version_number_;

  // Queue of writers to the manifest file
  std::deque<ManifestWriter*> manifest_writers_;

  // Store the manifest file size when it is checked.
  // Save us the cost of checking file size twice in LogAndApply
  uint64_t last_observed_manifest_size_;

  std::vector<FileMetaData*> obsolete_files_;

  // storage options for all reads and writes except compactions
  const EnvOptions& storage_options_;

  // storage options used for compactions. This is a copy of
  // storage_options_ but with readaheads set to readahead_compactions_.
  const EnvOptions storage_options_compactions_;

  // No copying allowed
  VersionSet(const VersionSet&);
  void operator=(const VersionSet&);

  // Return the total amount of data that is undergoing
  // compactions per level
  void SizeBeingCompacted(std::vector<uint64_t>&);

  // Returns true if any one of the parent files are being compacted
  bool ParentRangeInCompaction(const InternalKey* smallest,
    const InternalKey* largest, int level, int* index);

  // Returns true if any one of the specified files are being compacted
  bool FilesInCompaction(std::vector<FileMetaData*>& files);

  void LogAndApplyHelper(Builder*b, Version* v,
                           VersionEdit* edit, port::Mutex* mu);
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // will be merged.
  int level() const { return level_; }

  // Outputs will go to this level
  int output_level() const { return out_level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Whether compression will be enabled for compaction outputs
  bool enable_compression() const { return enable_compression_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

  void Summary(char* output, int len);

  // Return the score that was used to pick this compaction run.
  double score() const { return score_; }

  // Is this compaction creating a file in the bottom most level?
  bool BottomMostLevel() { return bottommost_level_; }

  // Does this compaction include all sst files?
  bool IsFullCompaction() { return is_full_compaction_; }

 private:
  friend class Version;
  friend class VersionSet;

  explicit Compaction(int level, int out_level, uint64_t target_file_size,
    uint64_t max_grandparent_overlap_bytes, int number_levels,
    bool seek_compaction = false, bool enable_compression = true);

  int level_;
  int out_level_; // levels to which output files are stored
  uint64_t max_output_file_size_;
  uint64_t maxGrandParentOverlapBytes_;
  Version* input_version_;
  VersionEdit* edit_;
  int number_levels_;

  bool seek_compaction_;
  bool enable_compression_;

  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<FileMetaData*> inputs_[2];      // The two sets of inputs

  // State used to check for number of of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData*> grandparents_;
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  uint64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files
  int base_index_;   // index of the file in files_[level_]
  int parent_index_; // index of some file with same range in files_[level_+1]
  double score_;     // score that was used to pick this compaction.

  // Is this compaction creating a file in the bottom most level?
  bool bottommost_level_;
  // Does this compaction include all sst files?
  bool is_full_compaction_;

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  std::vector<size_t> level_ptrs_;

  // mark (or clear) all files that are being compacted
  void MarkFilesBeingCompacted(bool);

  // Initialize whether compaction producing files at the bottommost level
  void SetupBottomMostLevel(bool isManual);

  // In case of compaction error, reset the nextIndex that is used
  // to pick up the next file to be compacted from files_by_size_
  void ResetNextCompactionIndex();
};

}  // namespace rocksdb
