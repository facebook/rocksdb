//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "db/version_set.h"

namespace rocksdb {

class Version;
class ColumnFamilyData;

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

  // Returns input version of the compaction
  Version* input_version() const { return input_version_; }

  ColumnFamilyData* column_family_data() const { return cfd_; }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  std::vector<FileMetaData*>* inputs(int which) { return &inputs_[which]; }

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

  // Clear all files to indicate that they are not being compacted
  // Delete this compaction from the list of running compactions.
  void ReleaseCompactionFiles(Status status);

  void Summary(char* output, int len);

  // Return the score that was used to pick this compaction run.
  double score() const { return score_; }

  // Is this compaction creating a file in the bottom most level?
  bool BottomMostLevel() { return bottommost_level_; }

  // Does this compaction include all sst files?
  bool IsFullCompaction() { return is_full_compaction_; }

  // Was this compaction triggered manually by the client?
  bool IsManualCompaction() { return is_manual_compaction_; }

 private:
  friend class CompactionPicker;
  friend class UniversalCompactionPicker;
  friend class LevelCompactionPicker;

  Compaction(Version* input_version, int level, int out_level,
             uint64_t target_file_size, uint64_t max_grandparent_overlap_bytes,
             bool seek_compaction = false, bool enable_compression = true);

  int level_;
  int out_level_; // levels to which output files are stored
  uint64_t max_output_file_size_;
  uint64_t max_grandparent_overlap_bytes_;
  Version* input_version_;
  VersionEdit* edit_;
  int number_levels_;
  ColumnFamilyData* cfd_;

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

  // Is this compaction requested by the client?
  bool is_manual_compaction_;

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
