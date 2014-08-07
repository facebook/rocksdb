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
#include "db/compaction.h"
#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"

#include <vector>
#include <memory>
#include <set>

namespace rocksdb {

class LogBuffer;
class Compaction;
class Version;

class CompactionPicker {
 public:
  CompactionPicker(const Options* options, const InternalKeyComparator* icmp);
  virtual ~CompactionPicker();

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  virtual Compaction* PickCompaction(Version* version,
                                     LogBuffer* log_buffer) = 0;

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  //
  // The returned Compaction might not include the whole requested range.
  // In that case, compaction_end will be set to the next key that needs
  // compacting. In case the compaction will compact the whole range,
  // compaction_end will be set to nullptr.
  // Client is responsible for compaction_end storage -- when called,
  // *compaction_end should point to valid InternalKey!
  virtual Compaction* CompactRange(Version* version, int input_level,
                                   int output_level, uint32_t output_path_id,
                                   const InternalKey* begin,
                                   const InternalKey* end,
                                   InternalKey** compaction_end);

  // Given the current number of levels, returns the lowest allowed level
  // for compaction input.
  virtual int MaxInputLevel(int current_num_levels) const = 0;

  // Free up the files that participated in a compaction
  void ReleaseCompactionFiles(Compaction* c, Status status);

  // Return the total amount of data that is undergoing
  // compactions per level
  void SizeBeingCompacted(std::vector<uint64_t>& sizes);

  // Returns maximum total overlap bytes with grandparent
  // level (i.e., level+2) before we stop building a single
  // file in level->level+1 compaction.
  uint64_t MaxGrandParentOverlapBytes(int level);

  // Returns maximum total bytes of data on a given level.
  double MaxBytesForLevel(int level);

  // Get the max file size in a given level.
  uint64_t MaxFileSizeForLevel(int level) const;

 protected:
  int NumberLevels() const { return num_levels_; }

  // Stores the minimal range that covers all entries in inputs in
  // *smallest, *largest.
  // REQUIRES: inputs is not empty
  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  // Stores the minimal range that covers all entries in inputs1 and inputs2
  // in *smallest, *largest.
  // REQUIRES: inputs is not empty
  void GetRange(const std::vector<FileMetaData*>& inputs1,
                const std::vector<FileMetaData*>& inputs2,
                InternalKey* smallest, InternalKey* largest);

  // Add more files to the inputs on "level" to make sure that
  // no newer version of a key is compacted to "level+1" while leaving an older
  // version in a "level". Otherwise, any Get() will search "level" first,
  // and will likely return an old/stale value for the key, since it always
  // searches in increasing order of level to find the value. This could
  // also scramble the order of merge operands. This function should be
  // called any time a new Compaction is created, and its inputs_[0] are
  // populated.
  //
  // Will return false if it is impossible to apply this compaction.
  bool ExpandWhileOverlapping(Compaction* c);

  uint64_t ExpandedCompactionByteSizeLimit(int level);

  // Returns true if any one of the specified files are being compacted
  bool FilesInCompaction(std::vector<FileMetaData*>& files);

  // Returns true if any one of the parent files are being compacted
  bool ParentRangeInCompaction(Version* version, const InternalKey* smallest,
                               const InternalKey* largest, int level,
                               int* index);

  void SetupOtherInputs(Compaction* c);

  // record all the ongoing compactions for all levels
  std::vector<std::set<Compaction*>> compactions_in_progress_;

  // Per-level target file size.
  std::unique_ptr<uint64_t[]> max_file_size_;

  // Per-level max bytes
  std::unique_ptr<uint64_t[]> level_max_bytes_;

  const Options* const options_;

 private:
  int num_levels_;

  const InternalKeyComparator* const icmp_;
};

class UniversalCompactionPicker : public CompactionPicker {
 public:
  UniversalCompactionPicker(const Options* options,
                            const InternalKeyComparator* icmp)
      : CompactionPicker(options, icmp) {}
  virtual Compaction* PickCompaction(Version* version,
                                     LogBuffer* log_buffer) override;

  // The maxinum allowed input level.  Always return 0.
  virtual int MaxInputLevel(int current_num_levels) const override {
    return 0;
  }

 private:
  // Pick Universal compaction to limit read amplification
  Compaction* PickCompactionUniversalReadAmp(Version* version, double score,
                                             unsigned int ratio,
                                             unsigned int num_files,
                                             LogBuffer* log_buffer);

  // Pick Universal compaction to limit space amplification.
  Compaction* PickCompactionUniversalSizeAmp(Version* version, double score,
                                             LogBuffer* log_buffer);

  // Pick a path ID to place a newly generated file, with its estimated file
  // size.
  static uint32_t GetPathId(const Options& options, uint64_t file_size);
};

class LevelCompactionPicker : public CompactionPicker {
 public:
  LevelCompactionPicker(const Options* options,
                        const InternalKeyComparator* icmp)
      : CompactionPicker(options, icmp) {}
  virtual Compaction* PickCompaction(Version* version,
                                     LogBuffer* log_buffer) override;

  // Returns current_num_levels - 2, meaning the last level cannot be
  // compaction input level.
  virtual int MaxInputLevel(int current_num_levels) const override {
    return current_num_levels - 2;
  }

 private:
  // For the specfied level, pick a compaction.
  // Returns nullptr if there is no compaction to be done.
  // If level is 0 and there is already a compaction on that level, this
  // function will return nullptr.
  Compaction* PickCompactionBySize(Version* version, int level, double score);
};

class FIFOCompactionPicker : public CompactionPicker {
 public:
  FIFOCompactionPicker(const Options* options,
                       const InternalKeyComparator* icmp)
      : CompactionPicker(options, icmp) {}

  virtual Compaction* PickCompaction(Version* version,
                                     LogBuffer* log_buffer) override;

  virtual Compaction* CompactRange(Version* version, int input_level,
                                   int output_level, uint32_t output_path_id,
                                   const InternalKey* begin,
                                   const InternalKey* end,
                                   InternalKey** compaction_end) override;

  // The maxinum allowed input level.  Always return 0.
  virtual int MaxInputLevel(int current_num_levels) const override {
    return 0;
  }
};

// Utility function
extern uint64_t TotalCompensatedFileSize(const std::vector<FileMetaData*>& files);

}  // namespace rocksdb
