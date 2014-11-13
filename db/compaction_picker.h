//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <vector>
#include <memory>
#include <set>
#include <unordered_set>

#include "db/version_set.h"
#include "db/compaction.h"
#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"
#include "util/mutable_cf_options.h"

#include <vector>
#include <memory>
#include <set>
#include <string>

namespace rocksdb {

class LogBuffer;
class Compaction;
class VersionStorageInfo;
struct CompactionInputFiles;

class CompactionPicker {
 public:
  CompactionPicker(const ImmutableCFOptions& ioptions,
                   const InternalKeyComparator* icmp);
  virtual ~CompactionPicker();

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  virtual Compaction* PickCompaction(const std::string& cf_name,
                                     const MutableCFOptions& mutable_cf_options,
                                     VersionStorageInfo* vstorage,
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
  virtual Compaction* CompactRange(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, int input_level, int output_level,
      uint32_t output_path_id, const InternalKey* begin, const InternalKey* end,
      InternalKey** compaction_end);

  // Given the current number of levels, returns the lowest allowed level
  // for compaction input.
  virtual int MaxInputLevel(int current_num_levels) const = 0;

  // The maximum allowed output level.  Default value is NumberLevels() - 1.
  virtual int MaxOutputLevel() const {
    return NumberLevels() - 1;
  }

  virtual bool NeedsCompaction(const VersionStorageInfo* vstorage) const = 0;

  // Sanitize the input set of compaction input files.
  // When the input parameters do not describe a valid compaction, the
  // function will try to fix the input_files by adding necessary
  // files.  If it's not possible to conver an invalid input_files
  // into a valid one by adding more files, the function will return a
  // non-ok status with specific reason.
#ifndef ROCKSDB_LITE
  Status SanitizeCompactionInputFiles(
      std::unordered_set<uint64_t>* input_files,
      const ColumnFamilyMetaData& cf_meta,
      const int output_level) const;
#endif  // ROCKSDB_LITE

  // Free up the files that participated in a compaction
  void ReleaseCompactionFiles(Compaction* c, Status status);

  // Return the total amount of data that is undergoing
  // compactions per level
  void SizeBeingCompacted(std::vector<uint64_t>& sizes);

  // Returns true if any one of the specified files are being compacted
  bool FilesInCompaction(const std::vector<FileMetaData*>& files);

  // Takes a list of CompactionInputFiles and returns a Compaction object.
  Compaction* FormCompaction(
      const CompactionOptions& compact_options,
      const autovector<CompactionInputFiles>& input_files,
      int output_level, VersionStorageInfo* vstorage,
      const MutableCFOptions& mutable_cf_options) const;

  // Converts a set of compaction input file numbers into
  // a list of CompactionInputFiles.
  Status GetCompactionInputsFromFileNumbers(
      autovector<CompactionInputFiles>* input_files,
      std::unordered_set<uint64_t>* input_set,
      const VersionStorageInfo* vstorage,
      const CompactionOptions& compact_options) const;

 protected:
  int NumberLevels() const { return ioptions_.num_levels; }

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
  bool ExpandWhileOverlapping(const std::string& cf_name,
                              VersionStorageInfo* vstorage, Compaction* c);

  // Returns true if any one of the parent files are being compacted
  bool ParentRangeInCompaction(VersionStorageInfo* vstorage,
                               const InternalKey* smallest,
                               const InternalKey* largest, int level,
                               int* index);

  void SetupOtherInputs(const std::string& cf_name,
                        const MutableCFOptions& mutable_cf_options,
                        VersionStorageInfo* vstorage, Compaction* c);

  const ImmutableCFOptions& ioptions_;

  // A helper function to SanitizeCompactionInputFiles() that
  // sanitizes "input_files" by adding necessary files.
#ifndef ROCKSDB_LITE
  virtual Status SanitizeCompactionInputFilesForAllLevels(
      std::unordered_set<uint64_t>* input_files,
      const ColumnFamilyMetaData& cf_meta,
      const int output_level) const;
#endif  // ROCKSDB_LITE

  // record all the ongoing compactions for all levels
  std::vector<std::set<Compaction*>> compactions_in_progress_;

  const InternalKeyComparator* const icmp_;
};

class UniversalCompactionPicker : public CompactionPicker {
 public:
  UniversalCompactionPicker(const ImmutableCFOptions& ioptions,
                            const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}
  virtual Compaction* PickCompaction(const std::string& cf_name,
                                     const MutableCFOptions& mutable_cf_options,
                                     VersionStorageInfo* vstorage,
                                     LogBuffer* log_buffer) override;

  // The maxinum allowed input level.  Always returns 0.
  virtual int MaxInputLevel(int current_num_levels) const override {
    return 0;
  }

  // The maximum allowed output level.  Always returns 0.
  virtual int MaxOutputLevel() const override {
    return 0;
  }

  virtual bool NeedsCompaction(const VersionStorageInfo* vstorage) const
      override;

 private:
  // Pick Universal compaction to limit read amplification
  Compaction* PickCompactionUniversalReadAmp(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, double score, unsigned int ratio,
      unsigned int num_files, LogBuffer* log_buffer);

  // Pick Universal compaction to limit space amplification.
  Compaction* PickCompactionUniversalSizeAmp(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, double score, LogBuffer* log_buffer);

  // Pick a path ID to place a newly generated file, with its estimated file
  // size.
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            uint64_t file_size);
};

class LevelCompactionPicker : public CompactionPicker {
 public:
  LevelCompactionPicker(const ImmutableCFOptions& ioptions,
                        const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}
  virtual Compaction* PickCompaction(const std::string& cf_name,
                                     const MutableCFOptions& mutable_cf_options,
                                     VersionStorageInfo* vstorage,
                                     LogBuffer* log_buffer) override;

  // Returns current_num_levels - 2, meaning the last level cannot be
  // compaction input level.
  virtual int MaxInputLevel(int current_num_levels) const override {
    return current_num_levels - 2;
  }

  virtual bool NeedsCompaction(const VersionStorageInfo* vstorage) const
      override;

 private:
  // For the specfied level, pick a compaction.
  // Returns nullptr if there is no compaction to be done.
  // If level is 0 and there is already a compaction on that level, this
  // function will return nullptr.
  Compaction* PickCompactionBySize(const MutableCFOptions& mutable_cf_options,
                                   VersionStorageInfo* vstorage, int level,
                                   double score);
};

class FIFOCompactionPicker : public CompactionPicker {
 public:
  FIFOCompactionPicker(const ImmutableCFOptions& ioptions,
                       const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}

  virtual Compaction* PickCompaction(const std::string& cf_name,
                                     const MutableCFOptions& mutable_cf_options,
                                     VersionStorageInfo* version,
                                     LogBuffer* log_buffer) override;

  virtual Compaction* CompactRange(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, int input_level, int output_level,
      uint32_t output_path_id, const InternalKey* begin, const InternalKey* end,
      InternalKey** compaction_end) override;

  // The maxinum allowed input level.  Always returns 0.
  virtual int MaxInputLevel(int current_num_levels) const override {
    return 0;
  }

  // The maximum allowed output level.  Always returns 0.
  virtual int MaxOutputLevel() const override {
    return 0;
  }

  virtual bool NeedsCompaction(const VersionStorageInfo* vstorage) const
      override;
};

class NullCompactionPicker : public CompactionPicker {
 public:
  NullCompactionPicker(const ImmutableCFOptions& ioptions,
                       const InternalKeyComparator* icmp) :
      CompactionPicker(ioptions, icmp) {}
  virtual ~NullCompactionPicker() {}

  // Always return "nullptr"
  Compaction* PickCompaction(const std::string& cf_name,
                             const MutableCFOptions& mutable_cf_options,
                             VersionStorageInfo* vstorage,
                             LogBuffer* log_buffer) override {
    return nullptr;
  }

  // Always return "nullptr"
  Compaction* CompactRange(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, int input_level, int output_level,
      uint32_t output_path_id, const InternalKey* begin, const InternalKey* end,
      InternalKey** compaction_end) override {
    return nullptr;
  }

  // Given the current number of levels, returns the highest allowed level
  // for compaction input.
  virtual int MaxInputLevel(int current_num_levels) const {
    return current_num_levels - 2;
  }

  // Always returns false.
  virtual bool NeedsCompaction(const VersionStorageInfo* vstorage) const
      override {
    return false;
  }
};

// Utility function
extern uint64_t TotalCompensatedFileSize(const std::vector<FileMetaData*>& files);

}  // namespace rocksdb
