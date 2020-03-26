//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include <utility>
#include <vector>

#include "db/compaction/compaction_picker_level.h"
#include "logging/log_buffer.h"
#include "test_util/sync_point.h"

namespace rocksdb {

bool LevelCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  if (!vstorage->ExpiredTtlFiles().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
    return true;
  }
  if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
    if (vstorage->CompactionScore(i) >= 1) {
      return true;
    }
  }
  return false;
}

namespace {
// A class to build a leveled compaction step-by-step.
class LevelCompactionBuilder {
 public:
  LevelCompactionBuilder(const std::string& cf_name,
                         VersionStorageInfo* vstorage,
                         CompactionPicker* compaction_picker,
                         LogBuffer* log_buffer,
                         const MutableCFOptions& mutable_cf_options,
                         const ImmutableCFOptions& ioptions)
      : cf_name_(cf_name),
        vstorage_(vstorage),
        compaction_picker_(compaction_picker),
        log_buffer_(log_buffer),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions) {}

  // Pick and return a compaction.
  Compaction* PickCompaction();

  // Pick the initial files to compact to the next level. (or together
  // in Intra-L0 compactions)
  void SetupInitialFiles();

  // If the initial files are from L0 level, pick other L0
  // files if needed.Return false if compaction should be aborted
  int64_t SetupOtherL0FilesIfNeeded();

  // Based on initial files, setup other files need to be compacted
  // in this compaction, accordingly.
  bool SetupOtherInputsIfNeeded();

  Compaction* GetCompaction();

  // For the specfied level, pick a file that we want to compact.
  // Returns false if there is no file to compact.
  // If it returns true, inputs->files.size() will be exactly one.
  // If level is 0 and there is already a compaction on that level, this
  // function will return false.
  bool PickFileToCompact();

  // For L0->L0, picks the longest span of files that aren't currently
  // undergoing compaction for which work-per-deleted-file decreases. The span
  // always starts from the newest L0 file.
  //
  // Intra-L0 compaction is independent of all other files, so it can be
  // performed even when L0->base_level compactions are blocked.
  //
  // Returns true if `inputs` is populated with a span of files to be compacted;
  // otherwise, returns false.
  bool PickIntraL0Compaction();

  void PickExpiredTtlFiles();

  void PickFilesMarkedForPeriodicCompaction();

  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;
  int start_level_ = -1;
  int output_level_ = -1;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_manual_ = false;
  CompactionInputFiles start_level_inputs_;
  std::vector<CompactionInputFiles> compaction_inputs_;
  CompactionInputFiles output_level_inputs_;
  std::vector<FileMetaData*> grandparents_;
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;
  // will hold the ring number if Active Recycling called for
  size_t ringno_;
  // will hold the file number of the last file in the recycled area
  VLogRingRefFileno lastfileno_;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableCFOptions& ioptions_;
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            int level);

  static const int kMinFilesForIntraL0Compaction = 4;
};

void LevelCompactionBuilder::PickExpiredTtlFiles() {
  if (vstorage_->ExpiredTtlFiles().empty()) {
    return;
  }

  auto continuation = [&](std::pair<int, FileMetaData*> level_file) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    start_level_ = level_file.first;
    output_level_ =
        (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;

    if ((start_level_ == vstorage_->num_non_empty_levels() - 1) ||
        (start_level_ == 0 &&
         !compaction_picker_->level0_compactions_in_progress()->empty())) {
      return false;
    }

    start_level_inputs_.files = {level_file.second};
    start_level_inputs_.level = start_level_;
    return compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                      &start_level_inputs_);
  };

  for (auto& level_file : vstorage_->ExpiredTtlFiles()) {
    if (continuation(level_file)) {
      // found the compaction!
      return;
    }
  }

  start_level_inputs_.files.clear();
}

void LevelCompactionBuilder::PickFilesMarkedForPeriodicCompaction() {
  if (vstorage_->FilesMarkedForPeriodicCompaction().empty()) {
    return;
  }

  auto continuation = [&](std::pair<int, FileMetaData*> level_file) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    output_level_ = start_level_ = level_file.first;

    if (start_level_ == 0 &&
        !compaction_picker_->level0_compactions_in_progress()->empty()) {
      return false;
    }

    start_level_inputs_.files = {level_file.second};
    start_level_inputs_.level = start_level_;
    return compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                      &start_level_inputs_);
  };

  for (auto& level_file : vstorage_->FilesMarkedForPeriodicCompaction()) {
    if (continuation(level_file)) {
      // found the compaction!
      return;
    }
  }

  start_level_inputs_.files.clear();
}

void LevelCompactionBuilder::SetupInitialFiles() {
  // Find the compactions by size on all levels.
  bool skipped_l0_to_base = false;
  for (int i = 0; i < compaction_picker_->NumberLevels() - 1; i++) {
    start_level_score_ = vstorage_->CompactionScore(i);
    start_level_ = vstorage_->CompactionScoreLevel(i);
    assert(i == 0 || start_level_score_ <= vstorage_->CompactionScore(i - 1));
    if (start_level_score_ >= 1) {
      if (skipped_l0_to_base && start_level_ == vstorage_->base_level()) {
        // If L0->base_level compaction is pending, don't schedule further
        // compaction from base level. Otherwise L0->base_level compaction
        // may starve.
        continue;
      }
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
      if (PickFileToCompact()) {
        // found the compaction!
        if (start_level_ == 0) {
          // L0 score = `num L0 files` / `level0_file_num_compaction_trigger`
          compaction_reason_ = CompactionReason::kLevelL0FilesNum;
        } else {
          // L1+ score = `Level files size` / `MaxBytesForLevel`
          compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
        }
        break;
      } else {
        // didn't find the compaction, clear the inputs
        start_level_inputs_.clear();
        if (start_level_ == 0) {
          skipped_l0_to_base = true;
          // L0->base_level may be blocked due to ongoing L0->base_level
          // compactions. It may also be blocked by an ongoing compaction from
          // base_level downwards.
          //
          // In these cases, to reduce L0 file count and thus reduce likelihood
          // of write stalls, we can attempt compacting a span of files within
          // L0.
          if (PickIntraL0Compaction()) {
            output_level_ = 0;
            compaction_reason_ = CompactionReason::kLevelL0FilesNum;
            break;
          }
        }
      }
    }
  }

  // if we didn't find a compaction, check if there are any files marked for
  // compaction
  if (start_level_inputs_.empty()) {
    parent_index_ = base_index_ = -1;

    compaction_picker_->PickFilesMarkedForCompaction(
        cf_name_, vstorage_, &start_level_, &output_level_,
        &start_level_inputs_);
    if (!start_level_inputs_.empty()) {
      is_manual_ = true;
      compaction_reason_ = CompactionReason::kFilesMarkedForCompaction;
      return;
    }
  }

  // Bottommost Files Compaction on deleting tombstones
  if (start_level_inputs_.empty()) {
    size_t i;
    for (i = 0; i < vstorage_->BottommostFilesMarkedForCompaction().size();
         ++i) {
      auto& level_and_file = vstorage_->BottommostFilesMarkedForCompaction()[i];
      assert(!level_and_file.second->being_compacted);
      start_level_inputs_.level = output_level_ = start_level_ =
          level_and_file.first;
      start_level_inputs_.files = {level_and_file.second};
      if (compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                     &start_level_inputs_)) {
        break;
      }
    }
    if (i == vstorage_->BottommostFilesMarkedForCompaction().size()) {
      start_level_inputs_.clear();
    } else {
      assert(!start_level_inputs_.empty());
      compaction_reason_ = CompactionReason::kBottommostFiles;
      return;
    }
  }

  // TTL Compaction
  if (start_level_inputs_.empty()) {
    PickExpiredTtlFiles();
    if (!start_level_inputs_.empty()) {
      compaction_reason_ = CompactionReason::kTtl;
      return;
    }
  }

  // Periodic Compaction
  if (start_level_inputs_.empty()) {
    PickFilesMarkedForPeriodicCompaction();
    if (!start_level_inputs_.empty()) {
      compaction_reason_ = CompactionReason::kPeriodicCompaction;
      return;
    }
  }
}

int64_t LevelCompactionBuilder::SetupOtherL0FilesIfNeeded() {
  if (start_level_ == 0 && output_level_ != 0) {
    return compaction_picker_->GetOverlappingL0Files(
        vstorage_, &start_level_inputs_, output_level_, &parent_index_, &ioptions_);
  }
  return true;
}

bool LevelCompactionBuilder::SetupOtherInputsIfNeeded() {
  // Setup input files from output level. For output to L0, we only compact
  // spans of files that do not interact with any pending compactions, so don't
  // need to consider other levels.
  if (output_level_ != 0) {
    output_level_inputs_.level = output_level_;
    if (!compaction_picker_->SetupOtherInputs(
            cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs_,
            &output_level_inputs_, &parent_index_, base_index_)) {
      return false;
    }

    compaction_inputs_.push_back(start_level_inputs_);
    if (!output_level_inputs_.empty()) {
      compaction_inputs_.push_back(output_level_inputs_);
    }

    // In some edge cases we could pick a compaction that will be compacting
    // a key range that overlap with another running compaction, and both
    // of them have the same output level. This could happen if
    // (1) we are running a non-exclusive manual compaction
    // (2) AddFile ingest a new file into the LSM tree
    // We need to disallow this from happening.
    if (compaction_picker_->FilesRangeOverlapWithCompaction(compaction_inputs_,
                                                            output_level_)) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      return false;
    }
    compaction_picker_->GetGrandparents(vstorage_, start_level_inputs_,
                                        output_level_inputs_, &grandparents_);
  } else {
    compaction_inputs_.push_back(start_level_inputs_);
  }
  return true;
}

Compaction* LevelCompactionBuilder::PickCompaction() {
  // See if we need to perform an Active Recycling pass becase the fragmentation
  // is getting too high
  compaction_inputs_.clear();  // start with an empty set of files
  // get pointer to CF.  Can be null only during tests that don't open a CF
  ColumnFamilyData *cfd =  vstorage_->GetCfd();
#ifndef NDEBUG
    // notify the test code of the compaction-picking info
    // inlevel, picked min ref0, min ref0 in lower levels, vlog file min,
    // vlog file max, output level
    size_t pickerinfo[8]={0,(size_t)~0,(size_t)~0,0,0,0,0};
    pickerinfo[6]=(size_t)cfd;  // 6 cf
#endif
  if (cfd!=nullptr) {
    // see if we select a set of files to recycle
    cfd->CheckForActiveRecycle(compaction_inputs_, ringno_,
                               lastfileno_, mutable_cf_options_);
  }
  if(!compaction_inputs_.empty()) {
    // Active Recycling needed.  Indicate that as the reason for compaction.
    // This reason code controls processing during the compaction
    compaction_reason_ = CompactionReason::kActiveRecycling;
    // Active Recycling processes SSTs in-place without changing any keys
    // - it just remaps old VLog blocks.  We can skip most of the checking and
    // stat-gathering for a full compaction.  Right here we will calculate the
    // stats we need: start_level_ and output_level_.
    // start_level_ is needed because it is used later to decide whether to
    // suppress other level-0 compaction.  output_level_ is used only for things
    // like figuring the path to use for the output files; we just set it to the
    // largest level we find
    // scaf problem: this sets compression for all levels to be the same as for
    // the max level
    start_level_ = output_level_ = compaction_inputs_[0].level;
    for(size_t i = 1;i<compaction_inputs_.size();++i){
      if (start_level_>compaction_inputs_[i].level) {
	// find minimum level touched
        start_level_ = compaction_inputs_[i].level;
      }
      if (output_level_<compaction_inputs_[i].level) {
	// find maximum level touched
        output_level_ = compaction_inputs_[i].level;
      }
    }
#ifndef NDEBUG
    // notify the test code of the compaction-picking info
    pickerinfo[7]=1;  // 7 set to 1 if AR
    pickerinfo[2] = compaction_inputs_.size();  // 2 # AR inputs
#endif
  } else {
    // Not Active Recycling.  Do a normal compaction,  pick files, etc.

  // Pick up the first file to start compaction. It may have been extended
  // to a clean cut.
  SetupInitialFiles();
  if (start_level_inputs_.empty()) {
    return nullptr;
  }
  assert(start_level_ >= 0 && output_level_ >= 0);

#ifndef NDEBUG
    // find the smallest ref0 in the input files (our expected ref0)
    for(auto startfile : start_level_inputs_.files) {
      ParsedFnameRing avgparent(startfile->avgparentfileno);
      if (avgparent.fileno()!=0) {
	// scaf wired to ring 0
        pickerinfo[1]=std::min(pickerinfo[1],avgparent.fileno());
      }
    }
#endif

  // If it is a L0 -> base level compaction, we need to set up other L0
  // files if needed.
  int64_t l0status=SetupOtherL0FilesIfNeeded();
  if (l0status<0) { //error
    return nullptr;
  } else if (l0status>0) {
    // if sequential-key load, load into last level
    output_level_ = vstorage_->num_levels()-1;
  }

  // Pick files in the output level and expand more files in the start level
  // if needed.
  if (!SetupOtherInputsIfNeeded()) {
    return nullptr;
  }
#ifndef NDEBUG
  // find the smallest ref0 among the descendants of the start level
  for(auto cfiles : compaction_inputs_) {
    if(cfiles.level>start_level_) {
      // find the smallest ref0 in the input files (our expected ref0)
      for(auto cfile : cfiles.files) {
        if(cfile->indirect_ref_0.size() && cfile->indirect_ref_0[0]!=0) {
	  // scaf wired to ring 0
          pickerinfo[2]=std::min(pickerinfo[2],cfile->indirect_ref_0[0]);
        }
      }
    }
  }
#endif
  } // if(!compaction_inputs_.empty())...else
#ifndef NDEBUG
  // 0 start level
  pickerinfo[0]=start_level_;
  // 5 output level
  pickerinfo[5]=output_level_;
  // 3 4 get the current file limits from the VLog
  if (cfd!=nullptr && cfd->vlog()!=nullptr &&
     cfd->vlog()->rings().size()!=0) {
    pickerinfo[3] = cfd->vlog()->rings()[0]->ringtail();
    pickerinfo[4]=cfd->vlog()->rings()[0]->ringhead();
  }
  TEST_SYNC_POINT_CALLBACK("LevelCompactionBuilder::PickCompaction",
                           &pickerinfo);
#endif

  // Form a compaction object containing the files we picked.
  Compaction* c = GetCompaction();

  TEST_SYNC_POINT_CALLBACK("LevelCompactionPicker::PickCompaction:Return", c);

  return c;
}

Compaction* LevelCompactionBuilder::GetCompaction() {
  auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, std::move(compaction_inputs_),
      output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          ioptions_.compaction_style, vstorage_->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options_.max_compaction_bytes,
      GetPathId(ioptions_, mutable_cf_options_, output_level_),
      GetCompressionType(ioptions_, vstorage_, mutable_cf_options_,
                         output_level_, vstorage_->base_level()),
      GetCompressionOptions(ioptions_, vstorage_, output_level_),
      /* max_subcompactions */ 0, std::move(grandparents_), is_manual_,
      start_level_score_, false /* deletion_compaction */, compaction_reason_,
      ringno_ , lastfileno_  , start_level_);
      // parms for Active Recycling, used if compaction_reason_ indicates

  // If it's level 0 compaction, make sure we don't execute any other level 0
  // compactions in parallel
  compaction_picker_->RegisterCompaction(c);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  return c;
}

/*
 * Find the optimal path to place a file
 * Given a level, finds the path where levels up to it will fit in levels
 * up to and including this path
 */
uint32_t LevelCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, int level) {
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = ioptions.cf_paths[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = mutable_cf_options.max_bytes_for_level_base;

  // Last path is the fallback
  while (p < ioptions.cf_paths.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > 0) {
          if (ioptions.level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/master/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier);
          } else {
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier *
                mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = ioptions.cf_paths[p].target_size;
  }
  return p;
}

bool LevelCompactionBuilder::PickFileToCompact() {
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);   // files at the current level

  // actually this is OK whether you have indirect values or not.  It allows
  // multiple L0 compactions.  If you keep this, make sure
  // UpdateFilesByCompactionPri() uses kReverseOrder for L0
  // If a compaction from L0 is running, usually there is no reason to bother
  // trying to start another, since the keys usually overlap.
  // The exception is sequential load, when each new L0 file has keys beyond the
  // range of all previous L0 files.
  // We want to detect this case as quickly as possible.  The point is that if a
  // set of L0 files does not overlap any L0 file currently in compaction,
  // it is safe to compact the lot.  It could be that the L0 files overlap some
  // L1 file that overlaps (and thus is part of) a current L0 compaction, but we
  // will detect that in the usual way, when we look for output-level files that
  // are in compaction.
  if (start_level_ == 0 &&
      !compaction_picker_->level0_compactions_in_progress()->empty()) {
    // If we are getting behind on a sequential load, the first compaction will
    // have originally been a single file that was expanded by
    // GetOverlappingL0Files, throwing in all the L0 files.  That means that for
    // us to need to start another compaction, there must be at least twice as
    // many files in L0 as needed to trigger a compaction.  This test will turn
    // away all normal cases except when we are unable to keep up with Puts.
    if (level_files.size()<
        (size_t)(2*mutable_cf_options_.level0_file_num_compaction_trigger)) {
      TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
      return false;
    }
    // Go through the files for level 0, to find the index of the first file
    // that is being compacted and the index of the first file NOT being
    // compacted. We scan through the files from back to front since any files
    // not in the current compaction will normally have been added at the end
    // If there is no file not being compacted, return failure.
    // index to a file being compacted, and one that is not
    int64_t compactx, noncompactx;
    for (noncompactx = level_files.size()-1;noncompactx>=0;--noncompactx) {
      if (!level_files[noncompactx]->being_compacted) break;
    }
    // if there can't be enough non-compacting to start a compaction, stop looking
    if (noncompactx+1<mutable_cf_options_.level0_file_num_compaction_trigger) {
      TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
      return false;
    }
    for (compactx = 0;compactx<(int64_t)level_files.size();++compactx) {
      if (level_files[compactx]->being_compacted) break;
    }
    if(compactx<(int64_t)level_files.size()){
      // There is a file being compacted.  We have to make sure the
      // non-compacting files do not overlap the keys of the compacting files.
      // We will check only for the ascending-key case, i. e. highest compacting
      // key less than smallest non-compacting key
      // It is possible that the L1 files being created by the running
      // compaction will overlap with the files we select here: that will be
      // detected later in the usual way
      // the user's comparator
      const InternalKeyComparator *icmp = compaction_picker_->GetComparator();

      // To give an early exit, compare the keys for the first 2 files and avoid
      // further searching if there is overlap
      // if overlap. we can't process it
      if (icmp->Compare(level_files[compactx]->largest,
            level_files[noncompactx]->smallest)>=0) {
        TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
	return false;
      }
      // No overlap in the first compare.  There's a very good chance that this
      // is a sequential write.  Compare the rest of the files
      // Find the smallest key among the noncompacting files, and the largest
      // key among the compacting
      // loop sets minx to index of noncompacting file with smallest smallest
      // key; n iis number of noncompacted files
      int64_t minx = noncompactx, noncompactn = 1;
      for(--noncompactx;noncompactx>=0;--noncompactx){
        if(!level_files[noncompactx]->being_compacted){
          ++noncompactn;
          if(icmp->Compare(level_files[noncompactx]->smallest,
               level_files[minx]->smallest)<0) {
             minx=noncompactx;
          }
        }
      }
      // If there aren't enough noncompacted files to start a compaction, abort
      if (noncompactn<mutable_cf_options_.level0_file_num_compaction_trigger) {
        TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
        return false;
      }
      // loop sets maxx to index of compacting file with largest largest key
      int64_t maxx = compactx;
      for(++compactx;compactx<(int64_t)level_files.size();++compactx){
        if(level_files[compactx]->being_compacted &&
           icmp->Compare(level_files[compactx]->largest,
             level_files[maxx]->largest)>0) {
          maxx=compactx;
        }
      }
      // abort if there is overlap
      // if overlap. we can't process it
      if (icmp->Compare(level_files[maxx]->largest,level_files[minx]->smallest)>=0) {
        TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
	return false;
      }
    }
    // no overlap, OK to continue with the compaction picking.  We will pick the
    // earliest uncompacted file (regardless of
    // level0_file_num_compaction_trigger) and then expand the selection with
    // anything that overlaps it.  Eventually we will throw in all the other
    // files L0 too, known to be safe since they don't overlap any existing
    // compaction.
  }
  /*
  // level 0 files are overlapping. So we cannot pick more
  // than one concurrent compactions at this level. This
  // could be made better by looking at key-ranges that are
  // being compacted at level 0.
  if (start_level_ == 0 &&
      !compaction_picker_->level0_compactions_in_progress()->empty()) {
    TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
    return false;
  }
  */

  start_level_inputs_.clear();

  assert(start_level_ >= 0);

  // Pick the largest file in this level that is not already
  // being compacted
  const std::vector<int>& file_size =
      vstorage_->FilesByCompactionPri(start_level_);

  unsigned int cmp_idx;
  for (cmp_idx = vstorage_->NextCompactionIndex(start_level_);
       cmp_idx < file_size.size(); cmp_idx++) {
    int index = file_size[cmp_idx];
    auto* f = level_files[index];

    // do not pick a file to compact if it is being compacted
    // from n-1 level.
    if (f->being_compacted) {
      continue;
    }

    start_level_inputs_.files.push_back(f);
    start_level_inputs_.level = start_level_;
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &start_level_inputs_) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {start_level_inputs_}, output_level_)) {
      // A locked (pending compaction) input-level file was pulled in due to
      // user-key overlap.
      start_level_inputs_.clear();
      continue;
    }

    // Now that input level is fully expanded, we check whether any output files
    // are locked due to pending compaction.
    //
    // Note we rely on ExpandInputsToCleanCut() to tell us whether any output-
    // level files are locked, not just the extra ones pulled in for user-key
    // overlap.
    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (!output_level_inputs.empty() &&
        !compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &output_level_inputs)) {
      start_level_inputs_.clear();
      continue;
    }
    base_index_ = index;
    break;
  }

  // store where to start the iteration in the next call to PickCompaction
  vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);

  return start_level_inputs_.size() > 0;
}

bool LevelCompactionBuilder::PickIntraL0Compaction() {
  start_level_inputs_.clear();
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(0 /* level */);
  if (level_files.size() <
          static_cast<size_t>(
              mutable_cf_options_.level0_file_num_compaction_trigger + 2) ||
      level_files[0]->being_compacted) {
    // If L0 isn't accumulating much files beyond the regular trigger, don't
    // resort to L0->L0 compaction yet.
    return false;
  }
  return FindIntraL0Compaction(
      level_files, kMinFilesForIntraL0Compaction, port::kMaxUint64,
      mutable_cf_options_.max_compaction_bytes, &start_level_inputs_);
}
}  // namespace

Compaction* LevelCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, LogBuffer* log_buffer) {
  LevelCompactionBuilder builder(cf_name, vstorage, this, log_buffer,
                                 mutable_cf_options, ioptions_);
  return builder.PickCompaction();
}
}  // namespace rocksdb
