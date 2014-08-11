//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <vector>

#include "db/column_family.h"
#include "util/logging.h"

namespace rocksdb {

uint64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  uint64_t sum = 0;
  for (size_t i = 0; i < files.size() && files[i]; i++) {
    sum += files[i]->fd.GetFileSize();
  }
  return sum;
}

Compaction::Compaction(Version* input_version, int start_level, int out_level,
                       uint64_t target_file_size,
                       uint64_t max_grandparent_overlap_bytes,
                       uint32_t output_path_id,
                       CompressionType output_compression, bool seek_compaction,
                       bool deletion_compaction)
    : start_level_(start_level),
      output_level_(out_level),
      max_output_file_size_(target_file_size),
      max_grandparent_overlap_bytes_(max_grandparent_overlap_bytes),
      input_version_(input_version),
      number_levels_(input_version_->NumberLevels()),
      cfd_(input_version_->cfd_),
      output_path_id_(output_path_id),
      output_compression_(output_compression),
      seek_compaction_(seek_compaction),
      deletion_compaction_(deletion_compaction),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0),
      base_index_(-1),
      parent_index_(-1),
      score_(0),
      bottommost_level_(false),
      is_full_compaction_(false),
      is_manual_compaction_(false),
      level_ptrs_(std::vector<size_t>(number_levels_)) {

  cfd_->Ref();
  input_version_->Ref();
  edit_ = new VersionEdit();
  edit_->SetColumnFamily(cfd_->GetID());
  for (int i = 0; i < number_levels_; i++) {
    level_ptrs_[i] = 0;
  }
  int num_levels = output_level_ - start_level_ + 1;
  input_levels_.resize(num_levels);
  inputs_.resize(num_levels);
  for (int i = 0; i < num_levels; ++i) {
    inputs_[i].level = start_level_ + i;
  }
}

Compaction::~Compaction() {
  delete edit_;
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
  if (cfd_ != nullptr) {
    if (cfd_->Unref()) {
      delete cfd_;
    }
  }
}

void Compaction::GenerateFileLevels() {
  input_levels_.resize(num_input_levels());
  for (int which = 0; which < num_input_levels(); which++) {
    DoGenerateFileLevel(&input_levels_[which], inputs_[which].files, &arena_);
  }
}

bool Compaction::IsTrivialMove() const {
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  // If start_level_== output_level_, the purpose is to force compaction
  // filter to be applied to that level, and thus cannot be a trivia move.
  return (start_level_ != output_level_ &&
          num_input_levels() == 2 &&
          num_input_files(0) == 1 &&
          num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <= max_grandparent_overlap_bytes_);
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < num_input_levels(); which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->DeleteFile(level(which), inputs_[which][i]->fd.GetNumber());
    }
  }
}

bool Compaction::KeyNotExistsBeyondOutputLevel(const Slice& user_key) {
  assert(cfd_->options()->compaction_style != kCompactionStyleFIFO);
  if (cfd_->options()->compaction_style == kCompactionStyleUniversal) {
    return bottommost_level_;
  }
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = cfd_->user_comparator();
  for (int lvl = output_level_ + 1; lvl < number_levels_; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    for (; level_ptrs_[lvl] < files.size(); ) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely
          // exists beyond output level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &cfd_->internal_comparator();
  while (grandparent_index_ < grandparents_.size() &&
      icmp->Compare(internal_key,
                    grandparents_[grandparent_index_]->largest.Encode()) > 0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->fd.GetFileSize();
    }
    assert(grandparent_index_ + 1 >= grandparents_.size() ||
           icmp->Compare(grandparents_[grandparent_index_]->largest.Encode(),
                         grandparents_[grandparent_index_+1]->smallest.Encode())
                         < 0);
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > max_grandparent_overlap_bytes_) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

// Mark (or clear) each file that is being compacted
void Compaction::MarkFilesBeingCompacted(bool mark_as_compacted) {
  for (int i = 0; i < num_input_levels(); i++) {
    for (unsigned int j = 0; j < inputs_[i].size(); j++) {
      assert(mark_as_compacted ? !inputs_[i][j]->being_compacted :
                                  inputs_[i][j]->being_compacted);
      inputs_[i][j]->being_compacted = mark_as_compacted;
    }
  }
}

// Is this compaction producing files at the bottommost level?
void Compaction::SetupBottomMostLevel(bool is_manual) {
  assert(cfd_->options()->compaction_style != kCompactionStyleFIFO);
  if (cfd_->options()->compaction_style == kCompactionStyleUniversal) {
    // If universal compaction style is used and manual
    // compaction is occuring, then we are guaranteed that
    // all files will be picked in a single compaction
    // run. We can safely set bottommost_level_ = true.
    // If it is not manual compaction, then bottommost_level_
    // is already set when the Compaction was created.
    if (is_manual) {
      bottommost_level_ = true;
    }
    return;
  }
  bottommost_level_ = true;
  // checks whether there are files living beyond the output_level.
  for (int i = output_level_ + 1; i < number_levels_; i++) {
    if (input_version_->NumLevelFiles(i) > 0) {
      bottommost_level_ = false;
      break;
    }
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
  if (cfd_ != nullptr) {
    if (cfd_->Unref()) {
      delete cfd_;
    }
    cfd_ = nullptr;
  }
}

void Compaction::ReleaseCompactionFiles(Status status) {
  cfd_->compaction_picker()->ReleaseCompactionFiles(this, status);
}

void Compaction::ResetNextCompactionIndex() {
  input_version_->ResetNextCompactionIndex(start_level_);
}

namespace {
int InputSummary(const std::vector<FileMetaData*>& files, char* output,
                 int len) {
  *output = '\0';
  int write = 0;
  for (unsigned int i = 0; i < files.size(); i++) {
    int sz = len - write;
    int ret;
    char sztxt[16];
    AppendHumanBytes(files.at(i)->fd.GetFileSize(), sztxt, 16);
    ret = snprintf(output + write, sz, "%" PRIu64 "(%s) ",
                   files.at(i)->fd.GetNumber(), sztxt);
    if (ret < 0 || ret >= sz) break;
    write += ret;
  }
  // if files.size() is non-zero, overwrite the last space
  return write - !!files.size();
}
}  // namespace

void Compaction::Summary(char* output, int len) {
  int write =
      snprintf(output, len, "Base version %" PRIu64
                            " Base level %d, seek compaction:%d, inputs: [",
               input_version_->GetVersionNumber(),
               start_level_, seek_compaction_);
  if (write < 0 || write >= len) {
    return;
  }

  for (int level = 0; level < num_input_levels(); ++level) {
    if (level > 0) {
      write += snprintf(output + write, len - write, "], [");
      if (write < 0 || write >= len) {
        return;
      }
    }
    write += InputSummary(inputs_[level].files, output + write, len - write);
    if (write < 0 || write >= len) {
      return;
    }
  }

  snprintf(output + write, len - write, "]");
}

uint64_t Compaction::OutputFilePreallocationSize() {
  uint64_t preallocation_size = 0;

  if (cfd_->options()->compaction_style == kCompactionStyleLevel) {
    preallocation_size =
        cfd_->compaction_picker()->MaxFileSizeForLevel(output_level());
  } else {
    for (int level = 0; level < num_input_levels(); ++level) {
      for (const auto& f : inputs_[level].files) {
        preallocation_size += f->fd.GetFileSize();
      }
    }
  }
  // Over-estimate slightly so we don't end up just barely crossing
  // the threshold
  return preallocation_size * 1.1;
}

}  // namespace rocksdb
