//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE
#include "db/forward_iterator.h"

#include <limits>
#include <string>
#include <utility>

#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/column_family.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "table/merger.h"
#include "db/dbformat.h"

namespace rocksdb {

// Usage:
//     LevelIterator iter;
//     iter.SetFileIndex(file_index);
//     iter.Seek(target);
//     iter.Next()
class LevelIterator : public Iterator {
 public:
  LevelIterator(const ColumnFamilyData* const cfd,
      const ReadOptions& read_options,
      const std::vector<FileMetaData*>& files)
    : cfd_(cfd), read_options_(read_options), files_(files), valid_(false),
      file_index_(std::numeric_limits<uint32_t>::max()) {}

  void SetFileIndex(uint32_t file_index) {
    assert(file_index < files_.size());
    if (file_index != file_index_) {
      file_index_ = file_index;
      Reset();
    }
    valid_ = false;
  }
  void Reset() {
    assert(file_index_ < files_.size());
    file_iter_.reset(cfd_->table_cache()->NewIterator(
        read_options_, *(cfd_->soptions()), cfd_->internal_comparator(),
        files_[file_index_]->fd, nullptr /* table_reader_ptr */, false));
  }
  void SeekToLast() override {
    status_ = Status::NotSupported("LevelIterator::SeekToLast()");
    valid_ = false;
  }
  void Prev() {
    status_ = Status::NotSupported("LevelIterator::Prev()");
    valid_ = false;
  }
  bool Valid() const override {
    return valid_;
  }
  void SeekToFirst() override {
    SetFileIndex(0);
    file_iter_->SeekToFirst();
    valid_ = file_iter_->Valid();
  }
  void Seek(const Slice& internal_key) override {
    assert(file_iter_ != nullptr);
    file_iter_->Seek(internal_key);
    valid_ = file_iter_->Valid();
  }
  void Next() override {
    assert(valid_);
    file_iter_->Next();
    for (;;) {
      if (file_iter_->status().IsIncomplete() || file_iter_->Valid()) {
        valid_ = !file_iter_->status().IsIncomplete();
        return;
      }
      if (file_index_ + 1 >= files_.size()) {
        valid_ = false;
        return;
      }
      SetFileIndex(file_index_ + 1);
      file_iter_->SeekToFirst();
    }
  }
  Slice key() const override {
    assert(valid_);
    return file_iter_->key();
  }
  Slice value() const override {
    assert(valid_);
    return file_iter_->value();
  }
  Status status() const override {
    if (!status_.ok()) {
      return status_;
    } else if (file_iter_ && !file_iter_->status().ok()) {
      return file_iter_->status();
    }
    return Status::OK();
  }

 private:
  const ColumnFamilyData* const cfd_;
  const ReadOptions& read_options_;
  const std::vector<FileMetaData*>& files_;

  bool valid_;
  uint32_t file_index_;
  Status status_;
  std::unique_ptr<Iterator> file_iter_;
};

ForwardIterator::ForwardIterator(DBImpl* db, const ReadOptions& read_options,
                                 ColumnFamilyData* cfd)
    : db_(db),
      read_options_(read_options),
      cfd_(cfd),
      prefix_extractor_(cfd->options()->prefix_extractor.get()),
      user_comparator_(cfd->user_comparator()),
      immutable_min_heap_(MinIterComparator(&cfd_->internal_comparator())),
      sv_(nullptr),
      mutable_iter_(nullptr),
      current_(nullptr),
      valid_(false),
      is_prev_set_(false) {}

ForwardIterator::~ForwardIterator() {
  Cleanup();
}

void ForwardIterator::Cleanup() {
  delete mutable_iter_;
  for (auto* m : imm_iters_) {
    delete m;
  }
  imm_iters_.clear();
  for (auto* f : l0_iters_) {
    delete f;
  }
  l0_iters_.clear();
  for (auto* l : level_iters_) {
    delete l;
  }
  level_iters_.clear();

  if (sv_ != nullptr && sv_->Unref()) {
    DBImpl::DeletionState deletion_state;
    db_->mutex_.Lock();
    sv_->Cleanup();
    db_->FindObsoleteFiles(deletion_state, false, true);
    db_->mutex_.Unlock();
    delete sv_;
    if (deletion_state.HaveSomethingToDelete()) {
      db_->PurgeObsoleteFiles(deletion_state);
    }
  }
}

bool ForwardIterator::Valid() const {
  return valid_;
}

void ForwardIterator::SeekToFirst() {
  if (sv_ == nullptr ||
      sv_ ->version_number != cfd_->GetSuperVersionNumber()) {
    RebuildIterators();
  } else if (status_.IsIncomplete()) {
    ResetIncompleteIterators();
  }
  SeekInternal(Slice(), true);
}

void ForwardIterator::Seek(const Slice& internal_key) {
  if (sv_ == nullptr ||
      sv_ ->version_number != cfd_->GetSuperVersionNumber()) {
    RebuildIterators();
  } else if (status_.IsIncomplete()) {
    ResetIncompleteIterators();
  }
  SeekInternal(internal_key, false);
}

void ForwardIterator::SeekInternal(const Slice& internal_key,
                                   bool seek_to_first) {
  // mutable
  seek_to_first ? mutable_iter_->SeekToFirst() :
                  mutable_iter_->Seek(internal_key);

  // immutable
  // TODO(ljin): NeedToSeekImmutable has negative impact on performance
  // if it turns to need to seek immutable often. We probably want to have
  // an option to turn it off.
  if (seek_to_first || NeedToSeekImmutable(internal_key)) {
    {
      auto tmp = MinIterHeap(MinIterComparator(&cfd_->internal_comparator()));
      immutable_min_heap_.swap(tmp);
    }
    for (auto* m : imm_iters_) {
      seek_to_first ? m->SeekToFirst() : m->Seek(internal_key);
      if (m->Valid()) {
        immutable_min_heap_.push(m);
      }
    }

    Slice user_key;
    if (!seek_to_first) {
      user_key = ExtractUserKey(internal_key);
    }
    auto* files = sv_->current->files_;
    for (uint32_t i = 0; i < files[0].size(); ++i) {
      if (seek_to_first) {
        l0_iters_[i]->SeekToFirst();
      } else {
        // If the target key passes over the larget key, we are sure Next()
        // won't go over this file.
        if (user_comparator_->Compare(user_key,
              files[0][i]->largest.user_key()) > 0) {
          continue;
        }
        l0_iters_[i]->Seek(internal_key);
      }

      if (l0_iters_[i]->status().IsIncomplete()) {
        // if any of the immutable iterators is incomplete (no-io option was
        // used), we are unable to reliably find the smallest key
        assert(read_options_.read_tier == kBlockCacheTier);
        status_ = l0_iters_[i]->status();
        valid_ = false;
        return;
      } else if (l0_iters_[i]->Valid()) {
        immutable_min_heap_.push(l0_iters_[i]);
      }
    }

    int32_t search_left_bound = 0;
    int32_t search_right_bound = FileIndexer::kLevelMaxIndex;
    for (int32_t level = 1; level < sv_->current->NumberLevels(); ++level) {
      if (files[level].empty()) {
        search_left_bound = 0;
        search_right_bound = FileIndexer::kLevelMaxIndex;
        continue;
      }
      assert(level_iters_[level - 1] != nullptr);
      uint32_t f_idx = 0;
      if (!seek_to_first) {
        // TODO(ljin): remove before committing
        // f_idx = FindFileInRange(
        //    files[level], internal_key, 0, files[level].size());

        if (search_left_bound == search_right_bound) {
          f_idx = search_left_bound;
        } else if (search_left_bound < search_right_bound) {
          f_idx = FindFileInRange(
              files[level], internal_key, search_left_bound,
              search_right_bound == FileIndexer::kLevelMaxIndex ?
                files[level].size() : search_right_bound);
        } else {
          // search_left_bound > search_right_bound
          // There are only 2 cases this can happen:
          // (1) target key is smaller than left most file
          // (2) target key is larger than right most file
          assert(search_left_bound == (int32_t)files[level].size() ||
                 search_right_bound == -1);
          if (search_right_bound == -1) {
            assert(search_left_bound == 0);
            f_idx = 0;
          } else {
            sv_->current->file_indexer_.GetNextLevelIndex(
                level, files[level].size() - 1,
                1, 1, &search_left_bound, &search_right_bound);
            continue;
          }
        }

        // Prepare hints for the next level
        if (f_idx < files[level].size()) {
          int cmp_smallest = user_comparator_->Compare(
              user_key, files[level][f_idx]->smallest.user_key());
          int cmp_largest = -1;
          if (cmp_smallest >= 0) {
            cmp_smallest = user_comparator_->Compare(
                user_key, files[level][f_idx]->smallest.user_key());
          }
          sv_->current->file_indexer_.GetNextLevelIndex(level, f_idx,
              cmp_smallest, cmp_largest,
              &search_left_bound, &search_right_bound);
        } else {
          sv_->current->file_indexer_.GetNextLevelIndex(
              level, files[level].size() - 1,
              1, 1, &search_left_bound, &search_right_bound);
        }
      }

      // Seek
      if (f_idx < files[level].size()) {
        level_iters_[level - 1]->SetFileIndex(f_idx);
        seek_to_first ? level_iters_[level - 1]->SeekToFirst() :
                        level_iters_[level - 1]->Seek(internal_key);

        if (level_iters_[level - 1]->status().IsIncomplete()) {
          // see above
          assert(read_options_.read_tier == kBlockCacheTier);
          status_ = level_iters_[level - 1]->status();
          valid_ = false;
          return;
        } else if (level_iters_[level - 1]->Valid()) {
          immutable_min_heap_.push(level_iters_[level - 1]);
        }
      }
    }

    if (seek_to_first || immutable_min_heap_.empty()) {
      is_prev_set_ = false;
    } else {
      prev_key_.SetKey(internal_key);
      is_prev_set_ = true;
    }
  } else if (current_ && current_ != mutable_iter_) {
    // current_ is one of immutable iterators, push it back to the heap
    immutable_min_heap_.push(current_);
  }

  UpdateCurrent();
}

void ForwardIterator::Next() {
  assert(valid_);

  if (sv_ == nullptr ||
      sv_->version_number != cfd_->GetSuperVersionNumber()) {
    std::string current_key = key().ToString();
    Slice old_key(current_key.data(), current_key.size());

    RebuildIterators();
    SeekInternal(old_key, false);
    if (!valid_ || key().compare(old_key) != 0) {
      return;
    }
  } else if (current_ != mutable_iter_) {
    // It is going to advance immutable iterator
    prev_key_.SetKey(current_->key());
    is_prev_set_ = true;
  }

  current_->Next();
  if (current_ != mutable_iter_) {
    if (current_->status().IsIncomplete()) {
      assert(read_options_.read_tier == kBlockCacheTier);
      status_ = current_->status();
      valid_ = false;
      return;
    } else if (current_->Valid()) {
      immutable_min_heap_.push(current_);
    }
  }

  UpdateCurrent();
}

Slice ForwardIterator::key() const {
  assert(valid_);
  return current_->key();
}

Slice ForwardIterator::value() const {
  assert(valid_);
  return current_->value();
}

Status ForwardIterator::status() const {
  if (!status_.ok()) {
    return status_;
  } else if (!mutable_iter_->status().ok()) {
    return mutable_iter_->status();
  }

  for (auto *it : imm_iters_) {
    if (it && !it->status().ok()) {
      return it->status();
    }
  }
  for (auto *it : l0_iters_) {
    if (it && !it->status().ok()) {
      return it->status();
    }
  }
  for (auto *it : level_iters_) {
    if (it && !it->status().ok()) {
      return it->status();
    }
  }

  return Status::OK();
}

void ForwardIterator::RebuildIterators() {
  // Clean up
  Cleanup();
  // New
  sv_ = cfd_->GetReferencedSuperVersion(&(db_->mutex_));
  mutable_iter_ = sv_->mem->NewIterator(read_options_);
  sv_->imm->AddIterators(read_options_, &imm_iters_);
  const auto& l0_files = sv_->current->files_[0];
  l0_iters_.reserve(l0_files.size());
  for (const auto* l0 : l0_files) {
    l0_iters_.push_back(cfd_->table_cache()->NewIterator(
        read_options_, *cfd_->soptions(), cfd_->internal_comparator(), l0->fd));
  }
  level_iters_.reserve(sv_->current->NumberLevels() - 1);
  for (int32_t level = 1; level < sv_->current->NumberLevels(); ++level) {
    if (sv_->current->files_[level].empty()) {
      level_iters_.push_back(nullptr);
    } else {
      level_iters_.push_back(new LevelIterator(cfd_, read_options_,
          sv_->current->files_[level]));
    }
  }

  current_ = nullptr;
  is_prev_set_ = false;
}

void ForwardIterator::ResetIncompleteIterators() {
  const auto& l0_files = sv_->current->files_[0];
  for (uint32_t i = 0; i < l0_iters_.size(); ++i) {
    assert(i < l0_files.size());
    if (!l0_iters_[i]->status().IsIncomplete()) {
      continue;
    }
    delete l0_iters_[i];
    l0_iters_[i] = cfd_->table_cache()->NewIterator(
        read_options_, *cfd_->soptions(), cfd_->internal_comparator(),
        l0_files[i]->fd);
  }

  for (auto* level_iter : level_iters_) {
    if (level_iter && level_iter->status().IsIncomplete()) {
      level_iter->Reset();
    }
  }

  current_ = nullptr;
  is_prev_set_ = false;
}

void ForwardIterator::UpdateCurrent() {
  if (immutable_min_heap_.empty() && !mutable_iter_->Valid()) {
    current_ = nullptr;
  } else if (immutable_min_heap_.empty()) {
    current_ = mutable_iter_;
  } else if (!mutable_iter_->Valid()) {
    current_ = immutable_min_heap_.top();
    immutable_min_heap_.pop();
  } else {
    current_ = immutable_min_heap_.top();
    assert(current_ != nullptr);
    assert(current_->Valid());
    int cmp = cfd_->internal_comparator().InternalKeyComparator::Compare(
        mutable_iter_->key(), current_->key());
    assert(cmp != 0);
    if (cmp > 0) {
      immutable_min_heap_.pop();
    } else {
      current_ = mutable_iter_;
    }
  }
  valid_ = (current_ != nullptr);
  if (!status_.ok()) {
    status_ = Status::OK();
  }
}

bool ForwardIterator::NeedToSeekImmutable(const Slice& target) {
  if (!valid_ || !is_prev_set_) {
    return true;
  }
  Slice prev_key = prev_key_.GetKey();
  if (prefix_extractor_ && prefix_extractor_->Transform(target).compare(
    prefix_extractor_->Transform(prev_key)) != 0) {
    return true;
  }
  if (cfd_->internal_comparator().InternalKeyComparator::Compare(
        prev_key, target) >= 0) {
    return true;
  }
  if (immutable_min_heap_.empty() ||
      cfd_->internal_comparator().InternalKeyComparator::Compare(
          target, current_ == mutable_iter_ ? immutable_min_heap_.top()->key()
                                            : current_->key()) > 0) {
    return true;
  }
  return false;
}

uint32_t ForwardIterator::FindFileInRange(
    const std::vector<FileMetaData*>& files, const Slice& internal_key,
    uint32_t left, uint32_t right) {
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (cfd_->internal_comparator().InternalKeyComparator::Compare(
          f->largest.Encode(), internal_key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
