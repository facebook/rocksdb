//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"
#include "db/pinned_iterators_manager.h"
#include "db/version_edit.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "util/arena.h"
#include "util/heap.h"

namespace rocksdb {

namespace {

class TwoLevelIndexIterator : public InternalIteratorBase<BlockHandle> {
 public:
  explicit TwoLevelIndexIterator(
      TwoLevelIteratorState* state,
      InternalIteratorBase<BlockHandle>* first_level_iter);

  virtual ~TwoLevelIndexIterator() {
    first_level_iter_.DeleteIter(false /* is_arena_mode */);
    second_level_iter_.DeleteIter(false /* is_arena_mode */);
    delete state_;
  }

  virtual void Seek(const Slice& target) override;
  virtual void SeekForPrev(const Slice& target) override;
  virtual void SeekToFirst() override;
  virtual void SeekToLast() override;
  virtual void Next() override;
  virtual void Prev() override;

  virtual bool Valid() const override { return second_level_iter_.Valid(); }
  virtual Slice key() const override {
    assert(Valid());
    return second_level_iter_.key();
  }
  virtual BlockHandle value() const override {
    assert(Valid());
    return second_level_iter_.value();
  }
  virtual Status status() const override {
    if (!first_level_iter_.status().ok()) {
      assert(second_level_iter_.iter() == nullptr);
      return first_level_iter_.status();
    } else if (second_level_iter_.iter() != nullptr &&
               !second_level_iter_.status().ok()) {
      return second_level_iter_.status();
    } else {
      return status_;
    }
  }
  virtual uint64_t FileNumber() const override {
    return second_level_iter_.FileNumber();
  }
  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* /*pinned_iters_mgr*/) override {}
  virtual bool IsKeyPinned() const override { return false; }
  virtual bool IsValuePinned() const override { return false; }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetSecondLevelIterator(InternalIteratorBase<BlockHandle>* iter);
  void InitDataBlock();

  TwoLevelIteratorState* state_;
  IteratorWrapperBase<BlockHandle> first_level_iter_;
  IteratorWrapperBase<BlockHandle> second_level_iter_;  // May be nullptr
  Status status_;
  // If second_level_iter is non-nullptr, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the second_level_iter.
  BlockHandle data_block_handle_;
};

TwoLevelIndexIterator::TwoLevelIndexIterator(
    TwoLevelIteratorState* state,
    InternalIteratorBase<BlockHandle>* first_level_iter)
    : state_(state), first_level_iter_(first_level_iter) {}

void TwoLevelIndexIterator::Seek(const Slice& target) {
  first_level_iter_.Seek(target);

  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.Seek(target);
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelIndexIterator::SeekForPrev(const Slice& target) {
  first_level_iter_.Seek(target);
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekForPrev(target);
  }
  if (!Valid()) {
    if (!first_level_iter_.Valid() && first_level_iter_.status().ok()) {
      first_level_iter_.SeekToLast();
      InitDataBlock();
      if (second_level_iter_.iter() != nullptr) {
        second_level_iter_.SeekForPrev(target);
      }
    }
    SkipEmptyDataBlocksBackward();
  }
}

void TwoLevelIndexIterator::SeekToFirst() {
  first_level_iter_.SeekToFirst();
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekToFirst();
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelIndexIterator::SeekToLast() {
  first_level_iter_.SeekToLast();
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekToLast();
  }
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIndexIterator::Next() {
  assert(Valid());
  second_level_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIndexIterator::Prev() {
  assert(Valid());
  second_level_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIndexIterator::SkipEmptyDataBlocksForward() {
  while (second_level_iter_.iter() == nullptr ||
         (!second_level_iter_.Valid() && second_level_iter_.status().ok())) {
    // Move to next block
    if (!first_level_iter_.Valid()) {
      SetSecondLevelIterator(nullptr);
      return;
    }
    first_level_iter_.Next();
    InitDataBlock();
    if (second_level_iter_.iter() != nullptr) {
      second_level_iter_.SeekToFirst();
    }
  }
}

void TwoLevelIndexIterator::SkipEmptyDataBlocksBackward() {
  while (second_level_iter_.iter() == nullptr ||
         (!second_level_iter_.Valid() && second_level_iter_.status().ok())) {
    // Move to next block
    if (!first_level_iter_.Valid()) {
      SetSecondLevelIterator(nullptr);
      return;
    }
    first_level_iter_.Prev();
    InitDataBlock();
    if (second_level_iter_.iter() != nullptr) {
      second_level_iter_.SeekToLast();
    }
  }
}

void TwoLevelIndexIterator::SetSecondLevelIterator(
    InternalIteratorBase<BlockHandle>* iter) {
  InternalIteratorBase<BlockHandle>* old_iter = second_level_iter_.Set(iter);
  delete old_iter;
}

void TwoLevelIndexIterator::InitDataBlock() {
  if (!first_level_iter_.Valid()) {
    SetSecondLevelIterator(nullptr);
  } else {
    BlockHandle handle = first_level_iter_.value();
    if (second_level_iter_.iter() != nullptr &&
        !second_level_iter_.status().IsIncomplete() &&
        handle.offset() == data_block_handle_.offset()) {
      // second_level_iter is already constructed with this iterator, so
      // no need to change anything
    } else {
      InternalIteratorBase<BlockHandle>* iter =
          state_->NewSecondaryIterator(handle);
      data_block_handle_ = handle;
      SetSecondLevelIterator(iter);
    }
  }
}

class LinkSstIterator final : public InternalIterator {
 private:
  const FileMetaData& file_meta_;
  InternalIterator* first_level_iter_;
  InternalIterator* second_level_iter_;
  Slice bound_;
  InternalKey bound_storage_;
  bool has_bound_;
  bool is_backword_;
  Status status_;
  const InternalKeyComparator& icomp_;
  IteratorCache iterator_cache_;

  bool InitFirstLevelIter() {
    if (!first_level_iter_->Valid()) {
      second_level_iter_ = nullptr;
      return false;
    }
    bound_ = first_level_iter_->key();
    return InitSecondLevelIter();
  }

  bool InitSecondLevelIter() {
    // Manual inline LinkSstElement::Decode
    uint64_t file_number;
    Slice value_copy = first_level_iter_->value();
    if (!GetFixed64(&value_copy, &file_number)) {
      status_ = Status::Corruption("Link sst invalid value");
      second_level_iter_ = nullptr;
      return false;
    }
    assert(std::binary_search(file_meta_.sst_depend.begin(),
                              file_meta_.sst_depend.end(), file_number));
    second_level_iter_ = iterator_cache_.GetIterator(file_number);
    if (!second_level_iter_->status().ok()) {
      status_ = second_level_iter_->status();
      second_level_iter_ = nullptr;
      return false;
    }
    return true;
  }

 public:
  LinkSstIterator(const FileMetaData& file_meta, InternalIterator* iter,
                  const DependFileMap& depend_files,
                  const InternalKeyComparator& icomp, void* create_arg,
                  const IteratorCache::CreateIterCallback& create)
      : file_meta_(file_meta),
        first_level_iter_(iter),
        second_level_iter_(nullptr),
        has_bound_(false),
        is_backword_(false),
        icomp_(icomp),
        iterator_cache_(depend_files, create_arg, create) {
    if (file_meta_.sst_purpose != kLinkSst) {
      abort();
    }
  }

  virtual bool Valid() const override {
    return second_level_iter_ != nullptr && second_level_iter_->Valid();
  }
  virtual void SeekToFirst() override {
    is_backword_ = false;
    first_level_iter_->SeekToFirst();
    if (InitFirstLevelIter()) {
      second_level_iter_->Seek(file_meta_.smallest.Encode());
    }
  }
  virtual void SeekToLast() override {
    is_backword_ = false;
    first_level_iter_->SeekToLast();
    if (InitFirstLevelIter()) {
      second_level_iter_->SeekForPrev(file_meta_.largest.Encode());
    }
  }
  virtual void Seek(const Slice& target) override {
    is_backword_ = false;
    Slice seek_target = target;
    if (icomp_.Compare(target, file_meta_.smallest.Encode()) < 0) {
      seek_target = file_meta_.smallest.Encode();
    }
    first_level_iter_->Seek(seek_target);
    if (InitFirstLevelIter()) {
      second_level_iter_->Seek(seek_target);
      assert(second_level_iter_->Valid());
      assert(icomp_.Compare(second_level_iter_->key(), bound_) <= 0);
      assert(icomp_.Compare(second_level_iter_->key(), target) >= 0);
      if (icomp_.Compare(second_level_iter_->key(),
                         file_meta_.largest.Encode()) > 0) {
        second_level_iter_ = nullptr;
      }
    }
  }
  virtual void SeekForPrev(const Slice& target) override {
    LinkSstIterator::Seek(target);
    if (!LinkSstIterator::Valid()) {
      LinkSstIterator::SeekToLast();
    } else if (icomp_.Compare(LinkSstIterator::key(), target) != 0) {
      LinkSstIterator::Prev();
    }
  }
  virtual void Next() override {
    if (is_backword_) {
      if (has_bound_) {
        first_level_iter_->Next();
      } else {
        first_level_iter_->SeekToFirst();
      }
      bound_ = first_level_iter_->key();
      is_backword_ = false;
    }
    second_level_iter_->Next();
    if (!second_level_iter_->Valid() ||
        icomp_.Compare(second_level_iter_->key(), bound_) > 0) {
      bound_storage_.DecodeFrom(bound_);
      first_level_iter_->Next();
      if (InitFirstLevelIter()) {
        second_level_iter_->Seek(bound_storage_.Encode());
        assert(second_level_iter_->Valid());
        assert(icomp_.Compare(second_level_iter_->key(),
                              bound_storage_.Encode()) > 0);
      }
    }
  }
  virtual void Prev() override {
    if (!is_backword_) {
      first_level_iter_->Prev();
      has_bound_ = first_level_iter_->Valid();
      if (has_bound_) {
        bound_ = first_level_iter_->key();
      }
      is_backword_ = true;
    }
    second_level_iter_->Prev();
    if (has_bound_) {
      if ((!second_level_iter_->Valid() ||
           icomp_.Compare(second_level_iter_->key(), bound_) <= 0) &&
          InitSecondLevelIter()) {
        second_level_iter_->SeekForPrev(bound_);
        assert(second_level_iter_->Valid());
        assert(icomp_.Compare(second_level_iter_->key(), bound_) == 0);
        first_level_iter_->Prev();
        has_bound_ = first_level_iter_->Valid();
        if (has_bound_) {
          bound_ = first_level_iter_->key();
        }
      }
    } else if (second_level_iter_->Valid() &&
               icomp_.Compare(second_level_iter_->key(),
                              file_meta_.smallest.Encode()) < 0) {
      second_level_iter_ = nullptr;
    }
  }
  virtual Slice key() const override { return second_level_iter_->key(); }
  virtual Slice value() const override { return second_level_iter_->value(); }
  virtual Status status() const override { return status_; }
  virtual uint64_t FileNumber() const override {
    return second_level_iter_ != nullptr ? second_level_iter_->FileNumber()
                                         : uint64_t(-1);
  }
  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override {
    iterator_cache_.SetPinnedItersMgr(pinned_iters_mgr);
  }
  virtual bool IsKeyPinned() const override {
    return second_level_iter_ != nullptr && second_level_iter_->IsKeyPinned();
  }
  virtual bool IsValuePinned() const override {
    return second_level_iter_ != nullptr &&
           second_level_iter_->IsValuePinned();
  }
};

class MapSstIterator final : public InternalIterator {
 private:
  const FileMetaData& file_meta_;
  InternalIterator* first_level_iter_;
  bool is_backword_;
  Status status_;
  IteratorCache iterator_cache_;
  Slice smallest_key_;
  Slice largest_key_;
  int include_smallest_;
  int include_largest_;
  std::vector<uint64_t> link_;
  struct HeapElement {
    InternalIterator* iter;
    Slice key;
  };
  template<bool is_less>
  class HeapComparator {
   public:
    HeapComparator(const InternalKeyComparator& comparator) : c_(comparator) {}

    bool operator()(const HeapElement& a, const HeapElement& b) const {
      return is_less ? c_.Compare(a.key, b.key) < 0
                     : c_.Compare(a.key, b.key) > 0;
    }

    const InternalKeyComparator& internal_comparator() const { return c_; }

   private:
    const InternalKeyComparator& c_;
  };
  typedef std::vector<HeapElement> HeapVectorType;
  union {
    // They have same layout, but we only use one of them at same time
    BinaryHeap<HeapElement, HeapComparator<0>, HeapVectorType> min_heap_;
    BinaryHeap<HeapElement, HeapComparator<1>, HeapVectorType> max_heap_;
  };

  enum TryInitFirstLevelIterResult {
    kInitFirstIterOK,
    kInitFirstIterEmpty,
    kInitFirstIterInvalid,
  };

  TryInitFirstLevelIterResult TryInitFirstLevelIter() {
    min_heap_.clear();
    if (!first_level_iter_->Valid()) {
      return kInitFirstIterInvalid;
    }
    // Manual inline MapSstElement::Decode
    Slice map_input = first_level_iter_->value();
    link_.clear();
    largest_key_ = first_level_iter_->key();
    uint64_t link_count;
    uint64_t flags;
    if (!GetLengthPrefixedSlice(&map_input, &smallest_key_) ||
        !GetVarint64(&map_input, &link_count) ||
        !GetVarint64(&map_input, &flags) ||
        map_input.size() < link_count * sizeof(uint64_t)) {
      status_ = Status::Corruption("Map sst invalid value");
      return kInitFirstIterInvalid;
    }
    if ((flags >> MapSstElement::kNoRecords) & 1) {
      return kInitFirstIterEmpty;
    }
    include_smallest_ = (flags >> MapSstElement::kIncludeSmallest) & 1;
    include_largest_ = (flags >> MapSstElement::kIncludeLargest) & 1;
    link_.resize(link_count);
    for (uint64_t i = 0; i < link_count; ++i) {
      GetFixed64(&map_input, &link_[i]);
      assert(std::binary_search(file_meta_.sst_depend.begin(),
                                file_meta_.sst_depend.end(), link_[i]));
    }
    return kInitFirstIterOK;
  }

  bool InitFirstLevelIter() {
    auto result = TryInitFirstLevelIter();
    while (result == kInitFirstIterEmpty) {
      if (is_backword_) {
        first_level_iter_->Prev();
      } else {
        first_level_iter_->Next();
      }
      result = TryInitFirstLevelIter();
    }
    return result == kInitFirstIterOK;
  }

  void InitSecondLevelMinHeap(const Slice& target, bool include) {
    assert(min_heap_.empty());
    auto& icomp = min_heap_.comparator().internal_comparator();
    for (auto file_number : link_) {
      auto it = iterator_cache_.GetIterator(file_number);
      if (!it->status().ok()) {
        status_ = it->status();
        min_heap_.clear();
        return;
      }
      it->Seek(target);
      if (!it->Valid()) {
        continue;
      }
      if (!include && icomp.Compare(it->key(), target) == 0) {
        it->Next();
        if (!it->Valid()) {
          continue;
        }
      }
      auto k = it->key();
      if (icomp.Compare(k, largest_key_) < include_largest_) {
        min_heap_.push(HeapElement{it, k});
      }
    }
  }

  void InitSecondLevelMaxHeap(const Slice& target, bool include) {
    assert(max_heap_.empty());
    auto& icomp = min_heap_.comparator().internal_comparator();
    for (auto file_number : link_) {
      auto it = iterator_cache_.GetIterator(file_number);
      if (!it->status().ok()) {
        status_ = it->status();
        max_heap_.clear();
        return;
      }
      it->SeekForPrev(target);
      if (!it->Valid()) {
        continue;
      }
      if (!include && icomp.Compare(it->key(), target) == 0) {
        it->Prev();
        if (!it->Valid()) {
          continue;
        }
      }
      auto k = it->key();
      if (icomp.Compare(smallest_key_, k) < include_smallest_) {
        max_heap_.push(HeapElement{it, k});
      }
    }
  }

  bool IsInRange() {
    auto& icomp = min_heap_.comparator().internal_comparator();
    auto k = max_heap_.top().key;
    return icomp.Compare(smallest_key_, k) < include_smallest_ &&
           icomp.Compare(k, largest_key_) < include_largest_;
  }

 public:
  MapSstIterator(const FileMetaData& file_meta, InternalIterator* iter,
                 const DependFileMap& depend_files,
                 const InternalKeyComparator& icomp, void* create_arg,
                 const IteratorCache::CreateIterCallback& create)
      : file_meta_(file_meta),
        first_level_iter_(iter),
        is_backword_(false),
        iterator_cache_(depend_files, create_arg, create),
        include_smallest_(false),
        include_largest_(false),
        min_heap_(icomp) {
    if (file_meta_.sst_purpose != kMapSst) {
      abort();
    }
  }

  ~MapSstIterator() { min_heap_.~BinaryHeap(); }

  virtual bool Valid() const override { return !min_heap_.empty(); }
  virtual void SeekToFirst() override {
    is_backword_ = false;
    first_level_iter_->SeekToFirst();
    if (InitFirstLevelIter()) {
      InitSecondLevelMinHeap(smallest_key_, include_smallest_);
      assert(!min_heap_.empty());
      assert(IsInRange());
    }
  }
  virtual void SeekToLast() override {
    is_backword_ = true;
    first_level_iter_->SeekToLast();
    if (InitFirstLevelIter()) {
      InitSecondLevelMaxHeap(largest_key_, include_largest_);
      assert(!max_heap_.empty());
      assert(IsInRange());
    }
  }
  virtual void Seek(const Slice& target) override {
    is_backword_ = false;
    first_level_iter_->Seek(target);
    if (!InitFirstLevelIter()) {
      assert(min_heap_.empty());
      return;
    }
    auto& icomp = min_heap_.comparator().internal_comparator();
    Slice seek_target = target;
    bool include = true;
    // include_smallest ? cmp_result > 0 : cmp_result >= 0
    if (icomp.Compare(smallest_key_, target) >= include_smallest_) {
      seek_target = smallest_key_;
      include = include_smallest_;
    } else if (icomp.Compare(target, largest_key_) == 0 && !include_largest_) {
      first_level_iter_->Next();
      if (!InitFirstLevelIter()) {
        assert(min_heap_.empty());
        return;
      }
      seek_target = smallest_key_;
      include = include_smallest_;
    }
    InitSecondLevelMinHeap(seek_target, include);
    if (min_heap_.empty()) {
      first_level_iter_->Next();
      if (InitFirstLevelIter()) {
        InitSecondLevelMinHeap(smallest_key_, include_smallest_);
        assert(!min_heap_.empty());
        assert(IsInRange());
      }
    } else {
      assert(IsInRange());
    }
  }
  virtual void SeekForPrev(const Slice& target) override {
    is_backword_ = true;
    first_level_iter_->Seek(target);
    if (!InitFirstLevelIter()) {
      MapSstIterator::SeekToLast();
      return;
    }
    auto& icomp = min_heap_.comparator().internal_comparator();
    Slice seek_target = target;
    bool include = true;
    // include_smallest ? cmp_result > 0 : cmp_result >= 0
    if (icomp.Compare(smallest_key_, target) >= include_smallest_) {
      first_level_iter_->Prev();
      if (!InitFirstLevelIter()) {
        assert(max_heap_.empty());
        return;
      }
      seek_target = largest_key_;
      include = include_largest_;
    } else if (icomp.Compare(target, largest_key_) == 0 && !include_largest_) {
      include = false;
    }
    InitSecondLevelMaxHeap(seek_target, include);
    if (max_heap_.empty()) {
      first_level_iter_->Prev();
      if (InitFirstLevelIter()) {
        InitSecondLevelMaxHeap(largest_key_, include_largest_);
        assert(!max_heap_.empty());
        assert(IsInRange());
      }
    } else {
      assert(IsInRange());
    }
  }
  virtual void Next() override {
    if (is_backword_) {
      InternalKey where;
      where.DecodeFrom(max_heap_.top().key);
      max_heap_.clear();
      InitSecondLevelMinHeap(where.Encode(), false);
      is_backword_ = false;
    } else {
      auto current = min_heap_.top();
      current.iter->Next();
      if (current.iter->Valid()) {
        current.key = current.iter->key();
        min_heap_.replace_top(current);
      } else {
        min_heap_.pop();
      }
    }
    auto& icomp = min_heap_.comparator().internal_comparator();
    if (min_heap_.empty() ||
        icomp.Compare(min_heap_.top().key, largest_key_) >= include_largest_) {
      // out of largest bound
      first_level_iter_->Next();
      if (InitFirstLevelIter()) {
        InitSecondLevelMinHeap(smallest_key_, include_smallest_);
        assert(!min_heap_.empty());
        assert(IsInRange());
      }
    } else {
      assert(IsInRange());
    }
  }
  virtual void Prev() override {
    if (!is_backword_) {
      InternalKey where;
      where.DecodeFrom(min_heap_.top().key);
      min_heap_.clear();
      InitSecondLevelMaxHeap(where.Encode(), false);
      is_backword_ = true;
    } else {
      auto current = max_heap_.top();
      current.iter->Prev();
      if (current.iter->Valid()) {
        current.key = current.iter->key();
        max_heap_.replace_top(current);
      } else {
        max_heap_.pop();
      }
    }
    auto& icomp = min_heap_.comparator().internal_comparator();
    if (max_heap_.empty() ||
        icomp.Compare(smallest_key_, max_heap_.top().key) >=
            include_smallest_) {
      // out of smallest bound
      first_level_iter_->Prev();
      if (InitFirstLevelIter()) {
        InitSecondLevelMaxHeap(largest_key_, include_largest_);
        assert(!max_heap_.empty());
        assert(IsInRange());
      }
    } else {
      assert(IsInRange());
    }
  }
  virtual Slice key() const override { return min_heap_.top().key; }
  virtual Slice value() const override {
    return min_heap_.top().iter->value();
  }
  virtual Status status() const override { return status_; }
  virtual uint64_t FileNumber() const override {
    return !min_heap_.empty() ? min_heap_.top().iter->FileNumber()
                              : uint64_t(-1);
  }
  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override {
    iterator_cache_.SetPinnedItersMgr(pinned_iters_mgr);
  }
  virtual bool IsKeyPinned() const override {
    return !min_heap_.empty() && min_heap_.top().iter->IsKeyPinned();
  }
  virtual bool IsValuePinned() const override {
    return !min_heap_.empty() && min_heap_.top().iter->IsValuePinned();
  }
};

template <class IteratorType>
InternalIterator* NewCompositeSstIteratorTpl(
    const FileMetaData& file_meta, InternalIterator* mediate_sst_iter,
    const DependFileMap& depend_files, const InternalKeyComparator& icomp,
    void* callback_arg, const IteratorCache::CreateIterCallback& create_iter,
    Arena* arena) {
  if (arena == nullptr) {
    return new IteratorType(file_meta, mediate_sst_iter, depend_files, icomp,
                            callback_arg, create_iter);
  } else {
    void* buffer = arena->AllocateAligned(sizeof(IteratorType));
    return new (buffer)
        IteratorType(file_meta, mediate_sst_iter, depend_files, icomp,
                     callback_arg, create_iter);
  }
}

}  // namespace

InternalIteratorBase<BlockHandle>* NewTwoLevelIterator(
    TwoLevelIteratorState* state,
    InternalIteratorBase<BlockHandle>* first_level_iter) {
  return new TwoLevelIndexIterator(state, first_level_iter);
}

InternalIterator* NewCompositeSstIterator(
    const FileMetaData& file_meta, InternalIterator* mediate_sst_iter,
    const DependFileMap& depend_files, const InternalKeyComparator& icomp,
    void* callback_arg, const IteratorCache::CreateIterCallback& create_iter,
    Arena* arena) {
  switch (file_meta.sst_purpose) {
    case kLinkSst:
      return NewCompositeSstIteratorTpl<LinkSstIterator>(
          file_meta, mediate_sst_iter, depend_files, icomp, callback_arg,
          create_iter, arena);
    case kMapSst:
      return NewCompositeSstIteratorTpl<MapSstIterator>(
          file_meta, mediate_sst_iter, depend_files, icomp, callback_arg,
          create_iter, arena);
    default:
      assert(file_meta.sst_purpose != 0);
      return mediate_sst_iter;
  }
}

}  // namespace rocksdb
