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
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "util/arena.h"

namespace rocksdb {

namespace {

class TwoLevelIterator : public InternalIterator {
 public:
  explicit TwoLevelIterator(TwoLevelIteratorState* state,
                            InternalIterator* first_level_iter);

  virtual ~TwoLevelIterator() {
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
  virtual Slice value() const override {
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
  void SetSecondLevelIterator(InternalIterator* iter);
  void InitDataBlock();

  TwoLevelIteratorState* state_;
  IteratorWrapper first_level_iter_;
  IteratorWrapper second_level_iter_;  // May be nullptr
  Status status_;
  // If second_level_iter is non-nullptr, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the second_level_iter.
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(TwoLevelIteratorState* state,
                                   InternalIterator* first_level_iter)
    : state_(state), first_level_iter_(first_level_iter) {}

void TwoLevelIterator::Seek(const Slice& target) {
  first_level_iter_.Seek(target);

  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.Seek(target);
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekForPrev(const Slice& target) {
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

void TwoLevelIterator::SeekToFirst() {
  first_level_iter_.SeekToFirst();
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekToFirst();
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  first_level_iter_.SeekToLast();
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekToLast();
  }
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  second_level_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  second_level_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
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

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
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

void TwoLevelIterator::SetSecondLevelIterator(InternalIterator* iter) {
  InternalIterator* old_iter = second_level_iter_.Set(iter);
  delete old_iter;
}

void TwoLevelIterator::InitDataBlock() {
  if (!first_level_iter_.Valid()) {
    SetSecondLevelIterator(nullptr);
  } else {
    Slice handle = first_level_iter_.value();
    if (second_level_iter_.iter() != nullptr &&
        !second_level_iter_.status().IsIncomplete() &&
        handle.compare(data_block_handle_) == 0) {
      // second_level_iter is already constructed with this iterator, so
      // no need to change anything
    } else {
      InternalIterator* iter = state_->NewSecondaryIterator(handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetSecondLevelIterator(iter);
    }
  }
}

 class LinkSstIterator final : public InternalIterator {
  private:
   InternalIterator* first_level_iter_;
   InternalIterator* second_level_iter_;
   Slice bound_;
   bool has_bound_;
   bool is_backword_;
   Arena arena_;
   Status status_;
   const InternalKeyComparator& icomp_;
   std::function<InternalIterator*(uint64_t, Arena*)> create_iterator_;
   std::unordered_map<uint64_t, InternalIterator*> iterator_cache_;
   PinnedIteratorsManager* pinned_iters_mgr_;

   InternalIterator* GetIterator(uint64_t sst_id) {
     auto find = iterator_cache_.find(sst_id);
     if (find != iterator_cache_.end()) {
       return find->second;
     }
     auto iter = create_iterator_(sst_id, &arena_);
     iterator_cache_.emplace(sst_id, iter);
     if (iter != nullptr) {
       iter->SetPinnedItersMgr(pinned_iters_mgr_);
     } else {
       status_ = Status::Corruption("Link sst depend files missing");
     }
     return iter;
   }

   bool InitSecondLevelIter() {
     // Manual inline SstLinkElement::Decode
     uint64_t sst_id;
     Slice value_copy = first_level_iter_->value();
     if (!GetFixed64(&value_copy, &sst_id)) {
       status_ = Status::Corruption("Link sst invalid value");
       second_level_iter_ = nullptr;
       return false;
     }
     second_level_iter_ = GetIterator(sst_id);
     return second_level_iter_ != nullptr;
   }

   bool InitFirstLevelIter() {
     if (!first_level_iter_->Valid()) {
       second_level_iter_ = nullptr;
       return false;
     }
     bound_ = first_level_iter_->key();
     return InitSecondLevelIter();
   }

  public:
   LinkSstIterator(
       InternalIterator* iter,
       const InternalKeyComparator& icomp,
       const std::function<InternalIterator*(uint64_t, Arena*)>& create)
       : first_level_iter_(iter),
         second_level_iter_(nullptr),
         has_bound_(false),
         is_backword_(false),
         icomp_(icomp),
         create_iterator_(create),
         pinned_iters_mgr_(nullptr) {}

   ~LinkSstIterator() {
     for (auto pair : iterator_cache_) {
       pair.second->~InternalIterator();
     }
   }

   virtual bool Valid() const override {
     return second_level_iter_ != nullptr && second_level_iter_->Valid();
   }
   virtual void SeekToFirst() override {
     first_level_iter_->SeekToFirst();
     if (InitFirstLevelIter()) {
       second_level_iter_->SeekToFirst();
       is_backword_ = false;
     }
   }
   virtual void SeekToLast() override {
     first_level_iter_->SeekToLast();
     if (InitFirstLevelIter()) {
       second_level_iter_->SeekToLast();
       is_backword_ = false;
     }
   }
   virtual void Seek(const Slice& target) override {
     first_level_iter_->Seek(target);
     if (InitFirstLevelIter()) {
       second_level_iter_->Seek(target);
       is_backword_ = false;
     }
   }
   virtual void SeekForPrev(const Slice& target) override {
     LinkSstIterator::Seek(target);
     if (!LinkSstIterator::Valid()) {
       LinkSstIterator::SeekToLast();
     } else if (LinkSstIterator::key() != target) {
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
     if (second_level_iter_->key() != bound_) {
       second_level_iter_->Next();
       assert(second_level_iter_->Valid());
       return;
     }
     InternalKey where;
     where.DecodeFrom(bound_);
     first_level_iter_->Next();
     if (InitFirstLevelIter()) {
       second_level_iter_->Seek(where.Encode());
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
     if (!has_bound_) {
       second_level_iter_->Prev();
       return;
     }
     second_level_iter_->Prev();
     if (second_level_iter_->Valid() &&
         icomp_.Compare(second_level_iter_->key(), bound_) > 0) {
       return;
     }
     if (InitSecondLevelIter()) {
       second_level_iter_->SeekForPrev(bound_);
       first_level_iter_->Prev();
       has_bound_ = first_level_iter_->Valid();
       if (has_bound_) {
         bound_ = first_level_iter_->key();
       }
     }
   }
   virtual Slice key() const override {
     return second_level_iter_->key();
   }
   virtual Slice value() const override {
     return second_level_iter_->value();
   }
   virtual Status status() const override {
     return status_;
   }
   virtual IteratorSource source() const override {
     return second_level_iter_->source();
   }
   virtual void SetPinnedItersMgr(
       PinnedIteratorsManager* pinned_iters_mgr) override {
     pinned_iters_mgr_ = pinned_iters_mgr;
     for (auto pair : iterator_cache_) {
       pair.second->SetPinnedItersMgr(pinned_iters_mgr);
     }
   }
   virtual bool IsKeyPinned() const override {
     return second_level_iter_ != nullptr &&
            second_level_iter_->IsKeyPinned();
   }
   virtual bool IsValuePinned() const override {
     return second_level_iter_ != nullptr &&
            second_level_iter_->IsValuePinned();
   }
 };

}  // namespace

InternalIterator* NewTwoLevelIterator(TwoLevelIteratorState* state,
                                      InternalIterator* first_level_iter) {
  return new TwoLevelIterator(state, first_level_iter);
}

InternalIterator* NewLinkSstIterator(
    InternalIterator* link_sst_iter,
    const InternalKeyComparator& icomp,
    const std::function<InternalIterator*(uint64_t, Arena*)>& create_iter,
    Arena* arena) {
  if (arena == nullptr) {
    return new LinkSstIterator(link_sst_iter, icomp, create_iter);
  } else {
    void* buffer = arena->AllocateAligned(sizeof(LinkSstIterator));
    return new(buffer) LinkSstIterator(link_sst_iter, icomp, create_iter);
  }
}

}  // namespace rocksdb
