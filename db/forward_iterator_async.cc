//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "db/forward_iterator.h"

#include <limits>
#include <string>
#include <type_traits>
#include <utility>


#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/job_context.h"
#include "db/table_cache_request.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "table/merging_iterator.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {

using RangeDelHost = ForwardIteratorAsync::RangeDelHost;

// Usage:
//     LevelIteratorAsync iter;
//     iter.SetFileIndex(file_index);
//     iter.Seek(target);
//     iter.Next()

using LvlHost = LevelIteratorAsync;

class LevelIteratorAsync : public InternalIterator {;
 public:
  LevelIteratorAsync(const ColumnFamilyData* const cfd,
                const ReadOptions& read_options,
                const std::vector<FileMetaData*>& files)
      : cfd_(cfd),
        read_options_(read_options),
        files_(files),
        valid_(false),
        file_index_(std::numeric_limits<uint32_t>::max()),
        file_iter_(nullptr),
        pinned_iters_mgr_(nullptr) {}

  ~LevelIteratorAsync() {
    // Last async must either destroy it
    // OR we opreate sync
    assert(!ctx_constructed_);
    // Reset current pointer
    if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
      pinned_iters_mgr_->PinIterator(file_iter_);
    } else {
      delete file_iter_;
    }
  }

  Status SetFileIndex(const TableCache::NewIteratorCallback& iter_cb,
    InternalIterator** iter, RangeDelAggregator* range_del_agg,
    uint32_t file_index) {

    Status s;
    assert(iter != nullptr);
    assert(range_del_agg != nullptr);
    *iter = nullptr;
    assert(file_index < files_.size());
    if (file_index != file_index_) {
      file_index_ = file_index;
      s = Reset(iter_cb, iter, range_del_agg);
    }

    return s;
  }
  Status Reset(const TableCache::NewIteratorCallback& iter_cb, InternalIterator** iter,
    RangeDelAggregator* range_del_agg) {
    Status s;
    assert(file_index_ < files_.size());

    // Reset current pointer
    if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
      pinned_iters_mgr_->PinIterator(file_iter_);
    } else {
      delete file_iter_;
    }

    if (iter_cb) {
      s = async::TableCacheNewIteratorContext::RequestCreate(iter_cb,
        cfd_->table_cache(), read_options_,
        *(cfd_->soptions()), cfd_->internal_comparator(), files_[file_index_]->fd,
        read_options_.ignore_range_deletions ? nullptr : range_del_agg, iter,
        nullptr /* table_reader_ptr */, nullptr /*file_read_hist*/,
        false /*for_compaction*/);

    } else {
      s = async::TableCacheNewIteratorContext::Create(cfd_->table_cache(),
        read_options_, *(cfd_->soptions()), cfd_->internal_comparator(),
        files_[file_index_]->fd,
        read_options_.ignore_range_deletions ? nullptr : range_del_agg, iter,
        nullptr /* table_reader_ptr */, nullptr  /*file_read_hist*/,
        false /*for_compaction*/);
    }

    return s;
  }

  void SeekToLast() override {
    assert(false);
    status_ = Status::NotSupported("LevelIteratorAsync::SeekToLast()");
    valid_ = false;
  }

  Status RequestSeekToLast(const Callback&) override {
    assert(false);
    status_ = Status::NotSupported("LevelIteratorAsync::RequestSeekToLast()");
    valid_ = false;
    return status_;
  }

  void Prev() override {
    assert(false);
    status_ = Status::NotSupported("LevelIteratorAsync::Prev()");
    valid_ = false;
  }

  Status RequestPrev(const Callback&) override {
    assert(false);
    status_ = Status::NotSupported("LevelIteratorAsync::Prev()");
    valid_ = false;
    return status_;
  }

  bool Valid() const override {
    return valid_;
  }

  // Will support this for sync testing if needed
  void SeekToFirst() override;

  Status RequestSeekToFirst(const Callback& cb) override;

  void Seek(const Slice&) override;

  Status RequestSeek(const Callback& cb, const Slice& internal_key) override;

  void SeekForPrev(const Slice& internal_key) override {
    status_ = Status::NotSupported("LevelIteratorAsync::SeekForPrev()");
    valid_ = false;
  }

  void Next() override;

  Status RequestNext(const Callback& cb) override;

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
  bool IsKeyPinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_->IsKeyPinned();
  }
  bool IsValuePinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_->IsValuePinned();
  }
  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    if (file_iter_) {
      file_iter_->SetPinnedItersMgr(pinned_iters_mgr_);
    }
  }

  //////////////////////////////////////////////////////////////
  // Async related
  void CompleteReset(InternalIterator* file_iterator, RangeDelAggregator* range_del_agg) {
    file_iter_ = file_iterator;
    file_iter_->SetPinnedItersMgr(pinned_iters_mgr_);
    if (range_del_agg != nullptr && !range_del_agg->IsEmpty()) {
      status_ = Status::NotSupported(
        "Range tombstones unsupported with ForwardIteratorAsync");
      valid_ = false;
    }
  }

  void CompleteSetFileIndex(InternalIterator* file_iterator, RangeDelAggregator* range_del_agg) {
    // with nullptr no new iterator was set
    if (file_iterator != nullptr) {
      CompleteReset(file_iterator, range_del_agg);
    }
    valid_ = false;
  }

private:
  const ColumnFamilyData* const cfd_;
  const ReadOptions& read_options_;
  const std::vector<FileMetaData*>& files_;

  bool valid_;
  uint32_t file_index_;
  Status status_;
  InternalIterator* file_iter_;
  PinnedIteratorsManager* pinned_iters_mgr_;

  void CompleteSeek() {
    valid_ = file_iter_->Valid();
  }

  // SeekToFirst()
  void CompleteSeekToFirst() {
    valid_ = file_iter_->Valid();
  }

  // Async context classes to be allocated inline with the
  // body of the LevelIteratorAsync
  class CtxBase :
    protected async::AsyncStatusCapture {
  public:
    using Callback = InternalIterator::Callback;
    virtual ~CtxBase() {}
  protected:
    CtxBase(LvlHost* lvlhost, const Callback& cb, const Slice& target) :
      lvlhost_(lvlhost), cb_(cb), target_(target) {}
    CtxBase(LvlHost* lvlhost, const Callback& cb) :
      CtxBase(lvlhost, cb, Slice()) {}
    Status OnComplete(const Status& status) {
      Status s(lvlhost_->status());
      if (async()) {
        s.async(true);
        cb_.Invoke(s);
        lvlhost_->DestroyContext();
      }
      return s;
    }
  protected:
    LvlHost*           lvlhost_;
    Callback           cb_;
    Slice              target_;
  };

  // Separate base and management for expensive
  // Range Del Aggregator. For repeated SetIndex
  // we will need construct/destruct loop
  class CtxBaseRangeDel : public CtxBase {
  private:
    RangeDelHost  range_del_;
  public:
    CtxBaseRangeDel(LvlHost* lvlhost, const Callback& cb) :
      CtxBaseRangeDel(lvlhost, cb, Slice()) {}
    CtxBaseRangeDel(LvlHost* lvlhost, const Callback& cb, const Slice& target) :
      CtxBase(lvlhost, cb, target) {}
    void Construct() {
      range_del_.Construct(lvlhost_->cfd_->internal_comparator());
    }
    void Destruct() {
      range_del_.Destruct();
    }
    RangeDelAggregator* GetAgg() {
      return range_del_.GetAgg();
    }
  };

  class CtxSeekToFirst : public CtxBaseRangeDel {
    Callback  file_iter_cb_;
  public:
    CtxSeekToFirst(LvlHost* lvlhost, const Callback& cb) :
      CtxBaseRangeDel(lvlhost, cb) {
      CtxBaseRangeDel::Construct();
      if (cb) {
        async::CallableFactory<CtxSeekToFirst, Status, const Status&> f(this);
        file_iter_cb_ = f.GetCallable<&CtxSeekToFirst::OnFileIterSeekToFirst>();
      }
    }
    Status OnSetIndex(const Status& status, InternalIterator* iter,
      TableReader*) {
      async(status);
      Status s;
      lvlhost_->CompleteSetFileIndex(iter, GetAgg());
      if (cb_) {
        s = lvlhost_->file_iter_->RequestSeekToFirst(file_iter_cb_);
      } else {
        lvlhost_->file_iter_->SeekToFirst();
      }
      if (s.IsIOPending()) {
        return s;
      }
      lvlhost_->CompleteSeekToFirst();
      return OnComplete(status);
    }

    Status OnFileIterSeekToFirst(const Status& status) {
      async(status);
      lvlhost_->CompleteSeekToFirst();
      return OnComplete(status);
    }
  };

  class CtxSeek : public CtxBase {
  public:
    CtxSeek(LvlHost* lvlhost, const Callback& cb, const Slice& target) :
      CtxBase(lvlhost, cb, target) {}
    Status OnFileIterSeek(const Status& status) {
      async(status);
      lvlhost_->CompleteSeek();
      return OnComplete(status);
    }
  };

  class CtxNext : public CtxBaseRangeDel {
    TableCache::NewIteratorCallback on_sefile_index_;
    Callback                        on_filter_next_;
  public:
    CtxNext(LvlHost* lvlhost, const Callback& cb) :
      CtxBaseRangeDel(lvlhost, cb) {
      if (cb) {
        async::CallableFactory<CtxNext, Status, const Status&, InternalIterator*,
          TableReader*> f(this);
        on_sefile_index_ = f.GetCallable<&CtxNext::OnSetFileIndex>();
        async::CallableFactory<CtxNext, Status, const Status&> fn(this);
        on_filter_next_ = fn.GetCallable<&CtxNext::OnFileIterNext>();
      }
    }
    const Callback& GetOnFileIterNext() const {
      return  on_filter_next_;
    }
    Status OnFileIterNext(const Status& status);

  private:

    bool CheckForTerminate();

    Status OnSetFileIndex(const Status& status, InternalIterator* iter,
      TableReader*);
  };

  // Async context constructed for each of the async operations
  // inline with the class. Declare enough storage for all of them
  // properly aligned
  std::aligned_union<sizeof(CtxBase), CtxSeekToFirst, CtxSeek, CtxNext>::type async_context_;
  bool ctx_constructed_ = false;

  template<class T,class... Args>
  void ConstructContext(Args... args) {
    assert(!ctx_constructed_);
    new (&async_context_) T(std::forward<Args>(args)...);
    ctx_constructed_ = true;
  }
  template<class T>
  T* GetCtx() {
    assert(ctx_constructed_);
    return reinterpret_cast<T*>(&async_context_);
  }
  // Destruction takes place from OnComplete
  // we rely on virtual destructor
  void DestroyContext() {
    assert(ctx_constructed_);
    GetCtx<CtxBase>()->~CtxBase();
    ctx_constructed_ = false;
  }
}; // LevelIteratorAsync

inline
Status LvlHost::CtxNext::OnFileIterNext(const Status& status) {
  async(status);

  if (!CheckForTerminate()) {
    CtxBaseRangeDel::Construct();
    InternalIterator* iter = nullptr;
    Status s = lvlhost_->SetFileIndex(on_sefile_index_, &iter, GetAgg(),
      lvlhost_->file_index_ + 1);

    if (!s.IsIOPending()) {
      s = OnSetFileIndex(s, iter, nullptr);
    }
    return s;
  }
  return OnComplete(lvlhost_->file_iter_->status());
}

inline
bool LvlHost::CtxNext::CheckForTerminate() {
  bool result = false;
  if (lvlhost_->file_iter_->status().IsIncomplete() ||
    lvlhost_->file_iter_->Valid()) {
    lvlhost_->valid_ = !lvlhost_->file_iter_->status().IsIncomplete();
    result = true;
  } else if (lvlhost_->file_index_ + 1 >= lvlhost_->files_.size()) {
    lvlhost_->valid_ = false;
    result = true;
  }
  return result;
}

inline
Status LvlHost::CtxNext::OnSetFileIndex(const Status& status, InternalIterator* iter,
  TableReader*) {
  async(status);

  lvlhost_->CompleteSetFileIndex(iter, GetAgg());
  CtxBaseRangeDel::Destruct();

  Status s;
  if (on_filter_next_) {
    s = lvlhost_->file_iter_->RequestSeekToFirst(on_filter_next_);
    if (s.IsIOPending()) {
      return s;
    }
  } else {
    lvlhost_->file_iter_->SeekToFirst();
  }

  while (!CheckForTerminate()) {

    CtxBaseRangeDel::Construct();
    s = lvlhost_->SetFileIndex(on_sefile_index_, &iter, GetAgg(),
      lvlhost_->file_index_ + 1);
    if (s.IsIOPending()) {
      return s;
    }
    lvlhost_->CompleteSetFileIndex(iter, GetAgg());
    CtxBaseRangeDel::Destruct();

    if (on_filter_next_) {
      s = lvlhost_->file_iter_->RequestSeekToFirst(on_filter_next_);
      if (s.IsIOPending()) {
        return s;
      }
    } else {
      lvlhost_->file_iter_->SeekToFirst();
    }
  }
  return OnComplete(lvlhost_->file_iter_->status());
}


void LevelIteratorAsync::SeekToFirst() {
  ConstructContext<CtxSeekToFirst>(this, Callback());
  auto* ctx = GetCtx<CtxSeekToFirst>();
  InternalIterator* iter = nullptr;
  TableCache::NewIteratorCallback on_set_index;
  Status s = SetFileIndex(on_set_index, &iter, ctx->GetAgg(), 0);
  ctx->OnSetIndex(s, iter, nullptr);
  DestroyContext();
}

Status LevelIteratorAsync::RequestSeekToFirst(const Callback& cb) {
  assert(cb);

  Status s;
  ConstructContext<CtxSeekToFirst>(this, cb);
  auto* ctx = GetCtx<CtxSeekToFirst>();
  async::CallableFactory<CtxSeekToFirst, Status, const Status&, InternalIterator*, TableReader*>
    f(ctx);
  auto on_set_index = f.GetCallable<&CtxSeekToFirst::OnSetIndex>();

  InternalIterator* iter = nullptr;
  s = SetFileIndex(on_set_index, &iter, ctx->GetAgg(), 0);
  if (!s.IsIOPending()) {
    s = ctx->OnSetIndex(s, iter, nullptr);
    if (!s.IsIOPending()) {
      DestroyContext();
    }
  }

  return s;
}

void LevelIteratorAsync::Seek(const Slice& internal_key) {
  assert(file_iter_ != nullptr);
  ConstructContext<CtxSeek>(this, Callback(), internal_key);
  file_iter_->Seek(internal_key);
  GetCtx<CtxSeek>()->OnFileIterSeek(Status());
  DestroyContext();
}


Status LevelIteratorAsync::RequestSeek(const Callback& cb, const Slice& internal_key) {
  assert(file_iter_ != nullptr);

  Status s;
  ConstructContext<CtxSeek>(this, cb, internal_key);
  async::CallableFactory<CtxSeek, Status, const Status&> f(GetCtx<CtxSeek>());
  auto on_fileiter_seek = f.GetCallable<&CtxSeek::OnFileIterSeek>();

  s = file_iter_->RequestSeek(on_fileiter_seek, internal_key);
  if (!s.IsIOPending()) {
    s = GetCtx<CtxSeek>()->OnFileIterSeek(s);
    DestroyContext();
  }
  return s;
}

void LevelIteratorAsync::Next() {
  assert(valid_);
  ConstructContext<CtxNext>(this, Callback());
  file_iter_->Next();
  GetCtx<CtxNext>()->OnFileIterNext(Status());
  DestroyContext();
}

Status LevelIteratorAsync::RequestNext(const Callback& cb) {
  assert(valid_);
  assert(cb);

  ConstructContext<CtxNext>(this, cb);
  auto* ctx = GetCtx<CtxNext>();
  Status s = file_iter_->RequestNext(ctx->GetOnFileIterNext());
  if (!s.IsIOPending()) {
    s = ctx->OnFileIterNext(s);
  }
  if (!s.IsIOPending()) {
    DestroyContext();
  }
  return s;
}

/////////////////////////////////////////////////////////////////////////////////////////
inline
void ForwardIteratorAsync::ConstructMemIterators(SuperVersion* sv, RangeDelAggregator* range_del_agg) {

  mutable_iter_ = sv->mem->NewIterator(read_options_, &arena_);
  sv->imm->AddIterators(read_options_, &imm_iters_, &arena_);
  if (!read_options_.ignore_range_deletions) {
    std::unique_ptr<InternalIterator> range_del_iter(
      sv->mem->NewRangeTombstoneIterator(read_options_));
    range_del_agg->AddTombstones(std::move(range_del_iter));
    sv->imm->AddRangeTombstoneIterators(read_options_, &arena_,
      range_del_agg);
  }
}

inline
void ForwardIteratorAsync::CleanupAndRefreshSuperversion(bool refresh_sv) {
  Cleanup(refresh_sv);
  if (refresh_sv) {
    // New
    sv_ = cfd_->GetReferencedSuperVersion(&(db_->mutex_));
  }
}

inline
SuperVersion* ForwardIteratorAsync::GetLatestSuperVersion() {
  assert(sv_);
  return cfd_->GetReferencedSuperVersion(&(db_->mutex_));
}

inline
void ForwardIteratorAsync::DeleteMemIterators() {
  if (mutable_iter_ != nullptr) {
    DeleteIterator(mutable_iter_, true /* is_arena */);
  }
  for (auto* m : imm_iters_) {
    DeleteIterator(m, true /* is_arena */);
  }
  imm_iters_.clear();
}

inline
void ForwardIteratorAsync::CleanupOldIterators(std::vector<InternalIterator*>& l0_iters_new) {
  for (auto* f : l0_iters_) {
    DeleteIterator(f);
  }
  l0_iters_.clear();
  l0_iters_.swap(l0_iters_new);

  for (auto* l : level_iters_) {
    DeleteIterator(l);
  }
  level_iters_.clear();
}


// Context Base class FwdIter async operations
using FIHost = ForwardIteratorAsync;

inline
FIHost::CtxBase::CtxBase(FIHost* fihost, const Callback& cb) :
  fihost_(fihost), cb_(cb),
  current_l0_file_(0),
  svnew_(nullptr),
  current_level_file_(0),
  internal_key_(),
  seek_to_first_(false),
  current_level_(0) {
  // This is needed for rebuildIterators
  if (cb_) {
    async::CallableFactory<CtxBase, Status, const Status&, InternalIterator*,
      TableReader*> f(this);
    on_new_iter_rebuild_cb_ = f.GetCallable<&CtxBase::OnLeve0RebuildIterator>();
    on_new_iter_renew_cb_ = f.GetCallable<&CtxBase::OnLeve0RenewIterator>();
    on_reset_incomplete_cb_ = f.GetCallable<&CtxBase::OnLeve0ResetIncomplete>();
    on_incomplete_reset_cb_ = f.GetCallable<&CtxBase::OnIncompleteLevelReset>();
  }
}

inline
Status FIHost::CtxBase::OnComplete(const Status& status) {
  Status s(fihost_->status());
  if (async()) {
    s.async(true);
    cb_.Invoke(s);
    fihost_->DestroyContext();
  }
  return s;
}

////////////////////////////////////////////////////////////////////////
// RebuildIterators entry point
//
// We start and continue with RebuildLevel0 and resume the loop everytime
// on the next level 0 file. We bounce between the callback OnLevel0Iterator
// and RebuildLevel0. Once we are done we sync BuildLevelIterators since there are
// no async calls there and then finish with CompleteRebuild() which on async
// invokes the continuation callback provided (on_rebuild_complete)
// Note that this is not the end of the iterator operation but just the rebuild portion.
// The rest is done by a specific context
inline
Status FIHost::CtxBase::RebuildIterators(bool refresh_sv, const Callback& on_rebuild_complete) {

  Status s;

  assert(!cb_ || on_rebuild_complete);

  on_internal_complete_ = on_rebuild_complete;

  fihost_->CleanupAndRefreshSuperversion(refresh_sv);
  range_del_.Construct(InternalKeyComparator(fihost_->cfd_->internal_comparator()));
  fihost_->ConstructMemIterators(fihost_->sv_, range_del_.GetAgg());
  fihost_->has_iter_trimmed_for_upper_bound_ = false;

  const auto* vstorage = fihost_->sv_->current->storage_info();
  const auto& l0_files = vstorage->LevelFiles(0);

  current_l0_file_ = 0;
  fihost_->l0_iters_.reserve(l0_files.size());
  s = Rebuildlevel0();
 
  if (!s.IsIOPending()) {
    // Looks like we rebuild all level0
    // continue with rebuilding level iterators
    fihost_->BuildLevelIterators(vstorage);
    CompleteRebuild();
  }
  return s;
}

inline
Status FIHost::CtxBase::Rebuildlevel0() {

  Status s;
  const auto* vstorage = fihost_->sv_->current->storage_info();
  const auto& l0_files = vstorage->LevelFiles(0);
  auto& read_options = fihost_->read_options_;
  auto* cfd = fihost_->cfd_;

  const size_t l0_size = l0_files.size();
  for (; current_l0_file_ < l0_size; ++current_l0_file_) {
    auto* l0 = l0_files[current_l0_file_];
    if ((read_options.iterate_upper_bound != nullptr) &&
      cfd->internal_comparator().user_comparator()->Compare(
        l0->smallest.user_key(), *read_options.iterate_upper_bound) > 0) {
      fihost_->has_iter_trimmed_for_upper_bound_ = true;
      fihost_->l0_iters_.push_back(nullptr);
      continue;
    }

    InternalIterator* iter = nullptr;
    if (on_internal_complete_) {
      s = async::TableCacheNewIteratorContext::RequestCreate(on_new_iter_rebuild_cb_,
        cfd->table_cache(), read_options,
        *cfd->soptions(), cfd->internal_comparator(), l0->fd,
        range_del_.GetAgg(), &iter);


      if (s.IsIOPending()) {
        break;
      }
    } else {
      s = async::TableCacheNewIteratorContext::Create(cfd->table_cache(),
        read_options, *(cfd->soptions()), cfd->internal_comparator(), l0->fd,
        read_options.ignore_range_deletions ? nullptr : range_del_.GetAgg(), &iter);
    }

    assert(iter != nullptr);
    fihost_->l0_iters_.push_back(iter);
  }
  return s;
}

// When a new L0 iterator created async
inline
Status FIHost::CtxBase::OnLeve0RebuildIterator(const Status& status, InternalIterator* iter, TableReader*) {
  async(status);

  Status s;

  assert(iter != nullptr);
  fihost_->l0_iters_.push_back(iter);

  ++current_l0_file_;
  const auto* vstorage = fihost_->sv_->current->storage_info();
  const auto& l0_files = vstorage->LevelFiles(0);
  if (current_l0_file_ < l0_files.size()) {
    // Continue rebuilding
    s = Rebuildlevel0();
  }

  if (!s.IsIOPending()) {
    // Continue with Level iterators
    fihost_->BuildLevelIterators(vstorage);
    CompleteRebuild();
  }

  return s;
}

inline
void FIHost::CtxBase::CompleteRebuild() {

  fihost_->current_ = nullptr;
  fihost_->is_prev_set_ = false;

  fihost_->UpdateChildrenPinnedItersMgr();
  if (!range_del_.GetAgg()->IsEmpty()) {
    fihost_->status_ = Status::NotSupported(
      "Range tombstones unsupported with ForwardIteratorAsync");
    fihost_->valid_ = false;
  }

  range_del_.Destruct();

  if (async()) {
    Status s(fihost_->status_);
    s.async(true);
    // We expect the callback to be valid
    on_internal_complete_.Invoke(s);
  }
}

/////////////////////////////////////////////////////////////////////
// RenewIterators entry point
inline
Status FIHost::CtxBase::RenewIterators(const Callback& on_renew_iterators) {
  Status s;

  assert(!cb_ || on_renew_iterators);
  on_internal_complete_ = on_renew_iterators;

  svnew_ = fihost_->GetLatestSuperVersion();
  fihost_->DeleteMemIterators();

  range_del_.Construct(InternalKeyComparator(fihost_->cfd_->internal_comparator()));
  fihost_->ConstructMemIterators(svnew_, range_del_.GetAgg());

  // Setup renewal
  const auto* vstorage_new = svnew_->current->storage_info();
  const auto& l0_files_new = vstorage_new->LevelFiles(0);

  l0_iters_new_.clear();
  l0_iters_new_.reserve(l0_files_new.size());
  current_level_file_ = 0;

  s = RenewLevel0();
  if (!s.IsIOPending()) {
    // Looks like we rebuild all level0
    // continue with rebuilding level iterators
    fihost_->CleanupOldIterators(l0_iters_new_);
    fihost_->BuildLevelIterators(vstorage_new);
    CompleteRenew();
  }
  return s;
}
inline
Status FIHost::CtxBase::RenewLevel0() {
  Status s;
  const auto* vstorage = fihost_->sv_->current->storage_info();
  const auto& l0_files = vstorage->LevelFiles(0);
  const auto* vstorage_new = svnew_->current->storage_info();
  const auto& l0_files_new = vstorage_new->LevelFiles(0);

  auto& read_options = fihost_->read_options_;
  auto* cfd = fihost_->cfd_;

  // Start or continue on callback
  for (; current_level_file_ < l0_files_new.size(); ++current_level_file_) {

    bool found = false;
    size_t iold = 0;
    for (; iold < l0_files.size(); ++iold) {
      if (l0_files[iold] == l0_files_new[current_level_file_]) {
        found = true;
        break;
      }
    }

    // Reuse the old iterator
    if(found) {
      l0_iters_new_.push_back(fihost_->l0_iters_[iold]);
      if (fihost_->l0_iters_[iold] == nullptr) {
        TEST_SYNC_POINT_CALLBACK("ForwardIterator::RenewIterators:Null", fihost_);
      } else {
        fihost_->l0_iters_[iold] = nullptr;
        TEST_SYNC_POINT_CALLBACK("ForwardIterator::RenewIterators:Copy", fihost_);
      }
      continue;
    }

    InternalIterator* iter = nullptr;
    if (on_internal_complete_) {
      s = async::TableCacheNewIteratorContext::RequestCreate(on_new_iter_renew_cb_,
        cfd->table_cache(), read_options,
        *cfd->soptions(), cfd->internal_comparator(), l0_files_new[current_level_file_]->fd,
        range_del_.GetAgg(), &iter);

      if (s.IsIOPending()) {
        break;
      }
    } else {
      s = async::TableCacheNewIteratorContext::Create(cfd->table_cache(),
        read_options, *(cfd->soptions()), cfd->internal_comparator(), l0_files_new[current_level_file_]->fd,
        read_options.ignore_range_deletions ? nullptr : range_del_.GetAgg(), &iter);
    }

    l0_iters_new_.push_back(iter);
  }

  return s;
}

inline
Status FIHost::CtxBase::OnLeve0RenewIterator(const Status& status, InternalIterator* iter, TableReader*) {
  async(status);

  Status s;

  assert(iter != nullptr);
  l0_iters_new_.push_back(iter);

  const auto* vstorage_new = svnew_->current->storage_info();
  const auto& l0_files_new = vstorage_new->LevelFiles(0);

  ++current_level_file_;
  if (current_level_file_ < l0_files_new.size()) {
    s = RenewLevel0();
  }

  if (!s.IsIOPending()) {
    // Continue with Level iterators
    fihost_->CleanupOldIterators(l0_iters_new_);
    fihost_->BuildLevelIterators(vstorage_new);
    CompleteRenew();
  }

  return s;
}
inline
void FIHost::CtxBase::CompleteRenew() {
  fihost_->current_ = nullptr;
  fihost_->is_prev_set_ = false;
  fihost_->SVCleanup();
  fihost_->sv_ = svnew_;
  svnew_ = nullptr; 

  fihost_->UpdateChildrenPinnedItersMgr();

  if (!range_del_.GetAgg()->IsEmpty()) {
    fihost_->status_ = Status::NotSupported(
      "Range tombstones unsupported with ForwardIteratorAsync");
    fihost_->valid_ = false;
  }

  range_del_.Destruct();

  if (async()) {
    Status s(fihost_->status_);
    s.async(true);
    on_internal_complete_.Invoke(s);
  }
}

/////////////////////////////////////////////////////////////////
// Reset Incomplete iterators
inline
Status FIHost::CtxBase::ResetIncompleteIterators(const Callback& on_reset) {
  Status s;
  assert(!cb_ || on_reset);
  on_internal_complete_ = on_reset;
  current_l0_file_ = 0;
  s = ResetIncompleteLevel0();

  if (!s.IsIOPending()) {
    current_level_file_ = 0;
    s = ResetIncompleteLevelIterators();
    if (!s.IsIOPending()) {
      CompleteReset();
    }
  }
  return s;
}
inline
Status FIHost::CtxBase::ResetIncompleteLevel0() {
  Status s;

  const auto& l0_files = 
    fihost_->sv_->current->storage_info()->LevelFiles(0);
  auto& l0_iters = fihost_->l0_iters_;

  auto& read_options = fihost_->read_options_;
  auto* cfd = fihost_->cfd_;

  for (size_t i = current_l0_file_; 
    i < l0_iters.size(); ++i, ++current_l0_file_) {
    assert(i < l0_files.size());
    if (!l0_iters[i] || !l0_iters[i]->status().IsIncomplete()) {
      continue;
    }

    fihost_->DeleteIterator(l0_iters[i]);
    l0_iters[i] = nullptr;

    InternalIterator* iter = nullptr;
    if (on_internal_complete_) {
      s = async::TableCacheNewIteratorContext::RequestCreate(on_reset_incomplete_cb_,
        cfd->table_cache(), read_options,
        *cfd->soptions(), cfd->internal_comparator(), l0_files[i]->fd,
        nullptr /* range_del_agg*/, &iter);

      if (s.IsIOPending()) {
        break;
      }
    } else {
      s = async::TableCacheNewIteratorContext::Create(cfd->table_cache(),
        read_options, *(cfd->soptions()), cfd->internal_comparator(), l0_files[i]->fd,
        nullptr /* range_del_agg*/, &iter);
    }

    l0_iters[i] = iter;
    l0_iters[i]->SetPinnedItersMgr(fihost_->pinned_iters_mgr_);
  }
  return s;
}

inline
Status FIHost::CtxBase::OnLeve0ResetIncomplete(const Status& status, InternalIterator* iter, TableReader*) {
  async(status);

  Status s;

  assert(iter != nullptr);

  const auto* vstorage = svnew_->current->storage_info();
  const auto& l0_files = vstorage->LevelFiles(0);
  auto& l0_iters = fihost_->l0_iters_;

  l0_iters[current_l0_file_] = iter;
  l0_iters[current_l0_file_]->SetPinnedItersMgr(fihost_->pinned_iters_mgr_);

  ++current_l0_file_;
  if (current_l0_file_ < l0_files.size()) {
    s = ResetIncompleteLevel0();
  }

  if (!s.IsIOPending()) {
    current_level_file_ = 0;
    s = ResetIncompleteLevelIterators();
    if (!s.IsIOPending()) {
      CompleteReset();
    }
  }
  return s;
}

inline
Status FIHost::CtxBase::ResetIncompleteLevelIterators() {

  Status s;
  const size_t lvl_iters_size = fihost_->level_iters_.size();
  for (; current_level_file_ < lvl_iters_size; ++current_level_file_) {
    auto* lvl_iter = fihost_->level_iters_[current_level_file_];
    if (lvl_iter && lvl_iter->status().IsIncomplete()) {
      range_del_.Construct(fihost_->cfd_->internal_comparator());
      InternalIterator* file_iter = nullptr;
      if (on_internal_complete_) {
        s = lvl_iter->Reset(on_incomplete_reset_cb_, &file_iter, range_del_.GetAgg());
      } else {
        s = lvl_iter->Reset(NewIteratorCB(), &file_iter, range_del_.GetAgg());
      }
      if (s.IsIOPending()) {
        return s;
      }
      assert(file_iter != nullptr);
      lvl_iter->CompleteReset(file_iter, range_del_.GetAgg());
      range_del_.Destruct();
    }
  }
  return s;
}

inline
Status FIHost::CtxBase::OnIncompleteLevelReset(const Status& status, InternalIterator* file_iter, TableReader*) {
  async(status);
  Status s;

  assert(file_iter != nullptr);
  auto* lvl_iter = fihost_->level_iters_[current_level_file_];
  lvl_iter->CompleteReset(file_iter, range_del_.GetAgg());
  range_del_.Destruct();

  ++current_level_file_;
  if (current_level_file_ < fihost_->level_iters_.size()) {
    s = ResetIncompleteLevelIterators();
  }
  if (!s.IsIOPending()) {
    CompleteReset();
  }
  return s;
}

inline
void FIHost::CtxBase::CompleteReset() {
  fihost_->current_ = nullptr;
  fihost_->is_prev_set_ = false;
  if (async()) {
    Status s(fihost_->status_);
    s.async(true);
    on_internal_complete_.Invoke(s);
  }
}

////////////////////////////////////////////////////
// SeekInternal
inline
Status FIHost::CtxBase::SeekInternal(const Callback& on_seek_internal, const Slice& internal_key,
  bool seek_to_first) {

  Status s;
  // Save async state in case we go async
  on_internal_complete_ = on_seek_internal;
  internal_key_ = internal_key;
  seek_to_first_ = seek_to_first;

  assert(fihost_->mutable_iter_);
  // mutable always sync
  seek_to_first ? fihost_->mutable_iter_->SeekToFirst() :
    fihost_->mutable_iter_->Seek(internal_key);

  if (!seek_to_first && !fihost_->NeedToSeekImmutable(internal_key)) {
    if (fihost_->current_ && fihost_->current_ != fihost_->mutable_iter_) {
      // current_ is one of immutable iterators, push it back to the heap
      fihost_->immutable_min_heap_.push(fihost_->current_);
    }
    CompleteSeekInternal();
    return s;
  }

  fihost_->immutable_status_ = Status::OK();
  if (fihost_->has_iter_trimmed_for_upper_bound_ &&
    (
      // prev_ is not set yet
      fihost_->is_prev_set_ == false ||
      // We are doing SeekToFirst() and internal_key.size() = 0
      seek_to_first ||
      // prev_key_ > internal_key
      fihost_->cfd_->internal_comparator().InternalKeyComparator::Compare(
        fihost_->prev_key_.GetInternalKey(), internal_key) > 0)) {
    // Some iterators are trimmed. Need to rebuild.
    Callback on_seek_rebuild;
    if (on_internal_complete_) {
      async::CallableFactory<CtxBase, Status, const Status&> f(this);
      on_seek_rebuild = f.GetCallable<&CtxBase::OnSeekRebuildContinue>();
    }

    s = RebuildIterators(true, on_seek_rebuild);
    if (s.IsIOPending()) {
      return s;
    }
    s = OnSeekRebuildContinue(s);
  } else {
    s = SeekAllLevels();
  }

  return s;
}

inline
Status FIHost::CtxBase::OnSeekRebuildContinue(const Status& status) {
  async(status);

  Status s;

  // Already seeked mutable iter, so seek again
  seek_to_first_ ? fihost_->mutable_iter_->SeekToFirst()
    : fihost_->mutable_iter_->Seek(internal_key_);

  return SeekAllLevels();
}

inline
Status FIHost::CtxBase::SeekAllLevels() {

  SeekImmutable();

  if (!seek_to_first_) {
    user_key_ = ExtractUserKey(internal_key_);
  }
  current_l0_file_ = 0;
  Status s = SeekLevel0();
  if (!s.IsIOPending()) {
    current_level_ = 1;
    s = SeekLevels();
    if (!s.IsIOPending()) {
      CompleteSeekLevels();
      CompleteSeekInternal();
    }
  }
  return s;
}

inline
void FIHost::CtxBase::SeekImmutable() {

  auto& immutable_min_heap = fihost_->immutable_min_heap_;
  auto& imm_iters = fihost_->imm_iters_;
  {
    MinIterHeap tmp(MinIterComparator(&fihost_->cfd_->internal_comparator()));
    immutable_min_heap.swap(tmp);
  }
  // Search immutable memtables, these always sync
  for (size_t i = 0; i < imm_iters.size(); i++) {
    auto* m = imm_iters[i];
    seek_to_first_ ? m->SeekToFirst() : m->Seek(internal_key_);
    if (!m->status().ok()) {
      fihost_->immutable_status_ = m->status();
    } else if (m->Valid()) {
      immutable_min_heap.push(m);
    }
  }
}

inline
Status FIHost::CtxBase::SeekLevel0() {

  Status s;

  Callback on_l0_seek;
  if (on_internal_complete_) {
    async::CallableFactory <CtxBase, Status, const Status&> f(this);
    on_l0_seek = f.GetCallable<&CtxBase::OnSeekLevel0>();
  }

  const VersionStorageInfo* vstorage = fihost_->sv_->current->storage_info();
  const std::vector<FileMetaData*>& l0 = vstorage->LevelFiles(0);
  auto& l0_iters = fihost_->l0_iters_;

  for (size_t i = current_l0_file_; i < l0.size(); ++i, ++current_l0_file_) {
    if (!l0_iters[i]) {
      continue;
    }

    if (seek_to_first_) {
      // Sync and async are separate
      if (on_l0_seek) {
        s = l0_iters[i]->RequestSeekToFirst(on_l0_seek);
      } else {
        l0_iters[i]->SeekToFirst();
      }
    } else {
      // If the target key passes over the largest key, we are sure Next()
      // won't go over this file.
      if (fihost_->user_comparator_->Compare(user_key_,
        l0[i]->largest.user_key()) > 0) {
        if (fihost_->read_options_.iterate_upper_bound != nullptr) {
          fihost_->has_iter_trimmed_for_upper_bound_ = true;
          fihost_->DeleteIterator(l0_iters[i]);
          l0_iters[i] = nullptr;
        }
        continue;
      }
      if (on_l0_seek) {
        s = l0_iters[i]->RequestSeek(on_l0_seek, internal_key_);
      } else {
        l0_iters[i]->Seek(internal_key_);
      }
    }

    if (s.IsIOPending()) {
      return s;
    }
    CompleteSeekLevel0();
  }

  return s;
}

inline
void FIHost::CtxBase::CompleteSeekLevel0() {

  auto& immutable_min_heap = fihost_->immutable_min_heap_;
  auto& l0_iters = fihost_->l0_iters_;

  if (!l0_iters[current_l0_file_]->status().ok()) {
    fihost_->immutable_status_ = l0_iters[current_l0_file_]->status();
  } else if (l0_iters[current_l0_file_]->Valid()) {
    if (!fihost_->IsOverUpperBound(l0_iters[current_l0_file_]->key())) {
      immutable_min_heap.push(l0_iters[current_l0_file_]);
    } else {
      fihost_->has_iter_trimmed_for_upper_bound_ = true;
      fihost_->DeleteIterator(l0_iters[current_l0_file_]);
      l0_iters[current_l0_file_] = nullptr;
    }
  }
}

inline
Status FIHost::CtxBase::OnSeekLevel0(const Status& status) {
  async(status);
  Status s;

  CompleteSeekLevel0();

  const VersionStorageInfo* vstorage = fihost_->sv_->current->storage_info();
  const std::vector<FileMetaData*>& l0 = vstorage->LevelFiles(0);

  ++current_l0_file_;
  if (current_l0_file_ < l0.size()) {
    s = SeekLevel0();
  }

  if(!s.IsIOPending()) {
    current_level_ = 1;
    s = SeekLevels();
    if (!s.IsIOPending()) {
      CompleteSeekLevels();
      CompleteSeekInternal();
    }
  }
  return s;
}

inline
Status FIHost::CtxBase::SeekLevels() {
  Status s;

  NewIteratorCB on_set_file_index;
  if (on_internal_complete_) {
    async::CallableFactory<CtxBase, Status, const Status&, InternalIterator*, TableReader*> f(this);
    on_set_file_index = f.GetCallable<&CtxBase::OnSetFileIndex>();
  }

  const auto* vstorage = fihost_->sv_->current->storage_info();
  auto& level_iters = fihost_->level_iters_;

  for (int32_t level = current_level_; level < vstorage->num_levels();
    ++level, ++current_level_) {
    const auto& level_files =  vstorage->LevelFiles(level);
    if (level_files.empty()) {
      continue;
    }
    if (level_iters[level - 1] == nullptr) {
      continue;
    }
    uint32_t f_idx = 0;
    if (!seek_to_first_) {
      f_idx = fihost_->FindFileInRange(level_files, internal_key_, 0,
        static_cast<uint32_t>(level_files.size()));
    }

    // Seek
    if (f_idx < level_files.size()) {
      range_del_.Construct(fihost_->cfd_->internal_comparator());
      InternalIterator* iter = nullptr;
      s = level_iters[level - 1]->SetFileIndex(on_set_file_index, &iter, 
        range_del_.GetAgg(), f_idx);

      if (!s.IsIOPending()) {
        CompleteSetFileIndex(iter);
        s = SeekLevelIterator();
      }

      if (s.IsIOPending()) {
        return s;
      }
      CompleteLevelSeek();
    }
  }
  return s;
}

inline
void FIHost::CtxBase::CompleteSetFileIndex(InternalIterator* file_iter) {
  auto& level_iters = fihost_->level_iters_;
  level_iters[current_level_ - 1]->CompleteSetFileIndex(file_iter, range_del_.GetAgg());
  range_del_.Destruct();
}

inline
Status FIHost::CtxBase::SeekLevelIterator() {
  Status s;
  auto& level_iters = fihost_->level_iters_;

  // Now we want to seek the iterator
  if (on_internal_complete_) {
    async::CallableFactory<CtxBase, Status, const Status&> f(this);
    auto on_seek_level = f.GetCallable<&CtxBase::OnSeekLevel>();
    if (seek_to_first_) {
      s = level_iters[current_level_ - 1]->RequestSeekToFirst(on_seek_level);
    } else {
      s = level_iters[current_level_ - 1]->RequestSeek(on_seek_level, internal_key_);
    }
  } else {
    seek_to_first_ ? level_iters[current_level_ - 1]->SeekToFirst() :
      level_iters[current_level_ - 1]->Seek(internal_key_);
  }
  return s;
}

// Make sure this is invoked only async
inline
Status FIHost::CtxBase::OnSetFileIndex(const Status& status, InternalIterator* file_iter, TableReader*) {
  assert(status.async());
  async(status);
  Status s;
  CompleteSetFileIndex(file_iter);
  s = SeekLevelIterator();
  if (!s.IsIOPending()) {
    s = OnSeekLevel(s);
  }
  return s;
}

inline
void FIHost::CtxBase::CompleteLevelSeek() {

  auto& level_iters = fihost_->level_iters_;

  if (!level_iters[current_level_ - 1]->status().ok()) {
    fihost_->immutable_status_ = level_iters[current_level_ - 1]->status();
  } else if (level_iters[current_level_ - 1]->Valid()) {
    if (!fihost_->IsOverUpperBound(level_iters[current_level_ - 1]->key())) {
      fihost_->immutable_min_heap_.push(level_iters[current_level_ - 1]);
    } else {
      // Nothing in this level is interesting. Remove.
      fihost_->has_iter_trimmed_for_upper_bound_ = true;
      fihost_->DeleteIterator(level_iters[current_level_ - 1]);
      level_iters[current_level_ - 1] = nullptr;
    }
  }
}

inline
Status FIHost::CtxBase::OnSeekLevel(const Status& status) {
  async(status);
  Status s;
  CompleteLevelSeek();
  const auto* vstorage = fihost_->sv_->current->storage_info();
  ++current_level_;
  if (current_level_ < vstorage->num_levels()) {
    // SeekLevels() will call CompleteSeekLevels()
    // and CompleteSeekInternal() if completes sync
    s = SeekLevels();
  } 
  if(!s.IsIOPending()) {
    CompleteSeekLevels();
    CompleteSeekInternal();
  }
  return s;
}

inline
void FIHost::CtxBase::CompleteSeekLevels() {
  if (seek_to_first_) {
    fihost_->is_prev_set_ = false;
  } else {
    fihost_->prev_key_.SetInternalKey(internal_key_);
    fihost_->is_prev_set_ = true;
    fihost_->is_prev_inclusive_ = true;
  }
}

inline
void FIHost::CtxBase::CompleteSeekInternal() {
  fihost_->UpdateCurrent();
  TEST_SYNC_POINT_CALLBACK("ForwardIterator::SeekInternal:Return", fihost_);
  if (async()) {
    Status s(fihost_->status());
    s.async(true);
    on_internal_complete_.Invoke(s);
  }
}


// SeekToFirst/Seek context
inline
FIHost::CtxSeek::CtxSeek(FIHost* fihost, const Callback& cb,  const Slice& target, bool seek_to_first) :
  CtxBase(fihost, cb),
  target_(target),
  seek_to_first_(seek_to_first) {
  if (cb_) {
    // We go straight to OnComplete()
    async::CallableFactory<CtxSeek, Status, const Status&> f(this);
    on_seek_internal_ = f.GetCallable<&CtxSeek::CompleteSeek>();
  }
}

inline
Status FIHost::CtxSeek::SeekImpl() {
  Status s;

  Callback on_iterators_fixup;
  if (cb_) {
    async::CallableFactory<CtxSeek, Status, const Status&> f(this);
    on_iterators_fixup = f.GetCallable<&CtxSeek::OnSeekIteratorsFixup>();
  }

  if (fihost_->sv_ == nullptr) {
    s = RebuildIterators(true, on_iterators_fixup);
  } else if (fihost_->sv_->version_number != fihost_->cfd_->GetSuperVersionNumber()) {
    s = RenewIterators(on_iterators_fixup);
  } else if (fihost_->immutable_status_.IsIncomplete()) {
    s = ResetIncompleteIterators(on_iterators_fixup);
  }

  if (!s.IsIOPending()) {
    s = OnSeekIteratorsFixup(s);
  }
  return s;
}

inline
Status FIHost::CtxSeek::OnSeekIteratorsFixup(const Status& status) {
  async(status);
  Status s = SeekInternal(on_seek_internal_, target_, seek_to_first_);
  if (!s.IsIOPending()) {
    s = OnComplete(s);
  }
  return s;
}

inline
FIHost::CtxNext::CtxNext(FIHost* fihost, const Callback& cb) :
  CtxBase(fihost, cb),
  update_prev_key_(false) {
}

inline
Status FIHost::CtxNext::NextImpl() {
  Status s;
  Callback on_iterators_fixup;
  if (cb_) {
    async::CallableFactory<CtxNext, Status, const Status&> f(this);
    on_iterators_fixup = f.GetCallable<&CtxNext::OnNextIteratorsFixup>();
  }

  auto* sv = fihost_->sv_;

  if (sv == nullptr ||
    sv->version_number != fihost_->cfd_->GetSuperVersionNumber()) {
    old_key_ = fihost_->key().ToString();

    if (sv == nullptr) {
      s = RebuildIterators(true, on_iterators_fixup);
    } else {
      s = RenewIterators(on_iterators_fixup);
    }

    if (!s.IsIOPending()) {
      s = OnNextIteratorsFixup(s);
    }
  } else if (fihost_->current_ != fihost_->mutable_iter_) {
    // It is going to advance immutable iterator

    if (fihost_->is_prev_set_ && fihost_->prefix_extractor_) {
      // advance prev_key_ to current_ only if they share the same prefix
      update_prev_key_ =
        fihost_->prefix_extractor_->Transform(fihost_->prev_key_.GetUserKey())
        .compare(fihost_->prefix_extractor_->Transform(fihost_->current_->key())) == 0;
    } else {
      update_prev_key_ = true;
    }


    if (update_prev_key_) {
      fihost_->prev_key_.SetInternalKey(fihost_->current_->key());
      fihost_->is_prev_set_ = true;
      fihost_->is_prev_inclusive_ = false;
    }

    s = AdvanceCurrent(s);
  }
  return s;
}

inline
Status FIHost::CtxNext::OnNextIteratorsFixup(const Status& status) {
  async(status);

  Callback on_seek_internal;
  if (cb_) {
    async::CallableFactory<CtxNext, Status, const Status&> f(this);
    on_seek_internal = f.GetCallable<&CtxNext::AdvanceCurrent>();
  }

  Status s = SeekInternal(on_seek_internal, old_key_, false);
  if (!s.IsIOPending()) {
    if (CheckInValid()) {
      return OnComplete(s);
    }
    s = AdvanceCurrent(s);
  }
  return s;
}

inline
Status FIHost::CtxNext::AdvanceCurrent(const Status& status) {
  async(status);
  Status s;

  if (cb_) {
    async::CallableFactory<CtxNext, Status, const Status&> f(this);
    auto complete_next = f.GetCallable<&CtxNext::CompleteNext>();
    s = fihost_->current_->RequestNext(complete_next);
  } else {
    fihost_->current_->Next();
  }

  if (!s.IsIOPending()) {
    s = CompleteNext(s);
  }

  return s;
}

inline
Status FIHost::CtxNext::CompleteNext(const Status& status) {
  async(status);

  if (fihost_->current_ != fihost_->mutable_iter_) {
    if (!fihost_->current_->status().ok()) {
      fihost_->immutable_status_ = fihost_->current_->status();
    } else if ((fihost_->current_->Valid()) && 
      (!fihost_->IsOverUpperBound(fihost_->current_->key()))) {
      fihost_->immutable_min_heap_.push(fihost_->current_);
    } else {
      if ((fihost_->current_->Valid()) &&
        (fihost_->IsOverUpperBound(fihost_->current_->key()))) {
        // remove the current iterator
        fihost_->DeleteCurrentIter();
        fihost_->current_ = nullptr;
      }
      // This seek is in memory
      if (update_prev_key_) {
        fihost_->mutable_iter_->Seek(fihost_->prev_key_.GetInternalKey());
      }
    }
  }
  fihost_->UpdateCurrent();
  TEST_SYNC_POINT_CALLBACK("ForwardIterator::Next:Return", fihost_);
  return OnComplete(status);
}

inline
FIHost::CtxCreate::CtxCreate(FIHost* fihost, const CreateCallback& cb) :
  CtxBase(fihost, Callback()),
  create_cb_(cb) {
  // Prevent any sync execution when async is in force
  // For that we make sure that CtxBase cb_ is not empty
  // although for creation purposes we introduce
  // our own callback
  if (create_cb_) {
    async::CallableFactory<CtxCreate, Status, const Status&> f(this);
    cb_ = f.GetCallable<&CtxCreate::DummyCB>();
  }
}

inline
Status FIHost::CtxCreate::Rebuild(SuperVersion* current_sv) {
  Status s;
  // The context should not be created/invoked
  // if no current_sv is supplied because we need
  // this context only to rebuild iterators at the creation
  // time and that is only needed if supplied sv is present
  assert(current_sv != nullptr);

  Callback rebuild_complete;
  if (create_cb_) {
    async::CallableFactory<CtxCreate, Status, const Status&> f(this);
    rebuild_complete = f.GetCallable<&CtxCreate::RebildComplete>();
  }

  s = RebuildIterators(false, rebuild_complete);
  if (!s.IsIOPending()) {
    s = RebildComplete(s);
  }
  return s;
}

InternalIterator* ForwardIteratorAsync::Create(DBImpl* db, const ReadOptions& read_options,
  ColumnFamilyData* cfd, SuperVersion* current_sv) {

  ForwardIteratorAsync* result = new ForwardIteratorAsync(db, read_options, cfd, current_sv);
  if (current_sv != nullptr) {
    result->ConstructContext<CtxCreate>(result, CreateCallback());
    result->GetCtx<CtxCreate>()->Rebuild(current_sv);
    result->DestroyContext();
  }
  return result;
}

Status ForwardIteratorAsync::Create(const CreateCallback& cb, DBImpl* db, const ReadOptions& read_options,
  ColumnFamilyData* cfd, InternalIterator** iter, SuperVersion* current_sv) {

  Status s;
  assert(iter);
  *iter = nullptr;

  ForwardIteratorAsync* result = new ForwardIteratorAsync(db, read_options, cfd, current_sv);
  if (current_sv != nullptr) {
    result->ConstructContext<CtxCreate>(result, cb);
    s = result->GetCtx<CtxCreate>()->Rebuild(current_sv);
    if (!s.IsIOPending()) {
      result->DestroyContext();
    }
  }
  if (!s.IsIOPending()) {
    *iter = result;
  }
  return s;
}

ForwardIteratorAsync::ForwardIteratorAsync(DBImpl* db, const ReadOptions& read_options,
                                 ColumnFamilyData* cfd,
                                 SuperVersion* current_sv)
    : db_(db),
      read_options_(read_options),
      cfd_(cfd),
      prefix_extractor_(cfd->ioptions()->prefix_extractor),
      user_comparator_(cfd->user_comparator()),
      immutable_min_heap_(MinIterComparator(&cfd_->internal_comparator())),
      sv_(current_sv),
      mutable_iter_(nullptr),
      current_(nullptr),
      valid_(false),
      status_(Status::OK()),
      immutable_status_(Status::OK()),
      has_iter_trimmed_for_upper_bound_(false),
      current_over_upper_bound_(false),
      is_prev_set_(false),
      is_prev_inclusive_(false),
      pinned_iters_mgr_(nullptr) {
}

ForwardIteratorAsync::~ForwardIteratorAsync() {
  Cleanup(true);
}

namespace {
// Used in PinnedIteratorsManager to release pinned SuperVersion
static void ReleaseSuperVersionFunc(void* sv) {
  delete reinterpret_cast<SuperVersion*>(sv);
}
}  // namespace

void ForwardIteratorAsync::SVCleanup() {
  if (sv_ != nullptr && sv_->Unref()) {
    // Job id == 0 means that this is not our background process, but rather
    // user thread
    JobContext job_context(0);
    db_->mutex_.Lock();
    sv_->Cleanup();
    db_->FindObsoleteFiles(&job_context, false, true);
    if (read_options_.background_purge_on_iterator_cleanup) {
      db_->ScheduleBgLogWriterClose(&job_context);
    }
    db_->mutex_.Unlock();
    if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
      pinned_iters_mgr_->PinPtr(sv_, &ReleaseSuperVersionFunc);
    } else {
      delete sv_;
    }
    if (job_context.HaveSomethingToDelete()) {
      db_->PurgeObsoleteFiles(
          job_context, read_options_.background_purge_on_iterator_cleanup);
    }
    job_context.Clean();
  }
}

void ForwardIteratorAsync::Cleanup(bool release_sv) {
  if (mutable_iter_ != nullptr) {
    DeleteIterator(mutable_iter_, true /* is_arena */);
  }

  for (auto* m : imm_iters_) {
    DeleteIterator(m, true /* is_arena */);
  }
  imm_iters_.clear();

  for (auto* f : l0_iters_) {
    DeleteIterator(f);
  }
  l0_iters_.clear();

  for (auto* l : level_iters_) {
    DeleteIterator(l);
  }
  level_iters_.clear();

  if (release_sv) {
    SVCleanup();
  }
}

bool ForwardIteratorAsync::Valid() const {
  // See UpdateCurrent().
  return valid_ ? !current_over_upper_bound_ : false;
}

bool ForwardIteratorAsync::IsOverUpperBound(const Slice& internal_key) const {
  return !(read_options_.iterate_upper_bound == nullptr ||
    cfd_->internal_comparator().user_comparator()->Compare(
      ExtractUserKey(internal_key),
      *read_options_.iterate_upper_bound) < 0);
}

void ForwardIteratorAsync::SeekToFirst() {
  ConstructContext<CtxSeek>(this, Callback(), Slice(), true);
  GetCtx<CtxSeek>()->SeekImpl();
  DestroyContext();
}

Status ForwardIteratorAsync::RequestSeekToFirst(const Callback& cb) {
  ConstructContext<CtxSeek>(this, cb, Slice(), true);
  Status s = GetCtx<CtxSeek>()->SeekImpl();
  if (!s.IsIOPending()) {
    DestroyContext();
  }
  return s;
}

void ForwardIteratorAsync::Seek(const Slice& internal_key) {
  if (IsOverUpperBound(internal_key)) {
    valid_ = false;
  }
  ConstructContext<CtxSeek>(this, Callback(), internal_key, false);
  GetCtx<CtxSeek>()->SeekImpl();
  DestroyContext();
}

Status ForwardIteratorAsync::RequestSeek(const Callback& cb, const Slice& internal_key) {
  Status s;
  if (IsOverUpperBound(internal_key)) {
    valid_ = false;
  }
  ConstructContext<CtxSeek>(this, cb, internal_key, false);
  s = GetCtx<CtxSeek>()->SeekImpl();
  if (!s.IsIOPending()) {
    DestroyContext();
  }
  return s;
}

void  ForwardIteratorAsync::Next() {
  assert(valid_);
  ConstructContext<CtxNext>(this, Callback());
  GetCtx<CtxNext>()->NextImpl();
  DestroyContext();
}


Status ForwardIteratorAsync::RequestNext(const Callback& cb) {
  assert(valid_);
  ConstructContext<CtxNext>(this, cb);
  Status s = GetCtx<CtxNext>()->NextImpl();
  if (!s.IsIOPending()) {
    DestroyContext();
  }
  return s;
}

Slice ForwardIteratorAsync::key() const {
  assert(valid_);
  return current_->key();
}

Slice ForwardIteratorAsync::value() const {
  assert(valid_);
  return current_->value();
}

Status ForwardIteratorAsync::status() const {
  if (!status_.ok()) {
    return status_;
  } else if (!mutable_iter_->status().ok()) {
    return mutable_iter_->status();
  }

  return immutable_status_;
}

Status ForwardIteratorAsync::GetProperty(std::string prop_name, std::string* prop) {
  assert(prop != nullptr);
  if (prop_name == "rocksdb.iterator.super-version-number") {
    *prop = ToString(sv_->version_number);
    return Status::OK();
  }
  return Status::InvalidArgument();
}

void ForwardIteratorAsync::SetPinnedItersMgr(
    PinnedIteratorsManager* pinned_iters_mgr) {
  pinned_iters_mgr_ = pinned_iters_mgr;
  UpdateChildrenPinnedItersMgr();
}

void ForwardIteratorAsync::UpdateChildrenPinnedItersMgr() {
  // Set PinnedIteratorsManager for mutable memtable iterator.
  if (mutable_iter_) {
    mutable_iter_->SetPinnedItersMgr(pinned_iters_mgr_);
  }

  // Set PinnedIteratorsManager for immutable memtable iterators.
  for (InternalIterator* child_iter : imm_iters_) {
    if (child_iter) {
      child_iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
  }

  // Set PinnedIteratorsManager for L0 files iterators.
  for (InternalIterator* child_iter : l0_iters_) {
    if (child_iter) {
      child_iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
  }

  // Set PinnedIteratorsManager for L1+ levels iterators.
  for (LevelIteratorAsync* child_iter : level_iters_) {
    if (child_iter) {
      child_iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
  }
}

bool ForwardIteratorAsync::IsKeyPinned() const {
  return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
         current_->IsKeyPinned();
}

bool ForwardIteratorAsync::IsValuePinned() const {
  return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
         current_->IsValuePinned();
}


void ForwardIteratorAsync::BuildLevelIterators(const VersionStorageInfo* vstorage) {
  level_iters_.reserve(vstorage->num_levels() - 1);
  for (int32_t level = 1; level < vstorage->num_levels(); ++level) {
    const auto& level_files = vstorage->LevelFiles(level);
    if ((level_files.empty()) ||
        ((read_options_.iterate_upper_bound != nullptr) &&
         (user_comparator_->Compare(*read_options_.iterate_upper_bound,
                                    level_files[0]->smallest.user_key()) <
          0))) {
      level_iters_.push_back(nullptr);
      if (!level_files.empty()) {
        has_iter_trimmed_for_upper_bound_ = true;
      }
    } else {
      level_iters_.push_back(
          new LevelIteratorAsync(cfd_, read_options_, level_files));
    }
  }
}

void ForwardIteratorAsync::UpdateCurrent() {
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

  // Upper bound doesn't apply to the memtable iterator. We want Valid() to
  // return false when all iterators are over iterate_upper_bound, but can't
  // just set valid_ to false, as that would effectively disable the tailing
  // optimization (Seek() would be called on all immutable iterators regardless
  // of whether the target key is greater than prev_key_).
  current_over_upper_bound_ = valid_ && IsOverUpperBound(current_->key());
}

bool ForwardIteratorAsync::NeedToSeekImmutable(const Slice& target) {
  // We maintain the interval (prev_key_, immutable_min_heap_.top()->key())
  // such that there are no records with keys within that range in
  // immutable_min_heap_. Since immutable structures (SST files and immutable
  // memtables) can't change in this version, we don't need to do a seek if
  // 'target' belongs to that interval (immutable_min_heap_.top() is already
  // at the correct position).

  if (!valid_ || !current_ || !is_prev_set_ || !immutable_status_.ok()) {
    return true;
  }
  Slice prev_key = prev_key_.GetInternalKey();
  if (prefix_extractor_ && prefix_extractor_->Transform(target).compare(
    prefix_extractor_->Transform(prev_key)) != 0) {
    return true;
  }
  if (cfd_->internal_comparator().InternalKeyComparator::Compare(
        prev_key, target) >= (is_prev_inclusive_ ? 1 : 0)) {
    return true;
  }

  if (immutable_min_heap_.empty() && current_ == mutable_iter_) {
    // Nothing to seek on.
    return false;
  }
  if (cfd_->internal_comparator().InternalKeyComparator::Compare(
        target, current_ == mutable_iter_ ? immutable_min_heap_.top()->key()
                                          : current_->key()) > 0) {
    return true;
  }
  return false;
}

void ForwardIteratorAsync::DeleteCurrentIter() {
  const VersionStorageInfo* vstorage = sv_->current->storage_info();
  const std::vector<FileMetaData*>& l0 = vstorage->LevelFiles(0);
  for (size_t i = 0; i < l0.size(); ++i) {
    if (!l0_iters_[i]) {
      continue;
    }
    if (l0_iters_[i] == current_) {
      has_iter_trimmed_for_upper_bound_ = true;
      DeleteIterator(l0_iters_[i]);
      l0_iters_[i] = nullptr;
      return;
    }
  }

  for (int32_t level = 1; level < vstorage->num_levels(); ++level) {
    if (level_iters_[level - 1] == nullptr) {
      continue;
    }
    if (level_iters_[level - 1] == current_) {
      has_iter_trimmed_for_upper_bound_ = true;
      DeleteIterator(level_iters_[level - 1]);
      level_iters_[level - 1] = nullptr;
    }
  }
}

bool ForwardIteratorAsync::TEST_CheckDeletedIters(int* pdeleted_iters,
                                             int* pnum_iters) {
  bool retval = false;
  int deleted_iters = 0;
  int num_iters = 0;

  const VersionStorageInfo* vstorage = sv_->current->storage_info();
  const std::vector<FileMetaData*>& l0 = vstorage->LevelFiles(0);
  for (size_t i = 0; i < l0.size(); ++i) {
    if (!l0_iters_[i]) {
      retval = true;
      deleted_iters++;
    } else {
      num_iters++;
    }
  }

  for (int32_t level = 1; level < vstorage->num_levels(); ++level) {
    if ((level_iters_[level - 1] == nullptr) &&
        (!vstorage->LevelFiles(level).empty())) {
      retval = true;
      deleted_iters++;
    } else if (!vstorage->LevelFiles(level).empty()) {
      num_iters++;
    }
  }
  if ((!retval) && num_iters <= 1) {
    retval = true;
  }
  if (pdeleted_iters) {
    *pdeleted_iters = deleted_iters;
  }
  if (pnum_iters) {
    *pnum_iters = num_iters;
  }
  return retval;
}

uint32_t ForwardIteratorAsync::FindFileInRange(
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

void ForwardIteratorAsync::DeleteIterator(InternalIterator* iter, bool is_arena) {
  if (iter == nullptr) {
    return;
  }

  if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
    pinned_iters_mgr_->PinIterator(iter, is_arena);
  } else {
    if (is_arena) {
      iter->~InternalIterator();
    } else {
      delete iter;
    }
  }
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
