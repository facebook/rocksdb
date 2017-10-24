//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#ifndef ROCKSDB_LITE

#include <string>
#include <type_traits>
#include <vector>
#include <queue>

#include "async/async_status_capture.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "table/internal_iterator.h"
#include "util/arena.h"

namespace rocksdb {

class DBImpl;
class Env;
struct SuperVersion;
class ColumnFamilyData;
class LevelIterator;
class VersionStorageInfo;
struct FileMetaData;

class MinIterComparator {
 public:
  explicit MinIterComparator(const Comparator* comparator) :
    comparator_(comparator) {}

  bool operator()(InternalIterator* a, InternalIterator* b) {
    return comparator_->Compare(a->key(), b->key()) > 0;
  }
 private:
  const Comparator* comparator_;
};

typedef std::priority_queue<InternalIterator*, std::vector<InternalIterator*>,
                            MinIterComparator> MinIterHeap;

/**
 * ForwardIterator is a special type of iterator that only supports Seek()
 * and Next(). It is expected to perform better than TailingIterator by
 * removing the encapsulation and making all information accessible within
 * the iterator. At the current implementation, snapshot is taken at the
 * time Seek() is called. The Next() followed do not see new values after.
 */
class ForwardIterator : public InternalIterator {
 public:
  ForwardIterator(DBImpl* db, const ReadOptions& read_options,
                  ColumnFamilyData* cfd, SuperVersion* current_sv = nullptr);
  virtual ~ForwardIterator();

  void SeekForPrev(const Slice& target) override {
    status_ = Status::NotSupported("ForwardIterator::SeekForPrev()");
    valid_ = false;
  }
  void SeekToLast() override {
    status_ = Status::NotSupported("ForwardIterator::SeekToLast()");
    valid_ = false;
  }
  void Prev() override {
    status_ = Status::NotSupported("ForwardIterator::Prev");
    valid_ = false;
  }

  virtual bool Valid() const override;
  void SeekToFirst() override;
  virtual void Seek(const Slice& target) override;
  virtual void Next() override;
  virtual Slice key() const override;
  virtual Slice value() const override;
  virtual Status status() const override;
  virtual Status GetProperty(std::string prop_name, std::string* prop) override;
  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override;
  virtual bool IsKeyPinned() const override;
  virtual bool IsValuePinned() const override;

  bool TEST_CheckDeletedIters(int* deleted_iters, int* num_iters);

 private:
  void Cleanup(bool release_sv);
  void SVCleanup();
  void RebuildIterators(bool refresh_sv);
  void RenewIterators();
  void BuildLevelIterators(const VersionStorageInfo* vstorage);
  void ResetIncompleteIterators();
  void SeekInternal(const Slice& internal_key, bool seek_to_first);
  void UpdateCurrent();
  bool NeedToSeekImmutable(const Slice& internal_key);
  void DeleteCurrentIter();
  uint32_t FindFileInRange(
    const std::vector<FileMetaData*>& files, const Slice& internal_key,
    uint32_t left, uint32_t right);

  bool IsOverUpperBound(const Slice& internal_key) const;

  // Set PinnedIteratorsManager for all children Iterators, this function should
  // be called whenever we update children Iterators or pinned_iters_mgr_.
  void UpdateChildrenPinnedItersMgr();

  // A helper function that will release iter in the proper manner, or pass it
  // to pinned_iters_mgr_ to release it later if pinning is enabled.
  void DeleteIterator(InternalIterator* iter, bool is_arena = false);

  DBImpl* const db_;
  const ReadOptions read_options_;
  ColumnFamilyData* const cfd_;
  const SliceTransform* const prefix_extractor_;
  const Comparator* user_comparator_;
  MinIterHeap immutable_min_heap_;

  SuperVersion* sv_;
  InternalIterator* mutable_iter_;
  std::vector<InternalIterator*> imm_iters_;
  std::vector<InternalIterator*> l0_iters_;
  std::vector<LevelIterator*> level_iters_;
  InternalIterator* current_;
  bool valid_;

  // Internal iterator status; set only by one of the unsupported methods.
  Status status_;
  // Status of immutable iterators, maintained here to avoid iterating over
  // all of them in status().
  Status immutable_status_;
  // Indicates that at least one of the immutable iterators pointed to a key
  // larger than iterate_upper_bound and was therefore destroyed. Seek() may
  // need to rebuild such iterators.
  bool has_iter_trimmed_for_upper_bound_;
  // Is current key larger than iterate_upper_bound? If so, makes Valid()
  // return false.
  bool current_over_upper_bound_;

  // Left endpoint of the range of keys that immutable iterators currently
  // cover. When Seek() is called with a key that's within that range, immutable
  // iterators don't need to be moved; see NeedToSeekImmutable(). This key is
  // included in the range after a Seek(), but excluded when advancing the
  // iterator using Next().
  IterKey prev_key_;
  bool is_prev_set_;
  bool is_prev_inclusive_;

  PinnedIteratorsManager* pinned_iters_mgr_;
  Arena arena_;
};

class LevelIteratorAsync;
class RangeDelAggregator;

class ForwardIteratorAsync : public InternalIterator {
  ForwardIteratorAsync(DBImpl* db, const ReadOptions& read_options,
    ColumnFamilyData* cfd, SuperVersion* current_sv = nullptr);
public:
  ~ForwardIteratorAsync();

  // Allow for async creation since RebuildIterators are originally
  // invoked from a ctor

  // This is invoked on async creation
  using CreateCallback = async::Callable<void, const Status&, InternalIterator*>;

  static Status Create(const CreateCallback&, DBImpl* db, const ReadOptions& read_options,
    ColumnFamilyData* cfd, InternalIterator** iter, 
    SuperVersion* current_sv = nullptr);

  static InternalIterator* Create(DBImpl* db, const ReadOptions& read_options,
    ColumnFamilyData* cfd, SuperVersion* current_sv = nullptr);

  void SeekForPrev(const Slice& target) override {
    status_ = Status::NotSupported("ForwardIteratorAsync::SeekForPrev()");
    valid_ = false;
  }
  void SeekToLast() override {
    status_ = Status::NotSupported("ForwardIteratorAsync::SeekToLast()");
    valid_ = false;
  }
  void Prev() override {
    status_ = Status::NotSupported("ForwardIteratorAsync::Prev");
    valid_ = false;
  }

  bool Valid() const override;

  void SeekToFirst() override;

  Status RequestSeekToFirst(const Callback&) override;

  void Seek(const Slice& target) override;

  Status RequestSeek(const Callback&, const Slice& target) override;

  void Next() override;

  Status RequestNext(const Callback&) override;

  virtual Slice key() const override;
  virtual Slice value() const override;
  virtual Status status() const override;

  virtual Status GetProperty(std::string prop_name, std::string* prop) override;

  virtual void SetPinnedItersMgr(
    PinnedIteratorsManager* pinned_iters_mgr) override;
  virtual bool IsKeyPinned() const override;
  virtual bool IsValuePinned() const override;

  bool TEST_CheckDeletedIters(int* deleted_iters, int* num_iters);

  // Support construction/destruction of RangeDelAggregator
  // inline with the host class
  class RangeDelHost {
  private:
    bool constructed_;
    std::aligned_storage<sizeof(RangeDelAggregator)>::type range_del_agg_;
  public:
    RangeDelHost() :
      constructed_(false) {}
    ~RangeDelHost() {
      if (constructed_) {
        Destruct();
      }
    }
    void Construct(const InternalKeyComparator& ikc) {
      assert(!constructed_);
      new (&range_del_agg_) RangeDelAggregator(ikc, {} /* snapshots */);
      constructed_ = true;
    }
    void Destruct() {
      assert(constructed_);
      GetAgg()->~RangeDelAggregator();
      constructed_ = false;
    }
    RangeDelAggregator* GetAgg() {
      assert(constructed_);
      return reinterpret_cast<RangeDelAggregator*>(&range_del_agg_);
    }
  };

private:
  void Cleanup(bool release_sv);
  void SVCleanup();

  void UpdateCurrent();
  bool NeedToSeekImmutable(const Slice& internal_key);
  void DeleteCurrentIter();
  uint32_t FindFileInRange(
    const std::vector<FileMetaData*>& files, const Slice& internal_key,
    uint32_t left, uint32_t right);

  bool IsOverUpperBound(const Slice& internal_key) const;

  void BuildLevelIterators(const VersionStorageInfo* vstorage);

  // Set PinnedIteratorsManager for all children Iterators, this function should
  // be called whenever we update children Iterators or pinned_iters_mgr_.
  void UpdateChildrenPinnedItersMgr();

  // A helper function that will release iter in the proper manner, or pass it
  // to pinned_iters_mgr_ to release it later if pinning is enabled.
  void DeleteIterator(InternalIterator* iter, bool is_arena = false);

  void CleanupAndRefreshSuperversion(bool refresh_sv);

  SuperVersion* GetLatestSuperVersion();

  void ConstructMemIterators(SuperVersion* sv, RangeDelAggregator* range_del_agg);

  void DeleteMemIterators();

  // replace l0 with new iterators and delete ol level iterators
  void CleanupOldIterators(std::vector<InternalIterator*>& l0_iters_new);

  DBImpl* const db_;
  const ReadOptions read_options_;
  ColumnFamilyData* const cfd_;
  const SliceTransform* const prefix_extractor_;
  const Comparator* user_comparator_;
  MinIterHeap immutable_min_heap_;

  SuperVersion* sv_;
  InternalIterator* mutable_iter_;
  std::vector<InternalIterator*> imm_iters_;
  std::vector<InternalIterator*> l0_iters_;
  std::vector<LevelIteratorAsync*> level_iters_;
  InternalIterator* current_;
  bool valid_;

  // Internal iterator status; set only by one of the unsupported methods.
  Status status_;
  // Status of immutable iterators, maintained here to avoid iterating over
  // all of them in status().
  Status immutable_status_;
  // Indicates that at least one of the immutable iterators pointed to a key
  // larger than iterate_upper_bound and was therefore destroyed. Seek() may
  // need to rebuild such iterators.
  bool has_iter_trimmed_for_upper_bound_;
  // Is current key larger than iterate_upper_bound? If so, makes Valid()
  // return false.
  bool current_over_upper_bound_;

  // Left endpoint of the range of keys that immutable iterators currently
  // cover. When Seek() is called with a key that's within that range, immutable
  // iterators don't need to be moved; see NeedToSeekImmutable(). This key is
  // included in the range after a Seek(), but excluded when advancing the
  // iterator using Next().
  IterKey prev_key_;
  bool is_prev_set_;
  bool is_prev_inclusive_;

  PinnedIteratorsManager* pinned_iters_mgr_;
  Arena arena_;

  using FIHost = ForwardIteratorAsync;

  class CtxBase : protected async::AsyncStatusCapture {
  public:
    virtual ~CtxBase() {}
  protected:
    using NewIteratorCB = TableCache::NewIteratorCallback;

    FIHost*      fihost_;
    Callback     cb_;
    RangeDelHost range_del_;
    // This is used when we need a continuation
    // on Rebuild/Renew/Reset/SeekInternal
    // we store a callback here. Note this is not
    // the final callback for the async iterator API
    // which is represented by cb_
    Callback      on_internal_complete_;
    // Iterator rebuilding code
    NewIteratorCB on_new_iter_rebuild_cb_;
    size_t        current_l0_file_;
    // Iterator renewing
    NewIteratorCB on_new_iter_renew_cb_;
    SuperVersion* svnew_;
    std::vector<InternalIterator*> l0_iters_new_;
    size_t       current_level_file_;

    // ResetIncompleteIterators
    NewIteratorCB on_reset_incomplete_cb_;
    NewIteratorCB on_incomplete_reset_cb_;

    // Seek members
    Slice internal_key_;
    bool  seek_to_first_;
    Slice   user_key_;
    int32_t current_level_;

    CtxBase(const CtxBase&) = delete;
    CtxBase& operator=(const CtxBase&) = delete;

    CtxBase(FIHost* fihost, const Callback& cb);

    Status OnComplete(const Status& status);

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
    Status RebuildIterators(bool refresh_sv, const Callback& on_rebuild_complete);
    Status Rebuildlevel0();
    Status OnLeve0RebuildIterator(const Status& status, InternalIterator* iter, TableReader*);
    void CompleteRebuild();
    /////////////////////////////////////////////////////////////////////
    // RenewIterators entry point
    Status RenewIterators(const Callback& on_renew_iterators);
    Status RenewLevel0();
    Status OnLeve0RenewIterator(const Status& status, InternalIterator* iter, TableReader*);
    void CompleteRenew();
    /////////////////////////////////////////////////////////////////
    // Reset Incomplete iterators
    Status ResetIncompleteIterators(const Callback& on_reset);
    Status ResetIncompleteLevel0();
    Status OnLeve0ResetIncomplete(const Status& status, InternalIterator* iter, TableReader*);
    Status ResetIncompleteLevelIterators();
    Status OnIncompleteLevelReset(const Status& status, InternalIterator* file_iter, TableReader*);
    void CompleteReset();
    ////////////////////////////////////////////////////
    // SeekInternal
    Status SeekInternal(const Callback& on_seek_internal, const Slice& internal_key,
      bool seek_to_first);
    Status OnSeekRebuildContinue(const Status& status);
    Status SeekAllLevels();
    void SeekImmutable();
    Status SeekLevel0();
    void CompleteSeekLevel0();
    Status OnSeekLevel0(const Status& status);
    Status SeekLevels();
    void CompleteSetFileIndex(InternalIterator* file_iter);
    Status SeekLevelIterator();
    Status OnSetFileIndex(const Status& status, InternalIterator* file_iter, TableReader*);
    void CompleteLevelSeek();
    Status OnSeekLevel(const Status& status);
    void CompleteSeekLevels();
    void CompleteSeekInternal();
  };

  class CtxSeek : public CtxBase {
    Slice   target_;
    bool seek_to_first_;
    Callback on_seek_internal_;
  public:
    CtxSeek(FIHost* fihost, const Callback& cb, const Slice& target, bool seek_to_first);
    Status SeekImpl();
    Status OnSeekIteratorsFixup(const Status& status);
    Status CompleteSeek(const Status& status) {
      async(status);
      return OnComplete(status);
    }
  };

  class CtxNext : public CtxBase {
    std::string old_key_;
    bool update_prev_key_;
  public:
    CtxNext(FIHost* fihost, const Callback& cb);
    Status NextImpl();
    bool CheckInValid() {
      return (!fihost_->valid_ || fihost_->key().compare(old_key_) != 0);
    }
    Status OnNextIteratorsFixup(const Status& status);
    Status AdvanceCurrent(const Status& status);
    Status CompleteNext(const Status& status);
  };

  class CtxCreate : public CtxBase {
    CreateCallback        create_cb_;
  public:
    // fihost is the iterator being created
    // in this case we want run RebuildIterators on it
    CtxCreate(FIHost* fihost, const CreateCallback& cb);
    Status Rebuild(SuperVersion* current_sv);
    Status DummyCB(const Status& s) {
      // should never be invoked
      assert(false);
      return s;
    }
    Status RebildComplete(const Status& status) {
      async(status);
      CompleteCreate();
      return status;
    }
    // Replacement for CtxBase::OnComplete
    void CompleteCreate() {
      if (async()) {
        Status s(fihost_->status());
        s.async(true);
        create_cb_.Invoke(s, fihost_);
        fihost_->DestroyContext();
      }
    }
  };

  // Async context is created within the iterator instead of heap
  std::aligned_union<sizeof(CtxBase), CtxSeek, CtxNext, CtxCreate>::type async_context_;
  bool ctx_constructed_ = false;

  template<class T, class... Args>
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
};


}  // namespace rocksdb
#endif  // ROCKSDB_LITE
