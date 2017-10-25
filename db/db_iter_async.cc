//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"
#include <stdexcept>
#include <deque>
#include <string>
#include <limits>

#include "async/async_status_capture.h"
#include "db/dbformat.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "monitoring/perf_context_imp.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "table/internal_iterator.h"
#include "util/arena.h"
#include "util/autovector.h"
#include "util/filename.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/string_util.h"

namespace rocksdb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->RequestSeekToFirst(); iter->Valid(); iter->RequestNext()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

// Depth of internal callback stack
// pre-allocated entries inine with
// the instance of the class to prevent any reallocations
const size_t CtxInternalStackSize = 4U;

// DbIter invokes itself via  public API. Duh!!!
const size_t CtxRecursiveStackSize = 4U;

using Host = DBIterAsync;
// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIterAsync
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIterAsync: public Iterator {
 public:
  // The following is grossly complicated. TODO: clean it up
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction {
    kForward,
    kReverse
  };

  // LocalStatistics contain Statistics counters that will be aggregated per
  // each iterator instance and then will be sent to the global statistics when
  // the iterator is destroyed.
  //
  // The purpose of this approach is to avoid perf regression happening
  // when multiple threads bump the atomic counters from a DBIterAsyncAsync::Next().
  struct LocalStatistics {
    explicit LocalStatistics() { ResetCounters(); }

    void ResetCounters() {
      next_count_ = 0;
      next_found_count_ = 0;
      prev_count_ = 0;
      prev_found_count_ = 0;
      bytes_read_ = 0;
    }

    void BumpGlobalStatistics(Statistics* global_statistics) {
      RecordTick(global_statistics, NUMBER_DB_NEXT, next_count_);
      RecordTick(global_statistics, NUMBER_DB_NEXT_FOUND, next_found_count_);
      RecordTick(global_statistics, NUMBER_DB_PREV, prev_count_);
      RecordTick(global_statistics, NUMBER_DB_PREV_FOUND, prev_found_count_);
      RecordTick(global_statistics, ITER_BYTES_READ, bytes_read_);
      ResetCounters();
    }

    // Map to Tickers::NUMBER_DB_NEXT
    uint64_t next_count_;
    // Map to Tickers::NUMBER_DB_NEXT_FOUND
    uint64_t next_found_count_;
    // Map to Tickers::NUMBER_DB_PREV
    uint64_t prev_count_;
    // Map to Tickers::NUMBER_DB_PREV_FOUND
    uint64_t prev_found_count_;
    // Map to Tickers::ITER_BYTES_READ
    uint64_t bytes_read_;
  };

  DBIterAsync(Env* env, const ReadOptions& read_options,
              const ImmutableCFOptions& cf_options, const Comparator* cmp,
              InternalIterator* iter, SequenceNumber s, bool arena_mode,
              uint64_t max_sequential_skip_in_iterations, uint64_t version_number)
    : arena_mode_(arena_mode),
      env_(env),
      logger_(cf_options.info_log),
      user_comparator_(cmp),
      merge_operator_(cf_options.merge_operator),
      iter_(iter),
      sequence_(s),
      direction_(kForward),
      valid_(false),
      current_entry_is_merged_(false),
      statistics_(cf_options.statistics),
      version_number_(version_number),
      iterate_upper_bound_(read_options.iterate_upper_bound),
      prefix_same_as_start_(read_options.prefix_same_as_start),
      pin_thru_lifetime_(read_options.pin_data),
      total_order_seek_(read_options.total_order_seek),
      range_del_agg_(cf_options.internal_comparator, s,
                     true /* collapse_deletions */) {
    RecordTick(statistics_, NO_ITERATORS);
    prefix_extractor_ = cf_options.prefix_extractor;
    max_skip_ = max_sequential_skip_in_iterations;
    max_skippable_internal_keys_ = read_options.max_skippable_internal_keys;
    if (pin_thru_lifetime_) {
      pinned_iters_mgr_.StartPinning();
    }
    if (iter_) {
      iter_->SetPinnedItersMgr(&pinned_iters_mgr_);
    }
  }
  virtual ~DBIterAsync() {
    // Release pinned data if any
    if (pinned_iters_mgr_.PinningEnabled()) {
      pinned_iters_mgr_.ReleasePinnedData();
    }
    // Compiler warning issue filed:
    // https://github.com/facebook/rocksdb/issues/3013
    RecordTick(statistics_, NO_ITERATORS, uint64_t(-1));
    local_stats_.BumpGlobalStatistics(statistics_);
    if (!arena_mode_) {
      delete iter_;
    } else {
      iter_->~InternalIterator();
    }
  }

  // No copying allowed
  DBIterAsync(const DBIterAsync&) = delete;
  DBIterAsync& operator=(const DBIterAsync&) = delete;


  virtual void SetIter(InternalIterator* iter) {
    assert(iter_ == nullptr);
    iter_ = iter;
    iter_->SetPinnedItersMgr(&pinned_iters_mgr_);
  }
  virtual RangeDelAggregator* GetRangeDelAggregator() {
    return &range_del_agg_;
  }

  virtual bool Valid() const override { return valid_; }
  virtual Slice key() const override {
    assert(valid_);
    return saved_key_.GetUserKey();
  }
  virtual Slice value() const override {
    assert(valid_);
    if (current_entry_is_merged_) {
      // If pinned_value_ is set then the result of merge operator is one of
      // the merge operands and we should return it.
      return pinned_value_.data() ? pinned_value_ : saved_value_;
    } else if (direction_ == kReverse) {
      return pinned_value_;
    } else {
      return iter_->value();
    }
  }
  virtual Status status() const override {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  virtual Status GetProperty(std::string prop_name,
                             std::string* prop) override {
    if (prop == nullptr) {
      return Status::InvalidArgument("prop is nullptr");
    }
    if (prop_name == "rocksdb.iterator.super-version-number") {
      // First try to pass the value returned from inner iterator.
      if (!iter_->GetProperty(prop_name, prop).ok()) {
        *prop = ToString(version_number_);
      }
      return Status::OK();
    } else if (prop_name == "rocksdb.iterator.is-key-pinned") {
      if (valid_) {
        *prop = (pin_thru_lifetime_ && saved_key_.IsKeyPinned()) ? "1" : "0";
      } else {
        *prop = "Iterator is not valid.";
      }
      return Status::OK();
    }
    return Status::InvalidArgument("Undentified property.");
  }

  virtual void Next() override;
  Status RequestNext(const Callback&) override;

  virtual void Prev() override;
  Status RequestPrev(const Callback& cb) override;

  virtual void Seek(const Slice& target) override;
  Status RequestSeek(const Callback& cb, const Slice& target) override;

  virtual void SeekForPrev(const Slice& target) override;
  Status RequestSeekForPrev(const Callback& cb, const Slice& target) override;

  virtual void SeekToFirst() override;
  Status RequestSeekToFirst(const Callback& cb) override;

  virtual void SeekToLast() override;
  Status RequestSeekToLast(const Callback& cb) override;

 private:
  bool ParseKey(ParsedInternalKey* key);
  bool TooManyInternalKeysSkipped(bool increment = true);

  // Temporarily pin the blocks that we encounter until ReleaseTempPinnedData()
  // is called
  void TempPinData() {
    if (!pin_thru_lifetime_) {
      pinned_iters_mgr_.StartPinning();
    }
  }

  // Release blocks pinned by TempPinData()
  void ReleaseTempPinnedData() {
    if (!pin_thru_lifetime_ && pinned_iters_mgr_.PinningEnabled()) {
      pinned_iters_mgr_.ReleasePinnedData();
    }
  }

  void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  void ResetInternalKeysSkippedCounter() {
    num_internal_keys_skipped_ = 0;
  }

  void CheckAndUpdatePrefixStart() {
    if (valid_ && prefix_extractor_ && prefix_same_as_start_) {
      prefix_start_buf_.SetUserKey(
        prefix_extractor_->Transform(saved_key_.GetUserKey()));
      prefix_start_key_ = prefix_start_buf_.GetUserKey();
    }
  }

  const SliceTransform* prefix_extractor_;
  bool arena_mode_;
  Env* const env_;
  Logger* logger_;
  const Comparator* const user_comparator_;
  const MergeOperator* const merge_operator_;
  InternalIterator* iter_;
  SequenceNumber const sequence_;

  Status status_;
  IterKey saved_key_;
  std::string saved_value_;
  Slice pinned_value_;
  Direction direction_;
  bool valid_;
  bool current_entry_is_merged_;
  // for prefix seek mode to support prev()
  Statistics* statistics_;
  uint64_t max_skip_;
  uint64_t max_skippable_internal_keys_;
  uint64_t num_internal_keys_skipped_;
  uint64_t version_number_;
  const Slice* iterate_upper_bound_;
  IterKey prefix_start_buf_;
  Slice prefix_start_key_;
  const bool prefix_same_as_start_;
  // Means that we will pin all data blocks we read as long the Iterator
  // is not deleted, will be true if ReadOptions::pin_data is true
  const bool pin_thru_lifetime_;
  const bool total_order_seek_;
  // List of operands for merge operator.
  MergeContext merge_context_;
  RangeDelAggregator range_del_agg_;
  LocalStatistics local_stats_;
  PinnedIteratorsManager pinned_iters_mgr_;

  // Async implementation support
  using IntCallback = InternalIterator::Callback;

  class CtxBase : public async::AsyncStatusCapture {
  public:
    virtual ~CtxBase() {}
  protected:
    DBIterAsync* host_;
    Callback  cb_;
    // A stack of callbacks for internal ops
    // The class is complex enough so some common
    // code callbacks other parts of the class
    // after their invoke it.
    struct S {
      Callback cb_;
      bool     async_;
    };
    autovector<S, CtxInternalStackSize> internal_stack_;
    async::CallableFactory<CtxBase, Status, const Status&> iter_fac_;
    // FindParseableKey
    ParsedInternalKey  ikey_;  // re-used across apis
    Direction          direction_; // Only used for FindParseableKey
                                   // FindPrevUserKey
    size_t             num_skipped_;
    // FindNextUserEntry
    PERF_METER_DECL(find_next_user_entry_time);
    bool               skipping_;
    bool               prefix_check_;
    // FindValueForCurrentKey
    ValueType          last_not_merge_type_;
    ValueType          last_key_entry_type_;
    std::string        last_key_;
  protected:
    CtxBase(const CtxBase&) = delete;
    CtxBase& operator=(const CtxBase&) = delete;
    CtxBase(DBIterAsync* host, const Callback& cb) :
      host_(host), cb_(cb),
      iter_fac_(this),
      direction_(kForward),
      num_skipped_(0),
      PERF_METER_INIT(find_next_user_entry_time),
      skipping_(false),
      prefix_check_(false),
      last_not_merge_type_(kTypeValue),
      last_key_entry_type_(kTypeValue) {}

    bool AsyncSetup() const {
      return cb_;
    }
    void PushStack(const Callback& cb) {
      if (AsyncSetup()) {
        assert(internal_stack_.size() < CtxInternalStackSize);
        internal_stack_.push_back({ cb, false });
      }
    }
    void PopAndInvoke(const Status& s) {
      if (AsyncSetup()) {
        assert(!internal_stack_.empty());
        auto si = internal_stack_.back();
        internal_stack_.pop_back();
        if (si.async_) {
          Status sa(s);
          sa.async(true);
          si.cb_.Invoke(sa);
        }
      }
    }
    void MarkStackTopAsync(const Status& status) {
      async(status);
      if (status.async()) {
        assert(AsyncSetup());
        assert(!internal_stack_.empty());
        internal_stack_.back().async_ = true;
      }
    }
    // On Public API complete
    void OnComplete(const Status& s);
    void UpdateSeekStats();
    void UpdateStatsAndStartKeyClear();
    //////////////////////////////////////////////////////////////////////////
    /// FindParseableKey
    Status FindParseableKey(const Callback& internal_cb, Direction direction);
    Status IterateFindParseable(const Status& status);
    ////////////////////////////////////////////////////////
    // FindNextUserKey
    Status FindNextUserKey(const Callback& internal_cb);
    Status IterateFindNextUserKey(const Status& status);
    Status IterateFindNextUserKeyContinue(const Status& status);
    ////////////////////////////////////////////////////////////////////////////
    /// FindPrevUserKey
    Status FindPrevUserKey(const Callback& internal_cb);
    void UpdateSkippedCounts();
    Status SeekPrev(const Callback& continue_2);
    Status IterateFindPrevUserKey(const Status& status);
    Status FindPrevUserContinue1(const Status& status);
    Status FindPrevUserKeyContinue2(const Status& status);
    ////////////////////////////////////////////////////////////////////////
    /// MergeValuesNewToOld
    // Merge values of the same user key starting from the current iter_ position
    // Scan from the newer entries to older entries.
    // PRE: iter_->key() points to the first merge type entry
    //      saved_key_ stores the user key
    // POST: saved_value_ has the merged value for the user key
    //       iter_ points to the next entry (or invalid)
    Status MergeValuesNewToOld(const IntCallback& internal_cb);
    Status IterateMergeValues(const Status& status);
    Status CompleteMergeNoMerge(const Status& status) {
      MarkStackTopAsync(status);
      PopAndInvoke(status);
      return status;
    }
    Status CompleteMergeNewValues(const Status& status);
    ////////////////////////////////////////////////////////////////////////
    /// FindNextUserEntry
    Status FindNextUserEntry(const IntCallback& internal_cb, bool skipping,
      bool prefix_check);
    Status IteratorNextUserEntry(const Status& status);
    Status CompleteNextUserEntry(const Status& status) {
      MarkStackTopAsync(status);
      PERF_METER_STOP(find_next_user_entry_time);
      PopAndInvoke(status);
      return status;
    }
    ///////////////////////////////////////////////////////////////////
    // FindValueForCurrentKey
    // This function checks, if the entry with biggest sequence_number <= sequence_
    // is non kTypeDeletion or kTypeSingleDeletion. If it's not, we save value in
    // saved_value_
    // Original function returned a boolean. This returns Status. Value of IOPending
    // indicates async op in progress and the callback will be invoked.
    // Value of OK() indicates true, any other value indicates false.
    Status FindValueForCurrentKey(const IntCallback& internal_cb);
    Status IterateFindValueForCurrentKey(const Status& status);
    Status SwitchLastKeyEntryType();
    Status OnFindValueForKeyPrev(const Status& status);
    Status FindValueForCurrentKeyUsingSeek();
    Status OnSeekLastKey(const Status& status);
    Status OnFindValueSeekParseable(const Status& status);
    Status IterateFindValueUsingSeek(const Status& status);
    Status OFindValueSeekNext(const Status& status);
    Status CompleteFindValueForCurrentKey(const Status& status);
    /////////////////////////////////////////////////////
    // PrevInternal entry point
    Status PrevInternal(const IntCallback& internal_cb);
    void CheckPrevInternalIteratorValid();
    Status IteratePrevInternal(const Status& status);
    Status CompleteOnLoopReturn(const Status& status);
    Status OnFindValueFindParseable(const Status& status);
    Status OnFindValueForPrevInternalLoopReturn(const Status& status);
    Status LoopTailFindParseable(const Status& status);
  };
  class CtxNext : protected CtxBase {
    async::CallableFactory<CtxNext, Status, const Status&> next_fac_;
  public:
    CtxNext(Host* host, const Callback& cb) :
      CtxBase(host, cb),
      next_fac_(this) {}
    Status NextImpl();
  private:
    //Inline helper
    void UpdateStats() {
      if (host_->statistics_ != nullptr && host_->valid_) {
        host_->local_stats_.next_found_count_++;
        host_->local_stats_.bytes_read_ += (host_->key().size() +
          host_->value().size());
      }
    }
    Status NextSecondPart(const Status& status);
    Status UpdateStatsAndCompleteNext(const Status& status) {
      assert(status.async());
      async(status);
      UpdateStats();
      OnComplete(status);
      return status;
    }
    // ReverseToFoward implementation is only used in Next
    Status ReverseToForward(const IntCallback& internal_cb);
    Status ReverseToForwardAfterSeek(const Status& status);
    Status ReverseToForwardAfterFindNext(const Status& status);
    Status CompleteReverseToForward(const Status& status) {
      assert(status.async());
      MarkStackTopAsync(status);
      host_->range_del_agg_.InvalidateTombstoneMapPositions();
      PopAndInvoke(status);
      return status;
    }
  };
  class CtxPrev : protected CtxBase {
    async::CallableFactory<CtxPrev, Status, const Status&> prev_fac_;
    bool invalidate_after_seektolast_;
  public:
    CtxPrev(DBIterAsync* host, const Callback& cb) :
      CtxBase(host, cb),
      prev_fac_(this),
      invalidate_after_seektolast_(false) {}
    Status PrevImpl();
  private:
    void UpdateStats();
    Status ContinueAfterReverseToBackwards(const Status& status);
    // Complete on async call from PrevInternal above
    Status CompletePrev(const Status& status) {
      assert(status.async());
      async(status);
      UpdateStats();
      OnComplete(status);
      return status;
    }
    Status ReverseToBackward(const IntCallback& internal_cb);
    Status PrevAfterSeekForPrev(const Status& status);
    Status ContinueAfterMerged();
    Status OnCompleteReverseToBackward(const Status& status);
    Status AfterSeekToLast(const Status& status);
    Status IterateReverseToBackward(const Status& status);
    Status OnIterateReversePrev(const Status& status);
  };
  class CtxSeek : protected CtxBase {
    Slice target_;
    StopWatch sw_;
    PERF_TIMER_DECL(seek_internal_seek_time);
  public:
    CtxSeek(DBIterAsync* host, const Callback& cb, const Slice& target) :
      CtxBase(host, cb),
      target_(target),
      sw_(host_->env_, host_->statistics_, DB_SEEK, true /*do not start*/),
      PERF_TIMER_INIT(seek_internal_seek_time) {}
    Status SeekImpl();
  private:
    Status AfterSeekContinue(const Status& status);
    Status AfterFindNextUserEntry(const Status& status);
  };
  class CtxSeekForPrev : protected CtxBase {
    StopWatch sw_;
    Slice target_;
    PERF_METER_DECL(seek_internal_seek_time);
  public:
    CtxSeekForPrev(DBIterAsync* host, const Callback& cb, const Slice& target) :
      CtxBase(host, cb),
      sw_(host_->env_, host_->statistics_, DB_SEEK, true /* do not start*/),
      target_(target),
      PERF_METER_INIT(seek_internal_seek_time) {}
    Status SeekForPrevImpl();
  private:
    Status ContinueSeekForPrev(const Status& status);
    Status CompleteSeekForPrev(const Status& status);
  };
  class CtxSeekToFirst : protected CtxBase {
    PERF_METER_DECL(seek_internal_seek_time);
  public:
    CtxSeekToFirst(DBIterAsync* host, const Callback& cb) :
      CtxBase(host, cb),
      PERF_METER_INIT(seek_internal_seek_time) {}
    Status SeekToFirstImpl();
  private:
    Status ContinueAfterSeek(const Status& status);
    Status CompleteSeekToFirst(const Status& status);
  };
  class CtxSeekToLast : protected CtxBase {
    PERF_METER_DECL(seek_internal_seek_time);
  public:
    CtxSeekToLast(DBIterAsync* host, const Callback& cb) :
      CtxBase(host, cb),
      PERF_METER_INIT(seek_internal_seek_time) {}
    Status SeekToLastImpl();
  private:
    Status ContinueAfterSeekToLast(const Status& status);
    Status ContinueAferSeekForPrev(const Status& status);
    Status CompleteAfterPrevInternal(const Status& status);
  };

  // Async context is created within the iterator instead of heap
  // contexts are now allocated in place using placement new
  // since DBIter besides being grossly complex also invokes itself
  // public API we have to create an internal stack of async contexts
  using PlaceHolder =
    std::aligned_union<sizeof(CtxBase), CtxNext, CtxPrev, CtxSeek, CtxSeekForPrev,
      CtxSeekToFirst, CtxSeekToLast>::type;

  // As DBIter invokes itself we add contexts at the back of the stack
  // and destroy on return
  autovector<PlaceHolder, CtxRecursiveStackSize> ctx_stack_;

  template<class T, class... Args>
  void ConstructContext(Args... args) {
    if (ctx_stack_.size() >= CtxRecursiveStackSize) {
      // Need to increase the non-heap stack size
      assert(false);
      abort();
    }
    ctx_stack_.resize(ctx_stack_.size() + 1);
    new (&ctx_stack_.back()) T(std::forward<Args>(args)...);
  }
  template<class T>
  T* GetCtx() {
    assert(!ctx_stack_.empty());
    return reinterpret_cast<T*>(&ctx_stack_.back());
  }
  // Destruction takes place from OnComplete
  // or for syn completion by an explicit call
  // on exit from the public API
  // we rely on virtual destructor
  void DestroyContext() {
    // autovector pop_back() does not
    // and in this case can not
    // call __Dtor for the on-stack elements 
    GetCtx<CtxBase>()->~CtxBase();
    ctx_stack_.pop_back();
  }
};

inline bool DBIterAsync::ParseKey(ParsedInternalKey* ikey) {
  if (!ParseInternalKey(iter_->key(), ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIterAsync");
    ROCKS_LOG_ERROR(logger_, "corrupted internal key in DBIterAsync: %s",
                    iter_->key().ToString(true).c_str());
    return false;
  } else {
    return true;
  }
}

inline
void Host::CtxBase::OnComplete(const Status& s) {
  if (async()) {
    assert(AsyncSetup());
    auto cb = cb_;
    // Special case, this must be destroyed before
    // the cb is invoked as the next recursive context
    // must be on the top of the recursive stack
    host_->DestroyContext();
    Status sa(s);
    sa.async(true);
    cb.Invoke(s);
  }
}

// Inline Seek Helper
inline
void Host::CtxBase::UpdateSeekStats() {
  if (host_->statistics_ != nullptr) {
    if (host_->valid_) {
      RecordTick(host_->statistics_, NUMBER_DB_SEEK_FOUND);
      RecordTick(host_->statistics_, ITER_BYTES_READ,
                  host_->key().size() + host_->value().size());
    }
  }
}

inline
void Host::CtxBase::UpdateStatsAndStartKeyClear() {
  if (!host_->valid_) {
    host_->prefix_start_key_.clear();
  }
  UpdateSeekStats();
}

//////////////////////////////////////////////////////////////////////////
/// FindParseableKey
inline
Status Host::CtxBase::FindParseableKey(const Callback& internal_cb, Direction direction) {
  Status s;
  PushStack(internal_cb);
  direction_ = direction;
  return IterateFindParseable(s);
}

inline
Status Host::CtxBase::IterateFindParseable(const Status& status) {
  MarkStackTopAsync(status);
  Status s;
  if (AsyncSetup()) {
    auto iterate_findparseable_cb = 
      iter_fac_.GetCallable<&CtxBase::IterateFindParseable>();
    while (host_->iter_->Valid() && !host_->ParseKey(&ikey_)) {
      if (direction_ == kReverse) {
        s = host_->iter_->RequestPrev(iterate_findparseable_cb);
      } else {
        s = host_->iter_->RequestNext(iterate_findparseable_cb);
      }
      if (s.IsIOPending()) {
        break;
      }
    }
  } else {
    while (host_->iter_->Valid() && !host_->ParseKey(&ikey_)) {
      if (direction_ == kReverse) {
        host_->iter_->Prev();
      } else {
        host_->iter_->Next();
      }
    }
  }
  if (!s.IsIOPending()) {
    PopAndInvoke(s);
  }
  return s;
}
////////////////////////////////////////////////////////
// FindNextUserKey
inline
Status Host::CtxBase::FindNextUserKey(const Callback& internal_cb) {
  Status s;
  if (!host_->iter_->Valid()) {
    return s;
  }
  PushStack(internal_cb);
  auto iterate_findnextuserkey_cb =
    iter_fac_.GetCallable<&CtxBase::IterateFindNextUserKey>();
  s = FindParseableKey(iterate_findnextuserkey_cb, kForward);
  if (!s.IsIOPending()) {
    s = IterateFindNextUserKey(s);
  }
  return s;
}

inline
Status Host::CtxBase::IterateFindNextUserKey(const Status& status) {
  MarkStackTopAsync(status);
  Status s;

  auto iterate_findnext_uk_cont_cb =
    iter_fac_.GetCallable<&CtxBase::IterateFindNextUserKeyContinue>();
  auto iterate_findnextuserkey_cb =
    iter_fac_.GetCallable<&CtxBase::IterateFindNextUserKey>();

  while (host_->iter_->Valid() &&
          !host_->user_comparator_->Equal(ikey_.user_key,
                                          host_->saved_key_.GetUserKey())) {
    if (AsyncSetup()) {
      s = host_->iter_->RequestNext(iterate_findnext_uk_cont_cb);
    } else {
      host_->iter_->Next();
    }
    if (!s.IsIOPending()) {
      s = FindParseableKey(iterate_findnextuserkey_cb, kForward);
    }
    if (s.IsIOPending()) {
      return s;
    }
  }
  if (!s.IsIOPending()) {
    PopAndInvoke(s);
  }
  return s;
}

inline
Status Host::CtxBase::IterateFindNextUserKeyContinue(const Status& status) {
  MarkStackTopAsync(status);
  assert(async());
  auto iterate_findnextuserkey_cb =
    iter_fac_.GetCallable<&CtxBase::IterateFindNextUserKey>();
  Status s = FindParseableKey(iterate_findnextuserkey_cb, kForward);
  if (!s.IsIOPending()) {
    s = IterateFindNextUserKey(s);
  }
  return s;
}

////////////////////////////////////////////////////////////////////////////
/// FindPrevUserKey
inline
Status Host::CtxBase::FindPrevUserKey(const Callback& internal_cb) {
  Status s;
  if (!host_->iter_->Valid()) {
    return s;
  }
  PushStack(internal_cb);
  auto iterate_findprev_uk_cb = iter_fac_.GetCallable<&CtxBase::IterateFindPrevUserKey>();
  num_skipped_ = 0;
  s = FindParseableKey(iterate_findprev_uk_cb, kReverse);
  if (!s.IsIOPending()) {
    s = IterateFindPrevUserKey(s);
  }
  return s;
}

inline
void Host::CtxBase::UpdateSkippedCounts() {
  if (ikey_.sequence > host_->sequence_) {
    PERF_COUNTER_ADD(internal_recent_skipped_count, 1);
  } else {
    PERF_COUNTER_ADD(internal_key_skipped_count, 1);
  }
}

inline
Status Host::CtxBase::SeekPrev(const Callback& continue_2) {
  Status s;
  auto iterate_findprev_uk_cb = iter_fac_.GetCallable<&CtxBase::IterateFindPrevUserKey>();
  if (AsyncSetup()) {
    s = host_->iter_->RequestPrev(continue_2);
  } else {
    host_->iter_->Prev();
  }
  if (!s.IsIOPending()) {
    // This will callback in this function again if async
    s = FindParseableKey(iterate_findprev_uk_cb, kReverse);
  }
  return s;
}

inline
Status Host::CtxBase::IterateFindPrevUserKey(const Status& status) {
  MarkStackTopAsync(status);
  auto continue_1 = iter_fac_.GetCallable<&CtxBase::FindPrevUserContinue1>();
  auto continue_2 = iter_fac_.GetCallable<&CtxBase::FindPrevUserKeyContinue2>();

  Status s;
  int cmp = 0;
  while (host_->iter_->Valid() &&
          ((cmp = host_->user_comparator_->Compare(ikey_.user_key,
                  host_->saved_key_.GetUserKey())) == 0 ||
          (cmp > 0 && ikey_.sequence > host_->sequence_))) {

    if (host_->TooManyInternalKeysSkipped()) {
      break;
    }
    if (cmp == 0) {
      if (num_skipped_ >= host_->max_skip_) {
        num_skipped_ = 0;
        IterKey last_key;
        last_key.SetInternalKey(ParsedInternalKey(
                                  host_->saved_key_.GetUserKey(), kMaxSequenceNumber, kValueTypeForSeek));

        if (AsyncSetup()) {
          s = host_->iter_->RequestSeek(continue_1, last_key.GetInternalKey());
        } else {
          host_->iter_->Seek(last_key.GetInternalKey());
        }
        if (s.IsIOPending()) {
          return s;
        }
        RecordTick(host_->statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
      } else {
        ++num_skipped_;
      }
    }

    UpdateSkippedCounts();

    s = SeekPrev(continue_2);
    if (s.IsIOPending()) {
      break;
    }
  }
  if (!s.IsIOPending()) {
    PopAndInvoke(s);
  }
  return s;
}

inline
Status Host::CtxBase::FindPrevUserContinue1(const Status& status) {
  MarkStackTopAsync(status);
  Status s;

  RecordTick(host_->statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
  UpdateSkippedCounts();

  auto continue_2 = iter_fac_.GetCallable<&CtxBase::FindPrevUserKeyContinue2>();
  s = SeekPrev(continue_2);
  if (!s.IsIOPending()) {
    s = IterateFindPrevUserKey(s);
  }
  return s;
}

inline
Status Host::CtxBase::FindPrevUserKeyContinue2(const Status& status) {
  MarkStackTopAsync(status);
  auto iterate_findprev_uk_cb = iter_fac_.GetCallable<&CtxBase::IterateFindPrevUserKey>();
  Status s = FindParseableKey(iterate_findprev_uk_cb, kReverse);
  if (!s.IsIOPending()) {
    s = IterateFindPrevUserKey(s);
  }
  return s;
}
////////////////////////////////////////////////////////////////////////
/// MergeValuesNewToOld
// Merge values of the same user key starting from the current iter_ position
// Scan from the newer entries to older entries.
// PRE: iter_->key() points to the first merge type entry
//      saved_key_ stores the user key
// POST: saved_value_ has the merged value for the user key
//       iter_ points to the next entry (or invalid)
inline
Status Host::CtxBase::MergeValuesNewToOld(const IntCallback& internal_cb) {
  Status s;

  if (!host_->merge_operator_) {
    ROCKS_LOG_ERROR(host_->logger_, "Options::merge_operator is null.");
    host_->status_ = Status::InvalidArgument("merge_operator_ must be set.");
    host_->valid_ = false;
    return s;
  }

  PushStack(internal_cb);
  auto iterate_merge_cb = iter_fac_.GetCallable<&CtxBase::IterateMergeValues>();

  // Temporarily pin the blocks that hold merge operands
  host_->TempPinData();
  host_->merge_context_.Clear();
  // Start the merge process by pushing the first operand
  host_->merge_context_.PushOperand(host_->iter_->value(),
                                    host_->iter_->IsValuePinned() /* operand_pinned */);

  if (AsyncSetup()) {
    s = host_->iter_->RequestNext(iterate_merge_cb);
  } else {
    host_->iter_->Next();
  }

  if (!s.IsIOPending()) {
    s = IterateMergeValues(s);
  }
  return s;
}

inline
Status Host::CtxBase::IterateMergeValues(const Status& status) {
  Status s;
  MarkStackTopAsync(status);
  auto complete_merge_cb = iter_fac_.GetCallable<&CtxBase::CompleteMergeNewValues>();
  auto complete_nomerge_cb = iter_fac_.GetCallable<&CtxBase::CompleteMergeNoMerge>();
  auto iterate_merge_cb = iter_fac_.GetCallable<&CtxBase::IterateMergeValues>();

  while(host_->iter_->Valid()) {
    if (!host_->ParseKey(&ikey_)) {
      // skip corrupted key
      if (AsyncSetup()) {
        s = host_->iter_->RequestNext(iterate_merge_cb);
        if (s.IsIOPending()) {
          return s;
        }
      } else {
        host_->iter_->Next();
      }
      continue;
    }
    if (!host_->user_comparator_->Equal(ikey_.user_key,
                                        host_->saved_key_.GetUserKey())) {
      // hit the next user key, stop right here
      break;
    } else if (kTypeDeletion == ikey_.type || kTypeSingleDeletion == ikey_.type ||
                host_->range_del_agg_.ShouldDelete(
                  ikey_, RangeDelAggregator::RangePositioningMode::
                  kForwardTraversal)) {
      // hit a delete with the same user key, stop right here
      // iter_ is positioned after delete
      if (AsyncSetup()) {
        s = host_->iter_->RequestNext(complete_merge_cb);
        if (s.IsIOPending()) {
          return s;
        }
      } else {
        host_->iter_->Next();
      }
      break;
    } else if (kTypeValue == ikey_.type) {
      // hit a put, merge the put value with operands and store the
      // final result in saved_value_. We are done!
      // ignore corruption if there is any.
      const Slice val = host_->iter_->value();
      s = MergeHelper::TimedFullMerge(
            host_->merge_operator_, ikey_.user_key, &val,
            host_->merge_context_.GetOperands(),
            &host_->saved_value_, host_->logger_, host_->statistics_, host_->env_,
            &host_->pinned_value_, true);
      if (!s.ok()) {
        host_->status_ = s;
      }
      // iter_ is positioned after put
      if (AsyncSetup()) {
        s = host_->iter_->RequestNext(complete_nomerge_cb);
        if (s.IsIOPending()) {
          return s;
        }
      } else {
        host_->iter_->Next();
      }
      PopAndInvoke(s);
      return s;
    } else if (kTypeMerge == ikey_.type) {
      // hit a merge, add the value as an operand and run associative merge.
      // when complete, add result to operands and continue.
      host_->merge_context_.PushOperand(host_->iter_->value(),
                                        host_->iter_->IsValuePinned() /* operand_pinned */);
      PERF_COUNTER_ADD(internal_merge_count, 1);
    } else {
      assert(false);
    }
    // This will re-invoke this function
    if (AsyncSetup()) {
      s = host_->iter_->RequestNext(iterate_merge_cb);
    } else {
      host_->iter_->Next();
    }
    if (s.IsIOPending()) {
      return s;
    }
  }
  assert(!s.IsIOPending());
  return CompleteMergeNewValues(s);
}

inline
Status Host::CtxBase::CompleteMergeNewValues(const Status& status) {
  Status s;
  MarkStackTopAsync(status);

  // we either exhausted all internal keys under this user key, or hit
  // a deletion marker.
  // feed null as the existing value to the merge operator, such that
  // client can differentiate this scenario and do things accordingly.
  // The merge itself was done for kTypeValue type
  s = MergeHelper::TimedFullMerge(host_->merge_operator_,
                                  host_->saved_key_.GetUserKey(),
                                  nullptr, host_->merge_context_.GetOperands(),
                                  &host_->saved_value_, host_->logger_, host_->statistics_, host_->env_,
                                  &host_->pinned_value_, true);

  if (!s.ok()) {
    host_->status_ = s;
  }
  PopAndInvoke(s);
  return s;
}

////////////////////////////////////////////////////////////////////////
/// FindNextUserEntry
inline
Status Host::CtxBase::FindNextUserEntry(const IntCallback& internal_cb, bool skipping,
                          bool prefix_check) {
  PERF_METER_START(find_next_user_entry_time);
  // Loop until we hit an acceptable entry to yield
  assert(host_->iter_->Valid());
  assert(host_->direction_ == kForward);
  host_->current_entry_is_merged_ = false;
  PushStack(internal_cb);

  skipping_ = skipping;
  prefix_check_ = prefix_check;
  // How many times in a row we have skipped an entry with user key less than
  // or equal to saved_key_. We could skip these entries either because
  // sequence numbers were too high or because skipping = true.
  // What saved_key_ contains throughout this method:
  //  - if skipping        : saved_key_ contains the key that we need to skip,
  //                         and we haven't seen any keys greater than that,
  //  - if num_skipped > 0 : saved_key_ contains the key that we have skipped
  //                         num_skipped times, and we haven't seen any keys
  //                         greater than that,
  //  - none of the above  : saved_key_ can contain anything, it doesn't matter.
  num_skipped_ = 0;

  return IteratorNextUserEntry(Status());
}

Status Host::CtxBase::IteratorNextUserEntry(const Status& status) {
  MarkStackTopAsync(status);
  Status s;

  auto iterate_next_user_entry_cb = 
    iter_fac_.GetCallable<&CtxBase::IteratorNextUserEntry>();
  while (host_->iter_->Valid()) {
    if (!host_->ParseKey(&ikey_)) {
      // Skip corrupted keys.
      if (AsyncSetup()) {
        // will call into this function
        s = host_->iter_->RequestNext(iterate_next_user_entry_cb);
      } else {
        host_->iter_->Next();
      }
      if (s.IsIOPending()) {
        return s;
      }
      continue;
    }

    if (host_->iterate_upper_bound_ != nullptr &&
        host_->user_comparator_->Compare(ikey_.user_key,
                                          *host_->iterate_upper_bound_) >= 0) {
      break;
    }

    if (host_->prefix_extractor_ && prefix_check_ &&
        host_->prefix_extractor_->Transform(ikey_.user_key)
        .compare(host_->prefix_start_key_) != 0) {
      break;
    }

    if (host_->TooManyInternalKeysSkipped()) {
      break;
    }

    if (ikey_.sequence <= host_->sequence_) {
      if (skipping_ &&
          host_->user_comparator_->Compare(ikey_.user_key,
                                            host_->saved_key_.GetUserKey()) <= 0) {
        num_skipped_++;  // skip this entry
        PERF_COUNTER_ADD(internal_key_skipped_count, 1);
      } else {
        num_skipped_ = 0;
        switch (ikey_.type) {
        case kTypeDeletion:
        case kTypeSingleDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          host_->saved_key_.SetUserKey(
            ikey_.user_key,
            !host_->iter_->IsKeyPinned() || !host_->pin_thru_lifetime_ /* copy */);
          skipping_ = true;
          PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
          break;
        case kTypeValue:
          host_->saved_key_.SetUserKey(
            ikey_.user_key,
            !host_->iter_->IsKeyPinned() || !host_->pin_thru_lifetime_ /* copy */);
          if (host_->range_del_agg_.ShouldDelete(
                ikey_, RangeDelAggregator::RangePositioningMode::
                kForwardTraversal)) {
            // Arrange to skip all upcoming entries for this key since
            // they are hidden by this deletion.
            skipping_ = true;
            num_skipped_ = 0;
            PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
          } else {
            host_->valid_ = true;
            return CompleteNextUserEntry(s);
          }
          break;
        case kTypeMerge:
          host_->saved_key_.SetUserKey(
            ikey_.user_key,
            !host_->iter_->IsKeyPinned() || !host_->pin_thru_lifetime_ /* copy */);
          if (host_->range_del_agg_.ShouldDelete(
                ikey_, RangeDelAggregator::RangePositioningMode::
                kForwardTraversal)) {
            // Arrange to skip all upcoming entries for this key since
            // they are hidden by this deletion.
            skipping_ = true;
            num_skipped_ = 0;
            PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
          } else {
            // By now, we are sure the current ikey is going to yield a
            // value
            host_->current_entry_is_merged_ = true;
            host_->valid_ = true;
            auto complete_next_entry_cb = iter_fac_.GetCallable<&CtxBase::CompleteNextUserEntry>();
            s = MergeValuesNewToOld(
                  complete_next_entry_cb);  // Go to a different state machine
            if (!s.IsIOPending()) {
              s = CompleteNextUserEntry(s);
            }
            return s;
          }
          break;
        default:
          assert(false);
          break;
        }
      }
    } else {
      // This key was inserted after our snapshot was taken.
      PERF_COUNTER_ADD(internal_recent_skipped_count, 1);

      // Here saved_key_ may contain some old key, or the default empty key, or
      // key assigned by some random other method. We don't care.
      if (host_->user_comparator_->Compare(ikey_.user_key,
                                            host_->saved_key_.GetUserKey()) <=
          0) {
        num_skipped_++;
      } else {
        host_->saved_key_.SetUserKey(
          ikey_.user_key,
          !host_->iter_->IsKeyPinned() || !host_->pin_thru_lifetime_ /* copy */);
        skipping_ = false;
        num_skipped_ = 0;
      }
    }

    // If we have sequentially iterated via numerous equal keys, then it's
    // better to seek so that we can avoid too many key comparisons.
    if (num_skipped_ > host_->max_skip_) {
      num_skipped_ = 0;
      std::string last_key;
      if (skipping_) {
        // We're looking for the next user-key but all we see are the same
        // user-key with decreasing sequence numbers. Fast forward to
        // sequence number 0 and type deletion (the smallest type).
        AppendInternalKey(&last_key, ParsedInternalKey(host_->saved_key_.GetUserKey(),
                          0, kTypeDeletion));
        // Don't set skipping = false because we may still see more user-keys
        // equal to saved_key_.
      } else {
        // We saw multiple entries with this user key and sequence numbers
        // higher than sequence_. Fast forward to sequence_.
        // Note that this only covers a case when a higher key was overwritten
        // many times since our snapshot was taken, not the case when a lot of
        // different keys were inserted after our snapshot was taken.
        AppendInternalKey(&last_key,
                          ParsedInternalKey(host_->saved_key_.GetUserKey(), host_->sequence_,
                                            kValueTypeForSeek));
      }

      RecordTick(host_->statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
      if (AsyncSetup()) {
        s = host_->iter_->RequestSeek(iterate_next_user_entry_cb, last_key);
      } else {
        host_->iter_->Seek(last_key);
      }
    } else {
      if (AsyncSetup()) {
        // will call into this function
        s = host_->iter_->RequestNext(iterate_next_user_entry_cb);
      } else {
        host_->iter_->Next();
      }
    }
    if (s.IsIOPending()) {
      return s;
    }
  }

  host_->valid_ = false;
  // We should return all IOPending cases and
  // complete cases above
  PERF_METER_STOP(find_next_user_entry_time);
  PopAndInvoke(s);
  return s;
}

///////////////////////////////////////////////////////////////////
// FindValueForCurrentKey
// This function checks, if the entry with biggest sequence_number <= sequence_
// is non kTypeDeletion or kTypeSingleDeletion. If it's not, we save value in
// saved_value_
// Original function returned a boolean. This returns Status. Value of IOPending
// indicates async op in progress and the callback will be invoked.
// Value of OK() indicates true, any other value indicates false.
inline
Status Host::CtxBase::FindValueForCurrentKey(const IntCallback& internal_cb) {
  Status s;
  PushStack(internal_cb);
  assert(host_->iter_->Valid());
  host_->merge_context_.Clear();
  host_->current_entry_is_merged_ = false;
  // last entry before merge (could be kTypeDeletion, kTypeSingleDeletion or
  // kTypeValue)
  last_not_merge_type_ = kTypeDeletion;
  last_key_entry_type_ = kTypeDeletion;

  // Temporarily pin blocks that hold (merge operands / the value)
  host_->ReleaseTempPinnedData();
  host_->TempPinData();

  num_skipped_ = 0;
  auto iterate_find_value_cb =
    iter_fac_.GetCallable<&CtxBase::IterateFindValueForCurrentKey>();
  s = FindParseableKey(iterate_find_value_cb, kReverse);
  if (!s.IsIOPending()) {
    s = IterateFindValueForCurrentKey(s);
  }
  return s;
}

inline
Status Host::CtxBase::IterateFindValueForCurrentKey(const Status& status) {
  Status s;
  MarkStackTopAsync(status);

  auto on_prev_cb = iter_fac_.GetCallable<&CtxBase::OnFindValueForKeyPrev>();
  auto iterate_find_value_cb =
    iter_fac_.GetCallable<&CtxBase::IterateFindValueForCurrentKey>();

  while (host_->iter_->Valid() && ikey_.sequence <= host_->sequence_ &&
          host_->user_comparator_->Equal(ikey_.user_key,
                                        host_->saved_key_.GetUserKey())) {

    if (host_->TooManyInternalKeysSkipped()) {
      s = Status::NotFound();
      s = CompleteFindValueForCurrentKey(s);
      return s;
    }

    // We iterate too much: let's use Seek() to avoid too much key comparisons
    if (num_skipped_ >= host_->max_skip_) {
      return FindValueForCurrentKeyUsingSeek();
    }

    last_key_entry_type_ = ikey_.type;
    switch (last_key_entry_type_) {
    case kTypeValue:
      if (host_->range_del_agg_.ShouldDelete(
            ikey_,
            RangeDelAggregator::RangePositioningMode::kBackwardTraversal)) {
        last_key_entry_type_ = kTypeRangeDeletion;
        PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
      } else {
        assert(host_->iter_->IsValuePinned());
        host_->pinned_value_ = host_->iter_->value();
      }
      host_->merge_context_.Clear();
      last_not_merge_type_ = last_key_entry_type_;
      break;
    case kTypeDeletion:
    case kTypeSingleDeletion:
      host_->merge_context_.Clear();
      last_not_merge_type_ = last_key_entry_type_;
      PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
      break;
    case kTypeMerge:
      if (host_->range_del_agg_.ShouldDelete(
            ikey_,
            RangeDelAggregator::RangePositioningMode::kBackwardTraversal)) {
        host_->merge_context_.Clear();
        last_key_entry_type_ = kTypeRangeDeletion;
        last_not_merge_type_ = last_key_entry_type_;
        PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
      } else {
        assert(host_->merge_operator_ != nullptr);
        host_->merge_context_.PushOperandBack(
          host_->iter_->value(), host_->iter_->IsValuePinned() /* operand_pinned */);
        PERF_COUNTER_ADD(internal_merge_count, 1);
      }
      break;
    default:
      assert(false);
    }

    PERF_COUNTER_ADD(internal_key_skipped_count, 1);
    assert(host_->user_comparator_->Equal(ikey_.user_key,
                                          host_->saved_key_.GetUserKey()));
    ++num_skipped_;
    if(AsyncSetup()) {
      s = host_->iter_->RequestPrev(on_prev_cb);
    } else {
      host_->iter_->Prev();
    }
    if (!s.IsIOPending()) {
      s = FindParseableKey(iterate_find_value_cb, kReverse);
    }
    if (s.IsIOPending()) {
      return s;
    }
  }

  s = SwitchLastKeyEntryType();
  return CompleteFindValueForCurrentKey(s);
}

inline
Status Host::CtxBase::SwitchLastKeyEntryType() {
  Status s;
  switch (last_key_entry_type_) {
  case kTypeDeletion:
  case kTypeSingleDeletion:
  case kTypeRangeDeletion:
    host_->valid_ = false;
    s = Status::NotFound();
    break;
  case kTypeMerge:
    host_->current_entry_is_merged_ = true;
    if (last_not_merge_type_ == kTypeDeletion ||
        last_not_merge_type_ == kTypeSingleDeletion ||
        last_not_merge_type_ == kTypeRangeDeletion) {
      s = MergeHelper::TimedFullMerge(
            host_->merge_operator_, host_->saved_key_.GetUserKey(), nullptr,
            host_->merge_context_.GetOperands(), &host_->saved_value_, host_->logger_,
            host_->statistics_, host_->env_, &host_->pinned_value_, true);
    } else {
      assert(last_not_merge_type_ == kTypeValue);
      s = MergeHelper::TimedFullMerge(
            host_->merge_operator_, host_->saved_key_.GetUserKey(), &host_->pinned_value_,
            host_->merge_context_.GetOperands(), &host_->saved_value_, host_->logger_,
            host_->statistics_,
            host_->env_, &host_->pinned_value_, true);
    }
    break;
  case kTypeValue:
    // do nothing - we've already has value in saved_value_
    break;
  default:
    assert(false);
    break;
  }
  return s;
}

inline
Status Host::CtxBase::OnFindValueForKeyPrev(const Status& status) {
  MarkStackTopAsync(status);
  auto iterate_find_value_cb =
    iter_fac_.GetCallable<&CtxBase::IterateFindValueForCurrentKey>();
  Status s = FindParseableKey(iterate_find_value_cb, kReverse);
  if (!s.IsIOPending()) {
    s = IterateFindValueForCurrentKey(s);
  }
  return s;
}

  // This function is used in FindValueForCurrentKey.
  // We use Seek() function instead of Prev() to find necessary value
inline
Status Host::CtxBase::FindValueForCurrentKeyUsingSeek() {
  Status s;
  // FindValueForCurrentKey will enable pinning before calling
  // FindValueForCurrentKeyUsingSeek()
  assert(host_->pinned_iters_mgr_.PinningEnabled());
  last_key_.clear();
  AppendInternalKey(&last_key_, ParsedInternalKey(host_->saved_key_.GetUserKey(),
                    host_->sequence_, kValueTypeForSeek));

  RecordTick(host_->statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
  if (AsyncSetup()) {
    auto on_seek_last_key = iter_fac_.GetCallable<&CtxBase::OnSeekLastKey>();
    s = host_->iter_->RequestSeek(on_seek_last_key, last_key_);
  } else {
    host_->iter_->Seek(last_key_);
  }

  if (!s.IsIOPending()) {
    s = OnSeekLastKey(s);
  }
  return s;
}

inline
Status Host::CtxBase::OnSeekLastKey(const Status& status) {
  MarkStackTopAsync(status);
  IntCallback on_find_parseable;
  if (AsyncSetup()) {
    on_find_parseable = iter_fac_.GetCallable<&CtxBase::OnFindValueSeekParseable>();
  }
  Status s = FindParseableKey(on_find_parseable, kForward);
  if (!s.IsIOPending()) {
    s = OnFindValueSeekParseable(s);
  }
  return s;
}

inline
Status Host::CtxBase::OnFindValueSeekParseable(const Status& status) {
  MarkStackTopAsync(status);
  Status s;

  if (ikey_.type == kTypeDeletion || ikey_.type == kTypeSingleDeletion ||
      host_->range_del_agg_.ShouldDelete(
        ikey_, RangeDelAggregator::RangePositioningMode::kBackwardTraversal)) {
    host_->valid_ = false;
    s = Status::NotFound();
    s = CompleteFindValueForCurrentKey(s);
    return s;
  }
  if (ikey_.type == kTypeValue) {
    assert(host_->iter_->IsValuePinned());
    host_->pinned_value_ = host_->iter_->value();
    host_->valid_ = true;
    return CompleteFindValueForCurrentKey(Status::OK());
  }

  // kTypeMerge. We need to collect all kTypeMerge values and save them
  // in operands
  host_->current_entry_is_merged_ = true;
  host_->merge_context_.Clear();

  return IterateFindValueUsingSeek(s);
}

inline
Status Host::CtxBase::IterateFindValueUsingSeek(const Status& status) {
  MarkStackTopAsync(status);
  Status s;

  IntCallback complete_find_value =
    iter_fac_.GetCallable<&CtxBase::CompleteFindValueForCurrentKey>();
  IntCallback on_iterate_next = iter_fac_.GetCallable<&CtxBase::OFindValueSeekNext>();

  while (
    host_->iter_->Valid() &&
    host_->user_comparator_->Equal(ikey_.user_key, host_->saved_key_.GetUserKey())
    &&
    ikey_.type == kTypeMerge &&
    !host_->range_del_agg_.ShouldDelete(
      ikey_, RangeDelAggregator::RangePositioningMode::kBackwardTraversal)) {
    host_->merge_context_.PushOperand(host_->iter_->value(),
                                      host_->iter_->IsValuePinned() /* operand_pinned */);
    PERF_COUNTER_ADD(internal_merge_count, 1);
    if (AsyncSetup()) {
      s = host_->iter_->RequestNext(on_iterate_next);
    } else {
      host_->iter_->Next();
    }
    if (!s.IsIOPending()) {
      s = OFindValueSeekNext(s);
    }
    if (s.IsIOPending()) {
      return s;
    }
  }

  if (!host_->iter_->Valid() ||
      !host_->user_comparator_->Equal(ikey_.user_key,
                                      host_->saved_key_.GetUserKey()) ||
      ikey_.type == kTypeDeletion || ikey_.type == kTypeSingleDeletion ||
      host_->range_del_agg_.ShouldDelete(
        ikey_, RangeDelAggregator::RangePositioningMode::kBackwardTraversal)) {
    s = MergeHelper::TimedFullMerge(host_->merge_operator_,
                                    host_->saved_key_.GetUserKey(),
                                    nullptr, host_->merge_context_.GetOperands(),
                                    &host_->saved_value_, host_->logger_, host_->statistics_, host_->env_,
                                    &host_->pinned_value_, true);
    // Make iter_ valid and point to saved_key_
    if (!host_->iter_->Valid() ||
        !host_->user_comparator_->Equal(ikey_.user_key,
                                        host_->saved_key_.GetUserKey())) {
      RecordTick(host_->statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
      if (AsyncSetup()) {
        s = host_->iter_->RequestSeek(complete_find_value, last_key_);
      } else {
        host_->iter_->Seek(last_key_);
      }
    }
    if (!s.IsIOPending()) {
      return CompleteFindValueForCurrentKey(s);
    } else {
      return s;
    }
  }

  const Slice& val = host_->iter_->value();
  s = MergeHelper::TimedFullMerge(host_->merge_operator_,
                                  host_->saved_key_.GetUserKey(),
                                  &val, host_->merge_context_.GetOperands(),
                                  &host_->saved_value_, host_->logger_, host_->statistics_, host_->env_,
                                  &host_->pinned_value_, true);

  return CompleteFindValueForCurrentKey(s);
}

inline
Status Host::CtxBase::OFindValueSeekNext(const Status& status) {
  MarkStackTopAsync(status);
  auto iterate_findvalue_usingseek_cb =
    iter_fac_.GetCallable<&CtxBase::IterateFindValueUsingSeek>();
  Status s = FindParseableKey(iterate_findvalue_usingseek_cb, kForward);
  if (!s.IsIOPending()) {
    s = IterateFindValueUsingSeek(s);
  }
  return s;
}

inline
Status Host::CtxBase::CompleteFindValueForCurrentKey(const Status& status) {
  MarkStackTopAsync(status);
  Status s(status);
  if (!s.IsNotFound()) {
    host_->valid_ = true;
    if (!s.ok()) {
      host_->status_ = s;
    }
    s = Status::OK();
  }
  PopAndInvoke(s);
  return s;
}

/////////////////////////////////////////////////////
// PrevInternal entry point
inline
Status Host::CtxBase::PrevInternal(const IntCallback& internal_cb) {
  Status s;
  if (!host_->iter_->Valid()) {
    host_->valid_ = false;
    return s;
  }
  PushStack(internal_cb);
  return IteratePrevInternal(s);
}

// inline helper
inline
void Host::CtxBase::CheckPrevInternalIteratorValid() {
  if (host_->valid_ && host_->prefix_extractor_ && host_->prefix_same_as_start_
      &&
      host_->prefix_extractor_->Transform(host_->saved_key_.GetUserKey())
      .compare(host_->prefix_start_key_) != 0) {
    host_->valid_ = false;
  }
}

Status Host::CtxBase::IteratePrevInternal(const Status& status) {
  MarkStackTopAsync(status);
  auto iterate_prev_internal_cb = 
    iter_fac_.GetCallable<&CtxBase::IteratePrevInternal>();
  auto on_findvalue_prev_internal =
    iter_fac_.GetCallable<&CtxBase::OnFindValueForPrevInternalLoopReturn>();

  Status s;
  while (host_->iter_->Valid()) {
    host_->saved_key_.SetUserKey(
      ExtractUserKey(host_->iter_->key()),
      !host_->iter_->IsKeyPinned() || !host_->pin_thru_lifetime_ /* copy */);

    s = FindValueForCurrentKey(on_findvalue_prev_internal);
    if (s.IsIOPending()) {
      return s;
    }

    if (s.ok()) {
      // This is the case where we handle found value and do some
      // other things before returning from the loop
      return OnFindValueForPrevInternalLoopReturn(s);
    }

    // Did not find the value case s = NotFound
    if (host_->TooManyInternalKeysSkipped(false)) {
      PopAndInvoke(s);
      return s;
    }

    if (host_->iter_->Valid()) {
      auto loop_tail_find_parseable =
        iter_fac_.GetCallable<&CtxBase::LoopTailFindParseable>();
      s = FindParseableKey(loop_tail_find_parseable, kReverse);
      if (!s.IsIOPending()) {
        if (host_->user_comparator_->Equal(ikey_.user_key,
                                            host_->saved_key_.GetUserKey())) {
          s = FindPrevUserKey(iterate_prev_internal_cb);
        }
      }
      if (s.IsIOPending()) {
        return s;
      }
    }
  }
  // We haven't found any key - iterator is not valid
  // Or the prefix is different than start prefix
  assert(!host_->iter_->Valid());
  host_->valid_ = false;
  PopAndInvoke(s);
  return s;
}

inline
Status Host::CtxBase::CompleteOnLoopReturn(const Status& status) {
  MarkStackTopAsync(status);
  Status s;
  CheckPrevInternalIteratorValid();
  PopAndInvoke(s);
  return s;
}

inline
Status Host::CtxBase::OnFindValueFindParseable(const Status& status) {
  MarkStackTopAsync(status);
  Status s;

  if (host_->user_comparator_->Equal(ikey_.user_key,
                                      host_->saved_key_.GetUserKey())) {
    auto complete_onloop_return = iter_fac_.GetCallable<&CtxBase::CompleteOnLoopReturn>();
    s = FindPrevUserKey(complete_onloop_return);
  }

  if (!s.IsIOPending()) {
    s = CompleteOnLoopReturn(s);
  }
  return s;
}

// This returns from the loop or, in case the value not found
// will invoke the loop again but only on async
inline
Status Host::CtxBase::OnFindValueForPrevInternalLoopReturn(const Status& status) {
  MarkStackTopAsync(status);
  Status s(status);
  // Value found
  if (s.ok()) {
    host_->valid_ = true;
    if (!host_->iter_->Valid()) {
      PopAndInvoke(s);
      return s;
    }

    auto findvalue_findparseable_cb =
      iter_fac_.GetCallable<&CtxBase::OnFindValueFindParseable>();
    s = FindParseableKey(findvalue_findparseable_cb, kReverse);

    if (!s.IsIOPending()) {
      s = OnFindValueFindParseable(s);
    }
  } else {
    // NOT FOUND case, need loop continuation
    // This is only possible on async invocation of this CB
    assert(status.async());
    assert(s.IsNotFound());

    if (host_->TooManyInternalKeysSkipped(false)) {
      PopAndInvoke(s);
    } else {
      s = Status::OK();
      // Should continue into the loop
      if (host_->iter_->Valid()) {
        auto loop_tail_find_parseable =
          iter_fac_.GetCallable<&CtxBase::LoopTailFindParseable>();
        s = FindParseableKey(loop_tail_find_parseable, kReverse);
        if (!s.IsIOPending()) {
          s = LoopTailFindParseable(s);
        }
      } else {
        // Fall out of the loop and complete
        host_->valid_ = false;
        PopAndInvoke(s);
      }
    }
  }
  return s;
}

// Callback for FindParseable at the end of the loop
inline
Status Host::CtxBase::LoopTailFindParseable(const Status& status) {
  MarkStackTopAsync(status);
  Status s;

  auto iterate_prev_internal_cb = 
    iter_fac_.GetCallable<&CtxBase::IteratePrevInternal>();

  if (host_->user_comparator_->Equal(ikey_.user_key,
                                      host_->saved_key_.GetUserKey())) {
    s = FindPrevUserKey(iterate_prev_internal_cb);
  }

  // Return back to the loop
  if (!s.IsIOPending()) {
    s = IteratePrevInternal(s);
  }
  return s;
}

inline
Status Host::CtxNext::NextImpl() {
  Status s;
  auto next_second_part = next_fac_.GetCallable<&CtxNext::NextSecondPart>();
  if (host_->direction_ == kReverse) {
    s = ReverseToForward(next_second_part);
  } else if (host_->iter_->Valid() && !host_->current_entry_is_merged_) {
    // If the current value is not a merge, the iter position is the
    // current key, which is already returned. We can safely issue a
    // Next() without checking the current key.
    // If the current key is a merge, very likely iter already points
    // to the next internal position.
    PERF_COUNTER_ADD(internal_key_skipped_count, 1);
    if (AsyncSetup()) {
      s = host_->iter_->RequestNext(next_second_part);
    } else {
      host_->iter_->Next();
    }
  }

  if (!s.IsIOPending()) {
    s = NextSecondPart(s);
  }

  return s;
}

inline
Status Host::CtxNext::NextSecondPart(const Status& status) {
  async(status);
  Status s;

  if (host_->statistics_ != nullptr) {
    host_->local_stats_.next_count_++;
  }
  // Now we point to the next internal position, for both of merge and
  // not merge cases.
  if (!host_->iter_->Valid()) {
    host_->valid_ = false;
    OnComplete(status);
    return status;
  }

  auto update_complete = next_fac_.GetCallable<&CtxNext::UpdateStatsAndCompleteNext>();
  s = FindNextUserEntry(update_complete,
                        true /* skipping the current user key */, host_->prefix_same_as_start_);
  if (!s.IsIOPending()) {
    UpdateStats();
    OnComplete(s);
  }
  return s;
}

// ReverseToFoward implementation is only used in Next
inline
Status Host::CtxNext::ReverseToForward(const IntCallback& internal_cb) {
  PushStack(internal_cb);
  Status s;

  if (host_->prefix_extractor_ != nullptr && !host_->total_order_seek_) {
    IterKey last_key;
    last_key.SetInternalKey(ParsedInternalKey(
                              host_->saved_key_.GetUserKey(), kMaxSequenceNumber, kValueTypeForSeek));
    if (AsyncSetup()) {
      auto after_seek = next_fac_.GetCallable<&CtxNext::ReverseToForwardAfterSeek>();
      s = host_->iter_->RequestSeek(after_seek, last_key.GetInternalKey());
    } else {
      host_->iter_->Seek(last_key.GetInternalKey());
    }
  }
  if (!s.IsIOPending()) {
    s = ReverseToForwardAfterSeek(s);
  }
  return s;
}

inline
Status Host::CtxNext::ReverseToForwardAfterSeek(const Status& status) {
  MarkStackTopAsync(status);
  Status s;
  auto after_find_next =
    next_fac_.GetCallable<&CtxNext::ReverseToForwardAfterFindNext>();

  s = FindNextUserKey(after_find_next);
  if (!s.IsIOPending()) {
    s = ReverseToForwardAfterFindNext(s);
  }
  return s;
}

inline
Status Host::CtxNext::ReverseToForwardAfterFindNext(const Status& status) {
  MarkStackTopAsync(status);
  Status s;
  host_->direction_ = kForward;
  if (!host_->iter_->Valid()) {
    if (AsyncSetup()) {
      auto after_seek = next_fac_.GetCallable<&CtxNext::CompleteReverseToForward>();
      s = host_->iter_->RequestSeekToFirst(after_seek);
    } else {
      host_->iter_->SeekToFirst();
    }
    if (!s.IsIOPending()) {
      host_->range_del_agg_.InvalidateTombstoneMapPositions();
    }
  }

  if (!s.IsIOPending()) {
    PopAndInvoke(s);
  }
  return s;
}

Status DBIterAsync::RequestNext(const Callback& cb) {
  assert(valid_);
  // Release temporarily pinned blocks from last operation
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();

  ConstructContext<CtxNext>(this, cb);
  Status s = GetCtx<CtxNext>()->NextImpl();
  if (!s.IsIOPending()) {
    DestroyContext();
  }
  return s;
}

void DBIterAsync::Next() {
  assert(valid_);
  // Release temporarily pinned blocks from last operation
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();

  ConstructContext<CtxNext>(this, Callback());
  GetCtx<CtxNext>()->NextImpl();
  DestroyContext();
}

inline
Status Host::CtxPrev::PrevImpl() {
  Status s;
  if (host_->direction_ == kForward) {
    auto complete_after_reverse =
      prev_fac_.GetCallable<&CtxPrev::ContinueAfterReverseToBackwards>();
    s = ReverseToBackward(complete_after_reverse);
  }
  if(!s.IsIOPending()) {
    s = ContinueAfterReverseToBackwards(s);
  }
  return s;
}

// Inline helper
inline
void Host::CtxPrev::UpdateStats() {
  if (host_->statistics_ != nullptr) {
    host_->local_stats_.prev_count_++;
    if (host_->valid_) {
      host_->local_stats_.prev_found_count_++;
      host_->local_stats_.bytes_read_ += (host_->key().size()
                                          + host_->value().size());
    }
  }
}

inline
Status  Host::CtxPrev::ContinueAfterReverseToBackwards(const Status& status) {
  async(status);
  auto complete_prev = prev_fac_.GetCallable<&CtxPrev::CompletePrev>();
  Status s = PrevInternal(complete_prev);
  if (!s.IsIOPending()) {
    UpdateStats();
    OnComplete(s);
  }
  return s;
}

// Entry point for ReverseToBackwards
inline
Status Host::CtxPrev::ReverseToBackward(const IntCallback& internal_cb) {
  PushStack(internal_cb);
  Status s;
  if (host_->prefix_extractor_ != nullptr && !host_->total_order_seek_) {
    IterKey last_key;
    last_key.SetInternalKey(ParsedInternalKey(host_->saved_key_.GetUserKey(), 0,
                            kValueTypeForSeekForPrev));
    if (AsyncSetup()) {
      auto after_seek_forprev = prev_fac_.GetCallable<&CtxPrev::PrevAfterSeekForPrev>();
      s = host_->iter_->RequestSeekForPrev(after_seek_forprev, last_key.GetInternalKey());
    } else {
      host_->iter_->SeekForPrev(last_key.GetInternalKey());
    }
  }
  if (!s.IsIOPending()) {
    s = PrevAfterSeekForPrev(s);
  }
  return s;
}

inline
Status Host::CtxPrev::PrevAfterSeekForPrev(const Status& status) {
  MarkStackTopAsync(status);
  Status s;

  if (host_->current_entry_is_merged_) {
    // Not placed in the same key. Need to call Prev() until finding the
    // previous key.
    if (!host_->iter_->Valid()) {
      invalidate_after_seektolast_ = true;
      if (AsyncSetup()) {
        auto after_seek_tolast = prev_fac_.GetCallable<&CtxPrev::AfterSeekToLast>();
        s = host_->iter_->RequestSeekToLast(after_seek_tolast);
      } else {
        host_->iter_->SeekToLast();
      }
    }
    if (!s.IsIOPending()) {
      s = AfterSeekToLast(s);
    }
  } else {
    s = ContinueAfterMerged();
  }
  return s;
}

// Last part of ReverseToBackwards and
// completion
inline
Status Host::CtxPrev::ContinueAfterMerged() {
  Status s;
  if (host_->iter_->Valid()) {
    ParsedInternalKey ikey;
    assert(host_->ParseKey(&ikey));
    assert(host_->user_comparator_->Compare(ikey.user_key,
                                            host_->saved_key_.GetUserKey()) <=
            0);
  }
  auto on_complete_reverse =
    prev_fac_.GetCallable<&CtxPrev::OnCompleteReverseToBackward>();
  s = FindPrevUserKey(on_complete_reverse);
  if (!s.IsIOPending()) {
    host_->direction_ = kReverse;
    PopAndInvoke(s);
  }
  return s;
}

inline
Status Host::CtxPrev::OnCompleteReverseToBackward(const Status& status) {
  assert(status.async());  // Do not call sync
  MarkStackTopAsync(status);
  host_->direction_ = kReverse;
  PopAndInvoke(status);
  return status;
}

// Mid-point of ReverseToBackwards
inline
Status Host::CtxPrev::AfterSeekToLast(const Status& status) {
  MarkStackTopAsync(status);
  Status s;

  if (invalidate_after_seektolast_) {
    host_->range_del_agg_.InvalidateTombstoneMapPositions();
  }

  auto iterate_reverse = prev_fac_.GetCallable<&CtxPrev::IterateReverseToBackward>();
  s = FindParseableKey(iterate_reverse, kReverse);
  if (!s.IsIOPending()) {
    s = IterateReverseToBackward(s);
  }
  return s;
}


Status Host::CtxPrev::IterateReverseToBackward(const Status& status) {
  MarkStackTopAsync(status);
  Status s;

  auto iterate_reverse = prev_fac_.GetCallable<&CtxPrev::IterateReverseToBackward>();
  auto on_iterate_reverse_prev = prev_fac_.GetCallable<&CtxPrev::OnIterateReversePrev>();

  while (host_->iter_->Valid() &&
          host_->user_comparator_->Compare(ikey_.user_key,
                                          host_->saved_key_.GetUserKey()) >
          0) {
    if (ikey_.sequence > host_->sequence_) {
      PERF_COUNTER_ADD(internal_recent_skipped_count, 1);
    } else {
      PERF_COUNTER_ADD(internal_key_skipped_count, 1);
    }
    if (AsyncSetup()) {
      s = host_->iter_->RequestPrev(on_iterate_reverse_prev);
    } else {
      host_->iter_->Prev();
    }
    if (!s.IsIOPending()) {
      s = FindParseableKey(iterate_reverse, kReverse);
    }
    if (s.IsIOPending()) {
      return s;
    }
  }
  s = ContinueAfterMerged();
  return s;
}

inline
Status Host::CtxPrev::OnIterateReversePrev(const Status& status) {
  assert(status.async()); // Do not call sync
  MarkStackTopAsync(status);
  auto iterate_reverse = prev_fac_.GetCallable<&CtxPrev::IterateReverseToBackward>();
  Status s = FindParseableKey(iterate_reverse, kReverse);
  if (!s.IsIOPending()) {
    s = IterateReverseToBackward(s);
  }
  return s;
}

Status DBIterAsync::RequestPrev(const Callback& cb) {
  assert(valid_);
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();

  ConstructContext<CtxPrev>(this, cb);
  Status s = GetCtx<CtxPrev>()->PrevImpl();
  if (!s.IsIOPending()) {
    DestroyContext();
  }
  return s;
}

void DBIterAsync::Prev() {
  assert(valid_);
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();

  ConstructContext<CtxPrev>(this, Callback());
  GetCtx<CtxPrev>()->PrevImpl();
  DestroyContext();
}

bool DBIterAsync::TooManyInternalKeysSkipped(bool increment) {
  if ((max_skippable_internal_keys_ > 0) &&
      (num_internal_keys_skipped_ > max_skippable_internal_keys_)) {
    valid_ = false;
    status_ = Status::Incomplete("Too many internal keys skipped.");
    return true;
  } else if (increment) {
    num_internal_keys_skipped_++;
  }
  return false;
}

inline
Status Host::CtxSeek::SeekImpl() {
  Status s;
  sw_.start();

  host_->ReleaseTempPinnedData();
  host_->ResetInternalKeysSkippedCounter();
  host_->saved_key_.Clear();
  host_->saved_key_.SetInternalKey(target_, host_->sequence_);

  PERF_TIMER_START(seek_internal_seek_time);
  if (AsyncSetup()) {
    async::CallableFactory<CtxSeek, Status, const Status&> f(this);
    auto complete_seek = f.GetCallable<&CtxSeek::AfterSeekContinue>();
    s = host_->iter_->RequestSeek(complete_seek, host_->saved_key_.GetInternalKey());
  } else {
    host_->iter_->Seek(host_->saved_key_.GetInternalKey());
  }
  if (!s.IsIOPending()) {
    s = AfterSeekContinue(s);
  }
  return s;
}

inline
Status Host::CtxSeek::AfterSeekContinue(const Status& status) {
  PERF_TIMER_STOP(seek_internal_seek_time);
  async(status);
  Status s;
  RecordTick(host_->statistics_, NUMBER_DB_SEEK);

  if (host_->iter_->Valid()) {
    if (host_->prefix_extractor_ && host_->prefix_same_as_start_) {
      host_->prefix_start_key_ = host_->prefix_extractor_->Transform(target_);
    }
    host_->direction_ = kForward;
    host_->ClearSavedValue();

    async::CallableFactory<CtxSeek, Status, const Status&> f(this);
    auto after_findnext_entry = f.GetCallable<&CtxSeek::AfterFindNextUserEntry>();
    s = FindNextUserEntry(after_findnext_entry, false /* not skipping */,
                          host_->prefix_same_as_start_);
    if (s.IsIOPending()) {
      return s;
    }
    UpdateStatsAndStartKeyClear();
  } else {
    host_->valid_ = false;
  }

  host_->CheckAndUpdatePrefixStart();
  sw_.elapse_and_disarm();
  OnComplete(s);
  return s;
}

inline
Status Host::CtxSeek::AfterFindNextUserEntry(const Status& status) {
  async(status);
  UpdateStatsAndStartKeyClear();
  host_->CheckAndUpdatePrefixStart();
  sw_.elapse_and_disarm();
  OnComplete(status);
  return status;
}

Status DBIterAsync::RequestSeek(const Callback& cb, const Slice& target) {
  ConstructContext<CtxSeek>(this, cb, target);
  Status s = GetCtx<CtxSeek>()->SeekImpl();
  if (!s.IsIOPending()) {
    DestroyContext();
  }
  return s;
}

void DBIterAsync::Seek(const Slice& target) {
  ConstructContext<CtxSeek>(this, Callback(), target);
  GetCtx<CtxSeek>()->SeekImpl();
  DestroyContext();
}

inline
Status Host::CtxSeekForPrev::SeekForPrevImpl() {
  sw_.start();
  Status s;
  host_->ReleaseTempPinnedData();
  host_->ResetInternalKeysSkippedCounter();
  host_->saved_key_.Clear();
  // now saved_key is used to store internal key.
  host_->saved_key_.SetInternalKey(target_, 0 /* sequence_number */,
                                    kValueTypeForSeekForPrev);

  PERF_METER_START(seek_internal_seek_time);
  if (AsyncSetup()) {
    async::CallableFactory<CtxSeekForPrev, Status, const Status&> f(this);
    auto continue_seek = f.GetCallable<&CtxSeekForPrev::ContinueSeekForPrev>();
    s = host_->iter_->RequestSeekForPrev(continue_seek,
                                  host_->saved_key_.GetInternalKey());
  } else {
    host_->iter_->SeekForPrev(host_->saved_key_.GetInternalKey());
  }
  if (!s.IsIOPending()) {
    s = ContinueSeekForPrev(s);
  }
  return s;
}

inline
Status Host::CtxSeekForPrev::ContinueSeekForPrev(const Status& status) {
  PERF_METER_STOP(seek_internal_seek_time);

  Status s;
  async(status);
  host_->range_del_agg_.InvalidateTombstoneMapPositions();
  RecordTick(host_->statistics_, NUMBER_DB_SEEK);

  if (host_->iter_->Valid()) {
    if (host_->prefix_extractor_ && host_->prefix_same_as_start_) {
      host_->prefix_start_key_ = host_->prefix_extractor_->Transform(target_);
    }
    host_->direction_ = kReverse;
    host_->ClearSavedValue();
    async::CallableFactory<CtxSeekForPrev, Status, const Status&> f(this);
    auto complete_seek = f.GetCallable<&CtxSeekForPrev::CompleteSeekForPrev>();
    s = PrevInternal(complete_seek);
    if (s.IsIOPending()) {
      return s;
    }
    UpdateStatsAndStartKeyClear();
  } else {
    host_->valid_ = false;
  }

  host_->CheckAndUpdatePrefixStart();
  sw_.elapse_and_disarm();
  OnComplete(s);
  return s;
}

inline
Status Host::CtxSeekForPrev::CompleteSeekForPrev(const Status& status) {
  assert(status.async());
  async(status);
  UpdateStatsAndStartKeyClear();
  host_->CheckAndUpdatePrefixStart();
  sw_.elapse_and_disarm();
  OnComplete(status);
  return status;
}


Status DBIterAsync::RequestSeekForPrev(const Callback& cb, const Slice& target) {
  ConstructContext<CtxSeekForPrev>(this, cb, target);
  Status s = GetCtx<CtxSeekForPrev>()->SeekForPrevImpl();
  if (!s.IsIOPending()) {
    DestroyContext();
  }
  return s;
}

void DBIterAsync::SeekForPrev(const Slice& target) {
  ConstructContext<CtxSeekForPrev>(this, Callback(), target);
  GetCtx<CtxSeekForPrev>()->SeekForPrevImpl();
  DestroyContext();
}

inline
Status Host::CtxSeekToFirst::SeekToFirstImpl() {
  Status s;
  // Don't use iter_::Seek() if we set a prefix extractor
  // because prefix seek will be used.
  if (host_->prefix_extractor_ != nullptr) {
    host_->max_skip_ = std::numeric_limits<uint64_t>::max();
  }
  host_->direction_ = kForward;
  host_->ReleaseTempPinnedData();
  host_->ResetInternalKeysSkippedCounter();
  host_->ClearSavedValue();

  PERF_METER_START(seek_internal_seek_time);
  if (AsyncSetup()) {
    async::CallableFactory<CtxSeekToFirst, Status, const Status&> f(this);
    auto continue_after_seek = f.GetCallable<&CtxSeekToFirst::ContinueAfterSeek>();
    s = host_->iter_->RequestSeekToFirst(continue_after_seek);
  } else {
    host_->iter_->SeekToFirst();
  }
  if (!s.IsIOPending()) {
    s = ContinueAfterSeek(s);
  }
  return s;
}

inline
Status Host::CtxSeekToFirst::ContinueAfterSeek(const Status& status) {
  PERF_METER_STOP(seek_internal_seek_time);
  async(status);
  Status s;
  host_->range_del_agg_.InvalidateTombstoneMapPositions();
  RecordTick(host_->statistics_, NUMBER_DB_SEEK);

  if (host_->iter_->Valid()) {
    host_->saved_key_.SetUserKey(
      ExtractUserKey(host_->iter_->key()),
      !host_->iter_->IsKeyPinned() || !host_->pin_thru_lifetime_ /* copy */);

    async::CallableFactory<CtxSeekToFirst, Status, const Status&> f(this);
    auto complete_seek_to_first =
      f.GetCallable<&CtxSeekToFirst::CompleteSeekToFirst>();
    s = FindNextUserEntry(complete_seek_to_first, false /* not skipping */,
                          false /* no prefix check */);
    if (s.IsIOPending()) {
      return s;
    }
    UpdateSeekStats();
  } else {
    host_->valid_ = false;
  }
  host_->CheckAndUpdatePrefixStart();
  OnComplete(s);
  return s;
}

inline
Status Host::CtxSeekToFirst::CompleteSeekToFirst(const Status& status) {
  async(status);
  UpdateSeekStats();
  host_->CheckAndUpdatePrefixStart();
  OnComplete(status);
  return status;
}


Status DBIterAsync::RequestSeekToFirst(const Callback& cb) {
  ConstructContext<CtxSeekToFirst>(this, cb);
  Status s = GetCtx<CtxSeekToFirst>()->SeekToFirstImpl();
  if (!s.IsIOPending()) {
    DestroyContext();
  }
  return s;
}

void DBIterAsync::SeekToFirst() {
  ConstructContext<CtxSeekToFirst>(this, Callback());
  GetCtx<CtxSeekToFirst>()->SeekToFirstImpl();
  DestroyContext();
}

inline
Status Host::CtxSeekToLast::SeekToLastImpl() {
  Status s;
  // Don't use iter_::Seek() if we set a prefix extractor
  // because prefix seek will be used.
  if (host_->prefix_extractor_ != nullptr) {
    host_->max_skip_ = std::numeric_limits<uint64_t>::max();
  }
  host_->direction_ = kReverse;
  host_->ReleaseTempPinnedData();
  host_->ResetInternalKeysSkippedCounter();
  host_->ClearSavedValue();

  PERF_METER_START(seek_internal_seek_time);
  if (AsyncSetup()) {
    async::CallableFactory<CtxSeekToLast, Status, const Status&> f(this);
    auto continue_after_seek =
      f.GetCallable<&CtxSeekToLast::ContinueAfterSeekToLast>();
    s = host_->iter_->RequestSeekToLast(continue_after_seek);
  } else {
    host_->iter_->SeekToLast();
  }
  if (!s.IsIOPending()) {
    s = ContinueAfterSeekToLast(s);
  }
  return s;
}

inline
Status Host::CtxSeekToLast::ContinueAfterSeekToLast(const Status& status) {
  PERF_METER_STOP(seek_internal_seek_time);
  async(status);
  Status s;
  host_->range_del_agg_.InvalidateTombstoneMapPositions();

  // The following contains revusrsive calls into public interfaces
  // again
  async::CallableFactory<CtxSeekToLast, Status, const Status&> f(this);
  // When the iterate_upper_bound is set to a value,
  // it will seek to the last key before the
  // ReadOptions.iterate_upper_bound
  if (host_->iter_->Valid() && host_->iterate_upper_bound_ != nullptr) {
    if (AsyncSetup()) {
      auto complete_after_seek_for_repv =
        f.GetCallable<&CtxSeekToLast::ContinueAferSeekForPrev>();
      s = host_->RequestSeekForPrev(complete_after_seek_for_repv,
                              *host_->iterate_upper_bound_);
    } else {
      host_->SeekForPrev(*host_->iterate_upper_bound_);
    }
    if (s.IsIOPending()) {
      return s;
    }
    host_->range_del_agg_.InvalidateTombstoneMapPositions();
    if (!host_->Valid()) {
      OnComplete(s);
      return s;
    } else if (host_->user_comparator_->Equal(*host_->iterate_upper_bound_,
                host_->key())) {
      if (AsyncSetup()) {
        auto complete_after_prev_internal =
          f.GetCallable<&CtxSeekToLast::CompleteAfterPrevInternal>();
        s = host_->RequestPrev(complete_after_prev_internal);
      } else {
        host_->Prev();
      }
    }
  } else {
    auto compelte_after_prev_internal =
      f.GetCallable<&CtxSeekToLast::CompleteAfterPrevInternal>();
    s = PrevInternal(compelte_after_prev_internal);
  }

  if (s.IsIOPending()) {
    return s;
  }

  UpdateSeekStats();
  host_->CheckAndUpdatePrefixStart();
  OnComplete(s);
  return s;
}

inline
Status Host::CtxSeekToLast::ContinueAferSeekForPrev(const Status& status) {
  assert(status.async());
  async(status);
  host_->range_del_agg_.InvalidateTombstoneMapPositions();
  async::CallableFactory<CtxSeekToLast, Status, const Status&> f(this);
  if (!host_->Valid()) {
    OnComplete(status);
    return status;
  } else if ((host_->user_comparator_->Equal(*host_->iterate_upper_bound_,
              host_->key()))) {
    // Public interface calls as in the original code. Too bad.
    if (AsyncSetup()) {
      auto complete_after_prev_internal =
        f.GetCallable<&CtxSeekToLast::CompleteAfterPrevInternal>();
      Status s = host_->RequestPrev(complete_after_prev_internal);
      if (s.IsIOPending()) {
        return s;
      }
    } else {
      host_->Prev();
    }
  }
  UpdateSeekStats();
  host_->CheckAndUpdatePrefixStart();
  OnComplete(status);
  return status;
}

inline
Status Host::CtxSeekToLast::CompleteAfterPrevInternal(const Status& status) {
  assert(status.async());
  async(status);
  UpdateSeekStats();
  host_->CheckAndUpdatePrefixStart();
  OnComplete(status);
  return status;
}

Status DBIterAsync::RequestSeekToLast(const Callback& cb) {
  ConstructContext<CtxSeekToLast>(this, cb);
  Status s = GetCtx<CtxSeekToLast>()->SeekToLastImpl();
  if (!s.IsIOPending()) {
    DestroyContext();
  }
  return s;
}

void DBIterAsync::SeekToLast() {
  ConstructContext<CtxSeekToLast>(this, Callback());
  GetCtx<CtxSeekToLast>()->SeekToLastImpl();
  DestroyContext();
}

Iterator* NewDBIteratorAsync(Env* env, const ReadOptions& read_options,
                             const ImmutableCFOptions& cf_options,
                             const Comparator* user_key_comparator,
                             InternalIterator* internal_iter,
                             const SequenceNumber& sequence,
                             uint64_t max_sequential_skip_in_iterations,
                             uint64_t version_number) {
  DBIterAsync* db_iter = new DBIterAsync(
    env, read_options, cf_options, user_key_comparator, internal_iter,
    sequence, false, max_sequential_skip_in_iterations, version_number);
  return db_iter;
}

ArenaWrappedDBIterAsync::~ArenaWrappedDBIterAsync() {
  db_iter_->~DBIterAsync();
}

void ArenaWrappedDBIterAsync::SetDBIter(DBIterAsync* iter) {
  db_iter_ = iter;
}

RangeDelAggregator* ArenaWrappedDBIterAsync::GetRangeDelAggregator() {
  return db_iter_->GetRangeDelAggregator();
}

void ArenaWrappedDBIterAsync::SetIterUnderDBIter(InternalIterator* iter) {
  static_cast<DBIterAsync*>(db_iter_)->SetIter(iter);
}

bool ArenaWrappedDBIterAsync::Valid() const {
  return db_iter_->Valid();
}
void ArenaWrappedDBIterAsync::SeekToFirst() {
  db_iter_->SeekToFirst();
}
Status ArenaWrappedDBIterAsync::RequestSeekToFirst(const Callback& cb) {
  return db_iter_->RequestSeekToFirst(cb);
}
void ArenaWrappedDBIterAsync::SeekToLast() {
  db_iter_->SeekToLast();
}
Status ArenaWrappedDBIterAsync::RequestSeekToLast(const Callback& cb) {
  return db_iter_->RequestSeekToLast(cb);
}
void ArenaWrappedDBIterAsync::Seek(const Slice& target) {
  db_iter_->Seek(target);
}
Status ArenaWrappedDBIterAsync::RequestSeek(const Callback& cb, const Slice& target) {
  return db_iter_->RequestSeek(cb, target);
}
void ArenaWrappedDBIterAsync::SeekForPrev(const Slice& target) {
  db_iter_->SeekForPrev(target);
}
Status ArenaWrappedDBIterAsync::RequestSeekForPrev(const Callback& cb,
    const Slice& target) {
  return db_iter_->RequestSeekForPrev(cb, target);
}
void ArenaWrappedDBIterAsync::Next() {
  db_iter_->Next();
}
Status ArenaWrappedDBIterAsync::RequestNext(const Callback& cb) {
  return db_iter_->RequestNext(cb);
}
void ArenaWrappedDBIterAsync::Prev() {
  db_iter_->Prev();
}
Status ArenaWrappedDBIterAsync::RequestPrev(const Callback& cb) {
  return db_iter_->RequestPrev(cb);
}
Slice ArenaWrappedDBIterAsync::key() const {
  return db_iter_->key();
}
Slice ArenaWrappedDBIterAsync::value() const {
  return db_iter_->value();
}
Status ArenaWrappedDBIterAsync::status() const {
  return db_iter_->status();
}
Status ArenaWrappedDBIterAsync::GetProperty(std::string prop_name,
    std::string* prop) {
  return db_iter_->GetProperty(prop_name, prop);
}
void ArenaWrappedDBIterAsync::RegisterCleanup(CleanupFunction function,
    void* arg1,
    void* arg2) {
  db_iter_->RegisterCleanup(function, arg1, arg2);
}

ArenaWrappedDBIterAsync* NewArenaWrappedDbIteratorAsync(
  Env* env, const ReadOptions& read_options,
  const ImmutableCFOptions& cf_options, const Comparator* user_key_comparator,
  const SequenceNumber& sequence, uint64_t max_sequential_skip_in_iterations,
  uint64_t version_number) {
  ArenaWrappedDBIterAsync* iter = new ArenaWrappedDBIterAsync();
  Arena* arena = iter->GetArena();
  auto mem = arena->AllocateAligned(sizeof(DBIterAsync));
  DBIterAsync* db_iter = new (mem)
              DBIterAsync(env, read_options, cf_options, user_key_comparator, nullptr,
                          sequence, true, max_sequential_skip_in_iterations, version_number);

  iter->SetDBIter(db_iter);

  return iter;
}


}  // namespace rocksdb
