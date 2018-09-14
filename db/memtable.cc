//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"

#include <algorithm>
#include <limits>
#include <memory>

#include "db/dbformat.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/read_callback.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/merging_iterator.h"
#include "util/arena.h"
#include "util/autovector.h"
#include "util/coding.h"
#include "util/memory_usage.h"
#include "util/murmurhash.h"
#include "util/mutexlock.h"

namespace rocksdb {

ImmutableMemTableOptions::ImmutableMemTableOptions(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options)
    : arena_block_size(mutable_cf_options.arena_block_size),
      memtable_prefix_bloom_bits(
          static_cast<uint32_t>(
              static_cast<double>(mutable_cf_options.write_buffer_size) *
              mutable_cf_options.memtable_prefix_bloom_size_ratio) *
          8u),
      memtable_huge_page_size(mutable_cf_options.memtable_huge_page_size),
      inplace_update_support(ioptions.inplace_update_support),
      inplace_update_num_locks(mutable_cf_options.inplace_update_num_locks),
      inplace_callback(ioptions.inplace_callback),
      max_successive_merges(mutable_cf_options.max_successive_merges),
      statistics(ioptions.statistics),
      merge_operator(ioptions.merge_operator),
      info_log(ioptions.info_log) {}

MemTable::MemTable(const InternalKeyComparator& cmp,
                   const ImmutableCFOptions& ioptions,
                   const MutableCFOptions& mutable_cf_options,
                   WriteBufferManager* write_buffer_manager,
                   SequenceNumber latest_seq, uint32_t column_family_id)
    : comparator_(cmp),
      moptions_(ioptions, mutable_cf_options),
      refs_(0),
      kArenaBlockSize(OptimizeBlockSize(moptions_.arena_block_size)),
      mem_tracker_(write_buffer_manager),
      arena_(
          moptions_.arena_block_size,
          (write_buffer_manager != nullptr && write_buffer_manager->enabled())
              ? &mem_tracker_
              : nullptr,
          mutable_cf_options.memtable_huge_page_size),
      table_(mutable_cf_options.memtable_factory->CreateMemTableRep(
          comparator_, &arena_, ioptions, mutable_cf_options,
          column_family_id)),
      range_del_table_(SkipListFactory().CreateMemTableRep(
          comparator_, &arena_, nullptr /* transform */, ioptions.info_log,
          column_family_id)),
      is_range_del_table_empty_(true),
      data_size_(0),
      num_entries_(0),
      num_deletes_(0),
      write_buffer_size_(mutable_cf_options.write_buffer_size),
      flush_in_progress_(false),
      flush_completed_(false),
      file_number_(0),
      first_seqno_(0),
      earliest_seqno_(latest_seq),
      creation_seq_(latest_seq),
      mem_next_logfile_number_(0),
      min_prep_log_referenced_(0),
      locks_(moptions_.inplace_update_support
                 ? moptions_.inplace_update_num_locks
                 : 0),
      prefix_extractor_(mutable_cf_options.prefix_extractor.get()),
      flush_state_(FLUSH_NOT_REQUESTED),
      env_(ioptions.env),
      insert_with_hint_prefix_extractor_(
          ioptions.memtable_insert_with_hint_prefix_extractor),
      oldest_key_time_(std::numeric_limits<uint64_t>::max()) {
  UpdateFlushState();
  // something went wrong if we need to flush before inserting anything
  assert(!ShouldScheduleFlush());

  if (prefix_extractor_ && moptions_.memtable_prefix_bloom_bits > 0) {
    prefix_bloom_.reset(new DynamicBloom(
        &arena_, moptions_.memtable_prefix_bloom_bits, ioptions.bloom_locality,
        6 /* hard coded 6 probes */, nullptr, moptions_.memtable_huge_page_size,
        ioptions.info_log));
  }
}

MemTableRep* MemTableRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& key_cmp, Allocator* allocator,
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options,
    uint32_t /* column_family_id */) {
  return CreateMemTableRep(key_cmp, allocator,
                           mutable_cf_options.prefix_extractor.get(),
                           ioptions.info_log);
}

MemTable::~MemTable() {
  mem_tracker_.FreeMem();
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() {
  autovector<size_t> usages = {arena_.ApproximateMemoryUsage(),
                               table_->ApproximateMemoryUsage(),
                               range_del_table_->ApproximateMemoryUsage(),
                               rocksdb::ApproximateMemoryUsage(insert_hints_)};
  size_t total_usage = 0;
  for (size_t usage : usages) {
    // If usage + total_usage >= kMaxSizet, return kMaxSizet.
    // the following variation is to avoid numeric overflow.
    if (usage >= port::kMaxSizet - total_usage) {
      return port::kMaxSizet;
    }
    total_usage += usage;
  }
  // otherwise, return the actual usage
  return total_usage;
}

bool MemTable::ShouldFlushNow() const {
  size_t write_buffer_size = write_buffer_size_.load(std::memory_order_relaxed);
  // In a lot of times, we cannot allocate arena blocks that exactly matches the
  // buffer size. Thus we have to decide if we should over-allocate or
  // under-allocate.
  // This constant variable can be interpreted as: if we still have more than
  // "kAllowOverAllocationRatio * kArenaBlockSize" space left, we'd try to over
  // allocate one more block.
  const double kAllowOverAllocationRatio = 0.6;

  // If arena still have room for new block allocation, we can safely say it
  // shouldn't flush.
  auto allocated_memory = table_->ApproximateMemoryUsage() +
                          range_del_table_->ApproximateMemoryUsage() +
                          arena_.MemoryAllocatedBytes();

  // if we can still allocate one more block without exceeding the
  // over-allocation ratio, then we should not flush.
  if (allocated_memory + kArenaBlockSize <
      write_buffer_size + kArenaBlockSize * kAllowOverAllocationRatio) {
    return false;
  }

  // if user keeps adding entries that exceeds write_buffer_size, we need to
  // flush earlier even though we still have much available memory left.
  if (allocated_memory >
      write_buffer_size + kArenaBlockSize * kAllowOverAllocationRatio) {
    return true;
  }

  // In this code path, Arena has already allocated its "last block", which
  // means the total allocatedmemory size is either:
  //  (1) "moderately" over allocated the memory (no more than `0.6 * arena
  // block size`. Or,
  //  (2) the allocated memory is less than write buffer size, but we'll stop
  // here since if we allocate a new arena block, we'll over allocate too much
  // more (half of the arena block size) memory.
  //
  // In either case, to avoid over-allocate, the last block will stop allocation
  // when its usage reaches a certain ratio, which we carefully choose "0.75
  // full" as the stop condition because it addresses the following issue with
  // great simplicity: What if the next inserted entry's size is
  // bigger than AllocatedAndUnused()?
  //
  // The answer is: if the entry size is also bigger than 0.25 *
  // kArenaBlockSize, a dedicated block will be allocated for it; otherwise
  // arena will anyway skip the AllocatedAndUnused() and allocate a new, empty
  // and regular block. In either case, we *overly* over-allocated.
  //
  // Therefore, setting the last block to be at most "0.75 full" avoids both
  // cases.
  //
  // NOTE: the average percentage of waste space of this approach can be counted
  // as: "arena block size * 0.25 / write buffer size". User who specify a small
  // write buffer size and/or big arena block size may suffer.
  return arena_.AllocatedAndUnused() < kArenaBlockSize / 4;
}

void MemTable::UpdateFlushState() {
  auto state = flush_state_.load(std::memory_order_relaxed);
  if (state == FLUSH_NOT_REQUESTED && ShouldFlushNow()) {
    // ignore CAS failure, because that means somebody else requested
    // a flush
    flush_state_.compare_exchange_strong(state, FLUSH_REQUESTED,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed);
  }
}

void MemTable::UpdateOldestKeyTime() {
  uint64_t oldest_key_time = oldest_key_time_.load(std::memory_order_relaxed);
  if (oldest_key_time == std::numeric_limits<uint64_t>::max()) {
    int64_t current_time = 0;
    auto s = env_->GetCurrentTime(&current_time);
    if (s.ok()) {
      assert(current_time >= 0);
      // If fail, the timestamp is already set.
      oldest_key_time_.compare_exchange_strong(
          oldest_key_time, static_cast<uint64_t>(current_time),
          std::memory_order_relaxed, std::memory_order_relaxed);
    }
  }
}

int MemTable::KeyComparator::operator()(const char* prefix_len_key1,
                                        const char* prefix_len_key2) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice k1 = GetLengthPrefixedSlice(prefix_len_key1);
  Slice k2 = GetLengthPrefixedSlice(prefix_len_key2);
  return comparator.Compare(k1, k2);
}

int MemTable::KeyComparator::operator()(const char* prefix_len_key,
                                        const Slice& key) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(prefix_len_key);
  return comparator.Compare(a, key);
}

void MemTableRep::InsertConcurrently(KeyHandle /*handle*/) {
#ifndef ROCKSDB_LITE
  throw std::runtime_error("concurrent insert not supported");
#else
  abort();
#endif
}

const InternalKeyComparator* MemTable::KeyComparator::icomparator() const {
  return &comparator;
}

Slice MemTableRep::UserKey(const char* key) const {
  Slice slice = GetLengthPrefixedSlice(key);
  return Slice(slice.data(), slice.size() - 8);
}

size_t MemTableRep::EncodeKeyValueSize(const Slice& key,
                                       const Slice& value) {
  size_t buf_size = 0;
  buf_size += VarintLength(key.size()) + key.size();
  buf_size += VarintLength(value.size()) + value.size();
  return buf_size;
}

void MemTableRep::EncodeKeyValue(const Slice& key, const Slice& value,
                                 char* buf) {
  char* p = EncodeVarint32(buf, (uint32_t)key.size());
  memcpy(p, key.data(), key.size());
  p = EncodeVarint32(p + key.size(), (uint32_t)value.size());
  memcpy(p, value.data(), value.size());
}

bool MemTableRep::InsertKeyValue(const Slice& internal_key,
                                 const Slice& value) {
  size_t buf_size = EncodeKeyValueSize(internal_key, value);
  char* buf;
  KeyHandle handle = Allocate(buf_size, &buf);
  EncodeKeyValue(internal_key, value, buf);
  Insert(handle);
  return true;
}

bool MemTableRep::InsertKeyValueWithHint(const Slice& internal_key,
                                         const Slice& value,
                                         void** hint) {
  size_t buf_size = EncodeKeyValueSize(internal_key, value);
  char* buf;
  KeyHandle handle = Allocate(buf_size, &buf);
  EncodeKeyValue(internal_key, value, buf);
  InsertWithHint(handle, hint);
  return true;
}

bool MemTableRep::InsertKeyValueConcurrently(const Slice& internal_key,
                                             const Slice& value) {
  size_t buf_size = EncodeKeyValueSize(internal_key, value);
  char* buf;
  KeyHandle handle = Allocate(buf_size, &buf);
  EncodeKeyValue(internal_key, value, buf);
  InsertConcurrently(handle);
  return true;
}

KeyHandle MemTableRep::Allocate(const size_t len, char** buf) {
  *buf = allocator_->Allocate(len);
  return static_cast<KeyHandle>(*buf);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, static_cast<uint32_t>(target.size()));
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public InternalIterator {
 public:
  MemTableIterator(const MemTable& mem, const ReadOptions& read_options,
                   Arena* arena, bool use_range_del_table = false)
      : bloom_(nullptr),
        prefix_extractor_(mem.prefix_extractor_),
        comparator_(mem.comparator_),
        valid_(false),
        arena_mode_(arena != nullptr),
        value_pinned_(
            !mem.GetImmutableMemTableOptions()->inplace_update_support) {
    if (use_range_del_table) {
      iter_ = mem.range_del_table_->GetIterator(arena);
    } else if (prefix_extractor_ != nullptr && !read_options.total_order_seek) {
      bloom_ = mem.prefix_bloom_.get();
      iter_ = mem.table_->GetDynamicPrefixIterator(arena);
    } else {
      iter_ = mem.table_->GetIterator(arena);
    }
    is_seek_for_prev_supported_ = iter_->IsSeekForPrevSupported();
  }

  ~MemTableIterator() {
#ifndef NDEBUG
    // Assert that the MemTableIterator is never deleted while
    // Pinning is Enabled.
    assert(!pinned_iters_mgr_ ||
           (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
#endif
    if (arena_mode_) {
      iter_->~Iterator();
    } else {
      delete iter_;
    }
  }

#ifndef NDEBUG
  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;
#endif

  virtual bool Valid() const override { return valid_; }
  virtual void Seek(const Slice& k) override {
    PERF_TIMER_GUARD(seek_on_memtable_time);
    PERF_COUNTER_ADD(seek_on_memtable_count, 1);
    if (bloom_ != nullptr) {
      if (!bloom_->MayContain(
              prefix_extractor_->Transform(ExtractUserKey(k)))) {
        PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
        valid_ = false;
        return;
      } else {
        PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
      }
    }
    iter_->Seek(k, nullptr);
    valid_ = iter_->Valid();
  }
  virtual void SeekForPrev(const Slice& k) override {
    PERF_TIMER_GUARD(seek_on_memtable_time);
    PERF_COUNTER_ADD(seek_on_memtable_count, 1);
    if (bloom_ != nullptr) {
      if (!bloom_->MayContain(
              prefix_extractor_->Transform(ExtractUserKey(k)))) {
        PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
        valid_ = false;
        return;
      } else {
        PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
      }
    }
    if (is_seek_for_prev_supported_) {
      iter_->SeekForPrev(k, nullptr);
      valid_ = iter_->Valid();
    } else {
      iter_->Seek(k, nullptr);
      valid_ = iter_->Valid();
      if (!Valid()) {
        SeekToLast();
      }
      while (Valid() && comparator_.comparator.Compare(k, key()) < 0) {
        Prev();
      }
    }
  }
  virtual void SeekToFirst() override {
    iter_->SeekToFirst();
    valid_ = iter_->Valid();
  }
  virtual void SeekToLast() override {
    iter_->SeekToLast();
    valid_ = iter_->Valid();
  }
  virtual void Next() override {
    PERF_COUNTER_ADD(next_on_memtable_count, 1);
    assert(Valid());
    iter_->Next();
    valid_ = iter_->Valid();
  }
  virtual void Prev() override {
    PERF_COUNTER_ADD(prev_on_memtable_count, 1);
    assert(Valid());
    iter_->Prev();
    valid_ = iter_->Valid();
  }
  virtual Slice key() const override {
    assert(Valid());
    return iter_->GetKey();
  }
  virtual Slice value() const override {
    assert(Valid());
    return iter_->GetValue();
  }

  virtual Status status() const override { return Status::OK(); }

  virtual bool IsKeyPinned() const override { return iter_->IsKeyPinned(); }

  virtual bool IsValuePinned() const override {
    // memtable value is always pinned, except if we allow inplace update.
    return value_pinned_;
  }

 private:
  DynamicBloom* bloom_;
  const SliceTransform* const prefix_extractor_;
  const MemTable::KeyComparator comparator_;
  MemTableRep::Iterator* iter_;
  bool valid_;
  bool arena_mode_;
  bool value_pinned_;
  bool is_seek_for_prev_supported_;

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

InternalIterator* MemTable::NewIterator(const ReadOptions& read_options,
                                        Arena* arena) {
  assert(arena != nullptr);
  auto mem = arena->AllocateAligned(sizeof(MemTableIterator));
  return new (mem) MemTableIterator(*this, read_options, arena);
}

InternalIterator* MemTable::NewRangeTombstoneIterator(
    const ReadOptions& read_options) {
  if (read_options.ignore_range_deletions || is_range_del_table_empty_) {
    return nullptr;
  }
  return new MemTableIterator(*this, read_options, nullptr /* arena */,
                              true /* use_range_del_table */);
}

port::RWMutex* MemTable::GetLock(const Slice& key) {
  static murmur_hash hash;
  return &locks_[hash(key) % locks_.size()];
}

MemTable::MemTableStats MemTable::ApproximateStats(const Slice& start_ikey,
                                                   const Slice& end_ikey) {
  uint64_t entry_count = table_->ApproximateNumEntries(start_ikey, end_ikey);
  entry_count += range_del_table_->ApproximateNumEntries(start_ikey, end_ikey);
  if (entry_count == 0) {
    return {0, 0};
  }
  uint64_t n = num_entries_.load(std::memory_order_relaxed);
  if (n == 0) {
    return {0, 0};
  }
  if (entry_count > n) {
    // (range_del_)table_->ApproximateNumEntries() is just an estimate so it can
    // be larger than actual entries we have. Cap it to entries we have to limit
    // the inaccuracy.
    entry_count = n;
  }
  uint64_t data_size = data_size_.load(std::memory_order_relaxed);
  return {entry_count * (data_size / n), entry_count};
}

bool MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key, /* user key */
                   const Slice& value, bool allow_concurrent,
                   MemTablePostProcessInfo* post_process_info) {
  std::unique_ptr<MemTableRep>& table =
      type == kTypeRangeDeletion ? range_del_table_ : table_;

  InternalKey internal_key(key, s, type);
  size_t encoded_len =
      MemTableRep::EncodeKeyValueSize(internal_key.Encode(), value);
  if (!allow_concurrent) {
    // Extract prefix for insert with hint.
    if (insert_with_hint_prefix_extractor_ != nullptr &&
        insert_with_hint_prefix_extractor_->InDomain(internal_key.user_key())) {
      Slice prefix = insert_with_hint_prefix_extractor_->Transform(
          internal_key.user_key());
      bool res = table->InsertKeyValueWithHint(internal_key.Encode(), value,
                                               &insert_hints_[prefix]);
      if (UNLIKELY(!res)) {
        return res;
      }
    } else {
      bool res = table->InsertKeyValue(internal_key.Encode(), value);
      if (UNLIKELY(!res)) {
        return res;
      }
    }

    // this is a bit ugly, but is the way to avoid locked instructions
    // when incrementing an atomic
    num_entries_.store(num_entries_.load(std::memory_order_relaxed) + 1,
                       std::memory_order_relaxed);
    data_size_.store(data_size_.load(std::memory_order_relaxed) + encoded_len,
                     std::memory_order_relaxed);
    if (type == kTypeDeletion) {
      num_deletes_.store(num_deletes_.load(std::memory_order_relaxed) + 1,
                         std::memory_order_relaxed);
    }

    if (prefix_bloom_) {
      assert(prefix_extractor_);
      prefix_bloom_->Add(prefix_extractor_->Transform(key));
    }

    // The first sequence number inserted into the memtable
    assert(first_seqno_ == 0 || s >= first_seqno_);
    if (first_seqno_ == 0) {
      first_seqno_.store(s, std::memory_order_relaxed);

      if (earliest_seqno_ == kMaxSequenceNumber) {
        earliest_seqno_.store(GetFirstSequenceNumber(),
                              std::memory_order_relaxed);
      }
      assert(first_seqno_.load() >= earliest_seqno_.load());
    }
    assert(post_process_info == nullptr);
    UpdateFlushState();
  } else {
    bool res = table->InsertKeyValueConcurrently(internal_key.Encode(), value);
    if (UNLIKELY(!res)) {
      return res;
    }

    assert(post_process_info != nullptr);
    post_process_info->num_entries++;
    post_process_info->data_size += encoded_len;
    if (type == kTypeDeletion) {
      post_process_info->num_deletes++;
    }

    if (prefix_bloom_) {
      assert(prefix_extractor_);
      prefix_bloom_->AddConcurrently(prefix_extractor_->Transform(key));
    }

    // atomically update first_seqno_ and earliest_seqno_.
    uint64_t cur_seq_num = first_seqno_.load(std::memory_order_relaxed);
    while ((cur_seq_num == 0 || s < cur_seq_num) &&
           !first_seqno_.compare_exchange_weak(cur_seq_num, s)) {
    }
    uint64_t cur_earliest_seqno =
        earliest_seqno_.load(std::memory_order_relaxed);
    while (
        (cur_earliest_seqno == kMaxSequenceNumber || s < cur_earliest_seqno) &&
        !first_seqno_.compare_exchange_weak(cur_earliest_seqno, s)) {
    }
  }
  if (is_range_del_table_empty_ && type == kTypeRangeDeletion) {
    is_range_del_table_empty_ = false;
  }
  UpdateOldestKeyTime();
  return true;
}

// Callback from MemTable::Get()
namespace {

struct Saver {
  Status* status;
  const LookupKey* key;
  bool* found_final_value;  // Is value set correctly? Used by KeyMayExist
  bool* merge_in_progress;
  std::string* value;
  SequenceNumber seq;
  const MergeOperator* merge_operator;
  // the merge operations encountered;
  MergeContext* merge_context;
  RangeDelAggregator* range_del_agg;
  MemTable* mem;
  Logger* logger;
  Statistics* statistics;
  bool inplace_update_support;
  Env* env_;
  ReadCallback* callback_;
  bool* is_blob_index;

  bool CheckCallback(SequenceNumber _seq) {
    if (callback_) {
      return callback_->IsVisible(_seq);
    }
    return true;
  }
};
}  // namespace

static bool SaveValue(void* arg, const MemTableRep::KeyValuePair* pair) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  MergeContext* merge_context = s->merge_context;
  RangeDelAggregator* range_del_agg = s->range_del_agg;
  const MergeOperator* merge_operator = s->merge_operator;

  assert(s != nullptr && merge_context != nullptr && range_del_agg != nullptr);

  Slice internal_key, value;
  std::tie(internal_key, value) = pair->GetKeyValue();
  // Check that it belongs to same user key.  We do not check the
  // sequence number since the Seek() call above should have skipped
  // all entries with overly large sequence numbers.
  if (s->mem->GetInternalKeyComparator().user_comparator()->Equal(
          ExtractUserKey(internal_key), s->key->user_key())) {
    // Correct user key
    const uint64_t tag =
        DecodeFixed64(internal_key.data() + internal_key.size() - 8);
    ValueType type;
    SequenceNumber seq;
    UnPackSequenceAndType(tag, &seq, &type);
    // If the value is not in the snapshot, skip it
    if (!s->CheckCallback(seq)) {
      return true;  // to continue to the next seq
    }

    s->seq = seq;

    if ((type == kTypeValue || type == kTypeMerge || type == kTypeBlobIndex) &&
        range_del_agg->ShouldDelete(internal_key)) {
      type = kTypeRangeDeletion;
    }
    switch (type) {
      case kTypeBlobIndex:
        if (s->is_blob_index == nullptr) {
          ROCKS_LOG_ERROR(s->logger, "Encounter unexpected blob index.");
          *(s->status) = Status::NotSupported(
              "Encounter unsupported blob value. Please open DB with "
              "rocksdb::blob_db::BlobDB instead.");
        } else if (*(s->merge_in_progress)) {
          *(s->status) =
              Status::NotSupported("Blob DB does not support merge operator.");
        }
        if (!s->status->ok()) {
          *(s->found_final_value) = true;
          return false;
        }
      // intentional fallthrough
      case kTypeValue: {
        if (s->inplace_update_support) {
          s->mem->GetLock(s->key->user_key())->ReadLock();
        }
        *(s->status) = Status::OK();
        if (*(s->merge_in_progress)) {
          if (s->value != nullptr) {
            *(s->status) = MergeHelper::TimedFullMerge(
                merge_operator, s->key->user_key(), &value,
                merge_context->GetOperands(), s->value, s->logger,
                s->statistics, s->env_, nullptr /* result_operand */, true);
          }
        } else if (s->value != nullptr) {
          s->value->assign(value.data(), value.size());
        }
        if (s->inplace_update_support) {
          s->mem->GetLock(s->key->user_key())->ReadUnlock();
        }
        *(s->found_final_value) = true;
        if (s->is_blob_index != nullptr) {
          *(s->is_blob_index) = (type == kTypeBlobIndex);
        }
        return false;
      }
      case kTypeDeletion:
      case kTypeSingleDeletion:
      case kTypeRangeDeletion: {
        if (*(s->merge_in_progress)) {
          if (s->value != nullptr) {
            *(s->status) = MergeHelper::TimedFullMerge(
                merge_operator, s->key->user_key(), nullptr,
                merge_context->GetOperands(), s->value, s->logger,
                s->statistics, s->env_, nullptr /* result_operand */, true);
          }
        } else {
          *(s->status) = Status::NotFound();
        }
        *(s->found_final_value) = true;
        return false;
      }
      case kTypeMerge: {
        if (!merge_operator) {
          *(s->status) = Status::InvalidArgument(
              "merge_operator is not properly initialized.");
          // Normally we continue the loop (return true) when we see a merge
          // operand.  But in case of an error, we should stop the loop
          // immediately and pretend we have found the value to stop further
          // seek.  Otherwise, the later call will override this error status.
          *(s->found_final_value) = true;
          return false;
        }
        *(s->merge_in_progress) = true;
        merge_context->PushOperand(
            value, s->inplace_update_support == false /* operand_pinned */);
        if (merge_operator->ShouldMerge(merge_context->GetOperands())) {
          *(s->status) = MergeHelper::TimedFullMerge(
              merge_operator, s->key->user_key(), nullptr,
              merge_context->GetOperands(), s->value, s->logger, s->statistics,
              s->env_, nullptr /* result_operand */, true);
          *(s->found_final_value) = true;
          return false;
        }
        return true;
      }
      default:
        assert(false);
        return true;
    }
  }

  // s->state could be Corrupt, merge or notfound
  return false;
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s,
                   MergeContext* merge_context,
                   RangeDelAggregator* range_del_agg, SequenceNumber* seq,
                   const ReadOptions& read_opts, ReadCallback* callback,
                   bool* is_blob_index) {
  // The sequence number is updated synchronously in version_set.h
  if (IsEmpty()) {
    // Avoiding recording stats for speed.
    return false;
  }
  PERF_TIMER_GUARD(get_from_memtable_time);

  std::unique_ptr<InternalIterator> range_del_iter(
      NewRangeTombstoneIterator(read_opts));
  Status status = range_del_agg->AddTombstones(std::move(range_del_iter));
  if (!status.ok()) {
    *s = status;
    return false;
  }

  Slice user_key = key.user_key();
  bool found_final_value = false;
  bool merge_in_progress = s->IsMergeInProgress();
  bool const may_contain =
      nullptr == prefix_bloom_
          ? false
          : prefix_bloom_->MayContain(prefix_extractor_->Transform(user_key));
  if (prefix_bloom_ && !may_contain) {
    // iter is null if prefix bloom says the key does not exist
    PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
    *seq = kMaxSequenceNumber;
  } else {
    if (prefix_bloom_) {
      PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
    }
    Saver saver;
    saver.status = s;
    saver.found_final_value = &found_final_value;
    saver.merge_in_progress = &merge_in_progress;
    saver.key = &key;
    saver.value = value;
    saver.seq = kMaxSequenceNumber;
    saver.mem = this;
    saver.merge_context = merge_context;
    saver.range_del_agg = range_del_agg;
    saver.merge_operator = moptions_.merge_operator;
    saver.logger = moptions_.info_log;
    saver.inplace_update_support = moptions_.inplace_update_support;
    saver.statistics = moptions_.statistics;
    saver.env_ = env_;
    saver.callback_ = callback;
    saver.is_blob_index = is_blob_index;
    table_->Get(key, &saver, SaveValue);

    *seq = saver.seq;
  }

  // No change to value, since we have not yet found a Put/Delete
  if (!found_final_value && merge_in_progress) {
    *s = Status::MergeInProgress();
  }
  PERF_COUNTER_ADD(get_from_memtable_count, 1);
  return found_final_value;
}

void MemTable::Update(SequenceNumber seq, const Slice& key,
                      const Slice& value) {
  LookupKey lkey(key, seq);
  Slice mem_key = lkey.memtable_key();

  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetDynamicPrefixIterator());
  iter->Seek(lkey.internal_key(), mem_key.data());

  if (iter->Valid()) {
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    Slice internal_key, prev_value;
    std::tie(internal_key, prev_value) = iter->GetKeyValue();
    if (comparator_.comparator.user_comparator()->Equal(
            ExtractUserKey(internal_key), lkey.user_key())) {
      // Correct user key
      const uint64_t tag =
          DecodeFixed64(internal_key.data() + internal_key .size() - 8);
      ValueType type;
      SequenceNumber unused;
      UnPackSequenceAndType(tag, &unused, &type);
      if (type == kTypeValue) {
        uint32_t prev_size = static_cast<uint32_t>(prev_value.size());
        uint32_t new_size = static_cast<uint32_t>(value.size());

        // Update value, if new value size <= previous value size
        if (new_size <= prev_size) {
          char* p =
              const_cast<char*>(prev_value.data()) - VarintLength(prev_size);
          p = EncodeVarint32(p, new_size);
          WriteLock wl(GetLock(lkey.user_key()));
          memcpy(p, value.data(), value.size());
          RecordTick(moptions_.statistics, NUMBER_KEYS_UPDATED);
          return;
        }
      }
    }
  }

  // key doesn't exist
  Add(seq, kTypeValue, key, value);
}

bool MemTable::UpdateCallback(SequenceNumber seq, const Slice& key,
                              const Slice& delta) {
  LookupKey lkey(key, seq);
  Slice memkey = lkey.memtable_key();

  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetDynamicPrefixIterator());
  iter->Seek(lkey.internal_key(), memkey.data());

  if (iter->Valid()) {
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    Slice internal_key, prev_value;
    std::tie(internal_key, prev_value) = iter->GetKeyValue();
    if (comparator_.comparator.user_comparator()->Equal(
            ExtractUserKey(internal_key), lkey.user_key())) {
      // Correct user key
      const uint64_t tag =
          DecodeFixed64(internal_key.data() + internal_key.size() - 8);
      ValueType type;
      uint64_t unused;
      UnPackSequenceAndType(tag, &unused, &type);
      switch (type) {
        case kTypeValue: {
          uint32_t prev_size = static_cast<uint32_t>(prev_value.size());

          char* prev_buffer = const_cast<char*>(prev_value.data());
          uint32_t new_prev_size = prev_size;

          std::string str_value;
          WriteLock wl(GetLock(lkey.user_key()));
          auto status = moptions_.inplace_callback(prev_buffer, &new_prev_size,
                                                   delta, &str_value);
          if (status == UpdateStatus::UPDATED_INPLACE) {
            // Value already updated by callback.
            assert(new_prev_size <= prev_size);
            if (new_prev_size < prev_size) {
              // overwrite the new prev_size
              char* p =
                const_cast<char*>(prev_value.data()) - VarintLength(prev_size);
              p = EncodeVarint32(p, new_prev_size);
              if (p < prev_buffer) {
                // shift the value buffer as well.
                memmove(p, prev_buffer, new_prev_size);
              }
            }
            RecordTick(moptions_.statistics, NUMBER_KEYS_UPDATED);
            UpdateFlushState();
            return true;
          } else if (status == UpdateStatus::UPDATED) {
            Add(seq, kTypeValue, key, Slice(str_value));
            RecordTick(moptions_.statistics, NUMBER_KEYS_WRITTEN);
            UpdateFlushState();
            return true;
          } else if (status == UpdateStatus::UPDATE_FAILED) {
            // No action required. Return.
            UpdateFlushState();
            return true;
          }
        }
        default:
          break;
      }
    }
  }
  // If the latest value is not kTypeValue
  // or key doesn't exist
  return false;
}

size_t MemTable::CountSuccessiveMergeEntries(const LookupKey& key) {
  Slice memkey = key.memtable_key();

  // A total ordered iterator is costly for some memtablerep (prefix aware
  // reps). By passing in the user key, we allow efficient iterator creation.
  // The iterator only needs to be ordered within the same user key.
  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetDynamicPrefixIterator());
  iter->Seek(key.internal_key(), memkey.data());

  size_t num_successive_merges = 0;

  for (; iter->Valid(); iter->Next()) {
    Slice internal_key = iter->GetKey();
    if (!comparator_.comparator.user_comparator()->Equal(
            ExtractUserKey(internal_key), key.user_key())) {
      break;
    }

    const uint64_t tag =
        DecodeFixed64(internal_key.data() + internal_key.size() - 8);
    ValueType type;
    uint64_t unused;
    UnPackSequenceAndType(tag, &unused, &type);
    if (type != kTypeMerge) {
      break;
    }

    ++num_successive_merges;
  }

  return num_successive_merges;
}

void MemTableRep::Get(const LookupKey& k, void* callback_args,
                      bool (*callback_func)(void* arg, const KeyValuePair*)) {
  auto iter = GetDynamicPrefixIterator();
  for (iter->Seek(k.internal_key(), k.memtable_key().data());
       iter->Valid() && callback_func(callback_args, iter); iter->Next()) {
  }
  delete iter;
}

Slice MemTableRep::EncodedKeyValuePair::GetKey() const {
  return GetLengthPrefixedSlice(key_);
}

Slice MemTableRep::EncodedKeyValuePair::GetValue() const {
  Slice key_slice = GetLengthPrefixedSlice(key_);
  return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
}

std::pair<Slice, Slice> MemTableRep::EncodedKeyValuePair::GetKeyValue() const {
  Slice key_slice = GetLengthPrefixedSlice(key_);
  Slice value_slice =
      GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  return { key_slice, value_slice };
}

MemTableRep::KeyValuePair*
MemTableRep::EncodedKeyValuePair::SetKey(const char* key) {
  key_ = key;
  return this;
}

Slice MemTableRep::Iterator::GetKey() const {
  assert(Valid());
  return GetLengthPrefixedSlice(key());
}

Slice MemTableRep::Iterator::GetValue() const {
  assert(Valid());
  Slice key_slice = GetLengthPrefixedSlice(key());
  return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
}
std::pair<Slice, Slice> MemTableRep::Iterator::GetKeyValue() const {
  assert(Valid());
  Slice key_slice = GetLengthPrefixedSlice(key());
  Slice value_slice =
      GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  return { key_slice, value_slice };
}

void MemTable::RefLogContainingPrepSection(uint64_t log) {
  assert(log > 0);
  auto cur = min_prep_log_referenced_.load();
  while ((log < cur || cur == 0) &&
         !min_prep_log_referenced_.compare_exchange_strong(cur, log)) {
    cur = min_prep_log_referenced_.load();
  }
}

uint64_t MemTable::GetMinLogContainingPrepSection() {
  return min_prep_log_referenced_.load();
}

}  // namespace rocksdb
