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
#include <array>
#include <limits>
#include <memory>
#include <optional>

#include "db/dbformat.h"
#include "db/kv_checksum.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/read_callback.h"
#include "db/wide/wide_column_serialization.h"
#include "logging/logging.h"
#include "memory/arena.h"
#include "memory/memory_usage.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "port/lang.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/types.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/merging_iterator.h"
#include "util/autovector.h"
#include "util/coding.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

ImmutableMemTableOptions::ImmutableMemTableOptions(
    const ImmutableOptions& ioptions,
    const MutableCFOptions& mutable_cf_options)
    : arena_block_size(mutable_cf_options.arena_block_size),
      memtable_prefix_bloom_bits(
          static_cast<uint32_t>(
              static_cast<double>(mutable_cf_options.write_buffer_size) *
              mutable_cf_options.memtable_prefix_bloom_size_ratio) *
          8u),
      memtable_huge_page_size(mutable_cf_options.memtable_huge_page_size),
      memtable_whole_key_filtering(
          mutable_cf_options.memtable_whole_key_filtering),
      inplace_update_support(ioptions.inplace_update_support),
      inplace_update_num_locks(mutable_cf_options.inplace_update_num_locks),
      inplace_callback(ioptions.inplace_callback),
      max_successive_merges(mutable_cf_options.max_successive_merges),
      strict_max_successive_merges(
          mutable_cf_options.strict_max_successive_merges),
      statistics(ioptions.stats),
      merge_operator(ioptions.merge_operator.get()),
      info_log(ioptions.logger),
      protection_bytes_per_key(
          mutable_cf_options.memtable_protection_bytes_per_key),
      allow_data_in_errors(ioptions.allow_data_in_errors),
      paranoid_memory_checks(mutable_cf_options.paranoid_memory_checks),
      memtable_veirfy_per_key_checksum_on_seek(
          mutable_cf_options.memtable_veirfy_per_key_checksum_on_seek) {}

MemTable::MemTable(const InternalKeyComparator& cmp,
                   const ImmutableOptions& ioptions,
                   const MutableCFOptions& mutable_cf_options,
                   WriteBufferManager* write_buffer_manager,
                   SequenceNumber latest_seq, uint32_t column_family_id)
    : comparator_(cmp),
      moptions_(ioptions, mutable_cf_options),
      kArenaBlockSize(Arena::OptimizeBlockSize(moptions_.arena_block_size)),
      mem_tracker_(write_buffer_manager),
      arena_(moptions_.arena_block_size,
             (write_buffer_manager != nullptr &&
              (write_buffer_manager->enabled() ||
               write_buffer_manager->cost_to_cache()))
                 ? &mem_tracker_
                 : nullptr,
             mutable_cf_options.memtable_huge_page_size),
      table_(ioptions.memtable_factory->CreateMemTableRep(
          comparator_, &arena_, mutable_cf_options.prefix_extractor.get(),
          ioptions.logger, column_family_id)),
      range_del_table_(SkipListFactory().CreateMemTableRep(
          comparator_, &arena_, nullptr /* transform */, ioptions.logger,
          column_family_id)),
      is_range_del_table_empty_(true),
      data_size_(0),
      num_entries_(0),
      num_deletes_(0),
      num_range_deletes_(0),
      write_buffer_size_(mutable_cf_options.write_buffer_size),
      first_seqno_(0),
      earliest_seqno_(latest_seq),
      creation_seq_(latest_seq),
      min_prep_log_referenced_(0),
      locks_(moptions_.inplace_update_support
                 ? moptions_.inplace_update_num_locks
                 : 0),
      prefix_extractor_(mutable_cf_options.prefix_extractor.get()),
      flush_state_(FLUSH_NOT_REQUESTED),
      clock_(ioptions.clock),
      insert_with_hint_prefix_extractor_(
          ioptions.memtable_insert_with_hint_prefix_extractor.get()),
      oldest_key_time_(std::numeric_limits<uint64_t>::max()),
      approximate_memory_usage_(0),
      memtable_max_range_deletions_(
          mutable_cf_options.memtable_max_range_deletions),
      key_validation_callback_(
          (moptions_.protection_bytes_per_key != 0 &&
           moptions_.memtable_veirfy_per_key_checksum_on_seek)
              ? std::bind(&MemTable::ValidateKey, this, std::placeholders::_1,
                          std::placeholders::_2)
              : std::function<Status(const char*, bool)>(nullptr)) {
  UpdateFlushState();
  // something went wrong if we need to flush before inserting anything
  assert(!ShouldScheduleFlush());

  // use bloom_filter_ for both whole key and prefix bloom filter
  if ((prefix_extractor_ || moptions_.memtable_whole_key_filtering) &&
      moptions_.memtable_prefix_bloom_bits > 0) {
    bloom_filter_.reset(
        new DynamicBloom(&arena_, moptions_.memtable_prefix_bloom_bits,
                         6 /* hard coded 6 probes */,
                         moptions_.memtable_huge_page_size, ioptions.logger));
  }
  // Initialize cached_range_tombstone_ here since it could
  // be read before it is constructed in MemTable::Add(), which could also lead
  // to a data race on the global mutex table backing atomic shared_ptr.
  auto new_cache = std::make_shared<FragmentedRangeTombstoneListCache>();
  size_t size = cached_range_tombstone_.Size();
  for (size_t i = 0; i < size; ++i) {
#if defined(__cpp_lib_atomic_shared_ptr)
    std::atomic<std::shared_ptr<FragmentedRangeTombstoneListCache>>*
        local_cache_ref_ptr = cached_range_tombstone_.AccessAtCore(i);
    auto new_local_cache_ref = std::make_shared<
        const std::shared_ptr<FragmentedRangeTombstoneListCache>>(new_cache);
    std::shared_ptr<FragmentedRangeTombstoneListCache> aliased_ptr(
        new_local_cache_ref, new_cache.get());
    local_cache_ref_ptr->store(std::move(aliased_ptr),
                               std::memory_order_relaxed);
#else
    std::shared_ptr<FragmentedRangeTombstoneListCache>* local_cache_ref_ptr =
        cached_range_tombstone_.AccessAtCore(i);
    auto new_local_cache_ref = std::make_shared<
        const std::shared_ptr<FragmentedRangeTombstoneListCache>>(new_cache);
    std::atomic_store_explicit(
        local_cache_ref_ptr,
        std::shared_ptr<FragmentedRangeTombstoneListCache>(new_local_cache_ref,
                                                           new_cache.get()),
        std::memory_order_relaxed);
#endif
  }
  const Comparator* ucmp = cmp.user_comparator();
  assert(ucmp);
  ts_sz_ = ucmp->timestamp_size();
}

MemTable::~MemTable() {
  mem_tracker_.FreeMem();
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() {
  autovector<size_t> usages = {
      arena_.ApproximateMemoryUsage(), table_->ApproximateMemoryUsage(),
      range_del_table_->ApproximateMemoryUsage(),
      ROCKSDB_NAMESPACE::ApproximateMemoryUsage(insert_hints_)};
  size_t total_usage = 0;
  for (size_t usage : usages) {
    // If usage + total_usage >= kMaxSizet, return kMaxSizet.
    // the following variation is to avoid numeric overflow.
    if (usage >= std::numeric_limits<size_t>::max() - total_usage) {
      return std::numeric_limits<size_t>::max();
    }
    total_usage += usage;
  }
  approximate_memory_usage_.StoreRelaxed(total_usage);
  // otherwise, return the actual usage
  return total_usage;
}

bool MemTable::ShouldFlushNow() {
  if (IsMarkedForFlush()) {
    // TODO: dedicated flush reason when marked for flush
    return true;
  }

  // This is set if memtable_max_range_deletions is > 0,
  // and that many range deletions are done
  if (memtable_max_range_deletions_ > 0 &&
      num_range_deletes_.LoadRelaxed() >=
          static_cast<uint64_t>(memtable_max_range_deletions_)) {
    return true;
  }

  size_t write_buffer_size = write_buffer_size_.LoadRelaxed();
  // In a lot of times, we cannot allocate arena blocks that exactly matches the
  // buffer size. Thus we have to decide if we should over-allocate or
  // under-allocate.
  // This constant variable can be interpreted as: if we still have more than
  // "kAllowOverAllocationRatio * kArenaBlockSize" space left, we'd try to over
  // allocate one more block.
  const double kAllowOverAllocationRatio = 0.6;

  // range deletion use skip list which allocates all memeory through `arena_`
  assert(range_del_table_->ApproximateMemoryUsage() == 0);
  // If arena still have room for new block allocation, we can safely say it
  // shouldn't flush.
  auto allocated_memory =
      table_->ApproximateMemoryUsage() + arena_.MemoryAllocatedBytes();

  approximate_memory_usage_.StoreRelaxed(allocated_memory);

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
    auto s = clock_->GetCurrentTime(&current_time);
    if (s.ok()) {
      assert(current_time >= 0);
      // If fail, the timestamp is already set.
      oldest_key_time_.compare_exchange_strong(
          oldest_key_time, static_cast<uint64_t>(current_time),
          std::memory_order_relaxed, std::memory_order_relaxed);
    }
  }
}

Status MemTable::VerifyEntryChecksum(const char* entry,
                                     uint32_t protection_bytes_per_key,
                                     bool allow_data_in_errors) {
  if (protection_bytes_per_key == 0) {
    return Status::OK();
  }
  uint32_t key_length;
  const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
  if (key_ptr == nullptr) {
    return Status::Corruption("Unable to parse internal key length");
  }
  if (key_length < 8) {
    return Status::Corruption("Memtable entry internal key length too short.");
  }
  Slice user_key = Slice(key_ptr, key_length - 8);

  const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
  ValueType type;
  SequenceNumber seq;
  UnPackSequenceAndType(tag, &seq, &type);

  uint32_t value_length = 0;
  const char* value_ptr = GetVarint32Ptr(
      key_ptr + key_length, key_ptr + key_length + 5, &value_length);
  if (value_ptr == nullptr) {
    return Status::Corruption("Unable to parse internal key value");
  }
  Slice value = Slice(value_ptr, value_length);

  const char* checksum_ptr = value_ptr + value_length;
  bool match =
      ProtectionInfo64()
          .ProtectKVO(user_key, value, type)
          .ProtectS(seq)
          .Verify(static_cast<uint8_t>(protection_bytes_per_key), checksum_ptr);
  if (!match) {
    std::string msg(
        "Corrupted memtable entry, per key-value checksum verification "
        "failed.");
    if (allow_data_in_errors) {
      msg.append("Unrecognized value type: " +
                 std::to_string(static_cast<int>(type)) + ". ");
      msg.append("User key: " + user_key.ToString(/*hex=*/true) + ". ");
      msg.append("seq: " + std::to_string(seq) + ".");
    }
    return Status::Corruption(msg.c_str());
  }
  return Status::OK();
}

int MemTable::KeyComparator::operator()(const char* prefix_len_key1,
                                        const char* prefix_len_key2) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice k1 = GetLengthPrefixedSlice(prefix_len_key1);
  Slice k2 = GetLengthPrefixedSlice(prefix_len_key2);
  return comparator.CompareKeySeq(k1, k2);
}

int MemTable::KeyComparator::operator()(
    const char* prefix_len_key, const KeyComparator::DecodedType& key) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(prefix_len_key);
  return comparator.CompareKeySeq(a, key);
}

void MemTableRep::InsertConcurrently(KeyHandle /*handle*/) {
  throw std::runtime_error("concurrent insert not supported");
}

Slice MemTableRep::UserKey(const char* key) const {
  Slice slice = GetLengthPrefixedSlice(key);
  return Slice(slice.data(), slice.size() - 8);
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
  enum Kind { kPointEntries, kRangeDelEntries };
  MemTableIterator(
      Kind kind, const MemTable& mem, const ReadOptions& read_options,
      UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping = nullptr,
      Arena* arena = nullptr,
      const SliceTransform* cf_prefix_extractor = nullptr)
      : bloom_(nullptr),
        prefix_extractor_(mem.prefix_extractor_),
        comparator_(mem.comparator_),
        seqno_to_time_mapping_(seqno_to_time_mapping),
        status_(Status::OK()),
        logger_(mem.moptions_.info_log),
        ts_sz_(mem.ts_sz_),
        protection_bytes_per_key_(mem.moptions_.protection_bytes_per_key),
        valid_(false),
        value_pinned_(
            !mem.GetImmutableMemTableOptions()->inplace_update_support),
        arena_mode_(arena != nullptr),
        paranoid_memory_checks_(mem.moptions_.paranoid_memory_checks),
        validate_on_seek_(
            mem.moptions_.paranoid_memory_checks ||
            mem.moptions_.memtable_veirfy_per_key_checksum_on_seek),
        allow_data_in_error_(mem.moptions_.allow_data_in_errors),
        key_validation_callback_(mem.key_validation_callback_) {
    if (kind == kRangeDelEntries) {
      iter_ = mem.range_del_table_->GetIterator(arena);
    } else if (prefix_extractor_ != nullptr &&
               // NOTE: checking extractor equivalence when not pointer
               // equivalent is arguably too expensive for memtable
               prefix_extractor_ == cf_prefix_extractor &&
               (read_options.prefix_same_as_start ||
                (!read_options.total_order_seek &&
                 !read_options.auto_prefix_mode))) {
      // Auto prefix mode is not implemented in memtable yet.
      assert(kind == kPointEntries);
      bloom_ = mem.bloom_filter_.get();
      iter_ = mem.table_->GetDynamicPrefixIterator(arena);
    } else {
      assert(kind == kPointEntries);
      iter_ = mem.table_->GetIterator(arena);
    }
    status_.PermitUncheckedError();
  }
  // No copying allowed
  MemTableIterator(const MemTableIterator&) = delete;
  void operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override {
#ifndef NDEBUG
    // Assert that the MemTableIterator is never deleted while
    // Pinning is Enabled.
    assert(!pinned_iters_mgr_ || !pinned_iters_mgr_->PinningEnabled());
#endif
    if (arena_mode_) {
      iter_->~Iterator();
    } else {
      delete iter_;
    }
    status_.PermitUncheckedError();
  }

#ifndef NDEBUG
  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;
#endif

  bool Valid() const override {
    // If inner iter_ is not valid, then this iter should also not be valid.
    assert(iter_->Valid() || !(valid_ && status_.ok()));
    return valid_ && status_.ok();
  }

  void Seek(const Slice& k) override {
    PERF_TIMER_GUARD(seek_on_memtable_time);
    PERF_COUNTER_ADD(seek_on_memtable_count, 1);
    status_ = Status::OK();
    if (bloom_) {
      // iterator should only use prefix bloom filter
      Slice user_k_without_ts(ExtractUserKeyAndStripTimestamp(k, ts_sz_));
      if (prefix_extractor_->InDomain(user_k_without_ts)) {
        Slice prefix = prefix_extractor_->Transform(user_k_without_ts);
        if (!bloom_->MayContain(prefix)) {
          PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
          valid_ = false;
          return;
        } else {
          PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
        }
      }
    }
    if (validate_on_seek_) {
      status_ = iter_->SeekAndValidate(k, nullptr, allow_data_in_error_,
                                       paranoid_memory_checks_,
                                       key_validation_callback_);
    } else {
      iter_->Seek(k, nullptr);
    }
    valid_ = iter_->Valid();
    VerifyEntryChecksum();
  }
  void SeekForPrev(const Slice& k) override {
    PERF_TIMER_GUARD(seek_on_memtable_time);
    PERF_COUNTER_ADD(seek_on_memtable_count, 1);
    status_ = Status::OK();
    if (bloom_) {
      Slice user_k_without_ts(ExtractUserKeyAndStripTimestamp(k, ts_sz_));
      if (prefix_extractor_->InDomain(user_k_without_ts)) {
        if (!bloom_->MayContain(
                prefix_extractor_->Transform(user_k_without_ts))) {
          PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
          valid_ = false;
          return;
        } else {
          PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
        }
      }
    }
    if (validate_on_seek_) {
      status_ = iter_->SeekAndValidate(k, nullptr, allow_data_in_error_,
                                       paranoid_memory_checks_,
                                       key_validation_callback_);
    } else {
      iter_->Seek(k, nullptr);
    }
    valid_ = iter_->Valid();
    VerifyEntryChecksum();
    if (!Valid() && status().ok()) {
      SeekToLast();
    }
    while (Valid() && comparator_.comparator.Compare(k, key()) < 0) {
      Prev();
    }
  }
  void SeekToFirst() override {
    status_ = Status::OK();
    iter_->SeekToFirst();
    valid_ = iter_->Valid();
    VerifyEntryChecksum();
  }
  void SeekToLast() override {
    status_ = Status::OK();
    iter_->SeekToLast();
    valid_ = iter_->Valid();
    VerifyEntryChecksum();
  }
  void Next() override {
    PERF_COUNTER_ADD(next_on_memtable_count, 1);
    assert(Valid());
    if (paranoid_memory_checks_) {
      status_ = iter_->NextAndValidate(allow_data_in_error_);
    } else {
      iter_->Next();
      TEST_SYNC_POINT_CALLBACK("MemTableIterator::Next:0", iter_);
    }
    valid_ = iter_->Valid();
    VerifyEntryChecksum();
  }
  bool NextAndGetResult(IterateResult* result) override {
    Next();
    bool is_valid = Valid();
    if (is_valid) {
      result->key = key();
      result->bound_check_result = IterBoundCheck::kUnknown;
      result->value_prepared = true;
    }
    return is_valid;
  }
  void Prev() override {
    PERF_COUNTER_ADD(prev_on_memtable_count, 1);
    assert(Valid());
    if (paranoid_memory_checks_) {
      status_ = iter_->PrevAndValidate(allow_data_in_error_);
    } else {
      iter_->Prev();
    }
    valid_ = iter_->Valid();
    VerifyEntryChecksum();
  }
  Slice key() const override {
    assert(Valid());
    return GetLengthPrefixedSlice(iter_->key());
  }

  uint64_t write_unix_time() const override {
    assert(Valid());
    ParsedInternalKey pikey;
    Status s = ParseInternalKey(key(), &pikey, /*log_err_key=*/false);
    if (!s.ok()) {
      return std::numeric_limits<uint64_t>::max();
    } else if (kTypeValuePreferredSeqno == pikey.type) {
      return ParsePackedValueForWriteTime(value());
    } else if (!seqno_to_time_mapping_ || seqno_to_time_mapping_->Empty()) {
      return std::numeric_limits<uint64_t>::max();
    }
    return seqno_to_time_mapping_->GetProximalTimeBeforeSeqno(pikey.sequence);
  }

  Slice value() const override {
    assert(Valid());
    Slice key_slice = GetLengthPrefixedSlice(iter_->key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return status_; }

  bool IsKeyPinned() const override {
    // memtable data is always pinned
    return true;
  }

  bool IsValuePinned() const override {
    // memtable value is always pinned, except if we allow inplace update.
    return value_pinned_;
  }

 private:
  DynamicBloom* bloom_;
  const SliceTransform* const prefix_extractor_;
  const MemTable::KeyComparator comparator_;
  MemTableRep::Iterator* iter_;
  // The seqno to time mapping is owned by the SuperVersion.
  UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping_;
  Status status_;
  Logger* logger_;
  size_t ts_sz_;
  uint32_t protection_bytes_per_key_;
  bool valid_;
  bool value_pinned_;
  bool arena_mode_;
  const bool paranoid_memory_checks_;
  const bool validate_on_seek_;
  const bool allow_data_in_error_;
  const std::function<Status(const char*, bool)> key_validation_callback_;

  void VerifyEntryChecksum() {
    if (protection_bytes_per_key_ > 0 && Valid()) {
      status_ = MemTable::VerifyEntryChecksum(iter_->key(),
                                              protection_bytes_per_key_);
      if (!status_.ok()) {
        ROCKS_LOG_ERROR(logger_, "In MemtableIterator: %s", status_.getState());
      }
    }
  }
};

InternalIterator* MemTable::NewIterator(
    const ReadOptions& read_options,
    UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping, Arena* arena,
    const SliceTransform* prefix_extractor, bool /*for_flush*/) {
  assert(arena != nullptr);
  auto mem = arena->AllocateAligned(sizeof(MemTableIterator));
  return new (mem)
      MemTableIterator(MemTableIterator::kPointEntries, *this, read_options,
                       seqno_to_time_mapping, arena, prefix_extractor);
}

// An iterator wrapper that wraps a MemTableIterator and logically strips each
// key's user-defined timestamp.
class TimestampStrippingIterator : public InternalIterator {
 public:
  TimestampStrippingIterator(
      MemTableIterator::Kind kind, const MemTable& memtable,
      const ReadOptions& read_options,
      UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping, Arena* arena,
      const SliceTransform* cf_prefix_extractor, size_t ts_sz)
      : arena_mode_(arena != nullptr), kind_(kind), ts_sz_(ts_sz) {
    assert(ts_sz_ != 0);
    void* mem = arena ? arena->AllocateAligned(sizeof(MemTableIterator))
                      : operator new(sizeof(MemTableIterator));
    iter_ = new (mem)
        MemTableIterator(kind, memtable, read_options, seqno_to_time_mapping,
                         arena, cf_prefix_extractor);
  }

  // No copying allowed
  TimestampStrippingIterator(const TimestampStrippingIterator&) = delete;
  void operator=(const TimestampStrippingIterator&) = delete;

  ~TimestampStrippingIterator() override {
    if (arena_mode_) {
      iter_->~MemTableIterator();
    } else {
      delete iter_;
    }
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    iter_->SetPinnedItersMgr(pinned_iters_mgr);
  }

  bool Valid() const override { return iter_->Valid(); }
  void Seek(const Slice& k) override {
    iter_->Seek(k);
    UpdateKeyAndValueBuffer();
  }
  void SeekForPrev(const Slice& k) override {
    iter_->SeekForPrev(k);
    UpdateKeyAndValueBuffer();
  }
  void SeekToFirst() override {
    iter_->SeekToFirst();
    UpdateKeyAndValueBuffer();
  }
  void SeekToLast() override {
    iter_->SeekToLast();
    UpdateKeyAndValueBuffer();
  }
  void Next() override {
    iter_->Next();
    UpdateKeyAndValueBuffer();
  }
  bool NextAndGetResult(IterateResult* result) override {
    iter_->Next();
    UpdateKeyAndValueBuffer();
    bool is_valid = Valid();
    if (is_valid) {
      result->key = key();
      result->bound_check_result = IterBoundCheck::kUnknown;
      result->value_prepared = true;
    }
    return is_valid;
  }
  void Prev() override {
    iter_->Prev();
    UpdateKeyAndValueBuffer();
  }
  Slice key() const override {
    assert(Valid());
    return key_buf_;
  }

  uint64_t write_unix_time() const override { return iter_->write_unix_time(); }
  Slice value() const override {
    if (kind_ == MemTableIterator::Kind::kRangeDelEntries) {
      return value_buf_;
    }
    return iter_->value();
  }
  Status status() const override { return iter_->status(); }
  bool IsKeyPinned() const override {
    // Key is only in a buffer that is updated in each iteration.
    return false;
  }
  bool IsValuePinned() const override {
    if (kind_ == MemTableIterator::Kind::kRangeDelEntries) {
      return false;
    }
    return iter_->IsValuePinned();
  }

 private:
  void UpdateKeyAndValueBuffer() {
    key_buf_.clear();
    if (kind_ == MemTableIterator::Kind::kRangeDelEntries) {
      value_buf_.clear();
    }
    if (!Valid()) {
      return;
    }
    Slice original_key = iter_->key();
    ReplaceInternalKeyWithMinTimestamp(&key_buf_, original_key, ts_sz_);
    if (kind_ == MemTableIterator::Kind::kRangeDelEntries) {
      Slice original_value = iter_->value();
      AppendUserKeyWithMinTimestamp(&value_buf_, original_value, ts_sz_);
    }
  }
  bool arena_mode_;
  MemTableIterator::Kind kind_;
  size_t ts_sz_;
  MemTableIterator* iter_;
  std::string key_buf_;
  std::string value_buf_;
};

InternalIterator* MemTable::NewTimestampStrippingIterator(
    const ReadOptions& read_options,
    UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping, Arena* arena,
    const SliceTransform* prefix_extractor, size_t ts_sz) {
  assert(arena != nullptr);
  auto mem = arena->AllocateAligned(sizeof(TimestampStrippingIterator));
  return new (mem) TimestampStrippingIterator(
      MemTableIterator::kPointEntries, *this, read_options,
      seqno_to_time_mapping, arena, prefix_extractor, ts_sz);
}

FragmentedRangeTombstoneIterator* MemTable::NewRangeTombstoneIterator(
    const ReadOptions& read_options, SequenceNumber read_seq,
    bool immutable_memtable) {
  if (read_options.ignore_range_deletions ||
      is_range_del_table_empty_.LoadRelaxed()) {
    return nullptr;
  }
  return NewRangeTombstoneIteratorInternal(read_options, read_seq,
                                           immutable_memtable);
}

FragmentedRangeTombstoneIterator*
MemTable::NewTimestampStrippingRangeTombstoneIterator(
    const ReadOptions& read_options, SequenceNumber read_seq, size_t ts_sz) {
  if (read_options.ignore_range_deletions ||
      is_range_del_table_empty_.LoadRelaxed()) {
    return nullptr;
  }
  if (!timestamp_stripping_fragmented_range_tombstone_list_) {
    // TODO: plumb Env::IOActivity, Env::IOPriority
    auto* unfragmented_iter = new TimestampStrippingIterator(
        MemTableIterator::kRangeDelEntries, *this, ReadOptions(),
        /*seqno_to_time_mapping*/ nullptr, /* arena */ nullptr,
        /* prefix_extractor */ nullptr, ts_sz);

    timestamp_stripping_fragmented_range_tombstone_list_ =
        std::make_unique<FragmentedRangeTombstoneList>(
            std::unique_ptr<InternalIterator>(unfragmented_iter),
            comparator_.comparator);
  }
  return new FragmentedRangeTombstoneIterator(
      timestamp_stripping_fragmented_range_tombstone_list_.get(),
      comparator_.comparator, read_seq, read_options.timestamp);
}

FragmentedRangeTombstoneIterator* MemTable::NewRangeTombstoneIteratorInternal(
    const ReadOptions& read_options, SequenceNumber read_seq,
    bool immutable_memtable) {
  if (immutable_memtable) {
    // Note that caller should already have verified that
    // !is_range_del_table_empty_
    assert(IsFragmentedRangeTombstonesConstructed());
    return new FragmentedRangeTombstoneIterator(
        fragmented_range_tombstone_list_.get(), comparator_.comparator,
        read_seq, read_options.timestamp);
  }

  // takes current cache
  std::shared_ptr<FragmentedRangeTombstoneListCache> cache =
#if defined(__cpp_lib_atomic_shared_ptr)
      cached_range_tombstone_.Access()->load(std::memory_order_relaxed)
#else
      std::atomic_load_explicit(cached_range_tombstone_.Access(),
                                std::memory_order_relaxed)
#endif
      ;
  // construct fragmented tombstone list if necessary
  if (!cache->initialized.load(std::memory_order_acquire)) {
    cache->reader_mutex.lock();
    if (!cache->tombstones) {
      auto* unfragmented_iter = new MemTableIterator(
          MemTableIterator::kRangeDelEntries, *this, read_options);
      cache->tombstones.reset(new FragmentedRangeTombstoneList(
          std::unique_ptr<InternalIterator>(unfragmented_iter),
          comparator_.comparator));
      cache->initialized.store(true, std::memory_order_release);
    }
    cache->reader_mutex.unlock();
  }

  auto* fragmented_iter = new FragmentedRangeTombstoneIterator(
      cache, comparator_.comparator, read_seq, read_options.timestamp);
  return fragmented_iter;
}

void MemTable::ConstructFragmentedRangeTombstones() {
  // There should be no concurrent Construction.
  // We could also check fragmented_range_tombstone_list_ to avoid repeate
  // constructions. We just construct them here again to be safe.
  if (!is_range_del_table_empty_.LoadRelaxed()) {
    // TODO: plumb Env::IOActivity, Env::IOPriority
    auto* unfragmented_iter = new MemTableIterator(
        MemTableIterator::kRangeDelEntries, *this, ReadOptions());

    fragmented_range_tombstone_list_ =
        std::make_unique<FragmentedRangeTombstoneList>(
            std::unique_ptr<InternalIterator>(unfragmented_iter),
            comparator_.comparator);
  }
}

port::RWMutex* MemTable::GetLock(const Slice& key) {
  return &locks_[GetSliceRangedNPHash(key, locks_.size())];
}

ReadOnlyMemTable::MemTableStats MemTable::ApproximateStats(
    const Slice& start_ikey, const Slice& end_ikey) {
  uint64_t entry_count = table_->ApproximateNumEntries(start_ikey, end_ikey);
  entry_count += range_del_table_->ApproximateNumEntries(start_ikey, end_ikey);
  if (entry_count == 0) {
    return {0, 0};
  }
  uint64_t n = num_entries_.LoadRelaxed();
  if (n == 0) {
    return {0, 0};
  }
  if (entry_count > n) {
    // (range_del_)table_->ApproximateNumEntries() is just an estimate so it can
    // be larger than actual entries we have. Cap it to entries we have to limit
    // the inaccuracy.
    entry_count = n;
  }
  uint64_t data_size = data_size_.LoadRelaxed();
  return {entry_count * (data_size / n), entry_count};
}

Status MemTable::VerifyEncodedEntry(Slice encoded,
                                    const ProtectionInfoKVOS64& kv_prot_info) {
  uint32_t ikey_len = 0;
  if (!GetVarint32(&encoded, &ikey_len)) {
    return Status::Corruption("Unable to parse internal key length");
  }
  if (ikey_len < 8 + ts_sz_) {
    return Status::Corruption("Internal key length too short");
  }
  if (ikey_len > encoded.size()) {
    return Status::Corruption("Internal key length too long");
  }
  uint32_t value_len = 0;
  const size_t user_key_len = ikey_len - 8;
  Slice key(encoded.data(), user_key_len);
  encoded.remove_prefix(user_key_len);

  uint64_t packed = DecodeFixed64(encoded.data());
  ValueType value_type = kMaxValue;
  SequenceNumber sequence_number = kMaxSequenceNumber;
  UnPackSequenceAndType(packed, &sequence_number, &value_type);
  encoded.remove_prefix(8);

  if (!GetVarint32(&encoded, &value_len)) {
    return Status::Corruption("Unable to parse value length");
  }
  if (value_len < encoded.size()) {
    return Status::Corruption("Value length too short");
  }
  if (value_len > encoded.size()) {
    return Status::Corruption("Value length too long");
  }
  Slice value(encoded.data(), value_len);

  return kv_prot_info.StripS(sequence_number)
      .StripKVO(key, value, value_type)
      .GetStatus();
}

void MemTable::UpdateEntryChecksum(const ProtectionInfoKVOS64* kv_prot_info,
                                   const Slice& key, const Slice& value,
                                   ValueType type, SequenceNumber s,
                                   char* checksum_ptr) {
  if (moptions_.protection_bytes_per_key == 0) {
    return;
  }

  if (kv_prot_info == nullptr) {
    ProtectionInfo64()
        .ProtectKVO(key, value, type)
        .ProtectS(s)
        .Encode(static_cast<uint8_t>(moptions_.protection_bytes_per_key),
                checksum_ptr);
  } else {
    kv_prot_info->Encode(
        static_cast<uint8_t>(moptions_.protection_bytes_per_key), checksum_ptr);
  }
}

Status MemTable::Add(SequenceNumber s, ValueType type,
                     const Slice& key, /* user key */
                     const Slice& value,
                     const ProtectionInfoKVOS64* kv_prot_info,
                     bool allow_concurrent,
                     MemTablePostProcessInfo* post_process_info, void** hint) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  //  checksum     : char[moptions_.protection_bytes_per_key]
  uint32_t key_size = static_cast<uint32_t>(key.size());
  uint32_t val_size = static_cast<uint32_t>(value.size());
  uint32_t internal_key_size = key_size + 8;
  const uint32_t encoded_len = VarintLength(internal_key_size) +
                               internal_key_size + VarintLength(val_size) +
                               val_size + moptions_.protection_bytes_per_key;
  char* buf = nullptr;
  std::unique_ptr<MemTableRep>& table =
      type == kTypeRangeDeletion ? range_del_table_ : table_;
  KeyHandle handle = table->Allocate(encoded_len, &buf);

  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  Slice key_slice(p, key_size);
  p += key_size;
  uint64_t packed = PackSequenceAndType(s, type);
  EncodeFixed64(p, packed);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert((unsigned)(p + val_size - buf + moptions_.protection_bytes_per_key) ==
         (unsigned)encoded_len);

  UpdateEntryChecksum(kv_prot_info, key, value, type, s,
                      buf + encoded_len - moptions_.protection_bytes_per_key);
  Slice encoded(buf, encoded_len - moptions_.protection_bytes_per_key);
  if (kv_prot_info != nullptr) {
    TEST_SYNC_POINT_CALLBACK("MemTable::Add:Encoded", &encoded);
    Status status = VerifyEncodedEntry(encoded, *kv_prot_info);
    if (!status.ok()) {
      return status;
    }
  }

  Slice key_without_ts = StripTimestampFromUserKey(key, ts_sz_);

  if (!allow_concurrent) {
    // Extract prefix for insert with hint. Hints are for point key table
    // (`table_`) only, not `range_del_table_`.
    if (table == table_ && insert_with_hint_prefix_extractor_ != nullptr &&
        insert_with_hint_prefix_extractor_->InDomain(key_slice)) {
      Slice prefix = insert_with_hint_prefix_extractor_->Transform(key_slice);
      bool res = table->InsertKeyWithHint(handle, &insert_hints_[prefix]);
      if (UNLIKELY(!res)) {
        return Status::TryAgain("key+seq exists");
      }
    } else {
      bool res = table->InsertKey(handle);
      if (UNLIKELY(!res)) {
        return Status::TryAgain("key+seq exists");
      }
    }

    // this is a bit ugly, but is the way to avoid locked instructions
    // when incrementing an atomic
    num_entries_.StoreRelaxed(num_entries_.LoadRelaxed() + 1);
    data_size_.StoreRelaxed(data_size_.LoadRelaxed() + encoded_len);
    if (type == kTypeDeletion || type == kTypeSingleDeletion ||
        type == kTypeDeletionWithTimestamp) {
      num_deletes_.StoreRelaxed(num_deletes_.LoadRelaxed() + 1);
    } else if (type == kTypeRangeDeletion) {
      uint64_t val = num_range_deletes_.LoadRelaxed() + 1;
      num_range_deletes_.StoreRelaxed(val);
    }

    if (bloom_filter_ && prefix_extractor_ &&
        prefix_extractor_->InDomain(key_without_ts)) {
      bloom_filter_->Add(prefix_extractor_->Transform(key_without_ts));
    }
    if (bloom_filter_ && moptions_.memtable_whole_key_filtering) {
      bloom_filter_->Add(key_without_ts);
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
    // TODO(yuzhangyu): support updating newest UDT for when `allow_concurrent`
    // is true.
    MaybeUpdateNewestUDT(key_slice);
    UpdateFlushState();
  } else {
    bool res = (hint == nullptr)
                   ? table->InsertKeyConcurrently(handle)
                   : table->InsertKeyWithHintConcurrently(handle, hint);
    if (UNLIKELY(!res)) {
      return Status::TryAgain("key+seq exists");
    }

    assert(post_process_info != nullptr);
    post_process_info->num_entries++;
    post_process_info->data_size += encoded_len;
    if (type == kTypeDeletion) {
      post_process_info->num_deletes++;
    }

    if (bloom_filter_ && prefix_extractor_ &&
        prefix_extractor_->InDomain(key_without_ts)) {
      bloom_filter_->AddConcurrently(
          prefix_extractor_->Transform(key_without_ts));
    }
    if (bloom_filter_ && moptions_.memtable_whole_key_filtering) {
      bloom_filter_->AddConcurrently(key_without_ts);
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
        !earliest_seqno_.compare_exchange_weak(cur_earliest_seqno, s)) {
    }
  }
  if (type == kTypeRangeDeletion) {
    auto new_cache = std::make_shared<FragmentedRangeTombstoneListCache>();
    size_t size = cached_range_tombstone_.Size();
    if (allow_concurrent) {
      post_process_info->num_range_deletes++;
      range_del_mutex_.lock();
    }
    for (size_t i = 0; i < size; ++i) {
#if defined(__cpp_lib_atomic_shared_ptr)
      std::atomic<std::shared_ptr<FragmentedRangeTombstoneListCache>>*
          local_cache_ref_ptr = cached_range_tombstone_.AccessAtCore(i);
      auto new_local_cache_ref = std::make_shared<
          const std::shared_ptr<FragmentedRangeTombstoneListCache>>(new_cache);
      std::shared_ptr<FragmentedRangeTombstoneListCache> aliased_ptr(
          new_local_cache_ref, new_cache.get());
      local_cache_ref_ptr->store(std::move(aliased_ptr),
                                 std::memory_order_relaxed);
#else
      std::shared_ptr<FragmentedRangeTombstoneListCache>* local_cache_ref_ptr =
          cached_range_tombstone_.AccessAtCore(i);
      auto new_local_cache_ref = std::make_shared<
          const std::shared_ptr<FragmentedRangeTombstoneListCache>>(new_cache);
      // It is okay for some reader to load old cache during invalidation as
      // the new sequence number is not published yet.
      // Each core will have a shared_ptr to a shared_ptr to the cached
      // fragmented range tombstones, so that ref count is maintianed locally
      // per-core using the per-core shared_ptr.
      std::atomic_store_explicit(
          local_cache_ref_ptr,
          std::shared_ptr<FragmentedRangeTombstoneListCache>(
              new_local_cache_ref, new_cache.get()),
          std::memory_order_relaxed);
#endif
    }

    if (allow_concurrent) {
      range_del_mutex_.unlock();
    }
    is_range_del_table_empty_.StoreRelaxed(false);
  }
  UpdateOldestKeyTime();

  TEST_SYNC_POINT_CALLBACK("MemTable::Add:BeforeReturn:Encoded", &encoded);
  return Status::OK();
}

// Callback from MemTable::Get()
namespace {

struct Saver {
  Status* status;
  const LookupKey* key;
  bool* found_final_value;  // Is value set correctly? Used by KeyMayExist
  bool* merge_in_progress;
  std::string* value;
  PinnableWideColumns* columns;
  SequenceNumber seq;
  std::string* timestamp;
  const MergeOperator* merge_operator;
  // the merge operations encountered;
  MergeContext* merge_context;
  SequenceNumber max_covering_tombstone_seq;
  MemTable* mem;
  Logger* logger;
  Statistics* statistics;
  bool inplace_update_support;
  bool do_merge;
  SystemClock* clock;

  ReadCallback* callback_;
  bool* is_blob_index;
  bool allow_data_in_errors;
  uint32_t protection_bytes_per_key;
  bool CheckCallback(SequenceNumber _seq) {
    if (callback_) {
      return callback_->IsVisible(_seq);
    }
    return true;
  }
};
}  // anonymous namespace

static bool SaveValue(void* arg, const char* entry) {
  Saver* s = static_cast<Saver*>(arg);
  assert(s != nullptr);
  assert(!s->value || !s->columns);
  assert(!*(s->found_final_value));
  assert(s->status->ok() || s->status->IsMergeInProgress());

  MergeContext* merge_context = s->merge_context;
  SequenceNumber max_covering_tombstone_seq = s->max_covering_tombstone_seq;
  const MergeOperator* merge_operator = s->merge_operator;

  assert(merge_context != nullptr);

  // Refer to comments under MemTable::Add() for entry format.
  // Check that it belongs to same user key.
  uint32_t key_length = 0;
  const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
  assert(key_length >= 8);
  Slice user_key_slice = Slice(key_ptr, key_length - 8);
  const Comparator* user_comparator =
      s->mem->GetInternalKeyComparator().user_comparator();
  size_t ts_sz = user_comparator->timestamp_size();
  if (ts_sz && s->timestamp && max_covering_tombstone_seq > 0) {
    // timestamp should already be set to range tombstone timestamp
    assert(s->timestamp->size() == ts_sz);
  }
  if (user_comparator->EqualWithoutTimestamp(user_key_slice,
                                             s->key->user_key())) {
    // Correct user key
    TEST_SYNC_POINT_CALLBACK("Memtable::SaveValue:Found:entry", &entry);
    std::optional<ReadLock> read_lock;
    if (s->inplace_update_support) {
      read_lock.emplace(s->mem->GetLock(s->key->user_key()));
    }

    if (s->protection_bytes_per_key > 0) {
      *(s->status) = MemTable::VerifyEntryChecksum(
          entry, s->protection_bytes_per_key, s->allow_data_in_errors);
      if (!s->status->ok()) {
        *(s->found_final_value) = true;
        ROCKS_LOG_ERROR(s->logger, "In SaveValue: %s", s->status->getState());
        // Memtable entry corrupted
        return false;
      }
    }

    const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
    ValueType type;
    SequenceNumber seq;
    UnPackSequenceAndType(tag, &seq, &type);
    // If the value is not in the snapshot, skip it
    if (!s->CheckCallback(seq)) {
      return true;  // to continue to the next seq
    }

    if (s->seq == kMaxSequenceNumber) {
      s->seq = seq;
      if (s->seq > max_covering_tombstone_seq) {
        if (ts_sz && s->timestamp != nullptr) {
          // `timestamp` was set to range tombstone's timestamp before
          // `SaveValue` is ever called. This key has a higher sequence number
          // than range tombstone, and is the key with the highest seqno across
          // all keys with this user_key, so we update timestamp here.
          Slice ts = ExtractTimestampFromUserKey(user_key_slice, ts_sz);
          s->timestamp->assign(ts.data(), ts_sz);
        }
      } else {
        s->seq = max_covering_tombstone_seq;
      }
    }

    if (ts_sz > 0 && s->timestamp != nullptr) {
      if (!s->timestamp->empty()) {
        assert(ts_sz == s->timestamp->size());
      }
      // TODO optimize for smaller size ts
      const std::string kMaxTs(ts_sz, '\xff');
      if (s->timestamp->empty() ||
          user_comparator->CompareTimestamp(*(s->timestamp), kMaxTs) == 0) {
        Slice ts = ExtractTimestampFromUserKey(user_key_slice, ts_sz);
        s->timestamp->assign(ts.data(), ts_sz);
      }
    }

    if ((type == kTypeValue || type == kTypeMerge || type == kTypeBlobIndex ||
         type == kTypeWideColumnEntity || type == kTypeDeletion ||
         type == kTypeSingleDeletion || type == kTypeDeletionWithTimestamp ||
         type == kTypeValuePreferredSeqno) &&
        max_covering_tombstone_seq > seq) {
      type = kTypeRangeDeletion;
    }
    switch (type) {
      case kTypeBlobIndex: {
        if (!s->do_merge) {
          *(s->status) = Status::NotSupported(
              "GetMergeOperands not supported by stacked BlobDB");
          *(s->found_final_value) = true;
          return false;
        }

        if (*(s->merge_in_progress)) {
          *(s->status) = Status::NotSupported(
              "Merge operator not supported by stacked BlobDB");
          *(s->found_final_value) = true;
          return false;
        }

        if (s->is_blob_index == nullptr) {
          ROCKS_LOG_ERROR(s->logger, "Encountered unexpected blob index.");
          *(s->status) = Status::NotSupported(
              "Encountered unexpected blob index. Please open DB with "
              "ROCKSDB_NAMESPACE::blob_db::BlobDB.");
          *(s->found_final_value) = true;
          return false;
        }

        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);

        *(s->status) = Status::OK();

        if (s->value) {
          s->value->assign(v.data(), v.size());
        } else if (s->columns) {
          s->columns->SetPlainValue(v);
        }

        *(s->found_final_value) = true;
        *(s->is_blob_index) = true;

        return false;
      }
      case kTypeValue:
      case kTypeValuePreferredSeqno: {
        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
        if (type == kTypeValuePreferredSeqno) {
          v = ParsePackedValueForValue(v);
        }

        ReadOnlyMemTable::HandleTypeValue(
            s->key->user_key(), v, s->inplace_update_support == false,
            s->do_merge, *(s->merge_in_progress), merge_context,
            s->merge_operator, s->clock, s->statistics, s->logger, s->status,
            s->value, s->columns, s->is_blob_index);
        *(s->found_final_value) = true;
        return false;
      }
      case kTypeWideColumnEntity: {
        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);

        *(s->status) = Status::OK();

        if (!s->do_merge) {
          // Preserve the value with the goal of returning it as part of
          // raw merge operands to the user

          Slice value_of_default;
          *(s->status) = WideColumnSerialization::GetValueOfDefaultColumn(
              v, value_of_default);

          if (s->status->ok()) {
            merge_context->PushOperand(
                value_of_default,
                s->inplace_update_support == false /* operand_pinned */);
          }
        } else if (*(s->merge_in_progress)) {
          assert(s->do_merge);

          if (s->value || s->columns) {
            // `op_failure_scope` (an output parameter) is not provided (set
            // to nullptr) since a failure must be propagated regardless of
            // its value.
            *(s->status) = MergeHelper::TimedFullMerge(
                merge_operator, s->key->user_key(), MergeHelper::kWideBaseValue,
                v, merge_context->GetOperands(), s->logger, s->statistics,
                s->clock, /* update_num_ops_stats */ true,
                /* op_failure_scope */ nullptr, s->value, s->columns);
          }
        } else if (s->value) {
          Slice value_of_default;
          *(s->status) = WideColumnSerialization::GetValueOfDefaultColumn(
              v, value_of_default);
          if (s->status->ok()) {
            s->value->assign(value_of_default.data(), value_of_default.size());
          }
        } else if (s->columns) {
          *(s->status) = s->columns->SetWideColumnValue(v);
        }

        *(s->found_final_value) = true;

        if (s->is_blob_index != nullptr) {
          *(s->is_blob_index) = false;
        }

        return false;
      }
      case kTypeDeletion:
      case kTypeDeletionWithTimestamp:
      case kTypeSingleDeletion:
      case kTypeRangeDeletion: {
        ReadOnlyMemTable::HandleTypeDeletion(
            s->key->user_key(), *(s->merge_in_progress), s->merge_context,
            s->merge_operator, s->clock, s->statistics, s->logger, s->status,
            s->value, s->columns);
        *(s->found_final_value) = true;
        return false;
      }
      case kTypeMerge: {
        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
        *(s->merge_in_progress) = true;
        *(s->found_final_value) = ReadOnlyMemTable::HandleTypeMerge(
            s->key->user_key(), v, s->inplace_update_support == false,
            s->do_merge, merge_context, s->merge_operator, s->clock,
            s->statistics, s->logger, s->status, s->value, s->columns);
        return !*(s->found_final_value);
      }
      default: {
        std::string msg("Corrupted value not expected.");
        if (s->allow_data_in_errors) {
          msg.append("Unrecognized value type: " +
                     std::to_string(static_cast<int>(type)) + ". ");
          msg.append("User key: " + user_key_slice.ToString(/*hex=*/true) +
                     ". ");
          msg.append("seq: " + std::to_string(seq) + ".");
        }
        *(s->found_final_value) = true;
        *(s->status) = Status::Corruption(msg.c_str());
        return false;
      }
    }
  }

  // s->state could be Corrupt, merge or notfound
  return false;
}

bool MemTable::Get(const LookupKey& key, std::string* value,
                   PinnableWideColumns* columns, std::string* timestamp,
                   Status* s, MergeContext* merge_context,
                   SequenceNumber* max_covering_tombstone_seq,
                   SequenceNumber* seq, const ReadOptions& read_opts,
                   bool immutable_memtable, ReadCallback* callback,
                   bool* is_blob_index, bool do_merge) {
  // The sequence number is updated synchronously in version_set.h
  if (IsEmpty()) {
    // Avoiding recording stats for speed.
    return false;
  }

  PERF_TIMER_GUARD(get_from_memtable_time);

  std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
      NewRangeTombstoneIterator(read_opts,
                                GetInternalKeySeqno(key.internal_key()),
                                immutable_memtable));
  if (range_del_iter != nullptr) {
    SequenceNumber covering_seq =
        range_del_iter->MaxCoveringTombstoneSeqnum(key.user_key());
    if (covering_seq > *max_covering_tombstone_seq) {
      *max_covering_tombstone_seq = covering_seq;
      if (timestamp) {
        // Will be overwritten in SaveValue() if there is a point key with
        // a higher seqno.
        timestamp->assign(range_del_iter->timestamp().data(),
                          range_del_iter->timestamp().size());
      }
    }
  }

  bool found_final_value = false;
  bool merge_in_progress = s->IsMergeInProgress();
  bool may_contain = true;
  Slice user_key_without_ts = StripTimestampFromUserKey(key.user_key(), ts_sz_);
  bool bloom_checked = false;
  if (bloom_filter_) {
    // when both memtable_whole_key_filtering and prefix_extractor_ are set,
    // only do whole key filtering for Get() to save CPU
    if (moptions_.memtable_whole_key_filtering) {
      may_contain = bloom_filter_->MayContain(user_key_without_ts);
      bloom_checked = true;
    } else {
      assert(prefix_extractor_);
      if (prefix_extractor_->InDomain(user_key_without_ts)) {
        may_contain = bloom_filter_->MayContain(
            prefix_extractor_->Transform(user_key_without_ts));
        bloom_checked = true;
      }
    }
  }

  if (bloom_filter_ && !may_contain) {
    // iter is null if prefix bloom says the key does not exist
    PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
    *seq = kMaxSequenceNumber;
  } else {
    if (bloom_checked) {
      PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
    }
    GetFromTable(key, *max_covering_tombstone_seq, do_merge, callback,
                 is_blob_index, value, columns, timestamp, s, merge_context,
                 seq, &found_final_value, &merge_in_progress);
  }

  // No change to value, since we have not yet found a Put/Delete
  // Propagate corruption error
  if (!found_final_value && merge_in_progress) {
    if (s->ok()) {
      *s = Status::MergeInProgress();
    } else {
      assert(s->IsMergeInProgress());
    }
  }
  PERF_COUNTER_ADD(get_from_memtable_count, 1);
  return found_final_value;
}

void MemTable::GetFromTable(const LookupKey& key,
                            SequenceNumber max_covering_tombstone_seq,
                            bool do_merge, ReadCallback* callback,
                            bool* is_blob_index, std::string* value,
                            PinnableWideColumns* columns,
                            std::string* timestamp, Status* s,
                            MergeContext* merge_context, SequenceNumber* seq,
                            bool* found_final_value, bool* merge_in_progress) {
  Saver saver;
  saver.status = s;
  saver.found_final_value = found_final_value;
  saver.merge_in_progress = merge_in_progress;
  saver.key = &key;
  saver.value = value;
  saver.columns = columns;
  saver.timestamp = timestamp;
  saver.seq = kMaxSequenceNumber;
  saver.mem = this;
  saver.merge_context = merge_context;
  saver.max_covering_tombstone_seq = max_covering_tombstone_seq;
  saver.merge_operator = moptions_.merge_operator;
  saver.logger = moptions_.info_log;
  saver.inplace_update_support = moptions_.inplace_update_support;
  saver.statistics = moptions_.statistics;
  saver.clock = clock_;
  saver.callback_ = callback;
  saver.is_blob_index = is_blob_index;
  saver.do_merge = do_merge;
  saver.allow_data_in_errors = moptions_.allow_data_in_errors;
  saver.protection_bytes_per_key = moptions_.protection_bytes_per_key;

  if (!moptions_.paranoid_memory_checks &&
      !moptions_.memtable_veirfy_per_key_checksum_on_seek) {
    table_->Get(key, &saver, SaveValue);
  } else {
    Status check_s = table_->GetAndValidate(
        key, &saver, SaveValue, moptions_.allow_data_in_errors,
        moptions_.paranoid_memory_checks, key_validation_callback_);
    if (check_s.IsCorruption()) {
      *(saver.status) = check_s;
      // Should stop searching the LSM.
      *(saver.found_final_value) = true;
    }
  }
  assert(s->ok() || s->IsMergeInProgress() || *found_final_value);
  *seq = saver.seq;
}

Status MemTable::ValidateKey(const char* key, bool allow_data_in_errors) {
  return VerifyEntryChecksum(key, moptions_.protection_bytes_per_key,
                             allow_data_in_errors);
}

void MemTable::MultiGet(const ReadOptions& read_options, MultiGetRange* range,
                        ReadCallback* callback, bool immutable_memtable) {
  // The sequence number is updated synchronously in version_set.h
  if (IsEmpty()) {
    // Avoiding recording stats for speed.
    return;
  }
  PERF_TIMER_GUARD(get_from_memtable_time);

  // For now, memtable Bloom filter is effectively disabled if there are any
  // range tombstones. This is the simplest way to ensure range tombstones are
  // handled. TODO: allow Bloom checks where max_covering_tombstone_seq==0
  bool no_range_del = read_options.ignore_range_deletions ||
                      is_range_del_table_empty_.LoadRelaxed();
  MultiGetRange temp_range(*range, range->begin(), range->end());
  if (bloom_filter_ && no_range_del) {
    bool whole_key =
        !prefix_extractor_ || moptions_.memtable_whole_key_filtering;
    std::array<Slice, MultiGetContext::MAX_BATCH_SIZE> bloom_keys;
    std::array<bool, MultiGetContext::MAX_BATCH_SIZE> may_match;
    std::array<size_t, MultiGetContext::MAX_BATCH_SIZE> range_indexes;
    int num_keys = 0;
    for (auto iter = temp_range.begin(); iter != temp_range.end(); ++iter) {
      if (whole_key) {
        bloom_keys[num_keys] = iter->ukey_without_ts;
        range_indexes[num_keys++] = iter.index();
      } else if (prefix_extractor_->InDomain(iter->ukey_without_ts)) {
        bloom_keys[num_keys] =
            prefix_extractor_->Transform(iter->ukey_without_ts);
        range_indexes[num_keys++] = iter.index();
      }
    }
    bloom_filter_->MayContain(num_keys, bloom_keys.data(), may_match.data());
    for (int i = 0; i < num_keys; ++i) {
      if (!may_match[i]) {
        temp_range.SkipIndex(range_indexes[i]);
        PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
      } else {
        PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
      }
    }
  }
  for (auto iter = temp_range.begin(); iter != temp_range.end(); ++iter) {
    bool found_final_value{false};
    bool merge_in_progress = iter->s->IsMergeInProgress();
    if (!no_range_del) {
      std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
          NewRangeTombstoneIteratorInternal(
              read_options, GetInternalKeySeqno(iter->lkey->internal_key()),
              immutable_memtable));
      SequenceNumber covering_seq =
          range_del_iter->MaxCoveringTombstoneSeqnum(iter->lkey->user_key());
      if (covering_seq > iter->max_covering_tombstone_seq) {
        iter->max_covering_tombstone_seq = covering_seq;
        if (iter->timestamp) {
          // Will be overwritten in SaveValue() if there is a point key with
          // a higher seqno.
          iter->timestamp->assign(range_del_iter->timestamp().data(),
                                  range_del_iter->timestamp().size());
        }
      }
    }
    SequenceNumber dummy_seq;
    GetFromTable(*(iter->lkey), iter->max_covering_tombstone_seq, true,
                 callback, &iter->is_blob_index,
                 iter->value ? iter->value->GetSelf() : nullptr, iter->columns,
                 iter->timestamp, iter->s, &(iter->merge_context), &dummy_seq,
                 &found_final_value, &merge_in_progress);

    if (!found_final_value && merge_in_progress) {
      if (iter->s->ok()) {
        *(iter->s) = Status::MergeInProgress();
      } else {
        assert(iter->s->IsMergeInProgress());
      }
    }

    if (found_final_value ||
        (!iter->s->ok() && !iter->s->IsMergeInProgress())) {
      // `found_final_value` should be set if an error/corruption occurs.
      // The check on iter->s is just there in case GetFromTable() did not
      // set `found_final_value` properly.
      assert(found_final_value);
      if (iter->value) {
        iter->value->PinSelf();
        range->AddValueSize(iter->value->size());
      } else {
        assert(iter->columns);
        range->AddValueSize(iter->columns->serialized_size());
      }

      range->MarkKeyDone(iter);
      RecordTick(moptions_.statistics, MEMTABLE_HIT);
      if (range->GetValueSize() > read_options.value_size_soft_limit) {
        // Set all remaining keys in range to Abort
        for (auto range_iter = range->begin(); range_iter != range->end();
             ++range_iter) {
          range->MarkKeyDone(range_iter);
          *(range_iter->s) = Status::Aborted();
        }
        break;
      }
    }
  }
  PERF_COUNTER_ADD(get_from_memtable_count, 1);
}

Status MemTable::Update(SequenceNumber seq, ValueType value_type,
                        const Slice& key, const Slice& value,
                        const ProtectionInfoKVOS64* kv_prot_info) {
  LookupKey lkey(key, seq);
  Slice mem_key = lkey.memtable_key();

  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetDynamicPrefixIterator());
  iter->Seek(lkey.internal_key(), mem_key.data());

  if (iter->Valid()) {
    // Refer to comments under MemTable::Add() for entry format.
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter->key();
    uint32_t key_length = 0;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Equal(
            Slice(key_ptr, key_length - 8), lkey.user_key())) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      ValueType type;
      SequenceNumber existing_seq;
      UnPackSequenceAndType(tag, &existing_seq, &type);
      assert(existing_seq != seq);
      if (type == value_type) {
        Slice prev_value = GetLengthPrefixedSlice(key_ptr + key_length);
        uint32_t prev_size = static_cast<uint32_t>(prev_value.size());
        uint32_t new_size = static_cast<uint32_t>(value.size());

        // Update value, if new value size  <= previous value size
        if (new_size <= prev_size) {
          WriteLock wl(GetLock(lkey.user_key()));
          char* p =
              EncodeVarint32(const_cast<char*>(key_ptr) + key_length, new_size);
          memcpy(p, value.data(), value.size());
          assert((unsigned)((p + value.size()) - entry) ==
                 (unsigned)(VarintLength(key_length) + key_length +
                            VarintLength(value.size()) + value.size()));
          RecordTick(moptions_.statistics, NUMBER_KEYS_UPDATED);
          if (kv_prot_info != nullptr) {
            ProtectionInfoKVOS64 updated_kv_prot_info(*kv_prot_info);
            // `seq` is swallowed and `existing_seq` prevails.
            updated_kv_prot_info.UpdateS(seq, existing_seq);
            UpdateEntryChecksum(&updated_kv_prot_info, key, value, type,
                                existing_seq, p + value.size());
            Slice encoded(entry, p + value.size() - entry);
            return VerifyEncodedEntry(encoded, updated_kv_prot_info);
          } else {
            UpdateEntryChecksum(nullptr, key, value, type, existing_seq,
                                p + value.size());
          }
          return Status::OK();
        }
      }
    }
  }

  // The latest value is not value_type or key doesn't exist
  return Add(seq, value_type, key, value, kv_prot_info);
}

Status MemTable::UpdateCallback(SequenceNumber seq, const Slice& key,
                                const Slice& delta,
                                const ProtectionInfoKVOS64* kv_prot_info) {
  LookupKey lkey(key, seq);
  Slice memkey = lkey.memtable_key();

  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetDynamicPrefixIterator());
  iter->Seek(lkey.internal_key(), memkey.data());

  if (iter->Valid()) {
    // Refer to comments under MemTable::Add() for entry format.
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter->key();
    uint32_t key_length = 0;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Equal(
            Slice(key_ptr, key_length - 8), lkey.user_key())) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      ValueType type;
      uint64_t existing_seq;
      UnPackSequenceAndType(tag, &existing_seq, &type);
      if (type == kTypeValue) {
        Slice prev_value = GetLengthPrefixedSlice(key_ptr + key_length);
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
            char* p = EncodeVarint32(const_cast<char*>(key_ptr) + key_length,
                                     new_prev_size);
            if (VarintLength(new_prev_size) < VarintLength(prev_size)) {
              // shift the value buffer as well.
              memcpy(p, prev_buffer, new_prev_size);
              prev_buffer = p;
            }
          }
          RecordTick(moptions_.statistics, NUMBER_KEYS_UPDATED);
          UpdateFlushState();
          Slice new_value(prev_buffer, new_prev_size);
          if (kv_prot_info != nullptr) {
            ProtectionInfoKVOS64 updated_kv_prot_info(*kv_prot_info);
            // `seq` is swallowed and `existing_seq` prevails.
            updated_kv_prot_info.UpdateS(seq, existing_seq);
            updated_kv_prot_info.UpdateV(delta, new_value);
            Slice encoded(entry, prev_buffer + new_prev_size - entry);
            UpdateEntryChecksum(&updated_kv_prot_info, key, new_value, type,
                                existing_seq, prev_buffer + new_prev_size);
            return VerifyEncodedEntry(encoded, updated_kv_prot_info);
          } else {
            UpdateEntryChecksum(nullptr, key, new_value, type, existing_seq,
                                prev_buffer + new_prev_size);
          }
          return Status::OK();
        } else if (status == UpdateStatus::UPDATED) {
          Status s;
          if (kv_prot_info != nullptr) {
            ProtectionInfoKVOS64 updated_kv_prot_info(*kv_prot_info);
            updated_kv_prot_info.UpdateV(delta, str_value);
            s = Add(seq, kTypeValue, key, Slice(str_value),
                    &updated_kv_prot_info);
          } else {
            s = Add(seq, kTypeValue, key, Slice(str_value),
                    nullptr /* kv_prot_info */);
          }
          RecordTick(moptions_.statistics, NUMBER_KEYS_WRITTEN);
          UpdateFlushState();
          return s;
        } else if (status == UpdateStatus::UPDATE_FAILED) {
          // `UPDATE_FAILED` is named incorrectly. It indicates no update
          // happened. It does not indicate a failure happened.
          UpdateFlushState();
          return Status::OK();
        }
      }
    }
  }
  // The latest value is not `kTypeValue` or key doesn't exist
  return Status::NotFound();
}

size_t MemTable::CountSuccessiveMergeEntries(const LookupKey& key,
                                             size_t limit) {
  Slice memkey = key.memtable_key();

  // A total ordered iterator is costly for some memtablerep (prefix aware
  // reps). By passing in the user key, we allow efficient iterator creation.
  // The iterator only needs to be ordered within the same user key.
  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetDynamicPrefixIterator());
  iter->Seek(key.internal_key(), memkey.data());

  size_t num_successive_merges = 0;

  for (; iter->Valid() && num_successive_merges < limit; iter->Next()) {
    const char* entry = iter->key();
    uint32_t key_length = 0;
    const char* iter_key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (!comparator_.comparator.user_comparator()->Equal(
            Slice(iter_key_ptr, key_length - 8), key.user_key())) {
      break;
    }

    const uint64_t tag = DecodeFixed64(iter_key_ptr + key_length - 8);
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
                      bool (*callback_func)(void* arg, const char* entry)) {
  auto iter = GetDynamicPrefixIterator();
  for (iter->Seek(k.internal_key(), k.memtable_key().data());
       iter->Valid() && callback_func(callback_args, iter->key());
       iter->Next()) {
  }
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

void MemTable::MaybeUpdateNewestUDT(const Slice& user_key) {
  if (ts_sz_ == 0) {
    return;
  }
  const Comparator* ucmp = GetInternalKeyComparator().user_comparator();
  Slice udt = ExtractTimestampFromUserKey(user_key, ts_sz_);
  if (newest_udt_.empty() || ucmp->CompareTimestamp(udt, newest_udt_) > 0) {
    newest_udt_ = udt;
  }
}

const Slice& MemTable::GetNewestUDT() const {
  assert(ts_sz_ > 0);
  return newest_udt_;
}

}  // namespace ROCKSDB_NAMESPACE
