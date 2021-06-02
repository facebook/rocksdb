//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <algorithm>
#include <array>
#include <string>

#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "db/merge_context.h"
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/types.h"
#include "util/autovector.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {
class GetContext;

struct KeyContext {
  const Slice* key;
  LookupKey* lkey;
  Slice ukey_with_ts;
  Slice ukey_without_ts;
  Slice ikey;
  ColumnFamilyHandle* column_family;
  Status* s;
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq;
  bool key_exists;
  bool is_blob_index;
  void* cb_arg;
  PinnableSlice* value;
  std::string* timestamp;
  GetContext* get_context;

  KeyContext(ColumnFamilyHandle* col_family, const Slice& user_key,
             PinnableSlice* val, std::string* ts, Status* stat)
      : key(&user_key),
        lkey(nullptr),
        column_family(col_family),
        s(stat),
        max_covering_tombstone_seq(0),
        key_exists(false),
        is_blob_index(false),
        cb_arg(nullptr),
        value(val),
        timestamp(ts),
        get_context(nullptr) {}

  KeyContext() = default;
};

// The MultiGetContext class is a container for the sorted list of keys that
// we need to lookup in a batch. Its main purpose is to make batch execution
// easier by allowing various stages of the MultiGet lookups to operate on
// subsets of keys, potentially non-contiguous. In order to accomplish this,
// it defines the following classes -
//
// MultiGetContext::Range
// MultiGetContext::Range::Iterator
// MultiGetContext::Range::IteratorWrapper
//
// Here is an example of how this can be used -
//
// {
//    MultiGetContext ctx(...);
//    MultiGetContext::Range range = ctx.GetMultiGetRange();
//
//    // Iterate to determine some subset of the keys
//    MultiGetContext::Range::Iterator start = range.begin();
//    MultiGetContext::Range::Iterator end = ...;
//
//    // Make a new range with a subset of keys
//    MultiGetContext::Range subrange(range, start, end);
//
//    // Define an auxillary vector, if needed, to hold additional data for
//    // each key
//    std::array<Foo, MultiGetContext::MAX_BATCH_SIZE> aux;
//
//    // Iterate over the subrange and the auxillary vector simultaneously
//    MultiGetContext::Range::Iterator iter = subrange.begin();
//    for (; iter != subrange.end(); ++iter) {
//      KeyContext& key = *iter;
//      Foo& aux_key = aux_iter[iter.index()];
//      ...
//    }
//  }
class MultiGetContext {
 public:
  // Limit the number of keys in a batch to this number. Benchmarks show that
  // there is negligible benefit for batches exceeding this. Keeping this < 32
  // simplifies iteration, as well as reduces the amount of stack allocations
  // that need to be performed
  static const int MAX_BATCH_SIZE = 32;

  MultiGetContext(autovector<KeyContext*, MAX_BATCH_SIZE>* sorted_keys,
                  size_t begin, size_t num_keys, SequenceNumber snapshot,
                  const ReadOptions& read_opts)
      : num_keys_(num_keys),
        value_mask_(0),
        value_size_(0),
        lookup_key_ptr_(reinterpret_cast<LookupKey*>(lookup_key_stack_buf)) {
    if (num_keys > MAX_LOOKUP_KEYS_ON_STACK) {
      lookup_key_heap_buf.reset(new char[sizeof(LookupKey) * num_keys]);
      lookup_key_ptr_ = reinterpret_cast<LookupKey*>(
          lookup_key_heap_buf.get());
    }

    for (size_t iter = 0; iter != num_keys_; ++iter) {
      // autovector may not be contiguous storage, so make a copy
      sorted_keys_[iter] = (*sorted_keys)[begin + iter];
      sorted_keys_[iter]->lkey = new (&lookup_key_ptr_[iter])
          LookupKey(*sorted_keys_[iter]->key, snapshot, read_opts.timestamp);
      sorted_keys_[iter]->ukey_with_ts = sorted_keys_[iter]->lkey->user_key();
      sorted_keys_[iter]->ukey_without_ts = StripTimestampFromUserKey(
          sorted_keys_[iter]->lkey->user_key(),
          read_opts.timestamp == nullptr ? 0 : read_opts.timestamp->size());
      sorted_keys_[iter]->ikey = sorted_keys_[iter]->lkey->internal_key();
    }
  }

  ~MultiGetContext() {
    for (size_t i = 0; i < num_keys_; ++i) {
      lookup_key_ptr_[i].~LookupKey();
    }
  }

 private:
  static const int MAX_LOOKUP_KEYS_ON_STACK = 16;
  alignas(alignof(LookupKey))
    char lookup_key_stack_buf[sizeof(LookupKey) * MAX_LOOKUP_KEYS_ON_STACK];
  std::array<KeyContext*, MAX_BATCH_SIZE> sorted_keys_;
  size_t num_keys_;
  uint64_t value_mask_;
  uint64_t value_size_;
  std::unique_ptr<char[]> lookup_key_heap_buf;
  LookupKey* lookup_key_ptr_;

 public:
  // MultiGetContext::Range - Specifies a range of keys, by start and end index,
  // from the parent MultiGetContext. Each range contains a bit vector that
  // indicates whether the corresponding keys need to be processed or skipped.
  // A Range object can be copy constructed, and the new object inherits the
  // original Range's bit vector. This is useful for progressively skipping
  // keys as the lookup goes through various stages. For example, when looking
  // up keys in the same SST file, a Range is created excluding keys not
  // belonging to that file. A new Range is then copy constructed and individual
  // keys are skipped based on bloom filter lookup.
  class Range {
   public:
    // MultiGetContext::Range::Iterator - A forward iterator that iterates over
    // non-skippable keys in a Range, as well as keys whose final value has been
    // found. The latter is tracked by MultiGetContext::value_mask_
    class Iterator {
     public:
      // -- iterator traits
      typedef Iterator self_type;
      typedef KeyContext value_type;
      typedef KeyContext& reference;
      typedef KeyContext* pointer;
      typedef int difference_type;
      typedef std::forward_iterator_tag iterator_category;

      Iterator(const Range* range, size_t idx)
          : range_(range), ctx_(range->ctx_), index_(idx) {
        while (index_ < range_->end_ &&
               (uint64_t{1} << index_) &
                   (range_->ctx_->value_mask_ | range_->skip_mask_))
          index_++;
      }

      Iterator(const Iterator&) = default;
      Iterator& operator=(const Iterator&) = default;

      Iterator& operator++() {
        while (++index_ < range_->end_ &&
               (uint64_t{1} << index_) &
                   (range_->ctx_->value_mask_ | range_->skip_mask_))
          ;
        return *this;
      }

      bool operator==(Iterator other) const {
        assert(range_->ctx_ == other.range_->ctx_);
        return index_ == other.index_;
      }

      bool operator!=(Iterator other) const {
        assert(range_->ctx_ == other.range_->ctx_);
        return index_ != other.index_;
      }

      KeyContext& operator*() {
        assert(index_ < range_->end_ && index_ >= range_->start_);
        return *(ctx_->sorted_keys_[index_]);
      }

      KeyContext* operator->() {
        assert(index_ < range_->end_ && index_ >= range_->start_);
        return ctx_->sorted_keys_[index_];
      }

      size_t index() { return index_; }

     private:
      friend Range;
      const Range* range_;
      const MultiGetContext* ctx_;
      size_t index_;
    };

    Range(const Range& mget_range,
          const Iterator& first,
          const Iterator& last) {
      ctx_ = mget_range.ctx_;
      start_ = first.index_;
      end_ = last.index_;
      skip_mask_ = mget_range.skip_mask_;
      assert(start_ < 64);
      assert(end_ < 64);
    }

    Range() = default;

    Iterator begin() const { return Iterator(this, start_); }

    Iterator end() const { return Iterator(this, end_); }

    bool empty() const { return RemainingMask() == 0; }

    void SkipKey(const Iterator& iter) {
      skip_mask_ |= uint64_t{1} << iter.index_;
    }

    // Update the value_mask_ in MultiGetContext so its
    // immediately reflected in all the Range Iterators
    void MarkKeyDone(Iterator& iter) {
      ctx_->value_mask_ |= (uint64_t{1} << iter.index_);
    }

    bool CheckKeyDone(Iterator& iter) const {
      return ctx_->value_mask_ & (uint64_t{1} << iter.index_);
    }

    uint64_t KeysLeft() const { return BitsSetToOne(RemainingMask()); }

    void AddSkipsFrom(const Range& other) {
      assert(ctx_ == other.ctx_);
      skip_mask_ |= other.skip_mask_;
    }

    uint64_t GetValueSize() { return ctx_->value_size_; }

    void AddValueSize(uint64_t value_size) { ctx_->value_size_ += value_size; }

   private:
    friend MultiGetContext;
    MultiGetContext* ctx_;
    size_t start_;
    size_t end_;
    uint64_t skip_mask_;

    Range(MultiGetContext* ctx, size_t num_keys)
        : ctx_(ctx), start_(0), end_(num_keys), skip_mask_(0) {
      assert(num_keys < 64);
    }

    uint64_t RemainingMask() const {
      return (((uint64_t{1} << end_) - 1) & ~((uint64_t{1} << start_) - 1) &
              ~(ctx_->value_mask_ | skip_mask_));
    }
  };

  // Return the initial range that encompasses all the keys in the batch
  Range GetMultiGetRange() { return Range(this, num_keys_); }
};

}  // namespace ROCKSDB_NAMESPACE
