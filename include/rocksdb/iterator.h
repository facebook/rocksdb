// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.

#pragma once

#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "rocksdb/iterator_base.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

// A self-contained key-value pair returned by Iterator::PinCurrent(). The
// slices remain valid until Reset() or object destruction, regardless of
// subsequent iterator movement.
class PinnableKeyValue {
 public:
  PinnableKeyValue() = default;
  PinnableKeyValue(PinnableKeyValue&&) = default;
  PinnableKeyValue& operator=(PinnableKeyValue&&) = default;

  PinnableKeyValue(const PinnableKeyValue&) = delete;
  PinnableKeyValue& operator=(const PinnableKeyValue&) = delete;

  Slice key() const { return key_; }
  Slice value() const { return value_; }

  bool IsKeyPinned() const { return key_.IsPinned(); }
  bool IsValuePinned() const { return value_.IsPinned(); }

  size_t GetPinnedBlockCacheUsage() const { return pinned_block_cache_usage_; }
  size_t GetCopiedBytes() const { return copied_bytes_; }

  void Reset() {
    key_.Reset();
    value_.Reset();
    cleanup_.Reset();
    pinned_block_cache_usage_ = 0;
    copied_bytes_ = 0;
  }

 private:
  friend class DBIter;

  void PinKey(const Slice& key) { key_.PinSlice(key, nullptr); }
  void CopyKey(const Slice& key) {
    key_.PinSelf(key);
    copied_bytes_ += key.size();
  }

  void PinValue(const Slice& value) { value_.PinSlice(value, nullptr); }
  void CopyValue(const Slice& value) {
    value_.PinSelf(value);
    copied_bytes_ += value.size();
  }

  void SetCleanup(SharedCleanablePtr&& cleanup) {
    cleanup_ = std::move(cleanup);
  }

  void SetPinnedBlockCacheUsage(size_t pinned_block_cache_usage) {
    pinned_block_cache_usage_ = pinned_block_cache_usage;
  }

  PinnableSlice key_;
  PinnableSlice value_;
  SharedCleanablePtr cleanup_;
  size_t pinned_block_cache_usage_ = 0;
  size_t copied_bytes_ = 0;
};

// A self-contained batch of key-value pairs returned by
// Iterator::AppendPinnedCurrent(). Each appended row remains valid until
// Reset() or object destruction, regardless of subsequent iterator movement.
class PinnableKeyValueBatch {
 public:
  PinnableKeyValueBatch() = default;
  PinnableKeyValueBatch(PinnableKeyValueBatch&&) = default;
  PinnableKeyValueBatch& operator=(PinnableKeyValueBatch&&) = default;

  PinnableKeyValueBatch(const PinnableKeyValueBatch&) = delete;
  PinnableKeyValueBatch& operator=(const PinnableKeyValueBatch&) = delete;

  size_t size() const { return entries_.size(); }
  bool empty() const { return entries_.empty(); }

  Slice key(size_t index) const {
    assert(index < entries_.size());
    return entries_[index].key;
  }

  Slice value(size_t index) const {
    assert(index < entries_.size());
    return entries_[index].value;
  }

  bool IsKeyPinned(size_t index) const {
    assert(index < entries_.size());
    return entries_[index].key.IsPinned();
  }

  bool IsValuePinned(size_t index) const {
    assert(index < entries_.size());
    return entries_[index].value.IsPinned();
  }

  size_t GetPinnedBlockCacheUsage() const { return pinned_block_cache_usage_; }
  size_t GetCopiedBytes() const { return copied_bytes_; }

  void Reset() {
    entries_.clear();
    cleanups_.clear();
    cleanup_dedupe_tokens_.clear();
    pinned_block_cache_usage_ = 0;
    copied_bytes_ = 0;
  }

 private:
  friend class DBIter;

  struct Entry {
    PinnableSlice key;
    PinnableSlice value;
  };

  void Append(const Slice& key, bool pin_key, const Slice& value,
              bool pin_value, SharedCleanablePtr&& cleanup,
              const void* cleanup_dedupe_token,
              size_t pinned_block_cache_usage) {
    if (cleanup.get() != nullptr) {
      bool retain_cleanup = true;
      if (cleanup_dedupe_token != nullptr) {
        retain_cleanup =
            cleanup_dedupe_tokens_.insert(cleanup_dedupe_token).second;
      }
      if (retain_cleanup) {
        cleanups_.push_back(std::move(cleanup));
        pinned_block_cache_usage_ += pinned_block_cache_usage;
      }
    }

    entries_.emplace_back();
    Entry& entry = entries_.back();
    if (pin_key) {
      entry.key.PinSlice(key, nullptr);
    } else {
      entry.key.PinSelf(key);
      copied_bytes_ += key.size();
    }
    if (pin_value) {
      entry.value.PinSlice(value, nullptr);
    } else {
      entry.value.PinSelf(value);
      copied_bytes_ += value.size();
    }
  }

  std::vector<Entry> entries_;
  std::vector<SharedCleanablePtr> cleanups_;
  std::unordered_set<const void*> cleanup_dedupe_tokens_;
  size_t pinned_block_cache_usage_ = 0;
  size_t copied_bytes_ = 0;
};

class Iterator : public IteratorBase {
 public:
  Iterator() {}
  // No copying allowed
  Iterator(const Iterator&) = delete;
  void operator=(const Iterator&) = delete;

  virtual ~Iterator() override {}

  // Return the value for the current entry.  If the entry is a plain key-value,
  // return the value as-is; if it is a wide-column entity, return the value of
  // the default anonymous column (see kDefaultWideColumnName) if any, or an
  // empty value otherwise.  The underlying storage for the returned slice is
  // valid only until the next modification of the iterator (i.e. the next
  // SeekToFirst/SeekToLast/Seek/SeekForPrev/Next/Prev operation).
  // REQUIRES: Valid()
  virtual Slice value() const = 0;

  // Return the wide columns for the current entry.  If the entry is a
  // wide-column entity, return it as-is; if it is a plain key-value, return it
  // as an entity with a single anonymous column (see kDefaultWideColumnName)
  // which contains the value.  The underlying storage for the returned
  // structure is valid only until the next modification of the iterator (i.e.
  // the next SeekToFirst/SeekToLast/Seek/SeekForPrev/Next/Prev operation).
  // REQUIRES: Valid()
  virtual const WideColumns& columns() const {
    assert(false);
    return kNoWideColumns;
  }

  // Property "rocksdb.iterator.is-key-pinned":
  //   If returning "1", this means that the Slice returned by key() is valid
  //   as long as the iterator is not deleted.
  //   It is guaranteed to always return "1" if
  //      - Iterator created with ReadOptions::pin_data = true
  //      - DB tables were created with
  //        BlockBasedTableOptions::use_delta_encoding = false.
  // Property "rocksdb.iterator.is-value-pinned":
  //   If returning "1", this means that the Slice returned by value() is valid
  //   as long as the iterator is not deleted.
  //   It is guaranteed to always return "1" if
  //      - Iterator created with ReadOptions::pin_data = true
  //      - The value is found in a `kTypeValue` record
  // Property "rocksdb.iterator.super-version-number":
  //   LSM version used by the iterator. The same format as DB Property
  //   kCurrentSuperVersionNumber. See its comment for more information.
  // Property "rocksdb.iterator.internal-key":
  //   Get the user-key portion of the internal key at which the iteration
  //   stopped.
  // Property "rocksdb.iterator.write-time":
  //   Get the unix time of the best estimate of the write time of the entry.
  //   Returned as 64-bit raw value (8 bytes). It can be converted to uint64_t
  //   with util method `DecodeU64Ts`. The accuracy of the write time depends on
  //   settings like preserve_internal_time_seconds. The actual write time of
  //   the entry should be the same or newer than the returned write time. So
  //   this property can be interpreted as the possible oldest write time for
  //   the entry.
  //   If the seqno to time mapping recording is not enabled,
  //   std::numeric_limits<uint64_t>::max() will be returned to indicate the
  //   write time is unknown. For data entry whose sequence number has
  //   been zeroed out (possible when they reach the last level), 0 is returned
  //   no matter whether the seqno to time recording feature is enabled or not.
  virtual Status GetProperty(std::string prop_name, std::string* prop);

  virtual Slice timestamp() const {
    assert(false);
    return Slice();
  }

  // Prepare the iterator to scan the ranges specified in scan_opts. This
  // includes prefetching relevant blocks from disk. The upper bound and
  // other table specific limits should be specified for each
  // scan for best results. If an upper bound is not specified, Prepare may
  // skip prefetching as it cannot accurately determine how much to prefetch.
  //
  // Prepare should typically be followed by Seeks to the start keys in the
  // order they're specified in scan_opts. If the user does a Seek to some
  // other target key, the iterator should disregard the scan_opts from that
  // point onwards and behave like a normal iterator. Its the user's
  // responsibility to again call Prepare().
  //
  // If Prepare() is called, it overrides the iterate_upper_bound in
  // ReadOptions
  virtual void Prepare(const MultiScanArgs& /*scan_opts*/) {}

  // Populate `out` with the current key-value pair. Implementations may pin the
  // underlying storage or copy into `out` as needed. The result remains valid
  // until `out->Reset()` or object destruction. On any error, `out` is cleared.
  //
  // The DB iterator implementation currently supports valid forward-iteration
  // positions over plain kTypeValue rows only. It returns NotSupported for
  // reverse iteration, merge results, user-defined timestamps, wide-column
  // entities, blobs, deletion markers, and other internal value types.
  //
  // GetPinnedBlockCacheUsage() reports the block cache entry charge held by the
  // returned object. It might not include cache-implementation metadata.
  virtual Status PinCurrent(PinnableKeyValue* out);

  // Append the current key-value pair to `batch`. Implementations may pin the
  // underlying storage or copy into `batch` as needed. Appended rows remain
  // valid until `batch->Reset()` or object destruction.
  //
  // Same support limitations and pinned block cache usage semantics as
  // PinCurrent().
  virtual Status AppendPinnedCurrent(PinnableKeyValueBatch* batch);
};

// Return an empty iterator (yields nothing).
Iterator* NewEmptyIterator();

// Return an empty iterator with the specified status.
Iterator* NewErrorIterator(const Status& status);

}  // namespace ROCKSDB_NAMESPACE
