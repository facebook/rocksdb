//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "port/malloc.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "table/block_based/block_prefix_index.h"
#include "table/block_based/data_block_hash_index.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "test_util/sync_point.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

struct BlockContents;
class Comparator;
template <class TValue>
class BlockIter;
class DataBlockIter;
class IndexBlockIter;
class BlockPrefixIndex;

// BlockReadAmpBitmap is a bitmap that map the ROCKSDB_NAMESPACE::Block data
// bytes to a bitmap with ratio bytes_per_bit. Whenever we access a range of
// bytes in the Block we update the bitmap and increment
// READ_AMP_ESTIMATE_USEFUL_BYTES.
class BlockReadAmpBitmap {
 public:
  explicit BlockReadAmpBitmap(size_t block_size, size_t bytes_per_bit,
                              Statistics* statistics)
      : bitmap_(nullptr),
        bytes_per_bit_pow_(0),
        statistics_(statistics),
        rnd_(Random::GetTLSInstance()->Uniform(
            static_cast<int>(bytes_per_bit))) {
    TEST_SYNC_POINT_CALLBACK("BlockReadAmpBitmap:rnd", &rnd_);
    assert(block_size > 0 && bytes_per_bit > 0);

    // convert bytes_per_bit to be a power of 2
    while (bytes_per_bit >>= 1) {
      bytes_per_bit_pow_++;
    }

    // num_bits_needed = ceil(block_size / bytes_per_bit)
    size_t num_bits_needed = ((block_size - 1) >> bytes_per_bit_pow_) + 1;
    assert(num_bits_needed > 0);

    // bitmap_size = ceil(num_bits_needed / kBitsPerEntry)
    size_t bitmap_size = (num_bits_needed - 1) / kBitsPerEntry + 1;

    // Create bitmap and set all the bits to 0
    bitmap_ = new std::atomic<uint32_t>[bitmap_size]();

    RecordTick(GetStatistics(), READ_AMP_TOTAL_READ_BYTES, block_size);
  }

  ~BlockReadAmpBitmap() { delete[] bitmap_; }

  void Mark(uint32_t start_offset, uint32_t end_offset) {
    assert(end_offset >= start_offset);
    // Index of first bit in mask
    uint32_t start_bit =
        (start_offset + (1 << bytes_per_bit_pow_) - rnd_ - 1) >>
        bytes_per_bit_pow_;
    // Index of last bit in mask + 1
    uint32_t exclusive_end_bit =
        (end_offset + (1 << bytes_per_bit_pow_) - rnd_) >> bytes_per_bit_pow_;
    if (start_bit >= exclusive_end_bit) {
      return;
    }
    assert(exclusive_end_bit > 0);

    if (GetAndSet(start_bit) == 0) {
      uint32_t new_useful_bytes = (exclusive_end_bit - start_bit)
                                  << bytes_per_bit_pow_;
      RecordTick(GetStatistics(), READ_AMP_ESTIMATE_USEFUL_BYTES,
                 new_useful_bytes);
    }
  }

  Statistics* GetStatistics() {
    return statistics_.load(std::memory_order_relaxed);
  }

  void SetStatistics(Statistics* stats) { statistics_.store(stats); }

  uint32_t GetBytesPerBit() { return 1 << bytes_per_bit_pow_; }

  size_t ApproximateMemoryUsage() const {
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    return malloc_usable_size((void*)this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    return sizeof(*this);
  }

 private:
  // Get the current value of bit at `bit_idx` and set it to 1
  inline bool GetAndSet(uint32_t bit_idx) {
    const uint32_t byte_idx = bit_idx / kBitsPerEntry;
    const uint32_t bit_mask = 1 << (bit_idx % kBitsPerEntry);

    return bitmap_[byte_idx].fetch_or(bit_mask, std::memory_order_relaxed) &
           bit_mask;
  }

  const uint32_t kBytesPersEntry = sizeof(uint32_t);   // 4 bytes
  const uint32_t kBitsPerEntry = kBytesPersEntry * 8;  // 32 bits

  // Bitmap used to record the bytes that we read, use atomic to protect
  // against multiple threads updating the same bit
  std::atomic<uint32_t>* bitmap_;
  // (1 << bytes_per_bit_pow_) is bytes_per_bit. Use power of 2 to optimize
  // muliplication and division
  uint8_t bytes_per_bit_pow_;
  // Pointer to DB Statistics object, Since this bitmap may outlive the DB
  // this pointer maybe invalid, but the DB will update it to a valid pointer
  // by using SetStatistics() before calling Mark()
  std::atomic<Statistics*> statistics_;
  uint32_t rnd_;
};

// This Block class is not for any old block: it is designed to hold only
// uncompressed blocks containing sorted key-value pairs. It is thus
// suitable for storing uncompressed data blocks, index blocks (including
// partitions), range deletion blocks, properties blocks, metaindex blocks,
// as well as the top level of the partitioned filter structure (which is
// actually an index of the filter partitions). It is NOT suitable for
// compressed blocks in general, filter blocks/partitions, or compression
// dictionaries (since the latter do not contain sorted key-value pairs).
// Use BlockContents directly for those.
//
// See https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format
// for details of the format and the various block types.
class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(BlockContents&& contents, size_t read_amp_bytes_per_bit = 0,
                 Statistics* statistics = nullptr);
  // No copying allowed
  Block(const Block&) = delete;
  void operator=(const Block&) = delete;

  ~Block();

  size_t size() const { return size_; }
  const char* data() const { return data_; }
  // The additional memory space taken by the block data.
  size_t usable_size() const { return contents_.usable_size(); }
  uint32_t NumRestarts() const;
  bool own_bytes() const { return contents_.own_bytes(); }

  BlockBasedTableOptions::DataBlockIndexType IndexType() const;

  // raw_ucmp is a raw (i.e., not wrapped by `UserComparatorWrapper`) user key
  // comparator.
  //
  // If iter is null, return new Iterator
  // If iter is not null, update this one and return it as Iterator*
  //
  // Updates read_amp_bitmap_ if it is not nullptr.
  //
  // If `block_contents_pinned` is true, the caller will guarantee that when
  // the cleanup functions are transferred from the iterator to other
  // classes, e.g. PinnableSlice, the pointer to the bytes will still be
  // valid. Either the iterator holds cache handle or ownership of some resource
  // and release them in a release function, or caller is sure that the data
  // will not go away (for example, it's from mmapped file which will not be
  // closed).
  //
  // NOTE: for the hash based lookup, if a key prefix doesn't match any key,
  // the iterator will simply be set as "invalid", rather than returning
  // the key that is just pass the target key.
  DataBlockIter* NewDataIterator(const Comparator* raw_ucmp,
                                 SequenceNumber global_seqno,
                                 DataBlockIter* iter = nullptr,
                                 Statistics* stats = nullptr,
                                 bool block_contents_pinned = false);

  // raw_ucmp is a raw (i.e., not wrapped by `UserComparatorWrapper`) user key
  // comparator.
  //
  // key_includes_seq, default true, means that the keys are in internal key
  // format.
  // value_is_full, default true, means that no delta encoding is
  // applied to values.
  //
  // If `prefix_index` is not nullptr this block will do hash lookup for the key
  // prefix. If total_order_seek is true, prefix_index_ is ignored.
  //
  // `have_first_key` controls whether IndexValue will contain
  // first_internal_key. It affects data serialization format, so the same value
  // have_first_key must be used when writing and reading index.
  // It is determined by IndexType property of the table.
  IndexBlockIter* NewIndexIterator(const Comparator* raw_ucmp,
                                   SequenceNumber global_seqno,
                                   IndexBlockIter* iter, Statistics* stats,
                                   bool total_order_seek, bool have_first_key,
                                   bool key_includes_seq, bool value_is_full,
                                   bool block_contents_pinned = false,
                                   BlockPrefixIndex* prefix_index = nullptr);

  // Report an approximation of how much memory has been used.
  size_t ApproximateMemoryUsage() const;

 private:
  BlockContents contents_;
  const char* data_;         // contents_.data.data()
  size_t size_;              // contents_.data.size()
  uint32_t restart_offset_;  // Offset in data_ of restart array
  uint32_t num_restarts_;
  std::unique_ptr<BlockReadAmpBitmap> read_amp_bitmap_;
  DataBlockHashIndex data_block_hash_index_;
};

// A `BlockIter` iterates over the entries in a `Block`'s data buffer. The
// format of this data buffer is an uncompressed, sorted sequence of key-value
// pairs (see `Block` API for more details).
//
// Notably, the keys may either be in internal key format or user key format.
// Subclasses are responsible for configuring the key format.
//
// `BlockIter` intends to provide final overrides for all of
// `InternalIteratorBase` functions that can move the iterator. It does
// this to guarantee `UpdateKey()` is called exactly once after each key
// movement potentially visible to users. In this step, the key is prepared
// (e.g., serialized if global seqno is in effect) so it can be returned
// immediately when the user asks for it via calling `key() const`.
//
// For its subclasses, it provides protected variants of the above-mentioned
// final-overridden methods. They are named with the "Impl" suffix, e.g.,
// `Seek()` logic would be implemented by subclasses in `SeekImpl()`. These
// "Impl" functions are responsible for positioning `raw_key_` but not
// invoking `UpdateKey()`.
template <class TValue>
class BlockIter : public InternalIteratorBase<TValue> {
 public:
  void InitializeBase(const Comparator* raw_ucmp, const char* data,
                      uint32_t restarts, uint32_t num_restarts,
                      SequenceNumber global_seqno, bool block_contents_pinned) {
    assert(data_ == nullptr);  // Ensure it is called only once
    assert(num_restarts > 0);  // Ensure the param is valid

    raw_ucmp_ = raw_ucmp;
    data_ = data;
    restarts_ = restarts;
    num_restarts_ = num_restarts;
    current_ = restarts_;
    restart_index_ = num_restarts_;
    global_seqno_ = global_seqno;
    block_contents_pinned_ = block_contents_pinned;
    cache_handle_ = nullptr;
  }

  // Makes Valid() return false, status() return `s`, and Seek()/Prev()/etc do
  // nothing. Calls cleanup functions.
  void InvalidateBase(Status s) {
    // Assert that the BlockIter is never deleted while Pinning is Enabled.
    assert(!pinned_iters_mgr_ ||
           (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));

    data_ = nullptr;
    current_ = restarts_;
    status_ = s;

    // Call cleanup callbacks.
    Cleanable::Reset();
  }

  bool Valid() const override { return current_ < restarts_; }

  virtual void SeekToFirst() override final {
    SeekToFirstImpl();
    UpdateKey();
  }

  virtual void SeekToLast() override final {
    SeekToLastImpl();
    UpdateKey();
  }

  virtual void Seek(const Slice& target) override final {
    SeekImpl(target);
    UpdateKey();
  }

  virtual void SeekForPrev(const Slice& target) override final {
    SeekForPrevImpl(target);
    UpdateKey();
  }

  virtual void Next() override final {
    NextImpl();
    UpdateKey();
  }

  virtual bool NextAndGetResult(IterateResult* result) override final {
    // This does not need to call `UpdateKey()` as the parent class only has
    // access to the `UpdateKey()`-invoking functions.
    return InternalIteratorBase<TValue>::NextAndGetResult(result);
  }

  virtual void Prev() override final {
    PrevImpl();
    UpdateKey();
  }

  Status status() const override { return status_; }
  Slice key() const override {
    assert(Valid());
    return key_;
  }

#ifndef NDEBUG
  ~BlockIter() override {
    // Assert that the BlockIter is never deleted while Pinning is Enabled.
    assert(!pinned_iters_mgr_ ||
           (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
    status_.PermitUncheckedError();
  }
  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;
#endif

  bool IsKeyPinned() const override {
    return block_contents_pinned_ && key_pinned_;
  }

  bool IsValuePinned() const override { return block_contents_pinned_; }

  size_t TEST_CurrentEntrySize() { return NextEntryOffset() - current_; }

  uint32_t ValueOffset() const {
    return static_cast<uint32_t>(value_.data() - data_);
  }

  void SetCacheHandle(Cache::Handle* handle) { cache_handle_ = handle; }

  Cache::Handle* cache_handle() { return cache_handle_; }

 protected:
  const char* data_;       // underlying block contents
  uint32_t num_restarts_;  // Number of uint32_t entries in restart array

  // Index of restart block in which current_ or current_-1 falls
  uint32_t restart_index_;
  uint32_t restarts_;  // Offset of restart array (list of fixed32)
  // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
  uint32_t current_;
  // Raw key from block.
  IterKey raw_key_;
  // Buffer for key data when global seqno assignment is enabled.
  IterKey key_buf_;
  Slice value_;
  Status status_;
  // Key to be exposed to users.
  Slice key_;
  bool key_pinned_;
  // Whether the block data is guaranteed to outlive this iterator, and
  // as long as the cleanup functions are transferred to another class,
  // e.g. PinnableSlice, the pointer to the bytes will still be valid.
  bool block_contents_pinned_;
  SequenceNumber global_seqno_;

  virtual void SeekToFirstImpl() = 0;
  virtual void SeekToLastImpl() = 0;
  virtual void SeekImpl(const Slice& target) = 0;
  virtual void SeekForPrevImpl(const Slice& target) = 0;
  virtual void NextImpl() = 0;
  virtual void PrevImpl() = 0;

  InternalKeyComparator icmp() {
    return InternalKeyComparator(raw_ucmp_, false /* named */);
  }

  UserComparatorWrapper ucmp() { return UserComparatorWrapper(raw_ucmp_); }

  // Must be called every time a key is found that needs to be returned to user,
  // and may be called when no key is found (as a no-op). Updates `key_`,
  // `key_buf_`, and `key_pinned_` with info about the found key.
  void UpdateKey() {
    key_buf_.Clear();
    if (!Valid()) {
      return;
    }
    if (raw_key_.IsUserKey()) {
      assert(global_seqno_ == kDisableGlobalSequenceNumber);
      key_ = raw_key_.GetUserKey();
      key_pinned_ = raw_key_.IsKeyPinned();
    } else if (global_seqno_ == kDisableGlobalSequenceNumber) {
      key_ = raw_key_.GetInternalKey();
      key_pinned_ = raw_key_.IsKeyPinned();
    } else {
      key_buf_.SetInternalKey(raw_key_.GetUserKey(), global_seqno_,
                              ExtractValueType(raw_key_.GetInternalKey()));
      key_ = key_buf_.GetInternalKey();
      key_pinned_ = false;
    }
  }

  // Returns the result of `Comparator::Compare()`, where the appropriate
  // comparator is used for the block contents, the LHS argument is the current
  // key with global seqno applied, and the RHS argument is `other`.
  int CompareCurrentKey(const Slice& other) {
    if (raw_key_.IsUserKey()) {
      assert(global_seqno_ == kDisableGlobalSequenceNumber);
      return ucmp().Compare(raw_key_.GetUserKey(), other);
    } else if (global_seqno_ == kDisableGlobalSequenceNumber) {
      return icmp().Compare(raw_key_.GetInternalKey(), other);
    }
    return icmp().Compare(raw_key_.GetInternalKey(), global_seqno_, other,
                          kDisableGlobalSequenceNumber);
  }

 private:
  const Comparator* raw_ucmp_;
  // Store the cache handle, if the block is cached. We need this since the
  // only other place the handle is stored is as an argument to the Cleanable
  // function callback, which is hard to retrieve. When multiple value
  // PinnableSlices reference the block, they need the cache handle in order
  // to bump up the ref count
  Cache::Handle* cache_handle_;

 public:
  // Return the offset in data_ just past the end of the current entry.
  inline uint32_t NextEntryOffset() const {
    // NOTE: We don't support blocks bigger than 2GB
    return static_cast<uint32_t>((value_.data() + value_.size()) - data_);
  }

  uint32_t GetRestartPoint(uint32_t index) {
    assert(index < num_restarts_);
    return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
  }

  void SeekToRestartPoint(uint32_t index) {
    raw_key_.Clear();
    restart_index_ = index;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of value_, so set value_ accordingly
    uint32_t offset = GetRestartPoint(index);
    value_ = Slice(data_ + offset, 0);
  }

  void CorruptionError();

 protected:
  template <typename DecodeKeyFunc>
  inline bool BinarySeek(const Slice& target, uint32_t* index,
                         bool* is_index_key_result);

  void FindKeyAfterBinarySeek(const Slice& target, uint32_t index,
                              bool is_index_key_result);
};

class DataBlockIter final : public BlockIter<Slice> {
 public:
  DataBlockIter()
      : BlockIter(), read_amp_bitmap_(nullptr), last_bitmap_offset_(0) {}
  DataBlockIter(const Comparator* raw_ucmp, const char* data, uint32_t restarts,
                uint32_t num_restarts, SequenceNumber global_seqno,
                BlockReadAmpBitmap* read_amp_bitmap, bool block_contents_pinned,
                DataBlockHashIndex* data_block_hash_index)
      : DataBlockIter() {
    Initialize(raw_ucmp, data, restarts, num_restarts, global_seqno,
               read_amp_bitmap, block_contents_pinned, data_block_hash_index);
  }
  void Initialize(const Comparator* raw_ucmp, const char* data,
                  uint32_t restarts, uint32_t num_restarts,
                  SequenceNumber global_seqno,
                  BlockReadAmpBitmap* read_amp_bitmap,
                  bool block_contents_pinned,
                  DataBlockHashIndex* data_block_hash_index) {
    InitializeBase(raw_ucmp, data, restarts, num_restarts, global_seqno,
                   block_contents_pinned);
    raw_key_.SetIsUserKey(false);
    read_amp_bitmap_ = read_amp_bitmap;
    last_bitmap_offset_ = current_ + 1;
    data_block_hash_index_ = data_block_hash_index;
  }

  Slice value() const override {
    assert(Valid());
    if (read_amp_bitmap_ && current_ < restarts_ &&
        current_ != last_bitmap_offset_) {
      read_amp_bitmap_->Mark(current_ /* current entry offset */,
                             NextEntryOffset() - 1);
      last_bitmap_offset_ = current_;
    }
    return value_;
  }

  inline bool SeekForGet(const Slice& target) {
    if (!data_block_hash_index_) {
      SeekImpl(target);
      UpdateKey();
      return true;
    }
    bool res = SeekForGetImpl(target);
    UpdateKey();
    return res;
  }

  // Try to advance to the next entry in the block. If there is data corruption
  // or error, report it to the caller instead of aborting the process. May
  // incur higher CPU overhead because we need to perform check on every entry.
  void NextOrReport() {
    NextOrReportImpl();
    UpdateKey();
  }

  // Try to seek to the first entry in the block. If there is data corruption
  // or error, report it to caller instead of aborting the process. May incur
  // higher CPU overhead because we need to perform check on every entry.
  void SeekToFirstOrReport() {
    SeekToFirstOrReportImpl();
    UpdateKey();
  }

  void Invalidate(Status s) {
    InvalidateBase(s);
    // Clear prev entries cache.
    prev_entries_keys_buff_.clear();
    prev_entries_.clear();
    prev_entries_idx_ = -1;
  }

 protected:
  virtual void SeekToFirstImpl() override;
  virtual void SeekToLastImpl() override;
  virtual void SeekImpl(const Slice& target) override;
  virtual void SeekForPrevImpl(const Slice& target) override;
  virtual void NextImpl() override;
  virtual void PrevImpl() override;

 private:
  // read-amp bitmap
  BlockReadAmpBitmap* read_amp_bitmap_;
  // last `current_` value we report to read-amp bitmp
  mutable uint32_t last_bitmap_offset_;
  struct CachedPrevEntry {
    explicit CachedPrevEntry(uint32_t _offset, const char* _key_ptr,
                             size_t _key_offset, size_t _key_size, Slice _value)
        : offset(_offset),
          key_ptr(_key_ptr),
          key_offset(_key_offset),
          key_size(_key_size),
          value(_value) {}

    // offset of entry in block
    uint32_t offset;
    // Pointer to key data in block (nullptr if key is delta-encoded)
    const char* key_ptr;
    // offset of key in prev_entries_keys_buff_ (0 if key_ptr is not nullptr)
    size_t key_offset;
    // size of key
    size_t key_size;
    // value slice pointing to data in block
    Slice value;
  };
  std::string prev_entries_keys_buff_;
  std::vector<CachedPrevEntry> prev_entries_;
  int32_t prev_entries_idx_ = -1;

  DataBlockHashIndex* data_block_hash_index_;

  template <typename DecodeEntryFunc>
  inline bool ParseNextDataKey(const char* limit = nullptr);

  bool SeekForGetImpl(const Slice& target);
  void NextOrReportImpl();
  void SeekToFirstOrReportImpl();
};

class IndexBlockIter final : public BlockIter<IndexValue> {
 public:
  IndexBlockIter() : BlockIter(), prefix_index_(nullptr) {}

  // key_includes_seq, default true, means that the keys are in internal key
  // format.
  // value_is_full, default true, means that no delta encoding is
  // applied to values.
  void Initialize(const Comparator* raw_ucmp, const char* data,
                  uint32_t restarts, uint32_t num_restarts,
                  SequenceNumber global_seqno, BlockPrefixIndex* prefix_index,
                  bool have_first_key, bool key_includes_seq,
                  bool value_is_full, bool block_contents_pinned) {
    InitializeBase(raw_ucmp, data, restarts, num_restarts,
                   kDisableGlobalSequenceNumber, block_contents_pinned);
    raw_key_.SetIsUserKey(!key_includes_seq);
    prefix_index_ = prefix_index;
    value_delta_encoded_ = !value_is_full;
    have_first_key_ = have_first_key;
    if (have_first_key_ && global_seqno != kDisableGlobalSequenceNumber) {
      global_seqno_state_.reset(new GlobalSeqnoState(global_seqno));
    } else {
      global_seqno_state_.reset();
    }
  }

  Slice user_key() const override {
    assert(Valid());
    return raw_key_.GetUserKey();
  }

  IndexValue value() const override {
    assert(Valid());
    if (value_delta_encoded_ || global_seqno_state_ != nullptr) {
      return decoded_value_;
    } else {
      IndexValue entry;
      Slice v = value_;
      Status decode_s __attribute__((__unused__)) =
          entry.DecodeFrom(&v, have_first_key_, nullptr);
      assert(decode_s.ok());
      return entry;
    }
  }

  void Invalidate(Status s) { InvalidateBase(s); }

  bool IsValuePinned() const override {
    return global_seqno_state_ != nullptr ? false : BlockIter::IsValuePinned();
  }

 protected:
  // IndexBlockIter follows a different contract for prefix iterator
  // from data iterators.
  // If prefix of the seek key `target` exists in the file, it must
  // return the same result as total order seek.
  // If the prefix of `target` doesn't exist in the file, it can either
  // return the result of total order seek, or set both of Valid() = false
  // and status() = NotFound().
  void SeekImpl(const Slice& target) override;

  void SeekForPrevImpl(const Slice&) override {
    assert(false);
    current_ = restarts_;
    restart_index_ = num_restarts_;
    status_ = Status::InvalidArgument(
        "RocksDB internal error: should never call SeekForPrev() on index "
        "blocks");
    raw_key_.Clear();
    value_.clear();
  }

  void PrevImpl() override;

  void NextImpl() override;

  void SeekToFirstImpl() override;

  void SeekToLastImpl() override;

 private:
  bool value_delta_encoded_;
  bool have_first_key_;  // value includes first_internal_key
  BlockPrefixIndex* prefix_index_;
  // Whether the value is delta encoded. In that case the value is assumed to be
  // BlockHandle. The first value in each restart interval is the full encoded
  // BlockHandle; the restart of encoded size part of the BlockHandle. The
  // offset of delta encoded BlockHandles is computed by adding the size of
  // previous delta encoded values in the same restart interval to the offset of
  // the first value in that restart interval.
  IndexValue decoded_value_;

  // When sequence number overwriting is enabled, this struct contains the seqno
  // to overwrite with, and current first_internal_key with overwritten seqno.
  // This is rarely used, so we put it behind a pointer and only allocate when
  // needed.
  struct GlobalSeqnoState {
    // First internal key according to current index entry, but with sequence
    // number overwritten to global_seqno.
    IterKey first_internal_key;
    SequenceNumber global_seqno;

    explicit GlobalSeqnoState(SequenceNumber seqno) : global_seqno(seqno) {}
  };

  std::unique_ptr<GlobalSeqnoState> global_seqno_state_;

  // Set *prefix_may_exist to false if no key possibly share the same prefix
  // as `target`. If not set, the result position should be the same as total
  // order Seek.
  bool PrefixSeek(const Slice& target, uint32_t* index, bool* prefix_may_exist);
  // Set *prefix_may_exist to false if no key can possibly share the same
  // prefix as `target`. If not set, the result position should be the same
  // as total order seek.
  bool BinaryBlockIndexSeek(const Slice& target, uint32_t* block_ids,
                            uint32_t left, uint32_t right, uint32_t* index,
                            bool* prefix_may_exist);
  inline int CompareBlockKey(uint32_t block_index, const Slice& target);

  inline bool ParseNextIndexKey();

  // When value_delta_encoded_ is enabled it decodes the value which is assumed
  // to be BlockHandle and put it to decoded_value_
  inline void DecodeCurrentValue(uint32_t shared);
};

}  // namespace ROCKSDB_NAMESPACE
