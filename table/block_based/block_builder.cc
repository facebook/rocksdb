//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32 (NOTE1)
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 (explicitly stored) for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.
//
// NOTE1: omitted for format_version >= 4 index blocks, because the value is
// composed of one (shared_bytes > 0) or two (shared_bytes == 0) varints, whose
// length is self-describing.

#include "table/block_based/block_builder.h"

#include <algorithm>
#include <cassert>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "table/block_based/data_block_footer.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

BlockBuilder::BlockBuilder(
    int block_restart_interval, bool use_delta_encoding,
    bool use_value_delta_encoding,
    BlockBasedTableOptions::DataBlockIndexType index_type,
    double data_block_hash_table_util_ratio, size_t ts_sz,
    bool persist_user_defined_timestamps, bool is_user_key,
    bool use_separated_kv_storage)
    : block_restart_interval_(block_restart_interval),
      use_delta_encoding_(use_delta_encoding),
      use_value_delta_encoding_(use_value_delta_encoding),
      strip_ts_sz_(persist_user_defined_timestamps ? 0 : ts_sz),
      is_user_key_(is_user_key),
      restarts_(1, 0),  // First restart point is at offset 0
      counter_(0),
      finished_(false),
      use_separated_kv_storage_(use_separated_kv_storage) {
  switch (index_type) {
    case BlockBasedTableOptions::kDataBlockBinarySearch:
      break;
    case BlockBasedTableOptions::kDataBlockBinaryAndHash:
      data_block_hash_index_builder_.Initialize(
          data_block_hash_table_util_ratio);
      break;
    default:
      assert(0);
  }
  assert(block_restart_interval_ >= 1);
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t) +
              (use_separated_kv_storage_ ? sizeof(uint32_t) : 0);
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.resize(1);  // First restart point is at offset 0
  assert(restarts_[0] == 0);
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t) +
              (use_separated_kv_storage_ ? sizeof(uint32_t) : 0);
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
  if (data_block_hash_index_builder_.Valid()) {
    data_block_hash_index_builder_.Reset();
  }
  values_buffer_.clear();

#ifndef NDEBUG
  add_with_last_key_called_ = false;
#endif
}

void BlockBuilder::SwapAndReset(std::string& buffer) {
  std::swap(buffer_, buffer);
  Reset();
}

size_t BlockBuilder::EstimateSizeAfterKV(const Slice& key,
                                         const Slice& value) const {
  size_t estimate = CurrentSizeEstimate();
  // Note: this is an imprecise estimate as it accounts for the whole key size
  // instead of non-shared key size.
  estimate += key.size();
  if (strip_ts_sz_ > 0) {
    estimate -= strip_ts_sz_;
  }
  // In value delta encoding we estimate the value delta size as half the full
  // value size since only the size field of block handle is encoded.
  estimate +=
      !use_value_delta_encoding_ || (counter_ >= block_restart_interval_)
          ? value.size()
          : value.size() / 2;

  if (counter_ >= block_restart_interval_) {
    estimate += sizeof(uint32_t);  // a new restart entry.
  }

  // For separated KV storage, value_offset varint is written at restart points
  if (use_separated_kv_storage_ &&
      (counter_ == 0 || counter_ >= block_restart_interval_)) {
    estimate += VarintLength(values_buffer_.size());
  }

  estimate += sizeof(int32_t);  // varint for shared prefix length.
  // Note: this is an imprecise estimate as we will have to encoded size, one
  // for shared key and one for non-shared key.
  estimate += VarintLength(key.size());  // varint for key length.
  if (!use_value_delta_encoding_ || (counter_ >= block_restart_interval_)) {
    estimate += VarintLength(value.size());  // varint for value length.
  }

  return estimate;
}

Slice BlockBuilder::Finish() {
  // Append restart array
  size_t values_buffer_offset = buffer_.size();

  if (use_separated_kv_storage_) {
    buffer_.append(values_buffer_);
  }

  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }

  DataBlockFooter footer;
  footer.num_restarts = static_cast<uint32_t>(restarts_.size());
  footer.index_type = BlockBasedTableOptions::kDataBlockBinarySearch;
  if (data_block_hash_index_builder_.Valid() &&
      CurrentSizeEstimate() <= kMaxBlockSizeSupportedByHashIndex) {
    data_block_hash_index_builder_.Finish(buffer_);
    footer.index_type = BlockBasedTableOptions::kDataBlockBinaryAndHash;
  }

  if (use_separated_kv_storage_) {
    footer.separated_kv = true;
    footer.values_section_offset = static_cast<uint32_t>(values_buffer_offset);
  }
  footer.EncodeTo(&buffer_);
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value,
                       const Slice* const delta_value,
                       bool skip_delta_encoding) {
  // Ensure no unsafe mixing of Add and AddWithLastKey
  assert(!add_with_last_key_called_);

  AddWithLastKeyImpl(key, value, last_key_, delta_value, skip_delta_encoding,
                     buffer_.size());
  if (use_delta_encoding_) {
    // Update state
    // We used to just copy the changed data, but it appears to be
    // faster to just copy the whole thing.
    last_key_.assign(key.data(), key.size());
  }
}

void BlockBuilder::AddWithLastKey(const Slice& key, const Slice& value,
                                  const Slice& last_key_param,
                                  const Slice* const delta_value,
                                  bool skip_delta_encoding) {
  // Ensure no unsafe mixing of Add and AddWithLastKey
  assert(last_key_.empty());
#ifndef NDEBUG
  add_with_last_key_called_ = false;
#endif

  // Here we make sure to use an empty `last_key` on first call after creation
  // or Reset. This is more convenient for the caller and we can be more
  // clever inside BlockBuilder. On this hot code path, we want to avoid
  // conditional jumps like `buffer_.empty() ? ... : ...` so we can use a
  // fast arithmetic operation instead, with an assertion to be sure our logic
  // is sound.
  size_t buffer_size = buffer_.size();
  size_t last_key_size = last_key_param.size();
  assert(buffer_size == 0 || buffer_size >= last_key_size - strip_ts_sz_);

  Slice last_key(last_key_param.data(), last_key_size * (buffer_size > 0));

  AddWithLastKeyImpl(key, value, last_key, delta_value, skip_delta_encoding,
                     buffer_size);
}

inline void BlockBuilder::AddWithLastKeyImpl(const Slice& key,
                                             const Slice& value,
                                             const Slice& last_key,
                                             const Slice* const delta_value,
                                             bool skip_delta_encoding,
                                             size_t buffer_size) {
  assert(!finished_);
  assert(counter_ <= block_restart_interval_);
  std::string key_buf;
  std::string last_key_buf;
  const Slice key_to_persist = MaybeStripTimestampFromKey(&key_buf, key);
  // For delta key encoding, the first key in each restart interval doesn't have
  // a last key to share bytes with.
  const Slice last_key_persisted =
      last_key.size() == 0
          ? last_key
          : MaybeStripTimestampFromKey(&last_key_buf, last_key);
  size_t shared = 0;  // number of bytes shared with prev key
  if (counter_ >= block_restart_interval_) {
    // Restart compression
    restarts_.push_back(static_cast<uint32_t>(buffer_size));
    estimate_ += sizeof(uint32_t);
    counter_ = 0;
  } else if (use_delta_encoding_ && !skip_delta_encoding) {
    // See how much sharing to do with previous string
    shared = key_to_persist.difference_offset(last_key_persisted);
  }

  const size_t non_shared = key_to_persist.size() - shared;
  const size_t previous_value_offset = values_buffer_.size();
  if (use_value_delta_encoding_) {
    if (use_separated_kv_storage_ && counter_ == 0) {
      // Add "<shared><non_shared><value_offset>" to buffer_
      PutVarint32(&buffer_, static_cast<uint32_t>(shared),
                  static_cast<uint32_t>(non_shared),
                  static_cast<uint32_t>(values_buffer_.size()));
    } else {
      // Add "<shared><non_shared>" to buffer_
      PutVarint32(&buffer_, static_cast<uint32_t>(shared),
                  static_cast<uint32_t>(non_shared));
    }
  } else {
    if (use_separated_kv_storage_ && counter_ == 0) {
      // Add "<shared><non_shared><value_size><value_offset>" to buffer_
      PutVarint32(&buffer_, static_cast<uint32_t>(shared),
                  static_cast<uint32_t>(non_shared),
                  static_cast<uint32_t>(value.size()),
                  static_cast<uint32_t>(values_buffer_.size()));
    } else {
      // Add "<shared><non_shared><value_size>" to buffer_
      PutVarint32(&buffer_, static_cast<uint32_t>(shared),
                  static_cast<uint32_t>(non_shared),
                  static_cast<uint32_t>(value.size()));
    }
  }

  // Add string delta to buffer_
  buffer_.append(key_to_persist.data() + shared, non_shared);

  auto& values_buffer = use_separated_kv_storage_ ? values_buffer_ : buffer_;
  // Use value delta encoding only when the key has shared bytes. This would
  // simplify the decoding, where it can figure which decoding to use simply by
  // looking at the shared bytes size.
  if (shared != 0 && use_value_delta_encoding_) {
    assert(delta_value != nullptr);
    values_buffer.append(delta_value->data(), delta_value->size());
  } else {
    values_buffer.append(value.data(), value.size());
  }

  // TODO(yuzhangyu): make user defined timestamp work with block hash index.
  if (data_block_hash_index_builder_.Valid()) {
    // Only data blocks should be using `kDataBlockBinaryAndHash` index type.
    // And data blocks should always be built with internal keys instead of
    // user keys.
    assert(!is_user_key_);
    data_block_hash_index_builder_.Add(ExtractUserKey(key),
                                       restarts_.size() - 1);
  }

  counter_++;
  estimate_ += buffer_.size() - buffer_size + values_buffer_.size() -
               previous_value_offset;
}

const Slice BlockBuilder::MaybeStripTimestampFromKey(std::string* key_buf,
                                                     const Slice& key) {
  Slice stripped_key = key;
  if (strip_ts_sz_ > 0) {
    if (is_user_key_) {
      stripped_key.remove_suffix(strip_ts_sz_);
    } else {
      StripTimestampFromInternalKey(key_buf, key, strip_ts_sz_);
      stripped_key = *key_buf;
    }
  }
  return stripped_key;
}
}  // namespace ROCKSDB_NAMESPACE
