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
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <assert.h>
#include "rocksdb/comparator.h"
#include "db/dbformat.h"
#include "util/coding.h"

namespace rocksdb {

BlockBuilder::BlockBuilder(int block_restart_interval, bool use_delta_encoding)
    : block_restart_interval_(block_restart_interval),
      use_delta_encoding_(use_delta_encoding),
      restarts_(),
      counter_(0),
      finished_(false) {
  assert(block_restart_interval_ >= 1);
  restarts_.push_back(0);       // First restart point is at offset 0
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t);
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);       // First restart point is at offset 0
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t);
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::EstimateSizeAfterKV(const Slice& key, const Slice& value)
  const {
  size_t estimate = CurrentSizeEstimate();
  estimate += key.size() + value.size();
  if (counter_ >= block_restart_interval_) {
    estimate += sizeof(uint32_t); // a new restart entry.
  }

  estimate += sizeof(int32_t); // varint for shared prefix length.
  estimate += VarintLength(key.size()); // varint for key length.
  estimate += VarintLength(value.size()); // varint for value length.

  return estimate;
}

Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, static_cast<uint32_t>(restarts_.size()));
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  assert(!finished_);
  assert(counter_ <= block_restart_interval_);
  size_t shared = 0;  // number of bytes shared with prev key
  if (counter_ >= block_restart_interval_) {
    // Restart compression
    restarts_.push_back(static_cast<uint32_t>(buffer_.size()));
    estimate_ += sizeof(uint32_t);
    counter_ = 0;

    if (use_delta_encoding_) {
      // Update state
      last_key_.assign(key.data(), key.size());
    }
  } else if (use_delta_encoding_) {
    Slice last_key_piece(last_key_);
    // See how much sharing to do with previous string
    shared = key.difference_offset(last_key_piece);

    // Update state
    // We used to just copy the changed data here, but it appears to be
    // faster to just copy the whole thing.
    last_key_.assign(key.data(), key.size());
  }

  const size_t non_shared = key.size() - shared;
  const size_t curr_size = buffer_.size();

  // Add "<shared><non_shared><value_size>" to buffer_
  PutVarint32Varint32Varint32(&buffer_, static_cast<uint32_t>(shared),
                              static_cast<uint32_t>(non_shared),
                              static_cast<uint32_t>(value.size()));

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  counter_++;
  estimate_ += buffer_.size() - curr_size;
}

}  // namespace rocksdb
