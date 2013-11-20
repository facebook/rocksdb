//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "db/dbformat.h"
#include "rocksdb/filter_policy.h"
#include "util/coding.h"

namespace rocksdb {

// See doc/table_format.txt for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const Options& opt)
                 : policy_(opt.filter_policy),
                   prefix_extractor_(opt.prefix_extractor),
                   whole_key_filtering_(opt.whole_key_filtering),
                   comparator_(opt.comparator){}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

bool FilterBlockBuilder::SamePrefix(const Slice &key1,
                                    const Slice &key2) const {
  if (!prefix_extractor_->InDomain(key1) &&
      !prefix_extractor_->InDomain(key2)) {
    return true;
  } else if (!prefix_extractor_->InDomain(key1) ||
             !prefix_extractor_->InDomain(key2)) {
    return false;
  } else {
    return (prefix_extractor_->Transform(key1) ==
            prefix_extractor_->Transform(key2));
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  // get slice for most recently added entry
  Slice prev;
  size_t added_to_start = 0;

  // add key to filter if needed
  if (whole_key_filtering_) {
    start_.push_back(entries_.size());
    ++added_to_start;
    entries_.append(key.data(), key.size());
  }

  if (start_.size() > added_to_start) {
    size_t prev_start = start_[start_.size() - 1 - added_to_start];
    const char* base = entries_.data() + prev_start;
    size_t length = entries_.size() - prev_start;
    prev = Slice(base, length);
  }

  // add prefix to filter if needed
  if (prefix_extractor_ && prefix_extractor_->InDomain(ExtractUserKey(key))) {
    // If prefix_extractor_, this filter_block layer assumes we only
    // operate on internal keys.
    Slice user_key = ExtractUserKey(key);
    // this assumes prefix(prefix(key)) == prefix(key), as the last
    // entry in entries_ may be either a key or prefix, and we use
    // prefix(last entry) to get the prefix of the last key.
    if (prev.size() == 0 ||
        !SamePrefix(user_key, ExtractUserKey(prev))) {
      Slice prefix = prefix_extractor_->Transform(user_key);
      InternalKey internal_prefix_tmp(prefix, 0, kTypeValue);
      Slice internal_prefix = internal_prefix_tmp.Encode();
      assert(comparator_->Compare(internal_prefix, key) <= 0);
      start_.push_back(entries_.size());
      entries_.append(internal_prefix.data(), internal_prefix.size());
    }
  }
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_entries = start_.size();
  if (num_entries == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(entries_.size());  // Simplify length computation
  tmp_entries_.resize(num_entries);
  for (size_t i = 0; i < num_entries; i++) {
    const char* base = entries_.data() + start_[i];
    size_t length = start_[i+1] - start_[i];
    tmp_entries_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_entries_[0], num_entries, &result_);

  tmp_entries_.clear();
  entries_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(
    const Options& opt, const Slice& contents, bool delete_contents_after_use)
    : policy_(opt.filter_policy),
      prefix_extractor_(opt.prefix_extractor),
      whole_key_filtering_(opt.whole_key_filtering),
      data_(nullptr),
      offset_(nullptr),
      num_(0),
      base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n-1];
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;
  if (delete_contents_after_use) {
    filter_data.reset(contents.data());
  }
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset,
                                    const Slice& key) {
  if (!whole_key_filtering_) {
    return true;
  }
  return MayMatch(block_offset, key);
}

bool FilterBlockReader::PrefixMayMatch(uint64_t block_offset,
                                       const Slice& prefix) {
  if (!prefix_extractor_) {
    return true;
  }
  return MayMatch(block_offset, prefix);
}

bool FilterBlockReader::MayMatch(uint64_t block_offset, const Slice& entry) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index*4);
    uint32_t limit = DecodeFixed32(offset_ + index*4 + 4);
    if (start <= limit && limit <= (offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(entry, filter);
    } else if (start == limit) {
      // Empty filters do not match any entries
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}
