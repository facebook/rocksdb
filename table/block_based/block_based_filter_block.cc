//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based/block_based_filter_block.h"
#include <algorithm>

#include "db/dbformat.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based/block_based_table_reader.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace rocksdb {

namespace {

void AppendItem(std::string* props, const std::string& key,
                const std::string& value) {
  char cspace = ' ';
  std::string value_str("");
  size_t i = 0;
  const size_t dataLength = 64;
  const size_t tabLength = 2;
  const size_t offLength = 16;

  value_str.append(&value[i], std::min(size_t(dataLength), value.size()));
  i += dataLength;
  while (i < value.size()) {
    value_str.append("\n");
    value_str.append(offLength, cspace);
    value_str.append(&value[i], std::min(size_t(dataLength), value.size() - i));
    i += dataLength;
  }

  std::string result("");
  if (key.size() < (offLength - tabLength))
    result.append(size_t((offLength - tabLength)) - key.size(), cspace);
  result.append(key);

  props->append(result + ": " + value_str + "\n");
}

template <class TKey>
void AppendItem(std::string* props, const TKey& key, const std::string& value) {
  std::string key_str = rocksdb::ToString(key);
  AppendItem(props, key_str, value);
}
}  // namespace

// See doc/table_format.txt for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

BlockBasedFilterBlockBuilder::BlockBasedFilterBlockBuilder(
    const SliceTransform* prefix_extractor,
    const BlockBasedTableOptions& table_opt)
    : policy_(table_opt.filter_policy.get()),
      prefix_extractor_(prefix_extractor),
      whole_key_filtering_(table_opt.whole_key_filtering),
      prev_prefix_start_(0),
      prev_prefix_size_(0),
      num_added_(0) {
  assert(policy_);
}

void BlockBasedFilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void BlockBasedFilterBlockBuilder::Add(const Slice& key) {
  if (prefix_extractor_ && prefix_extractor_->InDomain(key)) {
    AddPrefix(key);
  }

  if (whole_key_filtering_) {
    AddKey(key);
  }
}

// Add key to filter if needed
inline void BlockBasedFilterBlockBuilder::AddKey(const Slice& key) {
  num_added_++;
  start_.push_back(entries_.size());
  entries_.append(key.data(), key.size());
}

// Add prefix to filter if needed
inline void BlockBasedFilterBlockBuilder::AddPrefix(const Slice& key) {
  // get slice for most recently added entry
  Slice prev;
  if (prev_prefix_size_ > 0) {
    prev = Slice(entries_.data() + prev_prefix_start_, prev_prefix_size_);
  }

  Slice prefix = prefix_extractor_->Transform(key);
  // insert prefix only when it's different from the previous prefix.
  if (prev.size() == 0 || prefix != prev) {
    prev_prefix_start_ = entries_.size();
    prev_prefix_size_ = prefix.size();
    AddKey(prefix);
  }
}

Slice BlockBasedFilterBlockBuilder::Finish(const BlockHandle& /*tmp*/,
                                           Status* status) {
  // In this impl we ignore BlockHandle
  *status = Status::OK();
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = static_cast<uint32_t>(result_.size());
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void BlockBasedFilterBlockBuilder::GenerateFilter() {
  const size_t num_entries = start_.size();
  if (num_entries == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(static_cast<uint32_t>(result_.size()));
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(entries_.size());  // Simplify length computation
  tmp_entries_.resize(num_entries);
  for (size_t i = 0; i < num_entries; i++) {
    const char* base = entries_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_entries_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(static_cast<uint32_t>(result_.size()));
  policy_->CreateFilter(&tmp_entries_[0], static_cast<int>(num_entries),
                        &result_);

  tmp_entries_.clear();
  entries_.clear();
  start_.clear();
  prev_prefix_start_ = 0;
  prev_prefix_size_ = 0;
}

BlockBasedFilterBlockReader::BlockBasedFilterBlockReader(
    const BlockBasedTable* t, CachableEntry<BlockContents>&& filter_block)
    : FilterBlockReaderCommon(t, std::move(filter_block)) {
  assert(table());
  assert(table()->get_rep());
  assert(table()->get_rep()->filter_policy);
}

std::unique_ptr<FilterBlockReader> BlockBasedFilterBlockReader::Create(
    const BlockBasedTable* table, FilePrefetchBuffer* prefetch_buffer,
    bool use_cache, bool prefetch, bool pin,
    BlockCacheLookupContext* lookup_context) {
  assert(table);
  assert(table->get_rep());
  assert(!pin || prefetch);

  CachableEntry<BlockContents> filter_block;
  if (prefetch || !use_cache) {
    const Status s = ReadFilterBlock(table, prefetch_buffer, ReadOptions(),
                                     use_cache, nullptr /* get_context */,
                                     lookup_context, &filter_block);
    if (!s.ok()) {
      return std::unique_ptr<FilterBlockReader>();
    }

    if (use_cache && !pin) {
      filter_block.Reset();
    }
  }

  return std::unique_ptr<FilterBlockReader>(
      new BlockBasedFilterBlockReader(table, std::move(filter_block)));
}

bool BlockBasedFilterBlockReader::KeyMayMatch(
    const Slice& key, const SliceTransform* /* prefix_extractor */,
    uint64_t block_offset, const bool no_io,
    const Slice* const /*const_ikey_ptr*/, GetContext* get_context,
    BlockCacheLookupContext* lookup_context) {
  assert(block_offset != kNotValid);
  if (!whole_key_filtering()) {
    return true;
  }
  return MayMatch(key, block_offset, no_io, get_context, lookup_context);
}

bool BlockBasedFilterBlockReader::PrefixMayMatch(
    const Slice& prefix, const SliceTransform* /* prefix_extractor */,
    uint64_t block_offset, const bool no_io,
    const Slice* const /*const_ikey_ptr*/, GetContext* get_context,
    BlockCacheLookupContext* lookup_context) {
  assert(block_offset != kNotValid);
  return MayMatch(prefix, block_offset, no_io, get_context, lookup_context);
}

bool BlockBasedFilterBlockReader::ParseFieldsFromBlock(
    const BlockContents& contents, const char** data, const char** offset,
    size_t* num, size_t* base_lg) {
  assert(data);
  assert(offset);
  assert(num);
  assert(base_lg);

  const size_t n = contents.data.size();
  if (n < 5) {  // 1 byte for base_lg and 4 for start of offset array
    return false;
  }

  const uint32_t last_word = DecodeFixed32(contents.data.data() + n - 5);
  if (last_word > n - 5) {
    return false;
  }

  *data = contents.data.data();
  *offset = (*data) + last_word;
  *num = (n - 5 - last_word) / 4;
  *base_lg = contents.data[n - 1];

  return true;
}

bool BlockBasedFilterBlockReader::MayMatch(
    const Slice& entry, uint64_t block_offset, bool no_io,
    GetContext* get_context, BlockCacheLookupContext* lookup_context) const {
  CachableEntry<BlockContents> filter_block;

  const Status s =
      GetOrReadFilterBlock(no_io, get_context, lookup_context, &filter_block);
  if (!s.ok()) {
    return true;
  }

  assert(filter_block.GetValue());

  const char* data = nullptr;
  const char* offset = nullptr;
  size_t num = 0;
  size_t base_lg = 0;
  if (!ParseFieldsFromBlock(*filter_block.GetValue(), &data, &offset, &num,
                            &base_lg)) {
    return true;  // Errors are treated as potential matches
  }

  const uint64_t index = block_offset >> base_lg;
  if (index < num) {
    const uint32_t start = DecodeFixed32(offset + index * 4);
    const uint32_t limit = DecodeFixed32(offset + index * 4 + 4);
    if (start <= limit && limit <= (uint32_t)(offset - data)) {
      const Slice filter = Slice(data + start, limit - start);

      assert(table());
      assert(table()->get_rep());
      const FilterPolicy* const policy = table()->get_rep()->filter_policy;

      const bool may_match = policy->KeyMayMatch(entry, filter);
      if (may_match) {
        PERF_COUNTER_ADD(bloom_sst_hit_count, 1);
        return true;
      } else {
        PERF_COUNTER_ADD(bloom_sst_miss_count, 1);
        return false;
      }
    } else if (start == limit) {
      // Empty filters do not match any entries
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

size_t BlockBasedFilterBlockReader::ApproximateMemoryUsage() const {
  size_t usage = ApproximateFilterBlockMemoryUsage();
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size(const_cast<BlockBasedFilterBlockReader*>(this));
#else
  usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
  return usage;
}

std::string BlockBasedFilterBlockReader::ToString() const {
  CachableEntry<BlockContents> filter_block;

  const Status s =
      GetOrReadFilterBlock(false /* no_io */, nullptr /* get_context */,
                           nullptr /* lookup_context */, &filter_block);
  if (!s.ok()) {
    return std::string("Unable to retrieve filter block");
  }

  assert(filter_block.GetValue());

  const char* data = nullptr;
  const char* offset = nullptr;
  size_t num = 0;
  size_t base_lg = 0;
  if (!ParseFieldsFromBlock(*filter_block.GetValue(), &data, &offset, &num,
                            &base_lg)) {
    return std::string("Error parsing filter block");
  }

  std::string result;
  result.reserve(1024);

  std::string s_bo("Block offset"), s_hd("Hex dump"), s_fb("# filter blocks");
  AppendItem(&result, s_fb, rocksdb::ToString(num));
  AppendItem(&result, s_bo, s_hd);

  for (size_t index = 0; index < num; index++) {
    uint32_t start = DecodeFixed32(offset + index * 4);
    uint32_t limit = DecodeFixed32(offset + index * 4 + 4);

    if (start != limit) {
      result.append(" filter block # " + rocksdb::ToString(index + 1) + "\n");
      Slice filter = Slice(data + start, limit - start);
      AppendItem(&result, start, filter.ToString(true));
    }
  }
  return result;
}

}  // namespace rocksdb
