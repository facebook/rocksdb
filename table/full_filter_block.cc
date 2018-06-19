//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/full_filter_block.h"

#include "monitoring/perf_context_imp.h"
#include "port/port.h"
#include "rocksdb/filter_policy.h"
#include "util/coding.h"

namespace rocksdb {

FullFilterBlockBuilder::FullFilterBlockBuilder(
    const SliceTransform* prefix_extractor, bool whole_key_filtering,
    FilterBitsBuilder* filter_bits_builder)
    : prefix_extractor_(prefix_extractor),
      whole_key_filtering_(whole_key_filtering),
      last_whole_key_recorded_(false),
      last_prefix_recorded_(false),
      num_added_(0) {
  assert(filter_bits_builder != nullptr);
  filter_bits_builder_.reset(filter_bits_builder);
}

void FullFilterBlockBuilder::Add(const Slice& key) {
  const bool add_prefix = prefix_extractor_ && prefix_extractor_->InDomain(key);
  if (whole_key_filtering_) {
    if (!add_prefix) {
      AddKey(key);
    } else {
      // if both whole_key and prefix are added to bloom then we will have whole
      // key and prefix addition being interleaved and thus cannot rely on the
      // bits builder to properly detect the duplicates by comparing with the
      // last item.
      Slice last_whole_key = Slice(last_whole_key_str_);
      if (!last_whole_key_recorded_ || last_whole_key.compare(key) != 0) {
        AddKey(key);
        last_whole_key_recorded_ = true;
        last_whole_key_str_.assign(key.data(), key.size());
      }
    }
  }
  if (add_prefix) {
    AddPrefix(key);
  }
}

// Add key to filter if needed
inline void FullFilterBlockBuilder::AddKey(const Slice& key) {
  filter_bits_builder_->AddKey(key);
  num_added_++;
}

// Add prefix to filter if needed
inline void FullFilterBlockBuilder::AddPrefix(const Slice& key) {
  Slice prefix = prefix_extractor_->Transform(key);
  if (whole_key_filtering_) {
    // if both whole_key and prefix are added to bloom then we will have whole
    // key and prefix addition being interleaved and thus cannot rely on the
    // bits builder to properly detect the duplicates by comparing with the last
    // item.
    Slice last_prefix = Slice(last_prefix_str_);
    if (!last_prefix_recorded_ || last_prefix.compare(prefix) != 0) {
      AddKey(prefix);
      last_prefix_recorded_ = true;
      last_prefix_str_.assign(prefix.data(), prefix.size());
    }
  } else {
    AddKey(prefix);
  }
}

void FullFilterBlockBuilder::Reset() {
  last_whole_key_recorded_ = false;
  last_prefix_recorded_ = false;
}

Slice FullFilterBlockBuilder::Finish(const BlockHandle& /*tmp*/,
                                     Status* status) {
  Reset();
  // In this impl we ignore BlockHandle
  *status = Status::OK();
  if (num_added_ != 0) {
    num_added_ = 0;
    return filter_bits_builder_->Finish(&filter_data_);
  }
  return Slice();
}

FullFilterBlockReader::FullFilterBlockReader(
    const SliceTransform* prefix_extractor, bool _whole_key_filtering,
    const Slice& contents, FilterBitsReader* filter_bits_reader,
    Statistics* stats)
    : FilterBlockReader(contents.size(), stats, _whole_key_filtering),
      prefix_extractor_(prefix_extractor),
      contents_(contents) {
  assert(filter_bits_reader != nullptr);
  filter_bits_reader_.reset(filter_bits_reader);
}

FullFilterBlockReader::FullFilterBlockReader(
    const SliceTransform* prefix_extractor, bool _whole_key_filtering,
    BlockContents&& contents, FilterBitsReader* filter_bits_reader,
    Statistics* stats)
    : FullFilterBlockReader(prefix_extractor, _whole_key_filtering,
                            contents.data, filter_bits_reader, stats) {
  block_contents_ = std::move(contents);
}

bool FullFilterBlockReader::KeyMayMatch(const Slice& key, uint64_t block_offset,
                                        const bool /*no_io*/,
                                        const Slice* const /*const_ikey_ptr*/) {
#ifdef NDEBUG
  (void)block_offset;
#endif
  assert(block_offset == kNotValid);
  if (!whole_key_filtering_) {
    return true;
  }
  return MayMatch(key);
}

bool FullFilterBlockReader::PrefixMayMatch(
    const Slice& prefix, uint64_t block_offset, const bool /*no_io*/,
    const Slice* const /*const_ikey_ptr*/) {
#ifdef NDEBUG
  (void)block_offset;
#endif
  assert(block_offset == kNotValid);
  if (!prefix_extractor_) {
    return true;
  }
  return MayMatch(prefix);
}

bool FullFilterBlockReader::MayMatch(const Slice& entry) {
  if (contents_.size() != 0)  {
    if (filter_bits_reader_->MayMatch(entry)) {
      PERF_COUNTER_ADD(bloom_sst_hit_count, 1);
      return true;
    } else {
      PERF_COUNTER_ADD(bloom_sst_miss_count, 1);
      return false;
    }
  }
  return true;  // remain the same with block_based filter
}

size_t FullFilterBlockReader::ApproximateMemoryUsage() const {
  return contents_.size();
}
}  // namespace rocksdb
