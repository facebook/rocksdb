//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/full_filter_block.h"

#include <array>

#include "block_type.h"
#include "monitoring/perf_context_imp.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based/block_based_table_reader.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

FullFilterBlockBuilder::FullFilterBlockBuilder(
    const SliceTransform* _prefix_extractor, bool whole_key_filtering,
    FilterBitsBuilder* filter_bits_builder)
    : prefix_extractor_(_prefix_extractor),
      whole_key_filtering_(whole_key_filtering),
      last_whole_key_recorded_(false),
      last_prefix_recorded_(false),
      last_key_in_domain_(false),
      any_added_(false) {
  assert(filter_bits_builder != nullptr);
  filter_bits_builder_.reset(filter_bits_builder);
}

size_t FullFilterBlockBuilder::EstimateEntriesAdded() {
  return filter_bits_builder_->EstimateEntriesAdded();
}

void FullFilterBlockBuilder::Add(const Slice& key_without_ts) {
  const bool add_prefix =
      prefix_extractor_ && prefix_extractor_->InDomain(key_without_ts);

  if (!last_prefix_recorded_ && last_key_in_domain_) {
    // We can reach here when a new filter partition starts in partitioned
    // filter. The last prefix in the previous partition should be added if
    // necessary regardless of key_without_ts, to support prefix SeekForPrev.
    AddKey(last_prefix_str_);
    last_prefix_recorded_ = true;
  }

  if (whole_key_filtering_) {
    if (!add_prefix) {
      AddKey(key_without_ts);
    } else {
      // if both whole_key and prefix are added to bloom then we will have whole
      // key_without_ts and prefix addition being interleaved and thus cannot
      // rely on the bits builder to properly detect the duplicates by comparing
      // with the last item.
      Slice last_whole_key = Slice(last_whole_key_str_);
      if (!last_whole_key_recorded_ ||
          last_whole_key.compare(key_without_ts) != 0) {
        AddKey(key_without_ts);
        last_whole_key_recorded_ = true;
        last_whole_key_str_.assign(key_without_ts.data(),
                                   key_without_ts.size());
      }
    }
  }
  if (add_prefix) {
    last_key_in_domain_ = true;
    AddPrefix(key_without_ts);
  } else {
    last_key_in_domain_ = false;
  }
}

// Add key to filter if needed
inline void FullFilterBlockBuilder::AddKey(const Slice& key) {
  filter_bits_builder_->AddKey(key);
  any_added_ = true;
}

// Add prefix to filter if needed
void FullFilterBlockBuilder::AddPrefix(const Slice& key) {
  assert(prefix_extractor_ && prefix_extractor_->InDomain(key));
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

Slice FullFilterBlockBuilder::Finish(
    const BlockHandle& /*tmp*/, Status* status,
    std::unique_ptr<const char[]>* filter_data) {
  Reset();
  // In this impl we ignore BlockHandle
  *status = Status::OK();
  if (any_added_) {
    any_added_ = false;
    Slice filter_content = filter_bits_builder_->Finish(
        filter_data ? filter_data : &filter_data_, status);
    return filter_content;
  }
  return Slice();
}

FullFilterBlockReader::FullFilterBlockReader(
    const BlockBasedTable* t,
    CachableEntry<ParsedFullFilterBlock>&& filter_block)
    : FilterBlockReaderCommon(t, std::move(filter_block)) {}

bool FullFilterBlockReader::KeyMayMatch(const Slice& key, const bool no_io,
                                        const Slice* const /*const_ikey_ptr*/,
                                        GetContext* get_context,
                                        BlockCacheLookupContext* lookup_context,
                                        Env::IOPriority rate_limiter_priority) {
  if (!whole_key_filtering()) {
    return true;
  }
  return MayMatch(key, no_io, get_context, lookup_context,
                  rate_limiter_priority);
}

std::unique_ptr<FilterBlockReader> FullFilterBlockReader::Create(
    const BlockBasedTable* table, const ReadOptions& ro,
    FilePrefetchBuffer* prefetch_buffer, bool use_cache, bool prefetch,
    bool pin, BlockCacheLookupContext* lookup_context) {
  assert(table);
  assert(table->get_rep());
  assert(!pin || prefetch);

  CachableEntry<ParsedFullFilterBlock> filter_block;
  if (prefetch || !use_cache) {
    const Status s = ReadFilterBlock(table, prefetch_buffer, ro, use_cache,
                                     nullptr /* get_context */, lookup_context,
                                     &filter_block);
    if (!s.ok()) {
      IGNORE_STATUS_IF_ERROR(s);
      return std::unique_ptr<FilterBlockReader>();
    }

    if (use_cache && !pin) {
      filter_block.Reset();
    }
  }

  return std::unique_ptr<FilterBlockReader>(
      new FullFilterBlockReader(table, std::move(filter_block)));
}

bool FullFilterBlockReader::PrefixMayMatch(
    const Slice& prefix, const bool no_io,
    const Slice* const /*const_ikey_ptr*/, GetContext* get_context,
    BlockCacheLookupContext* lookup_context,
    Env::IOPriority rate_limiter_priority) {
  return MayMatch(prefix, no_io, get_context, lookup_context,
                  rate_limiter_priority);
}

bool FullFilterBlockReader::MayMatch(
    const Slice& entry, bool no_io, GetContext* get_context,
    BlockCacheLookupContext* lookup_context,
    Env::IOPriority rate_limiter_priority) const {
  CachableEntry<ParsedFullFilterBlock> filter_block;

  const Status s = GetOrReadFilterBlock(no_io, get_context, lookup_context,
                                        &filter_block, rate_limiter_priority);
  if (!s.ok()) {
    IGNORE_STATUS_IF_ERROR(s);
    return true;
  }

  assert(filter_block.GetValue());

  FilterBitsReader* const filter_bits_reader =
      filter_block.GetValue()->filter_bits_reader();

  if (filter_bits_reader) {
    if (filter_bits_reader->MayMatch(entry)) {
      PERF_COUNTER_ADD(bloom_sst_hit_count, 1);
      return true;
    } else {
      PERF_COUNTER_ADD(bloom_sst_miss_count, 1);
      return false;
    }
  }
  return true;
}

void FullFilterBlockReader::KeysMayMatch(
    MultiGetRange* range, const bool no_io,
    BlockCacheLookupContext* lookup_context,
    Env::IOPriority rate_limiter_priority) {
  if (!whole_key_filtering()) {
    // Simply return. Don't skip any key - consider all keys as likely to be
    // present
    return;
  }
  MayMatch(range, no_io, nullptr, lookup_context, rate_limiter_priority);
}

void FullFilterBlockReader::PrefixesMayMatch(
    MultiGetRange* range, const SliceTransform* prefix_extractor,
    const bool no_io, BlockCacheLookupContext* lookup_context,
    Env::IOPriority rate_limiter_priority) {
  MayMatch(range, no_io, prefix_extractor, lookup_context,
           rate_limiter_priority);
}

void FullFilterBlockReader::MayMatch(
    MultiGetRange* range, bool no_io, const SliceTransform* prefix_extractor,
    BlockCacheLookupContext* lookup_context,
    Env::IOPriority rate_limiter_priority) const {
  CachableEntry<ParsedFullFilterBlock> filter_block;

  const Status s =
      GetOrReadFilterBlock(no_io, range->begin()->get_context, lookup_context,
                           &filter_block, rate_limiter_priority);
  if (!s.ok()) {
    IGNORE_STATUS_IF_ERROR(s);
    return;
  }

  assert(filter_block.GetValue());

  FilterBitsReader* const filter_bits_reader =
      filter_block.GetValue()->filter_bits_reader();

  if (!filter_bits_reader) {
    return;
  }

  // We need to use an array instead of autovector for may_match since
  // &may_match[0] doesn't work for autovector<bool> (compiler error). So
  // declare both keys and may_match as arrays, which is also slightly less
  // expensive compared to autovector
  std::array<Slice*, MultiGetContext::MAX_BATCH_SIZE> keys;
  std::array<bool, MultiGetContext::MAX_BATCH_SIZE> may_match = {{true}};
  autovector<Slice, MultiGetContext::MAX_BATCH_SIZE> prefixes;
  int num_keys = 0;
  MultiGetRange filter_range(*range, range->begin(), range->end());
  for (auto iter = filter_range.begin(); iter != filter_range.end(); ++iter) {
    if (!prefix_extractor) {
      keys[num_keys++] = &iter->ukey_without_ts;
    } else if (prefix_extractor->InDomain(iter->ukey_without_ts)) {
      prefixes.emplace_back(prefix_extractor->Transform(iter->ukey_without_ts));
      keys[num_keys++] = &prefixes.back();
    } else {
      filter_range.SkipKey(iter);
    }
  }

  filter_bits_reader->MayMatch(num_keys, &keys[0], &may_match[0]);

  int i = 0;
  for (auto iter = filter_range.begin(); iter != filter_range.end(); ++iter) {
    if (!may_match[i]) {
      // Update original MultiGet range to skip this key. The filter_range
      // was temporarily used just to skip keys not in prefix_extractor domain
      range->SkipKey(iter);
      PERF_COUNTER_ADD(bloom_sst_miss_count, 1);
    } else {
      // PERF_COUNTER_ADD(bloom_sst_hit_count, 1);
      PerfContext* perf_ctx = get_perf_context();
      perf_ctx->bloom_sst_hit_count++;
    }
    ++i;
  }
}

size_t FullFilterBlockReader::ApproximateMemoryUsage() const {
  size_t usage = ApproximateFilterBlockMemoryUsage();
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size(const_cast<FullFilterBlockReader*>(this));
#else
  usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
  return usage;
}

}  // namespace ROCKSDB_NAMESPACE
