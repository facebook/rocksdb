//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "table/block_based/uncompression_dict_reader.h"
#include "monitoring/perf_context_imp.h"
#include "table/block_based/block_based_table_reader.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

Status UncompressionDictReader::Create(
    const BlockBasedTable* table, const ReadOptions& ro,
    FilePrefetchBuffer* prefetch_buffer, bool use_cache, bool prefetch,
    bool pin, BlockCacheLookupContext* lookup_context,
    std::unique_ptr<UncompressionDictReader>* uncompression_dict_reader) {
  assert(table);
  assert(table->get_rep());
  assert(!pin || prefetch);
  assert(uncompression_dict_reader);

  CachableEntry<UncompressionDict> uncompression_dict;
  if (prefetch || !use_cache) {
    const Status s = ReadUncompressionDictionary(
        table, prefetch_buffer, ro, use_cache, nullptr /* get_context */,
        lookup_context, &uncompression_dict);
    if (!s.ok()) {
      return s;
    }

    if (use_cache && !pin) {
      uncompression_dict.Reset();
    }
  }

  uncompression_dict_reader->reset(
      new UncompressionDictReader(table, std::move(uncompression_dict)));

  return Status::OK();
}

Status UncompressionDictReader::ReadUncompressionDictionary(
    const BlockBasedTable* table, FilePrefetchBuffer* prefetch_buffer,
    const ReadOptions& read_options, bool use_cache, GetContext* get_context,
    BlockCacheLookupContext* lookup_context,
    CachableEntry<UncompressionDict>* uncompression_dict) {
  // TODO: add perf counter for compression dictionary read time

  assert(table);
  assert(uncompression_dict);
  assert(uncompression_dict->IsEmpty());

  const BlockBasedTable::Rep* const rep = table->get_rep();
  assert(rep);
  assert(!rep->compression_dict_handle.IsNull());

  const Status s = table->RetrieveBlock(
      prefetch_buffer, read_options, rep->compression_dict_handle,
      UncompressionDict::GetEmptyDict(), uncompression_dict,
      BlockType::kCompressionDictionary, get_context, lookup_context,
      /* for_compaction */ false, use_cache, /* wait_for_cache */ true);

  if (!s.ok()) {
    ROCKS_LOG_WARN(
        rep->ioptions.logger,
        "Encountered error while reading data from compression dictionary "
        "block %s",
        s.ToString().c_str());
  }

  return s;
}

Status UncompressionDictReader::GetOrReadUncompressionDictionary(
    FilePrefetchBuffer* prefetch_buffer, bool no_io, GetContext* get_context,
    BlockCacheLookupContext* lookup_context,
    CachableEntry<UncompressionDict>* uncompression_dict) const {
  assert(uncompression_dict);

  if (!uncompression_dict_.IsEmpty()) {
    uncompression_dict->SetUnownedValue(uncompression_dict_.GetValue());
    return Status::OK();
  }

  ReadOptions read_options;
  if (no_io) {
    read_options.read_tier = kBlockCacheTier;
  }

  return ReadUncompressionDictionary(table_, prefetch_buffer, read_options,
                                     cache_dictionary_blocks(), get_context,
                                     lookup_context, uncompression_dict);
}

size_t UncompressionDictReader::ApproximateMemoryUsage() const {
  assert(!uncompression_dict_.GetOwnValue() ||
         uncompression_dict_.GetValue() != nullptr);
  size_t usage = uncompression_dict_.GetOwnValue()
                     ? uncompression_dict_.GetValue()->ApproximateMemoryUsage()
                     : 0;

#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size(const_cast<UncompressionDictReader*>(this));
#else
  usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE

  return usage;
}

bool UncompressionDictReader::cache_dictionary_blocks() const {
  assert(table_);
  assert(table_->get_rep());

  return table_->get_rep()->table_options.cache_index_and_filter_blocks;
}

}  // namespace ROCKSDB_NAMESPACE
