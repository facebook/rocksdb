//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <array>
#include <vector>

#include "util/coro_utils.h"

#if defined(WITHOUT_COROUTINES) || \
    (defined(USE_COROUTINES) && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {
namespace {

template <ExternalTableMode Mode>
DEFINE_SYNC_AND_ASYNC(Status, ExternalTableReaderAdapter<Mode>::Get)(
    const ReadOptions& read_options, const Slice& key, GetContext* get_context,
    const SliceTransform* prefix_extractor, bool /*skip_filters*/) {
  // TableReader reports a miss as OK without modifying GetContext.
  if constexpr (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts) {
    ParsedEntryInfo lookup_key;
    Status status =
        ParseEntry(key, internal_comparator_.user_comparator(), &lookup_key);
    if (!status.ok()) {
      CO_RETURN status;
    }
    // A file-wide sequence newer than the snapshot makes the whole table
    // invisible to this lookup.
    if (global_seqno_ != kDisableGlobalSequenceNumber &&
        global_seqno_ > lookup_key.sequence) {
      CO_RETURN Status::OK();
    }

    PinnableSlice value;
#if defined(WITH_COROUTINES)
    if (auto* coro_reader = reader_->GetCoroExternalTableReader()) {
      status = CO_AWAIT(coro_reader->Get, read_options, lookup_key.user_key,
                        prefix_extractor, &value);
    } else
#endif
    {
      status = reader_->Get(read_options, lookup_key.user_key, prefix_extractor,
                            &value);
    }
    if (status.IsNotFound()) {
      CO_RETURN Status::OK();
    }
    if (!status.ok()) {
      CO_RETURN status;
    }
    if (global_seqno_ == kDisableGlobalSequenceNumber || global_seqno_ == 0) {
      get_context->SaveValue(value, /*seq=*/0,
                             value.IsPinned() ? &value : nullptr);
      CO_RETURN Status::OK();
    }
    const ParsedInternalKey parsed_key(lookup_key.user_key, global_seqno_,
                                       kTypeValue);
    bool matched = false;
    Status read_status;
    get_context->SaveValue(parsed_key, value, &matched, &read_status,
                           value.IsPinned() ? &value : nullptr);
    CO_RETURN read_status;
  } else {
    Slice external_lookup_key = key;
    IterKey stored_lookup_key;
    Status status = SetExternalTableGlobalSeqnoLookupKey(
        &external_lookup_key, global_seqno_, kValueTypeForSeek,
        &stored_lookup_key);
    if (!status.ok()) {
      CO_RETURN status;
    }
    ExternalTableGetContextImpl context(get_context, &internal_comparator_, key,
                                        global_seqno_);
#if defined(WITH_COROUTINES)
    if (auto* coro_reader = reader_->GetCoroExternalTableReader()) {
      status = CO_AWAIT(coro_reader->Get, read_options, external_lookup_key,
                        prefix_extractor, &context);
    } else
#endif
    {
      status = reader_->Get(read_options, external_lookup_key, prefix_extractor,
                            &context);
    }
    if (status.IsNotFound()) {
      CO_RETURN Status::OK();
    }
    CO_RETURN status;
  }
}

template <ExternalTableMode Mode>
DEFINE_SYNC_AND_ASYNC(void, ExternalTableReaderAdapter<Mode>::MultiGet)(
    const ReadOptions& read_options, const MultiGetContext::Range* mget_range,
    const SliceTransform* prefix_extractor, bool /*skip_filters*/) {
  if constexpr (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts) {
    const size_t num_keys = mget_range->KeysLeft();
    std::vector<Slice> lookup_keys;
    lookup_keys.reserve(num_keys);
    // KeysLeft() is bounded by MultiGetContext::MAX_BATCH_SIZE.
    std::array<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> key_contexts{};

    size_t index = 0;
    for (auto iter = mget_range->begin(); iter != mget_range->end(); ++iter) {
      ParsedEntryInfo lookup_key;
      Status status = ParseEntry(
          iter->ikey, internal_comparator_.user_comparator(), &lookup_key);
      if (!status.ok()) {
        *iter->s = status;
        continue;
      }
      if (global_seqno_ != kDisableGlobalSequenceNumber &&
          global_seqno_ > lookup_key.sequence) {
        *iter->s = Status::OK();
        continue;
      }
      lookup_keys.push_back(lookup_key.user_key);
      key_contexts[index] = &*iter;
      ++index;
    }
    if (lookup_keys.empty()) {
      CO_RETURN;
    }

    std::vector<PinnableSlice> values(lookup_keys.size());
    std::vector<Status> statuses(lookup_keys.size());
#if defined(WITH_COROUTINES)
    if (auto* coro_reader = reader_->GetCoroExternalTableReader()) {
      CO_AWAIT(coro_reader->MultiGet, read_options, lookup_keys,
               prefix_extractor, &values, &statuses);
    } else
#endif
    {
      reader_->MultiGet(read_options, lookup_keys, prefix_extractor, &values,
                        &statuses);
    }
    for (size_t i = 0; i < statuses.size(); ++i) {
      if (statuses[i].IsNotFound()) {
        *key_contexts[i]->s = Status::OK();
      } else if (!statuses[i].ok()) {
        *key_contexts[i]->s = statuses[i];
      } else if (global_seqno_ == kDisableGlobalSequenceNumber ||
                 global_seqno_ == 0) {
        key_contexts[i]->get_context->SaveValue(
            values[i], /*seq=*/0, values[i].IsPinned() ? &values[i] : nullptr);
        *key_contexts[i]->s = Status::OK();
      } else {
        const ParsedInternalKey parsed_key(lookup_keys[i], global_seqno_,
                                           kTypeValue);
        bool matched = false;
        Status read_status;
        key_contexts[i]->get_context->SaveValue(
            parsed_key, values[i], &matched, &read_status,
            values[i].IsPinned() ? &values[i] : nullptr);
        *key_contexts[i]->s = std::move(read_status);
      }
    }
  } else {
    ExternalTableMultiGetContextImpl context(*mget_range, internal_comparator_,
                                             global_seqno_);
    if (context.Size() == 0) {
      CO_RETURN;
    }

    std::vector<Status> statuses(context.Size());
#if defined(WITH_COROUTINES)
    if (auto* coro_reader = reader_->GetCoroExternalTableReader()) {
      CO_AWAIT(coro_reader->MultiGet, read_options, prefix_extractor, &context,
               &statuses);
    } else
#endif
    {
      reader_->MultiGet(read_options, prefix_extractor, &context, &statuses);
    }
    context.ApplyStatuses(&statuses);
  }
  CO_RETURN;
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

#endif
