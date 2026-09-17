// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "util/coro_utils.h"

#if defined(WITHOUT_COROUTINES) || \
    (defined(USE_COROUTINES) && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {

DEFINE_SYNC_AND_ASYNC(std::vector<Status>, SstFileReader::Rep::MultiGet)
(const ReadOptions& roptions, const std::vector<Slice>& keys,
 std::vector<PinnableSlice>* values) {
  const size_t num_keys = keys.size();
  std::vector<Status> statuses(num_keys);
  values->resize(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    (*values)[i].Reset();
  }

  const Comparator* user_comparator =
      ioptions.internal_comparator.user_comparator();
  Statistics* statistics = ioptions.stats;

  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  autovector<GetContext, MultiGetContext::MAX_BATCH_SIZE> get_ctx;
  autovector<MergeContext, MultiGetContext::MAX_BATCH_SIZE> merge_ctx;
  key_context.reserve(num_keys);
  get_ctx.reserve(num_keys);
  merge_ctx.reserve(num_keys);
  sorted_keys.resize(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    PinnableSlice* val = &(*values)[i];
    merge_ctx.emplace_back();
    key_context.emplace_back(nullptr, keys[i], val, nullptr,
                             nullptr /* timestamp */, &statuses[i]);
    get_ctx.emplace_back(
        user_comparator, ioptions.merge_operator.get(), nullptr /* logger */,
        statistics, GetContext::kNotFound, *key_context[i].key, val,
        nullptr /* columns */, nullptr /* timestamp */,
        nullptr /* value_found */, &merge_ctx[i], true,
        &key_context[i].max_covering_tombstone_seq, ioptions.clock);
    key_context[i].get_context = &get_ctx[i];
  }
  for (size_t i = 0; i < num_keys; ++i) {
    sorted_keys[i] = &key_context[i];
  }

  struct CompareKeyContext {
    explicit CompareKeyContext(const Comparator* comp) : comparator(comp) {}
    bool operator()(const KeyContext* lhs, const KeyContext* rhs) const {
      return comparator->CompareWithoutTimestamp(*(lhs->key), false,
                                                 *(rhs->key), false) < 0;
    }
    const Comparator* comparator;
  };

  std::sort(sorted_keys.begin(), sorted_keys.end(),
            CompareKeyContext(user_comparator));
  const SequenceNumber sequence = roptions.snapshot != nullptr
                                      ? roptions.snapshot->GetSequenceNumber()
                                      : kMaxSequenceNumber;
#ifdef WITH_COROUTINES
  constexpr bool kUseCoroRead = true;
#else
  constexpr bool kUseCoroRead = false;
#endif
  for (size_t start_key = 0; start_key < num_keys;
       start_key += MultiGetContext::MAX_BATCH_SIZE) {
    const size_t batch_size =
        std::min<size_t>(MultiGetContext::MAX_BATCH_SIZE, num_keys - start_key);
    MultiGetContext ctx(&sorted_keys, start_key, batch_size, sequence, roptions,
                        ioptions.fs.get(), nullptr, kUseCoroRead);
    MultiGetRange range = ctx.GetMultiGetRange();
    CO_AWAIT(table_reader->MultiGet, roptions, &range,
             moptions.prefix_extractor.get(), false /* skip filters */);
  }

  for (size_t i = 0; i < num_keys; ++i) {
    get_ctx[i].ReportCounters();

    if (statuses[i].ok()) {
      switch (get_ctx[i].State()) {
        case GetContext::kFound:
          break;
        case GetContext::kNotFound:
        case GetContext::kDeleted:
          statuses[i] = Status::NotFound();
          break;
        case GetContext::kMerge:
          statuses[i] = Status::MergeInProgress();
          break;
        case GetContext::kCorrupt:
        case GetContext::kUnexpectedBlobIndex:
        case GetContext::kMergeOperatorFailed:
          statuses[i] = Status::Corruption();
          break;
      }
    }
  }
  CO_RETURN statuses;
}

DEFINE_SYNC_AND_ASYNC(Status, SstFileReader::Rep::Get)
(const ReadOptions& roptions, const Slice& key, PinnableSlice* value) {
  value->Reset();

  const Comparator* user_comparator =
      ioptions.internal_comparator.user_comparator();
  Statistics* statistics = ioptions.stats;

  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;
  GetContext get_ctx(user_comparator, ioptions.merge_operator.get(),
                     nullptr /* logger */, statistics, GetContext::kNotFound,
                     key, value, nullptr /* columns */, nullptr /* timestamp */,
                     nullptr /* value_found */, &merge_context, true,
                     &max_covering_tombstone_seq, ioptions.clock);

  LookupKey lkey(key, kMaxSequenceNumber);
  Status status =
      CO_AWAIT(table_reader->Get, roptions, lkey.internal_key(), &get_ctx,
               moptions.prefix_extractor.get(), false /* skip_filters */);

  get_ctx.ReportCounters();

  if (status.ok()) {
    switch (get_ctx.State()) {
      case GetContext::kFound:
        break;
      case GetContext::kNotFound:
      case GetContext::kDeleted:
        status = Status::NotFound();
        break;
      case GetContext::kMerge:
        status = Status::MergeInProgress();
        break;
      case GetContext::kCorrupt:
      case GetContext::kUnexpectedBlobIndex:
      case GetContext::kMergeOperatorFailed:
        status = Status::Corruption();
        break;
    }
  }
  CO_RETURN status;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // defined(WITHOUT_COROUTINES) ||
        // (defined(USE_COROUTINES) && defined(WITH_COROUTINES))
