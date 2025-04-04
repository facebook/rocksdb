//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/sst_file_reader.h"

#include "db/arena_wrapped_db_iter.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "file/random_access_file_reader.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "table/get_context.h"
#include "table/table_builder.h"
#include "table/table_iterator.h"
#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE {

struct SstFileReader::Rep {
  Options options;
  EnvOptions soptions;
  ImmutableOptions ioptions;
  MutableCFOptions moptions;
  // Keep a member variable for this, since `NewIterator()` uses a const
  // reference of `ReadOptions`.
  ReadOptions roptions_for_table_iter;

  std::unique_ptr<TableReader> table_reader;

  Rep(const Options& opts)
      : options(opts),
        soptions(options),
        ioptions(options),
        moptions(ColumnFamilyOptions(options)) {
    roptions_for_table_iter =
        ReadOptions(/*_verify_checksums=*/true, /*_fill_cache=*/false);
  }
};

SstFileReader::SstFileReader(const Options& options) : rep_(new Rep(options)) {}

SstFileReader::~SstFileReader() = default;

Status SstFileReader::Open(const std::string& file_path) {
  auto r = rep_.get();
  Status s;
  uint64_t file_size = 0;
  std::unique_ptr<FSRandomAccessFile> file;
  std::unique_ptr<RandomAccessFileReader> file_reader;
  FileOptions fopts(r->soptions);
  const auto& fs = r->options.env->GetFileSystem();

  s = fs->GetFileSize(file_path, fopts.io_options, &file_size, nullptr);
  if (s.ok()) {
    s = fs->NewRandomAccessFile(file_path, fopts, &file, nullptr);
  }
  if (s.ok()) {
    file_reader.reset(new RandomAccessFileReader(std::move(file), file_path));
  }
  if (s.ok()) {
    TableReaderOptions t_opt(
        r->ioptions, r->moptions.prefix_extractor, r->soptions,
        r->ioptions.internal_comparator,
        r->moptions.block_protection_bytes_per_key,
        /*skip_filters*/ false, /*immortal*/ false,
        /*force_direct_prefetch*/ false, /*level*/ -1,
        /*block_cache_tracer*/ nullptr,
        /*max_file_size_for_l0_meta_pin*/ 0, /*cur_db_session_id*/ "",
        /*cur_file_num*/ 0,
        /* unique_id */ {}, /* largest_seqno */ 0,
        /* tail_size */ 0, r->ioptions.persist_user_defined_timestamps);
    // Allow open file with global sequence number for backward compatibility.
    t_opt.largest_seqno = kMaxSequenceNumber;
    s = r->options.table_factory->NewTableReader(t_opt, std::move(file_reader),
                                                 file_size, &r->table_reader);
  }
  return s;
}

std::vector<Status> SstFileReader::MultiGet(const ReadOptions& roptions,
                                            const std::vector<Slice>& keys,
                                            std::vector<std::string>* values) {
  const auto num_keys = keys.size();
  std::vector<Status> statuses(num_keys, Status::OK());
  std::vector<PinnableSlice> pin_values(num_keys);

  auto r = rep_.get();
  const Comparator* user_comparator =
      r->ioptions.internal_comparator.user_comparator();

  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  autovector<GetContext, MultiGetContext::MAX_BATCH_SIZE> get_ctx;
  autovector<MergeContext, MultiGetContext::MAX_BATCH_SIZE> merge_ctx;
  sorted_keys.resize(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    PinnableSlice* val = &pin_values[i];
    val->Reset();
    merge_ctx.emplace_back();
    key_context.emplace_back(nullptr, keys[i], val, nullptr,
                             nullptr /* timestamp */, &statuses[i]);
    get_ctx.emplace_back(user_comparator, r->ioptions.merge_operator.get(),
                         nullptr, nullptr, GetContext::kNotFound,
                         *key_context[i].key, val, nullptr, nullptr, nullptr,
                         &merge_ctx[i], true,
                         &key_context[i].max_covering_tombstone_seq, nullptr);
    key_context[i].get_context = &get_ctx[i];
  }
  for (size_t i = 0; i < num_keys; ++i) {
    sorted_keys[i] = &key_context[i];
  }

  struct CompareKeyContext {
    explicit CompareKeyContext(const Comparator* comp) : comparator(comp) {}
    inline bool operator()(const KeyContext* lhs, const KeyContext* rhs) const {
      return comparator->CompareWithoutTimestamp(*(lhs->key), false,
                                                 *(rhs->key), false) < 0;
    }
    const Comparator* comparator;
  };

  std::sort(sorted_keys.begin(), sorted_keys.end(),
            CompareKeyContext(user_comparator));
  const auto sequence = roptions.snapshot != nullptr
                            ? roptions.snapshot->GetSequenceNumber()
                            : kMaxSequenceNumber;
  MultiGetContext ctx(&sorted_keys, 0, num_keys, sequence, roptions,
                      r->ioptions.fs.get(), nullptr);
  MultiGetRange range = ctx.GetMultiGetRange();
  r->table_reader->MultiGet(roptions, &range,
                            r->moptions.prefix_extractor.get(),
                            false /* skip filters */);

  values->resize(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    if (statuses[i].ok()) {
      switch (get_ctx[i].State()) {
        case GetContext::kFound:
          (*values)[i].assign(pin_values[i].data(), pin_values[i].size());
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
      };
    }
  }
  return statuses;
}

Iterator* SstFileReader::NewIterator(const ReadOptions& roptions) {
  assert(roptions.io_activity == Env::IOActivity::kUnknown);
  auto r = rep_.get();
  auto sequence = roptions.snapshot != nullptr
                      ? roptions.snapshot->GetSequenceNumber()
                      : kMaxSequenceNumber;
  ArenaWrappedDBIter* res = new ArenaWrappedDBIter();
  res->Init(r->options.env, roptions, r->ioptions, r->moptions,
            nullptr /* version */, sequence, 0 /* version_number */,
            nullptr /* read_callback */, nullptr /* cfh */,
            true /* expose_blob_index */, false /* allow_refresh */,
            /*active_mem=*/nullptr);
  auto internal_iter = r->table_reader->NewIterator(
      res->GetReadOptions(), r->moptions.prefix_extractor.get(),
      res->GetArena(), false /* skip_filters */,
      TableReaderCaller::kSSTFileReader);
  res->SetIterUnderDBIter(internal_iter);
  return res;
}

std::unique_ptr<Iterator> SstFileReader::NewTableIterator() {
  auto r = rep_.get();
  InternalIterator* internal_iter = r->table_reader->NewIterator(
      r->roptions_for_table_iter, r->moptions.prefix_extractor.get(),
      /*arena*/ nullptr, false /* skip_filters */,
      TableReaderCaller::kSSTFileReader);
  assert(internal_iter);
  if (internal_iter == nullptr) {
    // Do not attempt to create a TableIterator if we cannot get a valid
    // InternalIterator.
    return nullptr;
  }
  return std::make_unique<TableIterator>(internal_iter);
}

std::shared_ptr<const TableProperties> SstFileReader::GetTableProperties()
    const {
  return rep_->table_reader->GetTableProperties();
}

Status SstFileReader::VerifyChecksum(const ReadOptions& read_options) {
  assert(read_options.io_activity == Env::IOActivity::kUnknown);
  return rep_->table_reader->VerifyChecksum(read_options,
                                            TableReaderCaller::kSSTFileReader);
}

Status SstFileReader::VerifyNumEntries(const ReadOptions& read_options) {
  Rep* r = rep_.get();
  std::unique_ptr<InternalIterator> internal_iter{r->table_reader->NewIterator(
      read_options, r->moptions.prefix_extractor.get(), nullptr,
      false /* skip_filters */, TableReaderCaller::kSSTFileReader)};
  internal_iter->SeekToFirst();
  Status s = internal_iter->status();
  if (!s.ok()) {
    return s;
  }
  uint64_t num_read = 0;
  for (; internal_iter->Valid(); internal_iter->Next()) {
    ++num_read;
  }
  s = internal_iter->status();
  if (!s.ok()) {
    return s;
  }
  std::shared_ptr<const TableProperties> tp = GetTableProperties();
  if (!tp) {
    s = Status::Corruption("table properties not available");
  } else {
    // TODO: verify num_range_deletions
    uint64_t expected = tp->num_entries - tp->num_range_deletions;
    if (num_read != expected) {
      std::ostringstream oss;
      oss << "Table property expects " << expected
          << " entries when excluding range deletions,"
          << " but scanning the table returned " << std::to_string(num_read)
          << " entries";
      s = Status::Corruption(oss.str());
    }
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
