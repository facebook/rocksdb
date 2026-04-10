//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>

#include "db/seqno_to_time_mapping.h"
#include "rocksdb/index_factory.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_type.h"
#include "table/block_based/index_builder.h"

namespace ROCKSDB_NAMESPACE {

// ---------------------------------------------------------------------------
// Adapter classes for custom IndexFactory implementations.
//
// IndexFactoryIteratorWrapper adapts IndexFactoryIterator (public interface,
// user keys) to InternalIteratorBase<IndexValue> (internal interface, internal
// keys). This adapter is necessary because BlockBasedTableIterator expects
// the internal iterator interface.
//
// IndexFactoryReaderWrapper dispatches NewIterator calls to either the
// standard IndexReader or the custom IndexFactoryReader based on the
// index_mode config (kPrimary/kPrimaryOnly) or ReadOptions::read_index.
// ---------------------------------------------------------------------------

// Forward declaration for the reader wrapper.
class IndexFactoryIteratorWrapper;

class IndexFactoryIteratorWrapper : public InternalIteratorBase<IndexValue> {
 public:
  explicit IndexFactoryIteratorWrapper(
      std::unique_ptr<IndexFactoryIterator>&& udi_iter)
      : udi_iter_(std::move(udi_iter)), valid_(false) {}

  ~IndexFactoryIteratorWrapper() override = default;

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    status_ = udi_iter_->SeekToFirstAndGetResult(&result_);
    UpdateValidAndKey();
  }

  void SeekToLast() override {
    status_ = udi_iter_->SeekToLastAndGetResult(&result_);
    UpdateValidAndKey();
  }

  void Seek(const Slice& target) override {
    ParsedInternalKey pkey;
    status_ = ParseInternalKey(target, &pkey, /*log_err_key=*/false);
    if (status_.ok()) {
      // Pass both user key AND sequence number to the UDI iterator via
      // SeekContext. The sequence number is needed when the same user key
      // spans multiple data blocks with different sequence numbers (e.g.,
      // due to snapshots). Without it, the UDI cannot distinguish which
      // block to return for a given (user_key, seqno) target.
      IndexFactoryIterator::SeekContext ctx;
      ctx.target_tag = PackSequenceAndType(pkey.sequence, pkey.type);
      status_ = udi_iter_->SeekAndGetResult(pkey.user_key, &result_, ctx);
    }
    UpdateValidAndKey();
  }

  void Next() override {
    status_ = udi_iter_->NextAndGetResult(&result_);
    UpdateValidAndKey();
  }

  bool NextAndGetResult(IterateResult* result) override {
    status_ = udi_iter_->NextAndGetResult(&result_);
    UpdateValidAndKey();
    if (status_.ok()) {
      if (valid_) {
        result->key = key();
      }
      result->bound_check_result = result_.bound_check_result;
      result->value_prepared = result_.value_prepared;
    }
    return valid_;
  }

  void SeekForPrev(const Slice& /*target*/) override {
    // BlockBasedTableIterator never calls SeekForPrev on the index
    // iterator. It uses Seek + FindKeyBackward(Prev) instead. The standard
    // index's IndexBlockIter::SeekForPrevImpl is also assert(false). Keep
    // this as NotSupported for safety.
    valid_ = false;
    status_ = Status::NotSupported("SeekForPrev not supported");
  }

  void Prev() override {
    status_ = udi_iter_->PrevAndGetResult(&result_);
    UpdateValidAndKey();
  }

  Slice key() const override { return Slice(*ikey_.const_rep()); }

  IndexValue value() const override { return cached_value_; }

  Status status() const override { return status_; }

  void Prepare(const MultiScanArgs* scan_opts) override {
    if (scan_opts) {
      udi_iter_->Prepare(scan_opts->GetScanRanges().data(),
                         scan_opts->GetScanRanges().size());
    }
  }

  IterBoundCheck UpperBoundCheckResult() override {
    return result_.bound_check_result;
  }

 private:
  // Common logic after every UDI positioning operation: check status,
  // update valid_, and build the internal key + cache the IndexValue if
  // valid.
  void UpdateValidAndKey() {
    if (status_.ok()) {
      // IndexFactoryIterator implementations must set
      // bound_check_result=kInbound when they have a valid result.
      // kUnknown and kOutOfBound both mean no valid position (the
      // iterator is exhausted or the key is outside bounds).
      valid_ = result_.bound_check_result == IterBoundCheck::kInbound;
      if (valid_) {
        SetInternalKeyFromUDIResult();
      }
    } else {
      valid_ = false;
    }
  }

  // Convert the UDI result's user key into an internal key for the index
  // iterator contract. UDI separators are user keys, but
  // InternalIteratorBase<IndexValue> must expose internal keys (user key +
  // 8-byte tag). We use seq=0 / kTypeValue so that the resulting
  // internal key compares as "greater than or equal to" any real data key
  // with the same user key (lower seqno = later in internal key order),
  // which is the correct upper-bound semantics for an index separator.
  void SetInternalKeyFromUDIResult() {
    ikey_.Set(result_.key, 0, ValueType::kTypeValue);
    CacheCurrentValue();
  }

  // Cache the IndexValue after each positioning operation so that repeated
  // value() calls (5-10 per block in BlockBasedTableIterator) are a simple
  // field return instead of a virtual dispatch through udi_iter_->value().
  void CacheCurrentValue() {
    auto handle = udi_iter_->value();
    cached_value_ =
        IndexValue(BlockHandle(handle.offset, handle.size), Slice());
  }

  std::unique_ptr<IndexFactoryIterator> udi_iter_;
  IterateResult result_;
  InternalKey ikey_;
  IndexValue cached_value_;
  Status status_;
  bool valid_;
};

class IndexFactoryReaderWrapper : public BlockBasedTable::IndexReader {
 public:
  // @udi_is_primary: use UDI for all reads (default dispatch), including
  //   internal operations like compaction and VerifyChecksum that don't
  //   set ReadOptions::read_index.
  IndexFactoryReaderWrapper(
      const std::string& name,
      std::unique_ptr<BlockBasedTable::IndexReader>&& reader,
      std::unique_ptr<IndexFactoryReader>&& udi_reader, bool udi_is_primary)
      : name_(name),
        reader_(std::move(reader)),
        udi_reader_(std::move(udi_reader)),
        udi_is_primary_(udi_is_primary) {}

  InternalIteratorBase<IndexValue>* NewIterator(
      const ReadOptions& read_options, bool disable_prefix_seek,
      IndexBlockIter* iter, GetContext* get_context,
      BlockCacheLookupContext* lookup_context) override {
    // Determine whether to use the UDI for this read:
    //   kDefault → udi_is_primary_ (kPrimary/kPrimaryOnly → custom,
    //              kBuiltinOnly/kSecondary → standard)
    //   kBuiltin → force standard index
    //   kCustom  → force custom index
    bool use_udi;
    switch (read_options.read_index) {
      case ReadOptions::ReadIndex::kBuiltin:
        use_udi = false;
        break;
      case ReadOptions::ReadIndex::kCustom:
        use_udi = true;
        break;
      case ReadOptions::ReadIndex::kDefault:
      default:
        use_udi = udi_is_primary_;
        break;
    }

    if (use_udi) {
      std::unique_ptr<IndexFactoryIterator> udi_iter =
          udi_reader_->NewIterator(read_options);
      if (udi_iter) {
        return new IndexFactoryIteratorWrapper(std::move(udi_iter));
      }
      return NewErrorInternalIterator<IndexValue>(
          Status::Corruption("Could not create UDI iterator"));
    }

    return reader_->NewIterator(read_options, disable_prefix_seek, iter,
                                get_context, lookup_context);
  }

  Status CacheDependencies(const ReadOptions& ro, bool pin,
                           FilePrefetchBuffer* tail_prefetch_buffer) override {
    // The standard index is fully populated in kSecondary and kPrimary
    // modes. In kPrimaryOnly, it is an empty stub.
    return reader_->CacheDependencies(ro, pin, tail_prefetch_buffer);
  }

  size_t ApproximateMemoryUsage() const override {
    size_t usage = udi_reader_->ApproximateMemoryUsage();
    usage += reader_->ApproximateMemoryUsage();
    return usage;
  }

  void EraseFromCacheBeforeDestruction(
      uint32_t uncache_aggressiveness) override {
    reader_->EraseFromCacheBeforeDestruction(uncache_aggressiveness);
  }

 private:
  const std::string name_;
  std::unique_ptr<BlockBasedTable::IndexReader> reader_;
  std::unique_ptr<IndexFactoryReader> udi_reader_;
  const bool udi_is_primary_;
};

}  // namespace ROCKSDB_NAMESPACE
