//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/external_table_reader.h"

#include "rocksdb/table.h"
#include "table/internal_iterator.h"
#include "table/table_builder.h"
#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE {

namespace {

class ExternalTableIterator : public InternalIterator {
 public:
  explicit ExternalTableIterator(Iterator* iterator) : iterator_(iterator) {}

  // No copying allowed
  ExternalTableIterator(const ExternalTableIterator&) = delete;
  ExternalTableIterator& operator=(const ExternalTableIterator&) = delete;

  ~ExternalTableIterator() override {}

  bool Valid() const override { return iterator_ && iterator_->Valid(); }

  void SeekToFirst() override {
    status_ = Status::OK();
    if (iterator_) {
      iterator_->SeekToFirst();
      UpdateKey();
    }
  }

  void SeekToLast() override {
    status_ = Status::OK();
    if (iterator_) {
      iterator_->SeekToLast();
      UpdateKey();
    }
  }

  void Seek(const Slice& target) override {
    status_ = Status::OK();
    if (iterator_) {
      ParsedInternalKey pkey;
      status_ = ParseInternalKey(target, &pkey, /*log_err_key=*/false);
      if (status_.ok()) {
        iterator_->Seek(pkey.user_key);
        UpdateKey();
      }
    }
  }

  void SeekForPrev(const Slice& target) override {
    status_ = Status::OK();
    if (iterator_) {
      ParsedInternalKey pkey;
      status_ = ParseInternalKey(target, &pkey, /*log_err_key=*/false);
      if (status_.ok()) {
        iterator_->SeekForPrev(pkey.user_key);
        UpdateKey();
      }
    }
  }

  void Next() override {
    if (iterator_) {
      iterator_->Next();
      UpdateKey();
    }
  }

  void Prev() override {
    if (iterator_) {
      iterator_->Prev();
      UpdateKey();
    }
  }

  Slice key() const override {
    if (iterator_) {
      return Slice(*key_.const_rep());
    }
    return Slice();
  }

  Slice value() const override {
    if (iterator_) {
      return iterator_->value();
    }
    return Slice();
  }

  Status status() const override {
    return !status_.ok() ? status_
                         : (iterator_ ? iterator_->status() : Status::OK());
  }

 private:
  std::unique_ptr<Iterator> iterator_;
  InternalKey key_;
  Status status_;

  void UpdateKey() { key_.Set(iterator_->key(), 0, ValueType::kTypeValue); }
};

class ExternalTableReaderAdapter : public TableReader {
 public:
  explicit ExternalTableReaderAdapter(
      std::unique_ptr<ExternalTableReader> reader)
      : reader_(std::move(reader)) {}

  ~ExternalTableReaderAdapter() override {}

  // No copying allowed
  ExternalTableReaderAdapter(const ExternalTableReaderAdapter&) = delete;
  ExternalTableReaderAdapter& operator=(const ExternalTableReaderAdapter&) =
      delete;

  InternalIterator* NewIterator(
      const ReadOptions& read_options, const SliceTransform* prefix_extractor,
      Arena* arena, bool /* skip_filters */, TableReaderCaller /* caller */,
      size_t /* compaction_readahead_size */ = 0,
      bool /* allow_unprepared_value */ = false) override {
    auto iterator = reader_->NewIterator(read_options, prefix_extractor);
    if (arena == nullptr) {
      return new ExternalTableIterator(iterator);
    } else {
      auto* mem = arena->AllocateAligned(sizeof(ExternalTableIterator));
      return new (mem) ExternalTableIterator(iterator);
    }
  }

  uint64_t ApproximateOffsetOf(const ReadOptions&, const Slice&,
                               TableReaderCaller) override {
    return 0;
  }

  uint64_t ApproximateSize(const ReadOptions&, const Slice&, const Slice&,
                           TableReaderCaller) override {
    return 0;
  }

  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    std::shared_ptr<TableProperties> props =
        std::make_shared<TableProperties>(*reader_->GetTableProperties());
    props->key_largest_seqno = 0;
    return props;
  }

  size_t ApproximateMemoryUsage() const override { return 0; }

  Status Get(const ReadOptions&, const Slice&, GetContext*,
             const SliceTransform*, bool = false) override {
    return Status::NotSupported(
        "Get() not supported on external file iterator");
  }

  virtual Status VerifyChecksum(const ReadOptions& /*ro*/,
                                TableReaderCaller /*caller*/) override {
    return Status::OK();
  }

 private:
  std::unique_ptr<ExternalTableReader> reader_;
};

class ExternalTableFactoryAdapter : public TableFactory {
 public:
  explicit ExternalTableFactoryAdapter(
      std::shared_ptr<ExternalTableFactory> inner)
      : inner_(std::move(inner)) {}

  const char* Name() const override { return inner_->Name(); }

  using TableFactory::NewTableReader;
  Status NewTableReader(
      const ReadOptions& ro, const TableReaderOptions& topts,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t /* file_size */,
      std::unique_ptr<TableReader>* table_reader,
      bool /* prefetch_index_and_filter_in_cache */) const override {
    std::unique_ptr<ExternalTableReader> reader;
    ExternalTableOptions ext_topts(topts.prefix_extractor,
                                   topts.ioptions.user_comparator);
    auto status =
        inner_->NewTableReader(ro, file->file_name(), ext_topts, &reader);
    if (!status.ok()) {
      return status;
    }
    table_reader->reset(new ExternalTableReaderAdapter(std::move(reader)));
    file.reset();
    return Status::OK();
  }

  TableBuilder* NewTableBuilder(const TableBuilderOptions&,
                                WritableFileWriter*) const override {
    return nullptr;
  }

  std::unique_ptr<TableFactory> Clone() const override { return nullptr; }

 private:
  std::shared_ptr<ExternalTableFactory> inner_;
};

}  // namespace

std::shared_ptr<TableFactory> NewExternalTableFactory(
    std::shared_ptr<ExternalTableFactory> inner_factory) {
  std::shared_ptr<TableFactory> res;
  res.reset(new ExternalTableFactoryAdapter(std::move(inner_factory)));
  return res;
}

}  // namespace ROCKSDB_NAMESPACE
