//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/external_table.h"

#include "rocksdb/table.h"
#include "table/internal_iterator.h"
#include "table/table_builder.h"
#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE {

namespace {

class ExternalTableIteratorAdapter : public InternalIterator {
 public:
  explicit ExternalTableIteratorAdapter(ExternalTableIterator* iterator)
      : iterator_(iterator), valid_(false) {}

  // No copying allowed
  ExternalTableIteratorAdapter(const ExternalTableIteratorAdapter&) = delete;
  ExternalTableIteratorAdapter& operator=(const ExternalTableIteratorAdapter&) =
      delete;

  ~ExternalTableIteratorAdapter() override {}

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    status_ = Status::OK();
    if (iterator_) {
      iterator_->SeekToFirst();
      UpdateKey(OptSlice());
    }
  }

  void SeekToLast() override {
    status_ = Status::OK();
    if (iterator_) {
      iterator_->SeekToLast();
      UpdateKey(OptSlice());
    }
  }

  void Seek(const Slice& target) override {
    status_ = Status::OK();
    if (iterator_) {
      ParsedInternalKey pkey;
      status_ = ParseInternalKey(target, &pkey, /*log_err_key=*/false);
      if (status_.ok()) {
        iterator_->Seek(pkey.user_key);
        UpdateKey(OptSlice());
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
        UpdateKey(OptSlice());
      }
    }
  }

  void Next() override {
    if (iterator_) {
      iterator_->Next();
      UpdateKey(OptSlice());
    }
  }

  bool NextAndGetResult(IterateResult* result) override {
    if (iterator_) {
      valid_ = iterator_->NextAndGetResult(&result_);
      result->value_prepared = result_.value_prepared;
      result->bound_check_result = result_.bound_check_result;
      if (valid_) {
        UpdateKey(result_.key);
        result->key = key();
      }
    } else {
      valid_ = false;
    }
    return valid_;
  }

  bool PrepareValue() override {
    if (iterator_ && !result_.value_prepared) {
      valid_ = iterator_->PrepareValue();
      result_.value_prepared = true;
    }
    return valid_;
  }

  IterBoundCheck UpperBoundCheckResult() override {
    if (iterator_) {
      result_.bound_check_result = iterator_->UpperBoundCheckResult();
    }
    return result_.bound_check_result;
  }

  void Prev() override {
    if (iterator_) {
      iterator_->Prev();
      UpdateKey(OptSlice());
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

  Status status() const override { return status_; }

  void Prepare(const std::vector<ScanOptions>* scan_opts) override {
    if (iterator_) {
      iterator_->Prepare(scan_opts->data(), scan_opts->size());
    }
  }

 private:
  std::unique_ptr<ExternalTableIterator> iterator_;
  InternalKey key_;
  bool valid_;
  Status status_;
  IterateResult result_;

  void UpdateKey(OptSlice res) {
    if (iterator_) {
      valid_ = iterator_->Valid();
      status_ = iterator_->status();
      if (valid_ && status_.ok()) {
        key_.Set(res.has_value() ? res.value() : iterator_->key(), 0,
                 ValueType::kTypeValue);
      }
    }
  }
};

class ExternalTableReaderAdapter : public TableReader {
 public:
  explicit ExternalTableReaderAdapter(
      std::unique_ptr<ExternalTableReader>&& reader)
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
      return new ExternalTableIteratorAdapter(iterator);
    } else {
      auto* mem = arena->AllocateAligned(sizeof(ExternalTableIteratorAdapter));
      return new (mem) ExternalTableIteratorAdapter(iterator);
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

class ExternalTableBuilderAdapter : public TableBuilder {
 public:
  explicit ExternalTableBuilderAdapter(
      std::unique_ptr<ExternalTableBuilder>&& builder)
      : builder_(std::move(builder)), num_entries_(0) {}

  void Add(const Slice& key, const Slice& value) override {
    ParsedInternalKey pkey;
    status_ = ParseInternalKey(key, &pkey, /*log_err_key=*/false);
    if (status_.ok()) {
      if (pkey.type != ValueType::kTypeValue) {
        status_ = Status::NotSupported(
            "Value type " + std::to_string(pkey.type) + "not supported");
      } else {
        builder_->Add(pkey.user_key, value);
        num_entries_++;
      }
    }
  }

  Status status() const override {
    if (status_.ok()) {
      return builder_->status();
    } else {
      return status_;
    }
  }

  IOStatus io_status() const override { return status_to_io_status(status()); }

  Status Finish() override { return builder_->Finish(); }

  void Abandon() override { builder_->Abandon(); }

  uint64_t FileSize() const override { return builder_->FileSize(); }

  uint64_t NumEntries() const override { return num_entries_; }

  TableProperties GetTableProperties() const override {
    return builder_->GetTableProperties();
  }

  std::string GetFileChecksum() const override {
    return builder_->GetFileChecksum();
  }

  const char* GetFileChecksumFuncName() const override {
    return builder_->GetFileChecksumFuncName();
  }

 private:
  Status status_;
  std::unique_ptr<ExternalTableBuilder> builder_;
  uint64_t num_entries_;
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
    FileOptions fopts(topts.env_options);
    ExternalTableOptions ext_topts(topts.prefix_extractor,
                                   topts.ioptions.user_comparator,
                                   topts.ioptions.fs, fopts);
    auto status =
        inner_->NewTableReader(ro, file->file_name(), ext_topts, &reader);
    if (!status.ok()) {
      return status;
    }
    table_reader->reset(new ExternalTableReaderAdapter(std::move(reader)));
    file.reset();
    return Status::OK();
  }

  using TableFactory::NewTableBuilder;
  TableBuilder* NewTableBuilder(const TableBuilderOptions& topts,
                                WritableFileWriter* file) const override {
    std::unique_ptr<ExternalTableBuilder> builder;
    ExternalTableBuilderOptions ext_topts(
        topts.read_options, topts.write_options,
        topts.moptions.prefix_extractor, topts.ioptions.user_comparator,
        topts.column_family_name, topts.reason);
    builder.reset(inner_->NewTableBuilder(ext_topts, file->file_name(),
                                          file->writable_file()));
    if (builder) {
      return new ExternalTableBuilderAdapter(std::move(builder));
    }
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
