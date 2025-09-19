//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/external_table.h"

#include "logging/logging.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
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

  void Prepare(const MultiScanArgs* scan_opts) override {
    if (iterator_ && scan_opts) {
      iterator_->Prepare(scan_opts->GetScanRanges().data(), scan_opts->size());
    } else if (iterator_) {
      iterator_->Prepare(nullptr, 0);
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
      const ImmutableOptions& ioptions,
      std::unique_ptr<ExternalTableReader>&& reader)
      : ioptions_(ioptions), reader_(std::move(reader)) {}

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
    std::shared_ptr<TableProperties> props;
    std::unique_ptr<char[]> property_block;
    uint64_t property_block_size = 0;
    uint64_t property_block_offset = 0;
    Status s;
    // Get the raw properties block from the external table reader. We don't
    // support writing the global sequence number, but we still get and return
    // the correct global seqno offset in the file to prevent accidental
    // corruption.
    s = reader_->GetPropertiesBlock(&property_block, &property_block_size,
                                    &property_block_offset);
    if (s.ok()) {
      std::unique_ptr<TableProperties> table_properties =
          std::make_unique<TableProperties>();
      BlockContents block_contents(std::move(property_block),
                                   property_block_size);
      Block block(std::move(block_contents));
      s = ParsePropertiesBlock(ioptions_, property_block_offset, block,
                               table_properties);
      if (s.ok()) {
        props.reset(table_properties.release());
      }
    } else {
      // Fallback to getting a minimal table properties structure from the
      // external table reader
      props = std::make_shared<TableProperties>(*reader_->GetTableProperties());
      props->key_largest_seqno = 0;
      props->key_smallest_seqno = 0;
    }
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
  const ImmutableOptions& ioptions_;
  std::unique_ptr<ExternalTableReader> reader_;
};

class ExternalTableBuilderAdapter : public TableBuilder {
 public:
  explicit ExternalTableBuilderAdapter(
      const TableBuilderOptions& topts,
      std::unique_ptr<ExternalTableBuilder>&& builder,
      std::unique_ptr<FSWritableFile>&& file)
      : builder_(std::move(builder)),
        file_(std::move(file)),
        ioptions_(topts.ioptions) {
    properties_.num_data_blocks = 1;
    properties_.index_size = 0;
    properties_.filter_size = 0;
    properties_.format_version = 0;
    properties_.key_largest_seqno = 0;
    properties_.key_smallest_seqno = 0;
    properties_.column_family_id = topts.column_family_id;
    properties_.column_family_name = topts.column_family_name;
    properties_.db_id = topts.db_id;
    properties_.db_session_id = topts.db_session_id;
    properties_.db_host_id = topts.ioptions.db_host_id;
    if (!ReifyDbHostIdProperty(topts.ioptions.env, &properties_.db_host_id)
             .ok()) {
      ROCKS_LOG_INFO(topts.ioptions.logger,
                     "db_host_id property will not be set");
    }
    properties_.orig_file_number = topts.cur_file_num;
    properties_.comparator_name = topts.ioptions.user_comparator != nullptr
                                      ? topts.ioptions.user_comparator->Name()
                                      : "nullptr";
    properties_.prefix_extractor_name =
        topts.moptions.prefix_extractor != nullptr
            ? topts.moptions.prefix_extractor->AsString()
            : "nullptr";

    for (auto& factory : *topts.internal_tbl_prop_coll_factories) {
      assert(factory);
      std::unique_ptr<InternalTblPropColl> collector{
          factory->CreateInternalTblPropColl(topts.column_family_id,
                                             topts.level_at_creation,
                                             topts.ioptions.num_levels)};
      if (collector) {
        table_properties_collectors_.emplace_back(std::move(collector));
      }
    }
  }

  void Add(const Slice& key, const Slice& value) override {
    ParsedInternalKey pkey;
    status_ = ParseInternalKey(key, &pkey, /*log_err_key=*/false);
    if (status_.ok()) {
      if (pkey.type != ValueType::kTypeValue) {
        status_ = Status::NotSupported(
            "Value type " + std::to_string(pkey.type) + "not supported");
      } else {
        builder_->Add(pkey.user_key, value);
        properties_.num_entries++;
        properties_.raw_key_size += key.size();
        properties_.raw_value_size += value.size();
        NotifyCollectTableCollectorsOnAdd(key, value, /*file_size=*/0,
                                          table_properties_collectors_,
                                          ioptions_.logger);
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

  Status Finish() override {
    // Approximate the data size
    properties_.data_size =
        properties_.raw_key_size + properties_.raw_value_size;

    PropertyBlockBuilder property_block_builder;
    property_block_builder.AddTableProperty(properties_);
    UserCollectedProperties more_user_collected_properties;
    NotifyCollectTableCollectorsOnFinish(
        table_properties_collectors_, ioptions_.logger, &property_block_builder,
        more_user_collected_properties, properties_.readable_properties);
    properties_.user_collected_properties.insert(
        more_user_collected_properties.begin(),
        more_user_collected_properties.end());

    Slice prop_block = property_block_builder.Finish();
    Status s = builder_->PutPropertiesBlock(prop_block);
    if (s.ok() || s.IsNotSupported()) {
      // If the builder doesn't support writing the properties block,
      // we still call Finish() and let the external builder handle it.
      s = builder_->Finish();
    }

    return s;
  }

  void Abandon() override { builder_->Abandon(); }

  uint64_t FileSize() const override { return builder_->FileSize(); }

  uint64_t NumEntries() const override { return properties_.num_entries; }

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
  std::unique_ptr<FSWritableFile> file_;
  const ImmutableOptions& ioptions_;
  TableProperties properties_;
  std::vector<std::unique_ptr<InternalTblPropColl>>
      table_properties_collectors_;
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
    // SstFileReader specifies largest_seqno as kMaxSequenceNumber to denote
    // that its unknown
    if (topts.largest_seqno > 0 && topts.largest_seqno != kMaxSequenceNumber) {
      return Status::NotSupported(
          "Ingesting file with sequence number larger than 0");
    }
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
    table_reader->reset(
        new ExternalTableReaderAdapter(topts.ioptions, std::move(reader)));
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
    auto file_wrapper =
        std::make_unique<ExternalTableWritableFileWrapper>(file);
    builder.reset(inner_->NewTableBuilder(ext_topts, file->file_name(),
                                          file_wrapper.get()));
    if (builder) {
      return new ExternalTableBuilderAdapter(topts, std::move(builder),
                                             std::move(file_wrapper));
    }
    return nullptr;
  }

  std::unique_ptr<TableFactory> Clone() const override { return nullptr; }

 private:
  // An FSWritableFile subclass for wrapping a WritableFileWriter. The
  // latter is private to RocksDB, so we wrap it here in order to pass it
  // to the ExternalTableBuilder. This is necessary for WritableFileWriter
  // to intercept Append so that it can calculate the file checksum.
  class ExternalTableWritableFileWrapper : public FSWritableFile {
   public:
    explicit ExternalTableWritableFileWrapper(WritableFileWriter* writer)
        : writer_(writer) {}

    using FSWritableFile::Append;
    IOStatus Append(const Slice& data, const IOOptions& options,
                    IODebugContext* /*dbg*/) override {
      return writer_->Append(options, data);
    }

    IOStatus Close(const IOOptions& options, IODebugContext* /*dbg*/) override {
      return writer_->Close(options);
    }

    IOStatus Flush(const IOOptions& options, IODebugContext* /*dbg*/) override {
      return writer_->Flush(options);
    }

    IOStatus Sync(const IOOptions& options, IODebugContext* /*dbg*/) override {
      return writer_->Sync(options, /*use_fsync=*/false);
    }

    uint64_t GetFileSize(const IOOptions& options,
                         IODebugContext* dbg) override {
      return writer_->writable_file()->GetFileSize(options, dbg);
    }

   private:
    WritableFileWriter* writer_;
  };

  std::shared_ptr<ExternalTableFactory> inner_;
};

}  // namespace

std::unique_ptr<TableFactory> NewExternalTableFactory(
    std::shared_ptr<ExternalTableFactory> inner_factory) {
  std::unique_ptr<TableFactory> res;
  res = std::make_unique<ExternalTableFactoryAdapter>(std::move(inner_factory));
  return res;
}

}  // namespace ROCKSDB_NAMESPACE
