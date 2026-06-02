//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/external_table.h"

#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "logging/logging.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE {

namespace {

class ExternalTableIteratorAdapter : public InternalIterator {
 public:
  ExternalTableIteratorAdapter(ExternalTableIterator* iterator,
                               const InternalKeyComparator& internal_comparator,
                               SequenceNumber global_seqno)
      : iterator_(iterator),
        internal_comparator_(internal_comparator),
        global_seqno_(global_seqno),
        valid_(false) {
    if (iterator_) {
      status_ = iterator_->status();
    }
  }

  // No copying allowed
  ExternalTableIteratorAdapter(const ExternalTableIteratorAdapter&) = delete;
  ExternalTableIteratorAdapter& operator=(const ExternalTableIteratorAdapter&) =
      delete;

  ~ExternalTableIteratorAdapter() override {}

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    valid_ = false;
    ResetPinnedValue();
    if (UsableIterator()) {
      status_ = Status::OK();
      iterator_->SeekToFirst();
      UpdateKey(OptSlice());
    }
  }

  void SeekToLast() override {
    valid_ = false;
    ResetPinnedValue();
    if (UsableIterator()) {
      status_ = Status::OK();
      iterator_->SeekToLast();
      UpdateKey(OptSlice());
    }
  }

  void Seek(const Slice& target) override {
    valid_ = false;
    ResetPinnedValue();
    if (UsableIterator()) {
      status_ = Status::OK();
      ParsedInternalKey pkey;
      status_ = ParseInternalKey(target, &pkey, /*log_err_key=*/false);
      if (status_.ok()) {
        iterator_->Seek(pkey.user_key);
        UpdateKey(OptSlice());
        while (valid_ && status_.ok() &&
               internal_comparator_.Compare(key(), target) < 0) {
          iterator_->Next();
          UpdateKey(OptSlice());
        }
      }
    }
  }

  void SeekForPrev(const Slice& target) override {
    valid_ = false;
    ResetPinnedValue();
    if (UsableIterator()) {
      status_ = Status::OK();
      ParsedInternalKey pkey;
      status_ = ParseInternalKey(target, &pkey, /*log_err_key=*/false);
      if (status_.ok()) {
        iterator_->SeekForPrev(pkey.user_key);
        UpdateKey(OptSlice());
        while (valid_ && status_.ok() &&
               internal_comparator_.Compare(key(), target) > 0) {
          iterator_->Prev();
          UpdateKey(OptSlice());
        }
      }
    }
  }

  void Next() override {
    ResetPinnedValue();
    if (UsableIterator()) {
      iterator_->Next();
      UpdateKey(OptSlice());
    } else {
      valid_ = false;
    }
  }

  bool NextAndGetResult(IterateResult* result) override {
    ResetPinnedValue();
    if (UsableIterator()) {
      valid_ = iterator_->NextAndGetResult(&result_);
      result->value_prepared = result_.value_prepared;
      result->bound_check_result = result_.bound_check_result;
      if (valid_) {
        UpdateKey(result_.key);
        result->key = key();
      } else {
        status_ = iterator_->status();
      }
    } else {
      valid_ = false;
    }
    return valid_;
  }

  bool PrepareValue() override {
    if (UsableIterator() && !result_.value_prepared) {
      valid_ = iterator_->PrepareValue();
      status_ = iterator_->status();
      result_.value_prepared = valid_;
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
    ResetPinnedValue();
    if (UsableIterator()) {
      iterator_->Prev();
      UpdateKey(OptSlice());
    } else {
      valid_ = false;
    }
  }

  Slice key() const override {
    if (iterator_) {
      return key_.GetInternalKey();
    }
    return Slice();
  }

  Slice value() const override {
    if (iterator_) {
      Slice value = iterator_->value();
      if (pinned_iters_mgr_ != nullptr && pinned_iters_mgr_->PinningEnabled()) {
        // DBIter's reverse path requires values to stay valid after moving the
        // child iterator. ExternalTableIterator only promises movement-scoped
        // values, so copy only when DBIter explicitly enables pinning.
        if (pinned_value_ == nullptr) {
          pinned_value_ = new std::string(value.data(), value.size());
          pinned_iters_mgr_->PinPtr(pinned_value_, &ReleasePinnedValue);
        }
        return Slice(*pinned_value_);
      }
      return value;
    }
    return Slice();
  }

  Status status() const override { return status_; }

  void Prepare(const MultiScanArgs* scan_opts) override {
    if (!UsableIterator()) {
      return;
    }
    if (scan_opts) {
      iterator_->Prepare(scan_opts->GetScanRanges().data(), scan_opts->size());
    } else {
      iterator_->Prepare(nullptr, 0);
    }
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    if (pinned_iters_mgr_ != pinned_iters_mgr) {
      ResetPinnedValue();
    }
    pinned_iters_mgr_ = pinned_iters_mgr;
  }

  bool IsValuePinned() const override {
    // This mirrors BlockIter: pinning capability is reported for the current
    // position even before value() materializes the pinned backing buffer.
    return Valid() && pinned_iters_mgr_ != nullptr &&
           pinned_iters_mgr_->PinningEnabled();
  }

 private:
  std::unique_ptr<ExternalTableIterator> iterator_;
  // ExternalTableIterator exposes user keys. The adapter exposes internal
  // keys, so Seek/SeekForPrev boundary checks must use RocksDB's internal
  // ordering after applying the file-wide seqno.
  const InternalKeyComparator& internal_comparator_;
  SequenceNumber global_seqno_;
  IterKey key_;
  bool valid_;
  Status status_;
  IterateResult result_;
  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;
  mutable std::string* pinned_value_ = nullptr;

  static void ReleasePinnedValue(void* ptr) {
    delete static_cast<std::string*>(ptr);
  }

  void ResetPinnedValue() const { pinned_value_ = nullptr; }

  bool UsableIterator() {
    if (!status_.ok()) {
      valid_ = false;
      return false;
    }
    return iterator_ != nullptr;
  }

  void UpdateKey(OptSlice res) {
    if (iterator_) {
      status_ = iterator_->status();
      valid_ = iterator_->Valid() && status_.ok();
      if (valid_ && status_.ok()) {
        key_.SetInternalKey(res.has_value() ? res.value() : iterator_->key(),
                            global_seqno_, ValueType::kTypeValue);
      }
    }
  }
};

Status LoadExternalTableProperties(
    const ImmutableOptions& ioptions, ExternalTableReader* reader,
    SequenceNumber global_seqno,
    std::shared_ptr<const TableProperties>* props) {
  std::unique_ptr<char[]> property_block;
  uint64_t property_block_size = 0;
  uint64_t property_block_offset = 0;
  Status s = reader->GetPropertiesBlock(&property_block, &property_block_size,
                                        &property_block_offset);
  if (s.ok() && property_block_size > 0) {
    std::unique_ptr<TableProperties> table_properties =
        std::make_unique<TableProperties>();
    BlockContents block_contents(std::move(property_block),
                                 property_block_size);
    Block block(std::move(block_contents));
    s = ParsePropertiesBlock(ioptions, property_block_offset, block,
                             table_properties);
    if (!s.ok()) {
      return s;
    }
    props->reset(table_properties.release());
    return Status::OK();
  }
  if (!s.ok() && !s.IsNotSupported()) {
    return s;
  }

  // Fallback to a minimal properties structure. External formats store user
  // keys, so the manifest-provided file-wide seqno is the only seqno RocksDB
  // can expose when there is no RocksDB properties block.
  std::shared_ptr<const TableProperties> reader_props =
      reader->GetTableProperties();
  if (reader_props == nullptr) {
    return Status::Corruption(
        "External table reader returned null table properties");
  }
  auto mutable_props = std::make_shared<TableProperties>(*reader_props);
  mutable_props->key_largest_seqno = global_seqno;
  mutable_props->key_smallest_seqno = global_seqno;
  *props = std::move(mutable_props);
  return Status::OK();
}

Status CheckGlobalSeqnoConsistency(const TableProperties& props,
                                   SequenceNumber largest_seqno) {
  if (largest_seqno == kMaxSequenceNumber) {
    return Status::OK();
  }
  // External files produced through SstFileWriter store 0 in the RocksDB
  // properties block because the file format only contains user keys. Treat 0
  // as "unknown" and let the MANIFEST-provided seqno be authoritative.
  if ((props.key_largest_seqno != 0 &&
       props.key_largest_seqno != largest_seqno) ||
      (props.key_smallest_seqno != 0 &&
       props.key_smallest_seqno != largest_seqno)) {
    return Status::Corruption(
        "External table seqno properties do not match manifest seqno");
  }
  return Status::OK();
}

class ExternalTableReaderAdapter : public TableReader {
 public:
  explicit ExternalTableReaderAdapter(
      const InternalKeyComparator& internal_comparator,
      std::unique_ptr<ExternalTableReader>&& reader,
      SequenceNumber global_seqno,
      std::shared_ptr<const TableProperties>&& table_properties)
      : internal_comparator_(internal_comparator),
        reader_(std::move(reader)),
        global_seqno_(global_seqno),
        table_properties_(std::move(table_properties)) {}

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
    if (iterator == nullptr) {
      return NewErrorInternalIterator<Slice>(
          Status::Corruption("External table reader returned null iterator"),
          arena);
    }
    if (arena == nullptr) {
      return new ExternalTableIteratorAdapter(iterator, internal_comparator_,
                                              global_seqno_);
    } else {
      auto* mem = arena->AllocateAligned(sizeof(ExternalTableIteratorAdapter));
      return new (mem) ExternalTableIteratorAdapter(
          iterator, internal_comparator_, global_seqno_);
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
    return table_properties_;
  }

  size_t ApproximateMemoryUsage() const override { return 0; }

  Status Get(const ReadOptions& read_options, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool /*skip_filters*/ = false) override {
    ParsedInternalKey parsed_key;
    Status s = ParseInternalKey(key, &parsed_key, /*log_err_key=*/false);
    if (!s.ok()) {
      return s;
    }
    if (global_seqno_ > parsed_key.sequence) {
      return Status::OK();
    }

    PinnableSlice value;
    s = reader_->Get(read_options, parsed_key.user_key, prefix_extractor,
                     &value);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        return Status::OK();
      }
      return s;
    }

    bool use_simple_save_value =
        global_seqno_ == 0 && !get_context->has_callback() &&
        !get_context->NeedToReadSequence() &&
        (get_context->max_covering_tombstone_seq() == nullptr ||
         *get_context->max_covering_tombstone_seq() == 0);
    if (use_simple_save_value) {
      get_context->SaveValue(value, /*seq=*/0,
                             value.IsPinned() ? &value : nullptr);
    } else {
      ParsedInternalKey value_key(parsed_key.user_key, global_seqno_,
                                  ValueType::kTypeValue);
      bool matched = false;
      Status read_status;
      get_context->SaveValue(value_key, value, &matched, &read_status,
                             value.IsPinned() ? &value : nullptr);
      if (!read_status.ok()) {
        return read_status;
      }
    }
    return s;
  }

  Status VerifyChecksum(const ReadOptions& /*ro*/, TableReaderCaller /*caller*/,
                        bool /*meta_blocks_only*/ = false) override {
    return Status::OK();
  }

 private:
  const InternalKeyComparator& internal_comparator_;
  std::unique_ptr<ExternalTableReader> reader_;
  SequenceNumber global_seqno_;
  std::shared_ptr<const TableProperties> table_properties_;
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
    if (!status_.ok()) {
      return;
    }
    ParsedInternalKey pkey;
    status_ = ParseInternalKey(key, &pkey, /*log_err_key=*/false);
    if (status_.ok()) {
      if (pkey.type != ValueType::kTypeValue) {
        status_ = Status::NotSupported(
            "Value type " + std::to_string(pkey.type) + " not supported");
      } else {
        if (properties_.num_entries == 0) {
          properties_.key_smallest_seqno = pkey.sequence;
          properties_.key_largest_seqno = pkey.sequence;
        } else if (pkey.sequence != properties_.key_largest_seqno) {
          status_ = Status::NotSupported(
              "External table builder cannot encode multiple sequence "
              "numbers in one file");
          return;
        }
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
    if (finalized_) {
      return Status::InvalidArgument(
          "External table builder already finalized");
    }
    finalized_ = true;
    if (!status_.ok()) {
      builder_->Abandon();
      return status_;
    }

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
    if (!s.ok() && !s.IsNotSupported()) {
      builder_->Abandon();
      return s;
    }
    if (s.ok() || s.IsNotSupported()) {
      // If the builder doesn't support writing the properties block,
      // we still call Finish() and let the external builder handle it.
      s = builder_->Finish();
    }

    return s;
  }

  void Abandon() override {
    if (!finalized_) {
      finalized_ = true;
      builder_->Abandon();
    }
  }

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
  bool finalized_ = false;
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
    if (topts.ioptions.user_comparator != nullptr &&
        topts.ioptions.user_comparator->timestamp_size() != 0) {
      return Status::NotSupported(
          "External table does not support user-defined timestamps");
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
    SequenceNumber global_seqno = topts.largest_seqno;
    if (global_seqno == kMaxSequenceNumber) {
      global_seqno = 0;
    }
    std::shared_ptr<const TableProperties> props;
    status = LoadExternalTableProperties(topts.ioptions, reader.get(),
                                         global_seqno, &props);
    if (!status.ok()) {
      return status;
    }
    status = CheckGlobalSeqnoConsistency(*props, topts.largest_seqno);
    if (!status.ok()) {
      return status;
    }
    table_reader->reset(new ExternalTableReaderAdapter(
        topts.internal_comparator, std::move(reader), global_seqno,
        std::move(props)));
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
        topts.column_family_name, topts.reason, topts.ioptions.fs);
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
