//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/external_table.h"

#include <array>
#include <cstddef>
#include <unordered_map>

#include "db/dbformat.h"
#include "logging/logging.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/utilities/types_util.h"
#include "table/block_based/block.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "util/string_util.h"

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
        valid_(false) {}

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
    valid_ = false;
    if (iterator_) {
      ParsedEntryInfo lookup_key;
      status_ = ParseEntry(target, internal_comparator_.user_comparator(),
                           &lookup_key);
      if (status_.ok()) {
        iterator_->Seek(lookup_key.user_key);
        UpdateKey();
        while (valid_ && internal_comparator_.Compare(key(), target) < 0) {
          iterator_->Next();
          UpdateKey();
        }
      }
    }
  }

  void SeekForPrev(const Slice& target) override {
    status_ = Status::OK();
    valid_ = false;
    if (iterator_) {
      ParsedEntryInfo lookup_key;
      status_ = ParseEntry(target, internal_comparator_.user_comparator(),
                           &lookup_key);
      if (status_.ok()) {
        iterator_->SeekForPrev(lookup_key.user_key);
        UpdateKey();
        while (valid_ && internal_comparator_.Compare(key(), target) > 0) {
          iterator_->Prev();
          UpdateKey();
        }
      }
    }
  }

  void Next() override {
    if (iterator_) {
      iterator_->Next();
      UpdateKey();
    }
  }

  bool NextAndGetResult(IterateResult* result) override {
    if (iterator_) {
      valid_ = iterator_->NextAndGetResult(&result_);
      result->value_prepared = result_.value_prepared;
      result->bound_check_result = result_.bound_check_result;
      if (valid_) {
        UpdateKey(result_.key);
        if (valid_) {
          result->key = key();
        }
      } else {
        status_ = iterator_->status();
      }
    } else {
      valid_ = false;
    }
    return valid_;
  }

  bool PrepareValue() override {
    if (iterator_ && !result_.value_prepared) {
      valid_ = iterator_->PrepareValue();
      status_ = iterator_->status();
      if (!status_.ok()) {
        valid_ = false;
      }
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
      UpdateKey();
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
  const InternalKeyComparator& internal_comparator_;
  const SequenceNumber global_seqno_;
  IterKey key_;
  bool valid_;
  Status status_;
  IterateResult result_;

  void UpdateKey(OptSlice res = OptSlice()) {
    if (iterator_) {
      valid_ = iterator_->Valid();
      status_ = iterator_->status();
      if (valid_ && status_.ok()) {
        key_.SetInternalKey(
            res.has_value() ? res.value() : iterator_->key(),
            global_seqno_ == kDisableGlobalSequenceNumber ? 0 : global_seqno_,
            kTypeValue);
      } else if (!status_.ok()) {
        valid_ = false;
      }
    }
  }
};

// Prefer a RocksDB properties block, but allow the external implementation to
// provide TableProperties directly.
Status LoadExternalTableProperties(
    const ImmutableOptions& ioptions, ExternalTableReader* reader,
    std::shared_ptr<const TableProperties>* table_properties) {
  std::unique_ptr<char[]> property_block;
  uint64_t property_block_size = 0;
  uint64_t property_block_offset = 0;
  Status status = reader->GetPropertiesBlock(
      &property_block, &property_block_size, &property_block_offset);
  std::shared_ptr<TableProperties> loaded_properties;
  if (status.ok()) {
    auto parsed_properties = std::make_unique<TableProperties>();
    BlockContents block_contents(std::move(property_block),
                                 property_block_size);
    Block block(std::move(block_contents));
    status = ParsePropertiesBlock(ioptions, property_block_offset, block,
                                  parsed_properties);
    if (!status.ok()) {
      return status;
    }
    loaded_properties = std::move(parsed_properties);
  } else {
    // Propagate any other non is not supported errors
    if (!status.IsNotSupported()) {
      return status;
    }
    loaded_properties =
        std::make_shared<TableProperties>(*reader->GetTableProperties());
    loaded_properties->key_largest_seqno = 0;
    loaded_properties->key_smallest_seqno = 0;
  }

  if (loaded_properties->num_range_deletions != 0) {
    return Status::NotSupported(
        "External tables do not support range deletions");
  }

  *table_properties = std::move(loaded_properties);
  return Status::OK();
}

class ExternalTableReaderAdapter : public TableReader {
 public:
  ExternalTableReaderAdapter(
      const InternalKeyComparator& internal_comparator,
      std::unique_ptr<ExternalTableReader>&& reader,
      std::shared_ptr<const TableProperties> table_properties,
      SequenceNumber global_seqno)
      : internal_comparator_(internal_comparator),
        reader_(std::move(reader)),
        table_properties_(std::move(table_properties)),
        global_seqno_(global_seqno) {}

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
    ParsedEntryInfo lookup_key;
    Status status =
        ParseEntry(key, internal_comparator_.user_comparator(), &lookup_key);
    if (!status.ok()) {
      return status;
    }
    if (global_seqno_ != kDisableGlobalSequenceNumber &&
        global_seqno_ > lookup_key.sequence) {
      return Status::OK();
    }

    PinnableSlice value;
    status = reader_->Get(read_options, lookup_key.user_key, prefix_extractor,
                          &value);
    if (status.IsNotFound()) {
      return Status::OK();
    }
    if (!status.ok()) {
      return status;
    }

    if (global_seqno_ == kDisableGlobalSequenceNumber || global_seqno_ == 0) {
      get_context->SaveValue(value, /*seq=*/0,
                             value.IsPinned() ? &value : nullptr);
      return Status::OK();
    }
    const ParsedInternalKey parsed_key(lookup_key.user_key, global_seqno_,
                                       kTypeValue);
    bool matched = false;
    Status read_status;
    get_context->SaveValue(parsed_key, value, &matched, &read_status,
                           value.IsPinned() ? &value : nullptr);
    return read_status;
  }

  void MultiGet(const ReadOptions& read_options,
                const MultiGetContext::Range* mget_range,
                const SliceTransform* prefix_extractor,
                bool /*skip_filters*/ = false) override {
    const size_t num_keys = mget_range->KeysLeft();
    std::vector<Slice> lookup_keys;
    lookup_keys.reserve(num_keys);
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
      return;
    }

    std::vector<PinnableSlice> values(lookup_keys.size());
    std::vector<Status> statuses(lookup_keys.size());
    reader_->MultiGet(read_options, lookup_keys, prefix_extractor, &values,
                      &statuses);
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
  }

  Status VerifyChecksum(const ReadOptions& /*ro*/, TableReaderCaller /*caller*/,
                        bool /*meta_blocks_only*/ = false) override {
    return Status::OK();
  }

 private:
  const InternalKeyComparator& internal_comparator_;
  std::unique_ptr<ExternalTableReader> reader_;
  std::shared_ptr<const TableProperties> table_properties_;
  const SequenceNumber global_seqno_;
};

class ExternalTableBuilderAdapter : public TableBuilder {
 public:
  explicit ExternalTableBuilderAdapter(
      const TableBuilderOptions& topts,
      std::unique_ptr<ExternalTableBuilder>&& builder)
      : builder_(std::move(builder)), ioptions_(topts.ioptions) {
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

    ParsedEntryInfo entry;
    status_ = ParseEntry(key, ioptions_.user_comparator, &entry);
    if (!status_.ok()) {
      return;
    }
    if (!entry.timestamp.empty()) {
      status_ = Status::NotSupported(
          "External table does not support user-defined timestamps");
      return;
    }
    if (entry.sequence != 0 || entry.type != kEntryPut) {
      status_ = Status::NotSupported(
          "External table factory only supports sequence-zero Put entries");
      return;
    }

    builder_->Add(entry.user_key, value);
    status_ = builder_->status();
    if (!status_.ok()) {
      return;
    }

    properties_.num_entries++;
    properties_.raw_key_size += key.size();
    properties_.raw_value_size += value.size();
    NotifyCollectTableCollectorsOnAdd(key, value, /*file_size=*/0,
                                      table_properties_collectors_,
                                      ioptions_.logger);
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
  const ImmutableOptions& ioptions_;
  TableProperties properties_;
  std::vector<std::unique_ptr<InternalTblPropColl>>
      table_properties_collectors_;
};

struct ExternalTableFactoryAdapterOptions {
  static const char* kName() { return "ExternalTableFactoryAdapterOptions"; }

  std::string config;
};

static const std::unordered_map<std::string, OptionTypeInfo>&
GetExternalTableFactoryAdapterOptionsTypeInfo() {
  static const std::unordered_map<std::string, OptionTypeInfo> type_info = {
      {"external_table_config",
       OptionTypeInfo(offsetof(ExternalTableFactoryAdapterOptions, config),
                      OptionType::kString)
           .SetSerializeFunc([](const ConfigOptions&, const std::string&,
                                const void* addr, std::string* value) {
             const std::string* config = static_cast<const std::string*>(addr);
             *value = "{" + EscapeOptionString(*config) + "}";
             return Status::OK();
           })}};
  return type_info;
}

class ExternalTableFactoryAdapter : public TableFactory {
 public:
  explicit ExternalTableFactoryAdapter(
      std::shared_ptr<ExternalTableFactory> inner)
      : inner_(std::move(inner)) {
    RegisterOptions(&options_,
                    &GetExternalTableFactoryAdapterOptionsTypeInfo());
  }

  const char* Name() const override { return inner_->Name(); }

  Status PrepareOptions(const ConfigOptions& config_options) override {
    Status status = TableFactory::PrepareOptions(config_options);
    if (status.ok()) {
      status = inner_->Configure(options_.config);
    }
    return status;
  }

  using TableFactory::NewTableReader;
  Status NewTableReader(
      const ReadOptions& ro, const TableReaderOptions& topts,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool /* prefetch_index_and_filter_in_cache */) const override {
    if (topts.ioptions.user_comparator != nullptr &&
        topts.ioptions.user_comparator->timestamp_size() != 0) {
      return Status::NotSupported(
          "External table does not support user-defined timestamps");
    }
    std::unique_ptr<ExternalTableReader> reader;
    FileOptions fopts(topts.env_options);
    fopts.io_options.io_activity = ro.io_activity;
    // Files opened by external readers lack MANIFEST checksum metadata.
    fopts.file_checksum_func_name = kNoFileChecksumFuncName;
    ExternalTableOptions ext_topts(topts.prefix_extractor,
                                   topts.ioptions.user_comparator,
                                   topts.ioptions.fs, fopts);
    const std::string file_name = file->file_name();
    auto status = inner_->NewTableReader(ro, file_name, ext_topts,
                                         std::move(file), file_size, &reader);
    if (!status.ok()) {
      return status;
    }

    std::shared_ptr<const TableProperties> table_properties;
    status = LoadExternalTableProperties(topts.ioptions, reader.get(),
                                         &table_properties);
    if (!status.ok()) {
      return status;
    }

    // TableCache passes the MANIFEST-backed sequence through
    // TableReaderOptions. Resolve it once for readers and iterators.
    SequenceNumber global_seqno;
    status = GetGlobalSequenceNumber(*table_properties, topts.largest_seqno,
                                     &global_seqno);
    if (!status.ok()) {
      return status;
    }

    table_reader->reset(new ExternalTableReaderAdapter(
        topts.internal_comparator, std::move(reader),
        std::move(table_properties), global_seqno));
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
    builder.reset(inner_->NewTableBuilder(ext_topts, file->file_name(), file));
    if (builder) {
      return new ExternalTableBuilderAdapter(topts, std::move(builder));
    }
    return nullptr;
  }

  std::unique_ptr<TableFactory> Clone() const override { return nullptr; }

 private:
  std::shared_ptr<ExternalTableFactory> inner_;
  ExternalTableFactoryAdapterOptions options_;
};

}  // namespace

std::unique_ptr<TableFactory> NewExternalTableFactory(
    std::shared_ptr<ExternalTableFactory> inner_factory) {
  std::unique_ptr<TableFactory> res;
  res = std::make_unique<ExternalTableFactoryAdapter>(std::move(inner_factory));
  return res;
}

}  // namespace ROCKSDB_NAMESPACE
