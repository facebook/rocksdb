//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based_table_builder.h"
#include "table/block_based_table_factory.h"
#include "table/get_context.h"
#include "table/internal_sst_file_reader.h"

#include "rocksdb/sst_file_reader.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE

InternalSstFileReader::InternalSstFileReader(const std::string& file_name,
                                             Options options,
                                             const Comparator* comparator)
    : InternalSstFileReader(options, comparator) {
  Open(file_name);
}

InternalSstFileReader::InternalSstFileReader(Options options,
                                             const Comparator* comparator)
    : options_(options),
      ioptions_(options_),
      moptions_(ColumnFamilyOptions(options_)),
      internal_comparator_(comparator) {}

extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;

Status InternalSstFileReader::Open(const std::string& file_path) {
  file_name_ = file_path;
  init_result_ = GetTableReader(file_name_);
  return init_result_;
}

Status InternalSstFileReader::VerifyChecksum() {
  if (!table_reader_) {
    return init_result_;
  }
  return table_reader_->VerifyChecksum();
}

Status InternalSstFileReader::GetTableReader(const std::string& file_path) {
  // Warning about 'magic_number' being uninitialized shows up only in UBsan
  // builds. Though access is guarded by 's.ok()' checks, fix the issue to
  // avoid any warnings.
  uint64_t magic_number = Footer::kInvalidTableMagicNumber;

  // read table magic number
  Footer footer;

  std::unique_ptr<RandomAccessFile> file;
  uint64_t file_size = 0;
  Status s = options_.env->NewRandomAccessFile(file_path, &file, soptions_);
  if (s.ok()) {
    s = options_.env->GetFileSize(file_path, &file_size);
  }

  file_.reset(new RandomAccessFileReader(std::move(file), file_path));

  if (s.ok()) {
    s = ReadFooterFromFile(file_.get(), nullptr /* prefetch_buffer */,
                           file_size, &footer);
  }
  if (s.ok()) {
    magic_number = footer.table_magic_number();
  }

  if (s.ok()) {
    if (magic_number == kPlainTableMagicNumber ||
        magic_number == kLegacyPlainTableMagicNumber) {
      soptions_.use_mmap_reads = true;
      options_.env->NewRandomAccessFile(file_path, &file, soptions_);
      file_.reset(new RandomAccessFileReader(std::move(file), file_path));
    }
    options_.comparator = &internal_comparator_;
    // For old sst format, ReadTableProperties might fail but file can be read
    if (ReadTableProperties(magic_number, file_.get(), file_size).ok()) {
      SetTableOptionsByMagicNumber(magic_number);
    } else {
      SetOldTableOptions();
    }
  }

  if (s.ok()) {
    s = NewTableReader(ioptions_, soptions_, internal_comparator_, file_size,
                       &table_reader_);
  }
  return s;
}

Status InternalSstFileReader::SetOldTableOptions() {
  assert(table_properties_ == nullptr);
  options_.table_factory = std::make_shared<BlockBasedTableFactory>();
  return Status::OK();
}

InternalSstFileReader::SstFileFormat InternalSstFileReader::GetSstFileFormat(
    uint64_t table_magic_number) {
  assert(table_properties_);
  if (table_magic_number == kBlockBasedTableMagicNumber ||
      table_magic_number == kLegacyBlockBasedTableMagicNumber) {
    return SstFileFormat::BlockBased;
  } else if (table_magic_number == kPlainTableMagicNumber ||
             table_magic_number == kLegacyPlainTableMagicNumber) {
    return SstFileFormat::PlainTable;
  } else {
    return SstFileFormat::Unsupported;
  }
}

InternalIterator* InternalSstFileReader::NewIterator(
    const ReadOptions& read_options, const SliceTransform* prefix_extractor,
    Arena* arena, bool skip_filters, bool for_compaction) {
  if (!table_reader_) {
    return nullptr;
  }
  return table_reader_->NewIterator(read_options, prefix_extractor, arena,
                                    skip_filters, for_compaction);
}

void InternalSstFileReader::SetBlockBasedTableOptionsByMagicNumber() {
  options_.table_factory = std::make_shared<BlockBasedTableFactory>();
  auto& props = table_properties_->user_collected_properties;
  auto pos = props.find(BlockBasedTablePropertyNames::kIndexType);
  if (pos != props.end()) {
    auto index_type_on_file = static_cast<BlockBasedTableOptions::IndexType>(
        DecodeFixed32(pos->second.c_str()));
    if (index_type_on_file == BlockBasedTableOptions::IndexType::kHashSearch) {
      options_.prefix_extractor.reset(NewNoopTransform());
    }
  }
}

void InternalSstFileReader::SetPlainTableOptionsByMagicNumber() {
  options_.allow_mmap_reads = true;
  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = kPlainTableVariableLength;
  plain_table_options.bloom_bits_per_key = 0;
  plain_table_options.hash_table_ratio = 0;
  plain_table_options.index_sparseness = 1;
  plain_table_options.huge_page_tlb_size = 0;
  plain_table_options.encoding_type = kPlain;
  plain_table_options.full_scan_mode = true;
  options_.table_factory.reset(NewPlainTableFactory(plain_table_options));
}

Status InternalSstFileReader::SetTableOptionsByMagicNumber(
    uint64_t table_magic_number) {
  switch (GetSstFileFormat(table_magic_number)) {
    case SstFileFormat::BlockBased: {
      SetBlockBasedTableOptionsByMagicNumber();
      break;
    }
    case SstFileFormat::PlainTable: {
      SetPlainTableOptionsByMagicNumber();
      break;
    }
    case SstFileFormat::Unsupported: {
      return Status::NotSupported("Unsupported");
      break;
    }
  }
  return Status::OK();
}

Status InternalSstFileReader::ReadTableProperties(uint64_t table_magic_number,
                                                  RandomAccessFileReader* file,
                                                  uint64_t file_size) {
  TableProperties* table_properties = nullptr;
  Status s = rocksdb::ReadTableProperties(file, file_size, table_magic_number,
                                          ioptions_, &table_properties);
  if (s.ok()) {
    table_properties_.reset(table_properties);
  }
  return s;
}

Status InternalSstFileReader::NewTableReader(
    const ImmutableCFOptions& /*ioptions*/, const EnvOptions& /*soptions*/,
    const InternalKeyComparator& /*internal_comparator*/, uint64_t file_size,
    std::unique_ptr<TableReader>* /*table_reader*/) {
  // We need to turn off pre-fetching of index and filter nodes for
  // BlockBasedTable
  if (BlockBasedTableFactory::kName == options_.table_factory->Name()) {
    return options_.table_factory->NewTableReader(
        TableReaderOptions(ioptions_, moptions_.prefix_extractor.get(),
                           soptions_, internal_comparator_),
        std::move(file_), file_size, &table_reader_, /*enable_prefetch=*/false);
  }

  // For all other factory implementation
  return options_.table_factory->NewTableReader(
      TableReaderOptions(ioptions_, moptions_.prefix_extractor.get(), soptions_,
                         internal_comparator_),
      std::move(file_), file_size, &table_reader_);
}

Status InternalSstFileReader::ReadTableProperties(
    std::shared_ptr<const TableProperties>* table_properties) {
  if (!table_reader_) {
    return init_result_;
  }

  *table_properties = table_reader_->GetTableProperties();
  return init_result_;
}

class SstKVIteratorImpl : public SstKVIterator {
 public:
  explicit SstKVIteratorImpl(InternalIterator* iter) : iter_(iter) {}

  virtual bool Valid() const override { return iter_->Valid(); }
  virtual void SeekToFirst() override { iter_->SeekToFirst(); }
  virtual void SeekToLast() override { iter_->SeekToLast(); }
  virtual void Seek(const Slice& k) override {
    InternalKey ikey;
    ikey.SetMinPossibleForUserKey(k);
    iter_->Seek(ikey.Encode());
  }
  virtual void SeekForPrev(const Slice& k) override {
    InternalKey ikey;
    ikey.SetMinPossibleForUserKey(k);
    iter_->SeekForPrev(ikey.Encode());
  }
  virtual void Next() override { iter_->Next(); }
  virtual void Prev() override { iter_->Prev(); }

  virtual Slice value() const override { return iter_->value(); }

  Slice key() const override { return key(nullptr, nullptr); }

  Slice key(SequenceNumber* sequence, int* type) const override {
    ParsedInternalKey ikey;
    ikey.type = kTypeDeletion;
    ParseInternalKey(iter_->key(), &ikey);

    if (sequence != nullptr) {
      *sequence = ikey.sequence;
    }
    if (type != nullptr) {
      *type = static_cast<int>(ikey.type);
    }
    return ikey.user_key;
  }

  virtual Status status() const override { return iter_->status(); }

  virtual Status GetProperty(std::string prop_name,
                             std::string* prop) override {
    return iter_->GetProperty(prop_name, prop);
  }

  virtual ~SstKVIteratorImpl() { delete iter_; }

 private:
  InternalIterator* iter_;
};

struct SstFileReader::Rep : public InternalSstFileReader {
  Rep(const std::string& file_name, Options options,
      const Comparator* comparator)
      : InternalSstFileReader(file_name, options, comparator) {}

  SstKVIteratorImpl* NewIterator(const ReadOptions& read_options,
                                 const SliceTransform* prefix_extractor,
                                 Arena* arena, bool skip_filters,
                                 bool for_compaction) {
    InternalIterator* iter = InternalSstFileReader::NewIterator(
        read_options, prefix_extractor, arena, skip_filters, for_compaction);
    if (iter == nullptr) {
      return nullptr;
    }
    return new SstKVIteratorImpl(iter);
  }
};

Status SstFileReader::Open(std::shared_ptr<SstFileReader>* reader,
                           const std::string& file_name, Options options,
                           const Comparator* comparator) {
  if (reader == nullptr) {
    return Status::InvalidArgument("reader should not be nullptr");
  }
  std::unique_ptr<SstFileReader::Rep> rep(
      new SstFileReader::Rep(file_name, options, comparator));
  Status status = rep->getStatus();
  if (!status.ok()) {
    return status;
  } else {
    reader->reset(new SstFileReader(rep));
  }
  return Status::OK();
}

SstFileReader::SstFileReader(std::unique_ptr<SstFileReader::Rep>& rep)
    : rep_(std::move(rep)) {}

SstFileReader::~SstFileReader() {}

SstKVIterator* SstFileReader::NewIterator(
    const ReadOptions& read_options, const SliceTransform* prefix_extractor,
    Arena* arena, bool skip_filters, bool for_compaction) {
  return rep_->NewIterator(read_options, prefix_extractor, arena, skip_filters,
                           for_compaction);
}

Status SstFileReader::ReadTableProperties(
    std::shared_ptr<const TableProperties>* table_properties) {
  return rep_->ReadTableProperties(table_properties);
}

Status SstFileReader::VerifyChecksum() { return rep_->VerifyChecksum(); }

#endif  // !ROCKSDB_LITE

}  // namespace rocksdb
