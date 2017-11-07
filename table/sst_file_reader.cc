#include "rocksdb/sst_file_reader.h"

#include <inttypes.h>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "table/block.h"
#include "table/block_based_table_builder.h"
#include "table/block_based_table_factory.h"
#include "table/format.h"
#include "table/plain_table_factory.h"
#include "table/table_reader.h"
#include "util/compression.h"

#include "port/port.h"

namespace rocksdb {

SstFileReader::SstFileReader(const std::string& file_path, bool verify_checksum,
                             bool output_hex, bool verbose)
    : file_name_(file_path),
      read_num_(0),
      verify_checksum_(verify_checksum),
      output_hex_(output_hex),
      verbose_(verbose),
      ioptions_(options_),
      internal_comparator_(BytewiseComparator()) {
  if (verbose_) {
    fprintf(stdout, "Process %s\n", file_path.c_str());
  }
  init_result_ = GetTableReader(file_name_);
}

extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;

const char* testFileName = "test_file_name";

Status SstFileReader::GetTableReader(const std::string& file_path) {
  // Warning about 'magic_number' being uninitialized shows up only in UBsan
  // builds. Though access is guarded by 's.ok()' checks, fix the issue to
  // avoid any warnings.
  uint64_t magic_number = Footer::kInvalidTableMagicNumber;

  // read table magic number
  Footer footer;

  unique_ptr<RandomAccessFile> file;
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

Status SstFileReader::NewTableReader(
    const ImmutableCFOptions& ioptions, const EnvOptions& soptions,
    const InternalKeyComparator& internal_comparator, uint64_t file_size,
    unique_ptr<TableReader>* table_reader) {
  // We need to turn off pre-fetching of index and filter nodes for
  // BlockBasedTable
  if (BlockBasedTableFactory::kName == options_.table_factory->Name()) {
    return options_.table_factory->NewTableReader(
        TableReaderOptions(ioptions_, soptions_, internal_comparator_,
                           /*skip_filters=*/false),
        std::move(file_), file_size, &table_reader_, /*enable_prefetch=*/false);
  }

  // For all other factory implementation
  return options_.table_factory->NewTableReader(
      TableReaderOptions(ioptions_, soptions_, internal_comparator_),
      std::move(file_), file_size, &table_reader_);
}

Status SstFileReader::VerifyChecksum() {
  return table_reader_->VerifyChecksum();
}

Status SstFileReader::DumpTable(const std::string& out_filename) {
  unique_ptr<WritableFile> out_file;
  Env* env = Env::Default();
  env->NewWritableFile(out_filename, &out_file, soptions_);
  Status s = table_reader_->DumpTable(out_file.get());
  out_file->Close();
  return s;
}

uint64_t SstFileReader::CalculateCompressedTableSize(
    const TableBuilderOptions& tb_options, size_t block_size) {
  unique_ptr<WritableFile> out_file;
  unique_ptr<Env> env(NewMemEnv(Env::Default()));
  env->NewWritableFile(testFileName, &out_file, soptions_);
  unique_ptr<WritableFileWriter> dest_writer;
  dest_writer.reset(new WritableFileWriter(std::move(out_file), soptions_));
  BlockBasedTableOptions table_options;
  table_options.block_size = block_size;
  BlockBasedTableFactory block_based_tf(table_options);
  unique_ptr<TableBuilder> table_builder;
  table_builder.reset(block_based_tf.NewTableBuilder(
      tb_options,
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
      dest_writer.get()));
  unique_ptr<InternalIterator> iter(table_reader_->NewIterator(ReadOptions()));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (!iter->status().ok()) {
      fputs(iter->status().ToString().c_str(), stderr);
      exit(1);
    }
    table_builder->Add(iter->key(), iter->value());
  }
  Status s = table_builder->Finish();
  if (!s.ok()) {
    fputs(s.ToString().c_str(), stderr);
    exit(1);
  }
  uint64_t size = table_builder->FileSize();
  env->DeleteFile(testFileName);
  return size;
}

int SstFileReader::ShowAllCompressionSizes(
    size_t block_size,
    const std::vector<std::pair<CompressionType, const char*>>&
        compression_types) {
  ReadOptions read_options;
  Options opts;
  const ImmutableCFOptions imoptions(opts);
  rocksdb::InternalKeyComparator ikc(opts.comparator);
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
      block_based_table_factories;

  if (verbose_) {
    fprintf(stdout, "Block Size: %" ROCKSDB_PRIszt "\n", block_size);
  }

  for (auto& i : compression_types) {
    if (CompressionTypeSupported(i.first)) {
      CompressionOptions compress_opt;
      std::string column_family_name;
      int unknown_level = -1;
      TableBuilderOptions tb_opts(
          imoptions, ikc, &block_based_table_factories, i.first, compress_opt,
          nullptr /* compression_dict */, false /* skip_filters */,
          column_family_name, unknown_level);
      uint64_t file_size = CalculateCompressedTableSize(tb_opts, block_size);
      if (verbose_) {
        fprintf(stdout, "Compression: %s", i.second);
        fprintf(stdout, " Size: %" PRIu64 "\n", file_size);
      }
    } else if (verbose_) {
      fprintf(stdout, "Unsupported compression type: %s.\n", i.second);
    }
  }
  return 0;
}

Status SstFileReader::ReadTableProperties(uint64_t table_magic_number,
                                          RandomAccessFileReader* file,
                                          uint64_t file_size) {
  TableProperties* table_properties = nullptr;
  Status s = rocksdb::ReadTableProperties(file, file_size, table_magic_number,
                                          ioptions_, &table_properties);
  if (s.ok()) {
    table_properties_.reset(table_properties);
  } else if (verbose_) {
    fprintf(stdout, "Not able to read table properties\n");
  }
  return s;
}

Status SstFileReader::SetTableOptionsByMagicNumber(
    uint64_t table_magic_number) {
  assert(table_properties_);
  if (table_magic_number == kBlockBasedTableMagicNumber ||
      table_magic_number == kLegacyBlockBasedTableMagicNumber) {
    options_.table_factory = std::make_shared<BlockBasedTableFactory>();
    if (verbose_) {
      fprintf(stdout, "Sst file format: block-based\n");
    }
    auto& props = table_properties_->user_collected_properties;
    auto pos = props.find(BlockBasedTablePropertyNames::kIndexType);
    if (pos != props.end()) {
      auto index_type_on_file = static_cast<BlockBasedTableOptions::IndexType>(
          DecodeFixed32(pos->second.c_str()));
      if (index_type_on_file ==
          BlockBasedTableOptions::IndexType::kHashSearch) {
        options_.prefix_extractor.reset(NewNoopTransform());
      }
    }
  } else if (table_magic_number == kPlainTableMagicNumber ||
             table_magic_number == kLegacyPlainTableMagicNumber) {
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
    if (verbose_) {
      fprintf(stdout, "Sst file format: plain table\n");
    }
  } else {
    char error_msg_buffer[80];
    snprintf(error_msg_buffer, sizeof(error_msg_buffer) - 1,
             "Unsupported table magic number --- %lx",
             (long)table_magic_number);
    return Status::InvalidArgument(error_msg_buffer);
  }

  return Status::OK();
}

Status SstFileReader::SetOldTableOptions() {
  assert(table_properties_ == nullptr);
  options_.table_factory = std::make_shared<BlockBasedTableFactory>();
  if (verbose_) {
    fprintf(stdout, "Sst file format: block-based(old version)\n");
  }

  return Status::OK();
}

Status SstFileReader::ReadSequential(bool print_kv, uint64_t read_num,
                                     bool has_from, const std::string& from_key,
                                     bool has_to, const std::string& to_key,
                                     bool use_from_as_prefix) {
  if (!table_reader_) {
    return init_result_;
  }

  InternalIterator* iter =
      table_reader_->NewIterator(ReadOptions(verify_checksum_, false));
  uint64_t i = 0;
  if (has_from) {
    InternalKey ikey;
    ikey.SetMinPossibleForUserKey(from_key);
    iter->Seek(ikey.Encode());
  } else {
    iter->SeekToFirst();
  }
  for (; iter->Valid(); iter->Next()) {
    Slice key = iter->key();
    Slice value = iter->value();
    ++i;
    if (read_num > 0 && i > read_num) break;

    ParsedInternalKey ikey;
    if (!ParseInternalKey(key, &ikey)) {
      if (verbose_) {
        std::cerr << "Internal Key [" << key.ToString(true /* in hex*/)
                  << "] parse error!\n";
      }
      continue;
    }

    // the key returned is not prefixed with out 'from' key
    if (use_from_as_prefix && !ikey.user_key.starts_with(from_key)) {
      break;
    }

    // If end marker was specified, we stop before it
    if (has_to && BytewiseComparator()->Compare(ikey.user_key, to_key) >= 0) {
      break;
    }

    if (print_kv && verbose_) {
      fprintf(stdout, "%s => %s\n", ikey.DebugString(output_hex_).c_str(),
              value.ToString(output_hex_).c_str());
    }
  }

  read_num_ += i;

  Status ret = iter->status();
  delete iter;
  return ret;
}

Status SstFileReader::ReadTableProperties(
    std::shared_ptr<const TableProperties>* table_properties) {
  if (!table_reader_) {
    return init_result_;
  }

  *table_properties = table_reader_->GetTableProperties();
  return init_result_;
}

}  // namespace rocksdb