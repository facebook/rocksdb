//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "table/sst_file_dumper.h"

#include <chrono>
#include <cinttypes>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_builder.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/plain/plain_table_factory.h"
#include "table/table_reader.h"
#include "util/compression.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

SstFileDumper::SstFileDumper(const Options& options,
                             const std::string& file_path,
                             size_t readahead_size, bool verify_checksum,
                             bool output_hex, bool decode_blob_index,
                             const EnvOptions& soptions, bool silent)
    : file_name_(file_path),
      read_num_(0),
      output_hex_(output_hex),
      decode_blob_index_(decode_blob_index),
      soptions_(soptions),
      silent_(silent),
      options_(options),
      ioptions_(options_),
      moptions_(ColumnFamilyOptions(options_)),
      read_options_(verify_checksum, false),
      internal_comparator_(BytewiseComparator()) {
  read_options_.readahead_size = readahead_size;
  if (!silent_) {
    fprintf(stdout, "Process %s\n", file_path.c_str());
  }
  init_result_ = GetTableReader(file_name_);
}

extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;

const char* testFileName = "test_file_name";

Status SstFileDumper::GetTableReader(const std::string& file_path) {
  // Warning about 'magic_number' being uninitialized shows up only in UBsan
  // builds. Though access is guarded by 's.ok()' checks, fix the issue to
  // avoid any warnings.
  uint64_t magic_number = Footer::kNullTableMagicNumber;

  // read table magic number
  Footer footer;

  const auto& fs = options_.env->GetFileSystem();
  std::unique_ptr<FSRandomAccessFile> file;
  uint64_t file_size = 0;
  Status s = fs->NewRandomAccessFile(file_path, FileOptions(soptions_), &file,
                                     nullptr);
  if (s.ok()) {
    s = fs->GetFileSize(file_path, IOOptions(), &file_size, nullptr);
  }

  // check empty file
  // if true, skip further processing of this file
  if (file_size == 0) {
    return Status::Aborted(file_path, "Empty file");
  }

  file_.reset(new RandomAccessFileReader(std::move(file), file_path));

  FilePrefetchBuffer prefetch_buffer(
      0 /* readahead_size */, 0 /* max_readahead_size */, true /* enable */,
      false /* track_min_offset */);
  if (s.ok()) {
    const uint64_t kSstDumpTailPrefetchSize = 512 * 1024;
    uint64_t prefetch_size = (file_size > kSstDumpTailPrefetchSize)
                                 ? kSstDumpTailPrefetchSize
                                 : file_size;
    uint64_t prefetch_off = file_size - prefetch_size;
    IOOptions opts;
    s = prefetch_buffer.Prefetch(opts, file_.get(), prefetch_off,
                                 static_cast<size_t>(prefetch_size),
                                 Env::IO_TOTAL /* rate_limiter_priority */);

    s = ReadFooterFromFile(opts, file_.get(), &prefetch_buffer, file_size,
                           &footer);
  }
  if (s.ok()) {
    magic_number = footer.table_magic_number();
  }

  if (s.ok()) {
    if (magic_number == kPlainTableMagicNumber ||
        magic_number == kLegacyPlainTableMagicNumber) {
      soptions_.use_mmap_reads = true;

      fs->NewRandomAccessFile(file_path, FileOptions(soptions_), &file,
                              nullptr);
      file_.reset(new RandomAccessFileReader(std::move(file), file_path));
    }

    // For old sst format, ReadTableProperties might fail but file can be read
    if (ReadTableProperties(magic_number, file_.get(), file_size,
                            (magic_number == kBlockBasedTableMagicNumber)
                                ? &prefetch_buffer
                                : nullptr)
            .ok()) {
      s = SetTableOptionsByMagicNumber(magic_number);
      if (s.ok()) {
        if (table_properties_ && !table_properties_->comparator_name.empty()) {
          ConfigOptions config_options;
          const Comparator* user_comparator = nullptr;
          s = Comparator::CreateFromString(config_options,
                                           table_properties_->comparator_name,
                                           &user_comparator);
          if (s.ok()) {
            assert(user_comparator);
            internal_comparator_ =
                InternalKeyComparator(user_comparator, /*named=*/true);
          }
        }
      }
    } else {
      s = SetOldTableOptions();
    }
    options_.comparator = internal_comparator_.user_comparator();
  }

  if (s.ok()) {
    s = NewTableReader(ioptions_, soptions_, internal_comparator_, file_size,
                       &table_reader_);
  }
  return s;
}

Status SstFileDumper::NewTableReader(
    const ImmutableOptions& /*ioptions*/, const EnvOptions& /*soptions*/,
    const InternalKeyComparator& /*internal_comparator*/, uint64_t file_size,
    std::unique_ptr<TableReader>* /*table_reader*/) {
  auto t_opt =
      TableReaderOptions(ioptions_, moptions_.prefix_extractor, soptions_,
                         internal_comparator_, false /* skip_filters */,
                         false /* imortal */, true /* force_direct_prefetch */);
  // Allow open file with global sequence number for backward compatibility.
  t_opt.largest_seqno = kMaxSequenceNumber;

  // We need to turn off pre-fetching of index and filter nodes for
  // BlockBasedTable
  if (options_.table_factory->IsInstanceOf(
          TableFactory::kBlockBasedTableName())) {
    return options_.table_factory->NewTableReader(t_opt, std::move(file_),
                                                  file_size, &table_reader_,
                                                  /*enable_prefetch=*/false);
  }

  // For all other factory implementation
  return options_.table_factory->NewTableReader(t_opt, std::move(file_),
                                                file_size, &table_reader_);
}

Status SstFileDumper::VerifyChecksum() {
  // We could pass specific readahead setting into read options if needed.
  return table_reader_->VerifyChecksum(read_options_,
                                       TableReaderCaller::kSSTDumpTool);
}

Status SstFileDumper::DumpTable(const std::string& out_filename) {
  std::unique_ptr<WritableFile> out_file;
  Env* env = options_.env;
  Status s = env->NewWritableFile(out_filename, &out_file, soptions_);
  if (s.ok()) {
    s = table_reader_->DumpTable(out_file.get());
  }
  if (!s.ok()) {
    // close the file before return error, ignore the close error if there's any
    out_file->Close().PermitUncheckedError();
    return s;
  }
  return out_file->Close();
}

Status SstFileDumper::CalculateCompressedTableSize(
    const TableBuilderOptions& tb_options, size_t block_size,
    uint64_t* num_data_blocks, uint64_t* compressed_table_size) {
  std::unique_ptr<Env> env(NewMemEnv(options_.env));
  std::unique_ptr<WritableFileWriter> dest_writer;
  Status s =
      WritableFileWriter::Create(env->GetFileSystem(), testFileName,
                                 FileOptions(soptions_), &dest_writer, nullptr);
  if (!s.ok()) {
    return s;
  }
  BlockBasedTableOptions table_options;
  table_options.block_size = block_size;
  BlockBasedTableFactory block_based_tf(table_options);
  std::unique_ptr<TableBuilder> table_builder;
  table_builder.reset(block_based_tf.NewTableBuilder(
      tb_options,
      dest_writer.get()));
  std::unique_ptr<InternalIterator> iter(table_reader_->NewIterator(
      read_options_, moptions_.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kSSTDumpTool));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    table_builder->Add(iter->key(), iter->value());
  }
  s = iter->status();
  if (!s.ok()) {
    return s;
  }
  s = table_builder->Finish();
  if (!s.ok()) {
    return s;
  }
  *compressed_table_size = table_builder->FileSize();
  assert(num_data_blocks != nullptr);
  *num_data_blocks = table_builder->GetTableProperties().num_data_blocks;
  return env->DeleteFile(testFileName);
}

Status SstFileDumper::ShowAllCompressionSizes(
    size_t block_size,
    const std::vector<std::pair<CompressionType, const char*>>&
        compression_types,
    int32_t compress_level_from, int32_t compress_level_to,
    uint32_t max_dict_bytes, uint32_t zstd_max_train_bytes,
    uint64_t max_dict_buffer_bytes) {
  fprintf(stdout, "Block Size: %" ROCKSDB_PRIszt "\n", block_size);
  for (auto& i : compression_types) {
    if (CompressionTypeSupported(i.first)) {
      fprintf(stdout, "Compression: %-24s\n", i.second);
      CompressionOptions compress_opt;
      compress_opt.max_dict_bytes = max_dict_bytes;
      compress_opt.zstd_max_train_bytes = zstd_max_train_bytes;
      compress_opt.max_dict_buffer_bytes = max_dict_buffer_bytes;
      for (int32_t j = compress_level_from; j <= compress_level_to; j++) {
        fprintf(stdout, "Compression level: %d", j);
        compress_opt.level = j;
        Status s = ShowCompressionSize(block_size, i.first, compress_opt);
        if (!s.ok()) {
          return s;
        }
      }
    } else {
      fprintf(stdout, "Unsupported compression type: %s.\n", i.second);
    }
  }
  return Status::OK();
}

Status SstFileDumper::ShowCompressionSize(
    size_t block_size, CompressionType compress_type,
    const CompressionOptions& compress_opt) {
  Options opts;
  opts.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  opts.statistics->set_stats_level(StatsLevel::kAll);
  const ImmutableOptions imoptions(opts);
  const ColumnFamilyOptions cfo(opts);
  const MutableCFOptions moptions(cfo);
  ROCKSDB_NAMESPACE::InternalKeyComparator ikc(opts.comparator);
  IntTblPropCollectorFactories block_based_table_factories;

  std::string column_family_name;
  int unknown_level = -1;
  TableBuilderOptions tb_opts(
      imoptions, moptions, ikc, &block_based_table_factories, compress_type,
      compress_opt,
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
      column_family_name, unknown_level);
  uint64_t num_data_blocks = 0;
  std::chrono::steady_clock::time_point start =
      std::chrono::steady_clock::now();
  uint64_t file_size;
  Status s = CalculateCompressedTableSize(tb_opts, block_size, &num_data_blocks,
                                          &file_size);
  if (!s.ok()) {
    return s;
  }

  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  fprintf(stdout, " Size: %10" PRIu64, file_size);
  fprintf(stdout, " Blocks: %6" PRIu64, num_data_blocks);
  fprintf(stdout, " Time Taken: %10s microsecs",
          std::to_string(
              std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                  .count())
              .c_str());
  const uint64_t compressed_blocks =
      opts.statistics->getAndResetTickerCount(NUMBER_BLOCK_COMPRESSED);
  const uint64_t not_compressed_blocks =
      opts.statistics->getAndResetTickerCount(NUMBER_BLOCK_NOT_COMPRESSED);
  // When the option enable_index_compression is true,
  // NUMBER_BLOCK_COMPRESSED is incremented for index block(s).
  if ((compressed_blocks + not_compressed_blocks) > num_data_blocks) {
    num_data_blocks = compressed_blocks + not_compressed_blocks;
  }

  const uint64_t ratio_not_compressed_blocks =
      (num_data_blocks - compressed_blocks) - not_compressed_blocks;
  const double compressed_pcnt =
      (0 == num_data_blocks) ? 0.0
                             : ((static_cast<double>(compressed_blocks) /
                                 static_cast<double>(num_data_blocks)) *
                                100.0);
  const double ratio_not_compressed_pcnt =
      (0 == num_data_blocks)
          ? 0.0
          : ((static_cast<double>(ratio_not_compressed_blocks) /
              static_cast<double>(num_data_blocks)) *
             100.0);
  const double not_compressed_pcnt =
      (0 == num_data_blocks) ? 0.0
                             : ((static_cast<double>(not_compressed_blocks) /
                                 static_cast<double>(num_data_blocks)) *
                                100.0);
  fprintf(stdout, " Compressed: %6" PRIu64 " (%5.1f%%)", compressed_blocks,
          compressed_pcnt);
  fprintf(stdout, " Not compressed (ratio): %6" PRIu64 " (%5.1f%%)",
          ratio_not_compressed_blocks, ratio_not_compressed_pcnt);
  fprintf(stdout, " Not compressed (abort): %6" PRIu64 " (%5.1f%%)\n",
          not_compressed_blocks, not_compressed_pcnt);
  return Status::OK();
}

// Reads TableProperties prior to opening table reader in order to set up
// options.
Status SstFileDumper::ReadTableProperties(uint64_t table_magic_number,
                                          RandomAccessFileReader* file,
                                          uint64_t file_size,
                                          FilePrefetchBuffer* prefetch_buffer) {
  Status s = ROCKSDB_NAMESPACE::ReadTableProperties(
      file, file_size, table_magic_number, ioptions_, &table_properties_,
      /* memory_allocator= */ nullptr, prefetch_buffer);
  if (!s.ok()) {
    if (!silent_) {
      fprintf(stdout, "Not able to read table properties\n");
    }
  }
  return s;
}

Status SstFileDumper::SetTableOptionsByMagicNumber(
    uint64_t table_magic_number) {
  assert(table_properties_);
  if (table_magic_number == kBlockBasedTableMagicNumber ||
      table_magic_number == kLegacyBlockBasedTableMagicNumber) {
    BlockBasedTableFactory* bbtf = new BlockBasedTableFactory();
    // To force tail prefetching, we fake reporting two useful reads of 512KB
    // from the tail.
    // It needs at least two data points to warm up the stats.
    bbtf->tail_prefetch_stats()->RecordEffectiveSize(512 * 1024);
    bbtf->tail_prefetch_stats()->RecordEffectiveSize(512 * 1024);

    options_.table_factory.reset(bbtf);
    if (!silent_) {
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
    if (!silent_) {
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

Status SstFileDumper::SetOldTableOptions() {
  assert(table_properties_ == nullptr);
  options_.table_factory = std::make_shared<BlockBasedTableFactory>();
  if (!silent_) {
    fprintf(stdout, "Sst file format: block-based(old version)\n");
  }

  return Status::OK();
}

Status SstFileDumper::ReadSequential(bool print_kv, uint64_t read_num,
                                     bool has_from, const std::string& from_key,
                                     bool has_to, const std::string& to_key,
                                     bool use_from_as_prefix) {
  if (!table_reader_) {
    return init_result_;
  }

  InternalIterator* iter = table_reader_->NewIterator(
      read_options_, moptions_.prefix_extractor.get(),
      /*arena=*/nullptr, /*skip_filters=*/false,
      TableReaderCaller::kSSTDumpTool);
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
    Status pik_status = ParseInternalKey(key, &ikey, true /* log_err_key */);
    if (!pik_status.ok()) {
      std::cerr << pik_status.getState() << "\n";
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

    if (print_kv) {
      if (!decode_blob_index_ || ikey.type != kTypeBlobIndex) {
        fprintf(stdout, "%s => %s\n",
                ikey.DebugString(true, output_hex_).c_str(),
                value.ToString(output_hex_).c_str());
      } else {
        BlobIndex blob_index;

        const Status s = blob_index.DecodeFrom(value);
        if (!s.ok()) {
          fprintf(stderr, "%s => error decoding blob index\n",
                  ikey.DebugString(true, output_hex_).c_str());
          continue;
        }

        fprintf(stdout, "%s => %s\n",
                ikey.DebugString(true, output_hex_).c_str(),
                blob_index.DebugString(output_hex_).c_str());
      }
    }
  }

  read_num_ += i;

  Status ret = iter->status();
  delete iter;
  return ret;
}

// Provides TableProperties to API user
Status SstFileDumper::ReadTableProperties(
    std::shared_ptr<const TableProperties>* table_properties) {
  if (!table_reader_) {
    return init_result_;
  }

  *table_properties = table_reader_->GetTableProperties();
  return init_result_;
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
