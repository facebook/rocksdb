
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "tools/sst_dump_tool_imp.h"

#include <cinttypes>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "env/composite_env_wrapper.h"
#include "options/cf_options.h"
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

#include "port/port.h"

namespace ROCKSDB_NAMESPACE {

SstFileDumper::SstFileDumper(const Options& options,
                             const std::string& file_path,
                             size_t readahead_size, bool verify_checksum,
                             bool output_hex, bool decode_blob_index)
    : file_name_(file_path),
      read_num_(0),
      output_hex_(output_hex),
      decode_blob_index_(decode_blob_index),
      options_(options),
      ioptions_(options_),
      moptions_(ColumnFamilyOptions(options_)),
      read_options_(verify_checksum, false),
      internal_comparator_(BytewiseComparator()) {
  read_options_.readahead_size = readahead_size;
  fprintf(stdout, "Process %s\n", file_path.c_str());
  init_result_ = GetTableReader(file_name_);
}

extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;

const char* testFileName = "test_file_name";

static const std::vector<std::pair<CompressionType, const char*>>
    kCompressions = {
        {CompressionType::kNoCompression, "kNoCompression"},
        {CompressionType::kSnappyCompression, "kSnappyCompression"},
        {CompressionType::kZlibCompression, "kZlibCompression"},
        {CompressionType::kBZip2Compression, "kBZip2Compression"},
        {CompressionType::kLZ4Compression, "kLZ4Compression"},
        {CompressionType::kLZ4HCCompression, "kLZ4HCCompression"},
        {CompressionType::kXpressCompression, "kXpressCompression"},
        {CompressionType::kZSTD, "kZSTD"}};

Status SstFileDumper::GetTableReader(const std::string& file_path) {
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

  // check empty file
  // if true, skip further processing of this file
  if (file_size == 0) {
    return Status::Aborted(file_path, "Empty file");
  }

  file_.reset(new RandomAccessFileReader(NewLegacyRandomAccessFileWrapper(file),
                                         file_path));

  FilePrefetchBuffer prefetch_buffer(nullptr, 0, 0, true /* enable */,
                                     false /* track_min_offset */);
  if (s.ok()) {
    const uint64_t kSstDumpTailPrefetchSize = 512 * 1024;
    uint64_t prefetch_size = (file_size > kSstDumpTailPrefetchSize)
                                 ? kSstDumpTailPrefetchSize
                                 : file_size;
    uint64_t prefetch_off = file_size - prefetch_size;
    prefetch_buffer.Prefetch(file_.get(), prefetch_off,
                             static_cast<size_t>(prefetch_size));

    s = ReadFooterFromFile(file_.get(), &prefetch_buffer, file_size, &footer);
  }
  if (s.ok()) {
    magic_number = footer.table_magic_number();
  }

  if (s.ok()) {
    if (magic_number == kPlainTableMagicNumber ||
        magic_number == kLegacyPlainTableMagicNumber) {
      soptions_.use_mmap_reads = true;
      options_.env->NewRandomAccessFile(file_path, &file, soptions_);
      file_.reset(new RandomAccessFileReader(
          NewLegacyRandomAccessFileWrapper(file), file_path));
    }
    options_.comparator = &internal_comparator_;
    // For old sst format, ReadTableProperties might fail but file can be read
    if (ReadTableProperties(magic_number, file_.get(), file_size,
                            (magic_number == kBlockBasedTableMagicNumber)
                                ? &prefetch_buffer
                                : nullptr)
            .ok()) {
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

Status SstFileDumper::NewTableReader(
    const ImmutableCFOptions& /*ioptions*/, const EnvOptions& /*soptions*/,
    const InternalKeyComparator& /*internal_comparator*/, uint64_t file_size,
    std::unique_ptr<TableReader>* /*table_reader*/) {
  auto t_opt =
      TableReaderOptions(ioptions_, moptions_.prefix_extractor.get(), soptions_,
                         internal_comparator_, false /* skip_filters */,
                         false /* imortal */, true /* force_direct_prefetch */);
  // Allow open file with global sequence number for backward compatibility.
  t_opt.largest_seqno = kMaxSequenceNumber;

  // We need to turn off pre-fetching of index and filter nodes for
  // BlockBasedTable
  if (BlockBasedTableFactory::kName == options_.table_factory->Name()) {
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
  env->NewWritableFile(out_filename, &out_file, soptions_);
  Status s = table_reader_->DumpTable(out_file.get());
  out_file->Close();
  return s;
}

uint64_t SstFileDumper::CalculateCompressedTableSize(
    const TableBuilderOptions& tb_options, size_t block_size,
    uint64_t* num_data_blocks) {
  std::unique_ptr<WritableFile> out_file;
  std::unique_ptr<Env> env(NewMemEnv(options_.env));
  env->NewWritableFile(testFileName, &out_file, soptions_);
  std::unique_ptr<WritableFileWriter> dest_writer;
  dest_writer.reset(
      new WritableFileWriter(NewLegacyWritableFileWrapper(std::move(out_file)),
                             testFileName, soptions_));
  BlockBasedTableOptions table_options;
  table_options.block_size = block_size;
  BlockBasedTableFactory block_based_tf(table_options);
  std::unique_ptr<TableBuilder> table_builder;
  table_builder.reset(block_based_tf.NewTableBuilder(
      tb_options,
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
      dest_writer.get()));
  std::unique_ptr<InternalIterator> iter(table_reader_->NewIterator(
      read_options_, moptions_.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kSSTDumpTool));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    table_builder->Add(iter->key(), iter->value());
  }
  if (!iter->status().ok()) {
    fputs(iter->status().ToString().c_str(), stderr);
    exit(1);
  }
  Status s = table_builder->Finish();
  if (!s.ok()) {
    fputs(s.ToString().c_str(), stderr);
    exit(1);
  }
  uint64_t size = table_builder->FileSize();
  assert(num_data_blocks != nullptr);
  *num_data_blocks = table_builder->GetTableProperties().num_data_blocks;
  env->DeleteFile(testFileName);
  return size;
}

int SstFileDumper::ShowAllCompressionSizes(
    size_t block_size,
    const std::vector<std::pair<CompressionType, const char*>>&
      compression_types,
    int32_t compress_level_from,
    int32_t compress_level_to) {

  fprintf(stdout, "Block Size: %" ROCKSDB_PRIszt "\n", block_size);
  for (auto& i : compression_types) {
    if (CompressionTypeSupported(i.first)) {
      fprintf(stdout, "Compression: %-24s\n", i.second);
      CompressionOptions compress_opt;
      for(int32_t j = compress_level_from; j <= compress_level_to;
          j++) {
        fprintf(stdout, "Compression level: %d", j);
        compress_opt.level = j;
        ShowCompressionSize(block_size, i.first, compress_opt);
      }
    } else {
      fprintf(stdout, "Unsupported compression type: %s.\n", i.second);
    }
  }
  return 0;
}

int SstFileDumper::ShowCompressionSize(
    size_t block_size,
    CompressionType compress_type,
    const CompressionOptions& compress_opt) {
  Options opts;
  opts.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  opts.statistics->set_stats_level(StatsLevel::kAll);
  const ImmutableCFOptions imoptions(opts);
  const ColumnFamilyOptions cfo(opts);
  const MutableCFOptions moptions(cfo);
  ROCKSDB_NAMESPACE::InternalKeyComparator ikc(opts.comparator);
  std::vector<std::unique_ptr<IntTblPropCollectorFactory> >
      block_based_table_factories;

  std::string column_family_name;
  int unknown_level = -1;
  TableBuilderOptions tb_opts(
      imoptions, moptions, ikc, &block_based_table_factories, compress_type,
      0 /* sample_for_compression */, compress_opt,
      false /* skip_filters */, column_family_name, unknown_level);
  uint64_t num_data_blocks = 0;
  std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
  uint64_t file_size =
      CalculateCompressedTableSize(tb_opts, block_size, &num_data_blocks);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  fprintf(stdout, " Size: %10" PRIu64, file_size);
  fprintf(stdout, " Blocks: %6" PRIu64, num_data_blocks);
  fprintf(stdout, " Time Taken: %10s microsecs",
          std::to_string(std::chrono::duration_cast<std::chrono::microseconds>
                         (end-start).count()).c_str());
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
       (0 == num_data_blocks)
           ? 0.0
          : ((static_cast<double>(not_compressed_blocks) /
              static_cast<double>(num_data_blocks)) *
             100.0);
  fprintf(stdout, " Compressed: %6" PRIu64 " (%5.1f%%)", compressed_blocks,
          compressed_pcnt);
  fprintf(stdout, " Not compressed (ratio): %6" PRIu64 " (%5.1f%%)",
          ratio_not_compressed_blocks, ratio_not_compressed_pcnt);
  fprintf(stdout, " Not compressed (abort): %6" PRIu64 " (%5.1f%%)\n",
          not_compressed_blocks, not_compressed_pcnt);
  return 0;
}

Status SstFileDumper::ReadTableProperties(uint64_t table_magic_number,
                                          RandomAccessFileReader* file,
                                          uint64_t file_size,
                                          FilePrefetchBuffer* prefetch_buffer) {
  TableProperties* table_properties = nullptr;
  Status s = ROCKSDB_NAMESPACE::ReadTableProperties(
      file, file_size, table_magic_number, ioptions_, &table_properties,
      /* compression_type_missing= */ false,
      /* memory_allocator= */ nullptr, prefetch_buffer);
  if (s.ok()) {
    table_properties_.reset(table_properties);
  } else {
    fprintf(stdout, "Not able to read table properties\n");
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
    fprintf(stdout, "Sst file format: block-based\n");

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
    fprintf(stdout, "Sst file format: plain table\n");
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
  fprintf(stdout, "Sst file format: block-based(old version)\n");

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
    if (read_num > 0 && i > read_num)
      break;

    ParsedInternalKey ikey;
    if (!ParseInternalKey(key, &ikey)) {
      std::cerr << "Internal Key ["
                << key.ToString(true /* in hex*/)
                << "] parse error!\n";
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
        fprintf(stdout, "%s => %s\n", ikey.DebugString(output_hex_).c_str(),
                value.ToString(output_hex_).c_str());
      } else {
        BlobIndex blob_index;

        const Status s = blob_index.DecodeFrom(value);
        if (!s.ok()) {
          fprintf(stderr, "%s => error decoding blob index\n",
                  ikey.DebugString(output_hex_).c_str());
          continue;
        }

        fprintf(stdout, "%s => %s\n", ikey.DebugString(output_hex_).c_str(),
                blob_index.DebugString(output_hex_).c_str());
      }
    }
  }

  read_num_ += i;

  Status ret = iter->status();
  delete iter;
  return ret;
}

Status SstFileDumper::ReadTableProperties(
    std::shared_ptr<const TableProperties>* table_properties) {
  if (!table_reader_) {
    return init_result_;
  }

  *table_properties = table_reader_->GetTableProperties();
  return init_result_;
}

namespace {

void print_help(bool to_stderr) {
  fprintf(
      to_stderr ? stderr : stdout,
      R"(sst_dump --file=<data_dir_OR_sst_file> [--command=check|scan|raw|recompress|identify]
    --file=<data_dir_OR_sst_file>
      Path to SST file or directory containing SST files

    --env_uri=<uri of underlying Env>
      URI of underlying Env

    --command=check|scan|raw|verify|identify
        check: Iterate over entries in files but don't print anything except if an error is encountered (default command)
        scan: Iterate over entries in files and print them to screen
        raw: Dump all the table contents to <file_name>_dump.txt
        verify: Iterate all the blocks in files verifying checksum to detect possible corruption but don't print anything except if a corruption is encountered
        recompress: reports the SST file size if recompressed with different
                    compression types
        identify: Reports a file is a valid SST file or lists all valid SST files under a directory

    --output_hex
      Can be combined with scan command to print the keys and values in Hex

    --decode_blob_index
      Decode blob indexes and print them in a human-readable format during scans.

    --from=<user_key>
      Key to start reading from when executing check|scan

    --to=<user_key>
      Key to stop reading at when executing check|scan

    --prefix=<user_key>
      Returns all keys with this prefix when executing check|scan
      Cannot be used in conjunction with --from

    --read_num=<num>
      Maximum number of entries to read when executing check|scan

    --verify_checksum
      Verify file checksum when executing check|scan

    --input_key_hex
      Can be combined with --from and --to to indicate that these values are encoded in Hex

    --show_properties
      Print table properties after iterating over the file when executing
      check|scan|raw|identify

    --set_block_size=<block_size>
      Can be combined with --command=recompress to set the block size that will
      be used when trying different compression algorithms

    --compression_types=<comma-separated list of CompressionType members, e.g.,
      kSnappyCompression>
      Can be combined with --command=recompress to run recompression for this
      list of compression types

    --parse_internal_key=<0xKEY>
      Convenience option to parse an internal key on the command line. Dumps the
      internal key in hex format {'key' @ SN: type}

    --compression_level_from=<compression_level>
      Compression level to start compressing when executing recompress. One compression type
      and compression_level_to must also be specified

    --compression_level_to=<compression_level>
      Compression level to stop compressing when executing recompress. One compression type
      and compression_level_from must also be specified
)");
}

// arg_name would include all prefix, e.g. "--my_arg="
// arg_val is the parses value.
// True if there is a match. False otherwise.
// Woud exit after printing errmsg if cannot be parsed.
bool ParseIntArg(const char* arg, const std::string arg_name,
                 const std::string err_msg, int64_t* arg_val) {
  if (strncmp(arg, arg_name.c_str(), arg_name.size()) == 0) {
    std::string input_str = arg + arg_name.size();
    std::istringstream iss(input_str);
    iss >> *arg_val;
    if (iss.fail()) {
      fprintf(stderr, "%s\n", err_msg.c_str());
      exit(1);
    }
    return true;
  }
  return false;
}
}  // namespace

int SSTDumpTool::Run(int argc, char const* const* argv, Options options) {
  const char* env_uri = nullptr;
  const char* dir_or_file = nullptr;
  uint64_t read_num = std::numeric_limits<uint64_t>::max();
  std::string command;

  char junk;
  uint64_t n;
  bool verify_checksum = false;
  bool output_hex = false;
  bool decode_blob_index = false;
  bool input_key_hex = false;
  bool has_from = false;
  bool has_to = false;
  bool use_from_as_prefix = false;
  bool show_properties = false;
  bool show_summary = false;
  bool set_block_size = false;
  bool has_compression_level_from = false;
  bool has_compression_level_to = false;
  bool has_specified_compression_types = false;
  std::string from_key;
  std::string to_key;
  std::string block_size_str;
  std::string compression_level_from_str;
  std::string compression_level_to_str;
  size_t block_size = 0;
  size_t readahead_size = 2 * 1024 * 1024;
  std::vector<std::pair<CompressionType, const char*>> compression_types;
  uint64_t total_num_files = 0;
  uint64_t total_num_data_blocks = 0;
  uint64_t total_data_block_size = 0;
  uint64_t total_index_block_size = 0;
  uint64_t total_filter_block_size = 0;
  int32_t compress_level_from = CompressionOptions::kDefaultCompressionLevel;
  int32_t compress_level_to = CompressionOptions::kDefaultCompressionLevel;

  int64_t tmp_val;

  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--env_uri=", 10) == 0) {
      env_uri = argv[i] + 10;
    } else if (strncmp(argv[i], "--file=", 7) == 0) {
      dir_or_file = argv[i] + 7;
    } else if (strcmp(argv[i], "--output_hex") == 0) {
      output_hex = true;
    } else if (strcmp(argv[i], "--decode_blob_index") == 0) {
      decode_blob_index = true;
    } else if (strcmp(argv[i], "--input_key_hex") == 0) {
      input_key_hex = true;
    } else if (sscanf(argv[i], "--read_num=%lu%c", (unsigned long*)&n, &junk) ==
               1) {
      read_num = n;
    } else if (strcmp(argv[i], "--verify_checksum") == 0) {
      verify_checksum = true;
    } else if (strncmp(argv[i], "--command=", 10) == 0) {
      command = argv[i] + 10;
    } else if (strncmp(argv[i], "--from=", 7) == 0) {
      from_key = argv[i] + 7;
      has_from = true;
    } else if (strncmp(argv[i], "--to=", 5) == 0) {
      to_key = argv[i] + 5;
      has_to = true;
    } else if (strncmp(argv[i], "--prefix=", 9) == 0) {
      from_key = argv[i] + 9;
      use_from_as_prefix = true;
    } else if (strcmp(argv[i], "--show_properties") == 0) {
      show_properties = true;
    } else if (strcmp(argv[i], "--show_summary") == 0) {
      show_summary = true;
    } else if (ParseIntArg(argv[i], "--set_block_size=",
                           "block size must be numeric", &tmp_val)) {
      set_block_size = true;
      block_size = static_cast<size_t>(tmp_val);
    } else if (ParseIntArg(argv[i], "--readahead_size=",
                           "readahead_size must be numeric", &tmp_val)) {
      readahead_size = static_cast<size_t>(tmp_val);
    } else if (strncmp(argv[i], "--compression_types=", 20) == 0) {
      std::string compression_types_csv = argv[i] + 20;
      std::istringstream iss(compression_types_csv);
      std::string compression_type;
      has_specified_compression_types = true;
      while (std::getline(iss, compression_type, ',')) {
        auto iter = std::find_if(
            kCompressions.begin(), kCompressions.end(),
            [&compression_type](std::pair<CompressionType, const char*> curr) {
              return curr.second == compression_type;
            });
        if (iter == kCompressions.end()) {
          fprintf(stderr, "%s is not a valid CompressionType\n",
                  compression_type.c_str());
          exit(1);
        }
        compression_types.emplace_back(*iter);
      }
    } else if (strncmp(argv[i], "--parse_internal_key=", 21) == 0) {
      std::string in_key(argv[i] + 21);
      try {
        in_key = ROCKSDB_NAMESPACE::LDBCommand::HexToString(in_key);
      } catch (...) {
        std::cerr << "ERROR: Invalid key input '"
          << in_key
          << "' Use 0x{hex representation of internal rocksdb key}" << std::endl;
        return -1;
      }
      Slice sl_key = ROCKSDB_NAMESPACE::Slice(in_key);
      ParsedInternalKey ikey;
      int retc = 0;
      if (!ParseInternalKey(sl_key, &ikey)) {
        std::cerr << "Internal Key [" << sl_key.ToString(true /* in hex*/)
                  << "] parse error!\n";
        retc = -1;
      }
      fprintf(stdout, "key=%s\n", ikey.DebugString(true).c_str());
      return retc;
    } else if (ParseIntArg(argv[i], "--compression_level_from=",
                           "compression_level_from must be numeric",
                           &tmp_val)) {
      has_compression_level_from = true;
      compress_level_from = static_cast<int>(tmp_val);
    } else if (ParseIntArg(argv[i], "--compression_level_to=",
                           "compression_level_to must be numeric", &tmp_val)) {
      has_compression_level_to = true;
      compress_level_to = static_cast<int>(tmp_val);
    } else if (strcmp(argv[i], "--help") == 0) {
      print_help(/*to_stderr*/ false);
      return 0;
    } else if (strcmp(argv[i], "--version") == 0) {
      printf("sst_dump from RocksDB %d.%d.%d\n", ROCKSDB_MAJOR, ROCKSDB_MINOR,
             ROCKSDB_PATCH);
      return 0;
    } else {
      fprintf(stderr, "Unrecognized argument '%s'\n\n", argv[i]);
      print_help(/*to_stderr*/ true);
      return 1;
    }
  }

  if(has_compression_level_from && has_compression_level_to) {
    if(!has_specified_compression_types || compression_types.size() != 1) {
      fprintf(stderr, "Specify one compression type.\n\n");
      exit(1);
    }
  } else if(has_compression_level_from || has_compression_level_to) {
    fprintf(stderr, "Specify both --compression_level_from and "
                     "--compression_level_to.\n\n");
    exit(1);
  }

  if (use_from_as_prefix && has_from) {
    fprintf(stderr, "Cannot specify --prefix and --from\n\n");
    exit(1);
  }

  if (input_key_hex) {
    if (has_from || use_from_as_prefix) {
      from_key = ROCKSDB_NAMESPACE::LDBCommand::HexToString(from_key);
    }
    if (has_to) {
      to_key = ROCKSDB_NAMESPACE::LDBCommand::HexToString(to_key);
    }
  }

  if (dir_or_file == nullptr) {
    fprintf(stderr, "file or directory must be specified.\n\n");
    print_help(/*to_stderr*/ true);
    exit(1);
  }

  std::shared_ptr<ROCKSDB_NAMESPACE::Env> env_guard;

  // If caller of SSTDumpTool::Run(...) does not specify a different env other
  // than Env::Default(), then try to load custom env based on dir_or_file.
  // Otherwise, the caller is responsible for creating custom env.
  if (!options.env || options.env == ROCKSDB_NAMESPACE::Env::Default()) {
    Env* env = Env::Default();
    Status s = Env::LoadEnv(env_uri ? env_uri : "", &env, &env_guard);
    if (!s.ok() && !s.IsNotFound()) {
      fprintf(stderr, "LoadEnv: %s\n", s.ToString().c_str());
      exit(1);
    }
    options.env = env;
  } else {
    fprintf(stdout, "options.env is %p\n", options.env);
  }

  std::vector<std::string> filenames;
  ROCKSDB_NAMESPACE::Env* env = options.env;
  ROCKSDB_NAMESPACE::Status st = env->GetChildren(dir_or_file, &filenames);
  bool dir = true;
  if (!st.ok() || filenames.empty()) {
    // dir_or_file does not exist or does not contain children
    // Check its existence first
    Status s = env->FileExists(dir_or_file);
    // dir_or_file does not exist
    if (!s.ok()) {
      fprintf(stderr, "%s%s: No such file or directory\n", s.ToString().c_str(),
              dir_or_file);
      return 1;
    }
    // dir_or_file exists and is treated as a "file"
    // since it has no children
    // This is ok since later it will be checked
    // that whether it is a valid sst or not
    // (A directory "file" is not a valid sst)
    filenames.clear();
    filenames.push_back(dir_or_file);
    dir = false;
  }

  uint64_t total_read = 0;
  // List of RocksDB SST file without corruption
  std::vector<std::string> valid_sst_files;
  for (size_t i = 0; i < filenames.size(); i++) {
    std::string filename = filenames.at(i);
    if (filename.length() <= 4 ||
        filename.rfind(".sst") != filename.length() - 4) {
      // ignore
      continue;
    }

    if (dir) {
      filename = std::string(dir_or_file) + "/" + filename;
    }

    ROCKSDB_NAMESPACE::SstFileDumper dumper(options, filename, readahead_size,
                                            verify_checksum, output_hex,
                                            decode_blob_index);
    // Not a valid SST
    if (!dumper.getStatus().ok()) {
      fprintf(stderr, "%s: %s\n", filename.c_str(),
              dumper.getStatus().ToString().c_str());
      continue;
    } else {
      valid_sst_files.push_back(filename);
      // Print out from and to key information once
      // where there is at least one valid SST
      if (valid_sst_files.size() == 1) {
        // from_key and to_key are only used for "check", "scan", or ""
        if (command == "check" || command == "scan" || command == "") {
          fprintf(stdout, "from [%s] to [%s]\n",
                  ROCKSDB_NAMESPACE::Slice(from_key).ToString(true).c_str(),
                  ROCKSDB_NAMESPACE::Slice(to_key).ToString(true).c_str());
        }
      }
    }

    if (command == "recompress") {
      dumper.ShowAllCompressionSizes(
          set_block_size ? block_size : 16384,
          compression_types.empty() ? kCompressions : compression_types,
          compress_level_from, compress_level_to);
      return 0;
    }

    if (command == "raw") {
      std::string out_filename = filename.substr(0, filename.length() - 4);
      out_filename.append("_dump.txt");

      st = dumper.DumpTable(out_filename);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
        exit(1);
      } else {
        fprintf(stdout, "raw dump written to file %s\n", &out_filename[0]);
      }
      continue;
    }

    // scan all files in give file path.
    if (command == "" || command == "scan" || command == "check") {
      st = dumper.ReadSequential(
          command == "scan", read_num > 0 ? (read_num - total_read) : read_num,
          has_from || use_from_as_prefix, from_key, has_to, to_key,
          use_from_as_prefix);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(),
            st.ToString().c_str());
      }
      total_read += dumper.GetReadNumber();
      if (read_num > 0 && total_read > read_num) {
        break;
      }
    }

    if (command == "verify") {
      st = dumper.VerifyChecksum();
      if (!st.ok()) {
        fprintf(stderr, "%s is corrupted: %s\n", filename.c_str(),
                st.ToString().c_str());
      } else {
        fprintf(stdout, "The file is ok\n");
      }
      continue;
    }

    if (show_properties || show_summary) {
      const ROCKSDB_NAMESPACE::TableProperties* table_properties;

      std::shared_ptr<const ROCKSDB_NAMESPACE::TableProperties>
          table_properties_from_reader;
      st = dumper.ReadTableProperties(&table_properties_from_reader);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
        fprintf(stderr, "Try to use initial table properties\n");
        table_properties = dumper.GetInitTableProperties();
      } else {
        table_properties = table_properties_from_reader.get();
      }
      if (table_properties != nullptr) {
        if (show_properties) {
          fprintf(stdout,
                  "Table Properties:\n"
                  "------------------------------\n"
                  "  %s",
                  table_properties->ToString("\n  ", ": ").c_str());
        }
        total_num_files += 1;
        total_num_data_blocks += table_properties->num_data_blocks;
        total_data_block_size += table_properties->data_size;
        total_index_block_size += table_properties->index_size;
        total_filter_block_size += table_properties->filter_size;
        if (show_properties) {
          fprintf(stdout,
                  "Raw user collected properties\n"
                  "------------------------------\n");
          for (const auto& kv : table_properties->user_collected_properties) {
            std::string prop_name = kv.first;
            std::string prop_val = Slice(kv.second).ToString(true);
            fprintf(stdout, "  # %s: 0x%s\n", prop_name.c_str(),
                    prop_val.c_str());
          }
        }
      } else {
        fprintf(stderr, "Reader unexpectedly returned null properties\n");
      }
    }
  }
  if (show_summary) {
    fprintf(stdout, "total number of files: %" PRIu64 "\n", total_num_files);
    fprintf(stdout, "total number of data blocks: %" PRIu64 "\n",
            total_num_data_blocks);
    fprintf(stdout, "total data block size: %" PRIu64 "\n",
            total_data_block_size);
    fprintf(stdout, "total index block size: %" PRIu64 "\n",
            total_index_block_size);
    fprintf(stdout, "total filter block size: %" PRIu64 "\n",
            total_filter_block_size);
  }

  if (valid_sst_files.empty()) {
    // No valid SST files are found
    // Exit with an error state
    if (dir) {
      fprintf(stdout, "------------------------------\n");
      fprintf(stderr, "No valid SST files found in %s\n", dir_or_file);
    } else {
      fprintf(stderr, "%s is not a valid SST file\n", dir_or_file);
    }
    return 1;
  } else {
    if (command == "identify") {
      if (dir) {
        fprintf(stdout, "------------------------------\n");
        fprintf(stdout, "List of valid SST files found in %s:\n", dir_or_file);
        for (const auto& f : valid_sst_files) {
          fprintf(stdout, "%s\n", f.c_str());
        }
        fprintf(stdout, "Number of valid SST files: %zu\n",
                valid_sst_files.size());
      } else {
        fprintf(stdout, "%s is a valid SST file\n", dir_or_file);
      }
    }
    // At least one valid SST
    // exit with a success state
    return 0;
  }
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
