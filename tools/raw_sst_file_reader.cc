//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "rocksdb/raw_sst_file_reader.h"

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
#include "tools/raw_sst_file_reader_iterator.h"
#include "util/compression.h"
#include "util/random.h"
#include "db/dbformat.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"

namespace ROCKSDB_NAMESPACE {

struct RawSstFileReader::Rep {
  Options options;
  EnvOptions soptions_;
  ReadOptions read_options_;
  ImmutableOptions ioptions_;
  MutableCFOptions moptions_;
  InternalKeyComparator internal_comparator_;
  std::unique_ptr<TableProperties> table_properties_;
  std::unique_ptr<TableReader> table_reader_;
  std::unique_ptr<RandomAccessFileReader> file_;

  Rep(const Options& opts, bool verify_checksum, size_t readahead_size)
      : options(opts),
        soptions_(EnvOptions()),
        read_options_(verify_checksum, false),
        ioptions_(options),
        moptions_(ColumnFamilyOptions(options)),
        internal_comparator_(InternalKeyComparator(BytewiseComparator())) {
    read_options_.readahead_size = readahead_size;
  }
};

RawSstFileReader::RawSstFileReader(const Options& options,
                                   const std::string& file_name,
                                   size_t readahead_size,
                                   bool verify_checksum,
                                   bool silent) :rep_(new Rep(options,
                                                               verify_checksum,
                                                               readahead_size)) {
  file_name_ = file_name;
  silent_ = silent;
  options_ = options;
  file_temp_ = Temperature::kUnknown;
  init_result_ = GetTableReader(file_name_);
}

RawSstFileReader::~RawSstFileReader() {}



extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;

Status RawSstFileReader::GetTableReader(const std::string& file_path) {
  // Warning about 'magic_number' being uninitialized shows up only in UBsan
  // builds. Though access is guarded by 's.ok()' checks, fix the issue to
  // avoid any warnings.
  uint64_t magic_number = Footer::kNullTableMagicNumber;

  // read table magic number
  Footer footer;

  const auto& fs = options_.env->GetFileSystem();
  std::unique_ptr<FSRandomAccessFile> file;
  uint64_t file_size = 0;
  FileOptions fopts = rep_->soptions_;
  fopts.temperature = file_temp_;
  Status s = fs->NewRandomAccessFile(file_path, fopts, &file, nullptr);
  if (s.ok()) {
    s = fs->GetFileSize(file_path, IOOptions(), &file_size, nullptr);
  }

  // check empty file
  // if true, skip further processing of this file
  if (file_size == 0) {
    return Status::Aborted(file_path, "Empty file");
  }

  rep_->file_.reset(new RandomAccessFileReader(std::move(file), file_path));

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
    s = prefetch_buffer.Prefetch(opts, rep_->file_.get(), prefetch_off,
                                 static_cast<size_t>(prefetch_size),
                                 Env::IO_TOTAL /* rate_limiter_priority */);

    s = ReadFooterFromFile(opts, rep_->file_.get(), &prefetch_buffer, file_size,
                           &footer);
  }
  if (s.ok()) {
    magic_number = footer.table_magic_number();
  }

  if (s.ok()) {
    if (magic_number == kPlainTableMagicNumber ||
        magic_number == kLegacyPlainTableMagicNumber) {
      rep_->soptions_.use_mmap_reads = true;

      fs->NewRandomAccessFile(file_path, fopts, &file, nullptr);
      rep_->file_.reset(new RandomAccessFileReader(std::move(file), file_path));
    }

    s = ROCKSDB_NAMESPACE::ReadTableProperties(
        rep_->file_.get(), file_size, magic_number, rep_->ioptions_, &(rep_->table_properties_),
        /* memory_allocator= */ nullptr, (magic_number == kBlockBasedTableMagicNumber)
                                             ? &prefetch_buffer
                                             : nullptr);
    if (!s.ok()) {
      if (!silent_) {
        fprintf(stderr, "Not able to read table properties\n");
      }
    }
    // For old sst format, ReadTableProperties might fail but file can be read
    if (s.ok()) {
      s = SetTableOptionsByMagicNumber(magic_number);
      if (s.ok()) {
        if (rep_->table_properties_ && !rep_->table_properties_->comparator_name.empty()) {
          ConfigOptions config_options;
          const Comparator* user_comparator = nullptr;
          s = Comparator::CreateFromString(config_options,
                                           rep_->table_properties_->comparator_name,
                                           &user_comparator);
          if (s.ok()) {
            assert(user_comparator);
            rep_->internal_comparator_ = InternalKeyComparator(user_comparator);
          }
        }
      }
    } else {
      s = SetOldTableOptions();
    }
    options_.comparator = rep_->internal_comparator_.user_comparator();
  }

  if (s.ok()) {
    s = NewTableReader(file_size);
  }
  return s;
}

Status RawSstFileReader::NewTableReader(uint64_t file_size) {
  auto t_opt =
      TableReaderOptions(rep_->ioptions_, rep_->moptions_.prefix_extractor, rep_->soptions_,
                         rep_->internal_comparator_, false /* skip_filters */,
                         false /* imortal */, true /* force_direct_prefetch */);
  // Allow open file with global sequence number for backward compatibility.
  t_opt.largest_seqno = kMaxSequenceNumber;

  // We need to turn off pre-fetching of index and filter nodes for
  // BlockBasedTable
  if (options_.table_factory->IsInstanceOf(
          TableFactory::kBlockBasedTableName())) {
    return options_.table_factory->NewTableReader(t_opt, std::move(rep_->file_),
                                                  file_size, &(rep_->table_reader_),
                                                  /*enable_prefetch=*/false);
  }

  // For all other factory implementation
  return options_.table_factory->NewTableReader(t_opt, std::move(rep_->file_),
                                                file_size, &(rep_->table_reader_));
}

Status RawSstFileReader::SetTableOptionsByMagicNumber(
    uint64_t table_magic_number) {
  assert(rep_->table_properties_);
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

    auto& props = rep_->table_properties_->user_collected_properties;
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

Status RawSstFileReader::SetOldTableOptions() {
  assert(rep_->table_properties_ == nullptr);
  options_.table_factory = std::make_shared<BlockBasedTableFactory>();
  if (!silent_) {
    fprintf(stdout, "Sst file format: block-based(old version)\n");
  }

  return Status::OK();
}

RawIterator* RawSstFileReader::newIterator(
    bool has_from, Slice* from, bool has_to, Slice* to) {
  InternalIterator* iter = rep_->table_reader_->NewIterator(
      rep_->read_options_, rep_->moptions_.prefix_extractor.get(),
      /*arena=*/nullptr, /*skip_filters=*/false,
      TableReaderCaller::kSSTDumpTool);
  return new RawSstFileReaderIterator(iter, has_from, from, has_to, to);

}
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
