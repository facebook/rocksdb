//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/sst_file_reader.h"

#include "db/db_iter.h"
#include "db/dbformat.h"
#include "env/composite_env_wrapper.h"
#include "file/random_access_file_reader.h"
#include "options/cf_options.h"
#include "table/get_context.h"
#include "table/table_builder.h"
#include "table/table_reader.h"

namespace rocksdb {

struct SstFileReader::Rep {
  Options options;
  EnvOptions soptions;
  ImmutableCFOptions ioptions;
  MutableCFOptions moptions;

  std::unique_ptr<TableReader> table_reader;

  Rep(const Options& opts)
      : options(opts),
        soptions(options),
        ioptions(options),
        moptions(ColumnFamilyOptions(options)) {}
};

SstFileReader::SstFileReader(const Options& options) : rep_(new Rep(options)) {}

SstFileReader::~SstFileReader() {}

Status SstFileReader::Open(const std::string& file_path) {
  auto r = rep_.get();
  Status s;
  uint64_t file_size = 0;
  std::unique_ptr<RandomAccessFile> file;
  std::unique_ptr<RandomAccessFileReader> file_reader;
  s = r->options.env->GetFileSize(file_path, &file_size);
  if (s.ok()) {
    s = r->options.env->NewRandomAccessFile(file_path, &file, r->soptions);
  }
  if (s.ok()) {
    file_reader.reset(new RandomAccessFileReader(
        NewLegacyRandomAccessFileWrapper(file), file_path));
  }
  if (s.ok()) {
    TableReaderOptions t_opt(r->ioptions, r->moptions.prefix_extractor.get(),
                             r->soptions, r->ioptions.internal_comparator);
    // Allow open file with global sequence number for backward compatibility.
    t_opt.largest_seqno = kMaxSequenceNumber;
    s = r->options.table_factory->NewTableReader(t_opt, std::move(file_reader),
                                                 file_size, &r->table_reader);
  }
  return s;
}

Iterator* SstFileReader::NewIterator(const ReadOptions& options) {
  auto r = rep_.get();
  auto sequence = options.snapshot != nullptr
                      ? options.snapshot->GetSequenceNumber()
                      : kMaxSequenceNumber;
  auto internal_iter = r->table_reader->NewIterator(
      options, r->moptions.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kSSTFileReader);
  return NewDBIterator(r->options.env, options, r->ioptions, r->moptions,
                       r->ioptions.user_comparator, internal_iter, sequence,
                       r->moptions.max_sequential_skip_in_iterations,
                       nullptr /* read_callback */);
}

std::shared_ptr<const TableProperties> SstFileReader::GetTableProperties()
    const {
  return rep_->table_reader->GetTableProperties();
}

Status SstFileReader::VerifyChecksum(const ReadOptions& read_options) {
  return rep_->table_reader->VerifyChecksum(read_options,
                                            TableReaderCaller::kSSTFileReader);
}

}  // namespace rocksdb

#endif  // !ROCKSDB_LITE
