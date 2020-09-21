//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_reader.h"

#include <cassert>
#include <string>

#include "db/blob/blob_log_format.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "options/cf_options.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/get_context.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

Status BlobFileReader::Create(
    const ReadOptions& read_options,
    const ImmutableCFOptions& immutable_cf_options,
    const FileOptions& file_options, uint64_t blob_file_number,
    std::unique_ptr<BlobFileReader>* blob_file_reader) {
  assert(blob_file_reader);
  assert(!*blob_file_reader);

  Env* const env = immutable_cf_options.env;
  assert(env);

  FileOptions file_opts(file_options);

  {
    const Status s =
        PrepareIOFromReadOptions(read_options, env, file_opts.io_options);
    if (!s.ok()) {
      return s;
    }
  }

  FileSystem* const fs = immutable_cf_options.fs;
  assert(fs);

  const auto& cf_paths = immutable_cf_options.cf_paths;
  assert(!cf_paths.empty());

  const std::string blob_file_path =
      BlobFileName(cf_paths.front().path, blob_file_number);
  std::unique_ptr<FSRandomAccessFile> file;
  constexpr IODebugContext* dbg = nullptr;

  {
    const Status s =
        fs->NewRandomAccessFile(blob_file_path, file_opts, &file, dbg);
    if (!s.ok()) {
      return s;
    }
  }

  assert(file);

  if (immutable_cf_options.advise_random_on_open) {
    file->Hint(FSRandomAccessFile::kRandom);
  }

  // TODO
  constexpr IOTracer* io_tracer = nullptr;
  constexpr HistogramImpl* file_read_hist = nullptr;

  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(
          std::move(file), blob_file_path, env,
          std::shared_ptr<IOTracer>(io_tracer), immutable_cf_options.statistics,
          BLOB_DB_BLOB_FILE_READ_MICROS, file_read_hist,
          immutable_cf_options.rate_limiter, immutable_cf_options.listeners));

  // TODO: read header and retrieve compression etc.

  blob_file_reader->reset(new BlobFileReader(std::move(file_reader)));

  return Status::OK();
}

BlobFileReader::BlobFileReader(
    std::unique_ptr<RandomAccessFileReader>&& file_reader)
    : file_reader_(std::move(file_reader)) {
  assert(file_reader_);
}

BlobFileReader::~BlobFileReader() = default;

Status BlobFileReader::GetBlob(const ReadOptions& read_options,
                               const Slice& user_key, uint64_t offset,
                               uint64_t value_size,
                               GetContext* get_context) const {
  assert(get_context);

  const size_t key_size = user_key.size();

  if (!IsValidBlobOffset(offset, key_size)) {
    return Status::Corruption("Invalid blob offset");
  }

  // Note: if verify_checksum is set, we read the entire blob record to be able
  // to perform the verification; otherwise, we just read the blob itself. Since
  // the offset in BlobIndex actually points to the blob value, we need to make
  // an adjustment in the former case.
  const size_t adjustment =
      read_options.verify_checksums
          ? BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size)
          : 0;
  assert(offset >= adjustment);
  const uint64_t record_offset = offset - adjustment;
  const uint64_t record_size = value_size + adjustment;

  std::string buf;
  AlignedBuf aligned_buf;

  Slice record_slice;

  assert(file_reader_);

  if (file_reader_->use_direct_io()) {
    const Status s = file_reader_->Read(IOOptions(), record_offset,
                                        static_cast<size_t>(record_size),
                                        &record_slice, nullptr, &aligned_buf);
    if (!s.ok()) {
      return s;
    }
  } else {
    buf.reserve(static_cast<size_t>(record_size));
    const Status s = file_reader_->Read(IOOptions(), record_offset,
                                        static_cast<size_t>(record_size),
                                        &record_slice, &buf[0], nullptr);
    if (!s.ok()) {
      return s;
    }
  }

  if (record_slice.size() != record_size) {
    return Status::Corruption("Failed to retrieve blob record");
  }

  if (read_options.verify_checksums) {
    BlobLogRecord record;

    Slice header_slice(record_slice.data(), BlobLogRecord::kHeaderSize);

    {
      const Status s = record.DecodeHeaderFrom(header_slice);
      if (!s.ok()) {
        return s;
      }
    }

    if (record.key_size != key_size) {
      return Status::Corruption("Key size mismatch when reading blob");
    }

    if (record.value_size != value_size) {
      return Status::Corruption("Value size mismatch when reading blob");
    }

    record.key = Slice(record_slice.data() + BlobLogRecord::kHeaderSize,
                       record.key_size);
    if (record.key != user_key) {
      return Status::Corruption("Key mismatch when reading blob");
    }

    record.value = Slice(record_slice.data() + adjustment, value_size);

    {
      const Status s = record.CheckBlobCRC();
      if (!s.ok()) {
        return s;
      }
    }

    get_context->SaveValue(record.value, kMaxSequenceNumber);
  } else {
    assert(!adjustment);
    get_context->SaveValue(record_slice, kMaxSequenceNumber);
  }

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
