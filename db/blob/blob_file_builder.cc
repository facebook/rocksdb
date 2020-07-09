//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_builder.h"

#include <limits>

#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "db/dbformat.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

BlobFileBuilder::~BlobFileBuilder() = default;

Status BlobFileBuilder::Add(const Slice& key, const Slice& value,
                            Slice* blob_index) {
  assert(blob_index);
  assert(blob_index->empty());

  constexpr size_t min_blob_size = 128;  // TODO: config option

  if (value.size() < min_blob_size) {  // TODO: check value type?
    return Status::OK();
  }

  {
    const Status s = OpenBlobFileIfNeeded();
    if (!s.ok()) {
      return s;
    }
  }

  // TODO: compress if needed

  uint64_t blob_file_number = 0;
  uint64_t blob_offset = 0;

  {
    const Status s =
        WriteBlobToFile(key, value, &blob_file_number, &blob_offset);
    if (!s.ok()) {
      return s;
    }
  }

  {
    const Status s = CloseBlobFileIfNeeded();
    if (!s.ok()) {
      return s;
    }
  }

  BlobIndex::EncodeBlob(&blob_index_, blob_file_number, blob_offset,
                        /* FIXME */ value.size(), /* FIXME */ kNoCompression);
  *blob_index = blob_index_;

  return Status::OK();
}

Status BlobFileBuilder::Finish() {
  if (!IsBlobFileOpen()) {
    return Status::OK();
  }

  return CloseBlobFile();
}

bool BlobFileBuilder::IsBlobFileOpen() const { return !!writer_; }

Status BlobFileBuilder::OpenBlobFileIfNeeded() {
  if (IsBlobFileOpen()) {
    return Status::OK();
  }

  assert(versions_);
  assert(env_);
  assert(fs_);
  assert(immutable_cf_options_);
  assert(!immutable_cf_options_->cf_paths.empty());
  assert(file_options_);

  const uint64_t blob_file_number = versions_->NewFileNumber();
  const std::string blob_file_path = BlobFileName(
      immutable_cf_options_->cf_paths.front().path, blob_file_number);

  std::unique_ptr<FSWritableFile> file;

  {
    const Status s =
        NewWritableFile(fs_, blob_file_path, &file, *file_options_);
    if (!s.ok()) {
      return s;
    }

    // TODO: IO priority, lifetime hint?
  }

  Statistics* const statistics = immutable_cf_options_->statistics;

  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(file), blob_file_path, *file_options_,
                             env_, statistics, immutable_cf_options_->listeners,
                             immutable_cf_options_->file_checksum_gen_factory));

  std::unique_ptr<BlobLogWriter> blob_log_writer(new BlobLogWriter(
      std::move(file_writer), env_, statistics, blob_file_number,
      /* bytes_per_sync - TODO: remove */ 0, immutable_cf_options_->use_fsync));

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  BlobLogHeader header(column_family_id_, kNoCompression /* FIXME */, has_ttl,
                       expiration_range);

  {
    const Status s = blob_log_writer->WriteHeader(header);
    if (!s.ok()) {
      return s;
    }
  }

  writer_ = std::move(blob_log_writer);

  assert(IsBlobFileOpen());

  return Status::OK();
}

Status BlobFileBuilder::WriteBlobToFile(const Slice& key, const Slice& blob,
                                        uint64_t* blob_file_number,
                                        uint64_t* blob_offset) {
  assert(IsBlobFileOpen());
  assert(blob_file_number);
  assert(blob_offset);

  constexpr uint64_t expiration = std::numeric_limits<uint64_t>::max();
  uint64_t key_offset = 0;

  const Status s =
      writer_->AddRecord(key, blob, expiration, &key_offset, blob_offset);
  if (!s.ok()) {
    return s;
  }

  *blob_file_number = writer_->get_log_number();

  return Status::OK();
}

Status BlobFileBuilder::CloseBlobFile() {
  assert(IsBlobFileOpen());

  BlobLogFooter footer;

  // TODO: populate footer

  const Status s = writer_->AppendFooter(footer);
  if (!s.ok()) {
    return s;
  }

  writer_.reset();

  return Status::OK();
}

Status BlobFileBuilder::CloseBlobFileIfNeeded() {
  assert(IsBlobFileOpen());

  constexpr uint64_t file_size_limit = 1 << 30;
  if (writer_->file()->GetFileSize() < file_size_limit) {
    return Status::OK();
  }

  return CloseBlobFile();
}

}  // namespace ROCKSDB_NAMESPACE
