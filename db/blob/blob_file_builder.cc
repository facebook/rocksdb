//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_builder.h"

#include <limits>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/dbformat.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/block_based/block_based_table_builder.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

Status BlobFileBuilder::Add(const Slice& key, const Slice& value,
                            Slice* blob_index) {
  assert(blob_index);
  assert(blob_index->empty());

  blob_index_.clear();

  assert(immutable_cf_options_);
  if (value.size() < immutable_cf_options_->min_blob_size) {
    return Status::OK();
  }

  {
    const Status s = OpenBlobFileIfNeeded();
    if (!s.ok()) {
      return s;
    }
  }

  Slice blob = value;
  std::string compressed_blob;

  {
    const Status s = CompressBlobIfNeeded(
        &blob, immutable_cf_options_->blob_compression_type, &compressed_blob);
    if (!s.ok()) {
      return s;
    }
  }

  uint64_t blob_file_number = 0;
  uint64_t blob_offset = 0;

  {
    const Status s =
        WriteBlobToFile(key, blob, &blob_file_number, &blob_offset);
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
                        blob.size(),
                        immutable_cf_options_->blob_compression_type);
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
  assert(!blob_count_);
  assert(!blob_bytes_);

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
      immutable_cf_options_->use_fsync));

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  assert(immutable_cf_options_);
  BlobLogHeader header(column_family_id_,
                       immutable_cf_options_->blob_compression_type, has_ttl,
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

Status BlobFileBuilder::CompressBlobIfNeeded(
    Slice* blob, CompressionType compression_type,
    std::string* compressed_blob) const {
  assert(blob);
  assert(compressed_blob);

  if (compression_type == kNoCompression) {
    return Status::OK();
  }

  CompressionOptions opts;
  CompressionContext context(compression_type);
  constexpr uint64_t sample_for_compression = 0;

  CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(),
                       compression_type, sample_for_compression);

  // TODO: move out from block_based_table_builder and refactor
  assert(immutable_cf_options_);
  CompressionType type = immutable_cf_options_->blob_compression_type;
  CompressBlock(*blob, info, &type, 2, false, compressed_blob, nullptr,
                nullptr);

  *blob = Slice(*compressed_blob);

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

  ++blob_count_;
  blob_bytes_ += BlobLogRecord::kHeaderSize + key.size() + blob.size();

  return Status::OK();
}

Status BlobFileBuilder::CloseBlobFile() {
  assert(IsBlobFileOpen());

  BlobLogFooter footer;
  footer.blob_count = blob_count_;

  const Status s = writer_->AppendFooter(footer);
  if (!s.ok()) {
    return s;
  }

  const uint64_t blob_file_number = writer_->get_log_number();

  assert(blob_file_additions_);
  blob_file_additions_->emplace_back(blob_file_number, blob_count_, blob_bytes_,
                                     /* TODO: checksum_method */ std::string(),
                                     /* TODO: checksum_value */ std::string());

  writer_.reset();
  blob_count_ = 0;
  blob_bytes_ = 0;

  return Status::OK();
}

Status BlobFileBuilder::CloseBlobFileIfNeeded() {
  assert(IsBlobFileOpen());

  assert(immutable_cf_options_);
  if (writer_->file()->GetFileSize() < immutable_cf_options_->blob_file_size) {
    return Status::OK();
  }

  return CloseBlobFile();
}

}  // namespace ROCKSDB_NAMESPACE
