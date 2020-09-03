//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_builder.h"

#include <cassert>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "options/cf_options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

BlobFileBuilder::BlobFileBuilder(
    VersionSet* versions, Env* env, FileSystem* fs,
    const ImmutableCFOptions* immutable_cf_options,
    const MutableCFOptions* mutable_cf_options, const FileOptions* file_options,
    int job_id, uint32_t column_family_id,
    const std::string& column_family_name, Env::IOPriority io_priority,
    Env::WriteLifeTimeHint write_hint,
    std::vector<BlobFileAddition>* blob_file_additions)
    : BlobFileBuilder([versions]() { return versions->NewFileNumber(); }, env,
                      fs, immutable_cf_options, mutable_cf_options,
                      file_options, job_id, column_family_id,
                      column_family_name, io_priority, write_hint,
                      blob_file_additions) {}

BlobFileBuilder::BlobFileBuilder(
    std::function<uint64_t()> file_number_generator, Env* env, FileSystem* fs,
    const ImmutableCFOptions* immutable_cf_options,
    const MutableCFOptions* mutable_cf_options, const FileOptions* file_options,
    int job_id, uint32_t column_family_id,
    const std::string& column_family_name, Env::IOPriority io_priority,
    Env::WriteLifeTimeHint write_hint,
    std::vector<BlobFileAddition>* blob_file_additions)
    : file_number_generator_(std::move(file_number_generator)),
      env_(env),
      fs_(fs),
      immutable_cf_options_(immutable_cf_options),
      min_blob_size_(mutable_cf_options->min_blob_size),
      blob_file_size_(mutable_cf_options->blob_file_size),
      blob_compression_type_(mutable_cf_options->blob_compression_type),
      file_options_(file_options),
      job_id_(job_id),
      column_family_id_(column_family_id),
      column_family_name_(column_family_name),
      io_priority_(io_priority),
      write_hint_(write_hint),
      blob_file_additions_(blob_file_additions),
      blob_count_(0),
      blob_bytes_(0) {
  assert(file_number_generator_);
  assert(env_);
  assert(fs_);
  assert(immutable_cf_options_);
  assert(file_options_);
  assert(blob_file_additions_);
}

BlobFileBuilder::~BlobFileBuilder() = default;

Status BlobFileBuilder::Add(const Slice& key, const Slice& value,
                            std::string* blob_index) {
  assert(blob_index);
  assert(blob_index->empty());

  if (value.size() < min_blob_size_) {
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
    const Status s = CompressBlobIfNeeded(&blob, &compressed_blob);
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

  BlobIndex::EncodeBlob(blob_index, blob_file_number, blob_offset, blob.size(),
                        blob_compression_type_);

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

  assert(!blob_count_);
  assert(!blob_bytes_);

  assert(file_number_generator_);
  const uint64_t blob_file_number = file_number_generator_();

  assert(immutable_cf_options_);
  assert(!immutable_cf_options_->cf_paths.empty());
  const std::string blob_file_path = BlobFileName(
      immutable_cf_options_->cf_paths.front().path, blob_file_number);

  std::unique_ptr<FSWritableFile> file;

  {
    TEST_SYNC_POINT("BlobFileBuilder::OpenBlobFileIfNeeded:NewWritableFile");

    assert(file_options_);
    const Status s =
        NewWritableFile(fs_, blob_file_path, &file, *file_options_);
    if (!s.ok()) {
      return s;
    }
  }

  assert(file);
  file->SetIOPriority(io_priority_);
  file->SetWriteLifeTimeHint(write_hint_);

  Statistics* const statistics = immutable_cf_options_->statistics;

  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(file), blob_file_path, *file_options_,
                             env_, statistics, immutable_cf_options_->listeners,
                             immutable_cf_options_->file_checksum_gen_factory));

  std::unique_ptr<BlobLogWriter> blob_log_writer(
      new BlobLogWriter(std::move(file_writer), env_, statistics,
                        blob_file_number, immutable_cf_options_->use_fsync));

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  BlobLogHeader header(column_family_id_, blob_compression_type_, has_ttl,
                       expiration_range);

  {
    TEST_SYNC_POINT("BlobFileBuilder::OpenBlobFileIfNeeded:WriteHeader");

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
    Slice* blob, std::string* compressed_blob) const {
  assert(blob);
  assert(compressed_blob);
  assert(compressed_blob->empty());

  if (blob_compression_type_ == kNoCompression) {
    return Status::OK();
  }

  CompressionOptions opts;
  CompressionContext context(blob_compression_type_);
  constexpr uint64_t sample_for_compression = 0;

  CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(),
                       blob_compression_type_, sample_for_compression);

  constexpr uint32_t compression_format_version = 2;

  if (!CompressData(*blob, info, compression_format_version, compressed_blob)) {
    return Status::Corruption("Error compressing blob");
  }

  *blob = Slice(*compressed_blob);

  return Status::OK();
}

Status BlobFileBuilder::WriteBlobToFile(const Slice& key, const Slice& blob,
                                        uint64_t* blob_file_number,
                                        uint64_t* blob_offset) {
  assert(IsBlobFileOpen());
  assert(blob_file_number);
  assert(blob_offset);

  uint64_t key_offset = 0;

  TEST_SYNC_POINT("BlobFileBuilder::WriteBlobToFile:AddRecord");

  const Status s = writer_->AddRecord(key, blob, &key_offset, blob_offset);
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

  std::string checksum_method;
  std::string checksum_value;

  TEST_SYNC_POINT("BlobFileBuilder::WriteBlobToFile:AppendFooter");

  const Status s =
      writer_->AppendFooter(footer, &checksum_method, &checksum_value);
  if (!s.ok()) {
    return s;
  }

  const uint64_t blob_file_number = writer_->get_log_number();

  assert(blob_file_additions_);
  blob_file_additions_->emplace_back(blob_file_number, blob_count_, blob_bytes_,
                                     std::move(checksum_method),
                                     std::move(checksum_value));

  assert(immutable_cf_options_);
  ROCKS_LOG_INFO(immutable_cf_options_->info_log,
                 "[%s] [JOB %d] Generated blob file #%" PRIu64 ": %" PRIu64
                 " total blobs, %" PRIu64 " total bytes",
                 column_family_name_.c_str(), job_id_, blob_file_number,
                 blob_count_, blob_bytes_);

  writer_.reset();
  blob_count_ = 0;
  blob_bytes_ = 0;

  return Status::OK();
}

Status BlobFileBuilder::CloseBlobFileIfNeeded() {
  assert(IsBlobFileOpen());

  const WritableFileWriter* const file_writer = writer_->file();
  assert(file_writer);

  if (file_writer->GetFileSize() < blob_file_size_) {
    return Status::OK();
  }

  return CloseBlobFile();
}

}  // namespace ROCKSDB_NAMESPACE
