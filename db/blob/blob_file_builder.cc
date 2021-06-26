//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_builder.h"

#include <cassert>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_completion_callback.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "options/cf_options.h"
#include "options/options_helper.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "trace_replay/io_tracer.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

BlobFileBuilder::BlobFileBuilder(
    VersionSet* versions, FileSystem* fs,
    const ImmutableOptions* immutable_options,
    const MutableCFOptions* mutable_cf_options, const FileOptions* file_options,
    int job_id, uint32_t column_family_id,
    const std::string& column_family_name, Env::IOPriority io_priority,
    Env::WriteLifeTimeHint write_hint,
    const std::shared_ptr<IOTracer>& io_tracer,
    BlobFileCompletionCallback* blob_callback,
    std::vector<std::string>* blob_file_paths,
    std::vector<BlobFileAddition>* blob_file_additions)
    : BlobFileBuilder([versions]() { return versions->NewFileNumber(); }, fs,
                      immutable_options, mutable_cf_options, file_options,
                      job_id, column_family_id, column_family_name, io_priority,
                      write_hint, io_tracer, blob_callback, blob_file_paths,
                      blob_file_additions) {}

BlobFileBuilder::BlobFileBuilder(
    std::function<uint64_t()> file_number_generator, FileSystem* fs,
    const ImmutableOptions* immutable_options,
    const MutableCFOptions* mutable_cf_options, const FileOptions* file_options,
    int job_id, uint32_t column_family_id,
    const std::string& column_family_name, Env::IOPriority io_priority,
    Env::WriteLifeTimeHint write_hint,
    const std::shared_ptr<IOTracer>& io_tracer,
    BlobFileCompletionCallback* blob_callback,
    std::vector<std::string>* blob_file_paths,
    std::vector<BlobFileAddition>* blob_file_additions)
    : file_number_generator_(std::move(file_number_generator)),
      fs_(fs),
      immutable_options_(immutable_options),
      min_blob_size_(mutable_cf_options->min_blob_size),
      blob_file_size_(mutable_cf_options->blob_file_size),
      blob_compression_type_(mutable_cf_options->blob_compression_type),
      file_options_(file_options),
      job_id_(job_id),
      column_family_id_(column_family_id),
      column_family_name_(column_family_name),
      io_priority_(io_priority),
      write_hint_(write_hint),
      io_tracer_(io_tracer),
      blob_callback_(blob_callback),
      blob_file_paths_(blob_file_paths),
      blob_file_additions_(blob_file_additions),
      blob_count_(0),
      blob_bytes_(0) {
  assert(file_number_generator_);
  assert(fs_);
  assert(immutable_options_);
  assert(file_options_);
  assert(blob_file_paths_);
  assert(blob_file_paths_->empty());
  assert(blob_file_additions_);
  assert(blob_file_additions_->empty());
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

  assert(immutable_options_);
  assert(!immutable_options_->cf_paths.empty());
  std::string blob_file_path =
      BlobFileName(immutable_options_->cf_paths.front().path, blob_file_number);

  std::unique_ptr<FSWritableFile> file;

  {
    assert(file_options_);
    Status s = NewWritableFile(fs_, blob_file_path, &file, *file_options_);

    TEST_SYNC_POINT_CALLBACK(
        "BlobFileBuilder::OpenBlobFileIfNeeded:NewWritableFile", &s);

    if (!s.ok()) {
      return s;
    }
  }

  // Note: files get added to blob_file_paths_ right after the open, so they
  // can be cleaned up upon failure. Contrast this with blob_file_additions_,
  // which only contains successfully written files.
  assert(blob_file_paths_);
  blob_file_paths_->emplace_back(std::move(blob_file_path));

  assert(file);
  file->SetIOPriority(io_priority_);
  file->SetWriteLifeTimeHint(write_hint_);
  FileTypeSet tmp_set = immutable_options_->checksum_handoff_file_types;
  Statistics* const statistics = immutable_options_->stats;
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), blob_file_paths_->back(), *file_options_,
      immutable_options_->clock, io_tracer_, statistics,
      immutable_options_->listeners,
      immutable_options_->file_checksum_gen_factory.get(),
      tmp_set.Contains(FileType::kBlobFile), false));

  constexpr bool do_flush = false;

  std::unique_ptr<BlobLogWriter> blob_log_writer(new BlobLogWriter(
      std::move(file_writer), immutable_options_->clock, statistics,
      blob_file_number, immutable_options_->use_fsync, do_flush));

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  BlobLogHeader header(column_family_id_, blob_compression_type_, has_ttl,
                       expiration_range);

  {
    Status s = blob_log_writer->WriteHeader(header);

    TEST_SYNC_POINT_CALLBACK(
        "BlobFileBuilder::OpenBlobFileIfNeeded:WriteHeader", &s);

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

  Status s = writer_->AddRecord(key, blob, &key_offset, blob_offset);

  TEST_SYNC_POINT_CALLBACK("BlobFileBuilder::WriteBlobToFile:AddRecord", &s);

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

  Status s = writer_->AppendFooter(footer, &checksum_method, &checksum_value);

  TEST_SYNC_POINT_CALLBACK("BlobFileBuilder::WriteBlobToFile:AppendFooter", &s);

  if (!s.ok()) {
    return s;
  }

  const uint64_t blob_file_number = writer_->get_log_number();

  assert(blob_file_additions_);
  blob_file_additions_->emplace_back(blob_file_number, blob_count_, blob_bytes_,
                                     std::move(checksum_method),
                                     std::move(checksum_value));

  assert(immutable_options_);
  ROCKS_LOG_INFO(immutable_options_->logger,
                 "[%s] [JOB %d] Generated blob file #%" PRIu64 ": %" PRIu64
                 " total blobs, %" PRIu64 " total bytes",
                 column_family_name_.c_str(), job_id_, blob_file_number,
                 blob_count_, blob_bytes_);
  if (blob_callback_) {
    s = blob_callback_->OnBlobFileCompleted(blob_file_paths_->back());
  }

  writer_.reset();
  blob_count_ = 0;
  blob_bytes_ = 0;

  return s;
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

void BlobFileBuilder::Abandon() {
  if (!IsBlobFileOpen()) {
    return;
  }

  if (blob_callback_) {
    // BlobFileBuilder::Abandon() is called because of error while writing to
    // Blob files. So we can ignore the below error.
    blob_callback_->OnBlobFileCompleted(blob_file_paths_->back())
        .PermitUncheckedError();
  }

  writer_.reset();
  blob_count_ = 0;
  blob_bytes_ = 0;
}
}  // namespace ROCKSDB_NAMESPACE
