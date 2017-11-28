//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE
#include "utilities/blob_db/blob_file.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdio.h>

#include <algorithm>
#include <memory>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "util/filename.h"
#include "util/logging.h"
#include "utilities/blob_db/blob_db_impl.h"

namespace rocksdb {

namespace blob_db {

BlobFile::BlobFile()
    : parent_(nullptr),
      file_number_(0),
      has_ttl_(false),
      compression_(kNoCompression),
      blob_count_(0),
      gc_epoch_(-1),
      file_size_(0),
      deleted_count_(0),
      deleted_size_(0),
      closed_(false),
      obsolete_(false),
      gc_once_after_open_(false),
      expiration_range_({0, 0}),
      sequence_range_({kMaxSequenceNumber, 0}),
      last_access_(-1),
      last_fsync_(0),
      header_valid_(false) {}

BlobFile::BlobFile(const BlobDBImpl* p, const std::string& bdir, uint64_t fn)
    : parent_(p),
      path_to_dir_(bdir),
      file_number_(fn),
      has_ttl_(false),
      compression_(kNoCompression),
      blob_count_(0),
      gc_epoch_(-1),
      file_size_(0),
      deleted_count_(0),
      deleted_size_(0),
      closed_(false),
      obsolete_(false),
      gc_once_after_open_(false),
      expiration_range_({0, 0}),
      sequence_range_({kMaxSequenceNumber, 0}),
      last_access_(-1),
      last_fsync_(0),
      header_valid_(false) {}

BlobFile::~BlobFile() {
  if (obsolete_) {
    std::string pn(PathName());
    Status s = Env::Default()->DeleteFile(PathName());
    if (!s.ok()) {
      // ROCKS_LOG_INFO(db_options_.info_log,
      // "File could not be deleted %s", pn.c_str());
    }
  }
}

uint32_t BlobFile::column_family_id() const {
  // TODO(yiwu): Should return column family id encoded in blob file after
  // we add blob db column family support.
  return reinterpret_cast<ColumnFamilyHandle*>(parent_->DefaultColumnFamily())
      ->GetID();
}

std::string BlobFile::PathName() const {
  return BlobFileName(path_to_dir_, file_number_);
}

std::shared_ptr<Reader> BlobFile::OpenSequentialReader(
    Env* env, const DBOptions& db_options,
    const EnvOptions& env_options) const {
  std::unique_ptr<SequentialFile> sfile;
  Status s = env->NewSequentialFile(PathName(), &sfile, env_options);
  if (!s.ok()) {
    // report something here.
    return nullptr;
  }

  std::unique_ptr<SequentialFileReader> sfile_reader;
  sfile_reader.reset(new SequentialFileReader(std::move(sfile)));

  std::shared_ptr<Reader> log_reader = std::make_shared<Reader>(
      std::move(sfile_reader), db_options.env, db_options.statistics.get());

  return log_reader;
}

std::string BlobFile::DumpState() const {
  char str[1000];
  snprintf(str, sizeof(str),
           "path: %s fn: %" PRIu64 " blob_count: %" PRIu64 " gc_epoch: %" PRIu64
           " file_size: %" PRIu64 " deleted_count: %" PRIu64
           " deleted_size: %" PRIu64
           " closed: %d obsolete: %d expiration_range: (%" PRIu64 ", %" PRIu64
           ") sequence_range: (%" PRIu64 " %" PRIu64 "), writer: %d reader: %d",
           path_to_dir_.c_str(), file_number_, blob_count_.load(),
           gc_epoch_.load(), file_size_.load(), deleted_count_, deleted_size_,
           closed_.load(), obsolete_.load(), expiration_range_.first,
           expiration_range_.second, sequence_range_.first,
           sequence_range_.second, (!!log_writer_), (!!ra_file_reader_));
  return str;
}

void BlobFile::MarkObsolete(SequenceNumber sequence) {
  obsolete_sequence_ = sequence;
  obsolete_.store(true);
}

bool BlobFile::NeedsFsync(bool hard, uint64_t bytes_per_sync) const {
  assert(last_fsync_ <= file_size_);
  return (hard) ? file_size_ > last_fsync_
                : (file_size_ - last_fsync_) >= bytes_per_sync;
}

Status BlobFile::WriteFooterAndCloseLocked() {
  BlobLogFooter footer;
  footer.blob_count = blob_count_;
  if (HasTTL()) {
    footer.expiration_range = expiration_range_;
  }

  footer.sequence_range = sequence_range_;

  // this will close the file and reset the Writable File Pointer.
  Status s = log_writer_->AppendFooter(footer);
  if (s.ok()) {
    closed_ = true;
    file_size_ += BlobLogFooter::kSize;
  }
  // delete the sequential writer
  log_writer_.reset();
  return s;
}

Status BlobFile::ReadFooter(BlobLogFooter* bf) {
  if (file_size_ < (BlobLogHeader::kSize + BlobLogFooter::kSize)) {
    return Status::IOError("File does not have footer", PathName());
  }

  uint64_t footer_offset = file_size_ - BlobLogFooter::kSize;
  // assume that ra_file_reader_ is valid before we enter this
  assert(ra_file_reader_);

  Slice result;
  char scratch[BlobLogFooter::kSize + 10];
  Status s = ra_file_reader_->Read(footer_offset, BlobLogFooter::kSize, &result,
                                   scratch);
  if (!s.ok()) return s;
  if (result.size() != BlobLogFooter::kSize) {
    // should not happen
    return Status::IOError("EOF reached before footer");
  }

  s = bf->DecodeFrom(result);
  return s;
}

Status BlobFile::SetFromFooterLocked(const BlobLogFooter& footer) {
  // assume that file has been fully fsync'd
  last_fsync_.store(file_size_);
  blob_count_ = footer.blob_count;
  expiration_range_ = footer.expiration_range;
  sequence_range_ = footer.sequence_range;
  closed_ = true;
  return Status::OK();
}

void BlobFile::Fsync() {
  if (log_writer_.get()) {
    log_writer_->Sync();
    last_fsync_.store(file_size_.load());
  }
}

void BlobFile::CloseRandomAccessLocked() {
  ra_file_reader_.reset();
  last_access_ = -1;
}

std::shared_ptr<RandomAccessFileReader> BlobFile::GetOrOpenRandomAccessReader(
    Env* env, const EnvOptions& env_options, bool* fresh_open) {
  *fresh_open = false;
  int64_t current_time = 0;
  env->GetCurrentTime(&current_time);
  last_access_.store(current_time);

  {
    ReadLock lockbfile_r(&mutex_);
    if (ra_file_reader_) return ra_file_reader_;
  }

  WriteLock lockbfile_w(&mutex_);
  if (ra_file_reader_) return ra_file_reader_;

  std::unique_ptr<RandomAccessFile> rfile;
  Status s = env->NewRandomAccessFile(PathName(), &rfile, env_options);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(parent_->db_options_.info_log,
                    "Failed to open blob file for random-read: %s status: '%s'"
                    " exists: '%s'",
                    PathName().c_str(), s.ToString().c_str(),
                    env->FileExists(PathName()).ToString().c_str());
    return nullptr;
  }

  ra_file_reader_ = std::make_shared<RandomAccessFileReader>(std::move(rfile),
                                                             PathName());
  *fresh_open = true;
  return ra_file_reader_;
}

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
