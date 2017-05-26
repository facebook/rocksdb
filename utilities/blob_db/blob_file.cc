//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#ifndef ROCKSDB_LITE

#include <stdio.h>
#include <chrono>
#include <cinttypes>
#include <memory>
#include "utilities/blob_db/blob_db_impl.h"

#include "util/filename.h"

namespace rocksdb {

namespace blob_db {

BlobFile::BlobFile()
    : parent_(nullptr),
      file_number_(0),
      blob_count_(0),
      gc_epoch_(-1),
      file_size_(0),
      deleted_count_(0),
      deleted_size_(0),
      closed_(false),
      can_be_deleted_(false),
      gc_once_after_open_(false),
      ttl_range_(std::make_pair(0, 0)),
      time_range_(std::make_pair(0, 0)),
      sn_range_(std::make_pair(0, 0)),
      last_access_(-1),
      last_fsync_(0),
      header_valid_(false) {}

BlobFile::BlobFile(const BlobDBImpl* p, const std::string& bdir, uint64_t fn)
    : parent_(p),
      path_to_dir_(bdir),
      file_number_(fn),
      blob_count_(0),
      gc_epoch_(-1),
      file_size_(0),
      deleted_count_(0),
      deleted_size_(0),
      closed_(false),
      can_be_deleted_(false),
      gc_once_after_open_(false),
      ttl_range_(std::make_pair(0, 0)),
      time_range_(std::make_pair(0, 0)),
      sn_range_(std::make_pair(0, 0)),
      last_access_(-1),
      last_fsync_(0),
      header_valid_(false) {}

BlobFile::~BlobFile() {
  if (can_be_deleted_) {
    std::string pn(PathName());
    Status s = Env::Default()->DeleteFile(PathName());
    if (!s.ok()) {
      // Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
      // "File could not be deleted %s", pn.c_str());
    }
  }
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

  std::shared_ptr<Reader> log_reader =
      std::make_shared<Reader>(db_options.info_log, std::move(sfile_reader));

  return log_reader;
}

std::string BlobFile::DumpState() const {
  char str[1000];
  snprintf(str, sizeof(str),
           "path: %s fn: %" PRIu64 " blob_count: %" PRIu64 " gc_epoch: %" PRIu64
           " file_size: %" PRIu64 " deleted_count: %" PRIu64
           " deleted_size: %" PRIu64
           " closed: %d can_be_deleted: %d ttl_range: (%d, %d)"
           " sn_range: (%" PRIu64 " %" PRIu64 "), writer: %d reader: %d",
           path_to_dir_.c_str(), file_number_, blob_count_.load(),
           gc_epoch_.load(), file_size_.load(), deleted_count_, deleted_size_,
           closed_.load(), can_be_deleted_.load(), ttl_range_.first,
           ttl_range_.second, sn_range_.first, sn_range_.second,
           (!!log_writer_), (!!ra_file_reader_));
  return str;
}

bool BlobFile::NeedsFsync(bool hard, uint64_t bytes_per_sync) const {
  assert(last_fsync_ <= file_size_);
  return (hard) ? file_size_ > last_fsync_
                : (file_size_ - last_fsync_) >= bytes_per_sync;
}

Status BlobFile::WriteFooterAndCloseLocked() {
  Log(InfoLogLevel::INFO_LEVEL, parent_->db_options_.info_log,
      "File is being closed after footer %s", PathName().c_str());

  BlobLogFooter footer;
  footer.blob_count_ = blob_count_;
  if (HasTTL()) footer.set_ttl_range(ttl_range_);

  footer.sn_range_ = sn_range_;
  if (HasTimestamp()) footer.set_time_range(time_range_);

  // this will close the file and reset the Writable File Pointer.
  Status s = log_writer_->AppendFooter(footer);
  if (s.ok()) {
    closed_ = true;
    file_size_ += BlobLogFooter::kFooterSize;
  } else {
    Log(InfoLogLevel::ERROR_LEVEL, parent_->db_options_.info_log,
        "Failure to read Header for blob-file %s", PathName().c_str());
  }
  // delete the sequential writer
  log_writer_.reset();
  return s;
}

Status BlobFile::ReadFooter(BlobLogFooter* bf) {
  if (file_size_ < (BlobLogHeader::kHeaderSize + BlobLogFooter::kFooterSize)) {
    return Status::IOError("File does not have footer", PathName());
  }

  uint64_t footer_offset = file_size_ - BlobLogFooter::kFooterSize;
  // assume that ra_file_reader_ is valid before we enter this
  assert(ra_file_reader_);

  Slice result;
  char scratch[BlobLogFooter::kFooterSize + 10];
  Status s = ra_file_reader_->Read(footer_offset, BlobLogFooter::kFooterSize,
                                   &result, scratch);
  if (!s.ok()) return s;
  if (result.size() != BlobLogFooter::kFooterSize) {
    // should not happen
    return Status::IOError("EOF reached before footer");
  }

  s = bf->DecodeFrom(result);
  return s;
}

Status BlobFile::SetFromFooterLocked(const BlobLogFooter& footer) {
  if (footer.HasTTL() != header_.HasTTL()) {
    return Status::Corruption("has_ttl mismatch");
  }
  if (footer.HasTimestamp() != header_.HasTimestamp()) {
    return Status::Corruption("has_ts mismatch");
  }

  // assume that file has been fully fsync'd
  last_fsync_.store(file_size_);
  blob_count_ = footer.GetBlobCount();
  ttl_range_ = footer.GetTTLRange();
  time_range_ = footer.GetTimeRange();
  sn_range_ = footer.GetSNRange();
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
  last_access_ =
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  {
    ReadLock lockbfile_r(&mutex_);
    if (ra_file_reader_) return ra_file_reader_;
  }

  WriteLock lockbfile_w(&mutex_);
  if (ra_file_reader_) return ra_file_reader_;

  std::unique_ptr<RandomAccessFile> rfile;
  Status s = env->NewRandomAccessFile(PathName(), &rfile, env_options);
  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, parent_->db_options_.info_log,
        "Failed to open blob file for random-read: %s status: '%s'"
        " exists: '%s'",
        PathName().c_str(), s.ToString().c_str(),
        env->FileExists(PathName()).ToString().c_str());
    return nullptr;
  }

  ra_file_reader_ = std::make_shared<RandomAccessFileReader>(std::move(rfile));
  *fresh_open = true;
  return ra_file_reader_;
}

ColumnFamilyHandle* BlobFile::GetColumnFamily(DB* db) {
  return db->DefaultColumnFamily();
}

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
