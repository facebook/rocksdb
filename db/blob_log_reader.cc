//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/blob_log_reader.h"

#include <stdio.h>
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"

namespace rocksdb {
namespace blob_log {

Reader::Reporter::~Reporter() {
}

Reader::Reader(std::shared_ptr<Logger> info_log,
               unique_ptr<SequentialFileReader>&& _file, Reporter* reporter,
               bool checksum, uint64_t initial_offset, uint64_t log_num)
    : info_log_(info_log),
      file_(std::move(_file)),
      reporter_(reporter),
      //checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      bs_size_(kBlockSize),
      buffer_(),
      //eof_(false),
      //read_error_(false),
      //eof_offset_(0),
      //last_record_offset_(0),
      //end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      log_number_(log_num) {}

Reader::~Reader() {
  delete[] backing_store_;
}

void Reader::resizeBackingStore(uint64_t bss) {
  if (bss < bs_size_)
    return;

   bs_size_ = bss;
   delete [] backing_store_;
   backing_store_ = new char[bs_size_*2];
}

Status Reader::ReadHeader(blob_log::BlobLogHeader& header)
{
  Status status = file_->Read(blob_log::BlobLogHeader::kHeaderSize, &buffer_, backing_store_);
  if (!status.ok()) {
    return status;
  }

  status = header.DecodeFrom(&buffer_);
  return status;
}

Status Reader::ReadRecord(blob_log::BlobLogRecord& record,
  int level, WALRecoveryMode wal_recovery_mode) {

  record.clear();
  buffer_.clear();
  backing_store_[0] = '\0';

  Status status = file_->Read(blob_log::BlobLogRecord::kHeaderSize, &buffer_, backing_store_);
  if (!status.ok()) {
     return status;
  }

  status = record.DecodeFrom(&buffer_);
  if (!status.ok()) {
    return status;
  } 

  switch (level) {
    case 0 :
      file_->Skip(record.GetKeySize() + record.GetBlobSize());
      return status;

    case 1 :
      resizeBackingStore((uint64_t)record.GetKeySize());
      status = file_->Read(record.GetKeySize(), &record.key_, backing_store_);
      return status;

    case 2:
      resizeBackingStore((uint64_t)record.GetKeySize());
      status = file_->Read(record.GetKeySize(), &record.key_, backing_store_);
      resizeBackingStore((uint64_t)record.GetBlobSize());
      status = file_->Read(record.GetBlobSize(), &record.blob_, backing_store_);
      return status;
    default :
       return status;
  }
}

}  // namespace blob_log
}  // namespace rocksdb
