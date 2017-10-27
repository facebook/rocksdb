//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_log_reader.h"

#include <algorithm>

#include "util/file_reader_writer.h"

namespace rocksdb {
namespace blob_db {

Reader::Reader(std::shared_ptr<Logger> info_log,
               unique_ptr<SequentialFileReader>&& _file)
    : info_log_(info_log), file_(std::move(_file)), buffer_(), next_byte_(0) {}

Status Reader::ReadSlice(uint64_t size, Slice* slice, std::string* buf) {
  buf->reserve(size);
  Status s = file_->Read(size, slice, &(*buf)[0]);
  next_byte_ += size;
  if (!s.ok()) {
    return s;
  }
  if (slice->size() != size) {
    return Status::Corruption("EOF reached while reading record");
  }
  return s;
}

Status Reader::ReadHeader(BlobLogHeader* header) {
  assert(file_.get() != nullptr);
  assert(next_byte_ == 0);
  Status s = ReadSlice(BlobLogHeader::kSize, &buffer_, &backing_store_);
  if (!s.ok()) {
    return s;
  }

  if (buffer_.size() != BlobLogHeader::kSize) {
    return Status::Corruption("EOF reached before file header");
  }

  return header->DecodeFrom(buffer_);
}

Status Reader::ReadRecord(BlobLogRecord* record, ReadLevel level,
                          uint64_t* blob_offset) {
  Status s = ReadSlice(BlobLogRecord::kHeaderSize, &buffer_, &backing_store_);
  if (!s.ok()) {
    return s;
  }
  if (buffer_.size() != BlobLogRecord::kHeaderSize) {
    return Status::Corruption("EOF reached before record header");
  }

  s = record->DecodeHeaderFrom(buffer_);
  if (!s.ok()) {
    return s;
  }

  uint64_t kb_size = record->key_size + record->value_size;
  if (blob_offset != nullptr) {
    *blob_offset = next_byte_ + record->key_size;
  }

  switch (level) {
    case kReadHeader:
      file_->Skip(record->key_size + record->value_size);
      next_byte_ += kb_size;
      break;

    case kReadHeaderKey:
      s = ReadSlice(record->key_size, &record->key, &record->key_buf);
      file_->Skip(record->value_size);
      next_byte_ += record->value_size;
      break;

    case kReadHeaderKeyBlob:
      s = ReadSlice(record->key_size, &record->key, &record->key_buf);
      if (s.ok()) {
        s = ReadSlice(record->value_size, &record->value, &record->value_buf);
      }
      if (s.ok()) {
        s = record->CheckBlobCRC();
      }
      break;
  }
  return s;
}

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
