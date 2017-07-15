//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_log_writer.h"

#include <cstdint>
#include <limits>
#include <string>
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"

namespace rocksdb {
namespace blob_db {

Writer::Writer(unique_ptr<WritableFileWriter>&& dest, uint64_t log_number,
               uint64_t bpsync, bool use_fs, uint64_t boffset)
    : dest_(std::move(dest)),
      log_number_(log_number),
      block_offset_(boffset),
      bytes_per_sync_(bpsync),
      next_sync_offset_(0),
      use_fsync_(use_fs),
      last_elem_type_(kEtNone) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

Writer::~Writer() {}

void Writer::Sync() { dest_->Sync(use_fsync_); }

Status Writer::WriteHeader(const BlobLogHeader& header) {
  assert(block_offset_ == 0);
  assert(last_elem_type_ == kEtNone);
  std::string str;
  header.EncodeTo(&str);

  Status s = dest_->Append(Slice(str));
  if (s.ok()) {
    block_offset_ += str.size();
    s = dest_->Flush();
  }
  last_elem_type_ = kEtFileHdr;
  return s;
}

Status Writer::AppendFooter(const BlobLogFooter& footer) {
  assert(block_offset_ != 0);
  assert(last_elem_type_ == kEtFileHdr || last_elem_type_ == kEtFooter);

  std::string str;
  footer.EncodeTo(&str);

  Status s = dest_->Append(Slice(str));
  if (s.ok()) {
    block_offset_ += str.size();
    s = dest_->Close();
    dest_.reset();
  }

  last_elem_type_ = kEtFileFooter;
  return s;
}

Status Writer::AddRecord(const Slice& key, const Slice& val,
                         uint64_t* key_offset, uint64_t* blob_offset,
                         uint32_t ttl) {
  assert(block_offset_ != 0);
  assert(last_elem_type_ == kEtFileHdr || last_elem_type_ == kEtFooter);

  std::string buf;
  ConstructBlobHeader(&buf, key, val, ttl, -1);

  Status s = EmitPhysicalRecord(buf, key, val, key_offset, blob_offset);
  return s;
}

Status Writer::AddRecord(const Slice& key, const Slice& val,
                         uint64_t* key_offset, uint64_t* blob_offset) {
  assert(block_offset_ != 0);
  assert(last_elem_type_ == kEtFileHdr || last_elem_type_ == kEtFooter);

  std::string buf;
  ConstructBlobHeader(&buf, key, val, -1, -1);

  Status s = EmitPhysicalRecord(buf, key, val, key_offset, blob_offset);
  return s;
}

void Writer::ConstructBlobHeader(std::string* headerbuf, const Slice& key,
                                 const Slice& val, int32_t ttl, int64_t ts) {
  headerbuf->reserve(BlobLogRecord::kHeaderSize);

  uint32_t key_size = static_cast<uint32_t>(key.size());
  PutFixed32(headerbuf, key_size);
  PutFixed64(headerbuf, val.size());

  uint32_t ttl_write = (ttl != -1) ? static_cast<uint32_t>(ttl)
                                   : std::numeric_limits<uint32_t>::max();
  PutFixed32(headerbuf, ttl_write);

  uint64_t ts_write = (ts != -1) ? static_cast<uint64_t>(ts)
                                 : std::numeric_limits<uint64_t>::max();
  PutFixed64(headerbuf, ts_write);

  RecordType t = kFullType;
  headerbuf->push_back(static_cast<char>(t));

  RecordSubType st = kRegularType;
  if (ttl != -1) st = kTTLType;
  headerbuf->push_back(static_cast<char>(st));

  uint32_t header_crc = 0;
  header_crc =
      crc32c::Extend(header_crc, headerbuf->c_str(), headerbuf->size());
  header_crc = crc32c::Extend(header_crc, key.data(), key.size());
  header_crc = crc32c::Mask(header_crc);
  PutFixed32(headerbuf, header_crc);

  uint32_t crc = 0;
  // Compute the crc of the record type and the payload.
  crc = crc32c::Extend(crc, val.data(), val.size());
  crc = crc32c::Mask(crc);  // Adjust for storage
  PutFixed32(headerbuf, crc);
}

Status Writer::EmitPhysicalRecord(const std::string& headerbuf,
                                  const Slice& key, const Slice& val,
                                  uint64_t* key_offset, uint64_t* blob_offset) {
  Status s = dest_->Append(Slice(headerbuf));
  if (s.ok()) {
    s = dest_->Append(key);
    if (s.ok()) s = dest_->Append(val);
  }

  *key_offset = block_offset_ + BlobLogRecord::kHeaderSize;
  *blob_offset = *key_offset + key.size();
  block_offset_ = *blob_offset + val.size();
  last_elem_type_ = kEtRecord;
  return s;
}

Status Writer::AddRecordFooter(const SequenceNumber& seq) {
  assert(last_elem_type_ == kEtRecord);

  std::string buf;
  PutFixed64(&buf, seq);

  uint32_t footer_crc = crc32c::Extend(0, buf.c_str(), buf.size());
  footer_crc = crc32c::Mask(footer_crc);
  PutFixed32(&buf, footer_crc);

  Status s = dest_->Append(Slice(buf));
  block_offset_ += BlobLogRecord::kFooterSize;

  if (s.ok()) dest_->Flush();

  last_elem_type_ = kEtFooter;
  return s;
}

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
