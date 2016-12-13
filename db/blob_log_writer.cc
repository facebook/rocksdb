//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/blob_log_writer.h"

#include <cstdint>
#include <limits>
#include <string>
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"

namespace rocksdb {
namespace blob_log {


////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Writer::Writer(unique_ptr<WritableFileWriter>&& dest, uint64_t log_number,
  uint64_t bpsync, bool use_fs, uint64_t boffset)
  : dest_(std::move(dest)), log_number_(log_number), block_offset_(boffset),
    bytes_per_sync_(bpsync), next_sync_offset_(0), use_fsync_(use_fs),
    last_elem_type_(ET_NONE) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Writer::~Writer() {
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void Writer::Sync() {
  dest_->Sync(true);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status Writer::WriteHeader(const blob_log::BlobLogHeader& header) {
  assert(block_offset_ == 0);
  assert(last_elem_type_ == ET_NONE);
  std::string str;
  header.EncodeTo(&str);

  Status s = dest_->Append(Slice(str));
  if (s.ok()) {
    block_offset_ += str.size();
    s = dest_->Flush();
  }
  last_elem_type_ = ET_FILE_HDR;
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status Writer::AppendFooter(const blob_log::BlobLogFooter& footer) {
  assert(block_offset_ != 0);
  assert(last_elem_type_ == ET_FILE_HDR || last_elem_type_ == ET_FOOTER);

  std::string str;
  footer.EncodeTo(&str);

  Status s = dest_->Append(Slice(str));
  if (s.ok()) {
    block_offset_ += str.size();
    s = dest_->Close();
    dest_.reset();
  }

  last_elem_type_ = ET_FILE_FOOTER;
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status Writer::AddRecord(const Slice& key, const Slice& val,
  uint64_t* key_offset, uint64_t* blob_offset, uint32_t ttl) {
  assert(block_offset_ != 0);
  assert(last_elem_type_ == ET_FILE_HDR || last_elem_type_ == ET_FOOTER);

  char buf[blob_log::BlobLogRecord::kHeaderSize];
  ConstructBlobHeader(buf, key, val, ttl, -1);

  Status s = EmitPhysicalRecord(buf, key, val, key_offset, blob_offset);
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status Writer::AddRecord(const Slice& key, const Slice& val,
  uint64_t* key_offset, uint64_t* blob_offset) {
  assert(block_offset_ != 0);
  assert(last_elem_type_ == ET_FILE_HDR || last_elem_type_ == ET_FOOTER);

  char buf[blob_log::BlobLogRecord::kHeaderSize];
  ConstructBlobHeader(buf, key, val, -1, -1);

  Status s = EmitPhysicalRecord(buf, key, val, key_offset, blob_offset);
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void Writer::ConstructBlobHeader(char *headerbuf, const Slice& key,
  const Slice& val, int32_t ttl, int64_t ts) {
  RecordType t = kFullType;
  RecordSubType st = kRegularType;
  if (ttl != -1)
    st = kTTLType;

  uint32_t offset = 8;
  // Format the header
  EncodeFixed32(headerbuf+offset, key.size());
  offset += 4;
  EncodeFixed64(headerbuf+offset, val.size());
  offset += 8;
  if (ttl != -1) {
    EncodeFixed32(headerbuf+offset, (uint32_t)ttl);
  } else {
    EncodeFixed32(headerbuf+offset, std::numeric_limits<uint32_t>::max());
  }

  offset += 4;
  if (ts != -1) {
    EncodeFixed64(headerbuf+offset, (uint64_t)ts);
  } else {
    EncodeFixed64(headerbuf+offset, std::numeric_limits<uint64_t>::max());
  }
  offset += 8;

  headerbuf[offset] = static_cast<char>(t);
  offset++;
  headerbuf[offset] = static_cast<char>(st);

  uint32_t header_crc = 0;
  header_crc = crc32c::Extend(header_crc, headerbuf+2*sizeof(uint32_t),
    blob_log::BlobLogRecord::kHeaderSize - 2*sizeof(uint32_t));
  header_crc = crc32c::Mask(header_crc);
  EncodeFixed32(headerbuf + 4, header_crc);

  uint32_t crc = 0;
  // Compute the crc of the record type and the payload.
  crc = crc32c::Extend(crc, val.data(), val.size());
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(headerbuf, crc);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status Writer::EmitPhysicalRecord(const char *headerbuf, const Slice& key,
  const Slice& val, uint64_t* key_offset, uint64_t* blob_offset) {
  Status s = dest_->Append(Slice(headerbuf,
    blob_log::BlobLogRecord::kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(key);
    if (s.ok())
      s = dest_->Append(val);
  }

  *key_offset = block_offset_ + blob_log::BlobLogRecord::kHeaderSize;
  *blob_offset = *key_offset + key.size();
  block_offset_ = *blob_offset + val.size();
  last_elem_type_ = ET_RECORD;
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status Writer::AddRecordFooter(const SequenceNumber& seq) {
  assert(last_elem_type_ == ET_RECORD);

  char buf[8];
  EncodeFixed64(buf, seq);
  Status s = dest_->Append(Slice(buf, 8));
  block_offset_ += blob_log::BlobLogRecord::kFooterSize;

  if (s.ok())
    dest_->Flush();

  last_elem_type_ = ET_FOOTER;
  return s;
}

}  // namespace blob_log
}  // namespace rocksdb
