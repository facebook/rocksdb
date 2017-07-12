//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_log_format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace rocksdb {
namespace blob_db {

const uint32_t kMagicNumber = 2395959;
const uint32_t kVersion1 = 1;
const size_t kBlockSize = 32768;

BlobLogHeader::BlobLogHeader()
    : magic_number_(kMagicNumber), compression_(kNoCompression) {}

BlobLogHeader& BlobLogHeader::operator=(BlobLogHeader&& in) noexcept {
  if (this != &in) {
    magic_number_ = in.magic_number_;
    version_ = in.version_;
    ttl_guess_ = std::move(in.ttl_guess_);
    ts_guess_ = std::move(in.ts_guess_);
    compression_ = in.compression_;
  }
  return *this;
}

BlobLogFooter::BlobLogFooter() : magic_number_(kMagicNumber), blob_count_(0) {}

Status BlobLogFooter::DecodeFrom(const Slice& input) {
  Slice slice(input);
  uint32_t val;
  if (!GetFixed32(&slice, &val)) {
    return Status::Corruption("Invalid Blob Footer: flags");
  }

  bool has_ttl = false;
  bool has_ts = false;
  val >>= 8;
  RecordSubType st = static_cast<RecordSubType>(val);
  switch (st) {
    case kRegularType:
      break;
    case kTTLType:
      has_ttl = true;
      break;
    case kTimestampType:
      has_ts = true;
      break;
    default:
      return Status::Corruption("Invalid Blob Footer: flags_val");
  }

  if (!GetFixed64(&slice, &blob_count_)) {
    return Status::Corruption("Invalid Blob Footer: blob_count");
  }

  ttlrange_t temp_ttl;
  if (!GetFixed32(&slice, &temp_ttl.first) ||
      !GetFixed32(&slice, &temp_ttl.second)) {
    return Status::Corruption("Invalid Blob Footer: ttl_range");
  }
  if (has_ttl) {
    ttl_range_.reset(new ttlrange_t(temp_ttl));
  }

  if (!GetFixed64(&slice, &sn_range_.first) ||
      !GetFixed64(&slice, &sn_range_.second)) {
    return Status::Corruption("Invalid Blob Footer: sn_range");
  }

  tsrange_t temp_ts;
  if (!GetFixed64(&slice, &temp_ts.first) ||
      !GetFixed64(&slice, &temp_ts.second)) {
    return Status::Corruption("Invalid Blob Footer: ts_range");
  }
  if (has_ts) {
    ts_range_.reset(new tsrange_t(temp_ts));
  }

  if (!GetFixed32(&slice, &magic_number_) || magic_number_ != kMagicNumber) {
    return Status::Corruption("Invalid Blob Footer: magic");
  }

  return Status::OK();
}

void BlobLogFooter::EncodeTo(std::string* dst) const {
  dst->reserve(kFooterSize);

  RecordType rt = kFullType;
  RecordSubType st = kRegularType;
  if (HasTTL()) {
    st = kTTLType;
  } else if (HasTimestamp()) {
    st = kTimestampType;
  }
  uint32_t val = static_cast<uint32_t>(rt) | (static_cast<uint32_t>(st) << 8);
  PutFixed32(dst, val);

  PutFixed64(dst, blob_count_);
  bool has_ttl = HasTTL();
  bool has_ts = HasTimestamp();

  if (has_ttl) {
    PutFixed32(dst, ttl_range_.get()->first);
    PutFixed32(dst, ttl_range_.get()->second);
  } else {
    PutFixed32(dst, 0);
    PutFixed32(dst, 0);
  }
  PutFixed64(dst, sn_range_.first);
  PutFixed64(dst, sn_range_.second);

  if (has_ts) {
    PutFixed64(dst, ts_range_.get()->first);
    PutFixed64(dst, ts_range_.get()->second);
  } else {
    PutFixed64(dst, 0);
    PutFixed64(dst, 0);
  }

  PutFixed32(dst, magic_number_);
}

void BlobLogHeader::EncodeTo(std::string* dst) const {
  dst->reserve(kHeaderSize);

  PutFixed32(dst, magic_number_);

  PutFixed32(dst, version_);

  RecordSubType st = kRegularType;
  bool has_ttl = HasTTL();
  bool has_ts = HasTimestamp();

  if (has_ttl) {
    st = kTTLType;
  } else if (has_ts) {
    st = kTimestampType;
  }
  uint32_t val =
      static_cast<uint32_t>(st) | (static_cast<uint32_t>(compression_) << 8);
  PutFixed32(dst, val);

  if (has_ttl) {
    PutFixed32(dst, ttl_guess_.get()->first);
    PutFixed32(dst, ttl_guess_.get()->second);
  } else {
    PutFixed32(dst, 0);
    PutFixed32(dst, 0);
  }

  if (has_ts) {
    PutFixed64(dst, ts_guess_.get()->first);
    PutFixed64(dst, ts_guess_.get()->second);
  } else {
    PutFixed64(dst, 0);
    PutFixed64(dst, 0);
  }
}

Status BlobLogHeader::DecodeFrom(const Slice& input) {
  Slice slice(input);
  if (!GetFixed32(&slice, &magic_number_) || magic_number_ != kMagicNumber) {
    return Status::Corruption("Invalid Blob Log Header: magic");
  }

  // as of today, we only support 1 version
  if (!GetFixed32(&slice, &version_) || version_ != kVersion1) {
    return Status::Corruption("Invalid Blob Log Header: version");
  }

  uint32_t val;
  if (!GetFixed32(&slice, &val)) {
    return Status::Corruption("Invalid Blob Log Header: subtype");
  }

  bool has_ttl = false;
  bool has_ts = false;
  RecordSubType st = static_cast<RecordSubType>(val & 0xff);
  compression_ = static_cast<CompressionType>((val >> 8) & 0xff);
  switch (st) {
    case kRegularType:
      break;
    case kTTLType:
      has_ttl = true;
      break;
    case kTimestampType:
      has_ts = true;
      break;
    default:
      return Status::Corruption("Invalid Blob Log Header: subtype_2");
  }

  ttlrange_t temp_ttl;
  if (!GetFixed32(&slice, &temp_ttl.first) ||
      !GetFixed32(&slice, &temp_ttl.second)) {
    return Status::Corruption("Invalid Blob Log Header: ttl");
  }
  if (has_ttl) set_ttl_guess(temp_ttl);

  tsrange_t temp_ts;
  if (!GetFixed64(&slice, &temp_ts.first) ||
      !GetFixed64(&slice, &temp_ts.second)) {
    return Status::Corruption("Invalid Blob Log Header: timestamp");
  }
  if (has_ts) set_ts_guess(temp_ts);

  return Status::OK();
}

BlobLogRecord::BlobLogRecord()
    : checksum_(0),
      header_cksum_(0),
      key_size_(0),
      blob_size_(0),
      time_val_(0),
      ttl_val_(0),
      sn_(0),
      type_(0),
      subtype_(0) {}

BlobLogRecord::~BlobLogRecord() {}

void BlobLogRecord::ResizeKeyBuffer(size_t kbs) {
  if (kbs > key_buffer_.size()) {
    key_buffer_.resize(kbs);
  }
}

void BlobLogRecord::ResizeBlobBuffer(size_t bbs) {
  if (bbs > blob_buffer_.size()) {
    blob_buffer_.resize(bbs);
  }
}

void BlobLogRecord::Clear() {
  checksum_ = 0;
  header_cksum_ = 0;
  key_size_ = 0;
  blob_size_ = 0;
  time_val_ = 0;
  ttl_val_ = 0;
  sn_ = 0;
  type_ = subtype_ = 0;
  key_.clear();
  blob_.clear();
}

Status BlobLogRecord::DecodeHeaderFrom(const Slice& hdrslice) {
  Slice input = hdrslice;
  if (input.size() < kHeaderSize) {
    return Status::Corruption("Invalid Blob Record Header: size");
  }

  if (!GetFixed32(&input, &key_size_)) {
    return Status::Corruption("Invalid Blob Record Header: key_size");
  }
  if (!GetFixed64(&input, &blob_size_)) {
    return Status::Corruption("Invalid Blob Record Header: blob_size");
  }
  if (!GetFixed32(&input, &ttl_val_)) {
    return Status::Corruption("Invalid Blob Record Header: ttl_val");
  }
  if (!GetFixed64(&input, &time_val_)) {
    return Status::Corruption("Invalid Blob Record Header: time_val");
  }

  type_ = *(input.data());
  input.remove_prefix(1);
  subtype_ = *(input.data());
  input.remove_prefix(1);

  if (!GetFixed32(&input, &header_cksum_)) {
    return Status::Corruption("Invalid Blob Record Header: header_cksum");
  }
  if (!GetFixed32(&input, &checksum_)) {
    return Status::Corruption("Invalid Blob Record Header: checksum");
  }

  return Status::OK();
}

Status BlobLogRecord::DecodeFooterFrom(const Slice& footerslice) {
  Slice input = footerslice;
  if (input.size() < kFooterSize) {
    return Status::Corruption("Invalid Blob Record Footer: size");
  }

  uint32_t f_crc = crc32c::Extend(0, input.data(), 8);
  f_crc = crc32c::Mask(f_crc);

  if (!GetFixed64(&input, &sn_)) {
    return Status::Corruption("Invalid Blob Record Footer: sn");
  }

  if (!GetFixed32(&input, &footer_cksum_)) {
    return Status::Corruption("Invalid Blob Record Footer: cksum");
  }

  if (f_crc != footer_cksum_) {
    return Status::Corruption("Record Checksum mismatch: footer_cksum");
  }

  return Status::OK();
}

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
