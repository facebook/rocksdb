//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "db/blob/blob_log_format.h"

#include <algorithm>
#include <limits>

#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

void BlobLogHeader::EncodeTo(std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(BlobLogHeader::kSize);
  PutFixed32(dst, kMagicNumber);
  PutFixed32(dst, version);
  PutFixed32(dst, column_family_id);
  unsigned char flags = (has_ttl ? 1 : 0);
  dst->push_back(flags);
  dst->push_back(compression);
  PutFixed64(dst, expiration_range.first);
  PutFixed64(dst, expiration_range.second);
}

Status BlobLogHeader::DecodeFrom(Slice src) {
  const char* kErrorMessage = "Error while decoding blob log header";
  if (src.size() != BlobLogHeader::kSize) {
    return Status::Corruption(kErrorMessage,
                              "Unexpected blob file header size");
  }
  uint32_t magic_number;
  unsigned char flags;
  if (!GetFixed32(&src, &magic_number) || !GetFixed32(&src, &version) ||
      !GetFixed32(&src, &column_family_id)) {
    return Status::Corruption(
        kErrorMessage,
        "Error decoding magic number, version and column family id");
  }
  if (magic_number != kMagicNumber) {
    return Status::Corruption(kErrorMessage, "Magic number mismatch");
  }
  if (version != kVersion1) {
    return Status::Corruption(kErrorMessage, "Unknown header version");
  }
  flags = src.data()[0];
  compression = static_cast<CompressionType>(src.data()[1]);
  has_ttl = (flags & 1) == 1;
  src.remove_prefix(2);
  if (!GetFixed64(&src, &expiration_range.first) ||
      !GetFixed64(&src, &expiration_range.second)) {
    return Status::Corruption(kErrorMessage, "Error decoding expiration range");
  }
  return Status::OK();
}

void BlobLogHeaderV2::EncodeTo(std::string* dst) const {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(kSize);
  PutFixed32(dst, kMagicNumber);
  PutFixed32(dst, version);
  PutFixed32(dst, column_family_id);
  dst->push_back(0);  // flags (TTL is not supported)
  dst->push_back(static_cast<char>(compression));
  PutFixed64(dst, 0);  // expiration range lower bound
  PutFixed64(dst, 0);  // expiration range upper bound
  PutFixed64(dst, origin_file_number);
  assert(dst->size() == kSize);
}

Status BlobLogHeaderV2::DecodeFrom(Slice src) {
  constexpr char class_name[] = "BlobLogHeaderV2";
  if (src.size() != kSize) {
    return Status::Corruption(class_name, "Unexpected header size");
  }

  uint32_t magic_number = 0;
  if (!GetFixed32(&src, &magic_number) || !GetFixed32(&src, &version) ||
      !GetFixed32(&src, &column_family_id)) {
    return Status::Corruption(class_name, "Error decoding fixed fields");
  }
  if (magic_number != kMagicNumber) {
    return Status::Corruption(class_name, "Magic number mismatch");
  }
  if (version != kVersion2) {
    return Status::Corruption(class_name, "Unknown header version");
  }
  if (src.size() < 2) {
    return Status::Corruption(class_name, "Missing flags or compression");
  }
  const unsigned char flags = static_cast<unsigned char>(src.data()[0]);
  compression = static_cast<CompressionType>(src.data()[1]);
  src.remove_prefix(2);

  uint64_t expiration_lower = 0;
  uint64_t expiration_upper = 0;
  if (!GetFixed64(&src, &expiration_lower) ||
      !GetFixed64(&src, &expiration_upper) ||
      !GetFixed64(&src, &origin_file_number) || !src.empty()) {
    return Status::Corruption(class_name, "Error decoding trailing fields");
  }
  if (flags != 0 || expiration_lower != 0 || expiration_upper != 0) {
    return Status::Corruption(class_name, "TTL fields are not supported");
  }
  if (origin_file_number == 0) {
    return Status::Corruption(class_name, "Invalid origin file number");
  }
  return Status::OK();
}

void BlobLogFooter::EncodeTo(std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(BlobLogFooter::kSize);
  PutFixed32(dst, kMagicNumber);
  PutFixed64(dst, blob_count);
  PutFixed64(dst, expiration_range.first);
  PutFixed64(dst, expiration_range.second);
  crc = crc32c::Value(dst->c_str(), dst->size());
  crc = crc32c::Mask(crc);
  PutFixed32(dst, crc);
}

Status BlobLogFooter::DecodeFrom(Slice src) {
  const char* kErrorMessage = "Error while decoding blob log footer";
  if (src.size() != BlobLogFooter::kSize) {
    return Status::Corruption(kErrorMessage,
                              "Unexpected blob file footer size");
  }
  uint32_t src_crc = 0;
  src_crc = crc32c::Value(src.data(), BlobLogFooter::kSize - sizeof(uint32_t));
  src_crc = crc32c::Mask(src_crc);
  uint32_t magic_number = 0;
  if (!GetFixed32(&src, &magic_number) || !GetFixed64(&src, &blob_count) ||
      !GetFixed64(&src, &expiration_range.first) ||
      !GetFixed64(&src, &expiration_range.second) || !GetFixed32(&src, &crc)) {
    return Status::Corruption(kErrorMessage, "Error decoding content");
  }
  if (magic_number != kMagicNumber) {
    return Status::Corruption(kErrorMessage, "Magic number mismatch");
  }
  if (src_crc != crc) {
    return Status::Corruption(kErrorMessage, "CRC mismatch");
  }
  return Status::OK();
}

void BlobLogFooterV2::EncodeTo(std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(kSize);
  PutFixed32(dst, kMagicNumber);
  PutFixed32(dst, version);
  PutFixed64(dst, blob_count);
  PutFixed64(dst, origin_file_number);
  PutFixed64(dst, map_offset);
  PutFixed64(dst, map_size);
  PutFixed32(dst, map_crc);
  footer_crc = crc32c::Mask(crc32c::Value(dst->data(), dst->size()));
  PutFixed32(dst, footer_crc);
  assert(dst->size() == kSize);
}

Status BlobLogFooterV2::DecodeFrom(Slice src) {
  constexpr char class_name[] = "BlobLogFooterV2";
  if (src.size() != kSize) {
    return Status::Corruption(class_name, "Unexpected footer size");
  }

  const uint32_t actual_crc =
      crc32c::Mask(crc32c::Value(src.data(), kSize - sizeof(uint32_t)));
  uint32_t magic_number = 0;
  if (!GetFixed32(&src, &magic_number) || !GetFixed32(&src, &version) ||
      !GetFixed64(&src, &blob_count) ||
      !GetFixed64(&src, &origin_file_number) ||
      !GetFixed64(&src, &map_offset) || !GetFixed64(&src, &map_size) ||
      !GetFixed32(&src, &map_crc) || !GetFixed32(&src, &footer_crc) ||
      !src.empty()) {
    return Status::Corruption(class_name, "Error decoding content");
  }
  if (magic_number != kMagicNumber) {
    return Status::Corruption(class_name, "Magic number mismatch");
  }
  if (version != kVersion2) {
    return Status::Corruption(class_name, "Unknown footer version");
  }
  if (origin_file_number == 0) {
    return Status::Corruption(class_name, "Invalid origin file number");
  }
  if (actual_crc != footer_crc) {
    return Status::Corruption(class_name, "CRC mismatch");
  }
  return Status::OK();
}

void BlobLogRecord::EncodeHeaderTo(std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(BlobLogRecord::kHeaderSize + key.size() + value.size());
  PutFixed64(dst, key.size());
  PutFixed64(dst, value.size());
  PutFixed64(dst, expiration);
  header_crc = crc32c::Value(dst->c_str(), dst->size());
  header_crc = crc32c::Mask(header_crc);
  PutFixed32(dst, header_crc);
  blob_crc = crc32c::Value(key.data(), key.size());
  blob_crc = crc32c::Extend(blob_crc, value.data(), value.size());
  blob_crc = crc32c::Mask(blob_crc);
  PutFixed32(dst, blob_crc);
}

Status BlobLogRecord::DecodeHeaderFrom(Slice src) {
  const char* kErrorMessage = "Error while decoding blob record";
  if (src.size() != BlobLogRecord::kHeaderSize) {
    return Status::Corruption(kErrorMessage,
                              "Unexpected blob record header size");
  }
  uint32_t src_crc = 0;
  src_crc = crc32c::Value(src.data(), BlobLogRecord::kHeaderSize - 8);
  src_crc = crc32c::Mask(src_crc);
  if (!GetFixed64(&src, &key_size) || !GetFixed64(&src, &value_size) ||
      !GetFixed64(&src, &expiration) || !GetFixed32(&src, &header_crc) ||
      !GetFixed32(&src, &blob_crc)) {
    return Status::Corruption(kErrorMessage, "Error decoding content");
  }
  if (src_crc != header_crc) {
    return Status::Corruption(kErrorMessage, "Header CRC mismatch");
  }
  return Status::OK();
}

Status BlobLogRecord::CheckBlobCRC() const {
  uint32_t expected_crc = 0;
  expected_crc = crc32c::Value(key.data(), key.size());
  expected_crc = crc32c::Extend(expected_crc, value.data(), value.size());
  expected_crc = crc32c::Mask(expected_crc);
  if (expected_crc != blob_crc) {
    return Status::Corruption("Blob CRC mismatch");
  }
  return Status::OK();
}

void BlobLogRecordV2::EncodeHeaderTo(std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(kHeaderSize);
  PutFixed64(dst, key.size());
  PutFixed64(dst, value.size());
  PutFixed64(dst, expiration);
  PutFixed64(dst, origin_offset);
  header_crc = crc32c::Mask(crc32c::Value(dst->data(), dst->size()));
  PutFixed32(dst, header_crc);
  uint32_t checksum = crc32c::Value(key.data(), key.size());
  checksum = crc32c::Extend(checksum, value.data(), value.size());
  blob_crc = crc32c::Mask(checksum);
  PutFixed32(dst, blob_crc);
  assert(dst->size() == kHeaderSize);
}

Status BlobLogRecordV2::DecodeHeaderFrom(Slice src) {
  constexpr char class_name[] = "BlobLogRecordV2";
  if (src.size() != kHeaderSize) {
    return Status::Corruption(class_name, "Unexpected record header size");
  }

  const uint32_t actual_crc =
      crc32c::Mask(crc32c::Value(src.data(), kHeaderSize - 8));
  if (!GetFixed64(&src, &key_size) || !GetFixed64(&src, &value_size) ||
      !GetFixed64(&src, &expiration) || !GetFixed64(&src, &origin_offset) ||
      !GetFixed32(&src, &header_crc) || !GetFixed32(&src, &blob_crc) ||
      !src.empty()) {
    return Status::Corruption(class_name, "Error decoding content");
  }
  if (actual_crc != header_crc) {
    return Status::Corruption(class_name, "Header CRC mismatch");
  }
  return Status::OK();
}

Status BlobLogRecordV2::CheckBlobCRC() const {
  uint32_t expected_crc = crc32c::Value(key.data(), key.size());
  expected_crc = crc32c::Extend(expected_crc, value.data(), value.size());
  expected_crc = crc32c::Mask(expected_crc);
  if (expected_crc != blob_crc) {
    return Status::Corruption("Blob CRC mismatch");
  }
  return Status::OK();
}

Status BlobMap::Validate() const {
  for (size_t i = 1; i < entries_.size(); ++i) {
    if (entries_[i - 1].origin_offset >= entries_[i].origin_offset) {
      return Status::InvalidArgument(
          "BlobMap origin offsets must be strictly increasing");
    }
  }
  return Status::OK();
}

Status BlobMap::EncodeTo(std::string* dst, uint32_t* checksum) const {
  assert(dst != nullptr);
  assert(checksum != nullptr);

  const Status validation_status = Validate();
  if (!validation_status.ok()) {
    return validation_status;
  }

  if (entries_.size() >
      (std::numeric_limits<size_t>::max() - kHeaderSize - kTrailerSize) /
          kEntrySize) {
    return Status::InvalidArgument("BlobMap is too large to encode");
  }

  dst->clear();
  dst->reserve(kHeaderSize + entries_.size() * kEntrySize + kTrailerSize);
  PutFixed32(dst, kBlobMapMagicNumber);
  PutFixed32(dst, kVersion1);
  PutFixed64(dst, entries_.size());
  for (const BlobMapEntry& entry : entries_) {
    PutFixed64(dst, entry.origin_offset);
    PutFixed64(dst, entry.destination_offset);
  }
  *checksum = crc32c::Mask(crc32c::Value(dst->data(), dst->size()));
  PutFixed32(dst, *checksum);
  return Status::OK();
}

Status BlobMap::DecodeFrom(Slice src, uint32_t expected_checksum) {
  constexpr char class_name[] = "BlobMap";
  entries_.clear();
  if (src.size() < kHeaderSize + kTrailerSize) {
    return Status::Corruption(class_name, "Map is too short");
  }

  const size_t encoded_size = src.size();
  const uint32_t actual_checksum =
      crc32c::Mask(crc32c::Value(src.data(), encoded_size - kTrailerSize));

  uint32_t magic_number = 0;
  uint32_t version = 0;
  uint64_t count = 0;
  if (!GetFixed32(&src, &magic_number) || !GetFixed32(&src, &version) ||
      !GetFixed64(&src, &count)) {
    return Status::Corruption(class_name, "Error decoding header");
  }
  if (magic_number != kBlobMapMagicNumber) {
    return Status::Corruption(class_name, "Magic number mismatch");
  }
  if (version != kVersion1) {
    return Status::Corruption(class_name, "Unknown map version");
  }
  if (count >
      (std::numeric_limits<size_t>::max() - kHeaderSize - kTrailerSize) /
          kEntrySize) {
    return Status::Corruption(class_name, "Entry count overflow");
  }
  const size_t expected_size =
      kHeaderSize + static_cast<size_t>(count) * kEntrySize + kTrailerSize;
  if (expected_size != encoded_size) {
    return Status::Corruption(class_name, "Map size mismatch");
  }

  entries_.reserve(static_cast<size_t>(count));
  for (uint64_t i = 0; i < count; ++i) {
    BlobMapEntry entry;
    if (!GetFixed64(&src, &entry.origin_offset) ||
        !GetFixed64(&src, &entry.destination_offset)) {
      entries_.clear();
      return Status::Corruption(class_name, "Error decoding entry");
    }
    entries_.emplace_back(entry);
  }

  uint32_t stored_checksum = 0;
  if (!GetFixed32(&src, &stored_checksum) || !src.empty()) {
    entries_.clear();
    return Status::Corruption(class_name, "Error decoding checksum");
  }
  if (stored_checksum != actual_checksum ||
      stored_checksum != expected_checksum) {
    entries_.clear();
    return Status::Corruption(class_name, "Checksum mismatch");
  }

  const Status validation_status = Validate();
  if (!validation_status.ok()) {
    entries_.clear();
    return Status::Corruption(class_name, validation_status.ToString());
  }
  return Status::OK();
}

Status BlobMap::Find(uint64_t origin_offset,
                     uint64_t* destination_offset) const {
  assert(destination_offset != nullptr);
  const std::vector<BlobMapEntry>::const_iterator it =
      std::lower_bound(entries_.begin(), entries_.end(), origin_offset,
                       [](const BlobMapEntry& entry, uint64_t offset) {
                         return entry.origin_offset < offset;
                       });
  if (it == entries_.end() || it->origin_offset != origin_offset) {
    return Status::NotFound("BlobMap entry not found");
  }
  *destination_offset = it->destination_offset;
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
