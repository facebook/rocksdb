//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Log format information shared by reader and writer.

#pragma once

#include <memory>
#include <utility>

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

constexpr uint32_t kMagicNumber = 2395959;  // 0x00248f37
constexpr uint32_t kVersion1 = 1;

using ExpirationRange = std::pair<uint64_t, uint64_t>;

// Format of blob log file header (30 bytes):
//
//    +--------------+---------+---------+-------+-------------+-------------------+
//    | magic number | version |  cf id  | flags | compression | expiration range  |
//    +--------------+---------+---------+-------+-------------+-------------------+
//    |   Fixed32    | Fixed32 | Fixed32 | char  |    char     | Fixed64   Fixed64 |
//    +--------------+---------+---------+-------+-------------+-------------------+
//
// List of flags:
//   has_ttl: Whether the file contain TTL data.
//
// Expiration range in the header is a rough range based on
// blob_db_options.ttl_range_secs.
struct BlobLogHeader {
  static constexpr size_t kSize = 30;

  BlobLogHeader() = default;
  BlobLogHeader(uint32_t _column_family_id, CompressionType _compression,
                bool _has_ttl, const ExpirationRange& _expiration_range)
      : column_family_id(_column_family_id),
        compression(_compression),
        has_ttl(_has_ttl),
        expiration_range(_expiration_range) {}

  uint32_t version = kVersion1;
  uint32_t column_family_id = 0;
  CompressionType compression = kNoCompression;
  bool has_ttl = false;
  ExpirationRange expiration_range;

  void EncodeTo(std::string* dst);

  Status DecodeFrom(Slice slice);
};

// Format of blob log file footer (32 bytes):
//
//    +--------------+------------+-------------------+------------+
//    | magic number | blob count | expiration range  | footer CRC |
//    +--------------+------------+-------------------+------------+
//    |   Fixed32    |  Fixed64   | Fixed64 + Fixed64 |   Fixed32  |
//    +--------------+------------+-------------------+------------+
//
// The footer will be presented only when the blob file is properly closed.
//
// Unlike the same field in file header, expiration range in the footer is the
// range of smallest and largest expiration of the data in this file.
struct BlobLogFooter {
  static constexpr size_t kSize = 32;

  uint64_t blob_count = 0;
  ExpirationRange expiration_range = std::make_pair(0, 0);
  uint32_t crc = 0;

  void EncodeTo(std::string* dst);

  Status DecodeFrom(Slice slice);
};

// Blob record format (32 bytes header + key + value):
//
//    +------------+--------------+------------+------------+----------+---------+-----------+
//    | key length | value length | expiration | header CRC | blob CRC |   key   |   value   |
//    +------------+--------------+------------+------------+----------+---------+-----------+
//    |   Fixed64  |   Fixed64    |  Fixed64   |  Fixed32   | Fixed32  | key len | value len |
//    +------------+--------------+------------+------------+----------+---------+-----------+
//
// If file has has_ttl = false, expiration field is always 0, and the blob
// doesn't has expiration.
//
// Also note that if compression is used, value is compressed value and value
// length is compressed value length.
//
// Header CRC is the checksum of (key_len + val_len + expiration), while
// blob CRC is the checksum of (key + value).
//
// We could use variable length encoding (Varint64) to save more space, but it
// make reader more complicated.
struct BlobLogRecord {
  // header include fields up to blob CRC
  static constexpr size_t kHeaderSize = 32;

  // Note that the offset field of BlobIndex actually points to the blob value
  // as opposed to the start of the blob record. The following method can
  // be used to calculate the adjustment needed to read the blob record header.
  static uint64_t CalculateAdjustmentForRecordHeader(uint64_t key_size) {
    return key_size + kHeaderSize;
  }

  uint64_t key_size = 0;
  uint64_t value_size = 0;
  uint64_t expiration = 0;
  uint32_t header_crc = 0;
  uint32_t blob_crc = 0;
  Slice key;
  Slice value;
  std::unique_ptr<char[]> key_buf;
  std::unique_ptr<char[]> value_buf;

  uint64_t record_size() const { return kHeaderSize + key_size + value_size; }

  void EncodeHeaderTo(std::string* dst);

  Status DecodeHeaderFrom(Slice src);

  Status CheckBlobCRC() const;
};

// Checks whether a blob offset is potentially valid or not.
inline bool IsValidBlobOffset(uint64_t value_offset, uint64_t key_size,
                              uint64_t value_size, uint64_t file_size) {
  if (value_offset <
      BlobLogHeader::kSize + BlobLogRecord::kHeaderSize + key_size) {
    return false;
  }

  if (value_offset + value_size + BlobLogFooter::kSize > file_size) {
    return false;
  }

  return true;
}

}  // namespace ROCKSDB_NAMESPACE
