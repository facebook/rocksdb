//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.

#pragma once

#include <cstdint>
#include <utility>
#include <string>
#include <cstddef>
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace rocksdb {

  class BlobFile;

namespace blob_log {

enum RecordType {
  // Zero is reserved for preallocated files
  kFullType = 0,

  // For fragments
  kFirstType = 1,
  kMiddleType = 2,
  kLastType = 3,
};

enum RecordSubType {

  kRegularType = 0,
  kTTLType = 1,
  kTimestampType = 2,
};

const uint32_t kMagicNumber = 2395959;

class Reader;

class BlobLogHeader {

 private:

   uint32_t magic_number_ = 0;
   std::pair<uint32_t, uint32_t> ttl_guess_;
   std::pair<uint64_t, uint64_t> ts_guess_;
   bool has_ttl_;
   bool has_ts_;

 public:

  // magic number + flags + ttl guess + timestamp range
  static const size_t kHeaderSize = 4 + 4 + 4 * 2 + 8 * 2;
  // 32

  void setTTL(bool ttl = true)  { has_ttl_ = ttl; }

  void setTimestamps(bool ts = true) { has_ts_ = ts; }

  void setTTLGuess(const std::pair<uint32_t, uint32_t>& ttl) { ttl_guess_ = ttl; has_ttl_ = true; }

  void setTSGuess(const std::pair<uint64_t, uint64_t>& ts) { ts_guess_ = ts; has_ts_ = true; }

  void EncodeTo(std::string* dst) const;

  Status DecodeFrom(Slice* input);

  BlobLogHeader();

  bool HasTTL() const { return has_ttl_; }

  bool HasTimestamps() const { return has_ts_; }
};

// Footer encapsulates the fixed information stored at the tail
// end of every blob log file.
class BlobLogFooter {
  friend class rocksdb::BlobFile;

 public:

  // Use this constructor when you plan to write out the footer using
  // EncodeTo(). Never use this constructor with DecodeFrom().
  BlobLogFooter();

  uint64_t magic_number() const { return magic_number_; }

  void EncodeTo(std::string* dst) const;

  Status DecodeFrom(Slice* input);

  // convert this object to a human readable form
  std::string ToString() const;

  // footer size = 4 byte magic number
  // 8 bytes count
  // 4, 4 - ttl range
  // 8, 8 - sn range
  // 8, 8 - ts range
  static const int kFooterSize = 4 + 4 + 8 + (4 * 2) + (8 * 2) + (8 * 2);

  bool HasTTL() const { return has_ttl_; }

  bool HasTimestamps() const { return has_ts_; }

  uint64_t GetBlobCount() const { return blob_count_; }

  const std::pair<uint32_t, uint32_t>& GetTTLRange() const { return ttl_range_; }

  const std::pair<uint64_t, uint64_t>& GetTimeRange() const { return ts_range_; }

  const std::pair<SequenceNumber, SequenceNumber>& GetSNRange() const { return sn_range_; }

 private:

  uint32_t magic_number_ = 0;
  uint64_t blob_count_ = 0;
  bool has_ttl_;
  bool has_ts_;

  std::pair<uint32_t, uint32_t> ttl_range_;
  std::pair<uint64_t, uint64_t> ts_range_;
  std::pair<SequenceNumber, SequenceNumber> sn_range_;
};

static const int kMaxRecordType = kLastType;

static const unsigned int kBlockSize = 32768;


class BlobLogRecord {
  friend class Reader;

private:
  // this might not be set.
  uint32_t checksum_;
  uint32_t header_cksum_;
  uint32_t key_size_;
  uint64_t blob_size_;
  uint64_t time_val_;
  uint32_t ttl_val_;
  SequenceNumber sn_;
  char type_;
  char subtype_;
  Slice key_;
  Slice blob_;
  char *key_buffer_;
  uint64_t kbs_;
  char *blob_buffer_;
  uint64_t bbs_;

public:
  // Header is checksum (4 bytes), header checksum (4bytes), Key Length ( 4 bytes ),
  // Blob Length ( 8 bytes), timestamp/ttl (8 bytes),
  // type (1 byte), subtype (1 byte)
  static const int kHeaderSize = 4 + 4 + 4 + 8 + 4 + 8 + 1 + 1;
  // 34
  static const int kFooterSize = 8;

public:
   
  BlobLogRecord();

  ~BlobLogRecord();

  void clear();

  char *getKeyBuffer() { return key_buffer_; }

  char *getBlobBuffer() { return blob_buffer_; }

  void resizeKeyBuffer(uint64_t kbs);

  void resizeBlobBuffer(uint64_t bbs);

  const Slice& Key() const { return key_; }

  const Slice& Blob() const { return blob_; }

  uint32_t GetKeySize() const { return key_size_; }

  uint64_t GetBlobSize()  const { return blob_size_; }

  uint32_t GetTTL() const { return ttl_val_; }
  
  uint64_t GetTimeVal() const { return time_val_; }

  SequenceNumber GetSN() const { return sn_; }

  Status DecodeHeaderFrom(Slice* input);
};


}  // namespace blob_log
}  // namespace rocksdb
