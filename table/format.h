//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <string>
#include <stdint.h>
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace rocksdb {

class Block;
class RandomAccessFile;
struct ReadOptions;

// the length of the magic number in bytes.
const int kMagicNumberLengthByte = 8;

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
class BlockHandle {
 public:
  BlockHandle();
  BlockHandle(uint64_t offset, uint64_t size);

  // The offset of the block in the file.
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t offset) { offset_ = offset; }

  // The size of the stored block
  uint64_t size() const { return size_; }
  void set_size(uint64_t size) { size_ = size; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

  // if the block handle's offset and size are both "0", we will view it
  // as a null block handle that points to no where.
  bool IsNull() const {
    return offset_ == 0 && size_ == 0;
  }

  static const BlockHandle& NullBlockHandle() {
    return kNullBlockHandle;
  }

  // Maximum encoding length of a BlockHandle
  enum { kMaxEncodedLength = 10 + 10 };

 private:
  uint64_t offset_ = 0;
  uint64_t size_ = 0;

  static const BlockHandle kNullBlockHandle;
};

// Footer encapsulates the fixed information stored at the tail
// end of every table file.
class Footer {
 public:
  // Constructs a footer without specifying its table magic number.
  // In such case, the table magic number of such footer should be
  // initialized via @ReadFooterFromFile().
  Footer() : Footer(kInvalidTableMagicNumber) {}

  // @table_magic_number serves two purposes:
  //  1. Identify different types of the tables.
  //  2. Help us to identify if a given file is a valid sst.
  explicit Footer(uint64_t table_magic_number);

  // The version of the footer in this file
  uint32_t version() const { return version_; }

  // The checksum type used in this file
  ChecksumType checksum() const { return checksum_; }
  void set_checksum(const ChecksumType c) { checksum_ = c; }

  // The block handle for the metaindex block of the table
  const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
  void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

  // The block handle for the index block of the table
  const BlockHandle& index_handle() const { return index_handle_; }

  void set_index_handle(const BlockHandle& h) { index_handle_ = h; }

  uint64_t table_magic_number() const { return table_magic_number_; }

  // The version of Footer we encode
  enum {
    kLegacyFooter = 0,
    kFooterVersion = 1,
  };

  void EncodeTo(std::string* dst) const;

  // Set the current footer based on the input slice.  If table_magic_number_
  // is not set (i.e., HasInitializedTableMagicNumber() is true), then this
  // function will also initialize table_magic_number_.  Otherwise, this
  // function will verify whether the magic number specified in the input
  // slice matches table_magic_number_ and update the current footer only
  // when the test passes.
  Status DecodeFrom(Slice* input);

  // Encoded length of a Footer.  Note that the serialization of a Footer will
  // always occupy at least kMinEncodedLength bytes.  If fields are changed
  // the version number should be incremented and kMaxEncodedLength should be
  // increased accordingly.
  enum {
    // Footer version 0 (legacy) will always occupy exactly this many bytes.
    // It consists of two block handles, padding, and a magic number.
    kVersion0EncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8,
    // Footer version 1 will always occupy exactly this many bytes.
    // It consists of the checksum type, two block handles, padding,
    // a version number, and a magic number
    kVersion1EncodedLength = 1 + 2 * BlockHandle::kMaxEncodedLength + 4 + 8,

    kMinEncodedLength = kVersion0EncodedLength,
    kMaxEncodedLength = kVersion1EncodedLength
  };

  static const uint64_t kInvalidTableMagicNumber = 0;

 private:
  // REQUIRES: magic number wasn't initialized.
  void set_table_magic_number(uint64_t magic_number) {
    assert(!HasInitializedTableMagicNumber());
    table_magic_number_ = magic_number;
  }

  // return true if @table_magic_number_ is set to a value different
  // from @kInvalidTableMagicNumber.
  bool HasInitializedTableMagicNumber() const {
    return (table_magic_number_ != kInvalidTableMagicNumber);
  }

  uint32_t version_;
  ChecksumType checksum_;
  BlockHandle metaindex_handle_;
  BlockHandle index_handle_;
  uint64_t table_magic_number_ = 0;
};

// Read the footer from file
Status ReadFooterFromFile(RandomAccessFile* file,
                          uint64_t file_size,
                          Footer* footer);

// 1-byte type + 32-bit crc
static const size_t kBlockTrailerSize = 5;

struct BlockContents {
  Slice data;           // Actual contents of data
  bool cachable;        // True iff data can be cached
  bool heap_allocated;  // True iff caller should delete[] data.data()
  CompressionType compression_type;
};

// Read the block identified by "handle" from "file".  On failure
// return non-OK.  On success fill *result and return OK.
extern Status ReadBlockContents(RandomAccessFile* file,
                                const Footer& footer,
                                const ReadOptions& options,
                                const BlockHandle& handle,
                                BlockContents* result,
                                Env* env,
                                bool do_uncompress);

// The 'data' points to the raw block contents read in from file.
// This method allocates a new heap buffer and the raw block
// contents are uncompresed into this buffer. This buffer is
// returned via 'result' and it is upto the caller to
// free this buffer.
extern Status UncompressBlockContents(const char* data,
                                      size_t n,
                                      BlockContents* result);

// Implementation details follow.  Clients should ignore,

inline BlockHandle::BlockHandle()
    : BlockHandle(~static_cast<uint64_t>(0),
                  ~static_cast<uint64_t>(0)) {
}

inline BlockHandle::BlockHandle(uint64_t offset, uint64_t size)
    : offset_(offset),
      size_(size) {
}

}  // namespace rocksdb
