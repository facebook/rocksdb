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
  // @table_magic_number serves two purposes:
  //  1. Identify different types of the tables.
  //  2. Help us to identify if a given file is a valid sst.
  Footer(uint64_t table_magic_number) :
      kTableMagicNumber(table_magic_number) {
  }

  // The block handle for the metaindex block of the table
  const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
  void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

  // The block handle for the index block of the table
  const BlockHandle& index_handle() const {
    return index_handle_;
  }
  void set_index_handle(const BlockHandle& h) {
    index_handle_ = h;
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

  // Encoded length of a Footer.  Note that the serialization of a
  // Footer will always occupy exactly this many bytes.  It consists
  // of two block handles and a magic number.
  enum {
    kEncodedLength = 2*BlockHandle::kMaxEncodedLength + 8
  };

 private:
  BlockHandle metaindex_handle_;
  BlockHandle index_handle_;
  const uint64_t kTableMagicNumber;
};

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
