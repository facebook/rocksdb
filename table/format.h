//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>
#include <string>
#include "file/file_prefetch_buffer.h"
#include "file/random_access_file_reader.h"

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

#include "memory/memory_allocator.h"
#include "options/cf_options.h"
#include "port/malloc.h"
#include "port/port.h"  // noexcept
#include "table/persistent_cache_options.h"

namespace ROCKSDB_NAMESPACE {

class RandomAccessFile;
struct ReadOptions;

extern bool ShouldReportDetailedTime(Env* env, Statistics* stats);

// the length of the magic number in bytes.
const int kMagicNumberLengthByte = 8;

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
class BlockHandle {
 public:
  // Creates a block handle with special values indicating "uninitialized,"
  // distinct from the "null" block handle.
  BlockHandle();
  BlockHandle(uint64_t offset, uint64_t size);

  // The offset of the block in the file.
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t _offset) { offset_ = _offset; }

  // The size of the stored block
  uint64_t size() const { return size_; }
  void set_size(uint64_t _size) { size_ = _size; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
  Status DecodeSizeFrom(uint64_t offset, Slice* input);

  // Return a string that contains the copy of handle.
  std::string ToString(bool hex = true) const;

  // if the block handle's offset and size are both "0", we will view it
  // as a null block handle that points to no where.
  bool IsNull() const { return offset_ == 0 && size_ == 0; }

  static const BlockHandle& NullBlockHandle() { return kNullBlockHandle; }

  // Maximum encoding length of a BlockHandle
  enum { kMaxEncodedLength = 10 + 10 };

  inline bool operator==(const BlockHandle& rhs) const {
    return offset_ == rhs.offset_ && size_ == rhs.size_;
  }
  inline bool operator!=(const BlockHandle& rhs) const {
    return !(*this == rhs);
  }

 private:
  uint64_t offset_;
  uint64_t size_;

  static const BlockHandle kNullBlockHandle;
};

// Value in block-based table file index.
//
// The index entry for block n is: y -> h, [x],
// where: y is some key between the last key of block n (inclusive) and the
// first key of block n+1 (exclusive); h is BlockHandle pointing to block n;
// x, if present, is the first key of block n (unshortened).
// This struct represents the "h, [x]" part.
struct IndexValue {
  BlockHandle handle;
  // Empty means unknown.
  Slice first_internal_key;

  IndexValue() = default;
  IndexValue(BlockHandle _handle, Slice _first_internal_key)
      : handle(_handle), first_internal_key(_first_internal_key) {}

  // have_first_key indicates whether the `first_internal_key` is used.
  // If previous_handle is not null, delta encoding is used;
  // in this case, the two handles must point to consecutive blocks:
  // handle.offset() ==
  //     previous_handle->offset() + previous_handle->size() + kBlockTrailerSize
  void EncodeTo(std::string* dst, bool have_first_key,
                const BlockHandle* previous_handle) const;
  Status DecodeFrom(Slice* input, bool have_first_key,
                    const BlockHandle* previous_handle);

  std::string ToString(bool hex, bool have_first_key) const;
};

inline uint32_t GetCompressFormatForVersion(uint32_t format_version) {
  // As of format_version 2, we encode compressed block with
  // compress_format_version == 2. Before that, the version is 1.
  // DO NOT CHANGE THIS FUNCTION, it affects disk format
  return format_version >= 2 ? 2 : 1;
}

inline bool BlockBasedTableSupportedVersion(uint32_t version) {
  return version <= 5;
}

// Footer encapsulates the fixed information stored at the tail
// end of every table file.
class Footer {
 public:
  // Constructs a footer without specifying its table magic number.
  // In such case, the table magic number of such footer should be
  // initialized via @ReadFooterFromFile().
  // Use this when you plan to load Footer with DecodeFrom(). Never use this
  // when you plan to EncodeTo.
  Footer() : Footer(kInvalidTableMagicNumber, 0) {}

  // Use this constructor when you plan to write out the footer using
  // EncodeTo(). Never use this constructor with DecodeFrom().
  // `version` is same as `format_version` for block-based table.
  Footer(uint64_t table_magic_number, uint32_t version);

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

  void EncodeTo(std::string* dst) const;

  // Set the current footer based on the input slice.
  //
  // REQUIRES: table_magic_number_ is not set (i.e.,
  // HasInitializedTableMagicNumber() is true). The function will initialize the
  // magic number
  Status DecodeFrom(Slice* input);

  // Encoded length of a Footer.  Note that the serialization of a Footer will
  // always occupy at least kMinEncodedLength bytes.  If fields are changed
  // the version number should be incremented and kMaxEncodedLength should be
  // increased accordingly.
  enum {
    // Footer version 0 (legacy) will always occupy exactly this many bytes.
    // It consists of two block handles, padding, and a magic number.
    kVersion0EncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8,
    // Footer of versions 1 and higher will always occupy exactly this many
    // bytes. It consists of the checksum type, two block handles, padding,
    // a version number (bigger than 1), and a magic number
    kNewVersionsEncodedLength = 1 + 2 * BlockHandle::kMaxEncodedLength + 4 + 8,
    kMinEncodedLength = kVersion0EncodedLength,
    kMaxEncodedLength = kNewVersionsEncodedLength,
  };

  static const uint64_t kInvalidTableMagicNumber = 0;

  // convert this object to a human readable form
  std::string ToString() const;

  // Block trailer size used by file with this footer (e.g. 5 for block-based
  // table and 0 for plain table)
  inline size_t GetBlockTrailerSize() const { return block_trailer_size_; }

 private:
  // REQUIRES: magic number wasn't initialized.
  void set_table_magic_number(uint64_t magic_number);

  // return true if @table_magic_number_ is set to a value different
  // from @kInvalidTableMagicNumber.
  bool HasInitializedTableMagicNumber() const {
    return (table_magic_number_ != kInvalidTableMagicNumber);
  }

  uint32_t version_;
  ChecksumType checksum_;
  uint8_t block_trailer_size_ = 0;  // set based on magic number
  BlockHandle metaindex_handle_;
  BlockHandle index_handle_;
  uint64_t table_magic_number_ = 0;
};

// Read the footer from file
// If enforce_table_magic_number != 0, ReadFooterFromFile() will return
// corruption if table_magic number is not equal to enforce_table_magic_number
Status ReadFooterFromFile(const IOOptions& opts, RandomAccessFileReader* file,
                          FilePrefetchBuffer* prefetch_buffer,
                          uint64_t file_size, Footer* footer,
                          uint64_t enforce_table_magic_number = 0);

// Computes a checksum using the given ChecksumType. Sometimes we need to
// include one more input byte logically at the end but not part of the main
// data buffer. If data_size >= 1, then
// ComputeBuiltinChecksum(type, data, size)
// ==
// ComputeBuiltinChecksumWithLastByte(type, data, size - 1, data[size - 1])
uint32_t ComputeBuiltinChecksum(ChecksumType type, const char* data,
                                size_t size);
uint32_t ComputeBuiltinChecksumWithLastByte(ChecksumType type, const char* data,
                                            size_t size, char last_byte);

// Represents the contents of a block read from an SST file. Depending on how
// it's created, it may or may not own the actual block bytes. As an example,
// BlockContents objects representing data read from mmapped files only point
// into the mmapped region.
struct BlockContents {
  // Points to block payload (without trailer)
  Slice data;
  CacheAllocationPtr allocation;

#ifndef NDEBUG
  // Whether there is a known trailer after what is pointed to by `data`.
  // See BlockBasedTable::GetCompressionType.
  bool is_raw_block = false;
#endif  // NDEBUG

  BlockContents() {}

  // Does not take ownership of the underlying data bytes.
  BlockContents(const Slice& _data) : data(_data) {}

  // Takes ownership of the underlying data bytes.
  BlockContents(CacheAllocationPtr&& _data, size_t _size)
      : data(_data.get(), _size), allocation(std::move(_data)) {}

  // Takes ownership of the underlying data bytes.
  BlockContents(std::unique_ptr<char[]>&& _data, size_t _size)
      : data(_data.get(), _size) {
    allocation.reset(_data.release());
  }

  // Returns whether the object has ownership of the underlying data bytes.
  bool own_bytes() const { return allocation.get() != nullptr; }

  // The additional memory space taken by the block data.
  size_t usable_size() const {
    if (allocation.get() != nullptr) {
      auto allocator = allocation.get_deleter().allocator;
      if (allocator) {
        return allocator->UsableSize(allocation.get(), data.size());
      }
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      return malloc_usable_size(allocation.get());
#else
      return data.size();
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    } else {
      return 0;  // no extra memory is occupied by the data
    }
  }

  size_t ApproximateMemoryUsage() const {
    return usable_size() + sizeof(*this);
  }

  BlockContents(BlockContents&& other) ROCKSDB_NOEXCEPT {
    *this = std::move(other);
  }

  BlockContents& operator=(BlockContents&& other) {
    data = std::move(other.data);
    allocation = std::move(other.allocation);
#ifndef NDEBUG
    is_raw_block = other.is_raw_block;
#endif  // NDEBUG
    return *this;
  }
};

// The 'data' points to the raw block contents read in from file.
// This method allocates a new heap buffer and the raw block
// contents are uncompresed into this buffer. This buffer is
// returned via 'result' and it is upto the caller to
// free this buffer.
// For description of compress_format_version and possible values, see
// util/compression.h
extern Status UncompressBlockContents(const UncompressionInfo& info,
                                      const char* data, size_t n,
                                      BlockContents* contents,
                                      uint32_t compress_format_version,
                                      const ImmutableOptions& ioptions,
                                      MemoryAllocator* allocator = nullptr);

// This is an extension to UncompressBlockContents that accepts
// a specific compression type. This is used by un-wrapped blocks
// with no compression header.
extern Status UncompressBlockContentsForCompressionType(
    const UncompressionInfo& info, const char* data, size_t n,
    BlockContents* contents, uint32_t compress_format_version,
    const ImmutableOptions& ioptions, MemoryAllocator* allocator = nullptr);

// Replace db_host_id contents with the real hostname if necessary
extern Status ReifyDbHostIdProperty(Env* env, std::string* db_host_id);

// Implementation details follow.  Clients should ignore,

// TODO(andrewkr): we should prefer one way of representing a null/uninitialized
// BlockHandle. Currently we use zeros for null and use negation-of-zeros for
// uninitialized.
inline BlockHandle::BlockHandle() : BlockHandle(~uint64_t{0}, ~uint64_t{0}) {}

inline BlockHandle::BlockHandle(uint64_t _offset, uint64_t _size)
    : offset_(_offset), size_(_size) {}

}  // namespace ROCKSDB_NAMESPACE
