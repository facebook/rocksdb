//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <array>
#include <cstdint>
#include <string>

#include "file/file_prefetch_buffer.h"
#include "file/random_access_file_reader.h"
#include "memory/memory_allocator_impl.h"
#include "options/cf_options.h"
#include "port/malloc.h"
#include "port/port.h"  // noexcept
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

class RandomAccessFile;
struct ReadOptions;

bool ShouldReportDetailedTime(Env* env, Statistics* stats);

// the length of the magic number in bytes.
constexpr uint32_t kMagicNumberLengthByte = 8;

extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kBlockBasedTableMagicNumber;

extern const uint64_t kLegacyPlainTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;

extern const uint64_t kCuckooTableMagicNumber;

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
  char* EncodeTo(char* dst) const;
  Status DecodeFrom(Slice* input);
  Status DecodeSizeFrom(uint64_t offset, Slice* input);

  // Return a string that contains the copy of handle.
  std::string ToString(bool hex = true) const;

  // if the block handle's offset and size are both "0", we will view it
  // as a null block handle that points to no where.
  bool IsNull() const { return offset_ == 0 && size_ == 0; }

  static const BlockHandle& NullBlockHandle() { return kNullBlockHandle; }

  // Maximum encoding length of a BlockHandle
  static constexpr uint32_t kMaxEncodedLength = 2 * kMaxVarint64Length;

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

// Given a file's base_context_checksum and an offset of a block within that
// file, choose a 32-bit value that is as unique as possible. This value will
// be added to the standard checksum to get a checksum "with context," or can
// be subtracted to "remove" context. Returns zero (no modifier) if feature is
// disabled with base_context_checksum == 0.
inline uint32_t ChecksumModifierForContext(uint32_t base_context_checksum,
                                           uint64_t offset) {
  // To disable on base_context_checksum == 0, we could write
  // `if (base_context_checksum == 0) return 0;` but benchmarking shows
  // measurable performance penalty vs. this: compute the modifier
  // unconditionally and use an "all or nothing" bit mask to enable
  // or disable.
  uint32_t all_or_nothing = uint32_t{0} - (base_context_checksum != 0);

  // Desired properties:
  // (call this function f(b, o) where b = base and o = offset)
  // 1. Fast
  // 2. f(b1, o) == f(b2, o) iff b1 == b2
  //    (Perfectly preserve base entropy)
  // 3. f(b, o1) == f(b, o2) only if o1 == o2 or |o1-o2| >= 4 billion
  //    (Guaranteed uniqueness for nearby offsets)
  // 3. f(b, o + j * 2**32) == f(b, o + k * 2**32) only if j == k
  //    (Upper bits matter, and *aligned* misplacement fails check)
  // 4. f(b1, o) == f(b2, o + x) then preferably not
  //    f(b1, o + y) == f(b2, o + x + y)
  //    (Avoid linearly correlated matches)
  // 5. f(b, o) == 0 depends on both b and o
  //    (No predictable overlap with non-context checksums)
  uint32_t modifier =
      base_context_checksum ^ (Lower32of64(offset) + Upper32of64(offset));

  return modifier & all_or_nothing;
}

inline uint32_t GetCompressFormatForVersion(uint32_t format_version) {
  // As of format_version 2, we encode compressed block with
  // compress_format_version == 2. Before that, the version is 1.
  // DO NOT CHANGE THIS FUNCTION, it affects disk format
  return format_version >= 2 ? 2 : 1;
}

constexpr uint32_t kLatestFormatVersion = 6;

inline bool IsSupportedFormatVersion(uint32_t version) {
  return version <= kLatestFormatVersion;
}

// Same as having a unique id in footer.
inline bool FormatVersionUsesContextChecksum(uint32_t version) {
  return version >= 6;
}

inline bool FormatVersionUsesIndexHandleInFooter(uint32_t version) {
  return version < 6;
}

// Footer encapsulates the fixed information stored at the tail end of every
// SST file. In general, it should only include things that cannot go
// elsewhere under the metaindex block. For example, checksum_type is
// required for verifying metaindex block checksum (when applicable), but
// index block handle can easily go in metaindex block. See also FooterBuilder
// below.
class Footer {
 public:
  // Create empty. Populate using DecodeFrom.
  Footer() {}

  void Reset() {
    table_magic_number_ = kNullTableMagicNumber;
    format_version_ = kInvalidFormatVersion;
    base_context_checksum_ = 0;
    metaindex_handle_ = BlockHandle::NullBlockHandle();
    index_handle_ = BlockHandle::NullBlockHandle();
    checksum_type_ = kInvalidChecksumType;
    block_trailer_size_ = 0;
  }

  // Deserialize a footer (populate fields) from `input` and check for various
  // corruptions. `input_offset` is the offset within the target file of
  // `input` buffer, which is needed for verifying format_version >= 6 footer.
  // If enforce_table_magic_number != 0, will return corruption if table magic
  // number is not equal to enforce_table_magic_number.
  Status DecodeFrom(Slice input, uint64_t input_offset,
                    uint64_t enforce_table_magic_number = 0);

  // Table magic number identifies file as RocksDB SST file and which kind of
  // SST format is use.
  uint64_t table_magic_number() const { return table_magic_number_; }

  // A version (footer and more) within a kind of SST. (It would add more
  // unnecessary complexity to separate footer versions and
  // BBTO::format_version.)
  uint32_t format_version() const { return format_version_; }

  // See ChecksumModifierForContext()
  uint32_t base_context_checksum() const { return base_context_checksum_; }

  // Block handle for metaindex block.
  const BlockHandle& metaindex_handle() const { return metaindex_handle_; }

  // Block handle for (top-level) index block.
  // TODO? remove from this struct and only read on decode for legacy cases
  const BlockHandle& index_handle() const { return index_handle_; }

  // Checksum type used in the file, including footer for format version >= 6.
  ChecksumType checksum_type() const {
    return static_cast<ChecksumType>(checksum_type_);
  }

  // Block trailer size used by file with this footer (e.g. 5 for block-based
  // table and 0 for plain table). This is inferred from magic number so
  // not in the serialized form.
  inline size_t GetBlockTrailerSize() const { return block_trailer_size_; }

  // Convert this object to a human readable form
  std::string ToString() const;

  // Encoded lengths of Footers. Bytes for serialized Footer will always be
  // >= kMinEncodedLength and <= kMaxEncodedLength.
  //
  // Footer version 0 (legacy) will always occupy exactly this many bytes.
  // It consists of two block handles, padding, and a magic number.
  static constexpr uint32_t kVersion0EncodedLength =
      2 * BlockHandle::kMaxEncodedLength + kMagicNumberLengthByte;
  static constexpr uint32_t kMinEncodedLength = kVersion0EncodedLength;

  // Footer of versions 1 and higher will always occupy exactly this many
  // bytes. It originally consisted of the checksum type, two block handles,
  // padding (to maximum handle encoding size), a format version number, and a
  // magic number.
  static constexpr uint32_t kNewVersionsEncodedLength =
      1 + 2 * BlockHandle::kMaxEncodedLength + 4 + kMagicNumberLengthByte;
  static constexpr uint32_t kMaxEncodedLength = kNewVersionsEncodedLength;

  static constexpr uint64_t kNullTableMagicNumber = 0;

  static constexpr uint32_t kInvalidFormatVersion = 0xffffffffU;

 private:
  static constexpr int kInvalidChecksumType =
      (1 << (sizeof(ChecksumType) * 8)) | kNoChecksum;

  uint64_t table_magic_number_ = kNullTableMagicNumber;
  uint32_t format_version_ = kInvalidFormatVersion;
  uint32_t base_context_checksum_ = 0;
  BlockHandle metaindex_handle_;
  BlockHandle index_handle_;
  int checksum_type_ = kInvalidChecksumType;
  uint8_t block_trailer_size_ = 0;
};

// Builder for Footer
class FooterBuilder {
 public:
  // Run builder in inputs. This is a single step with lots of parameters for
  // efficiency (based on perf testing).
  // * table_magic_number identifies file as RocksDB SST file and which kind of
  // SST format is use.
  // * format_version is a version for the footer and can also apply to other
  // aspects of the SST file (see BlockBasedTableOptions::format_version).
  // NOTE: To save complexity in the caller, when format_version == 0 and
  // there is a corresponding legacy magic number to the one specified, the
  // legacy magic number will be written for forward compatibility.
  // * footer_offset is the file offset where the footer will be written
  // (for future use).
  // * checksum_type is for formats using block checksums.
  // * index_handle is optional for some SST kinds and (for caller convenience)
  // ignored when format_version >= 6. (Must be added to metaindex in that
  // case.)
  // * unique_id must be specified if format_vesion >= 6 and SST uses block
  // checksums with context. Otherwise, auto-generated if format_vesion >= 6.
  Status Build(uint64_t table_magic_number, uint32_t format_version,
               uint64_t footer_offset, ChecksumType checksum_type,
               const BlockHandle& metaindex_handle,
               const BlockHandle& index_handle = BlockHandle::NullBlockHandle(),
               uint32_t base_context_checksum = 0);

  // After Builder, get a Slice for the serialized Footer, backed by this
  // FooterBuilder.
  const Slice& GetSlice() const {
    assert(slice_.size());
    return slice_;
  }

 private:
  Slice slice_;
  std::array<char, Footer::kMaxEncodedLength> data_;
};

// Read the footer from file
// If enforce_table_magic_number != 0, ReadFooterFromFile() will return
// corruption if table_magic number is not equal to enforce_table_magic_number
Status ReadFooterFromFile(const IOOptions& opts, RandomAccessFileReader* file,
                          FileSystem& fs, FilePrefetchBuffer* prefetch_buffer,
                          uint64_t file_size, Footer* footer,
                          uint64_t enforce_table_magic_number = 0,
                          Statistics* stats = nullptr);

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
// into the mmapped region. Depending on context, it might be a serialized
// (potentially compressed) block, including a trailer beyond `size`, or an
// uncompressed block.
//
// Please try to use this terminology when dealing with blocks:
// * "Serialized block" - bytes that go into storage. For block-based table
// (usually the case) this includes the block trailer. Here the `size` does
// not include the trailer, but other places in code might include the trailer
// in the size.
// * "Maybe compressed block" - like a serialized block, but without the
// trailer (or no promise of including a trailer). Must be accompanied by a
// CompressionType in some other variable or field.
// * "Uncompressed block" - "payload" bytes that are either stored with no
// compression, used as input to compression function, or result of
// decompression function.
// * "Parsed block" - an in-memory form of a block in block cache, as it is
// used by the table reader. Different C++ types are used depending on the
// block type (see block_cache.h). Only trivially parsable block types
// use BlockContents as the parsed form.
//
struct BlockContents {
  // Points to block payload (without trailer)
  Slice data;
  CacheAllocationPtr allocation;

#ifndef NDEBUG
  // Whether there is a known trailer after what is pointed to by `data`.
  // See BlockBasedTable::GetCompressionType.
  bool has_trailer = false;
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

  BlockContents(BlockContents&& other) noexcept { *this = std::move(other); }

  BlockContents& operator=(BlockContents&& other) {
    data = std::move(other.data);
    allocation = std::move(other.allocation);
#ifndef NDEBUG
    has_trailer = other.has_trailer;
#endif  // NDEBUG
    return *this;
  }
};

// The `data` points to serialized block contents read in from file, which
// must be compressed and include a trailer beyond `size`. A new buffer is
// allocated with the given allocator (or default) and the uncompressed
// contents are returned in `out_contents`.
// format_version is as defined in include/rocksdb/table.h, which is
// used to determine compression format version.
Status UncompressSerializedBlock(const UncompressionInfo& info,
                                 const char* data, size_t size,
                                 BlockContents* out_contents,
                                 uint32_t format_version,
                                 const ImmutableOptions& ioptions,
                                 MemoryAllocator* allocator = nullptr);

// This is a variant of UncompressSerializedBlock that does not expect a
// block trailer beyond `size`. (CompressionType is taken from `info`.)
Status UncompressBlockData(const UncompressionInfo& info, const char* data,
                           size_t size, BlockContents* out_contents,
                           uint32_t format_version,
                           const ImmutableOptions& ioptions,
                           MemoryAllocator* allocator = nullptr);

// Replace db_host_id contents with the real hostname if necessary
Status ReifyDbHostIdProperty(Env* env, std::string* db_host_id);

// Implementation details follow.  Clients should ignore,

// TODO(andrewkr): we should prefer one way of representing a null/uninitialized
// BlockHandle. Currently we use zeros for null and use negation-of-zeros for
// uninitialized.
inline BlockHandle::BlockHandle() : BlockHandle(~uint64_t{0}, ~uint64_t{0}) {}

inline BlockHandle::BlockHandle(uint64_t _offset, uint64_t _size)
    : offset_(_offset), size_(_size) {}

}  // namespace ROCKSDB_NAMESPACE
