//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include <string>
#include <inttypes.h>

#include "port/port.h"
#include "rocksdb/env.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/perf_context_imp.h"
#include "util/xxhash.h"

namespace rocksdb {

extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kBlockBasedTableMagicNumber;

#ifndef ROCKSDB_LITE
extern const uint64_t kLegacyPlainTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;
#else
// ROCKSDB_LITE doesn't have plain table
const uint64_t kLegacyPlainTableMagicNumber = 0;
const uint64_t kPlainTableMagicNumber = 0;
#endif
const uint32_t DefaultStackBufferSize = 5000;

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) &&
      GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}
const BlockHandle BlockHandle::kNullBlockHandle(0, 0);

// legacy footer format:
//    metaindex handle (varint64 offset, varint64 size)
//    index handle     (varint64 offset, varint64 size)
//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength
//    table_magic_number (8 bytes)
// new footer format:
//    checksum (char, 1 byte)
//    metaindex handle (varint64 offset, varint64 size)
//    index handle     (varint64 offset, varint64 size)
//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength + 1
//    footer version (4 bytes)
//    table_magic_number (8 bytes)
void Footer::EncodeTo(std::string* dst) const {
  if (version() == kLegacyFooter) {
    // has to be default checksum with legacy footer
    assert(checksum_ == kCRC32c);
    const size_t original_size = dst->size();
    metaindex_handle_.EncodeTo(dst);
    index_handle_.EncodeTo(dst);
    dst->resize(original_size + 2 * BlockHandle::kMaxEncodedLength);  // Padding
    PutFixed32(dst, static_cast<uint32_t>(table_magic_number() & 0xffffffffu));
    PutFixed32(dst, static_cast<uint32_t>(table_magic_number() >> 32));
    assert(dst->size() == original_size + kVersion0EncodedLength);
  } else {
    const size_t original_size = dst->size();
    dst->push_back(static_cast<char>(checksum_));
    metaindex_handle_.EncodeTo(dst);
    index_handle_.EncodeTo(dst);
    dst->resize(original_size + kVersion1EncodedLength - 12);  // Padding
    PutFixed32(dst, kFooterVersion);
    PutFixed32(dst, static_cast<uint32_t>(table_magic_number() & 0xffffffffu));
    PutFixed32(dst, static_cast<uint32_t>(table_magic_number() >> 32));
    assert(dst->size() == original_size + kVersion1EncodedLength);
  }
}

namespace {
inline bool IsLegacyFooterFormat(uint64_t magic_number) {
  return magic_number == kLegacyBlockBasedTableMagicNumber ||
         magic_number == kLegacyPlainTableMagicNumber;
}

inline uint64_t UpconvertLegacyFooterFormat(uint64_t magic_number) {
  if (magic_number == kLegacyBlockBasedTableMagicNumber) {
    return kBlockBasedTableMagicNumber;
  }
  if (magic_number == kLegacyPlainTableMagicNumber) {
    return kPlainTableMagicNumber;
  }
  assert(false);
  return 0;
}
}  // namespace

Footer::Footer(uint64_t table_magic_number)
    : version_(IsLegacyFooterFormat(table_magic_number) ? kLegacyFooter
                                                        : kFooterVersion),
      checksum_(kCRC32c),
      table_magic_number_(table_magic_number) {}

Status Footer::DecodeFrom(Slice* input) {
  assert(input != nullptr);
  assert(input->size() >= kMinEncodedLength);

  const char *magic_ptr =
      input->data() + input->size() - kMagicNumberLengthByte;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                    (static_cast<uint64_t>(magic_lo)));

  // We check for legacy formats here and silently upconvert them
  bool legacy = IsLegacyFooterFormat(magic);
  if (legacy) {
    magic = UpconvertLegacyFooterFormat(magic);
  }
  if (HasInitializedTableMagicNumber()) {
    if (magic != table_magic_number()) {
      char buffer[80];
      snprintf(buffer, sizeof(buffer) - 1,
               "not an sstable (bad magic number --- %lx)",
               (long)magic);
      return Status::InvalidArgument(buffer);
    }
  } else {
    set_table_magic_number(magic);
  }

  if (legacy) {
    // The size is already asserted to be at least kMinEncodedLength
    // at the beginning of the function
    input->remove_prefix(input->size() - kVersion0EncodedLength);
    version_ = kLegacyFooter;
    checksum_ = kCRC32c;
  } else {
    version_ = DecodeFixed32(magic_ptr - 4);
    if (version_ != kFooterVersion) {
      return Status::Corruption("bad footer version");
    }
    // Footer version 1 will always occupy exactly this many bytes.
    // It consists of the checksum type, two block handles, padding,
    // a version number, and a magic number
    if (input->size() < kVersion1EncodedLength) {
      return Status::InvalidArgument("input is too short to be an sstable");
    } else {
      input->remove_prefix(input->size() - kVersion1EncodedLength);
    }
    uint32_t checksum;
    if (!GetVarint32(input, &checksum)) {
      return Status::Corruption("bad checksum type");
    }
    checksum_ = static_cast<ChecksumType>(checksum);
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + kMagicNumberLengthByte;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

Status ReadFooterFromFile(RandomAccessFile* file,
                          uint64_t file_size,
                          Footer* footer) {
  if (file_size < Footer::kMinEncodedLength) {
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  char footer_space[Footer::kMaxEncodedLength];
  Slice footer_input;
  size_t read_offset = (file_size > Footer::kMaxEncodedLength)
                           ? (file_size - Footer::kMaxEncodedLength)
                           : 0;
  Status s = file->Read(read_offset, Footer::kMaxEncodedLength, &footer_input,
                        footer_space);
  if (!s.ok()) return s;

  // Check that we actually read the whole footer from the file. It may be
  // that size isn't correct.
  if (footer_input.size() < Footer::kMinEncodedLength) {
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  return footer->DecodeFrom(&footer_input);
}

// Read a block and check its CRC
// contents is the result of reading.
// According to the implementation of file->Read, contents may not point to buf
Status ReadBlock(RandomAccessFile* file, const Footer& footer,
                  const ReadOptions& options, const BlockHandle& handle,
                  Slice* contents,  /* result of reading */ char* buf) {
  size_t n = static_cast<size_t>(handle.size());
  Status s;

  {
    PERF_TIMER_GUARD(block_read_time);
    s = file->Read(handle.offset(), n + kBlockTrailerSize, contents, buf);
  }

  PERF_COUNTER_ADD(block_read_count, 1);
  PERF_COUNTER_ADD(block_read_byte, n + kBlockTrailerSize);

  if (!s.ok()) {
    return s;
  }
  if (contents->size() != n + kBlockTrailerSize) {
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents->data();  // Pointer to where Read put the data
  if (options.verify_checksums) {
    PERF_TIMER_GUARD(block_checksum_time);
    uint32_t value = DecodeFixed32(data + n + 1);
    uint32_t actual = 0;
    switch (footer.checksum()) {
      case kCRC32c:
        value = crc32c::Unmask(value);
        actual = crc32c::Value(data, n + 1);
        break;
      case kxxHash:
        actual = XXH32(data, n + 1, 0);
        break;
      default:
        s = Status::Corruption("unknown checksum type");
    }
    if (s.ok() && actual != value) {
      s = Status::Corruption("block checksum mismatch");
    }
    if (!s.ok()) {
      return s;
    }
  }
  return s;
}

// Decompress a block according to params
// May need to malloc a space for cache usage
Status DecompressBlock(BlockContents* result, size_t block_size,
                          bool do_uncompress, const char* buf,
                          const Slice& contents, bool use_stack_buf) {
  Status s;
  size_t n = block_size;
  const char* data = contents.data();

  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  PERF_TIMER_GUARD(block_decompress_time);
  rocksdb::CompressionType compression_type =
      static_cast<rocksdb::CompressionType>(data[n]);
  // If the caller has requested that the block not be uncompressed
  if (!do_uncompress || compression_type == kNoCompression) {
    if (data != buf) {
      // File implementation gave us pointer to some other data.
      // Use it directly under the assumption that it will be live
      // while the file is open.
      result->data = Slice(data, n);
      result->heap_allocated = false;
      result->cachable = false;  // Do not double-cache
    } else {
      if (use_stack_buf) {
        // Need to allocate space in heap for cache usage
        char* new_buf = new char[n];
        memcpy(new_buf, buf, n);
        result->data = Slice(new_buf, n);
      } else {
        result->data = Slice(buf, n);
      }

      result->heap_allocated = true;
      result->cachable = true;
    }
    result->compression_type = compression_type;
    s = Status::OK();
  } else {
    s = UncompressBlockContents(data, n, result);
  }
  return s;
}

// Read and Decompress block
// Use buf in stack as temp reading buffer
Status ReadAndDecompressFast(RandomAccessFile* file, const Footer& footer,
                             const ReadOptions& options,
                             const BlockHandle& handle, BlockContents* result,
                             Env* env, bool do_uncompress) {
  Status s;
  Slice contents;
  size_t n = static_cast<size_t>(handle.size());
  char buf[DefaultStackBufferSize];

  s = ReadBlock(file, footer, options, handle, &contents, buf);
  if (!s.ok()) {
    return s;
  }
  s = DecompressBlock(result, n, do_uncompress, buf, contents, true);
  if (!s.ok()) {
    return s;
  }
  return s;
}

// Read and Decompress block
// Use buf in heap as temp reading buffer
Status ReadAndDecompress(RandomAccessFile* file, const Footer& footer,
                         const ReadOptions& options, const BlockHandle& handle,
                         BlockContents* result, Env* env, bool do_uncompress) {
  Status s;
  Slice contents;
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kBlockTrailerSize];

  s = ReadBlock(file, footer, options, handle, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  s = DecompressBlock(result, n, do_uncompress, buf, contents, false);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }

  if (result->data.data() != buf) {
    delete[] buf;
  }
  return s;
}

Status ReadBlockContents(RandomAccessFile* file, const Footer& footer,
                         const ReadOptions& options, const BlockHandle& handle,
                         BlockContents* result, Env* env, bool do_uncompress) {
  size_t n = static_cast<size_t>(handle.size());
  if (do_uncompress && n + kBlockTrailerSize < DefaultStackBufferSize) {
    return ReadAndDecompressFast(file, footer, options, handle, result, env,
                                 do_uncompress);
  } else {
    return ReadAndDecompress(file, footer, options, handle, result, env,
                             do_uncompress);
  }
}

//
// The 'data' points to the raw block contents that was read in from file.
// This method allocates a new heap buffer and the raw block
// contents are uncompresed into this buffer. This
// buffer is returned via 'result' and it is upto the caller to
// free this buffer.
Status UncompressBlockContents(const char* data, size_t n,
                               BlockContents* result) {
  char* ubuf = nullptr;
  int decompress_size = 0;
  assert(data[n] != kNoCompression);
  switch (data[n]) {
    case kSnappyCompression: {
      size_t ulength = 0;
      static char snappy_corrupt_msg[] =
        "Snappy not supported or corrupted Snappy compressed block contents";
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        return Status::Corruption(snappy_corrupt_msg);
      }
      ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] ubuf;
        return Status::Corruption(snappy_corrupt_msg);
      }
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    case kZlibCompression:
      ubuf = port::Zlib_Uncompress(data, n, &decompress_size);
      static char zlib_corrupt_msg[] =
        "Zlib not supported or corrupted Zlib compressed block contents";
      if (!ubuf) {
        return Status::Corruption(zlib_corrupt_msg);
      }
      result->data = Slice(ubuf, decompress_size);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    case kBZip2Compression:
      ubuf = port::BZip2_Uncompress(data, n, &decompress_size);
      static char bzip2_corrupt_msg[] =
        "Bzip2 not supported or corrupted Bzip2 compressed block contents";
      if (!ubuf) {
        return Status::Corruption(bzip2_corrupt_msg);
      }
      result->data = Slice(ubuf, decompress_size);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    case kLZ4Compression:
      ubuf = port::LZ4_Uncompress(data, n, &decompress_size);
      static char lz4_corrupt_msg[] =
          "LZ4 not supported or corrupted LZ4 compressed block contents";
      if (!ubuf) {
        return Status::Corruption(lz4_corrupt_msg);
      }
      result->data = Slice(ubuf, decompress_size);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    case kLZ4HCCompression:
      ubuf = port::LZ4_Uncompress(data, n, &decompress_size);
      static char lz4hc_corrupt_msg[] =
          "LZ4HC not supported or corrupted LZ4HC compressed block contents";
      if (!ubuf) {
        return Status::Corruption(lz4hc_corrupt_msg);
      }
      result->data = Slice(ubuf, decompress_size);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    default:
      return Status::Corruption("bad block type");
  }
  result->compression_type = kNoCompression;  // not compressed any more
  return Status::OK();
}

}  // namespace rocksdb
