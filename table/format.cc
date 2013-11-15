//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "port/port.h"
#include "rocksdb/env.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/perf_context_imp.h"

namespace rocksdb {

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

void Footer::EncodeTo(std::string* dst) const {
#ifndef NDEBUG
  const size_t original_size = dst->size();
#endif
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
}

Status Footer::DecodeFrom(Slice* input) {
  assert(input != nullptr);
  assert(input->size() >= kEncodedLength);

  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::InvalidArgument("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

Status ReadBlockContents(RandomAccessFile* file,
                         const ReadOptions& options,
                         const BlockHandle& handle,
                         BlockContents* result,
                         Env* env,
                         bool do_uncompress) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kBlockTrailerSize];
  Slice contents;

  StopWatchNano timer(env);
  StartPerfTimer(&timer);
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  BumpPerfCount(&perf_context.block_read_count);
  BumpPerfCount(&perf_context.block_read_byte, n + kBlockTrailerSize);
  BumpPerfTime(&perf_context.block_read_time, &timer);

  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents.data();    // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
    BumpPerfTime(&perf_context.block_checksum_time, &timer);
  }

  // If the caller has requested that the block not be uncompressed
  if (!do_uncompress || data[n] == kNoCompression) {
    if (data != buf) {
      // File implementation gave us pointer to some other data.
      // Use it directly under the assumption that it will be live
      // while the file is open.
      delete[] buf;
      result->data = Slice(data, n);
      result->heap_allocated = false;
      result->cachable = false;  // Do not double-cache
    } else {
      result->data = Slice(buf, n);
      result->heap_allocated = true;
      result->cachable = true;
    }
    result->compression_type = (rocksdb::CompressionType)data[n];
    s =  Status::OK();
  } else {
    s = UncompressBlockContents(data, n, result);
    delete[] buf;
  }
  BumpPerfTime(&perf_context.block_decompress_time, &timer);
  return s;
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
    default:
      return Status::Corruption("bad block type");
  }
  result->compression_type = kNoCompression; // not compressed any more
  return Status::OK();
}

}  // namespace rocksdb
