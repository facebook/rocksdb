//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>

#include "cache/cache_key.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/io_status.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct ReadOptions;
struct WriteOptions;
class RandomAccessFileReader;
class WritableFileWriter;
class Slice;
enum ChecksumType : char;

// "SimpleGen2Blob" is the second-generation on-disk format for a blob payload
// referenced by a same-file / context-relative blob index. A record is just the
// payload bytes followed by a small block-style trailer:
//
//   <payload bytes> <1-byte compression marker> <4-byte checksum>
//
// The checksum is a builtin block checksum over (payload + compression marker),
// context-modified by the record's absolute file offset (see
// ChecksumModifierForContext / ComputeBuiltinChecksumWithLastByte in
// table/format.h) so that an identical payload at a different offset gets a
// different stored checksum. Payloads are currently uncompressed only.
//
// This format is deliberately decoupled from any particular file type: the same
// record shape is read out of SST files (embedded blobs) and is intended to be
// reused by other blob-bearing files. The only file-type-specific inputs are
// the byte source (a RandomAccessFileReader) and the checksum context (checksum
// type + base context checksum), both passed in by the caller.
inline constexpr size_t kSimpleGen2BlobTrailerSize = 5;

// Cache key for the SimpleGen2Blob record at byte `record_offset` within a file
// whose base cache key is `base_cache_key`. Delegates to the shared
// OffsetableCacheKey::WithOffsetForMinSizeRecord scheme -- the exact same
// scheme block-based SST data blocks use via BlockBasedTable::GetCacheKey --
// which is valid because a SimpleGen2Blob record always carries a >= 5-byte
// trailer. As a result the cache key is collision-free with the file's data
// blocks even when the blob cache and block cache are the same cache, with no
// separately maintained algorithm to drift out of sync.
inline CacheKey GetSimpleGen2BlobCacheKey(
    const OffsetableCacheKey& base_cache_key, uint64_t record_offset) {
  return base_cache_key.WithOffsetForMinSizeRecord(record_offset);
}

// Reads a SimpleGen2Blob record of `record_size` bytes (which must equal
// `payload_size + kSimpleGen2BlobTrailerSize`) from `file` at `record_offset`
// into `buf` (capacity >= record_size) and verifies its compression marker and,
// when read_options.verify_checksums is set, its context-modified checksum.
//
// `checksum_type` and `base_context_checksum` are the file's checksum context
// (e.g. from the SST footer). `expected_compression` is the compression type
// the caller expects the record to carry (from the blob index); it must match
// the record's marker, and currently must be kNoCompression.
//
// On success, buf[0, payload_size) holds the verified, uncompressed payload
// (the trailer remains at the tail of `buf` and can be ignored).
Status ReadAndVerifySimpleGen2BlobRecord(
    const ReadOptions& read_options, RandomAccessFileReader* file,
    uint64_t record_offset, size_t payload_size, size_t record_size,
    ChecksumType checksum_type, uint32_t base_context_checksum,
    CompressionType expected_compression, char* buf);

// Writes a SimpleGen2Blob record for `payload` at the current end of `file`,
// which the caller asserts is byte offset `record_offset`. Appends the payload
// bytes followed by the 5-byte trailer (compression marker + context-modified
// builtin checksum), mirroring ReadAndVerifySimpleGen2BlobRecord so the on-disk
// record format lives in one module.
//
// `checksum_type` and `base_context_checksum` are the file's checksum context
// (e.g. from the SST footer). `compression` is the payload's compression type,
// which currently must be kNoCompression.
IOStatus WriteSimpleGen2BlobRecord(WritableFileWriter* file,
                                   const WriteOptions& write_options,
                                   ChecksumType checksum_type,
                                   uint32_t base_context_checksum,
                                   uint64_t record_offset, const Slice& payload,
                                   CompressionType compression);

}  // namespace ROCKSDB_NAMESPACE
