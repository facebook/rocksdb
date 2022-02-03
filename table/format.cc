//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include <cinttypes>
#include <string>

#include "block_fetcher.h"
#include "file/random_access_file_reader.h"
#include "memory/memory_allocator.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "options/options_helper.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/persistent_cache_helper.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/hash.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {

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
const char* kHostnameForDbHostId = "__hostname__";

bool ShouldReportDetailedTime(Env* env, Statistics* stats) {
  return env != nullptr && stats != nullptr &&
         stats->get_stats_level() > kExceptDetailedTimers;
}

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~uint64_t{0});
  assert(size_ != ~uint64_t{0});
  PutVarint64Varint64(dst, offset_, size_);
}

char* BlockHandle::EncodeTo(char* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~uint64_t{0});
  assert(size_ != ~uint64_t{0});
  char* cur = EncodeVarint64(dst, offset_);
  cur = EncodeVarint64(cur, size_);
  return cur;
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    // reset in case failure after partially decoding
    offset_ = 0;
    size_ = 0;
    return Status::Corruption("bad block handle");
  }
}

Status BlockHandle::DecodeSizeFrom(uint64_t _offset, Slice* input) {
  if (GetVarint64(input, &size_)) {
    offset_ = _offset;
    return Status::OK();
  } else {
    // reset in case failure after partially decoding
    offset_ = 0;
    size_ = 0;
    return Status::Corruption("bad block handle");
  }
}

// Return a string that contains the copy of handle.
std::string BlockHandle::ToString(bool hex) const {
  std::string handle_str;
  EncodeTo(&handle_str);
  if (hex) {
    return Slice(handle_str).ToString(true);
  } else {
    return handle_str;
  }
}

const BlockHandle BlockHandle::kNullBlockHandle(0, 0);

void IndexValue::EncodeTo(std::string* dst, bool have_first_key,
                          const BlockHandle* previous_handle) const {
  if (previous_handle) {
    // WART: this is specific to Block-based table
    assert(handle.offset() == previous_handle->offset() +
                                  previous_handle->size() +
                                  BlockBasedTable::kBlockTrailerSize);
    PutVarsignedint64(dst, handle.size() - previous_handle->size());
  } else {
    handle.EncodeTo(dst);
  }
  assert(dst->size() != 0);

  if (have_first_key) {
    PutLengthPrefixedSlice(dst, first_internal_key);
  }
}

Status IndexValue::DecodeFrom(Slice* input, bool have_first_key,
                              const BlockHandle* previous_handle) {
  if (previous_handle) {
    int64_t delta;
    if (!GetVarsignedint64(input, &delta)) {
      return Status::Corruption("bad delta-encoded index value");
    }
    // WART: this is specific to Block-based table
    handle = BlockHandle(previous_handle->offset() + previous_handle->size() +
                             BlockBasedTable::kBlockTrailerSize,
                         previous_handle->size() + delta);
  } else {
    Status s = handle.DecodeFrom(input);
    if (!s.ok()) {
      return s;
    }
  }

  if (!have_first_key) {
    first_internal_key = Slice();
  } else if (!GetLengthPrefixedSlice(input, &first_internal_key)) {
    return Status::Corruption("bad first key in block info");
  }

  return Status::OK();
}

std::string IndexValue::ToString(bool hex, bool have_first_key) const {
  std::string s;
  EncodeTo(&s, have_first_key, nullptr);
  if (hex) {
    return Slice(s).ToString(true);
  } else {
    return s;
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
  return magic_number;
}
inline uint64_t DownconvertToLegacyFooterFormat(uint64_t magic_number) {
  if (magic_number == kBlockBasedTableMagicNumber) {
    return kLegacyBlockBasedTableMagicNumber;
  }
  if (magic_number == kPlainTableMagicNumber) {
    return kLegacyPlainTableMagicNumber;
  }
  assert(false);
  return magic_number;
}
inline uint8_t BlockTrailerSizeForMagicNumber(uint64_t magic_number) {
  if (magic_number == kBlockBasedTableMagicNumber ||
      magic_number == kLegacyBlockBasedTableMagicNumber) {
    return static_cast<uint8_t>(BlockBasedTable::kBlockTrailerSize);
  } else {
    return 0;
  }
}

// Footer format, in three parts:
// * Part1
//   -> format_version == 0 (inferred from legacy magic number)
//      <empty> (0 bytes)
//   -> format_version >= 1
//      checksum type (char, 1 byte)
// * Part2
//      metaindex handle (varint64 offset, varint64 size)
//      index handle     (varint64 offset, varint64 size)
//      <zero padding> for part2 size = 2 * BlockHandle::kMaxEncodedLength = 40
// * Part3
//   -> format_version == 0 (inferred from legacy magic number)
//      legacy magic number (8 bytes)
//   -> format_version >= 1 (inferred from NOT legacy magic number)
//      format_version (uint32LE, 4 bytes), also called "footer version"
//      newer magic number (8 bytes)

constexpr size_t kFooterPart2Size = 2 * BlockHandle::kMaxEncodedLength;
}  // namespace

void FooterBuilder::Build(uint64_t magic_number, uint32_t format_version,
                          uint64_t footer_offset, ChecksumType checksum_type,
                          const BlockHandle& metaindex_handle,
                          const BlockHandle& index_handle) {
  (void)footer_offset;  // Future use

  assert(magic_number != Footer::kNullTableMagicNumber);
  assert(IsSupportedFormatVersion(format_version));

  char* part2;
  char* part3;
  if (format_version > 0) {
    slice_ = Slice(data_.data(), Footer::kNewVersionsEncodedLength);
    // Generate parts 1 and 3
    char* cur = data_.data();
    // Part 1
    *(cur++) = checksum_type;
    // Part 2
    part2 = cur;
    // Skip over part 2 for now
    cur += kFooterPart2Size;
    // Part 3
    part3 = cur;
    EncodeFixed32(cur, format_version);
    cur += 4;
    EncodeFixed64(cur, magic_number);
    assert(cur + 8 == slice_.data() + slice_.size());
  } else {
    slice_ = Slice(data_.data(), Footer::kVersion0EncodedLength);
    // Legacy SST files use kCRC32c checksum but it's not stored in footer.
    assert(checksum_type == kNoChecksum || checksum_type == kCRC32c);
    // Generate part 3 (part 1 empty, skip part 2 for now)
    part2 = data_.data();
    part3 = part2 + kFooterPart2Size;
    char* cur = part3;
    // Use legacy magic numbers to indicate format_version=0, for
    // compatibility. No other cases should use format_version=0.
    EncodeFixed64(cur, DownconvertToLegacyFooterFormat(magic_number));
    assert(cur + 8 == slice_.data() + slice_.size());
  }

  {
    char* cur = part2;
    cur = metaindex_handle.EncodeTo(cur);
    cur = index_handle.EncodeTo(cur);
    // Zero pad remainder
    std::fill(cur, part3, char{0});
  }
}

Status Footer::DecodeFrom(Slice input, uint64_t input_offset) {
  (void)input_offset;  // Future use

  // Only decode to unused Footer
  assert(table_magic_number_ == kNullTableMagicNumber);
  assert(input != nullptr);
  assert(input.size() >= kMinEncodedLength);

  const char* magic_ptr = input.data() + input.size() - kMagicNumberLengthByte;
  uint64_t magic = DecodeFixed64(magic_ptr);

  // We check for legacy formats here and silently upconvert them
  bool legacy = IsLegacyFooterFormat(magic);
  if (legacy) {
    magic = UpconvertLegacyFooterFormat(magic);
  }
  table_magic_number_ = magic;
  block_trailer_size_ = BlockTrailerSizeForMagicNumber(magic);

  // Parse Part3
  if (legacy) {
    // The size is already asserted to be at least kMinEncodedLength
    // at the beginning of the function
    input.remove_prefix(input.size() - kVersion0EncodedLength);
    format_version_ = 0 /* legacy */;
    checksum_type_ = kCRC32c;
  } else {
    const char* part3_ptr = magic_ptr - 4;
    format_version_ = DecodeFixed32(part3_ptr);
    if (!IsSupportedFormatVersion(format_version_)) {
      return Status::Corruption("Corrupt or unsupported format_version: " +
                                ROCKSDB_NAMESPACE::ToString(format_version_));
    }
    // All known format versions >= 1 occupy exactly this many bytes.
    if (input.size() < kNewVersionsEncodedLength) {
      return Status::Corruption("Input is too short to be an SST file");
    }
    uint64_t adjustment = input.size() - kNewVersionsEncodedLength;
    input.remove_prefix(adjustment);

    // Parse Part1
    char chksum = input.data()[0];
    checksum_type_ = lossless_cast<ChecksumType>(chksum);
    if (!IsSupportedChecksumType(checksum_type())) {
      return Status::Corruption(
          "Corrupt or unsupported checksum type: " +
          ROCKSDB_NAMESPACE::ToString(lossless_cast<uint8_t>(chksum)));
    }
    // Consume checksum type field
    input.remove_prefix(1);
  }

  // Parse Part2
  Status result = metaindex_handle_.DecodeFrom(&input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(&input);
  }
  return result;
  // Padding in part2 is ignored
}

std::string Footer::ToString() const {
  std::string result;
  result.reserve(1024);

  bool legacy = IsLegacyFooterFormat(table_magic_number_);
  if (legacy) {
    result.append("metaindex handle: " + metaindex_handle_.ToString() + "\n  ");
    result.append("index handle: " + index_handle_.ToString() + "\n  ");
    result.append("table_magic_number: " +
                  ROCKSDB_NAMESPACE::ToString(table_magic_number_) + "\n  ");
  } else {
    result.append("metaindex handle: " + metaindex_handle_.ToString() + "\n  ");
    result.append("index handle: " + index_handle_.ToString() + "\n  ");
    result.append("table_magic_number: " +
                  ROCKSDB_NAMESPACE::ToString(table_magic_number_) + "\n  ");
    result.append("format version: " +
                  ROCKSDB_NAMESPACE::ToString(format_version_) + "\n  ");
  }
  return result;
}

Status ReadFooterFromFile(const IOOptions& opts, RandomAccessFileReader* file,
                          FilePrefetchBuffer* prefetch_buffer,
                          uint64_t file_size, Footer* footer,
                          uint64_t enforce_table_magic_number) {
  if (file_size < Footer::kMinEncodedLength) {
    return Status::Corruption("file is too short (" + ToString(file_size) +
                              " bytes) to be an "
                              "sstable: " +
                              file->file_name());
  }

  std::string footer_buf;
  AlignedBuf internal_buf;
  Slice footer_input;
  uint64_t read_offset = (file_size > Footer::kMaxEncodedLength)
                             ? file_size - Footer::kMaxEncodedLength
                             : 0;
  Status s;
  // TODO: Need to pass appropriate deadline to TryReadFromCache(). Right now,
  // there is no readahead for point lookups, so TryReadFromCache will fail if
  // the required data is not in the prefetch buffer. Once deadline is enabled
  // for iterator, TryReadFromCache might do a readahead. Revisit to see if we
  // need to pass a timeout at that point
  // TODO: rate limit footer reads.
  if (prefetch_buffer == nullptr ||
      !prefetch_buffer->TryReadFromCache(
          IOOptions(), file, read_offset, Footer::kMaxEncodedLength,
          &footer_input, nullptr, Env::IO_TOTAL /* rate_limiter_priority */)) {
    if (file->use_direct_io()) {
      s = file->Read(opts, read_offset, Footer::kMaxEncodedLength,
                     &footer_input, nullptr, &internal_buf,
                     Env::IO_TOTAL /* rate_limiter_priority */);
    } else {
      footer_buf.reserve(Footer::kMaxEncodedLength);
      s = file->Read(opts, read_offset, Footer::kMaxEncodedLength,
                     &footer_input, &footer_buf[0], nullptr,
                     Env::IO_TOTAL /* rate_limiter_priority */);
    }
    if (!s.ok()) return s;
  }

  // Check that we actually read the whole footer from the file. It may be
  // that size isn't correct.
  if (footer_input.size() < Footer::kMinEncodedLength) {
    return Status::Corruption("file is too short (" + ToString(file_size) +
                              " bytes) to be an "
                              "sstable" +
                              file->file_name());
  }

  s = footer->DecodeFrom(footer_input, read_offset);
  if (!s.ok()) {
    return s;
  }
  if (enforce_table_magic_number != 0 &&
      enforce_table_magic_number != footer->table_magic_number()) {
    return Status::Corruption(
        "Bad table magic number: expected " +
        ToString(enforce_table_magic_number) + ", found " +
        ToString(footer->table_magic_number()) + " in " + file->file_name());
  }
  return Status::OK();
}

namespace {
// Custom handling for the last byte of a block, to avoid invoking streaming
// API to get an effective block checksum. This function is its own inverse
// because it uses xor.
inline uint32_t ModifyChecksumForLastByte(uint32_t checksum, char last_byte) {
  // This strategy bears some resemblance to extending a CRC checksum by one
  // more byte, except we don't need to re-mix the input checksum as long as
  // we do this step only once (per checksum).
  const uint32_t kRandomPrime = 0x6b9083d9;
  return checksum ^ lossless_cast<uint8_t>(last_byte) * kRandomPrime;
}
}  // namespace

uint32_t ComputeBuiltinChecksum(ChecksumType type, const char* data,
                                size_t data_size) {
  switch (type) {
    case kCRC32c:
      return crc32c::Mask(crc32c::Value(data, data_size));
    case kxxHash:
      return XXH32(data, data_size, /*seed*/ 0);
    case kxxHash64:
      return Lower32of64(XXH64(data, data_size, /*seed*/ 0));
    case kXXH3: {
      if (data_size == 0) {
        // Special case because of special handling for last byte, not
        // present in this case. Can be any value different from other
        // small input size checksums.
        return 0;
      } else {
        // See corresponding code in ComputeBuiltinChecksumWithLastByte
        uint32_t v = Lower32of64(XXH3_64bits(data, data_size - 1));
        return ModifyChecksumForLastByte(v, data[data_size - 1]);
      }
    }
    default:  // including kNoChecksum
      return 0;
  }
}

uint32_t ComputeBuiltinChecksumWithLastByte(ChecksumType type, const char* data,
                                            size_t data_size, char last_byte) {
  switch (type) {
    case kCRC32c: {
      uint32_t crc = crc32c::Value(data, data_size);
      // Extend to cover last byte (compression type)
      crc = crc32c::Extend(crc, &last_byte, 1);
      return crc32c::Mask(crc);
    }
    case kxxHash: {
      XXH32_state_t* const state = XXH32_createState();
      XXH32_reset(state, 0);
      XXH32_update(state, data, data_size);
      // Extend to cover last byte (compression type)
      XXH32_update(state, &last_byte, 1);
      uint32_t v = XXH32_digest(state);
      XXH32_freeState(state);
      return v;
    }
    case kxxHash64: {
      XXH64_state_t* const state = XXH64_createState();
      XXH64_reset(state, 0);
      XXH64_update(state, data, data_size);
      // Extend to cover last byte (compression type)
      XXH64_update(state, &last_byte, 1);
      uint32_t v = Lower32of64(XXH64_digest(state));
      XXH64_freeState(state);
      return v;
    }
    case kXXH3: {
      // XXH3 is a complicated hash function that is extremely fast on
      // contiguous input, but that makes its streaming support rather
      // complex. It is worth custom handling of the last byte (`type`)
      // in order to avoid allocating a large state object and bringing
      // that code complexity into CPU working set.
      uint32_t v = Lower32of64(XXH3_64bits(data, data_size));
      return ModifyChecksumForLastByte(v, last_byte);
    }
    default:  // including kNoChecksum
      return 0;
  }
}

Status UncompressBlockContentsForCompressionType(
    const UncompressionInfo& uncompression_info, const char* data, size_t n,
    BlockContents* contents, uint32_t format_version,
    const ImmutableOptions& ioptions, MemoryAllocator* allocator) {
  Status ret = Status::OK();

  assert(uncompression_info.type() != kNoCompression &&
         "Invalid compression type");

  StopWatchNano timer(ioptions.clock,
                      ShouldReportDetailedTime(ioptions.env, ioptions.stats));
  size_t uncompressed_size = 0;
  CacheAllocationPtr ubuf =
      UncompressData(uncompression_info, data, n, &uncompressed_size,
                     GetCompressFormatForVersion(format_version), allocator);
  if (!ubuf) {
    if (!CompressionTypeSupported(uncompression_info.type())) {
      return Status::NotSupported(
          "Unsupported compression method for this build",
          CompressionTypeToString(uncompression_info.type()));
    } else {
      return Status::Corruption(
          "Corrupted compressed block contents",
          CompressionTypeToString(uncompression_info.type()));
    }
  }

  *contents = BlockContents(std::move(ubuf), uncompressed_size);

  if (ShouldReportDetailedTime(ioptions.env, ioptions.stats)) {
    RecordTimeToHistogram(ioptions.stats, DECOMPRESSION_TIMES_NANOS,
                          timer.ElapsedNanos());
  }
  RecordTimeToHistogram(ioptions.stats, BYTES_DECOMPRESSED,
                        contents->data.size());
  RecordTick(ioptions.stats, NUMBER_BLOCK_DECOMPRESSED);

  TEST_SYNC_POINT_CALLBACK(
      "UncompressBlockContentsForCompressionType:TamperWithReturnValue",
      static_cast<void*>(&ret));
  TEST_SYNC_POINT_CALLBACK(
      "UncompressBlockContentsForCompressionType:"
      "TamperWithDecompressionOutput",
      static_cast<void*>(contents));

  return ret;
}

//
// The 'data' points to the raw block contents that was read in from file.
// This method allocates a new heap buffer and the raw block
// contents are uncompresed into this buffer. This
// buffer is returned via 'result' and it is upto the caller to
// free this buffer.
// format_version is the block format as defined in include/rocksdb/table.h
Status UncompressBlockContents(const UncompressionInfo& uncompression_info,
                               const char* data, size_t n,
                               BlockContents* contents, uint32_t format_version,
                               const ImmutableOptions& ioptions,
                               MemoryAllocator* allocator) {
  assert(data[n] != kNoCompression);
  assert(data[n] == static_cast<char>(uncompression_info.type()));
  return UncompressBlockContentsForCompressionType(uncompression_info, data, n,
                                                   contents, format_version,
                                                   ioptions, allocator);
}

// Replace the contents of db_host_id with the actual hostname, if db_host_id
// matches the keyword kHostnameForDbHostId
Status ReifyDbHostIdProperty(Env* env, std::string* db_host_id) {
  assert(db_host_id);
  if (*db_host_id == kHostnameForDbHostId) {
    Status s = env->GetHostNameString(db_host_id);
    if (!s.ok()) {
      db_host_id->clear();
    }
    return s;
  }

  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
