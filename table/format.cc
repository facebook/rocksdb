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
#include <cstdint>
#include <string>

#include "block_fetcher.h"
#include "file/random_access_file_reader.h"
#include "memory/memory_allocator_impl.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "options/options_helper.h"
#include "port/likely.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/persistent_cache_helper.h"
#include "unique_id_impl.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/hash.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {

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
//   -> format_version <= 5
//      metaindex handle (varint64 offset, varint64 size)
//      index handle     (varint64 offset, varint64 size)
//      <zero padding> for part2 size = 2 * BlockHandle::kMaxEncodedLength = 40
//        - This padding is unchecked/ignored
//   -> format_version >= 6
//      extended magic number (4 bytes) = 0x3e 0x00 0x7a 0x00
//        - Also surely invalid (size 0) handles if interpreted as older version
//        - (Helps ensure a corrupted format_version doesn't get us far with no
//           footer checksum.)
//      footer_checksum (uint32LE, 4 bytes)
//        - Checksum of above checksum type of whole footer, with this field
//          set to all zeros.
//      base_context_checksum (uint32LE, 4 bytes)
//      metaindex block size (uint32LE, 4 bytes)
//        - Assumed to be immediately before footer, < 4GB
//      <zero padding> (24 bytes, reserved for future use)
//        - Brings part2 size also to 40 bytes
//        - Checked that last eight bytes == 0, so reserved for a future
//          incompatible feature (but under format_version=6)
// * Part3
//   -> format_version == 0 (inferred from legacy magic number)
//      legacy magic number (8 bytes)
//   -> format_version >= 1 (inferred from NOT legacy magic number)
//      format_version (uint32LE, 4 bytes), also called "footer version"
//      newer magic number (8 bytes)
const std::array<char, 4> kExtendedMagic{{0x3e, 0x00, 0x7a, 0x00}};
constexpr size_t kFooterPart2Size = 2 * BlockHandle::kMaxEncodedLength;
}  // namespace

Status FooterBuilder::Build(uint64_t magic_number, uint32_t format_version,
                            uint64_t footer_offset, ChecksumType checksum_type,
                            const BlockHandle& metaindex_handle,
                            const BlockHandle& index_handle,
                            uint32_t base_context_checksum) {
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

  if (format_version >= 6) {
    if (BlockTrailerSizeForMagicNumber(magic_number) != 0) {
      // base context checksum required for table formats with block checksums
      assert(base_context_checksum != 0);
      assert(ChecksumModifierForContext(base_context_checksum, 0) != 0);
    } else {
      // base context checksum not used
      assert(base_context_checksum == 0);
      assert(ChecksumModifierForContext(base_context_checksum, 0) == 0);
    }

    // Start populating Part 2
    char* cur = data_.data() + /* part 1 size */ 1;
    // Set extended magic of part2
    std::copy(kExtendedMagic.begin(), kExtendedMagic.end(), cur);
    cur += kExtendedMagic.size();
    // Fill checksum data with zeros (for later computing checksum)
    char* checksum_data = cur;
    EncodeFixed32(cur, 0);
    cur += 4;
    // Save base context checksum
    EncodeFixed32(cur, base_context_checksum);
    cur += 4;
    // Compute and save metaindex size
    uint32_t metaindex_size = static_cast<uint32_t>(metaindex_handle.size());
    if (metaindex_size != metaindex_handle.size()) {
      return Status::NotSupported("Metaindex block size > 4GB");
    }
    // Metaindex must be adjacent to footer
    assert(metaindex_size == 0 ||
           metaindex_handle.offset() + metaindex_handle.size() ==
               footer_offset - BlockTrailerSizeForMagicNumber(magic_number));
    EncodeFixed32(cur, metaindex_size);
    cur += 4;

    // Zero pad remainder (for future use)
    std::fill_n(cur, 24U, char{0});
    assert(cur + 24 == part3);

    // Compute checksum, add context
    uint32_t checksum = ComputeBuiltinChecksum(
        checksum_type, data_.data(), Footer::kNewVersionsEncodedLength);
    checksum +=
        ChecksumModifierForContext(base_context_checksum, footer_offset);
    // Store it
    EncodeFixed32(checksum_data, checksum);
  } else {
    // Base context checksum not used
    assert(!FormatVersionUsesContextChecksum(format_version));
    // Should be left empty
    assert(base_context_checksum == 0);
    assert(ChecksumModifierForContext(base_context_checksum, 0) == 0);

    // Populate all of part 2
    char* cur = part2;
    cur = metaindex_handle.EncodeTo(cur);
    cur = index_handle.EncodeTo(cur);
    // Zero pad remainder
    std::fill(cur, part3, char{0});
  }
  return Status::OK();
}

Status Footer::DecodeFrom(Slice input, uint64_t input_offset,
                          uint64_t enforce_table_magic_number) {
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
  if (enforce_table_magic_number != 0 && enforce_table_magic_number != magic) {
    return Status::Corruption("Bad table magic number: expected " +
                              std::to_string(enforce_table_magic_number) +
                              ", found " + std::to_string(magic));
  }
  table_magic_number_ = magic;
  block_trailer_size_ = BlockTrailerSizeForMagicNumber(magic);

  // Parse Part3
  const char* part3_ptr = magic_ptr;
  uint32_t computed_checksum = 0;
  uint64_t footer_offset = 0;
  if (legacy) {
    // The size is already asserted to be at least kMinEncodedLength
    // at the beginning of the function
    input.remove_prefix(input.size() - kVersion0EncodedLength);
    format_version_ = 0 /* legacy */;
    checksum_type_ = kCRC32c;
  } else {
    part3_ptr = magic_ptr - 4;
    format_version_ = DecodeFixed32(part3_ptr);
    if (UNLIKELY(!IsSupportedFormatVersion(format_version_))) {
      return Status::Corruption("Corrupt or unsupported format_version: " +
                                std::to_string(format_version_));
    }
    // All known format versions >= 1 occupy exactly this many bytes.
    if (UNLIKELY(input.size() < kNewVersionsEncodedLength)) {
      return Status::Corruption("Input is too short to be an SST file");
    }
    uint64_t adjustment = input.size() - kNewVersionsEncodedLength;
    input.remove_prefix(adjustment);
    footer_offset = input_offset + adjustment;

    // Parse Part1
    char chksum = input.data()[0];
    checksum_type_ = lossless_cast<ChecksumType>(chksum);
    if (UNLIKELY(!IsSupportedChecksumType(checksum_type()))) {
      return Status::Corruption("Corrupt or unsupported checksum type: " +
                                std::to_string(lossless_cast<uint8_t>(chksum)));
    }
    // This is the most convenient place to compute the checksum
    if (checksum_type_ != kNoChecksum && format_version_ >= 6) {
      std::array<char, kNewVersionsEncodedLength> copy_without_checksum;
      std::copy_n(input.data(), kNewVersionsEncodedLength,
                  copy_without_checksum.data());
      EncodeFixed32(&copy_without_checksum[5], 0);  // Clear embedded checksum
      computed_checksum =
          ComputeBuiltinChecksum(checksum_type(), copy_without_checksum.data(),
                                 kNewVersionsEncodedLength);
    }
    // Consume checksum type field
    input.remove_prefix(1);
  }

  // Parse Part2
  if (format_version_ >= 6) {
    Slice ext_magic(input.data(), 4);
    if (UNLIKELY(ext_magic.compare(Slice(kExtendedMagic.data(),
                                         kExtendedMagic.size())) != 0)) {
      return Status::Corruption("Bad extended magic number: 0x" +
                                ext_magic.ToString(/*hex*/ true));
    }
    input.remove_prefix(4);
    uint32_t stored_checksum = 0, metaindex_size = 0;
    bool success;
    success = GetFixed32(&input, &stored_checksum);
    assert(success);
    success = GetFixed32(&input, &base_context_checksum_);
    assert(success);
    if (UNLIKELY(ChecksumModifierForContext(base_context_checksum_, 0) == 0)) {
      return Status::Corruption("Invalid base context checksum");
    }
    computed_checksum +=
        ChecksumModifierForContext(base_context_checksum_, footer_offset);
    if (UNLIKELY(computed_checksum != stored_checksum)) {
      return Status::Corruption("Footer at " + std::to_string(footer_offset) +
                                " checksum mismatch");
    }
    success = GetFixed32(&input, &metaindex_size);
    assert(success);
    (void)success;
    uint64_t metaindex_end = footer_offset - GetBlockTrailerSize();
    metaindex_handle_ =
        BlockHandle(metaindex_end - metaindex_size, metaindex_size);

    // Mark unpopulated
    index_handle_ = BlockHandle::NullBlockHandle();

    // 16 bytes of unchecked reserved padding
    input.remove_prefix(16U);

    // 8 bytes of checked reserved padding (expected to be zero unless using a
    // future feature).
    uint64_t reserved = 0;
    success = GetFixed64(&input, &reserved);
    assert(success);
    if (UNLIKELY(reserved != 0)) {
      return Status::NotSupported(
          "File uses a future feature not supported in this version");
    }
    // End of part 2
    assert(input.data() == part3_ptr);
  } else {
    // format_version_ < 6
    Status result = metaindex_handle_.DecodeFrom(&input);
    if (result.ok()) {
      result = index_handle_.DecodeFrom(&input);
    }
    if (!result.ok()) {
      return result;
    }
    // Padding in part2 is ignored
  }
  return Status::OK();
}

std::string Footer::ToString() const {
  std::string result;
  result.reserve(1024);

  result.append("metaindex handle: " + metaindex_handle_.ToString() +
                " offset: " + std::to_string(metaindex_handle_.offset()) +
                " size: " + std::to_string(metaindex_handle_.size()) + "\n  ");
  result.append("index handle: " + index_handle_.ToString() +
                " offset: " + std::to_string(index_handle_.offset()) +
                " size: " + std::to_string(index_handle_.size()) + "\n  ");
  result.append("table_magic_number: " + std::to_string(table_magic_number_) +
                "\n  ");
  if (!IsLegacyFooterFormat(table_magic_number_)) {
    result.append("format version: " + std::to_string(format_version_) + "\n");
  }
  return result;
}

static Status ReadFooterFromFileInternal(const IOOptions& opts,
                                         RandomAccessFileReader* file,
                                         FileSystem& fs,
                                         FilePrefetchBuffer* prefetch_buffer,
                                         uint64_t file_size, Footer* footer,
                                         uint64_t enforce_table_magic_number) {
  if (file_size < Footer::kMinEncodedLength) {
    return Status::Corruption("file is too short (" +
                              std::to_string(file_size) +
                              " bytes) to be an "
                              "sstable: " +
                              file->file_name());
  }

  std::array<char, Footer::kMaxEncodedLength + 1> footer_buf;
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
      !prefetch_buffer->TryReadFromCache(opts, file, read_offset,
                                         Footer::kMaxEncodedLength,
                                         &footer_input, nullptr)) {
    if (file->use_direct_io()) {
      s = file->Read(opts, read_offset, Footer::kMaxEncodedLength,
                     &footer_input, nullptr, &internal_buf);
    } else {
      s = file->Read(opts, read_offset, Footer::kMaxEncodedLength,
                     &footer_input, footer_buf.data(), nullptr);
    }
    if (!s.ok()) {
      return s;
    }
  }

  TEST_SYNC_POINT_CALLBACK("ReadFooterFromFileInternal:0", &footer_input);

  // Check that we actually read the whole footer from the file. It may be
  // that size isn't correct.
  if (footer_input.size() < Footer::kMinEncodedLength) {
    uint64_t size_on_disk = 0;
    if (fs.GetFileSize(file->file_name(), IOOptions(), &size_on_disk, nullptr)
            .ok()) {
      // Similar to CheckConsistency message, but not completely sure the
      // expected size always came from manifest.
      return Status::Corruption("Sst file size mismatch: " + file->file_name() +
                                ". Expected " + std::to_string(file_size) +
                                ", actual size " +
                                std::to_string(size_on_disk) + "\n");
    } else {
      return Status::Corruption(
          "Missing SST footer data in file " + file->file_name() +
          " File too short? Expected size: " + std::to_string(file_size));
    }
  }

  s = footer->DecodeFrom(footer_input, read_offset, enforce_table_magic_number);
  if (!s.ok()) {
    s = Status::CopyAppendMessage(s, " in ", file->file_name());
    return s;
  }
  return Status::OK();
}

Status ReadFooterFromFile(const IOOptions& opts, RandomAccessFileReader* file,
                          FileSystem& fs, FilePrefetchBuffer* prefetch_buffer,
                          uint64_t file_size, Footer* footer,
                          uint64_t enforce_table_magic_number,
                          Statistics* stats) {
  Status s =
      ReadFooterFromFileInternal(opts, file, fs, prefetch_buffer, file_size,
                                 footer, enforce_table_magic_number);
  if (s.IsCorruption() &&
      CheckFSFeatureSupport(&fs, FSSupportedOps::kVerifyAndReconstructRead)) {
    IOOptions new_opts = opts;
    new_opts.verify_and_reconstruct_read = true;
    footer->Reset();
    s = ReadFooterFromFileInternal(new_opts, file, fs, prefetch_buffer,
                                   file_size, footer,
                                   enforce_table_magic_number);
    RecordTick(stats, FILE_READ_CORRUPTION_RETRY_COUNT);
    if (s.ok()) {
      RecordTick(stats, FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT);
    }
  }
  return s;
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

Status UncompressBlockData(const UncompressionInfo& uncompression_info,
                           const char* data, size_t size,
                           BlockContents* out_contents, uint32_t format_version,
                           const ImmutableOptions& ioptions,
                           MemoryAllocator* allocator) {
  Status ret = Status::OK();

  assert(uncompression_info.type() != kNoCompression &&
         "Invalid compression type");

  StopWatchNano timer(ioptions.clock,
                      ShouldReportDetailedTime(ioptions.env, ioptions.stats));
  size_t uncompressed_size = 0;
  const char* error_msg = nullptr;
  CacheAllocationPtr ubuf = UncompressData(
      uncompression_info, data, size, &uncompressed_size,
      GetCompressFormatForVersion(format_version), allocator, &error_msg);
  if (!ubuf) {
    if (!CompressionTypeSupported(uncompression_info.type())) {
      ret = Status::NotSupported(
          "Unsupported compression method for this build",
          CompressionTypeToString(uncompression_info.type()));
    } else {
      std::ostringstream oss;
      oss << "Corrupted compressed block contents";
      if (error_msg) {
        oss << ": " << error_msg;
      }
      ret = Status::Corruption(
          oss.str(), CompressionTypeToString(uncompression_info.type()));
    }
    return ret;
  }

  *out_contents = BlockContents(std::move(ubuf), uncompressed_size);

  if (ShouldReportDetailedTime(ioptions.env, ioptions.stats)) {
    RecordTimeToHistogram(ioptions.stats, DECOMPRESSION_TIMES_NANOS,
                          timer.ElapsedNanos());
  }
  RecordTick(ioptions.stats, BYTES_DECOMPRESSED_FROM, size);
  RecordTick(ioptions.stats, BYTES_DECOMPRESSED_TO, out_contents->data.size());
  RecordTick(ioptions.stats, NUMBER_BLOCK_DECOMPRESSED);

  TEST_SYNC_POINT_CALLBACK("UncompressBlockData:TamperWithReturnValue",
                           static_cast<void*>(&ret));
  TEST_SYNC_POINT_CALLBACK(
      "UncompressBlockData:"
      "TamperWithDecompressionOutput",
      static_cast<void*>(out_contents));

  return ret;
}

Status UncompressSerializedBlock(const UncompressionInfo& uncompression_info,
                                 const char* data, size_t size,
                                 BlockContents* out_contents,
                                 uint32_t format_version,
                                 const ImmutableOptions& ioptions,
                                 MemoryAllocator* allocator) {
  assert(data[size] != kNoCompression);
  assert(data[size] == static_cast<char>(uncompression_info.type()));
  return UncompressBlockData(uncompression_info, data, size, out_contents,
                             format_version, ioptions, allocator);
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
