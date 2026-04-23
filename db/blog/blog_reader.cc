//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_reader.h"

#include <cassert>
#include <cstring>

#include "table/format.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

BlogFileReader::BlogFileReader(std::unique_ptr<SequentialFileReader>&& file,
                               Reporter* reporter, bool verify_checksums)
    : seq_file_(std::move(file)),
      reporter_(reporter),
      verify_checksums_(verify_checksums) {}

Status BlogFileReader::ReadHeader(BlogFileHeader* header) {
  assert(!header_read_);

  // Read the fixed header to get property_section_size
  std::string fixed_buf;
  Slice fixed_slice;
  Status s = ReadBytes(kBlogFileFixedHeaderSize, &fixed_slice, &fixed_buf);
  if (!s.ok()) {
    return s;
  }

  // Peek at property_section_size from bytes [32-35]
  uint32_t prop_section_size = DecodeFixed32(fixed_slice.data() + 32);

  // Read the property section
  std::string prop_buf;
  Slice prop_slice;
  s = ReadBytes(prop_section_size, &prop_slice, &prop_buf);
  if (!s.ok()) {
    return s;
  }

  // Combine into a single buffer for decoding
  std::string full_header;
  full_header.append(fixed_slice.data(), fixed_slice.size());
  full_header.append(prop_slice.data(), prop_slice.size());

  Slice input(full_header);
  s = header->DecodeFrom(&input);
  if (!s.ok()) {
    return s;
  }

  header_ = *header;
  header_read_ = true;

  // Skip padding after header to reach the first record's escape sequence.
  // Padding may be arbitrarily long (for stronger alignment or recycled
  // files of all zeros). EOF here is fine (header-only file, no records).
  s = SkipPadding();
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  return Status::OK();
}

Status BlogFileReader::ReadRecord(BlogRecordType* type, Slice* payload,
                                  std::string* scratch, uint64_t* record_offset,
                                  CompressionType* record_compression_type) {
  if (!header_read_) {
    return Status::Corruption("Blog reader: header not read");
  }
  if (eof_) {
    return Status::NotFound();
  }

  Status s;
  scratch->clear();

  // Read escape sequence. If we have a pending prefix from header padding
  // skip or inter-record padding skip, use it as the first 4 bytes.
  std::string esc_buf;
  if (!pending_escape_prefix_.empty()) {
    esc_buf = std::move(pending_escape_prefix_);
    pending_escape_prefix_.clear();
    Slice rest_slice;
    std::string rest_buf;
    s = ReadBytes(kBlogEscapeSequenceSize - 4, &rest_slice, &rest_buf);
    if (!s.ok()) {
      eof_ = true;
      // Short read after we had a pending prefix = incomplete trailing record
      return Status::Incomplete("Blog reader: incomplete escape sequence");
    }
    esc_buf.append(rest_slice.data(), rest_slice.size());
  } else {
    Slice esc_slice;
    s = ReadBytes(kBlogEscapeSequenceSize, &esc_slice, &esc_buf);
    if (!s.ok()) {
      eof_ = true;
      return Status::NotFound();  // clean EOF
    }
    if (esc_slice.data() != esc_buf.data()) {
      esc_buf.assign(esc_slice.data(), esc_slice.size());
    }
  }

  // Verify escape sequence matches
  if (memcmp(esc_buf.data(), header_.escape_sequence,
             kBlogEscapeSequenceSize) != 0) {
    return ReportCorruption(kBlogEscapeSequenceSize,
                            "escape sequence mismatch");
  }

  uint64_t rec_offset = offset_ - kBlogEscapeSequenceSize;
  if (record_offset) {
    *record_offset = rec_offset;
  }
  last_record_offset_ = rec_offset;

  // Read varint length. scratch receives the raw varint bytes for use
  // in prefix checksum verification (avoids fragile re-encoding).
  uint64_t length = 0;
  s = ReadVarint64(&length, scratch);
  if (!s.ok()) {
    eof_ = true;
    return Status::Incomplete("Blog reader: incomplete varint");
  }
  size_t varint_len = scratch->size();

  bool is_full_format = (varint_len > kBlogCompactVarintMaxBytes);

  BlogRecordType rec_type;
  std::string prefix_rest_buf;

  if (is_full_format) {
    // Full format: read type + compression_type + prefix_checksum
    Slice prefix_rest_slice;
    s = ReadBytes(6, &prefix_rest_slice, &prefix_rest_buf);  // 1+1+4
    if (!s.ok()) {
      eof_ = true;
      return Status::Incomplete("Blog reader: incomplete full record prefix");
    }

    rec_type =
        static_cast<BlogRecordType>(static_cast<uint8_t>(prefix_rest_slice[0]));

    if (verify_checksums_) {
      // Build the prefix data from the original varint bytes (in scratch)
      // plus type and compression_type bytes.
      scratch->push_back(prefix_rest_slice[0]);  // type
      scratch->push_back(prefix_rest_slice[1]);  // comp_type

      uint32_t stored_prefix_checksum =
          DecodeFixed32(prefix_rest_slice.data() + 2);
      uint32_t computed_prefix_checksum =
          ComputeBuiltinChecksum(header_.checksum_type, scratch->data(),
                                 scratch->size()) +
          ChecksumModifierForContext(header_.incarnation_id(), rec_offset);
      if (stored_prefix_checksum != computed_prefix_checksum) {
        return ReportCorruption(varint_len + 6, "prefix checksum mismatch");
      }
    }
  } else {
    rec_type = header_.compact_record_type;
  }

  *type = rec_type;

  if (length == 0) {
    payload->clear();
    if (record_compression_type) {
      *record_compression_type = kNoCompression;
    }
    s = SkipPadding();
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    return Status::OK();
  }

  if (length == kBlogUnspecifiedSize) {
    return ReportCorruption(0, "unspecified-size records not yet implemented");
  }

  // Read payload
  scratch->clear();
  Slice payload_slice;
  s = ReadBytes(static_cast<size_t>(length), &payload_slice, scratch);
  if (!s.ok()) {
    eof_ = true;
    return Status::Incomplete("Blog reader: incomplete payload");
  }
  if (payload_slice.data() != scratch->data()) {
    scratch->assign(payload_slice.data(), payload_slice.size());
  }

  // Read 5-byte trailer
  Slice trailer_slice;
  std::string trailer_buf;
  s = ReadBytes(kBlogBlockTrailerSize, &trailer_slice, &trailer_buf);
  if (!s.ok()) {
    eof_ = true;
    return Status::Incomplete("Blog reader: incomplete trailer");
  }

  char trailer_comp_type = trailer_slice[0];
  uint32_t stored_checksum = DecodeFixed32(trailer_slice.data() + 1);

  if (verify_checksums_) {
    uint32_t computed_checksum = ComputeBlogRecordChecksum(
        header_.checksum_type, scratch->data(), scratch->size(),
        trailer_comp_type, header_.incarnation_id(),
        offset_ - kBlogBlockTrailerSize - length);
    if (stored_checksum != computed_checksum) {
      return ReportCorruption(
          static_cast<size_t>(length) + kBlogBlockTrailerSize,
          "record checksum mismatch");
    }
  }

  *payload = Slice(*scratch);
  if (record_compression_type) {
    *record_compression_type =
        static_cast<CompressionType>(static_cast<uint8_t>(trailer_comp_type));
  }

  // Skip padding to next record.
  s = SkipPadding();
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  return Status::OK();
}

Status BlogFileReader::ScanForEscapeSequence(uint64_t* found_offset) {
  // Scan forward 4 bytes at a time (escape sequences are 32-bit aligned).
  // Check first 4 bytes of escape sequence, then verify remaining 6 bytes.
  uint32_t first4;
  memcpy(&first4, header_.escape_sequence, 4);

  std::string buf;
  // Align to 4-byte boundary first
  size_t remainder = offset_ % 4;
  if (remainder != 0) {
    size_t skip = 4 - remainder;
    Slice skip_slice;
    Status s = ReadBytes(skip, &skip_slice, &buf);
    if (!s.ok()) {
      return s;
    }
  }

  while (!eof_) {
    Slice chunk_slice;
    Status s = ReadBytes(4, &chunk_slice, &buf);
    if (!s.ok()) {
      if (eof_) {
        return Status::NotFound("escape sequence not found before EOF");
      }
      return s;
    }

    uint32_t candidate;
    memcpy(&candidate, chunk_slice.data(), 4);
    if (candidate == first4) {
      // Potential match - verify remaining 6 bytes
      uint64_t candidate_offset = offset_ - 4;
      Slice rest_slice;
      std::string rest_buf;
      s = ReadBytes(kBlogEscapeSequenceSize - 4, &rest_slice, &rest_buf);
      if (!s.ok()) {
        return s;
      }

      if (memcmp(rest_slice.data(), header_.escape_sequence + 4,
                 kBlogEscapeSequenceSize - 4) == 0) {
        *found_offset = candidate_offset;
        return Status::OK();
      }
      // False match, continue scanning. But we consumed 6 extra bytes that
      // might not be aligned. Re-align.
      remainder = offset_ % 4;
      if (remainder != 0) {
        size_t skip = 4 - remainder;
        Slice skip_s;
        s = ReadBytes(skip, &skip_s, &buf);
        if (!s.ok()) {
          return s;
        }
      }
    }
  }
  return Status::NotFound("escape sequence not found before EOF");
}

Status BlogFileReader::SkipPadding() {
  // Skip arbitrarily long padding (0x00 or 0xFF bytes). Padding is
  // 32-bit aligned. Since the escape sequence's first byte is guaranteed
  // to be neither 0x00 nor 0xFF, we align to 4 bytes then scan 4-byte
  // chunks until we find one that starts with a non-padding byte.
  size_t remainder = offset_ % 4;
  if (remainder != 0) {
    Slice skip_slice;
    std::string skip_buf;
    Status s = ReadBytes(4 - remainder, &skip_slice, &skip_buf);
    if (!s.ok()) {
      if (eof_) {
        return Status::NotFound();
      }
      return s;
    }
  }
  while (true) {
    Slice chunk_slice;
    std::string chunk_buf;
    Status s = ReadBytes(4, &chunk_slice, &chunk_buf);
    if (!s.ok()) {
      if (eof_) {
        return Status::NotFound();
      }
      return s;
    }
    uint8_t first = static_cast<uint8_t>(chunk_slice[0]);
    if (first != 0x00 && first != 0xFF) {
      // Found the start of the next escape sequence.
      pending_escape_prefix_.assign(chunk_slice.data(), 4);
      return Status::OK();
    }
  }
}

Status BlogFileReader::ReadBytes(size_t n, Slice* result,
                                 std::string* scratch) {
  scratch->resize(n);
  Slice read_result;
  IOStatus ios =
      seq_file_->Read(n, &read_result, scratch->data(), Env::IO_TOTAL);
  if (!ios.ok()) {
    return static_cast<Status>(ios);
  }
  if (read_result.size() < n) {
    eof_ = true;
    // Return Incomplete -- the caller (ReadRecord) determines whether
    // this is an acceptable incomplete tail vs. genuine corruption.
    return Status::Incomplete("Blog reader", "unexpected EOF");
  }
  offset_ += n;
  *result = Slice(scratch->data(), n);
  return Status::OK();
}

// FIXME? Consider a better encoding that could be processed more efficiently
Status BlogFileReader::ReadVarint64(uint64_t* value, std::string* scratch) {
  // Varints can be up to 10 bytes. Read one byte at a time.
  // The raw varint bytes are appended to scratch so the caller can use
  // them for checksum verification without re-encoding.
  scratch->clear();
  std::string byte_buf;
  for (int i = 0; i < 10; ++i) {
    Slice byte_slice;
    Status s = ReadBytes(1, &byte_slice, &byte_buf);
    if (!s.ok()) {
      return s;
    }
    scratch->push_back(byte_slice[0]);
    // Check if this is the terminal byte (high bit not set)
    if ((static_cast<uint8_t>(byte_slice[0]) & 0x80) == 0) {
      Slice varint_slice(*scratch);
      if (!GetVarint64(&varint_slice, value)) {
        return ReportCorruption(scratch->size(), "invalid varint");
      }
      return Status::OK();
    }
  }
  return ReportCorruption(10, "varint too long");
}

Status BlogFileReader::ReportCorruption(size_t bytes, const std::string& msg) {
  Status s = Status::Corruption("Blog reader", msg);
  if (reporter_) {
    reporter_->Corruption(bytes, s);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
