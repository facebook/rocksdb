//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_gen2_format.h"

#include <cstring>
#include <string>

#include "file/random_access_file_reader.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "table/format.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

Status ReadAndVerifySimpleGen2BlobRecord(
    const ReadOptions& read_options, RandomAccessFileReader* file,
    uint64_t record_offset, size_t payload_size, size_t record_size,
    ChecksumType checksum_type, uint32_t base_context_checksum,
    CompressionType expected_compression, char* buf) {
  assert(file != nullptr);
  assert(buf != nullptr);
  assert(record_size == payload_size + kSimpleGen2BlobTrailerSize);

  Slice result;
  IOOptions opts;
  IODebugContext dbg;
  Status s = file->PrepareIOOptions(read_options, opts, &dbg);
  if (s.ok()) {
    s = file->Read(opts, record_offset, record_size, &result, buf, nullptr,
                   &dbg);
  }
  if (!s.ok()) {
    return s;
  }
  if (result.size() != record_size) {
    return Status::Corruption("Could not read complete blob record");
  }
  // With mmap reads the data lands outside `buf`; copy it in so the caller can
  // rely on `buf` owning the bytes (this is the only copy on the mmap path).
  // TODO: fix this extra memcpy in the mmap case
  if (result.data() != buf) {
    memcpy(buf, result.data(), record_size);
  }

  const char* record = buf;
  const CompressionType compression =
      static_cast<CompressionType>(record[payload_size]);
  if (compression != expected_compression) {
    return Status::Corruption(
        "Blob record compression does not match blob index");
  }

  if (read_options.verify_checksums) {
    uint32_t stored = DecodeFixed32(record + payload_size + 1);
    stored -= ChecksumModifierForContext(base_context_checksum, record_offset);
    const uint32_t computed = ComputeBuiltinChecksumWithLastByte(
        checksum_type, record, payload_size, record[payload_size]);
    if (stored != computed) {
      return Status::Corruption("Blob record checksum mismatch in " +
                                file->file_name() + " offset " +
                                std::to_string(record_offset) + " size " +
                                std::to_string(payload_size));
    }
  }

  if (compression != kNoCompression) {
    return Status::Corruption("Blob record compression is not supported");
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
