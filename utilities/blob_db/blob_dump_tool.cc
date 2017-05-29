//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/blob_db/blob_dump_tool.h"
#include <inttypes.h>
#include <stdio.h>
#include <iostream>
#include <memory>
#include <string>
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/string_util.h"

namespace rocksdb {
namespace blob_db {

BlobDumpTool::BlobDumpTool()
    : reader_(nullptr), buffer_(nullptr), buffer_size_(0) {}

Status BlobDumpTool::Run(const std::string& filename, DisplayType show_key,
                         DisplayType show_blob) {
  Status s;
  Env* env = Env::Default();
  s = env->FileExists(filename);
  if (!s.ok()) {
    return s;
  }
  uint64_t file_size = 0;
  s = env->GetFileSize(filename, &file_size);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<RandomAccessFile> file;
  s = env->NewRandomAccessFile(filename, &file, EnvOptions());
  if (!s.ok()) {
    return s;
  }
  if (file_size == 0) {
    return Status::Corruption("File is empty.");
  }
  reader_.reset(new RandomAccessFileReader(std::move(file)));
  uint64_t offset = 0;
  uint64_t footer_offset = 0;
  s = DumpBlobLogHeader(&offset);
  if (!s.ok()) {
    return s;
  }
  s = DumpBlobLogFooter(file_size, &footer_offset);
  if (!s.ok()) {
    return s;
  }
  if (show_key != DisplayType::kNone) {
    while (offset < footer_offset) {
      s = DumpRecord(show_key, show_blob, &offset);
      if (!s.ok()) {
        return s;
      }
    }
  }
  return s;
}

Status BlobDumpTool::Read(uint64_t offset, size_t size, Slice* result) {
  if (buffer_size_ < size) {
    if (buffer_size_ == 0) {
      buffer_size_ = 4096;
    }
    while (buffer_size_ < size) {
      buffer_size_ *= 2;
    }
    buffer_.reset(new char[buffer_size_]);
  }
  Status s = reader_->Read(offset, size, result, buffer_.get());
  if (!s.ok()) {
    return s;
  }
  if (result->size() != size) {
    return Status::Corruption("Reach the end of the file unexpectedly.");
  }
  return s;
}

Status BlobDumpTool::DumpBlobLogHeader(uint64_t* offset) {
  Slice slice;
  Status s = Read(0, BlobLogHeader::kHeaderSize, &slice);
  if (!s.ok()) {
    return s;
  }
  BlobLogHeader header;
  s = header.DecodeFrom(slice);
  if (!s.ok()) {
    return s;
  }
  fprintf(stdout, "Blob log header:\n");
  fprintf(stdout, "  Magic Number   : %u\n", header.magic_number());
  fprintf(stdout, "  Version        : %d\n", header.version());
  CompressionType compression = header.compression();
  std::string compression_str;
  if (!GetStringFromCompressionType(&compression_str, compression).ok()) {
    compression_str = "Unrecongnized compression type (" +
                      ToString((int)header.compression()) + ")";
  }
  fprintf(stdout, "  Compression    : %s\n", compression_str.c_str());
  fprintf(stdout, "  TTL Range      : %s\n",
          GetString(header.ttl_range()).c_str());
  fprintf(stdout, "  Timestamp Range: %s\n",
          GetString(header.ts_range()).c_str());
  *offset = BlobLogHeader::kHeaderSize;
  return s;
}

Status BlobDumpTool::DumpBlobLogFooter(uint64_t file_size,
                                       uint64_t* footer_offset) {
  auto no_footer = [&]() {
    *footer_offset = file_size;
    fprintf(stdout, "No blob log footer.\n");
    return Status::OK();
  };
  if (file_size < BlobLogHeader::kHeaderSize + BlobLogFooter::kFooterSize) {
    return no_footer();
  }
  Slice slice;
  Status s = Read(file_size - 4, 4, &slice);
  if (!s.ok()) {
    return s;
  }
  uint32_t magic_number = DecodeFixed32(slice.data());
  if (magic_number != kMagicNumber) {
    return no_footer();
  }
  *footer_offset = file_size - BlobLogFooter::kFooterSize;
  s = Read(*footer_offset, BlobLogFooter::kFooterSize, &slice);
  if (!s.ok()) {
    return s;
  }
  BlobLogFooter footer;
  s = footer.DecodeFrom(slice);
  if (!s.ok()) {
    return s;
  }
  fprintf(stdout, "Blob log footer:\n");
  fprintf(stdout, "  Blob count     : %" PRIu64 "\n", footer.GetBlobCount());
  fprintf(stdout, "  TTL Range      : %s\n",
          GetString(footer.GetTTLRange()).c_str());
  fprintf(stdout, "  Time Range     : %s\n",
          GetString(footer.GetTimeRange()).c_str());
  fprintf(stdout, "  Sequence Range : %s\n",
          GetString(footer.GetSNRange()).c_str());
  return s;
}

Status BlobDumpTool::DumpRecord(DisplayType show_key, DisplayType show_blob,
                                uint64_t* offset) {
  fprintf(stdout, "Read record with offset 0x%" PRIx64 " (%" PRIu64 "):\n",
          *offset, *offset);
  Slice slice;
  Status s = Read(*offset, BlobLogRecord::kHeaderSize, &slice);
  if (!s.ok()) {
    return s;
  }
  BlobLogRecord record;
  s = record.DecodeHeaderFrom(slice);
  if (!s.ok()) {
    return s;
  }
  uint32_t key_size = record.GetKeySize();
  uint64_t blob_size = record.GetBlobSize();
  fprintf(stdout, "  key size   : %d\n", key_size);
  fprintf(stdout, "  blob size  : %" PRIu64 "\n", record.GetBlobSize());
  fprintf(stdout, "  TTL        : %u\n", record.GetTTL());
  fprintf(stdout, "  time       : %" PRIu64 "\n", record.GetTimeVal());
  fprintf(stdout, "  type       : %d, %d\n", record.type(), record.subtype());
  fprintf(stdout, "  header CRC : %u\n", record.header_checksum());
  fprintf(stdout, "  CRC        : %u\n", record.checksum());
  uint32_t header_crc =
      crc32c::Extend(0, slice.data(), slice.size() - 2 * sizeof(uint32_t));
  *offset += BlobLogRecord::kHeaderSize;
  s = Read(*offset, key_size + blob_size + BlobLogRecord::kFooterSize, &slice);
  if (!s.ok()) {
    return s;
  }
  header_crc = crc32c::Extend(header_crc, slice.data(), key_size);
  header_crc = crc32c::Mask(header_crc);
  if (header_crc != record.header_checksum()) {
    return Status::Corruption("Record header checksum mismatch.");
  }
  uint32_t blob_crc = crc32c::Extend(0, slice.data() + key_size, blob_size);
  blob_crc = crc32c::Mask(blob_crc);
  if (blob_crc != record.checksum()) {
    return Status::Corruption("Blob checksum mismatch.");
  }
  if (show_key != DisplayType::kNone) {
    fprintf(stdout, "  key        : ");
    DumpSlice(Slice(slice.data(), key_size), show_key);
    if (show_blob != DisplayType::kNone) {
      fprintf(stdout, "  blob       : ");
      DumpSlice(Slice(slice.data() + key_size, blob_size), show_blob);
    }
  }
  Slice footer_slice(slice.data() + record.GetKeySize() + record.GetBlobSize(),
                     BlobLogRecord::kFooterSize);
  s = record.DecodeFooterFrom(footer_slice);
  if (!s.ok()) {
    return s;
  }
  fprintf(stdout, "  footer CRC : %u\n", record.footer_checksum());
  fprintf(stdout, "  sequence   : %" PRIu64 "\n", record.GetSN());
  *offset += key_size + blob_size + BlobLogRecord::kFooterSize;
  return s;
}

void BlobDumpTool::DumpSlice(const Slice s, DisplayType type) {
  if (type == DisplayType::kRaw) {
    fprintf(stdout, "%s\n", s.ToString().c_str());
  } else if (type == DisplayType::kHex) {
    fprintf(stdout, "%s\n", s.ToString(true /*hex*/).c_str());
  } else if (type == DisplayType::kDetail) {
    char buf[100];
    for (size_t i = 0; i < s.size(); i += 16) {
      memset(buf, 0, sizeof(buf));
      for (size_t j = 0; j < 16 && i + j < s.size(); j++) {
        unsigned char c = s[i + j];
        snprintf(buf + j * 3 + 15, 2, "%x", c >> 4);
        snprintf(buf + j * 3 + 16, 2, "%x", c & 0xf);
        snprintf(buf + j + 65, 2, "%c", (0x20 <= c && c <= 0x7e) ? c : '.');
      }
      for (size_t p = 0; p < sizeof(buf) - 1; p++) {
        if (buf[p] == 0) {
          buf[p] = ' ';
        }
      }
      fprintf(stdout, "%s\n", i == 0 ? buf + 15 : buf);
    }
  }
}

template <class T>
std::string BlobDumpTool::GetString(std::pair<T, T> p) {
  if (p.first == 0 && p.second == 0) {
    return "nil";
  }
  return "(" + ToString(p.first) + ", " + ToString(p.second) + ")";
}

}  // namespace blob_db
}  // namespace rocksdb

#endif  // ROCKSDB_LITE
