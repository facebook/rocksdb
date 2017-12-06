//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
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
  reader_.reset(new RandomAccessFileReader(std::move(file), filename));
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
  Status s = Read(0, BlobLogHeader::kSize, &slice);
  if (!s.ok()) {
    return s;
  }
  BlobLogHeader header;
  s = header.DecodeFrom(slice);
  if (!s.ok()) {
    return s;
  }
  fprintf(stdout, "Blob log header:\n");
  fprintf(stdout, "  Version          : %" PRIu32 "\n", header.version);
  fprintf(stdout, "  Column Family ID : %" PRIu32 "\n",
          header.column_family_id);
  std::string compression_str;
  if (!GetStringFromCompressionType(&compression_str, header.compression)
           .ok()) {
    compression_str = "Unrecongnized compression type (" +
                      ToString((int)header.compression) + ")";
  }
  fprintf(stdout, "  Compression      : %s\n", compression_str.c_str());
  fprintf(stdout, "  Expiration range : %s\n",
          GetString(header.expiration_range).c_str());
  *offset = BlobLogHeader::kSize;
  return s;
}

Status BlobDumpTool::DumpBlobLogFooter(uint64_t file_size,
                                       uint64_t* footer_offset) {
  auto no_footer = [&]() {
    *footer_offset = file_size;
    fprintf(stdout, "No blob log footer.\n");
    return Status::OK();
  };
  if (file_size < BlobLogHeader::kSize + BlobLogFooter::kSize) {
    return no_footer();
  }
  Slice slice;
  *footer_offset = file_size - BlobLogFooter::kSize;
  Status s = Read(*footer_offset, BlobLogFooter::kSize, &slice);
  if (!s.ok()) {
    return s;
  }
  BlobLogFooter footer;
  s = footer.DecodeFrom(slice);
  if (!s.ok()) {
    return s;
  }
  fprintf(stdout, "Blob log footer:\n");
  fprintf(stdout, "  Blob count       : %" PRIu64 "\n", footer.blob_count);
  fprintf(stdout, "  Expiration Range : %s\n",
          GetString(footer.expiration_range).c_str());
  fprintf(stdout, "  Sequence Range   : %s\n",
          GetString(footer.sequence_range).c_str());
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
  uint64_t key_size = record.key_size;
  uint64_t value_size = record.value_size;
  fprintf(stdout, "  key size   : %" PRIu64 "\n", key_size);
  fprintf(stdout, "  value size : %" PRIu64 "\n", value_size);
  fprintf(stdout, "  expiration : %" PRIu64 "\n", record.expiration);
  *offset += BlobLogRecord::kHeaderSize;
  s = Read(*offset, key_size + value_size, &slice);
  if (!s.ok()) {
    return s;
  }
  if (show_key != DisplayType::kNone) {
    fprintf(stdout, "  key        : ");
    DumpSlice(Slice(slice.data(), key_size), show_key);
    if (show_blob != DisplayType::kNone) {
      fprintf(stdout, "  blob       : ");
      DumpSlice(Slice(slice.data() + key_size, value_size), show_blob);
    }
  }
  *offset += key_size + value_size;
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
