//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
#ifndef ROCKSDB_LITE
#include <inttypes.h>
#include <iostream>
#include <memory>
#include <string>
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "util/filename.h"
#include "util/file_reader_writer.h"
#include "util/string_util.h"
#include "utilities/blob_db/blob_dump_tool.h"

namespace rocksdb {
namespace blob_db {

int BlobDumpTool::Run(int argc, char** argv) {
  if (argc < 2) {
    fprintf(stderr, "No file specified.\n");
    return -1;
  }
  std::string filename(argv[1]);
  if (filename.length() < 6 ||
      filename.substr(filename.length() - 5, std::string::npos) != ".blob") {
    fprintf(stderr, "Given file is not a blob log file.\n");
    return -1;
  }
  Status s;
  Env* env = Env::Default();
  std::unique_ptr<SequentialFile> file;
  s = env->NewSequentialFile(filename, &file, EnvOptions());
  if (!s.ok()) {
    fprintf(stderr, "Failed to open file: %s\n", s.ToString().c_str());
    return -1;
  }
  std::shared_ptr<Logger> empty_logger;
  std::unique_ptr<SequentialFileReader> file_reader(new SequentialFileReader(std::move(file)));
  Reader reader(empty_logger, std::move(file_reader));
  if (!DumpBlobLogHeader(reader).ok()) {
    return -1;
  }
  while (s.ok()) {
    s = DumpRecord(reader);
  }
  if (!s.ok()) {
    return -1;
  }
  return 0;
}

Status BlobDumpTool::DumpBlobLogHeader(Reader& reader) {
  BlobLogHeader header;
  Status s = reader.ReadHeader(&header);
  if (!s.ok()) {
    fprintf(stderr, "Encounter error when reader header: %s\n", s.ToString().c_str());
    return s;
  }
  fprintf(stdout, "Magic Number   : %u\n", header.magic_number());
  fprintf(stdout, "Version        : %d\n", header.version());
  CompressionType compression = header.compression();
  std::string compression_str;
  if (!GetStringFromCompressionType(&compression_str, compression).ok()) {
    compression_str = "Unrecongnized compression type (" + ToString((int)header.compression()) + ")";
  }
  fprintf(stdout, "Compression    : %s\n", compression_str.c_str());
  fprintf(stdout, "TTL Range      : %s\n", GetString(header.ttl_range()).c_str());
  fprintf(stdout, "Timestamp Range: %s\n", GetString(header.ts_range()).c_str());
  return s;
}

Status BlobDumpTool::DumpRecord(Reader& reader) {
  const auto* file_reader = reader.file_reader();
  fprintf(stdout, "Read record @%" PRIx64 "\n", file_reader->offset());
  BlobLogRecord record;
  Status s = reader.ReadRecord(&record, Reader::kReadHdrKeyBlobFooter);
  if (!s.ok()) {
    fprintf(stderr, "Error parsing record: %s\n", s.ToString().c_str());
    return s;
  }
  fprintf(stdout, "  key size   : %d\n", record.GetKeySize());
  fprintf(stdout, "  blob size  : %" PRIu64 "\n", record.GetBlobSize());
  fprintf(stdout, "  TTL        : %u\n", record.GetTTL());
  fprintf(stdout, "  time       : %" PRIu64 "\n", record.GetTimeVal());
  fprintf(stdout, "  type       : %d, %d\n", record.type(), record.subtype());
  fprintf(stdout, "  header CRC : %u\n", record.header_checksum());
  fprintf(stdout, "  CRC        : %u\n", record.checksum());
  fprintf(stdout, "  footer CRC : %u\n", record.footer_checksum());
  fprintf(stdout, "  sequence   : %" PRIu64 "\n", record.GetSN());
  fprintf(stdout, "  key        :\n");
  DumpSlice(record.Key());
  fprintf(stdout, "  blob       :\n");
  DumpSlice(record.Blob());
  return s;
}

void BlobDumpTool::DumpSlice(const Slice& s) {
  char buf[80];
  for (size_t i = 0; i < s.size(); i += 16) {
    memset(buf, 0, sizeof(buf));
    for (size_t j = 0; j < 16 && i + j < s.size(); j++) {
      unsigned char c = s[i + j];
      sprintf(buf + j * 3 + 10, "%x", c >> 4);
      sprintf(buf + j * 3 + 11, "%x", c & 0xf);
      sprintf(buf + j + 60, "%c", (0x20 <= c && c <= 0x7e) ? c : '.');
    }
    for (size_t p = 0; p < sizeof(buf) - 1; p++) {
      if (buf[p] == 0) {
        buf[p] = ' ';
      }
    }
    fprintf(stdout, "%s\n", buf);
  }
}

std::string BlobDumpTool::GetString(ttlrange_t ttl) {
  if (ttl.first == 0 && ttl.second == 0) {
    return "nil";
  }
  return "(" + ToString(ttl.first) + ", " + ToString(ttl.second) + ")";
}

std::string BlobDumpTool::GetString(tsrange_t ts) {
  if (ts.first == 0 && ts.second == 0) {
    return "nil";
  }
  return "(" + ToString(ts.first) + ", " + ToString(ts.second) + ")";
}


}  // namespace blob_db
}  // namespace rocksdb

#endif  // ROCKSDB_LITE
