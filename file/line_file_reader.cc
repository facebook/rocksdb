//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "file/line_file_reader.h"

#include <cstring>

namespace ROCKSDB_NAMESPACE {

Status LineFileReader::Create(const std::shared_ptr<FileSystem>& fs,
                              const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<LineFileReader>* reader,
                              IODebugContext* dbg) {
  std::unique_ptr<FSSequentialFile> file;
  Status s = fs->NewSequentialFile(fname, file_opts, &file, dbg);
  if (s.ok()) {
    reader->reset(new LineFileReader(std::move(file), fname));
  }
  return s;
}

bool LineFileReader::ReadLine(std::string* out) {
  assert(out);
  if (!status_.ok()) {
    // Status should be checked (or permit unchecked) any time we return false.
    status_.MustCheck();
    return false;
  }
  out->clear();
  for (;;) {
    // Look for line delimiter
    const char* found = static_cast<const char*>(
        std::memchr(buf_begin_, '\n', buf_end_ - buf_begin_));
    if (found) {
      size_t len = found - buf_begin_;
      out->append(buf_begin_, len);
      buf_begin_ += len + /*delim*/ 1;
      ++line_number_;
      return true;
    }
    if (at_eof_) {
      status_.MustCheck();
      return false;
    }
    // else flush and reload buffer
    out->append(buf_begin_, buf_end_ - buf_begin_);
    Slice result;
    status_ = sfr_.Read(buf_.size(), &result, buf_.data());
    if (!status_.ok()) {
      status_.MustCheck();
      return false;
    }
    if (result.size() != buf_.size()) {
      // The obscure way of indicating EOF
      at_eof_ = true;
    }
    buf_begin_ = result.data();
    buf_end_ = result.data() + result.size();
  }
}

}  // namespace ROCKSDB_NAMESPACE
