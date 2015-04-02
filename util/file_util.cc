//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <string>
#include <algorithm>
#include "util/file_util.h"
#include "rocksdb/env.h"
#include "db/filename.h"

namespace rocksdb {

// Utility function to copy a file up to a specified length
Status CopyFile(Env* env, const std::string& source,
                const std::string& destination, uint64_t size) {
  const EnvOptions soptions;
  unique_ptr<SequentialFile> srcfile;
  Status s;
  s = env->NewSequentialFile(source, &srcfile, soptions);
  unique_ptr<WritableFile> destfile;
  if (s.ok()) {
    s = env->NewWritableFile(destination, &destfile, soptions);
  } else {
    return s;
  }

  if (size == 0) {
    // default argument means copy everything
    if (s.ok()) {
      s = env->GetFileSize(source, &size);
    } else {
      return s;
    }
  }

  char buffer[4096];
  Slice slice;
  while (size > 0) {
    uint64_t bytes_to_read =
        std::min(static_cast<uint64_t>(sizeof(buffer)), size);
    if (s.ok()) {
      s = srcfile->Read(bytes_to_read, &slice, buffer);
    }
    if (s.ok()) {
      if (slice.size() == 0) {
        return Status::Corruption("file too small");
      }
      s = destfile->Append(slice);
    }
    if (!s.ok()) {
      return s;
    }
    size -= slice.size();
  }
  return Status::OK();
}

}  // namespace rocksdb
