//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "util/file_util.h"

#include <string>
#include <algorithm>

#include "rocksdb/env.h"
#include "util/sst_file_manager_impl.h"
#include "util/file_reader_writer.h"

namespace rocksdb {

// Utility function to copy a file up to a specified length
Status CopyFile(Env* env, const std::string& source,
                const std::string& destination, uint64_t size, bool use_fsync) {
  const EnvOptions soptions;
  Status s;
  unique_ptr<SequentialFileReader> src_reader;
  unique_ptr<WritableFileWriter> dest_writer;

  {
    unique_ptr<SequentialFile> srcfile;
    s = env->NewSequentialFile(source, &srcfile, soptions);
    if (!s.ok()) {
      return s;
    }
    unique_ptr<WritableFile> destfile;
    s = env->NewWritableFile(destination, &destfile, soptions);
    if (!s.ok()) {
      return s;
    }

    if (size == 0) {
      // default argument means copy everything
      s = env->GetFileSize(source, &size);
      if (!s.ok()) {
        return s;
      }
    }
    src_reader.reset(new SequentialFileReader(std::move(srcfile)));
    dest_writer.reset(new WritableFileWriter(std::move(destfile), soptions));
  }

  char buffer[4096];
  Slice slice;
  while (size > 0) {
    size_t bytes_to_read = std::min(sizeof(buffer), static_cast<size_t>(size));
    s = src_reader->Read(bytes_to_read, &slice, buffer);
    if (!s.ok()) {
      return s;
    }
    if (slice.size() == 0) {
      return Status::Corruption("file too small");
    }
    s = dest_writer->Append(slice);
    if (!s.ok()) {
      return s;
    }
    size -= slice.size();
  }
  dest_writer->Sync(use_fsync);
  return Status::OK();
}

// Utility function to create a file with the provided contents
Status CreateFile(Env* env, const std::string& destination,
                  const std::string& contents) {
  const EnvOptions soptions;
  Status s;
  unique_ptr<WritableFileWriter> dest_writer;

  unique_ptr<WritableFile> destfile;
  s = env->NewWritableFile(destination, &destfile, soptions);
  if (!s.ok()) {
    return s;
  }
  dest_writer.reset(new WritableFileWriter(std::move(destfile), soptions));
  return dest_writer->Append(Slice(contents));
}

Status DeleteSSTFile(const ImmutableDBOptions* db_options,
                     const std::string& fname, uint32_t path_id) {
#ifndef ROCKSDB_LITE
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options->sst_file_manager.get());
  if (sfm) {
    return sfm->ScheduleFileDeletion(fname);
  } else {
    return db_options->env->DeleteFile(fname);
  }
#else
  // SstFileManager is not supported in ROCKSDB_LITE
  return db_options->env->DeleteFile(fname);
#endif
}

}  // namespace rocksdb
