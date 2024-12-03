//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/manifest_ops.h"

#include "db/version_edit_handler.h"
#include "file/filename.h"
#include "file/sequence_file_reader.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"

namespace ROCKSDB_NAMESPACE {

Status GetCurrentManifestPath(const std::string& abs_path, FileSystem* fs,
                              bool is_retry, std::string* manifest_path,
                              uint64_t* manifest_file_number) {
  assert(fs != nullptr);
  assert(manifest_path != nullptr);
  assert(manifest_file_number != nullptr);

  IOOptions opts;
  std::string fname;
  if (is_retry) {
    opts.verify_and_reconstruct_read = true;
  }
  Status s = ReadFileToString(fs, CurrentFileName(abs_path), opts, &fname);
  if (!s.ok()) {
    return s;
  }
  if (fname.empty() || fname.back() != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  // remove the trailing '\n'
  fname.resize(fname.size() - 1);
  FileType type;
  bool parse_ok = ParseFileName(fname, manifest_file_number, &type);
  if (!parse_ok || type != kDescriptorFile) {
    return Status::Corruption("CURRENT file corrupted");
  }
  *manifest_path = abs_path;
  if (abs_path.back() != '/') {
    manifest_path->push_back('/');
  }
  manifest_path->append(fname);
  return Status::OK();
}

// Return a list of all table (SST) and blob files checksum info.
// NOTE: This is a lightweight implementation that does not require opening DB.
Status GetFileChecksumsFromCurrentManifest(Env* src_env,
                                           const std::string& abs_path,
                                           uint64_t manifest_file_size,
                                           FileChecksumList* checksum_list) {
  std::string manifest_path;
  uint64_t manifest_file_number;
  Status s = GetCurrentManifestPath(abs_path, src_env->GetFileSystem().get(),
                                    true /* is_retry */, &manifest_path,
                                    &manifest_file_number);
  if (!s.ok()) {
    return s;
  }

  if (checksum_list == nullptr) {
    return Status::InvalidArgument("checksum_list is nullptr");
  }
  assert(checksum_list);

  const ReadOptions read_options(Env::IOActivity::kReadManifest);
  checksum_list->reset();

  std::unique_ptr<SequentialFileReader> file_reader;
  {
    std::unique_ptr<FSSequentialFile> file;
    const std::shared_ptr<FileSystem>& fs = src_env->GetFileSystem();
    s = fs->NewSequentialFile(manifest_path,
                              fs->OptimizeForManifestRead(FileOptions()), &file,
                              nullptr /* dbg */);
    if (!s.ok()) {
      return s;
    }
    file_reader.reset(new SequentialFileReader(std::move(file), manifest_path));
  }

  struct LogReporter : public log::Reader::Reporter {
    Status* status_ptr;
    void Corruption(size_t /*bytes*/, const Status& st) override {
      if (status_ptr->ok()) {
        *status_ptr = st;
      }
    }
  } reporter;
  reporter.status_ptr = &s;
  log::Reader reader(nullptr, std::move(file_reader), &reporter,
                     true /* checksum */, 0 /* log_number */);
  FileChecksumRetriever retriever(read_options, manifest_file_size,
                                  *checksum_list);
  retriever.Iterate(reader, &s);
  return retriever.status();
}

}  // namespace ROCKSDB_NAMESPACE
