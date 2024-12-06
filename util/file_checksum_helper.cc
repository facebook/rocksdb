//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  Copyright (c) 2011 The LevelDB Authors. All rights reserved.
//  Use of this source code is governed by a BSD-style license that can be
//  found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/file_checksum_helper.h"

#include <unordered_set>

#include "db/log_reader.h"
#include "db/version_edit.h"
#include "db/version_edit_handler.h"
#include "file/sequence_file_reader.h"
#include "rocksdb/utilities/customizable_util.h"
#include "util/manifest_reader.h"

namespace ROCKSDB_NAMESPACE {

void FileChecksumListImpl::reset() { checksum_map_.clear(); }

size_t FileChecksumListImpl::size() const { return checksum_map_.size(); }

Status FileChecksumListImpl::GetAllFileChecksums(
    std::vector<uint64_t>* file_numbers, std::vector<std::string>* checksums,
    std::vector<std::string>* checksum_func_names) {
  if (file_numbers == nullptr || checksums == nullptr ||
      checksum_func_names == nullptr) {
    return Status::InvalidArgument("Pointer has not been initiated");
  }

  for (const auto& i : checksum_map_) {
    file_numbers->push_back(i.first);
    checksums->push_back(i.second.first);
    checksum_func_names->push_back(i.second.second);
  }
  return Status::OK();
}

Status FileChecksumListImpl::SearchOneFileChecksum(
    uint64_t file_number, std::string* checksum,
    std::string* checksum_func_name) {
  if (checksum == nullptr || checksum_func_name == nullptr) {
    return Status::InvalidArgument("Pointer has not been initiated");
  }

  auto it = checksum_map_.find(file_number);
  if (it == checksum_map_.end()) {
    return Status::NotFound();
  } else {
    *checksum = it->second.first;
    *checksum_func_name = it->second.second;
  }
  return Status::OK();
}

Status FileChecksumListImpl::InsertOneFileChecksum(
    uint64_t file_number, const std::string& checksum,
    const std::string& checksum_func_name) {
  auto it = checksum_map_.find(file_number);
  if (it == checksum_map_.end()) {
    checksum_map_.insert(std::make_pair(
        file_number, std::make_pair(checksum, checksum_func_name)));
  } else {
    it->second.first = checksum;
    it->second.second = checksum_func_name;
  }
  return Status::OK();
}

Status FileChecksumListImpl::RemoveOneFileChecksum(uint64_t file_number) {
  auto it = checksum_map_.find(file_number);
  if (it == checksum_map_.end()) {
    return Status::NotFound();
  } else {
    checksum_map_.erase(it);
  }
  return Status::OK();
}

FileChecksumList* NewFileChecksumList() {
  FileChecksumListImpl* checksum_list = new FileChecksumListImpl();
  return checksum_list;
}

std::shared_ptr<FileChecksumGenFactory> GetFileChecksumGenCrc32cFactory() {
  static std::shared_ptr<FileChecksumGenFactory> default_crc32c_gen_factory(
      new FileChecksumGenCrc32cFactory());
  return default_crc32c_gen_factory;
}

Status GetFileChecksumsFromManifest(Env* src_env, const std::string& abs_path,
                                    uint64_t manifest_file_size,
                                    FileChecksumList* checksum_list) {
  if (checksum_list == nullptr) {
    return Status::InvalidArgument("checksum_list is nullptr");
  }
  assert(checksum_list);
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  checksum_list->reset();
  Status s;

  std::unique_ptr<SequentialFileReader> file_reader;
  {
    std::unique_ptr<FSSequentialFile> file;
    const std::shared_ptr<FileSystem>& fs = src_env->GetFileSystem();
    s = fs->NewSequentialFile(abs_path,
                              fs->OptimizeForManifestRead(FileOptions()), &file,
                              nullptr /* dbg */);
    if (!s.ok()) {
      return s;
    }
    file_reader.reset(new SequentialFileReader(std::move(file), abs_path));
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

Status GetFileChecksumsFromCurrentManifest(Env* src_env,
                                           const std::string &db_path,
                                           uint64_t manifest_file_size,
                                           FileChecksumList *checksum_list) {
  std::string manifest_path;
  uint64_t manifest_file_number;
  Status s = GetCurrentManifestPathUtil(db_path, src_env->GetFileSystem().get(),
    true /* is_retry */, &manifest_path, &manifest_file_number);
  if (!s.ok()) {
    return s;
  }
  return GetFileChecksumsFromManifest(src_env, manifest_path,
    manifest_file_size, checksum_list);
}

namespace {
static int RegisterFileChecksumGenFactories(ObjectLibrary& library,
                                            const std::string& /*arg*/) {
  library.AddFactory<FileChecksumGenFactory>(
      FileChecksumGenCrc32cFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<FileChecksumGenFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new FileChecksumGenCrc32cFactory());
        return guard->get();
      });
  return 1;
}
}  // namespace

Status FileChecksumGenFactory::CreateFromString(
    const ConfigOptions& options, const std::string& value,
    std::shared_ptr<FileChecksumGenFactory>* result) {
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterFileChecksumGenFactories(*(ObjectLibrary::Default().get()), "");
  });
  if (value == FileChecksumGenCrc32cFactory::kClassName()) {
    *result = GetFileChecksumGenCrc32cFactory();
    return Status::OK();
  } else {
    Status s = LoadSharedObject<FileChecksumGenFactory>(options, value, result);
    return s;
  }
}
}  // namespace ROCKSDB_NAMESPACE
