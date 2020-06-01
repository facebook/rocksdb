//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  Copyright (c) 2011 The LevelDB Authors. All rights reserved.
//  Use of this source code is governed by a BSD-style license that can be
//  found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/file_checksum_helper.h"

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

  for (auto i : checksum_map_) {
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

FileChecksumFunc* CreateFileChecksumFuncCrc32c() {
  FileChecksumFunc* file_checksum_crc32c = new FileChecksumFuncCrc32c();
  return file_checksum_crc32c;
}

}  // namespace ROCKSDB_NAMESPACE
