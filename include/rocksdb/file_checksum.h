// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cassert>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// FileChecksumFunc is the function class to generates the checksum value
// for each file when the file is written to the file system.
class FileChecksumFunc {
 public:
  virtual ~FileChecksumFunc() {}
  // Return the checksum of concat (A, data[0,n-1]) where init_checksum is the
  // returned value of some string A. It is used to maintain the checksum of a
  // stream of data
  virtual std::string Extend(const std::string& init_checksum, const char* data,
                             size_t n) = 0;

  // Return the checksum value of data[0,n-1]
  virtual std::string Value(const char* data, size_t n) = 0;

  // Return a processed value of the checksum for store in somewhere
  virtual std::string ProcessChecksum(const std::string& checksum) = 0;

  // Returns a name that identifies the current file checksum function.
  virtual const char* Name() const = 0;
};

// FileChecksumList stores the checksum information of a list of files (e.g.,
// SST files). The FileChecksumLIst can be used to store the checksum
// information of all SST file getting  from the MANIFEST, which are
// the checksum information of all valid SST file of a DB instance. It can
// also be used to store the checksum information of a list of SST files to
// be ingested.
class FileChecksumList {
 public:
  virtual ~FileChecksumList() {}

  // Clean the previously stored file checksum information.
  virtual void reset() = 0;

  // Get the number of checksums in the checksum list
  virtual size_t size() const = 0;

  // Return all the file checksum information being stored in a unordered_map.
  // File_number is the key, the first part of the value is checksum value,
  // and the second part of the value is checksum function name.
  virtual Status GetAllFileChecksums(
      std::vector<uint64_t>* file_numbers, std::vector<std::string>* checksums,
      std::vector<std::string>* checksum_func_names) = 0;

  // Given the file_number, it searches if the file checksum information is
  // stored.
  virtual Status SearchOneFileChecksum(uint64_t file_number,
                                       std::string* checksum,
                                       std::string* checksum_func_name) = 0;

  // Insert the checksum information of one file to the FileChecksumList.
  virtual Status InsertOneFileChecksum(
      uint64_t file_number, const std::string& checksum,
      const std::string& checksum_func_name) = 0;

  // Remove the checksum information of one SST file.
  virtual Status RemoveOneFileChecksum(uint64_t file_number) = 0;
};

// Create a new file checksum list.
extern FileChecksumList* NewFileChecksumList();

// Create a Crc32c based file checksum function
extern FileChecksumFunc* CreateFileChecksumFuncCrc32c();

}  // namespace ROCKSDB_NAMESPACE
