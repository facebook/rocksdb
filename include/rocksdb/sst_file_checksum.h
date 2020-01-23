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

namespace rocksdb {

// SstFileChecksumFunc is the function class to generates the checksum value
// for each SST file when the file is written to the file system. The checksum
// is stored in VersionSet as well as MANIFEST.
class SstFileChecksumFunc {
 public:
  virtual ~SstFileChecksumFunc() {}
  // Return the checksum of concat (A, data[0,n-1]) where init_checksum is the
  // returned value of some string A. It is used to maintain the checksum of a
  // stream of data
  virtual uint32_t Extend(uint32_t init_checksum, const char* data,
                          size_t n) = 0;

  // Return the checksum value of data[0,n-1]
  virtual uint32_t Value(const char* data, size_t n) = 0;

  // Return a processed value of the checksum for store in somewhere
  virtual uint32_t ProcessChecksum(const uint32_t checksum) = 0;

  // Returns a name that identifies the current file checksum function.
  virtual const char* Name() const = 0;
};

// FileChecksumList stores the checksum information of a list of SST files.
// The checksum information can be from the MANIFEST, which are the checksum
// information of all valid SST file of a DB instance. It can also be used to
// store the checksum information of a list of SST files to be ingested.
class FileChecksumList {
 public:
  virtual ~FileChecksumList() {}

  // Clean the previously stored file checksum information.
  virtual void reset() = 0;

  // Get the number of checksums in the checksum list
  virtual size_t size() = 0;

  // Return all the sst file checksums being stored in a unordered_map.
  // File_number is the key, the first part of the value is checksum value,
  // and the second part of the value is checksum function name.
  virtual Status GetAllFileChecksums(
      std::vector<uint64_t>* file_numbers, std::vector<uint32_t>* checksums,
      std::vector<std::string>* checksum_func_names) = 0;

  // Given the file_number, it searches if the file checksum information is
  // stored.
  virtual Status SearchOneFileChecksum(const uint64_t file_number,
                                       uint32_t* checksum,
                                       std::string* checksum_func_name) = 0;

  // Insert the checksum information of one file to the FileChecksumList.
  virtual Status InsertOneFileChecksum(
      const uint64_t file_number, const uint32_t checksum,
      const std::string checksum_func_name) = 0;

  // Remove the checksum information of one SST file.
  virtual Status RemoveOneFileChecksum(const uint64_t file_number) = 0;
};

// Create a new file checksum list.
extern FileChecksumList* NewFileChecksumList();

}  // namespace rocksdb
