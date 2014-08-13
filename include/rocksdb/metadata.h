// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <limits>
#include <string>
#include <vector>

#include "rocksdb/types.h"

#pragma once

namespace rocksdb {
struct DatabaseMetaData;
struct ColumnFamilyMetaData;
struct LevelMetaData;
struct SstFileMetaData;

// The metadata that describes a database.
struct DatabaseMetaData {
  DatabaseMetaData() : size(0), compensated_size(0), name("") {}

  // The DB size in bytes, which is equal to the sum of the size of
  // its "column_families".
  uint64_t size;
  // The compensated DB size in bytes, which is equal to the sum of the
  // compensated_size of its "column_families".
  // The compensated_size of each file is computed by giving 2X
  // average-value size to each deletion entry.
  uint64_t compensated_size;
  // The name of the DB.
  std::string name;
  // The meta-data of all column families belonging to this DB.
  std::vector<ColumnFamilyMetaData> column_families;
};

// The metadata that describes a column family.
struct ColumnFamilyMetaData {
  ColumnFamilyMetaData(const std::string& name,
                       uint64_t size, uint64_t compensated_size,
                       const std::vector<LevelMetaData>&& levels) :
      size(size), compensated_size(compensated_size),
      name(name), levels(levels) {}

  // The size of this column family in bytes, which is equal to the sum of
  // the file size of its "levels".
  const uint64_t size;
  // The compensated size of this column family in bytes, which is equal to
  // the sum of the compensated_size of its "levels".
  // The compensated_size of each file is computed by giving 2X
  // average-value size to each deletion entry.
  const uint64_t compensated_size;
  // The name of the column family.
  const std::string name;
  // The metadata of all levels in this column family.
  const std::vector<LevelMetaData> levels;
};

// The metadata that describes a level.
struct LevelMetaData {
  LevelMetaData(int level, uint64_t size, uint64_t compensated_size,
                const std::vector<SstFileMetaData>&& files) :
      level(level), size(size), compensated_size(compensated_size),
      files(files) {}

  // The level which this meta data describes.
  const int level;
  // The size of this level in bytes, which is equal to the sum of
  // the file size of its "files".
  const uint64_t size;
  // The compensated size of this level in bytes, which is equal to
  // the sum of the compensated_size of its "files".
  // The compensated_size of each file is computed by giving 2X
  // average-value size to each deletion entry.
  const uint64_t compensated_size;
  // The metadata of all sst files in this level.
  const std::vector<SstFileMetaData> files;
};

// The metadata that describes a SST file.
struct SstFileMetaData {
  SstFileMetaData(uint64_t file_number,
                  const std::string& path,
                  uint64_t size, uint64_t compensated_size,
                  SequenceNumber smallest_seqno,
                  SequenceNumber largest_seqno,
                  const std::string& smallestkey,
                  const std::string& largestkey) :
    file_number(file_number), size(size), compensated_size(size),
    path(path), smallest_seqno(smallest_seqno), largest_seqno(largest_seqno),
    smallestkey(smallestkey), largestkey(largestkey) {}

  // The number that can uniquely identify the current file inside same DB.
  const uint64_t file_number;
  // File size in bytes.
  const uint64_t size;
  // The compensated file size in bytes, computed by giving 2X average-value
  // size to each deletion entry.
  const uint64_t compensated_size;
  // The full path where the file locates.
  const std::string path;

  const SequenceNumber smallest_seqno;  // Smallest sequence number in file.
  const SequenceNumber largest_seqno;   // Largest sequence number in file.
  const std::string smallestkey;     // Smallest user defined key in the file.
  const std::string largestkey;      // Largest user defined key in the file.
};

}  // namespace rocksdb
