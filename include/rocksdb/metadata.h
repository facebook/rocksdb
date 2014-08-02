// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <limits>
#include <string>
#include <vector>

#include "rocksdb/types.h"

#ifndef INCLUDE_ROCKSDB_METADATA_H_
#define INCLUDE_ROCKSDB_METADATA_H_

namespace rocksdb {
struct DatabaseMetaData;
struct ColumnFamilyMetaData;
struct LevelMetaData;
struct SstFileMetaData;

struct DatabaseMetaData {
  DatabaseMetaData() : size(0), name("") {}

  uint64_t size;              // The size of the DB in bytes.
  std::string name;           // The name of the DB.

  // Meta-data of all column families belonging to this DB.
  std::vector<ColumnFamilyMetaData> column_families;
};

struct ColumnFamilyMetaData {
  ColumnFamilyMetaData(uint64_t size, const std::string& name,
                       const std::vector<LevelMetaData>& levels) :
      size(size), name(name), levels(levels) {}

  const uint64_t size;                      // The size of the CF in bytes.
  const std::string name;                   // Name of the column family
  const std::vector<LevelMetaData> levels;  // All levels in this CF.
};

struct LevelMetaData {
  LevelMetaData(uint64_t size, int level,
                SequenceNumber smallest_seqno, SequenceNumber largest_seqno,
                const std::vector<SstFileMetaData>& files) :
      size(size), level(level),
      smallest_seqno(smallest_seqno), largest_seqno(largest_seqno),
      files(files) {}

  const uint64_t size;                  // File size in bytes.
  const int level;                      // The level which this meta describes.

  const SequenceNumber smallest_seqno;       // Smallest seqno in file
  const SequenceNumber largest_seqno;        // Largest seqno in file

  const std::vector<SstFileMetaData> files;  // All sst files in this level.
};

struct SstFileMetaData {
  SstFileMetaData(uint64_t size, const std::string& name,
                  const std::string& path,
                  SequenceNumber smallest_seqno,
                  SequenceNumber largest_seqno,
                  const std::string& smallestkey,
                  const std::string& largestkey) :
    size(size), name(name), path(path),
    smallest_seqno(smallest_seqno), largest_seqno(largest_seqno),
    smallestkey(smallestkey), largestkey(largestkey) {}

  const uint64_t size;                  // File size in bytes.
  const std::string name;               // Name of the file
  const std::string path;               // The directory where the file locates.

  const SequenceNumber smallest_seqno;  // Smallest seqno in file
  const SequenceNumber largest_seqno;   // Largest seqno in file
  const std::string smallestkey;     // Smallest user defined key in the file.
  const std::string largestkey;      // Largest user defined key in the file.
};

}  // namespace rocksdb

#endif  // INCLUDE_ROCKSDB_METADATA_H_
