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
struct ColumnFamilyMetaData;
struct LevelMetaData;
struct SstFileMetaData;

// The metadata that describes a column family.
struct ColumnFamilyMetaData {
  ColumnFamilyMetaData() : size(0), name("") {}
  ColumnFamilyMetaData(const std::string& name, uint64_t size,
                       const std::vector<LevelMetaData>&& levels) :
      size(size), name(name), levels(levels) {}

  // The size of this column family in bytes, which is equal to the sum of
  // the file size of its "levels".
  uint64_t size;
  // The number of files in this column family.
  size_t file_count;
  // The name of the column family.
  std::string name;
  // The metadata of all levels in this column family.
  std::vector<LevelMetaData> levels;
};

// The metadata that describes a level.
struct LevelMetaData {
  LevelMetaData(int level, uint64_t size,
                const std::vector<SstFileMetaData>&& files) :
      level(level), size(size),
      files(files) {}

  // The level which this meta data describes.
  const int level;
  // The size of this level in bytes, which is equal to the sum of
  // the file size of its "files".
  const uint64_t size;
  // The metadata of all sst files in this level.
  const std::vector<SstFileMetaData> files;
};

// The metadata that describes a SST file.
struct SstFileMetaData {
  SstFileMetaData() {}
  SstFileMetaData(const std::string& file_name,
                  const std::string& path, uint64_t size,
                  SequenceNumber smallest_seqno,
                  SequenceNumber largest_seqno,
                  const std::string& smallestkey,
                  const std::string& largestkey,
                  bool being_compacted) :
    size(size), name(file_name),
    db_path(path), smallest_seqno(smallest_seqno), largest_seqno(largest_seqno),
    smallestkey(smallestkey), largestkey(largestkey),
    being_compacted(being_compacted) {}

  // File size in bytes.
  uint64_t size;
  // The name of the file.
  std::string name;
  // The full path where the file locates.
  std::string db_path;

  SequenceNumber smallest_seqno;  // Smallest sequence number in file.
  SequenceNumber largest_seqno;   // Largest sequence number in file.
  std::string smallestkey;     // Smallest user defined key in the file.
  std::string largestkey;      // Largest user defined key in the file.
  bool being_compacted;  // true if the file is currently being compacted.
};

// The full set of metadata associated with each SST file.
struct LiveFileMetaData : SstFileMetaData {
  std::string column_family_name;  // Name of the column family
  int level;               // Level at which this file resides.
};



}  // namespace rocksdb
