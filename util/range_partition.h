//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <vector>

#include "db/dbformat.h"
#include "rocksdb/db.h"

namespace rocksdb {

struct FileMetaData;
struct MapSstElement;
class VersionStorageInfo;

struct RangeWithDepend {
  InternalKey point[2];
  bool include[2];
  std::vector<uint64_t> depend;

  RangeWithDepend() = default;
  RangeWithDepend(const FileMetaData*);
  RangeWithDepend(const MapSstElement&);
};

// Merge two sorted non-overlap range vector
// a: [ -------- )      [ -------- ]
// b:       ( -------------- ]
// r: [ -- ]( -- )[ -- )[ -- ]( -- ]
std::vector<RangeWithDepend> MergeRangeWithDepend(
    const std::vector<RangeWithDepend>& range_a,
    const std::vector<RangeWithDepend>& range_b,
    const InternalKeyComparator& icomp);

// Delete range from sorted non-overlap range vector
// e: [ -------- )      [ -------- ]
// d:       ( -------------- ]
// r: [ -- ]                  ( -- ]
std::vector<RangeWithDepend> DeleteRangeWithDepend(
    const std::vector<RangeWithDepend>& range_e,
    const std::vector<RangePtr>& range_d,
    const InternalKeyComparator& icomp);

}
