//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <vector>
#include <unordered_set>

#include "db/dbformat.h"
#include "table/internal_iterator.h"
#include "rocksdb/db.h"

namespace rocksdb {

struct FileMetaData;
class IteratorCache;
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

struct FileMetaDataBoundBuilder {
  const InternalKeyComparator& icomp;
  InternalKey smallest, largest;
  SequenceNumber smallest_seqno;
  SequenceNumber largest_seqno;
  uint64_t creation_time;

  FileMetaDataBoundBuilder(const InternalKeyComparator& _icomp)
      : icomp(_icomp),
        smallest_seqno(kMaxSequenceNumber),
        largest_seqno(0),
        creation_time(0) {}

  void Update(const FileMetaData* f);
};

Status LoadRangeWithDepend(std::vector<RangeWithDepend>& ranges,
                           FileMetaDataBoundBuilder* bound_builder,
                           IteratorCache& iterator_cache,
                           const FileMetaData* const* file_meta, size_t n);

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

InternalIterator* NewShrinkRangeWithDependIterator(
    const std::vector<RangeWithDepend>& ranges,
    std::unordered_set<uint64_t>& sst_depend_build,
    IteratorCache& iterator_cache, const InternalKeyComparator& icomp,
    Arena* arena = nullptr);

}
