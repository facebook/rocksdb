//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#pragma once

#include <memory>

#include "db/version_edit.h"
#include "rocksdb/file_system.h"
#include "rocksdb/metadata.h"
#include "rocksdb/slice_transform.h"

namespace ROCKSDB_NAMESPACE {

struct ImmutableCFOptions;
class TableCache;
class VersionStorageInfo;
class VersionEdit;
struct FileMetaData;
class InternalStats;
class Version;
class VersionSet;
class ColumnFamilyData;
class CacheReservationManager;

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionBuilder {
 public:
  VersionBuilder(const FileOptions& file_options,
                 const ImmutableCFOptions* ioptions, TableCache* table_cache,
                 VersionStorageInfo* base_vstorage, VersionSet* version_set,
                 std::shared_ptr<CacheReservationManager>
                     file_metadata_cache_res_mgr = nullptr);
  ~VersionBuilder();

  bool CheckConsistencyForNumLevels();
  Status Apply(const VersionEdit* edit);
  Status SaveTo(VersionStorageInfo* vstorage) const;
  Status LoadTableHandlers(
      InternalStats* internal_stats, int max_threads,
      bool prefetch_index_and_filter_in_cache, bool is_initial_load,
      const std::shared_ptr<const SliceTransform>& prefix_extractor,
      size_t max_file_size_for_l0_meta_pin);
  uint64_t GetMinOldestBlobFileNumber() const;

 private:
  class Rep;
  std::unique_ptr<Rep> rep_;
};

// A wrapper of version builder which references the current version in
// constructor and unref it in the destructor.
// Both of the constructor and destructor need to be called inside DB Mutex.
class BaseReferencedVersionBuilder {
 public:
  explicit BaseReferencedVersionBuilder(ColumnFamilyData* cfd);
  BaseReferencedVersionBuilder(ColumnFamilyData* cfd, Version* v);
  ~BaseReferencedVersionBuilder();
  VersionBuilder* version_builder() const { return version_builder_.get(); }

 private:
  std::unique_ptr<VersionBuilder> version_builder_;
  Version* version_;
};

class NewestFirstBySeqNo {
 public:
  bool operator()(const FileMetaData* lhs, const FileMetaData* rhs) const {
    assert(lhs);
    assert(rhs);

    if (lhs->fd.largest_seqno != rhs->fd.largest_seqno) {
      return lhs->fd.largest_seqno > rhs->fd.largest_seqno;
    }

    if (lhs->fd.smallest_seqno != rhs->fd.smallest_seqno) {
      return lhs->fd.smallest_seqno > rhs->fd.smallest_seqno;
    }

    // Break ties by file number
    return lhs->fd.GetNumber() > rhs->fd.GetNumber();
  }
};
}  // namespace ROCKSDB_NAMESPACE
