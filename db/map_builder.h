//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <cstdint>
#include <vector>

#include "db/compaction.h"
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "rocksdb/db.h"

namespace rocksdb {

struct FileMetaData;
struct FileMetaDataBoundBuilder;
class InstrumentedMutex;
class InternalKeyComparator;
class RangeDelAggregator;
class MapSstElementIterator;
class TableCache;
class VersionEdit;
class VersionSet;

class MapBuilder {
 public:
  // All params are references or pointers
  MapBuilder(int job_id, const ImmutableDBOptions& db_options,
             const EnvOptions& env_options, VersionSet* versions,
             Statistics* stats, const std::string& dbname);

  // no copy/move
  MapBuilder(MapBuilder&& job) = delete;
  MapBuilder(const MapBuilder& job) = delete;
  MapBuilder& operator=(const MapBuilder& job) = delete;

  // All params are references or pointers
  // deleted_range use internal keys
  // added_files is sorted
  // file_meta::fd::file_size == 0 if don't need create map files
  // file_meta & porp nullptr if ignore
  Status Build(const std::vector<CompactionInputFiles>& inputs,
               const std::vector<Range>& deleted_range,
               const std::vector<const FileMetaData*>& added_files,
               SstPurpose compaction_purpose, int output_level,
               uint32_t output_path_id, VersionStorageInfo* vstorage,
               ColumnFamilyData* cfd, VersionEdit* edit,
               FileMetaData* file_meta = nullptr,
               std::unique_ptr<TableProperties>* porp = nullptr);

 private:
  Status WriteOutputFile(const FileMetaDataBoundBuilder& bound_builder,
                         MapSstElementIterator* range_iter,
                         uint32_t output_path_id, ColumnFamilyData* cfd,
                         FileMetaData* file_meta,
                         std::unique_ptr<TableProperties>* porp);

  int job_id_;

  // DBImpl state
  const std::string& dbname_;
  const ImmutableDBOptions& db_options_;
  const EnvOptions& env_options_;

  Env* env_;
  // env_option optimized for compaction table reads
  EnvOptions env_options_for_read_;
  VersionSet* versions_;
  Statistics* stats_;
};

extern InternalIterator* NewMapElementIterator(
    FileMetaData* const* meta_array, size_t meta_size, TableCache* table_cache,
    const ReadOptions& read_options, const EnvOptions& env_options,
    const InternalKeyComparator* icmp,
    const SliceTransform* slice_transform = nullptr, Arena* arena = nullptr);

extern bool IsPrefaceRange(const Range& range, const FileMetaData* f,
                           const InternalKeyComparator& icomp);

}  // namespace rocksdb
