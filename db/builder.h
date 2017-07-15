//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include <string>
#include <utility>
#include <vector>
#include "db/table_properties_collector.h"
#include "options/cf_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/types.h"
#include "table/scoped_arena_iterator.h"
#include "util/event_logger.h"

namespace rocksdb {

struct Options;
struct FileMetaData;

class Env;
struct EnvOptions;
class Iterator;
class TableCache;
class VersionEdit;
class TableBuilder;
class WritableFileWriter;
class InternalStats;
class InternalIterator;

// @param column_family_name Name of the column family that is also identified
//    by column_family_id, or empty string if unknown. It must outlive the
//    TableBuilder returned by this function.
// @param compression_dict Data for presetting the compression library's
//    dictionary, or nullptr.
TableBuilder* NewTableBuilder(
    const ImmutableCFOptions& options,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, const std::string& column_family_name,
    WritableFileWriter* file, const CompressionType compression_type,
    const CompressionOptions& compression_opts,
    int level,
    const std::string* compression_dict = nullptr,
    const bool skip_filters = false);

// Build a Table file from the contents of *iter.  The generated file
// will be named according to number specified in meta. On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
//
// @param column_family_name Name of the column family that is also identified
//    by column_family_id, or empty string if unknown.
extern Status BuildTable(
    const std::string& dbname, Env* env, const ImmutableCFOptions& options,
    const MutableCFOptions& mutable_cf_options, const EnvOptions& env_options,
    TableCache* table_cache, InternalIterator* iter,
    std::unique_ptr<InternalIterator> range_del_iter, FileMetaData* meta,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, const std::string& column_family_name,
    std::vector<SequenceNumber> snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    const CompressionType compression,
    const CompressionOptions& compression_opts, bool paranoid_file_checks,
    InternalStats* internal_stats, TableFileCreationReason reason,
    EventLogger* event_logger = nullptr, int job_id = 0,
    const Env::IOPriority io_priority = Env::IO_HIGH,
    TableProperties* table_properties = nullptr, int level = -1);

}  // namespace rocksdb
