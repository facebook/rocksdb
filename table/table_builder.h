//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <stdint.h>

#include <string>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "db/seqno_to_time_mapping.h"
#include "db/table_properties_collector.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/options.h"
#include "rocksdb/table_properties.h"
#include "table/unique_id_impl.h"
#include "trace_replay/block_cache_tracer.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class Status;

struct TableReaderOptions {
  // @param skip_filters Disables loading/accessing the filter block
  TableReaderOptions(
      const ImmutableOptions& _ioptions,
      const std::shared_ptr<const SliceTransform>& _prefix_extractor,
      const EnvOptions& _env_options,
      const InternalKeyComparator& _internal_comparator,
      bool _skip_filters = false, bool _immortal = false,
      bool _force_direct_prefetch = false, int _level = -1,
      BlockCacheTracer* const _block_cache_tracer = nullptr,
      size_t _max_file_size_for_l0_meta_pin = 0,
      const std::string& _cur_db_session_id = "", uint64_t _cur_file_num = 0,
      UniqueId64x2 _unique_id = {}, SequenceNumber _largest_seqno = 0)
      : ioptions(_ioptions),
        prefix_extractor(_prefix_extractor),
        env_options(_env_options),
        internal_comparator(_internal_comparator),
        skip_filters(_skip_filters),
        immortal(_immortal),
        force_direct_prefetch(_force_direct_prefetch),
        level(_level),
        largest_seqno(_largest_seqno),
        block_cache_tracer(_block_cache_tracer),
        max_file_size_for_l0_meta_pin(_max_file_size_for_l0_meta_pin),
        cur_db_session_id(_cur_db_session_id),
        cur_file_num(_cur_file_num),
        unique_id(_unique_id) {}

  const ImmutableOptions& ioptions;
  const std::shared_ptr<const SliceTransform>& prefix_extractor;
  const EnvOptions& env_options;
  const InternalKeyComparator& internal_comparator;
  // This is only used for BlockBasedTable (reader)
  bool skip_filters;
  // Whether the table will be valid as long as the DB is open
  bool immortal;
  // When data prefetching is needed, even if direct I/O is off, read data to
  // fetch into RocksDB's buffer, rather than relying
  // RandomAccessFile::Prefetch().
  bool force_direct_prefetch;
  // What level this table/file is on, -1 for "not set, don't know." Used
  // for level-specific statistics.
  int level;
  // largest seqno in the table (or 0 means unknown???)
  SequenceNumber largest_seqno;
  BlockCacheTracer* const block_cache_tracer;
  // Largest L0 file size whose meta-blocks may be pinned (can be zero when
  // unknown).
  const size_t max_file_size_for_l0_meta_pin;

  std::string cur_db_session_id;

  uint64_t cur_file_num;

  // Known unique_id or {}, kNullUniqueId64x2 means unknown
  UniqueId64x2 unique_id;
};

struct TableBuilderOptions {
  TableBuilderOptions(
      const ImmutableOptions& _ioptions, const MutableCFOptions& _moptions,
      const InternalKeyComparator& _internal_comparator,
      const IntTblPropCollectorFactories* _int_tbl_prop_collector_factories,
      CompressionType _compression_type,
      const CompressionOptions& _compression_opts, uint32_t _column_family_id,
      const std::string& _column_family_name, int _level,
      bool _is_bottommost = false,
      TableFileCreationReason _reason = TableFileCreationReason::kMisc,
      const int64_t _oldest_key_time = 0,
      const uint64_t _file_creation_time = 0, const std::string& _db_id = "",
      const std::string& _db_session_id = "",
      const uint64_t _target_file_size = 0, const uint64_t _cur_file_num = 0)
      : ioptions(_ioptions),
        moptions(_moptions),
        internal_comparator(_internal_comparator),
        int_tbl_prop_collector_factories(_int_tbl_prop_collector_factories),
        compression_type(_compression_type),
        compression_opts(_compression_opts),
        column_family_id(_column_family_id),
        column_family_name(_column_family_name),
        oldest_key_time(_oldest_key_time),
        target_file_size(_target_file_size),
        file_creation_time(_file_creation_time),
        db_id(_db_id),
        db_session_id(_db_session_id),
        level_at_creation(_level),
        is_bottommost(_is_bottommost),
        reason(_reason),
        cur_file_num(_cur_file_num) {}

  const ImmutableOptions& ioptions;
  const MutableCFOptions& moptions;
  const InternalKeyComparator& internal_comparator;
  const IntTblPropCollectorFactories* int_tbl_prop_collector_factories;
  const CompressionType compression_type;
  const CompressionOptions& compression_opts;
  const uint32_t column_family_id;
  const std::string& column_family_name;
  const int64_t oldest_key_time;
  const uint64_t target_file_size;
  const uint64_t file_creation_time;
  const std::string db_id;
  const std::string db_session_id;
  // BEGIN for FilterBuildingContext
  const int level_at_creation;
  const bool is_bottommost;
  const TableFileCreationReason reason;
  // END for FilterBuildingContext

  // XXX: only used by BlockBasedTableBuilder for SstFileWriter. If you
  // want to skip filters, that should be (for example) null filter_policy
  // in the table options of the ioptions.table_factory
  bool skip_filters = false;
  const uint64_t cur_file_num;
};

// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.
class TableBuilder {
 public:
  // REQUIRES: Either Finish() or Abandon() has been called.
  virtual ~TableBuilder() {}

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual void Add(const Slice& key, const Slice& value) = 0;

  // Return non-ok iff some error has been detected.
  virtual Status status() const = 0;

  // Return non-ok iff some error happens during IO.
  virtual IOStatus io_status() const = 0;

  // Finish building the table.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual Status Finish() = 0;

  // Indicate that the contents of this builder should be abandoned.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual void Abandon() = 0;

  // Number of calls to Add() so far.
  virtual uint64_t NumEntries() const = 0;

  // Whether the output file is completely empty. It has neither entries
  // or tombstones.
  virtual bool IsEmpty() const {
    return NumEntries() == 0 && GetTableProperties().num_range_deletions == 0;
  }

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  virtual uint64_t FileSize() const = 0;

  // Estimated size of the file generated so far. This is used when
  // FileSize() cannot estimate final SST size, e.g. parallel compression
  // is enabled.
  virtual uint64_t EstimatedFileSize() const { return FileSize(); }

  // If the user defined table properties collector suggest the file to
  // be further compacted.
  virtual bool NeedCompact() const { return false; }

  // Returns table properties
  virtual TableProperties GetTableProperties() const = 0;

  // Return file checksum
  virtual std::string GetFileChecksum() const = 0;

  // Return file checksum function name
  virtual const char* GetFileChecksumFuncName() const = 0;

  // Set the sequence number to time mapping
  virtual void SetSeqnoTimeTableProperties(
      const std::string& /*encoded_seqno_to_time_mapping*/,
      uint64_t /*oldest_ancestor_time*/){};
};

}  // namespace ROCKSDB_NAMESPACE
