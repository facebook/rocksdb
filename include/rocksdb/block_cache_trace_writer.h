// Copyright (c) 2021, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include "rocksdb/options.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/table_reader_caller.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record.h"

namespace ROCKSDB_NAMESPACE {
enum Boolean : char { kTrue = 1, kFalse = 0 };

// A record for block cache lookups/inserts. This is passed by the table
// reader to the BlockCacheTraceWriter for every block cache op.
struct BlockCacheTraceRecord {
  // Required fields for all accesses.
  uint64_t access_timestamp = 0;
  std::string block_key;
  TraceType block_type = TraceType::kTraceMax;
  uint64_t block_size = 0;
  uint64_t cf_id = 0;
  std::string cf_name;
  uint32_t level = 0;
  uint64_t sst_fd_number = 0;
  TableReaderCaller caller = TableReaderCaller::kMaxBlockCacheLookupCaller;
  Boolean is_cache_hit = Boolean::kFalse;
  Boolean no_insert = Boolean::kFalse;
  // Required field for Get and MultiGet
  uint64_t get_id;
  Boolean get_from_user_specified_snapshot = Boolean::kFalse;
  std::string referenced_key;
  // Required fields for data block and user Get/Multi-Get only.
  uint64_t referenced_data_size = 0;
  uint64_t num_keys_in_block = 0;
  Boolean referenced_key_exist_in_block = Boolean::kFalse;

  BlockCacheTraceRecord() {}

  BlockCacheTraceRecord(uint64_t _access_timestamp, std::string _block_key,
                        TraceType _block_type, uint64_t _block_size,
                        uint64_t _cf_id, std::string _cf_name, uint32_t _level,
                        uint64_t _sst_fd_number, TableReaderCaller _caller,
                        bool _is_cache_hit, bool _no_insert, uint64_t _get_id,
                        bool _get_from_user_specified_snapshot = false,
                        std::string _referenced_key = "",
                        uint64_t _referenced_data_size = 0,
                        uint64_t _num_keys_in_block = 0,
                        bool _referenced_key_exist_in_block = false)
      : access_timestamp(_access_timestamp),
        block_key(_block_key),
        block_type(_block_type),
        block_size(_block_size),
        cf_id(_cf_id),
        cf_name(_cf_name),
        level(_level),
        sst_fd_number(_sst_fd_number),
        caller(_caller),
        is_cache_hit(_is_cache_hit ? Boolean::kTrue : Boolean::kFalse),
        no_insert(_no_insert ? Boolean::kTrue : Boolean::kFalse),
        get_id(_get_id),
        get_from_user_specified_snapshot(_get_from_user_specified_snapshot
                                             ? Boolean::kTrue
                                             : Boolean::kFalse),
        referenced_key(_referenced_key),
        referenced_data_size(_referenced_data_size),
        num_keys_in_block(_num_keys_in_block),
        referenced_key_exist_in_block(
            _referenced_key_exist_in_block ? Boolean::kTrue : Boolean::kFalse) {
  }
};

// Options for tracing block cache accesses
struct BlockCacheTraceOptions {
  // Specify trace sampling option, i.e. capture one per how many requests.
  // Default to 1 (capture every request).
  uint64_t sampling_frequency = 1;
  // Note: The filtering happens before sampling.
  uint64_t filter = kTraceFilterNone;
};

// Options for the built-in implementation of BlockCacheTraceWriter
struct BlockCacheTraceWriterOptions {
  uint64_t max_trace_file_size = uint64_t{64} * 1024 * 1024 * 1024;
};

// BlockCacheTraceWriter is an abstract class that captures all RocksDB block
// cache accesses. Every RocksDB operation is passed to WriteBlockAccess()
// with a BlockCacheTraceRecord.
class BlockCacheTraceWriter {
 public:
  virtual ~BlockCacheTraceWriter() {}

  // Pass Slice references to avoid copy.
  virtual Status WriteBlockAccess(const BlockCacheTraceRecord& record,
                                  const Slice& block_key, const Slice& cf_name,
                                  const Slice& referenced_key) = 0;

  // Write a trace header at the beginning, typically on initiating a trace,
  // with some metadata like a magic number and RocksDB version.
  virtual Status WriteHeader() = 0;
};

// Allocate an instance of the built-in BlockCacheTraceWriter implementation,
// that traces all block cache accesses to a user-provided TraceWriter. Each
// access is traced to a file with a timestamp and type, followed by the
// payload.
std::unique_ptr<BlockCacheTraceWriter> NewBlockCacheTraceWriter(
    SystemClock* clock, const BlockCacheTraceWriterOptions& trace_options,
    std::unique_ptr<TraceWriter>&& trace_writer);

}  // namespace ROCKSDB_NAMESPACE
