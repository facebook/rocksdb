//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <vector>

#include "rocksdb/env.h"
#include "trace_replay/block_cache_tracer.h"

namespace rocksdb {

struct BlockStats {
  uint64_t num_accesses = 0;
  uint64_t block_size = 0;
  uint64_t first_access_time = 0;
  uint64_t last_access_time = 0;
  uint64_t num_keys;
  std::map<std::string, uint64_t> key_num_access_map;
  uint64_t num_referenced_key_not_exist;
  std::map<BlockCacheLookupCaller, uint64_t> caller_num_access_map;

  void AddAccess(const BlockCacheTraceRecord& access) {
    if (first_access_time == 0) {
      first_access_time = access.access_timestamp;
    }
    last_access_time = access.access_timestamp;
    block_size = access.block_size;
    caller_num_access_map[access.caller]++;
    num_accesses++;
    if (ShouldTraceReferencedKey(access)) {
      num_keys = access.num_keys_in_block;
      key_num_access_map[access.referenced_key]++;
      if (access.is_referenced_key_exist_in_block == Boolean::kFalse) {
        num_referenced_key_not_exist++;
      }
    }
  }
};

struct BlockTypeStats {
  std::map<std::string, BlockStats> block_stats_map;
};

struct SSTFileStats {
  uint32_t level;
  std::map<TraceType, BlockTypeStats> block_type_stats_map;
};

struct ColumnFamilyStats {
  std::map<uint64_t, SSTFileStats> fd_stats_map;
};

class BlockCacheTraceAnalyzer {
 public:
  BlockCacheTraceAnalyzer(const std::string& trace_file_path);

  // It maintains the stats of a block and aggregates the information by block
  // type, sst file, and column family.
  Status Analyze();

  void PrintStatsSummary();

  void PrintBlockSizeStats();

  void PrintAccessCountStats();

  std::map<std::string, ColumnFamilyStats>& Test_cf_stats_map() {
    return cf_stats_map_;
  }

 private:
  void RecordAccess(const BlockCacheTraceRecord& access);

  rocksdb::Env* env_;
  std::string trace_file_path_;
  BlockCacheTraceHeader header_;
  std::map<std::string, ColumnFamilyStats> cf_stats_map_;
};

}  // namespace rocksdb
