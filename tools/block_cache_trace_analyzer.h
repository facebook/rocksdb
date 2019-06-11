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

// Statistics of a block.
struct BlockAccessInfo {
  uint64_t num_accesses = 0;
  uint64_t block_size = 0;
  uint64_t first_access_time = 0;
  uint64_t last_access_time = 0;
  uint64_t num_keys = 0;
  std::map<std::string, uint64_t>
      key_num_access_map;  // for keys exist in this block.
  std::map<std::string, uint64_t>
      non_exist_key_num_access_map;  // for keys do not exist in this block.
  uint64_t num_referenced_key_exist_in_block = 0;
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

      if (access.is_referenced_key_exist_in_block == Boolean::kTrue) {
        key_num_access_map[access.referenced_key]++;
        num_referenced_key_exist_in_block++;
      } else {
        non_exist_key_num_access_map[access.referenced_key]++;
      }
    }
  }
};

// Aggregates stats of a block given a block type.
struct BlockTypeAccessInfoAggregate {
  std::map<std::string, BlockAccessInfo> block_access_info_map;
};

// Aggregates BlockTypeAggregate given a SST file.
struct SSTFileAccessInfoAggregate {
  uint32_t level;
  std::map<TraceType, BlockTypeAccessInfoAggregate> block_type_aggregates_map;
};

// Aggregates SSTFileAggregate given a column family.
struct ColumnFamilyAccessInfoAggregate {
  std::map<uint64_t, SSTFileAccessInfoAggregate> fd_aggregates_map;
};

class BlockCacheTraceAnalyzer {
 public:
  BlockCacheTraceAnalyzer(const std::string& trace_file_path);
  ~BlockCacheTraceAnalyzer() = default;
  // No copy and move.
  BlockCacheTraceAnalyzer(const BlockCacheTraceAnalyzer&) = delete;
  BlockCacheTraceAnalyzer& operator=(const BlockCacheTraceAnalyzer&) = delete;
  BlockCacheTraceAnalyzer(BlockCacheTraceAnalyzer&&) = delete;
  BlockCacheTraceAnalyzer& operator=(BlockCacheTraceAnalyzer&&) = delete;

  // Read all access records in the given trace_file, maintains the stats of
  // a block, and aggregates the information by block type, sst file, and column
  // family. Subsequently, the caller may call Print* functions to print
  // statistics.
  Status Analyze();

  // Print a summary of statistics of the trace, e.g.,
  // Number of files: 2 Number of blocks: 50 Number of accesses: 50
  // Number of Index blocks: 10
  // Number of Filter blocks: 10
  // Number of Data blocks: 10
  // Number of UncompressionDict blocks: 10
  // Number of RangeDeletion blocks: 10
  // ***************************************************************
  // Caller Get: Number of accesses 10
  // Caller Get: Number of accesses per level break down
  //          Level 0: Number of accesses: 10
  // Caller Get: Number of accesses per block type break down
  //          Block Type Index: Number of accesses: 2
  //          Block Type Filter: Number of accesses: 2
  //          Block Type Data: Number of accesses: 2
  //          Block Type UncompressionDict: Number of accesses: 2
  //          Block Type RangeDeletion: Number of accesses: 2
  void PrintStatsSummary() const;

  // Print block size distribution and the distribution break down by block type
  // and column family.
  void PrintBlockSizeStats() const;

  // Print access count distribution and the distribution break down by block
  // type and column family.
  void PrintAccessCountStats() const;

  // Print data block accesses by user Get and Multi-Get.
  // It prints out 1) A histogram on the percentage of keys accessed in a data
  // block break down by if a referenced key exists in the data block andthe
  // histogram break down by column family. 2) A histogram on the percentage of
  // accesses on keys exist in a data block and its break down by column family.
  void PrintDataBlockAccessStats() const;

  const std::map<std::string, ColumnFamilyAccessInfoAggregate>&
  TEST_cf_aggregates_map() const {
    return cf_aggregates_map_;
  }

 private:
  void RecordAccess(const BlockCacheTraceRecord& access);

  rocksdb::Env* env_;
  std::string trace_file_path_;
  BlockCacheTraceHeader header_;
  std::map<std::string, ColumnFamilyAccessInfoAggregate> cf_aggregates_map_;
};

}  // namespace rocksdb
