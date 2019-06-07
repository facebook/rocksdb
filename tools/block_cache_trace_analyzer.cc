//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "tools/block_cache_trace_analyzer.h"

#include <cinttypes>
#include <set>
#include "monitoring/histogram.h"

namespace rocksdb {
namespace {
std::string block_type_to_string(TraceType type) {
  switch (type) {
    case kBlockTraceFilterBlock:
      return "Filter";
    case kBlockTraceDataBlock:
      return "Data";
    case kBlockTraceIndexBlock:
      return "Index";
    case kBlockTraceRangeDeletionBlock:
      return "RangeDeletion";
    case kBlockTraceUncompressionDictBlock:
      return "UncompressionDict";
    default:
      break;
  }
  // This cannot happen.
  assert(false);
  return "";
}

std::string caller_to_string(BlockCacheLookupCaller caller) {
  switch (caller) {
    case kUserGet:
      return "Get";
    case kUserMGet:
      return "MultiGet";
    case kUserIterator:
      return "Iterator";
    case kPrefetch:
      return "Prefetch";
    case kCompaction:
      return "Compaction";
    default:
      break;
  }
  // This cannot happen.
  assert(false);
  return "";
}
}  // namespace

BlockCacheTraceAnalyzer::BlockCacheTraceAnalyzer(
    const std::string& trace_file_path)
    : trace_file_path_(trace_file_path) {
  env_ = rocksdb::Env::Default();
}

void BlockCacheTraceAnalyzer::RecordAccess(
    const BlockCacheTraceRecord& access) {
  ColumnFamilyStats& cf_stats = cf_stats_map_[access.cf_name];
  SSTFileStats& file_stats = cf_stats.fd_stats_map[access.sst_fd_number];
  file_stats.level = access.level;
  BlockTypeStats& block_type_stats =
      file_stats.block_type_stats_map[access.block_type];
  BlockStats& block_stats = block_type_stats.block_stats_map[access.block_key];
  block_stats.AddAccess(access);
}

Status BlockCacheTraceAnalyzer::Analyze() {
  std::unique_ptr<TraceReader> trace_reader;
  Status s =
      NewFileTraceReader(env_, EnvOptions(), trace_file_path_, &trace_reader);
  if (!s.ok()) {
    return s;
  }
  BlockCacheTraceReader reader(std::move(trace_reader));
  s = reader.ReadHeader(&header_);
  if (!s.ok()) {
    return s;
  }
  while (s.ok()) {
    BlockCacheTraceRecord access;
    s = reader.ReadAccess(&access);
    if (!s.ok()) {
      break;
    }
    RecordAccess(access);
  }
  return Status::OK();
}

void BlockCacheTraceAnalyzer::PrintBlockSizeStats() {
  HistogramStat bs_stats;
  std::map<TraceType, HistogramStat> bt_stats_map;
  std::map<std::string, std::map<TraceType, HistogramStat>> cf_bt_stats_map;
  for (auto const& cf_stats : cf_stats_map_) {
    // Stats per column family.
    const std::string& cf_name = cf_stats.first;
    for (auto const& file_stats : cf_stats.second.fd_stats_map) {
      // Stats per SST file.
      for (auto const& block_type_stats :
           file_stats.second.block_type_stats_map) {
        // Stats per block type.
        const TraceType type = block_type_stats.first;
        for (auto const& block_stats :
             block_type_stats.second.block_stats_map) {
          // Stats per block.
          bs_stats.Add(block_stats.second.block_size);
          bt_stats_map[type].Add(block_stats.second.block_size);
          cf_bt_stats_map[cf_name][type].Add(block_stats.second.block_size);
        }
      }
    }
  }
  fprintf(stdout, "Block size stats: \n%s", bs_stats.ToString().c_str());
  for (auto const& bt_stats : bt_stats_map) {
    fprintf(stdout, "Block size stats for block type %s: \n%s",
            block_type_to_string(bt_stats.first).c_str(),
            bt_stats.second.ToString().c_str());
  }
  for (auto const& cf_bt_stats : cf_bt_stats_map) {
    const std::string& cf_name = cf_bt_stats.first;
    for (auto const& bt_stats : cf_bt_stats.second) {
      fprintf(stdout,
              "Block size stats for column family %s and block type %s: \n%s",
              cf_name.c_str(), block_type_to_string(bt_stats.first).c_str(),
              bt_stats.second.ToString().c_str());
    }
  }
}

void BlockCacheTraceAnalyzer::PrintAccessCountStats() {
  HistogramStat access_stats;
  std::map<TraceType, HistogramStat> bt_stats_map;
  std::map<std::string, std::map<TraceType, HistogramStat>> cf_bt_stats_map;
  for (auto const& cf_stats : cf_stats_map_) {
    // Stats per column family.
    const std::string& cf_name = cf_stats.first;
    for (auto const& file_stats : cf_stats.second.fd_stats_map) {
      // Stats per SST file.
      for (auto const& block_type_stats :
           file_stats.second.block_type_stats_map) {
        // Stats per block type.
        const TraceType type = block_type_stats.first;
        for (auto const& block_stats :
             block_type_stats.second.block_stats_map) {
          // Stats per block.
          access_stats.Add(block_stats.second.num_accesses);
          bt_stats_map[type].Add(block_stats.second.num_accesses);
          cf_bt_stats_map[cf_name][type].Add(block_stats.second.num_accesses);
        }
      }
    }
  }
  fprintf(stdout, "Block size stats: \n%s", access_stats.ToString().c_str());
  for (auto const& bt_stats : bt_stats_map) {
    fprintf(stdout, "Block size stats for block type %s: \n%s",
            block_type_to_string(bt_stats.first).c_str(),
            bt_stats.second.ToString().c_str());
  }
  for (auto const& cf_bt_stats : cf_bt_stats_map) {
    const std::string& cf_name = cf_bt_stats.first;
    for (auto const& bt_stats : cf_bt_stats.second) {
      fprintf(stdout,
              "Block size stats for column family %s and block type %s: \n%s",
              cf_name.c_str(), block_type_to_string(bt_stats.first).c_str(),
              bt_stats.second.ToString().c_str());
    }
  }
}

void BlockCacheTraceAnalyzer::PrintStatsSummary() {
  uint64_t total_num_files = 0;
  uint64_t total_num_blocks = 0;
  uint64_t total_num_accesses = 0;
  std::map<TraceType, uint64_t> bt_num_blocks_map;
  std::map<BlockCacheLookupCaller, uint64_t> caller_num_access_map;
  std::map<BlockCacheLookupCaller, std::map<TraceType, uint64_t>>
      caller_bt_num_access_map;
  std::map<BlockCacheLookupCaller, std::map<uint32_t, uint64_t>>
      caller_level_num_access_map;
  for (auto const& cf_stats : cf_stats_map_) {
    // Stats per column family.
    const std::string& cf_name = cf_stats.first;
    uint64_t cf_num_files = 0;
    uint64_t cf_num_blocks = 0;
    std::map<TraceType, uint64_t> cf_bt_blocks;
    uint64_t cf_num_accesses = 0;
    std::map<BlockCacheLookupCaller, uint64_t> cf_caller_num_accesses_map;
    std::map<BlockCacheLookupCaller, std::map<uint64_t, uint64_t>>
        cf_caller_level_num_accesses_map;
    std::map<BlockCacheLookupCaller, std::map<uint64_t, uint64_t>>
        cf_caller_file_num_accesses_map;
    std::map<BlockCacheLookupCaller, std::map<TraceType, uint64_t>>
        cf_caller_bt_num_accesses_map;
    total_num_files += cf_stats.second.fd_stats_map.size();
    for (auto const& file_stats : cf_stats.second.fd_stats_map) {
      // Stats per SST file.
      const uint64_t fd = file_stats.first;
      const uint32_t level = file_stats.second.level;
      cf_num_files++;
      for (auto const& block_type_stats :
           file_stats.second.block_type_stats_map) {
        // Stats per block type.
        const TraceType type = block_type_stats.first;
        cf_bt_blocks[type] += block_type_stats.second.block_stats_map.size();
        total_num_blocks += block_type_stats.second.block_stats_map.size();
        bt_num_blocks_map[type] +=
            block_type_stats.second.block_stats_map.size();
        for (auto const& block_stats :
             block_type_stats.second.block_stats_map) {
          // Stats per block.
          cf_num_blocks++;
          for (auto const& stats : block_stats.second.caller_num_access_map) {
            // Stats per caller.
            const BlockCacheLookupCaller caller = stats.first;
            const uint64_t num_accesses = stats.second;
            // Overall stats.
            total_num_accesses += num_accesses;
            caller_num_access_map[caller] += num_accesses;
            caller_bt_num_access_map[caller][type] += num_accesses;
            caller_level_num_access_map[caller][level] += num_accesses;
            // Column Family stats.
            cf_num_accesses++;
            cf_caller_num_accesses_map[caller] += num_accesses;
            cf_caller_level_num_accesses_map[caller][level] += num_accesses;
            cf_caller_file_num_accesses_map[caller][fd] += num_accesses;
            cf_caller_bt_num_accesses_map[caller][type] += num_accesses;
          }
        }
      }
    }

    // Print stats.
    fprintf(
        stdout,
        "***************************************************************\n");
    fprintf(
        stdout,
        "***************************************************************\n");
    fprintf(
        stdout,
        "***************************************************************\n");
    fprintf(stdout, "Statistics for column family %s:\n", cf_name.c_str());
    fprintf(stdout,
            "Number of files:%" PRIu64 "Number of blocks: %" PRIu64
            "Number of accesses: %" PRIu64 "\n",
            cf_num_files, cf_num_blocks, cf_num_accesses);
    for (auto block_type : cf_bt_blocks) {
      fprintf(stdout, "Number of %s blocks: %" PRIu64 "\n",
              block_type_to_string(block_type.first).c_str(),
              block_type.second);
    }
    for (auto caller : cf_caller_num_accesses_map) {
      fprintf(
          stdout,
          "***************************************************************\n");
      fprintf(stdout, "Caller %s: Number of accesses %" PRIu64 "\n",
              caller_to_string(caller.first).c_str(), caller.second);
      fprintf(stdout, "Caller %s: Number of accesses per level break down\n",
              caller_to_string(caller.first).c_str());
      for (auto naccess_level :
           cf_caller_level_num_accesses_map[caller.first]) {
        fprintf(stdout,
                "\t Level %" PRIu64 ": Number of accesses: %" PRIu64 "\n",
                naccess_level.first, naccess_level.second);
      }
      fprintf(stdout, "Caller %s: Number of accesses per file break down\n",
              caller_to_string(caller.first).c_str());
      for (auto naccess_file : cf_caller_file_num_accesses_map[caller.first]) {
        fprintf(stdout,
                "\t File %" PRIu64 ": Number of accesses: %" PRIu64 "\n",
                naccess_file.first, naccess_file.second);
      }
      fprintf(stdout,
              "Caller %s: Number of accesses per block type break down\n",
              caller_to_string(caller.first).c_str());
      for (auto naccess_type : cf_caller_bt_num_accesses_map[caller.first]) {
        fprintf(stdout, "\t Block Type %s: Number of accesses: %" PRIu64 "\n",
                block_type_to_string(naccess_type.first).c_str(),
                naccess_type.second);
      }
    }
  }
  fprintf(stdout,
          "***************************************************************\n");
  fprintf(stdout,
          "***************************************************************\n");
  fprintf(stdout,
          "***************************************************************\n");
  fprintf(stdout, "Overall statistics:\n");
  fprintf(stdout,
          "Number of files: %" PRIu64 " Number of blocks: %" PRIu64
          " Number of accesses: %" PRIu64 "\n",
          total_num_files, total_num_blocks, total_num_accesses);
  for (auto block_type : bt_num_blocks_map) {
    fprintf(stdout, "Number of %s blocks: %" PRIu64 "\n",
            block_type_to_string(block_type.first).c_str(), block_type.second);
  }
  for (auto caller : caller_num_access_map) {
    fprintf(
        stdout,
        "***************************************************************\n");
    fprintf(stdout, "Caller %s: Number of accesses %" PRIu64 "\n",
            caller_to_string(caller.first).c_str(), caller.second);
    fprintf(stdout, "Caller %s: Number of accesses per level break down\n",
            caller_to_string(caller.first).c_str());
    for (auto naccess_level : caller_level_num_access_map[caller.first]) {
      fprintf(stdout, "\t Level %d: Number of accesses: %" PRIu64 "\n",
              naccess_level.first, naccess_level.second);
    }
    fprintf(stdout, "Caller %s: Number of accesses per block type break down\n",
            caller_to_string(caller.first).c_str());
    for (auto naccess_type : caller_bt_num_access_map[caller.first]) {
      fprintf(stdout, "\t Block Type %s: Number of accesses: %" PRIu64 "\n",
              block_type_to_string(naccess_type.first).c_str(),
              naccess_type.second);
    }
  }
}

}  // namespace rocksdb
