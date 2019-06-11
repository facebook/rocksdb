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
  return "InvalidType";
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
  return "InvalidCaller";
}
}  // namespace

BlockCacheTraceAnalyzer::BlockCacheTraceAnalyzer(
    const std::string& trace_file_path)
    : trace_file_path_(trace_file_path) {
  env_ = rocksdb::Env::Default();
}

void BlockCacheTraceAnalyzer::RecordAccess(
    const BlockCacheTraceRecord& access) {
  ColumnFamilyAccessInfoAggregate& cf_aggr = cf_aggregates_map_[access.cf_name];
  SSTFileAccessInfoAggregate& file_aggr =
      cf_aggr.fd_aggregates_map[access.sst_fd_number];
  file_aggr.level = access.level;
  BlockTypeAccessInfoAggregate& block_type_aggr =
      file_aggr.block_type_aggregates_map[access.block_type];
  BlockAccessInfo& block_access_info =
      block_type_aggr.block_access_info_map[access.block_key];
  block_access_info.AddAccess(access);
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
      return s;
    }
    RecordAccess(access);
  }
  return Status::OK();
}

void BlockCacheTraceAnalyzer::PrintBlockSizeStats() const {
  HistogramStat bs_stats;
  std::map<TraceType, HistogramStat> bt_stats_map;
  std::map<std::string, std::map<TraceType, HistogramStat>> cf_bt_stats_map;
  for (auto const& cf_aggregates : cf_aggregates_map_) {
    // Stats per column family.
    const std::string& cf_name = cf_aggregates.first;
    for (auto const& file_aggregates : cf_aggregates.second.fd_aggregates_map) {
      // Stats per SST file.
      for (auto const& block_type_aggregates :
           file_aggregates.second.block_type_aggregates_map) {
        // Stats per block type.
        const TraceType type = block_type_aggregates.first;
        for (auto const& block_access_info :
             block_type_aggregates.second.block_access_info_map) {
          // Stats per block.
          bs_stats.Add(block_access_info.second.block_size);
          bt_stats_map[type].Add(block_access_info.second.block_size);
          cf_bt_stats_map[cf_name][type].Add(
              block_access_info.second.block_size);
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

void BlockCacheTraceAnalyzer::PrintAccessCountStats() const {
  HistogramStat access_stats;
  std::map<TraceType, HistogramStat> bt_stats_map;
  std::map<std::string, std::map<TraceType, HistogramStat>> cf_bt_stats_map;
  for (auto const& cf_aggregates : cf_aggregates_map_) {
    // Stats per column family.
    const std::string& cf_name = cf_aggregates.first;
    for (auto const& file_aggregates : cf_aggregates.second.fd_aggregates_map) {
      // Stats per SST file.
      for (auto const& block_type_aggregates :
           file_aggregates.second.block_type_aggregates_map) {
        // Stats per block type.
        const TraceType type = block_type_aggregates.first;
        for (auto const& block_access_info :
             block_type_aggregates.second.block_access_info_map) {
          // Stats per block.
          access_stats.Add(block_access_info.second.num_accesses);
          bt_stats_map[type].Add(block_access_info.second.num_accesses);
          cf_bt_stats_map[cf_name][type].Add(
              block_access_info.second.num_accesses);
        }
      }
    }
  }
  fprintf(stdout, "Block access count stats: \n%s",
          access_stats.ToString().c_str());
  for (auto const& bt_stats : bt_stats_map) {
    fprintf(stdout, "Block access count stats for block type %s: \n%s",
            block_type_to_string(bt_stats.first).c_str(),
            bt_stats.second.ToString().c_str());
  }
  for (auto const& cf_bt_stats : cf_bt_stats_map) {
    const std::string& cf_name = cf_bt_stats.first;
    for (auto const& bt_stats : cf_bt_stats.second) {
      fprintf(stdout,
              "Block access count stats for column family %s and block type "
              "%s: \n%s",
              cf_name.c_str(), block_type_to_string(bt_stats.first).c_str(),
              bt_stats.second.ToString().c_str());
    }
  }
}

void BlockCacheTraceAnalyzer::PrintDataBlockAccessStats() const {
  HistogramStat existing_keys_stats;
  std::map<std::string, HistogramStat> cf_existing_keys_stats_map;
  HistogramStat non_existing_keys_stats;
  std::map<std::string, HistogramStat> cf_non_existing_keys_stats_map;
  HistogramStat block_access_stats;
  std::map<std::string, HistogramStat> cf_block_access_info;

  for (auto const& cf_aggregates : cf_aggregates_map_) {
    // Stats per column family.
    const std::string& cf_name = cf_aggregates.first;
    for (auto const& file_aggregates : cf_aggregates.second.fd_aggregates_map) {
      // Stats per SST file.
      for (auto const& block_type_aggregates :
           file_aggregates.second.block_type_aggregates_map) {
        // Stats per block type.
        for (auto const& block_access_info :
             block_type_aggregates.second.block_access_info_map) {
          // Stats per block.
          if (block_access_info.second.num_keys == 0) {
            continue;
          }
          // Use four decimal points.
          uint64_t percent_referenced_for_existing_keys = (uint64_t)(
              ((double)block_access_info.second.key_num_access_map.size() /
               (double)block_access_info.second.num_keys) *
              10000.0);
          uint64_t percent_referenced_for_non_existing_keys =
              (uint64_t)(((double)block_access_info.second
                              .non_exist_key_num_access_map.size() /
                          (double)block_access_info.second.num_keys) *
                         10000.0);
          uint64_t percent_accesses_for_existing_keys = (uint64_t)(
              ((double)
                   block_access_info.second.num_referenced_key_exist_in_block /
               (double)block_access_info.second.num_accesses) *
              10000.0);
          existing_keys_stats.Add(percent_referenced_for_existing_keys);
          cf_existing_keys_stats_map[cf_name].Add(
              percent_referenced_for_existing_keys);
          non_existing_keys_stats.Add(percent_referenced_for_non_existing_keys);
          cf_non_existing_keys_stats_map[cf_name].Add(
              percent_referenced_for_non_existing_keys);
          block_access_stats.Add(percent_accesses_for_existing_keys);
          cf_block_access_info[cf_name].Add(percent_accesses_for_existing_keys);
        }
      }
    }
  }
  fprintf(stdout,
          "Histogram on percentage of referenced keys existing in a block over "
          "the total number of keys in a block: \n%s",
          existing_keys_stats.ToString().c_str());
  for (auto const& cf_stats : cf_existing_keys_stats_map) {
    fprintf(stdout, "Break down by column family %s: \n%s",
            cf_stats.first.c_str(), cf_stats.second.ToString().c_str());
  }
  fprintf(
      stdout,
      "Histogram on percentage of referenced keys DO NOT exist in a block over "
      "the total number of keys in a block: \n%s",
      non_existing_keys_stats.ToString().c_str());
  for (auto const& cf_stats : cf_non_existing_keys_stats_map) {
    fprintf(stdout, "Break down by column family %s: \n%s",
            cf_stats.first.c_str(), cf_stats.second.ToString().c_str());
  }
  fprintf(stdout,
          "Histogram on percentage of accesses on keys exist in a block over "
          "the total number of accesses in a block: \n%s",
          block_access_stats.ToString().c_str());
  for (auto const& cf_stats : cf_block_access_info) {
    fprintf(stdout, "Break down by column family %s: \n%s",
            cf_stats.first.c_str(), cf_stats.second.ToString().c_str());
  }
}

void BlockCacheTraceAnalyzer::PrintStatsSummary() const {
  uint64_t total_num_files = 0;
  uint64_t total_num_blocks = 0;
  uint64_t total_num_accesses = 0;
  std::map<TraceType, uint64_t> bt_num_blocks_map;
  std::map<BlockCacheLookupCaller, uint64_t> caller_num_access_map;
  std::map<BlockCacheLookupCaller, std::map<TraceType, uint64_t>>
      caller_bt_num_access_map;
  std::map<BlockCacheLookupCaller, std::map<uint32_t, uint64_t>>
      caller_level_num_access_map;
  for (auto const& cf_aggregates : cf_aggregates_map_) {
    // Stats per column family.
    const std::string& cf_name = cf_aggregates.first;
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
    total_num_files += cf_aggregates.second.fd_aggregates_map.size();
    for (auto const& file_aggregates : cf_aggregates.second.fd_aggregates_map) {
      // Stats per SST file.
      const uint64_t fd = file_aggregates.first;
      const uint32_t level = file_aggregates.second.level;
      cf_num_files++;
      for (auto const& block_type_aggregates :
           file_aggregates.second.block_type_aggregates_map) {
        // Stats per block type.
        const TraceType type = block_type_aggregates.first;
        cf_bt_blocks[type] +=
            block_type_aggregates.second.block_access_info_map.size();
        total_num_blocks +=
            block_type_aggregates.second.block_access_info_map.size();
        bt_num_blocks_map[type] +=
            block_type_aggregates.second.block_access_info_map.size();
        for (auto const& block_access_info :
             block_type_aggregates.second.block_access_info_map) {
          // Stats per block.
          cf_num_blocks++;
          for (auto const& stats :
               block_access_info.second.caller_num_access_map) {
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
