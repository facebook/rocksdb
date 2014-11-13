//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <cassert>
#include <cctype>
#include <unordered_set>
#include "rocksdb/options.h"
#include "rocksdb/utilities/convenience.h"
#include "util/options_helper.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE

namespace {
CompressionType ParseCompressionType(const std::string& type) {
  if (type == "kNoCompression") {
    return kNoCompression;
  } else if (type == "kSnappyCompression") {
    return kSnappyCompression;
  } else if (type == "kZlibCompression") {
    return kZlibCompression;
  } else if (type == "kBZip2Compression") {
    return kBZip2Compression;
  } else if (type == "kLZ4Compression") {
    return kLZ4Compression;
  } else if (type == "kLZ4HCCompression") {
    return kLZ4HCCompression;
  } else {
    throw "unknown compression type: " + type;
  }
  return kNoCompression;
}

bool ParseBoolean(const std::string& type, const std::string& value) {
  if (value == "true" || value == "1") {
    return true;
  } else if (value == "false" || value == "0") {
    return false;
  } else {
    throw type;
  }
}
int ParseInt(const std::string& value) { return std::stoi(value); }

uint32_t ParseUint32(const std::string& value) {
  return static_cast<uint32_t>(std::stoul(value));
}

uint64_t ParseUint64(const std::string& value) {
  return std::stoull(value);
}

size_t ParseSizeT(const std::string& value) {
  return static_cast<size_t>(ParseUint64(value));
}

double ParseDouble(const std::string& value) {
  return std::stod(value);
}

CompactionStyle ParseCompactionStyle(const std::string& type) {
  if (type == "kCompactionStyleLevel") {
    return kCompactionStyleLevel;
  } else if (type == "kCompactionStyleUniversal") {
    return kCompactionStyleUniversal;
  } else if (type == "kCompactionStyleFIFO") {
    return kCompactionStyleFIFO;
  } else {
    throw "unknown compaction style: " + type;
  }
  return kCompactionStyleLevel;
}
}  // anonymouse namespace

template<typename OptionsType>
bool ParseMemtableOptions(const std::string& name, const std::string& value,
                          OptionsType* new_options) {
  if (name == "write_buffer_size") {
    new_options->write_buffer_size = ParseSizeT(value);
  } else if (name == "arena_block_size") {
    new_options->arena_block_size = ParseSizeT(value);
  } else if (name == "memtable_prefix_bloom_bits") {
    new_options->memtable_prefix_bloom_bits = ParseUint32(value);
  } else if (name == "memtable_prefix_bloom_probes") {
    new_options->memtable_prefix_bloom_probes = ParseUint32(value);
  } else if (name == "memtable_prefix_bloom_huge_page_tlb_size") {
    new_options->memtable_prefix_bloom_huge_page_tlb_size =
      ParseSizeT(value);
  } else if (name == "max_successive_merges") {
    new_options->max_successive_merges = ParseSizeT(value);
  } else if (name == "filter_deletes") {
    new_options->filter_deletes = ParseBoolean(name, value);
  } else if (name == "max_write_buffer_number") {
    new_options->max_write_buffer_number = ParseInt(value);
  } else if (name == "inplace_update_num_locks") {
    new_options->inplace_update_num_locks = ParseSizeT(value);
  } else {
    return false;
  }
  return true;
}

template<typename OptionsType>
bool ParseCompactionOptions(const std::string& name, const std::string& value,
                            OptionsType* new_options) {
  if (name == "disable_auto_compactions") {
    new_options->disable_auto_compactions = ParseBoolean(name, value);
  } else if (name == "soft_rate_limit") {
    new_options->soft_rate_limit = ParseDouble(value);
  } else if (name == "hard_rate_limit") {
    new_options->hard_rate_limit = ParseDouble(value);
  } else if (name == "level0_file_num_compaction_trigger") {
    new_options->level0_file_num_compaction_trigger = ParseInt(value);
  } else if (name == "level0_slowdown_writes_trigger") {
    new_options->level0_slowdown_writes_trigger = ParseInt(value);
  } else if (name == "level0_stop_writes_trigger") {
    new_options->level0_stop_writes_trigger = ParseInt(value);
  } else if (name == "max_grandparent_overlap_factor") {
    new_options->max_grandparent_overlap_factor = ParseInt(value);
  } else if (name == "expanded_compaction_factor") {
    new_options->expanded_compaction_factor = ParseInt(value);
  } else if (name == "source_compaction_factor") {
    new_options->source_compaction_factor = ParseInt(value);
  } else if (name == "target_file_size_base") {
    new_options->target_file_size_base = ParseInt(value);
  } else if (name == "target_file_size_multiplier") {
    new_options->target_file_size_multiplier = ParseInt(value);
  } else if (name == "max_bytes_for_level_base") {
    new_options->max_bytes_for_level_base = ParseUint64(value);
  } else if (name == "max_bytes_for_level_multiplier") {
    new_options->max_bytes_for_level_multiplier = ParseInt(value);
  } else if (name == "max_bytes_for_level_multiplier_additional") {
    new_options->max_bytes_for_level_multiplier_additional.clear();
    size_t start = 0;
    while (true) {
      size_t end = value.find_first_of(':', start);
      if (end == std::string::npos) {
        new_options->max_bytes_for_level_multiplier_additional.push_back(
            ParseInt(value.substr(start)));
        break;
      } else {
        new_options->max_bytes_for_level_multiplier_additional.push_back(
            ParseInt(value.substr(start, end - start)));
        start = end + 1;
      }
    }
  } else if (name == "max_mem_compaction_level") {
    new_options->max_mem_compaction_level = ParseInt(value);
  } else {
    return false;
  }
  return true;
}

template<typename OptionsType>
bool ParseMiscOptions(const std::string& name, const std::string& value,
                      OptionsType* new_options) {
  if (name == "max_sequential_skip_in_iterations") {
    new_options->max_sequential_skip_in_iterations = ParseUint64(value);
  } else {
    return false;
  }
  return true;
}

Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableCFOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  try {
    for (const auto& o : options_map) {
      if (ParseMemtableOptions(o.first, o.second, new_options)) {
      } else if (ParseCompactionOptions(o.first, o.second, new_options)) {
      } else if (ParseMiscOptions(o.first, o.second, new_options)) {
      } else {
        return Status::InvalidArgument(
            "unsupported dynamic option: " + o.first);
      }
    }
  } catch (std::exception& e) {
    return Status::InvalidArgument("error parsing " + std::string(e.what()));
  }
  return Status::OK();
}

namespace {

std::string trim(const std::string& str) {
  size_t start = 0;
  size_t end = str.size() - 1;
  while (isspace(str[start]) != 0 && start <= end) {
    ++start;
  }
  while (isspace(str[end]) != 0 && start <= end) {
    --end;
  }
  if (start <= end) {
    return str.substr(start, end - start + 1);
  }
  return std::string();
}

bool StringToMap(const std::string& opts_str,
                 std::unordered_map<std::string, std::string>* opts_map) {
  assert(opts_map);
  // Example:
  //   opts_str = "write_buffer_size=1024;max_write_buffer_number=2"
  size_t pos = 0;

  std::string opts = trim(opts_str);
  while (pos < opts.size()) {
    size_t eq_pos = opts.find('=', pos);
    if (eq_pos == std::string::npos) {
      return false;
    }
    std::string key = trim(opts.substr(pos, eq_pos - pos));

    size_t sc_pos = opts.find(';', eq_pos + 1);
    if (sc_pos == std::string::npos) {
      (*opts_map)[key] = trim(opts.substr(eq_pos + 1));
      // It either ends with a trailing semi-colon or the last key-value pair
      break;
    } else {
      (*opts_map)[key] = trim(opts.substr(eq_pos + 1, sc_pos - eq_pos - 1));
    }
    pos = sc_pos + 1;
  }

  return true;
}

}  // anonymous namespace

bool GetColumnFamilyOptionsFromMap(
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  for (const auto& o : opts_map) {
    try {
      if (ParseMemtableOptions(o.first, o.second, new_options)) {
      } else if (ParseCompactionOptions(o.first, o.second, new_options)) {
      } else if (ParseMiscOptions(o.first, o.second, new_options)) {
      } else if (o.first == "min_write_buffer_number_to_merge") {
        new_options->min_write_buffer_number_to_merge = ParseInt(o.second);
      } else if (o.first == "compression") {
        new_options->compression = ParseCompressionType(o.second);
      } else if (o.first == "compression_per_level") {
        new_options->compression_per_level.clear();
        size_t start = 0;
        while (true) {
          size_t end = o.second.find_first_of(':', start);
          if (end == std::string::npos) {
            new_options->compression_per_level.push_back(
                ParseCompressionType(o.second.substr(start)));
            break;
          } else {
            new_options->compression_per_level.push_back(
                ParseCompressionType(o.second.substr(start, end - start)));
            start = end + 1;
          }
        }
      } else if (o.first == "compression_opts") {
        size_t start = 0;
        size_t end = o.second.find_first_of(':');
        if (end == std::string::npos) {
          throw o.first;
        }
        new_options->compression_opts.window_bits =
            ParseInt(o.second.substr(start, end - start));
        start = end + 1;
        end = o.second.find_first_of(':', start);
        if (end == std::string::npos) {
          throw o.first;
        }
        new_options->compression_opts.level =
            ParseInt(o.second.substr(start, end - start));
        start = end + 1;
        if (start >= o.second.size()) {
          throw o.first;
        }
        new_options->compression_opts.strategy =
            ParseInt(o.second.substr(start, o.second.size() - start));
      } else if (o.first == "num_levels") {
        new_options->num_levels = ParseInt(o.second);
      } else if (o.first == "purge_redundant_kvs_while_flush") {
        new_options->purge_redundant_kvs_while_flush =
          ParseBoolean(o.first, o.second);
      } else if (o.first == "compaction_style") {
        new_options->compaction_style = ParseCompactionStyle(o.second);
      } else if (o.first == "verify_checksums_in_compaction") {
        new_options->verify_checksums_in_compaction =
          ParseBoolean(o.first, o.second);
      } else if (o.first == "compaction_options_universal") {
        // TODO(ljin): add support
        throw o.first;
      } else if (o.first == "compaction_options_fifo") {
        new_options->compaction_options_fifo.max_table_files_size
          = ParseUint64(o.second);
      } else if (o.first == "bloom_locality") {
        new_options->bloom_locality = ParseUint32(o.second);
      } else if (o.first == "min_partial_merge_operands") {
        new_options->min_partial_merge_operands = ParseUint32(o.second);
      } else if (o.first == "inplace_update_support") {
        new_options->inplace_update_support = ParseBoolean(o.first, o.second);
      } else {
        return false;
      }
    } catch (std::exception) {
      return false;
    }
  }
  return true;
}

bool GetColumnFamilyOptionsFromString(
    const ColumnFamilyOptions& base_options,
    const std::string& opts_str,
    ColumnFamilyOptions* new_options) {
  std::unordered_map<std::string, std::string> opts_map;
  if (!StringToMap(opts_str, &opts_map)) {
    return false;
  }
  return GetColumnFamilyOptionsFromMap(base_options, opts_map, new_options);
}

bool GetDBOptionsFromMap(
    const DBOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    DBOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  for (const auto& o : opts_map) {
    try {
      if (o.first == "create_if_missing") {
        new_options->create_if_missing = ParseBoolean(o.first, o.second);
      } else if (o.first == "create_missing_column_families") {
        new_options->create_missing_column_families =
          ParseBoolean(o.first, o.second);
      } else if (o.first == "error_if_exists") {
        new_options->error_if_exists = ParseBoolean(o.first, o.second);
      } else if (o.first == "paranoid_checks") {
        new_options->paranoid_checks = ParseBoolean(o.first, o.second);
      } else if (o.first == "max_open_files") {
        new_options->max_open_files = ParseInt(o.second);
      } else if (o.first == "max_total_wal_size") {
        new_options->max_total_wal_size = ParseUint64(o.second);
      } else if (o.first == "disable_data_sync") {
        new_options->disableDataSync = ParseBoolean(o.first, o.second);
      } else if (o.first == "use_fsync") {
        new_options->use_fsync = ParseBoolean(o.first, o.second);
      } else if (o.first == "db_paths") {
        // TODO(ljin): add support
        throw o.first;
      } else if (o.first == "db_log_dir") {
        new_options->db_log_dir = o.second;
      } else if (o.first == "wal_dir") {
        new_options->wal_dir = o.second;
      } else if (o.first == "delete_obsolete_files_period_micros") {
        new_options->delete_obsolete_files_period_micros =
          ParseUint64(o.second);
      } else if (o.first == "max_background_compactions") {
        new_options->max_background_compactions = ParseInt(o.second);
      } else if (o.first == "max_background_flushes") {
        new_options->max_background_flushes = ParseInt(o.second);
      } else if (o.first == "max_log_file_size") {
        new_options->max_log_file_size = ParseSizeT(o.second);
      } else if (o.first == "log_file_time_to_roll") {
        new_options->log_file_time_to_roll = ParseSizeT(o.second);
      } else if (o.first == "keep_log_file_num") {
        new_options->keep_log_file_num = ParseSizeT(o.second);
      } else if (o.first == "max_manifest_file_size") {
        new_options->max_manifest_file_size = ParseUint64(o.second);
      } else if (o.first == "table_cache_numshardbits") {
        new_options->table_cache_numshardbits = ParseInt(o.second);
      } else if (o.first == "table_cache_remove_scan_count_limit") {
        new_options->table_cache_remove_scan_count_limit = ParseInt(o.second);
      } else if (o.first == "WAL_ttl_seconds") {
        new_options->WAL_ttl_seconds = ParseUint64(o.second);
      } else if (o.first == "WAL_size_limit_MB") {
        new_options->WAL_size_limit_MB = ParseUint64(o.second);
      } else if (o.first == "manifest_preallocation_size") {
        new_options->manifest_preallocation_size = ParseSizeT(o.second);
      } else if (o.first == "allow_os_buffer") {
        new_options->allow_os_buffer = ParseBoolean(o.first, o.second);
      } else if (o.first == "allow_mmap_reads") {
        new_options->allow_mmap_reads = ParseBoolean(o.first, o.second);
      } else if (o.first == "allow_mmap_writes") {
        new_options->allow_mmap_writes = ParseBoolean(o.first, o.second);
      } else if (o.first == "is_fd_close_on_exec") {
        new_options->is_fd_close_on_exec = ParseBoolean(o.first, o.second);
      } else if (o.first == "skip_log_error_on_recovery") {
        new_options->skip_log_error_on_recovery =
          ParseBoolean(o.first, o.second);
      } else if (o.first == "stats_dump_period_sec") {
        new_options->stats_dump_period_sec = ParseUint32(o.second);
      } else if (o.first == "advise_random_on_open") {
        new_options->advise_random_on_open = ParseBoolean(o.first, o.second);
      } else if (o.first == "use_adaptive_mutex") {
        new_options->use_adaptive_mutex = ParseBoolean(o.first, o.second);
      } else if (o.first == "bytes_per_sync") {
        new_options->bytes_per_sync = ParseUint64(o.second);
      } else {
        return false;
      }
    } catch (std::exception) {
      return false;
    }
  }
  return true;
}

bool GetDBOptionsFromString(
    const DBOptions& base_options,
    const std::string& opts_str,
    DBOptions* new_options) {
  std::unordered_map<std::string, std::string> opts_map;
  if (!StringToMap(opts_str, &opts_map)) {
    return false;
  }
  return GetDBOptionsFromMap(base_options, opts_map, new_options);
}

#endif  // ROCKSDB_LITE
}  // namespace rocksdb
