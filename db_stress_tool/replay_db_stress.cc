//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// replay_db_stress: A tool to replay operations from db_stress test failures.
//
// This tool helps debug db_stress failures by parsing the op_logs output and
// replaying the exact sequence of operations that led to the failure.
//
// IMPORTANT: The database is opened in READ-ONLY mode to prevent any
// modifications to the database being debugged.
//
// Usage:
//   replay_db_stress --db=<db_path> --op_logs="<op_logs_string>"
//                    [--column_family=<cf_name>]
//
// Example op_logs format:
//   "S 000000000000038D000000000000012B0000000000000299 *N*N*N* SFP
//    000000000000038D000000000000012B00000000000002A2 *P*P*P*"
//
// Op codes:
//   S <key>   - Seek(key)
//   SFP <key> - SeekForPrev(key)
//   STF       - SeekToFirst()
//   STL       - SeekToLast()
//   N         - Next()
//   P         - Prev()
//   *         - PrepareValue() (for allow_unprepared_value)
//   Refresh   - Refresh()

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run replay_db_stress\n");
  return 1;
}
#else

#include <cstdio>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db_stress_tool/db_stress_compression_manager.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/options_util.h"
#include "util/coding.h"
#include "util/gflags_compat.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_string(db, "", "Path to the RocksDB database");
DEFINE_string(op_logs, "", "Operation logs string from db_stress failure");
DEFINE_string(column_family, "default",
              "Column family name (default: \"default\")");
DEFINE_bool(hex_keys, true, "Whether keys in op_logs are in hex format");
DEFINE_bool(verbose, true, "Print verbose output");
DEFINE_bool(allow_unprepared_value, false,
            "Set ReadOptions::allow_unprepared_value");
DEFINE_bool(try_load_options, true,
            "Try to load options from the database's OPTIONS file");
DEFINE_bool(ignore_unknown_options, false,
            "Ignore unknown options when loading OPTIONS file");

namespace ROCKSDB_NAMESPACE {

struct Operation {
  enum Type {
    kSeek,
    kSeekForPrev,
    kSeekToFirst,
    kSeekToLast,
    kNext,
    kPrev,
    kPrepareValue,
    kRefresh
  };

  Type type;
  std::string key;  // Only for Seek and SeekForPrev
  size_t position;  // Position in the op_logs string for debugging

  Operation(Type t, const std::string& k = "", size_t pos = 0)
      : type(t), key(k), position(pos) {}

  std::string ToString() const {
    switch (type) {
      case kSeek:
        return "Seek(" + Slice(key).ToString(true) + ")";
      case kSeekForPrev:
        return "SeekForPrev(" + Slice(key).ToString(true) + ")";
      case kSeekToFirst:
        return "SeekToFirst()";
      case kSeekToLast:
        return "SeekToLast()";
      case kNext:
        return "Next()";
      case kPrev:
        return "Prev()";
      case kPrepareValue:
        return "PrepareValue()";
      case kRefresh:
        return "Refresh()";
      default:
        return "Unknown";
    }
  }
};

// Convert hex string to binary string
std::string HexToString(const std::string& hex) {
  std::string result;
  if (hex.length() % 2 != 0) {
    fprintf(stderr, "Invalid hex string (odd length): %s\n", hex.c_str());
    return result;
  }

  for (size_t i = 0; i < hex.length(); i += 2) {
    std::string byte_str = hex.substr(i, 2);
    char byte = static_cast<char>(strtol(byte_str.c_str(), nullptr, 16));
    result.push_back(byte);
  }
  return result;
}

// Parse the op_logs string into a sequence of operations
std::vector<Operation> ParseOpLogs(const std::string& op_logs, bool hex_keys) {
  std::vector<Operation> operations;
  std::istringstream iss(op_logs);
  std::string token;
  size_t position = 0;

  while (iss >> token) {
    size_t current_pos = position;
    position += token.length() + 1;  // +1 for space

    if (token == "S") {
      // Seek operation
      std::string key_str;
      if (iss >> key_str) {
        position += key_str.length() + 1;
        std::string key = hex_keys ? HexToString(key_str) : key_str;
        operations.emplace_back(Operation::kSeek, key, current_pos);
      }
    } else if (token == "SFP") {
      // SeekForPrev operation
      std::string key_str;
      if (iss >> key_str) {
        position += key_str.length() + 1;
        std::string key = hex_keys ? HexToString(key_str) : key_str;
        operations.emplace_back(Operation::kSeekForPrev, key, current_pos);
      }
    } else if (token == "STF") {
      operations.emplace_back(Operation::kSeekToFirst, "", current_pos);
    } else if (token == "STL") {
      operations.emplace_back(Operation::kSeekToLast, "", current_pos);
    } else if (token == "Refresh") {
      operations.emplace_back(Operation::kRefresh, "", current_pos);
    } else if (token.find('N') != std::string::npos ||
               token.find('P') != std::string::npos ||
               token.find('*') != std::string::npos) {
      // Handle compound tokens like "*N*N*N*" or "P*P*"
      for (char c : token) {
        if (c == 'N') {
          operations.emplace_back(Operation::kNext, "", current_pos);
        } else if (c == 'P') {
          operations.emplace_back(Operation::kPrev, "", current_pos);
        } else if (c == '*') {
          operations.emplace_back(Operation::kPrepareValue, "", current_pos);
        }
      }
    } else if (token == ";") {
      // Separator, ignore
      continue;
    } else {
      fprintf(stderr, "Warning: Unknown token '%s' at position %zu\n",
              token.c_str(), current_pos);
    }
  }

  return operations;
}

// Replay operations on the database
int ReplayOperations(DB* db, ColumnFamilyHandle* cfh,
                     const std::vector<Operation>& operations, bool verbose) {
  ReadOptions read_opts;
  read_opts.allow_unprepared_value = FLAGS_allow_unprepared_value;
  read_opts.total_order_seek = true;

  std::unique_ptr<Iterator> iter(db->NewIterator(read_opts, cfh));

  printf("=== Starting operation replay (%zu operations) ===\n",
         operations.size());
  printf("ReadOptions: allow_unprepared_value=%d, total_order_seek=%d\n\n",
         read_opts.allow_unprepared_value, read_opts.total_order_seek);

  int error_count = 0;

  for (size_t i = 0; i < operations.size(); ++i) {
    const auto& op = operations[i];

    if (verbose) {
      printf("[%3zu] %s\n", i, op.ToString().c_str());
    }

    switch (op.type) {
      case Operation::kSeek:
        iter->Seek(op.key);
        break;
      case Operation::kSeekForPrev:
        iter->SeekForPrev(op.key);
        break;
      case Operation::kSeekToFirst:
        iter->SeekToFirst();
        break;
      case Operation::kSeekToLast:
        iter->SeekToLast();
        break;
      case Operation::kNext:
        iter->Next();
        break;
      case Operation::kPrev:
        iter->Prev();
        break;
      case Operation::kPrepareValue:
        if (iter->Valid()) {
          if (!iter->PrepareValue()) {
            printf("      PrepareValue() failed\n");
          }
        } else {
          printf("      Warning: PrepareValue() called on invalid iterator\n");
        }
        break;
      case Operation::kRefresh:
        // Refresh is not an iterator operation, skip for now
        printf(
            "      Note: Refresh() operation skipped (not an iterator "
            "operation)\n");
        continue;
    }

    // Check iterator status after each operation
    if (!iter->status().ok()) {
      printf("      ERROR: Iterator status: %s\n",
             iter->status().ToString().c_str());
      error_count++;
    }

    // Print current iterator state
    if (verbose) {
      if (iter->Valid()) {
        printf("      -> Valid: key=%s, value_size=%zu\n",
               iter->key().ToString(true).c_str(), iter->value().size());
      } else {
        printf("      -> Invalid (status: %s)\n",
               iter->status().ToString().c_str());
      }
    }
  }

  printf("\n=== Replay completed ===\n");
  printf("Total operations: %zu\n", operations.size());
  printf("Errors encountered: %d\n", error_count);

  // Print final iterator state
  printf("\nFinal iterator state:\n");
  if (iter->Valid()) {
    printf("  Valid: true\n");
    printf("  Key: %s\n", iter->key().ToString(true).c_str());
    printf("  Value size: %zu\n", iter->value().size());
  } else {
    printf("  Valid: false\n");
    printf("  Status: %s\n", iter->status().ToString().c_str());
  }

  return error_count;
}

int main(int argc, char** argv) {
  SetUsageMessage(
      "A tool to replay operations from db_stress test failures.\n"
      "Example:\n"
      "  replay_db_stress --db=/tmp/rocksdb_test \\\n"
      "    --op_logs=\"S 0000001A *N N SFP 000000FF *P\" \\\n"
      "    --column_family=default");

  ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_db.empty()) {
    fprintf(stderr, "Error: --db path is required\n");
    return 1;
  }

  if (FLAGS_op_logs.empty()) {
    fprintf(stderr, "Error: --op_logs string is required\n");
    return 1;
  }

  // Parse operations
  printf("Parsing op_logs...\n");
  std::vector<Operation> operations =
      ParseOpLogs(FLAGS_op_logs, FLAGS_hex_keys);

  if (operations.empty()) {
    fprintf(stderr, "Error: No valid operations found in op_logs\n");
    return 1;
  }

  printf("Parsed %zu operations\n\n", operations.size());

  // Open database
  printf("Opening database: %s\n", FLAGS_db.c_str());

  // Register custom compression manager for db_stress
  DbStressCustomCompressionManager::Register();

  DB* db = nullptr;
  Options options;
  ConfigOptions config_options;
  config_options.ignore_unknown_options = FLAGS_ignore_unknown_options;
  std::vector<ColumnFamilyDescriptor> cf_descriptors;

  // Try to load options from the database if requested
  if (FLAGS_try_load_options) {
    printf("Attempting to load options from database...\n");
    Status s =
        LoadLatestOptions(config_options, FLAGS_db, &options, &cf_descriptors);
    if (s.ok()) {
      printf("Successfully loaded options from database\n");

      // If merge operator is not set, set a string append operator
      // (similar to ldb behavior)
      for (auto& cf_entry : cf_descriptors) {
        if (!cf_entry.options.merge_operator) {
          cf_entry.options.merge_operator =
              MergeOperators::CreateStringAppendOperator(':');
        }
      }

      // Handle wal_dir if it doesn't exist
      if (!options.wal_dir.empty()) {
        if (options.env->FileExists(options.wal_dir).IsNotFound()) {
          options.wal_dir = FLAGS_db;
          fprintf(stderr,
                  "wal_dir loaded from the option file doesn't exist. "
                  "Ignoring it.\n");
        }
      }
    } else if (s.IsNotFound()) {
      printf("No OPTIONS file found, using default options\n");
      FLAGS_try_load_options = false;  // Fall back to default behavior
    } else {
      fprintf(stderr, "Error loading options: %s\n", s.ToString().c_str());
      fprintf(stderr,
              "You may want to try with --ignore_unknown_options or "
              "--try_load_options=false\n");
      return 1;
    }
  }

  options.create_if_missing = false;

  // List all column families if we didn't load them from options
  std::vector<std::string> cf_names;
  if (!FLAGS_try_load_options || cf_descriptors.empty()) {
    Status s = DB::ListColumnFamilies(options, FLAGS_db, &cf_names);
    if (!s.ok()) {
      fprintf(stderr, "Warning: Failed to list column families: %s\n",
              s.ToString().c_str());
      cf_names.push_back("default");
    }

    printf("Column families found: ");
    for (const auto& name : cf_names) {
      printf("%s ", name.c_str());
    }
    printf("\n");

    // Create column family descriptors with default options
    cf_descriptors.clear();
    for (const auto& name : cf_names) {
      cf_descriptors.emplace_back(name, ColumnFamilyOptions());
    }
  } else {
    printf("Using %zu column families from OPTIONS file\n",
           cf_descriptors.size());
  }
  printf("\n");

  // Open database in read-only mode to ensure we don't modify it
  std::vector<ColumnFamilyHandle*> cf_handles;
  Status s =
      DB::OpenForReadOnly(options, FLAGS_db, cf_descriptors, &cf_handles, &db);

  if (!s.ok()) {
    fprintf(stderr, "Error: Failed to open database: %s\n",
            s.ToString().c_str());
    return 1;
  }

  printf("Database opened successfully (read-only mode)\n\n");

  // Find the target column family
  ColumnFamilyHandle* target_cfh = nullptr;
  for (size_t i = 0; i < cf_handles.size(); ++i) {
    if (cf_handles[i]->GetName() == FLAGS_column_family) {
      target_cfh = cf_handles[i];
      break;
    }
  }

  if (target_cfh == nullptr) {
    fprintf(stderr, "Error: Column family '%s' not found\n",
            FLAGS_column_family.c_str());
    for (auto* handle : cf_handles) {
      delete handle;
    }
    delete db;
    return 1;
  }

  printf("Using column family: %s\n\n", FLAGS_column_family.c_str());

  // Replay operations
  int result = ReplayOperations(db, target_cfh, operations, FLAGS_verbose);

  // Cleanup
  for (auto* handle : cf_handles) {
    delete handle;
  }
  delete db;

  return result > 0 ? 1 : 0;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
