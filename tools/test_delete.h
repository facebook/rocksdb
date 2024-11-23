#pragma once

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <functional>
#include <thread>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/listener.h"
#include "rocksdb/table.h"
#include "table/mock_table.h"
#include "rocksdb/options.h"
#include "options/options_helper.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/slice.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/statistics.h"

#include "db/db_impl/db_impl.h"
#include "file/filename.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"

#include "util/cast_util.h"
#include "util/compression.h"
#include "util/mutexlock.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"

// #include "rocksdb/global_range_deleter.h"

#include <gflags/gflags.h>

namespace ROCKSDB_NAMESPACE {

// class GlobalRangeDeleterGCListener : public EventListener {
//  public:
//   explicit GlobalRangeDeleterGCListener(Options* db_options)
//       : db_options_(db_options) {}

// //   void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci, GlobalRangeDeleter<std::string>& global_range_deleter) override {
//   void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci) override {
//     if(ci.bottommost_level && (ci.output_level > 1)){
//       // std::cout << "Garbage collection ..." << std::endl;
//       // std::cout << "Bottommost level: " << ci.output_level << " least_input_seq: " << ci.least_input_seq << std::endl;
//       db->GarbageCollectGlobalRangeDeleter(ci.least_input_seq);
//     }
//     // std::cout << "Skip garbage collection ..." << std::endl;
//   }

//   int max_level_checked = 0;
//   const Options* db_options_;
// };

}  // namespace ROCKSDB_NAMESPACE

void WaitForCompaction(rocksdb::DB* db) {
  // This is an imperfect way of waiting for compaction. The loop and sleep
  // is done because a thread that finishes a compaction job should get a
  // chance to pickup a new compaction job.
  using rocksdb::DB;
  // 5 second
  db->GetEnv()->SleepForMicroseconds(10 * 1000000);
  std::vector<std::string> keys = {DB::Properties::kMemTableFlushPending,
                                   DB::Properties::kNumRunningFlushes,
                                   DB::Properties::kNumRunningCompactions};

  fprintf(stdout, "waitforcompaction(%s): started\n", db->GetName().c_str());

  while (true) {
    bool retry = false;

    for (const auto& k : keys) {
      uint64_t v;
      if (!db->GetIntProperty(k, &v)) {
        fprintf(stderr, "waitforcompaction(%s): GetIntProperty(%s) failed\n",
                db->GetName().c_str(), k.c_str());
        exit(1);
      } else if (v > 0) {
        fprintf(stdout, "waitforcompaction(%s): active(%s). Sleep 10 seconds\n",
                db->GetName().c_str(), k.c_str());
        retry = true;
        break;
      }
    }

    if (!retry) {
      fprintf(stdout, "waitforcompaction(%s): finished\n",
              db->GetName().c_str());
      return;
    }
    db->GetEnv()->SleepForMicroseconds(10 * 1000000);
  }
}

rocksdb::Options get_default_options() {
  std::cout << "Default setting initializing ..." << std::endl;
  auto options = rocksdb::Options();
  options.create_if_missing = true;
  options.write_buffer_size = 5 * 1024 * 1024;
  options.target_file_size_base = 1024 * 1024 * 1024;
  options.target_file_size_multiplier = 10;
  options.max_bytes_for_level_multiplier = 5;
  options.max_bytes_for_level_base = 25 * 1024 * 1024;
  options.level_compaction_dynamic_level_bytes = false;
  options.use_direct_reads = true;
  options.use_direct_io_for_flush_and_compaction = true;
  auto table_options =
      options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->no_block_cache = true;
  
  options.statistics = rocksdb::CreateDBStatistics();

  return options;
}

rocksdb::Options get_GRDR_options() {
  std::cout << "GRDR setting initializing ..." << std::endl;
  auto options = rocksdb::Options();
  options.create_if_missing = true;
  options.write_buffer_size = 5 * 1024 * 1024;
  options.target_file_size_base = 1024 * 1024 * 1024;
  options.target_file_size_multiplier = 10;
  options.max_bytes_for_level_multiplier = 5;
  options.max_bytes_for_level_base = 25 * 1024 * 1024;
  options.level_compaction_dynamic_level_bytes = false;
  options.use_direct_reads = true;
  options.use_direct_io_for_flush_and_compaction = true;

//   options.enable_global_range_deleter = true;

  auto table_options =
      options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->no_block_cache = true;
  
  options.statistics = rocksdb::CreateDBStatistics();

//   rocksdb::GlobalRangeDeleterGCListener* listener =
//       new rocksdb::GlobalRangeDeleterGCListener(&options);
//   options.listeners.emplace_back(listener);

  return options;
}
