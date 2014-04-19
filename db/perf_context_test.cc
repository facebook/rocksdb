//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <algorithm>
#include <iostream>
#include <vector>
#include "/usr/include/valgrind/callgrind.h"

#include "rocksdb/db.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/memtablerep.h"
#include "util/histogram.h"
#include "util/stop_watch.h"
#include "util/testharness.h"


bool FLAGS_random_key = false;
bool FLAGS_use_set_based_memetable = false;
int FLAGS_total_keys = 100;
int FLAGS_write_buffer_size = 1000000000;
int FLAGS_max_write_buffer_number = 8;
int FLAGS_min_write_buffer_number_to_merge = 7;

// Path to the database on file system
const std::string kDbName = rocksdb::test::TmpDir() + "/perf_context_test";

namespace rocksdb {

std::shared_ptr<DB> OpenDb() {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_write_buffer_number = FLAGS_max_write_buffer_number;
    options.min_write_buffer_number_to_merge =
      FLAGS_min_write_buffer_number_to_merge;

    if (FLAGS_use_set_based_memetable) {
      auto prefix_extractor = rocksdb::NewFixedPrefixTransform(0);
      options.memtable_factory.reset(
          NewHashSkipListRepFactory(prefix_extractor));
    }

    Status s = DB::Open(options, kDbName,  &db);
    ASSERT_OK(s);
    return std::shared_ptr<DB>(db);
}

class PerfContextTest { };

TEST(PerfContextTest, SeekIntoDeletion) {
  DestroyDB(kDbName, Options());
  auto db = OpenDb();
  WriteOptions write_options;
  ReadOptions read_options;

  for (int i = 0; i < FLAGS_total_keys; ++i) {
    std::string key = "k" + std::to_string(i);
    std::string value = "v" + std::to_string(i);

    db->Put(write_options, key, value);
  }

  for (int i = 0; i < FLAGS_total_keys -1 ; ++i) {
    std::string key = "k" + std::to_string(i);
    db->Delete(write_options, key);
  }

  HistogramImpl hist_get;
  HistogramImpl hist_get_time;
  for (int i = 0; i < FLAGS_total_keys - 1; ++i) {
    std::string key = "k" + std::to_string(i);
    std::string value;

    perf_context.Reset();
    StopWatchNano timer(Env::Default(), true);
    auto status = db->Get(read_options, key, &value);
    auto elapsed_nanos = timer.ElapsedNanos();
    ASSERT_TRUE(status.IsNotFound());
    hist_get.Add(perf_context.user_key_comparison_count);
    hist_get_time.Add(elapsed_nanos);
  }

  std::cout << "Get uesr key comparison: \n" << hist_get.ToString()
            << "Get time: \n" << hist_get_time.ToString();

  HistogramImpl hist_seek_to_first;
  std::unique_ptr<Iterator> iter(db->NewIterator(read_options));

  perf_context.Reset();
  StopWatchNano timer(Env::Default(), true);
  iter->SeekToFirst();
  hist_seek_to_first.Add(perf_context.user_key_comparison_count);
  auto elapsed_nanos = timer.ElapsedNanos();

  std::cout << "SeekToFirst uesr key comparison: \n" << hist_seek_to_first.ToString()
            << "ikey skipped: " << perf_context.internal_key_skipped_count << "\n"
            << "idelete skipped: " << perf_context.internal_delete_skipped_count << "\n"
            << "elapsed: " << elapsed_nanos << "\n";

  HistogramImpl hist_seek;
  for (int i = 0; i < FLAGS_total_keys; ++i) {
    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
    std::string key = "k" + std::to_string(i);

    perf_context.Reset();
    StopWatchNano timer(Env::Default(), true);
    iter->Seek(key);
    auto elapsed_nanos = timer.ElapsedNanos();
    hist_seek.Add(perf_context.user_key_comparison_count);
    std::cout << "seek cmp: " << perf_context.user_key_comparison_count
              << " ikey skipped " << perf_context.internal_key_skipped_count
              << " idelete skipped " << perf_context.internal_delete_skipped_count
              << " elapsed: " << elapsed_nanos << "ns\n";

    perf_context.Reset();
    ASSERT_TRUE(iter->Valid());
    StopWatchNano timer2(Env::Default(), true);
    iter->Next();
    auto elapsed_nanos2 = timer2.ElapsedNanos();
    std::cout << "next cmp: " << perf_context.user_key_comparison_count
              << "elapsed: " << elapsed_nanos2 << "ns\n";
  }

  std::cout << "Seek uesr key comparison: \n" << hist_seek.ToString();
}

TEST(PerfContextTest, StopWatchNanoOverhead) {
  // profile the timer cost by itself!
  const int kTotalIterations = 1000000;
  std::vector<uint64_t> timings(kTotalIterations);

  StopWatchNano timer(Env::Default(), true);
  for (auto& timing : timings) {
    timing = timer.ElapsedNanos(true /* reset */);
  }

  HistogramImpl histogram;
  for (const auto timing : timings) {
    histogram.Add(timing);
  }

  std::cout << histogram.ToString();
}

TEST(PerfContextTest, StopWatchOverhead) {
  // profile the timer cost by itself!
  const int kTotalIterations = 1000000;
  std::vector<uint64_t> timings(kTotalIterations);

  StopWatch timer(Env::Default());
  for (auto& timing : timings) {
    timing = timer.ElapsedMicros();
  }

  HistogramImpl histogram;
  uint64_t prev_timing = 0;
  for (const auto timing : timings) {
    histogram.Add(timing - prev_timing);
    prev_timing = timing;
  }

  std::cout << histogram.ToString();
}

void ProfileKeyComparison() {
  DestroyDB(kDbName, Options());    // Start this test with a fresh DB

  auto db = OpenDb();

  WriteOptions write_options;
  ReadOptions read_options;

  HistogramImpl hist_put;
  HistogramImpl hist_get;
  HistogramImpl hist_get_snapshot;
  HistogramImpl hist_get_memtable;
  HistogramImpl hist_get_post_process;
  HistogramImpl hist_num_memtable_checked;
  HistogramImpl hist_write_pre_post;
  HistogramImpl hist_write_wal_time;
  HistogramImpl hist_write_memtable_time;

  std::cout << "Inserting " << FLAGS_total_keys << " key/value pairs\n...\n";

  std::vector<int> keys;
  for (int i = 0; i < FLAGS_total_keys; ++i) {
    keys.push_back(i);
  }

  if (FLAGS_random_key) {
    std::random_shuffle(keys.begin(), keys.end());
  }

  for (const int i : keys) {
    std::string key = "k" + std::to_string(i);
    std::string value = "v" + std::to_string(i);

    perf_context.Reset();
    db->Put(write_options, key, value);
    hist_write_pre_post.Add(perf_context.write_pre_and_post_process_time);
    hist_write_wal_time.Add(perf_context.write_wal_time);
    hist_write_memtable_time.Add(perf_context.write_memtable_time);
    hist_put.Add(perf_context.user_key_comparison_count);

    perf_context.Reset();
    db->Get(read_options, key, &value);
    hist_get_snapshot.Add(perf_context.get_snapshot_time);
    hist_get_memtable.Add(perf_context.get_from_memtable_time);
    hist_num_memtable_checked.Add(perf_context.get_from_memtable_count);
    hist_get_post_process.Add(perf_context.get_post_process_time);
    hist_get.Add(perf_context.user_key_comparison_count);
  }

  std::cout << "Put uesr key comparison: \n" << hist_put.ToString()
            << "Get uesr key comparison: \n" << hist_get.ToString();
  std::cout << "Put(): Pre and Post Process Time: \n"
            << hist_write_pre_post.ToString()
            << " Writing WAL time: \n"
            << hist_write_wal_time.ToString() << "\n"
            << " Writing Mem Table time: \n"
            << hist_write_memtable_time.ToString() << "\n";

  std::cout << "Get(): Time to get snapshot: \n"
            << hist_get_snapshot.ToString()
            << " Time to get value from memtables: \n"
            << hist_get_memtable.ToString() << "\n"
            << " Number of memtables checked: \n"
            << hist_num_memtable_checked.ToString() << "\n"
            << " Time to post process: \n"
            << hist_get_post_process.ToString() << "\n";
}

TEST(PerfContextTest, KeyComparisonCount) {
  SetPerfLevel(kEnableCount);
  ProfileKeyComparison();

  SetPerfLevel(kDisable);
  ProfileKeyComparison();

  SetPerfLevel(kEnableTime);
  ProfileKeyComparison();
}

// make perf_context_test
// export ROCKSDB_TESTS=PerfContextTest.SeekKeyComparison
// For one memtable:
// ./perf_context_test --write_buffer_size=500000 --total_keys=10000
// For two memtables:
// ./perf_context_test --write_buffer_size=250000 --total_keys=10000
// Specify --random_key=1 to shuffle the key before insertion
// Results show that, for sequential insertion, worst-case Seek Key comparison
// is close to the total number of keys (linear), when there is only one
// memtable. When there are two memtables, even the avg Seek Key comparison
// starts to become linear to the input size.

TEST(PerfContextTest, SeekKeyComparison) {
  DestroyDB(kDbName, Options());
  auto db = OpenDb();
  WriteOptions write_options;
  ReadOptions read_options;

  std::cout << "Inserting " << FLAGS_total_keys << " key/value pairs\n...\n";

  std::vector<int> keys;
  for (int i = 0; i < FLAGS_total_keys; ++i) {
    keys.push_back(i);
  }

  if (FLAGS_random_key) {
    std::random_shuffle(keys.begin(), keys.end());
  }

  HistogramImpl hist_put_time;
  HistogramImpl hist_wal_time;
  HistogramImpl hist_time_diff;

  SetPerfLevel(kEnableTime);
  StopWatchNano timer(Env::Default());
  for (const int i : keys) {
    std::string key = "k" + std::to_string(i);
    std::string value = "v" + std::to_string(i);

    perf_context.Reset();
    timer.Start();
    db->Put(write_options, key, value);
    auto put_time = timer.ElapsedNanos();
    hist_put_time.Add(put_time);
    hist_wal_time.Add(perf_context.write_wal_time);
    hist_time_diff.Add(put_time - perf_context.write_wal_time);
  }

  std::cout << "Put time:\n" << hist_put_time.ToString()
            << "WAL time:\n" << hist_wal_time.ToString()
            << "time diff:\n" << hist_time_diff.ToString();

  HistogramImpl hist_seek;
  HistogramImpl hist_next;

  for (int i = 0; i < FLAGS_total_keys; ++i) {
    std::string key = "k" + std::to_string(i);
    std::string value = "v" + std::to_string(i);

    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
    perf_context.Reset();
    iter->Seek(key);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->value().ToString(), value);
    hist_seek.Add(perf_context.user_key_comparison_count);
  }

  std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
  for (iter->SeekToFirst(); iter->Valid();) {
    perf_context.Reset();
    iter->Next();
    hist_next.Add(perf_context.user_key_comparison_count);
  }

  std::cout << "Seek:\n" << hist_seek.ToString()
            << "Next:\n" << hist_next.ToString();
}

}

int main(int argc, char** argv) {

  for (int i = 1; i < argc; i++) {
    int n;
    char junk;

    if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    }

    if (sscanf(argv[i], "--total_keys=%d%c", &n, &junk) == 1) {
      FLAGS_total_keys = n;
    }

    if (sscanf(argv[i], "--random_key=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_random_key = n;
    }

    if (sscanf(argv[i], "--use_set_based_memetable=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_use_set_based_memetable = n;
    }

  }

  std::cout << kDbName << "\n";

  rocksdb::test::RunAllTests();
  return 0;
}
