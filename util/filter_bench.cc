//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else

#include <cinttypes>
#include <iostream>
#include <random>
#include <vector>

#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based/full_filter_block.h"
#include "table/block_based/mock_block_based_table.h"
#include "util/gflags_compat.h"
#include "util/hash.h"
#include "util/stop_watch.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_int64(seed, 0, "Seed for random number generators");

DEFINE_double(working_mem_size_mb, 200,
              "MB of memory to get up to among all filters");

DEFINE_uint32(average_keys_per_filter, 10000,
              "Average number of keys per filter");

DEFINE_uint32(key_size, 16, "Number of bytes each key should be");

DEFINE_uint32(batch_size, 8, "Number of keys to group in each batch");

DEFINE_uint32(bits_per_key, 10, "Bits per key setting for filters");

DEFINE_double(m_queries, 200, "Millions of queries for each test mode");

DEFINE_bool(use_full_block_reader, false,
            "Use FullFilterBlockReader interface rather than FilterBitsReader");

DEFINE_bool(quick, false, "Run more limited set of tests, fewer queries");

DEFINE_bool(allow_bad_fp_rate, false, "Continue even if FP rate is bad");

DEFINE_bool(legend, false,
            "Print more information about interpreting results instead of "
            "running tests");

void _always_assert_fail(int line, const char *file, const char *expr) {
  fprintf(stderr, "%s: %d: Assertion %s failed\n", file, line, expr);
  abort();
}

#define ALWAYS_ASSERT(cond) \
  ((cond) ? (void)0 : ::_always_assert_fail(__LINE__, __FILE__, #cond))

using rocksdb::BlockContents;
using rocksdb::CachableEntry;
using rocksdb::fastrange32;
using rocksdb::FilterBitsBuilder;
using rocksdb::FilterBitsReader;
using rocksdb::FullFilterBlockReader;
using rocksdb::Slice;
using rocksdb::mock::MockBlockBasedTableTester;

struct KeyMaker {
  KeyMaker(size_t size)
      : data_(new char[size]),
        slice_(data_.get(), size),
        vals_(reinterpret_cast<uint32_t *>(data_.get())) {
    assert(size >= 8);
    memset(data_.get(), 0, size);
  }
  std::unique_ptr<char[]> data_;
  Slice slice_;
  uint32_t *vals_;

  Slice Get(uint32_t filter_num, uint32_t val_num) {
    vals_[0] = filter_num + val_num;
    vals_[1] = val_num;
    return slice_;
  }
};

void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
  fprintf(stdout,
          "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
  fprintf(stdout,
          "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
}

struct FilterInfo {
  uint32_t filter_id_ = 0;
  std::unique_ptr<const char[]> owner_;
  Slice filter_;
  uint32_t keys_added_ = 0;
  std::unique_ptr<FilterBitsReader> reader_;
  std::unique_ptr<FullFilterBlockReader> full_block_reader_;
  uint64_t outside_queries_ = 0;
  uint64_t false_positives_ = 0;
};

enum TestMode {
  kSingleFilter,
  kBatchPrepared,
  kBatchUnprepared,
  kFiftyOneFilter,
  kEightyTwentyFilter,
  kRandomFilter,
};

static const std::vector<TestMode> allTestModes = {
    kSingleFilter,   kBatchPrepared,      kBatchUnprepared,
    kFiftyOneFilter, kEightyTwentyFilter, kRandomFilter,
};

static const std::vector<TestMode> quickTestModes = {
    kSingleFilter,
    kRandomFilter,
};

const char *TestModeToString(TestMode tm) {
  switch (tm) {
    case kSingleFilter:
      return "Single filter";
    case kBatchPrepared:
      return "Batched, prepared";
    case kBatchUnprepared:
      return "Batched, unprepared";
    case kFiftyOneFilter:
      return "Skewed 50% in 1%";
    case kEightyTwentyFilter:
      return "Skewed 80% in 20%";
    case kRandomFilter:
      return "Random filter";
  }
  return "Bad TestMode";
}

struct FilterBench : public MockBlockBasedTableTester {
  std::vector<KeyMaker> kms_;
  std::vector<FilterInfo> infos_;
  std::mt19937 random_;

  FilterBench()
      : MockBlockBasedTableTester(
            rocksdb::NewBloomFilterPolicy(FLAGS_bits_per_key)),
        random_(FLAGS_seed) {
    for (uint32_t i = 0; i < FLAGS_batch_size; ++i) {
      kms_.emplace_back(FLAGS_key_size < 8 ? 8 : FLAGS_key_size);
    }
  }

  void Go();

  void RandomQueryTest(bool inside, bool dry_run, TestMode mode);
};

void FilterBench::Go() {
  std::unique_ptr<FilterBitsBuilder> builder(
      table_options_.filter_policy->GetFilterBitsBuilder());

  uint32_t variance_mask = 1;
  while (variance_mask * variance_mask * 4 < FLAGS_average_keys_per_filter) {
    variance_mask = variance_mask * 2 + 1;
  }

  const std::vector<TestMode> &testModes =
      FLAGS_quick ? quickTestModes : allTestModes;
  if (FLAGS_quick) {
    FLAGS_m_queries /= 10.0;
  }

  std::cout << "Building..." << std::endl;

  size_t total_memory_used = 0;
  size_t total_keys_added = 0;

  rocksdb::StopWatchNano timer(rocksdb::Env::Default(), true);

  while (total_memory_used < 1024 * 1024 * FLAGS_working_mem_size_mb) {
    uint32_t filter_id = random_();
    uint32_t keys_to_add = FLAGS_average_keys_per_filter +
                           (random_() & variance_mask) - (variance_mask / 2);
    for (uint32_t i = 0; i < keys_to_add; ++i) {
      builder->AddKey(kms_[0].Get(filter_id, i));
    }
    infos_.emplace_back();
    FilterInfo &info = infos_.back();
    info.filter_id_ = filter_id;
    info.filter_ = builder->Finish(&info.owner_);
    info.keys_added_ = keys_to_add;
    info.reader_.reset(
        table_options_.filter_policy->GetFilterBitsReader(info.filter_));
    CachableEntry<BlockContents> block(
        new BlockContents(info.filter_), nullptr /* cache */,
        nullptr /* cache_handle */, true /* own_value */);
    info.full_block_reader_.reset(
        new FullFilterBlockReader(table_.get(), std::move(block)));
    total_memory_used += info.filter_.size();
    total_keys_added += keys_to_add;
  }

  uint64_t elapsed_nanos = timer.ElapsedNanos();
  double ns = double(elapsed_nanos) / total_keys_added;
  std::cout << "Build avg ns/key: " << ns << std::endl;
  std::cout << "Number of filters: " << infos_.size() << std::endl;
  std::cout << "Total memory (MB): " << total_memory_used / 1024.0 / 1024.0
            << std::endl;

  double bpk = total_memory_used * 8.0 / total_keys_added;
  std::cout << "Bits/key actual: " << bpk << std::endl;
  if (!FLAGS_quick) {
    double tolerable_rate = std::pow(2.0, -(bpk - 1.0) / (1.4 + bpk / 50.0));
    std::cout << "Best possible FP rate %: " << 100.0 * std::pow(2.0, -bpk)
              << std::endl;
    std::cout << "Tolerable FP rate %: " << 100.0 * tolerable_rate << std::endl;

    std::cout << "----------------------------" << std::endl;
    std::cout << "Verifying..." << std::endl;

    uint32_t outside_q_per_f = 1000000 / infos_.size();
    uint64_t fps = 0;
    for (uint32_t i = 0; i < infos_.size(); ++i) {
      FilterInfo &info = infos_[i];
      for (uint32_t j = 0; j < info.keys_added_; ++j) {
        ALWAYS_ASSERT(info.reader_->MayMatch(kms_[0].Get(info.filter_id_, j)));
      }
      for (uint32_t j = 0; j < outside_q_per_f; ++j) {
        fps += info.reader_->MayMatch(
            kms_[0].Get(info.filter_id_, j | 0x80000000));
      }
    }
    std::cout << " No FNs :)" << std::endl;
    double prelim_rate = double(fps) / outside_q_per_f / infos_.size();
    std::cout << " Prelim FP rate %: " << (100.0 * prelim_rate) << std::endl;

    if (!FLAGS_allow_bad_fp_rate) {
      ALWAYS_ASSERT(prelim_rate < tolerable_rate);
    }
  }

  std::cout << "----------------------------" << std::endl;
  std::cout << "Inside queries..." << std::endl;
  random_.seed(FLAGS_seed + 1);
  RandomQueryTest(/*inside*/ true, /*dry_run*/ true, kRandomFilter);
  for (TestMode tm : testModes) {
    random_.seed(FLAGS_seed + 1);
    RandomQueryTest(/*inside*/ true, /*dry_run*/ false, tm);
  }

  std::cout << "----------------------------" << std::endl;
  std::cout << "Outside queries..." << std::endl;
  random_.seed(FLAGS_seed + 2);
  RandomQueryTest(/*inside*/ false, /*dry_run*/ true, kRandomFilter);
  for (TestMode tm : testModes) {
    random_.seed(FLAGS_seed + 2);
    RandomQueryTest(/*inside*/ false, /*dry_run*/ false, tm);
  }

  std::cout << "----------------------------" << std::endl;
  std::cout << "Done. (For more info, run with -legend or -help.)" << std::endl;
}

void FilterBench::RandomQueryTest(bool inside, bool dry_run, TestMode mode) {
  for (auto &info : infos_) {
    info.outside_queries_ = 0;
    info.false_positives_ = 0;
  }

  uint32_t dry_run_hash = 0;
  uint64_t max_queries =
      static_cast<uint64_t>(FLAGS_m_queries * 1000000 + 0.50);
  // Some filters may be considered secondary in order to implement skewed
  // queries. num_primary_filters is the number that are to be treated as
  // equal, and any remainder will be treated as secondary.
  size_t num_primary_filters = infos_.size();
  // The proportion (when divided by 2^32 - 1) of filter queries going to
  // the primary filters (default = all). The remainder of queries are
  // against secondary filters.
  uint32_t primary_filter_threshold = 0xffffffff;
  if (mode == kSingleFilter) {
    // 100% of queries to 1 filter
    num_primary_filters = 1;
  } else if (mode == kFiftyOneFilter) {
    // 50% of queries
    primary_filter_threshold /= 2;
    // to 1% of filters
    num_primary_filters = (num_primary_filters + 99) / 100;
  } else if (mode == kEightyTwentyFilter) {
    // 80% of queries
    primary_filter_threshold = primary_filter_threshold / 5 * 4;
    // to 20% of filters
    num_primary_filters = (num_primary_filters + 4) / 5;
  }
  size_t batch_size = 1;
  std::unique_ptr<Slice *[]> batch_slices;
  std::unique_ptr<bool[]> batch_results;
  if (mode == kBatchPrepared || mode == kBatchUnprepared) {
    batch_size = kms_.size();
    batch_slices.reset(new Slice *[batch_size]);
    batch_results.reset(new bool[batch_size]);
    for (size_t i = 0; i < batch_size; ++i) {
      batch_slices[i] = &kms_[i].slice_;
      batch_results[i] = false;
    }
  }

  rocksdb::StopWatchNano timer(rocksdb::Env::Default(), true);

  for (uint64_t q = 0; q < max_queries; q += batch_size) {
    uint32_t filter_index;
    if (random_() <= primary_filter_threshold) {
      filter_index = fastrange32(num_primary_filters, random_());
    } else {
      // secondary
      filter_index =
          num_primary_filters +
          fastrange32(infos_.size() - num_primary_filters, random_());
    }
    FilterInfo &info = infos_[filter_index];
    for (size_t i = 0; i < batch_size; ++i) {
      if (inside) {
        kms_[i].Get(info.filter_id_, fastrange32(info.keys_added_, random_()));
      } else {
        kms_[i].Get(info.filter_id_, random_() | 0x80000000);
        info.outside_queries_++;
      }
    }
    // TODO: implement batched interface to full block reader
    if (mode == kBatchPrepared && !dry_run && !FLAGS_use_full_block_reader) {
      for (size_t i = 0; i < batch_size; ++i) {
        batch_results[i] = false;
      }
      info.reader_->MayMatch(batch_size, batch_slices.get(),
                             batch_results.get());
      for (size_t i = 0; i < batch_size; ++i) {
        if (inside) {
          ALWAYS_ASSERT(batch_results[i]);
        } else {
          info.false_positives_ += batch_results[i];
        }
      }
    } else {
      for (size_t i = 0; i < batch_size; ++i) {
        if (dry_run) {
          dry_run_hash ^= rocksdb::BloomHash(kms_[i].slice_);
        } else {
          bool may_match;
          if (FLAGS_use_full_block_reader) {
            may_match = info.full_block_reader_->KeyMayMatch(
                kms_[i].slice_,
                /*prefix_extractor=*/nullptr,
                /*block_offset=*/rocksdb::kNotValid,
                /*no_io=*/false, /*const_ikey_ptr=*/nullptr,
                /*get_context=*/nullptr,
                /*lookup_context=*/nullptr);
          } else {
            may_match = info.reader_->MayMatch(kms_[i].slice_);
          }
          if (inside) {
            ALWAYS_ASSERT(may_match);
          } else {
            info.false_positives_ += may_match;
          }
        }
      }
    }
  }

  uint64_t elapsed_nanos = timer.ElapsedNanos();
  double ns = double(elapsed_nanos) / max_queries;

  if (dry_run) {
    // Printing part of hash prevents dry run components from being optimized
    // away by compiler
    std::cout << "  Dry run (" << std::hex << (dry_run_hash & 0xfff) << std::dec
              << ") ";
  } else {
    std::cout << "  " << TestModeToString(mode) << " ";
  }
  std::cout << "ns/op: " << ns << std::endl;

  if (!inside && !dry_run && mode == kRandomFilter) {
    uint64_t q = 0;
    uint64_t fp = 0;
    double worst_fp_rate = 0.0;
    double best_fp_rate = 1.0;
    for (auto &info : infos_) {
      q += info.outside_queries_;
      fp += info.false_positives_;
      if (info.outside_queries_ > 0) {
        double fp_rate = double(info.false_positives_) / info.outside_queries_;
        worst_fp_rate = std::max(worst_fp_rate, fp_rate);
        best_fp_rate = std::min(best_fp_rate, fp_rate);
      }
    }
    std::cout << "    Average FP rate %: " << 100.0 * fp / q << std::endl;
    if (!FLAGS_quick) {
      std::cout << "    Worst   FP rate %: " << 100.0 * worst_fp_rate
                << std::endl;
      std::cout << "    Best    FP rate %: " << 100.0 * best_fp_rate
                << std::endl;
      std::cout << "    Best possible bits/key: "
                << -std::log(double(fp) / q) / std::log(2.0) << std::endl;
    }
  }
}

int main(int argc, char **argv) {
  rocksdb::port::InstallStackTraceHandler();
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  " [-quick] [OTHER OPTIONS]...");
  ParseCommandLineFlags(&argc, &argv, true);

  PrintWarnings();

  if (FLAGS_legend) {
    std::cout
        << "Legend:" << std::endl
        << "  \"Inside\" - key that was added to filter" << std::endl
        << "  \"Outside\" - key that was not added to filter" << std::endl
        << "  \"FN\" - false negative query (must not happen)" << std::endl
        << "  \"FP\" - false positive query (OK at low rate)" << std::endl
        << "  \"Dry run\" - cost of testing and hashing overhead. Consider"
        << "\n     subtracting this cost from the others." << std::endl
        << "  \"Single filter\" - essentially minimum cost, assuming filter"
        << "\n     fits easily in L1 CPU cache." << std::endl
        << "  \"Batched, prepared\" - several queries at once against a"
        << "\n     randomly chosen filter, using multi-query interface."
        << std::endl
        << "  \"Batched, unprepared\" - similar, but using serial calls"
        << "\n     to single query interface." << std::endl
        << "  \"Random filter\" - a filter is chosen at random as target"
        << "\n     of each query." << std::endl
        << "  \"Skewed X% in Y%\" - like \"Random filter\" except Y% of"
        << "\n      the filters are designated as \"hot\" and receive X%"
        << "\n      of queries." << std::endl;
  } else {
    FilterBench b;
    b.Go();
  }

  return 0;
}

#endif  // GFLAGS
