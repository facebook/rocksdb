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

#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "file/random_access_file_reader.h"
#include "monitoring/histogram.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/plain/plain_table_factory.h"
#include "table/table_builder.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/gflags_compat.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::SetUsageMessage;

namespace ROCKSDB_NAMESPACE {

namespace {
// Make a key that i determines the first 4 characters and j determines the
// last 4 characters.
static std::string MakeKey(int i, int j, bool through_db) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%04d__key___%04d", i, j);
  if (through_db) {
    return std::string(buf);
  }
  // If we directly query table, which operates on internal keys
  // instead of user keys, we need to add 8 bytes of internal
  // information (row type etc) to user key to make an internal
  // key.
  InternalKey key(std::string(buf), 0, ValueType::kTypeValue);
  return key.Encode().ToString();
}

uint64_t Now(SystemClock* clock, bool measured_by_nanosecond) {
  return measured_by_nanosecond ? clock->NowNanos() : clock->NowMicros();
}
}  // namespace

// A very simple benchmark that.
// Create a table with roughly numKey1 * numKey2 keys,
// where there are numKey1 prefixes of the key, each has numKey2 number of
// distinguished key, differing in the suffix part.
// If if_query_empty_keys = false, query the existing keys numKey1 * numKey2
// times randomly.
// If if_query_empty_keys = true, query numKey1 * numKey2 random empty keys.
// Print out the total time.
// If through_db=true, a full DB will be created and queries will be against
// it. Otherwise, operations will be directly through table level.
//
// If for_terator=true, instead of just query one key each time, it queries
// a range sharing the same prefix.
namespace {
void TableReaderBenchmark(Options& opts, EnvOptions& env_options,
                          ReadOptions& read_options, int num_keys1,
                          int num_keys2, int num_iter, int /*prefix_len*/,
                          bool if_query_empty_keys, bool for_iterator,
                          bool through_db, bool measured_by_nanosecond) {
  ROCKSDB_NAMESPACE::InternalKeyComparator ikc(opts.comparator);

  std::string file_name =
      test::PerThreadDBPath("rocksdb_table_reader_benchmark");
  std::string dbname = test::PerThreadDBPath("rocksdb_table_reader_bench_db");
  WriteOptions wo;
  Env* env = Env::Default();
  auto* clock = env->GetSystemClock().get();
  TableBuilder* tb = nullptr;
  DB* db = nullptr;
  Status s;
  const ImmutableCFOptions ioptions(opts);
  const ColumnFamilyOptions cfo(opts);
  const MutableCFOptions moptions(cfo);
  std::unique_ptr<WritableFileWriter> file_writer;
  if (!through_db) {
    ASSERT_OK(WritableFileWriter::Create(env->GetFileSystem(), file_name,
                                         FileOptions(env_options), &file_writer,
                                         nullptr));

    std::vector<std::unique_ptr<IntTblPropCollectorFactory> >
        int_tbl_prop_collector_factories;

    int unknown_level = -1;
    tb = opts.table_factory->NewTableBuilder(
        TableBuilderOptions(
            ioptions, moptions, ikc, &int_tbl_prop_collector_factories,
            CompressionType::kNoCompression, 0 /* sample_for_compression */,
            CompressionOptions(), false /* skip_filters */,
            kDefaultColumnFamilyName, unknown_level),
        0 /* column_family_id */, file_writer.get());
  } else {
    s = DB::Open(opts, dbname, &db);
    ASSERT_OK(s);
    ASSERT_TRUE(db != nullptr);
  }
  // Populate slightly more than 1M keys
  for (int i = 0; i < num_keys1; i++) {
    for (int j = 0; j < num_keys2; j++) {
      std::string key = MakeKey(i * 2, j, through_db);
      if (!through_db) {
        tb->Add(key, key);
      } else {
        db->Put(wo, key, key);
      }
    }
  }
  if (!through_db) {
    tb->Finish();
    file_writer->Close();
  } else {
    db->Flush(FlushOptions());
  }

  std::unique_ptr<TableReader> table_reader;
  if (!through_db) {
    const auto& fs = env->GetFileSystem();
    FileOptions fopts(env_options);

    std::unique_ptr<FSRandomAccessFile> raf;
    s = fs->NewRandomAccessFile(file_name, fopts, &raf, nullptr);
    if (!s.ok()) {
      fprintf(stderr, "Create File Error: %s\n", s.ToString().c_str());
      exit(1);
    }
    uint64_t file_size;
    fs->GetFileSize(file_name, fopts.io_options, &file_size, nullptr);
    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(raf), file_name));
    s = opts.table_factory->NewTableReader(
        TableReaderOptions(ioptions, moptions.prefix_extractor.get(),
                           env_options, ikc),
        std::move(file_reader), file_size, &table_reader);
    if (!s.ok()) {
      fprintf(stderr, "Open Table Error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  Random rnd(301);
  std::string result;
  HistogramImpl hist;

  for (int it = 0; it < num_iter; it++) {
    for (int i = 0; i < num_keys1; i++) {
      for (int j = 0; j < num_keys2; j++) {
        int r1 = rnd.Uniform(num_keys1) * 2;
        int r2 = rnd.Uniform(num_keys2);
        if (if_query_empty_keys) {
          r1++;
          r2 = num_keys2 * 2 - r2;
        }

        if (!for_iterator) {
          // Query one existing key;
          std::string key = MakeKey(r1, r2, through_db);
          uint64_t start_time = Now(clock, measured_by_nanosecond);
          if (!through_db) {
            PinnableSlice value;
            MergeContext merge_context;
            SequenceNumber max_covering_tombstone_seq = 0;
            GetContext get_context(ioptions.user_comparator,
                                   ioptions.merge_operator, ioptions.info_log,
                                   ioptions.statistics, GetContext::kNotFound,
                                   Slice(key), &value, nullptr, &merge_context,
                                   true, &max_covering_tombstone_seq, clock);
            s = table_reader->Get(read_options, key, &get_context, nullptr);
          } else {
            s = db->Get(read_options, key, &result);
          }
          hist.Add(Now(clock, measured_by_nanosecond) - start_time);
        } else {
          int r2_len;
          if (if_query_empty_keys) {
            r2_len = 0;
          } else {
            r2_len = rnd.Uniform(num_keys2) + 1;
            if (r2_len + r2 > num_keys2) {
              r2_len = num_keys2 - r2;
            }
          }
          std::string start_key = MakeKey(r1, r2, through_db);
          std::string end_key = MakeKey(r1, r2 + r2_len, through_db);
          uint64_t total_time = 0;
          uint64_t start_time = Now(clock, measured_by_nanosecond);
          Iterator* iter = nullptr;
          InternalIterator* iiter = nullptr;
          if (!through_db) {
            iiter = table_reader->NewIterator(
                read_options, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
                /*skip_filters=*/false, TableReaderCaller::kUncategorized);
          } else {
            iter = db->NewIterator(read_options);
          }
          int count = 0;
          for (through_db ? iter->Seek(start_key) : iiter->Seek(start_key);
               through_db ? iter->Valid() : iiter->Valid();
               through_db ? iter->Next() : iiter->Next()) {
            if (if_query_empty_keys) {
              break;
            }
            // verify key;
            total_time += Now(clock, measured_by_nanosecond) - start_time;
            assert(Slice(MakeKey(r1, r2 + count, through_db)) ==
                   (through_db ? iter->key() : iiter->key()));
            start_time = Now(clock, measured_by_nanosecond);
            if (++count >= r2_len) {
              break;
            }
          }
          if (count != r2_len) {
            fprintf(
                stderr, "Iterator cannot iterate expected number of entries. "
                "Expected %d but got %d\n", r2_len, count);
            assert(false);
          }
          delete iter;
          total_time += Now(clock, measured_by_nanosecond) - start_time;
          hist.Add(total_time);
        }
      }
    }
  }

  fprintf(
      stderr,
      "==================================================="
      "====================================================\n"
      "InMemoryTableSimpleBenchmark: %20s   num_key1:  %5d   "
      "num_key2: %5d  %10s\n"
      "==================================================="
      "===================================================="
      "\nHistogram (unit: %s): \n%s",
      opts.table_factory->Name(), num_keys1, num_keys2,
      for_iterator ? "iterator" : (if_query_empty_keys ? "empty" : "non_empty"),
      measured_by_nanosecond ? "nanosecond" : "microsecond",
      hist.ToString().c_str());
  if (!through_db) {
    env->DeleteFile(file_name);
  } else {
    delete db;
    db = nullptr;
    DestroyDB(dbname, opts);
  }
}
}  // namespace
}  // namespace ROCKSDB_NAMESPACE

DEFINE_bool(query_empty, false, "query non-existing keys instead of existing "
            "ones.");
DEFINE_int32(num_keys1, 4096, "number of distinguish prefix of keys");
DEFINE_int32(num_keys2, 512, "number of distinguish keys for each prefix");
DEFINE_int32(iter, 3, "query non-existing keys instead of existing ones");
DEFINE_int32(prefix_len, 16, "Prefix length used for iterators and indexes");
DEFINE_bool(iterator, false, "For test iterator");
DEFINE_bool(through_db, false, "If enable, a DB instance will be created and "
            "the query will be against DB. Otherwise, will be directly against "
            "a table reader.");
DEFINE_bool(mmap_read, true, "Whether use mmap read");
DEFINE_string(table_factory, "block_based",
              "Table factory to use: `block_based` (default), `plain_table` or "
              "`cuckoo_hash`.");
DEFINE_string(time_unit, "microsecond",
              "The time unit used for measuring performance. User can specify "
              "`microsecond` (default) or `nanosecond`");

int main(int argc, char** argv) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
  ParseCommandLineFlags(&argc, &argv, true);

  std::shared_ptr<ROCKSDB_NAMESPACE::TableFactory> tf;
  ROCKSDB_NAMESPACE::Options options;
  if (FLAGS_prefix_len < 16) {
    options.prefix_extractor.reset(
        ROCKSDB_NAMESPACE::NewFixedPrefixTransform(FLAGS_prefix_len));
  }
  ROCKSDB_NAMESPACE::ReadOptions ro;
  ROCKSDB_NAMESPACE::EnvOptions env_options;
  options.create_if_missing = true;
  options.compression = ROCKSDB_NAMESPACE::CompressionType::kNoCompression;

  if (FLAGS_table_factory == "cuckoo_hash") {
#ifndef ROCKSDB_LITE
    options.allow_mmap_reads = FLAGS_mmap_read;
    env_options.use_mmap_reads = FLAGS_mmap_read;
    ROCKSDB_NAMESPACE::CuckooTableOptions table_options;
    table_options.hash_table_ratio = 0.75;
    tf.reset(ROCKSDB_NAMESPACE::NewCuckooTableFactory(table_options));
#else
    fprintf(stderr, "Plain table is not supported in lite mode\n");
    exit(1);
#endif  // ROCKSDB_LITE
  } else if (FLAGS_table_factory == "plain_table") {
#ifndef ROCKSDB_LITE
    options.allow_mmap_reads = FLAGS_mmap_read;
    env_options.use_mmap_reads = FLAGS_mmap_read;

    ROCKSDB_NAMESPACE::PlainTableOptions plain_table_options;
    plain_table_options.user_key_len = 16;
    plain_table_options.bloom_bits_per_key = (FLAGS_prefix_len == 16) ? 0 : 8;
    plain_table_options.hash_table_ratio = 0.75;

    tf.reset(new ROCKSDB_NAMESPACE::PlainTableFactory(plain_table_options));
    options.prefix_extractor.reset(
        ROCKSDB_NAMESPACE::NewFixedPrefixTransform(FLAGS_prefix_len));
#else
    fprintf(stderr, "Cuckoo table is not supported in lite mode\n");
    exit(1);
#endif  // ROCKSDB_LITE
  } else if (FLAGS_table_factory == "block_based") {
    tf.reset(new ROCKSDB_NAMESPACE::BlockBasedTableFactory());
  } else {
    fprintf(stderr, "Invalid table type %s\n", FLAGS_table_factory.c_str());
  }

  if (tf) {
    // if user provides invalid options, just fall back to microsecond.
    bool measured_by_nanosecond = FLAGS_time_unit == "nanosecond";

    options.table_factory = tf;
    ROCKSDB_NAMESPACE::TableReaderBenchmark(
        options, env_options, ro, FLAGS_num_keys1, FLAGS_num_keys2, FLAGS_iter,
        FLAGS_prefix_len, FLAGS_query_empty, FLAGS_iterator, FLAGS_through_db,
        measured_by_nanosecond);
  } else {
    return 1;
  }

  return 0;
}

#endif  // GFLAGS
