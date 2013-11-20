#include <algorithm>
#include <iostream>
#include <vector>

#include <gflags/gflags.h>
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/perf_context.h"
#include "util/histogram.h"
#include "util/stop_watch.h"
#include "util/testharness.h"

DEFINE_bool(use_prefix_hash_memtable, true, "");
DEFINE_bool(use_nolock_version, true, "");
DEFINE_bool(trigger_deadlock, false,
            "issue delete in range scan to trigger PrefixHashMap deadlock");
DEFINE_uint64(bucket_count, 100000, "number of buckets");
DEFINE_uint64(num_locks, 10001, "number of locks");
DEFINE_bool(random_prefix, false, "randomize prefix");
DEFINE_uint64(total_prefixes, 1000, "total number of prefixes");
DEFINE_uint64(items_per_prefix, 10, "total number of values per prefix");
DEFINE_int64(write_buffer_size, 1000000000, "");
DEFINE_int64(max_write_buffer_number, 8, "");
DEFINE_int64(min_write_buffer_number_to_merge, 7, "");

// Path to the database on file system
const std::string kDbName = rocksdb::test::TmpDir() + "/prefix_test";

namespace rocksdb {

struct TestKey {
  uint64_t prefix;
  uint64_t sorted;

  TestKey(uint64_t prefix, uint64_t sorted) : prefix(prefix), sorted(sorted) {}
};

// return a slice backed by test_key
inline Slice TestKeyToSlice(const TestKey& test_key) {
  return Slice((const char*)&test_key, sizeof(test_key));
}

inline const TestKey* SliceToTestKey(const Slice& slice) {
  return (const TestKey*)slice.data();
}

class TestKeyComparator : public Comparator {
 public:

  // Compare needs to be aware of the possibility of a and/or b is
  // prefix only
  virtual int Compare(const Slice& a, const Slice& b) const {
    const TestKey* key_a = SliceToTestKey(a);
    const TestKey* key_b = SliceToTestKey(b);
    if (key_a->prefix != key_b->prefix) {
      if (key_a->prefix < key_b->prefix) return -1;
      if (key_a->prefix > key_b->prefix) return 1;
    } else {
      ASSERT_TRUE(key_a->prefix == key_b->prefix);
      // note, both a and b could be prefix only
      if (a.size() != b.size()) {
        // one of them is prefix
        ASSERT_TRUE(
          (a.size() == sizeof(uint64_t) && b.size() == sizeof(TestKey)) ||
          (b.size() == sizeof(uint64_t) && a.size() == sizeof(TestKey)));
        if (a.size() < b.size()) return -1;
        if (a.size() > b.size()) return 1;
      } else {
        // both a and b are prefix
        if (a.size() == sizeof(uint64_t)) {
          return 0;
        }

        // both a and b are whole key
        ASSERT_TRUE(a.size() == sizeof(TestKey) && b.size() == sizeof(TestKey));
        if (key_a->sorted < key_b->sorted) return -1;
        if (key_a->sorted > key_b->sorted) return 1;
        if (key_a->sorted == key_b->sorted) return 0;
      }
    }
    return 0;
  }

  virtual const char* Name() const override {
    return "TestKeyComparator";
  }

  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
  }

  virtual void FindShortSuccessor(std::string* key) const {}

};

class PrefixTest {
 public:
  std::shared_ptr<DB> OpenDb() {
    DB* db;

    options.create_if_missing = true;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_write_buffer_number = FLAGS_max_write_buffer_number;
    options.min_write_buffer_number_to_merge =
      FLAGS_min_write_buffer_number_to_merge;

    options.comparator = new TestKeyComparator();
    if (FLAGS_use_prefix_hash_memtable) {
      auto prefix_extractor = NewFixedPrefixTransform(8);
      options.prefix_extractor = prefix_extractor;
      if (FLAGS_use_nolock_version) {
        options.memtable_factory.reset(NewHashSkipListRepFactory(
            prefix_extractor, FLAGS_bucket_count));
      } else {
        options.memtable_factory =
          std::make_shared<rocksdb::PrefixHashRepFactory>(
            prefix_extractor, FLAGS_bucket_count, FLAGS_num_locks);
      }
    }

    Status s = DB::Open(options, kDbName,  &db);
    ASSERT_OK(s);
    return std::shared_ptr<DB>(db);
  }
  ~PrefixTest() {
    delete options.comparator;
  }
 protected:
  Options options;
};

TEST(PrefixTest, DynamicPrefixIterator) {

  DestroyDB(kDbName, Options());
  auto db = OpenDb();
  WriteOptions write_options;
  ReadOptions read_options;

  std::vector<uint64_t> prefixes;
  for (uint64_t i = 0; i < FLAGS_total_prefixes; ++i) {
    prefixes.push_back(i);
  }

  if (FLAGS_random_prefix) {
    std::random_shuffle(prefixes.begin(), prefixes.end());
  }

  // insert x random prefix, each with y continuous element.
  for (auto prefix : prefixes) {
     for (uint64_t sorted = 0; sorted < FLAGS_items_per_prefix; sorted++) {
      TestKey test_key(prefix, sorted);

      Slice key = TestKeyToSlice(test_key);
      std::string value = "v" + std::to_string(sorted);

      ASSERT_OK(db->Put(write_options, key, value));
    }
  }

  // test seek existing keys
  HistogramImpl hist_seek_time;
  HistogramImpl hist_seek_comparison;

  if (FLAGS_use_prefix_hash_memtable) {
    read_options.prefix_seek = true;
  }
  std::unique_ptr<Iterator> iter(db->NewIterator(read_options));

  for (auto prefix : prefixes) {
    TestKey test_key(prefix, FLAGS_items_per_prefix / 2);
    Slice key = TestKeyToSlice(test_key);
    std::string value = "v" + std::to_string(0);

    perf_context.Reset();
    StopWatchNano timer(Env::Default(), true);
    uint64_t total_keys = 0;
    for (iter->Seek(key); iter->Valid(); iter->Next()) {
      if (FLAGS_trigger_deadlock) {
        std::cout << "Behold the deadlock!\n";
        db->Delete(write_options, iter->key());
      }
      auto test_key = SliceToTestKey(iter->key());
      if (test_key->prefix != prefix) break;
      total_keys++;
    }
    hist_seek_time.Add(timer.ElapsedNanos());
    hist_seek_comparison.Add(perf_context.user_key_comparison_count);
    ASSERT_EQ(total_keys, FLAGS_items_per_prefix - FLAGS_items_per_prefix/2);
  }

  std::cout << "Seek key comparison: \n"
            << hist_seek_comparison.ToString()
            << "Seek time: \n"
            << hist_seek_time.ToString();

  // test non-existing keys
  HistogramImpl hist_no_seek_time;
  HistogramImpl hist_no_seek_comparison;

  for (auto prefix = FLAGS_total_prefixes;
       prefix < FLAGS_total_prefixes + 100;
       prefix++) {
    TestKey test_key(prefix, 0);
    Slice key = TestKeyToSlice(test_key);

    perf_context.Reset();
    StopWatchNano timer(Env::Default(), true);
    iter->Seek(key);
    hist_no_seek_time.Add(timer.ElapsedNanos());
    hist_no_seek_comparison.Add(perf_context.user_key_comparison_count);
    ASSERT_TRUE(!iter->Valid());
  }

  std::cout << "non-existing Seek key comparison: \n"
            << hist_no_seek_comparison.ToString()
            << "non-existing Seek time: \n"
            << hist_no_seek_time.ToString();
}

TEST(PrefixTest, PrefixHash) {

  DestroyDB(kDbName, Options());
  auto db = OpenDb();
  WriteOptions write_options;
  ReadOptions read_options;

  std::vector<uint64_t> prefixes;
  for (uint64_t i = 0; i < FLAGS_total_prefixes; ++i) {
    prefixes.push_back(i);
  }

  if (FLAGS_random_prefix) {
    std::random_shuffle(prefixes.begin(), prefixes.end());
  }

  // insert x random prefix, each with y continuous element.
  HistogramImpl hist_put_time;
  HistogramImpl hist_put_comparison;

  for (auto prefix : prefixes) {
     for (uint64_t sorted = 0; sorted < FLAGS_items_per_prefix; sorted++) {
      TestKey test_key(prefix, sorted);

      Slice key = TestKeyToSlice(test_key);
      std::string value = "v" + std::to_string(sorted);

      perf_context.Reset();
      StopWatchNano timer(Env::Default(), true);
      ASSERT_OK(db->Put(write_options, key, value));
      hist_put_time.Add(timer.ElapsedNanos());
      hist_put_comparison.Add(perf_context.user_key_comparison_count);
    }
  }

  std::cout << "Put key comparison: \n" << hist_put_comparison.ToString()
            << "Put time: \n" << hist_put_time.ToString();


  // test seek existing keys
  HistogramImpl hist_seek_time;
  HistogramImpl hist_seek_comparison;

  for (auto prefix : prefixes) {
    TestKey test_key(prefix, 0);
    Slice key = TestKeyToSlice(test_key);
    std::string value = "v" + std::to_string(0);

    Slice key_prefix;
    if (FLAGS_use_prefix_hash_memtable) {
      key_prefix = options.prefix_extractor->Transform(key);
      read_options.prefix = &key_prefix;
    }
    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));

    perf_context.Reset();
    StopWatchNano timer(Env::Default(), true);
    uint64_t total_keys = 0;
    for (iter->Seek(key); iter->Valid(); iter->Next()) {
      if (FLAGS_trigger_deadlock) {
        std::cout << "Behold the deadlock!\n";
        db->Delete(write_options, iter->key());
      }
      auto test_key = SliceToTestKey(iter->key());
      if (test_key->prefix != prefix) break;
      total_keys++;
    }
    hist_seek_time.Add(timer.ElapsedNanos());
    hist_seek_comparison.Add(perf_context.user_key_comparison_count);
    ASSERT_EQ(total_keys, FLAGS_items_per_prefix);
  }

  std::cout << "Seek key comparison: \n"
            << hist_seek_comparison.ToString()
            << "Seek time: \n"
            << hist_seek_time.ToString();

  // test non-existing keys
  HistogramImpl hist_no_seek_time;
  HistogramImpl hist_no_seek_comparison;

  for (auto prefix = FLAGS_total_prefixes;
       prefix < FLAGS_total_prefixes + 100;
       prefix++) {
    TestKey test_key(prefix, 0);
    Slice key = TestKeyToSlice(test_key);

    if (FLAGS_use_prefix_hash_memtable) {
      Slice key_prefix = options.prefix_extractor->Transform(key);
      read_options.prefix = &key_prefix;
    }
    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));

    perf_context.Reset();
    StopWatchNano timer(Env::Default(), true);
    iter->Seek(key);
    hist_no_seek_time.Add(timer.ElapsedNanos());
    hist_no_seek_comparison.Add(perf_context.user_key_comparison_count);
    ASSERT_TRUE(!iter->Valid());
  }

  std::cout << "non-existing Seek key comparison: \n"
            << hist_no_seek_comparison.ToString()
            << "non-existing Seek time: \n"
            << hist_no_seek_time.ToString();
}

}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  std::cout << kDbName << "\n";

  rocksdb::test::RunAllTests();
  return 0;
}
