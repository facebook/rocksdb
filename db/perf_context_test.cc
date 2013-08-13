#include <iostream>

#include "leveldb/db.h"
#include "leveldb/perf_context.h"
#include "util/testharness.h"


namespace leveldb {

// Path to the database on file system
const std::string kDbName = test::TmpDir() + "/perf_context_test";

std::shared_ptr<DB> OpenDb() {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 1000000000;     // give it a big memtable
    Status s = DB::Open(options, kDbName,  &db);
    ASSERT_OK(s);
    return std::shared_ptr<DB>(db);
}

class PerfContextTest { };

int kTotalKeys = 100;

TEST(PerfContextTest, KeyComparisonCount) {

  DestroyDB(kDbName, Options());    // Start this test with a fresh DB

  auto db = OpenDb();

  WriteOptions write_options;
  ReadOptions read_options;

  uint64_t total_user_key_comparison_get = 0;
  uint64_t total_user_key_comparison_put = 0;
  uint64_t max_user_key_comparison_get = 0;

  std::cout << "Inserting " << kTotalKeys << " key/value pairs\n...\n";

  for (int i = 0; i < kTotalKeys; ++i) {
    std::string key = "k" + std::to_string(i);
    std::string value = "v" + std::to_string(i);

    perf_context.Reset();
    db->Put(write_options, key, value);
    total_user_key_comparison_put += perf_context.user_key_comparison_count;

    perf_context.Reset();
    db->Get(read_options, key, &value);
    total_user_key_comparison_get += perf_context.user_key_comparison_count;
    max_user_key_comparison_get =
      std::max(max_user_key_comparison_get,
               perf_context.user_key_comparison_count);
  }

  std::cout << "total user key comparison get: "
            << total_user_key_comparison_get << "\n"
            << "total user key comparison put: "
            << total_user_key_comparison_put << "\n"
            << "max user key comparison get: "
            << max_user_key_comparison_get << "\n"
            << "avg user key comparison get:"
            << total_user_key_comparison_get/kTotalKeys << "\n";

}


}


int main(int argc, char** argv) {

  if (argc > 1) {
    leveldb::kTotalKeys = std::stoi(argv[1]);
  }

  leveldb::test::RunAllTests();
  return 0;
}
