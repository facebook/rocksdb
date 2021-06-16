// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>

#include "db/db_test_util.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

// ======= General Information ======= (from GitHub Wiki).
// There are three scenarios where memtable flush can be triggered:
//
// 1 - Memtable size exceeds ColumnFamilyOptions::write_buffer_size
//     after a write.
// 2 - Total memtable size across all column families exceeds
// DBOptions::db_write_buffer_size,
//     or DBOptions::write_buffer_manager signals a flush. In this scenario
//     the largest memtable will be flushed.
// 3 - Total WAL file size exceeds DBOptions::max_total_wal_size.
//     In this scenario the memtable with the oldest data will be flushed,
//     in order to allow the WAL file with data from this memtable to be
//     purged.
//
// As a result, a memtable can be flushed before it is full. This is one
// reason the generated SST file can be smaller than the corresponding
// memtable. Compression is another factor to make SST file smaller than
// corresponding memtable, since data in memtable is uncompressed.

class MemtableGarbageTest : public DBTestBase {
 public:
  MemtableGarbageTest() : DBTestBase("/memtable_garbage_test", true) {}
};

TEST_F(MemtableGarbageTest, Basic) {
  Options options = CurrentOptions();

  // The following options are used to enforce several values that
  // may already exist as default values to make this test resilient
  // to default value updates in the future.
  options.statistics = CreateDBStatistics();

  // Record all statistics.
  options.statistics->set_stats_level(StatsLevel::kAll);

  // create the DB if it's not already present
  options.create_if_missing = true;

  // Useful for now as we are trying to compare uncompressed data savings on
  // flush().
  options.compression = kNoCompression;

  // Prevent memtable in place updates. Should already be disabled
  // (from Wiki:
  //  In place updates can be enabled by toggling on the bool
  //  inplace_update_support flag. However, this flag is by default set to
  //  false
  //  because this thread-safe in-place update support is not compatible
  //  with concurrent memtable writes. Note that the bool
  //  allow_concurrent_memtable_write is set to true by default )
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 64MB (64MB = 67108864 bytes).
  options.write_buffer_size = 64 << 20;

  ASSERT_OK(TryReopen(options));

  // Put multiple times the same key-values.
  // The encoded length of a db entry in the memtable is
  // defined in db/memtable.cc (MemTable::Add) as the variable:
  // encoded_len=  VarintLength(internal_key_size)  --> =
  // log_256(internal_key).
  // Min # of bytes
  //                                                       necessary to
  //                                                       store
  //                                                       internal_key_size.
  //             + internal_key_size                --> = actual key string,
  //             (size key_size: w/o term null char)
  //                                                      + 8 bytes for
  //                                                      fixed uint64 "seq
  //                                                      number
  // +
  //                                                      insertion type"
  //             + VarintLength(val_size)           --> = min # of bytes to
  //             store val_size
  //             + val_size                         --> = actual value
  //             string
  // For example, in our situation, "key1" : size 4, "value1" : size 6
  // (the terminating null characters are not copied over to the memtable).
  // And therefore encoded_len = 1 + (4+8) + 1 + 6 = 20 bytes per entry.
  // However in terms of raw data contained in the memtable, and written
  // over to the SSTable, we only count internal_key_size and val_size,
  // because this is the only raw chunk of bytes that contains everything
  // necessary to reconstruct a user entry: sequence number, insertion type,
  // key, and value.

  // To test the relevance of our Memtable garbage statistics,
  // namely MEMTABLE_DATA_BYTES and MEMTABLE_GARBAGE_BYTES,
  // we insert K-V pairs with 3 distinct keys (of length 4),
  // and random values of arbitrary length RAND_VALUES_LENGTH,
  // and we repeat this step NUM_REPEAT times total.
  // At the end, we insert 3 final K-V pairs with the same 3 keys
  // and known values (these will be the final values, of length 6).
  // I chose NUM_REPEAT=2,000 such that no automatic flush is
  // triggered (the number of bytes in the memtable is therefore
  // well below any meaningful heuristic for a memtable of size 64MB).
  // As a result, since each K-V pair is inserted as a payload
  // of N meaningful bytes (sequence number, insertion type,
  // key, and value = 8 + 4 + RAND_VALUE_LENGTH),
  // MEMTABLE_GARBAGE_BYTES should be equal to 2,000 * N bytes
  // and MEMTABLE_DATA_BYTES = MEMTABLE_GARBAGE_BYTES + (3*(8 + 4 + 6))
  // bytes.
  // For RAND_VALUE_LENGTH = 172 (arbitrary value), we expect:
  //      N = 8 + 4 + 172 = 184 bytes
  //      MEMTABLE_GARBAGE_BYTES = 2,000 * 184 = 368,000 bytes.
  //      MEMTABLE_DATA_BYTES = 368,000 + 3*18 = 368,054 bytes.

  const size_t NUM_REPEAT = 2000;
  const size_t RAND_VALUES_LENGTH = 172;
  const std::string KEY1 = "key1";
  const std::string KEY2 = "key2";
  const std::string KEY3 = "key3";
  const std::string VALUE1 = "value1";
  const std::string VALUE2 = "value2";
  const std::string VALUE3 = "value3";
  uint64_t EXPECTED_MEMTABLE_DATA_BYTES = 0;
  uint64_t EXPECTED_MEMTABLE_GARBAGE_BYTES = 0;

  Random rnd(301);
  // Insertion of of K-V pairs, multiple times.
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    std::string p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEY1, p_v1));
    ASSERT_OK(Put(KEY2, p_v2));
    ASSERT_OK(Put(KEY3, p_v3));
    EXPECTED_MEMTABLE_GARBAGE_BYTES +=
        KEY1.size() + p_v1.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES +=
        KEY2.size() + p_v2.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES +=
        KEY3.size() + p_v3.size() + sizeof(uint64_t);
  }

  // The memtable data bytes includes the "garbage"
  // bytes along with the useful payload.
  EXPECTED_MEMTABLE_DATA_BYTES = EXPECTED_MEMTABLE_GARBAGE_BYTES;

  ASSERT_OK(Put(KEY1, VALUE1));
  ASSERT_OK(Put(KEY2, VALUE2));
  ASSERT_OK(Put(KEY3, VALUE3));

  // Add useful payload to the memtable data bytes:
  EXPECTED_MEMTABLE_DATA_BYTES += KEY1.size() + VALUE1.size() + KEY2.size() +
                                  VALUE2.size() + KEY3.size() + VALUE3.size() +
                                  3 * sizeof(uint64_t);

  // We assert that the last K-V pairs have been successfully inserted,
  // and that the valid values are VALUE1, VALUE2, VALUE3.
  PinnableSlice value;
  ASSERT_OK(Get(KEY1, &value));
  ASSERT_EQ(value.ToString(), VALUE1);
  ASSERT_OK(Get(KEY2, &value));
  ASSERT_EQ(value.ToString(), VALUE2);
  ASSERT_OK(Get(KEY3, &value));
  ASSERT_EQ(value.ToString(), VALUE3);

  // Force flush to SST. Increments the statistics counter.
  ASSERT_OK(Flush());

  // Collect statistics.
  uint64_t mem_data_bytes = TestGetTickerCount(options, MEMTABLE_DATA_BYTES);
  uint64_t mem_garbage_bytes =
      TestGetTickerCount(options, MEMTABLE_GARBAGE_BYTES);

  EXPECT_EQ(mem_data_bytes, EXPECTED_MEMTABLE_DATA_BYTES);
  EXPECT_EQ(mem_garbage_bytes, EXPECTED_MEMTABLE_GARBAGE_BYTES);

  Close();
}

TEST_F(MemtableGarbageTest, InsertAndDeletes) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;
  options.write_buffer_size = 67108864;

  ASSERT_OK(TryReopen(options));

  const size_t NUM_REPEAT = 2000;
  const size_t RAND_VALUES_LENGTH = 37;
  const std::string KEY1 = "key1";
  const std::string KEY2 = "key2";
  const std::string KEY3 = "key3";
  const std::string KEY4 = "key4";
  const std::string KEY5 = "key5";
  const std::string KEY6 = "key6";

  uint64_t EXPECTED_MEMTABLE_DATA_BYTES = 0;
  uint64_t EXPECTED_MEMTABLE_GARBAGE_BYTES = 0;

  WriteBatch batch;

  Random rnd(301);
  // Insertion of of K-V pairs, multiple times.
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    std::string p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEY1, p_v1));
    ASSERT_OK(Put(KEY2, p_v2));
    ASSERT_OK(Put(KEY3, p_v3));
    EXPECTED_MEMTABLE_GARBAGE_BYTES +=
        KEY1.size() + p_v1.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES +=
        KEY2.size() + p_v2.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES +=
        KEY3.size() + p_v3.size() + sizeof(uint64_t);
    ASSERT_OK(Delete(KEY1));
    ASSERT_OK(Delete(KEY2));
    ASSERT_OK(Delete(KEY3));
    EXPECTED_MEMTABLE_GARBAGE_BYTES +=
        KEY1.size() + KEY2.size() + KEY3.size() + 3 * sizeof(uint64_t);
  }

  // The memtable data bytes includes the "garbage"
  // bytes along with the useful payload.
  EXPECTED_MEMTABLE_DATA_BYTES = EXPECTED_MEMTABLE_GARBAGE_BYTES;

  // Note : one set of delete for KEY1, KEY2, KEY3 is written to
  // SSTable to propagate the delete operations to K-V pairs
  // that could have been inserted into the database during past Flush
  // opeartions.
  EXPECTED_MEMTABLE_GARBAGE_BYTES -=
      KEY1.size() + KEY2.size() + KEY3.size() + 3 * sizeof(uint64_t);

  // Additional useful paylaod.
  ASSERT_OK(Delete(KEY4));
  ASSERT_OK(Delete(KEY5));
  ASSERT_OK(Delete(KEY6));

  // // Add useful payload to the memtable data bytes:
  EXPECTED_MEMTABLE_DATA_BYTES +=
      KEY4.size() + KEY5.size() + KEY6.size() + 3 * sizeof(uint64_t);

  // We assert that the K-V pairs have been successfully deleted.
  PinnableSlice value;
  ASSERT_NOK(Get(KEY1, &value));
  ASSERT_NOK(Get(KEY2, &value));
  ASSERT_NOK(Get(KEY3, &value));

  // Force flush to SST. Increments the statistics counter.
  ASSERT_OK(Flush());

  // Collect statistics.
  uint64_t mem_data_bytes = TestGetTickerCount(options, MEMTABLE_DATA_BYTES);
  uint64_t mem_garbage_bytes =
      TestGetTickerCount(options, MEMTABLE_GARBAGE_BYTES);

  EXPECT_EQ(mem_data_bytes, EXPECTED_MEMTABLE_DATA_BYTES);
  EXPECT_EQ(mem_garbage_bytes, EXPECTED_MEMTABLE_GARBAGE_BYTES);

  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
