// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run this test\n");
  return 1;
}
#else

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <gflags/gflags.h>
#include <vector>
#include <string>
#include <map>

#include "table/meta_blocks.h"
#include "table/cuckoo_table_builder.h"
#include "table/cuckoo_table_reader.h"
#include "table/cuckoo_table_factory.h"
#include "util/arena.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

using GFLAGS::ParseCommandLineFlags;
using GFLAGS::SetUsageMessage;

DEFINE_string(file_dir, "", "Directory where the files will be created"
    " for benchmark. Added for using tmpfs.");
DEFINE_bool(enable_perf, false, "Run Benchmark Tests too.");
DEFINE_bool(write, false,
    "Should write new values to file in performance tests?");

namespace rocksdb {

extern const uint64_t kCuckooTableMagicNumber;
extern const uint64_t kMaxNumHashTable;

namespace {
const uint32_t kNumHashFunc = 10;
// Methods, variables related to Hash functions.
std::unordered_map<std::string, std::vector<uint64_t>> hash_map;

void AddHashLookups(const std::string& s, uint64_t bucket_id,
        uint32_t num_hash_fun) {
  std::vector<uint64_t> v;
  for (uint32_t i = 0; i < num_hash_fun; i++) {
    v.push_back(bucket_id + i);
  }
  hash_map[s] = v;
}

uint64_t GetSliceHash(const Slice& s, uint32_t index,
    uint64_t max_num_buckets) {
  return hash_map[s.ToString()][index];
}

// Methods, variables for checking key and values read.
struct ValuesToAssert {
  ValuesToAssert(const std::string& key, const Slice& value)
    : expected_user_key(key),
      expected_value(value),
      call_count(0) {}
  std::string expected_user_key;
  Slice expected_value;
  int call_count;
};

bool AssertValues(void* assert_obj,
    const ParsedInternalKey& k, const Slice& v) {
  ValuesToAssert *ptr = reinterpret_cast<ValuesToAssert*>(assert_obj);
  ASSERT_EQ(ptr->expected_value.ToString(), v.ToString());
  ASSERT_EQ(ptr->expected_user_key, k.user_key.ToString());
  ++ptr->call_count;
  return false;
}
}  // namespace

class CuckooReaderTest {
 public:
  CuckooReaderTest() {
    options.allow_mmap_reads = true;
    env = options.env;
    env_options = EnvOptions(options);
  }

  void SetUp(int num_items) {
    this->num_items = num_items;
    hash_map.clear();
    keys.clear();
    keys.resize(num_items);
    user_keys.clear();
    user_keys.resize(num_items);
    values.clear();
    values.resize(num_items);
  }

  std::string NumToStr(int64_t i) {
    return std::string(reinterpret_cast<char*>(&i), sizeof(i));
  }

  void CreateCuckooFileAndCheckReader() {
    std::unique_ptr<WritableFile> writable_file;
    ASSERT_OK(env->NewWritableFile(fname, &writable_file, env_options));
    CuckooTableBuilder builder(
        writable_file.get(), 0.9, kNumHashFunc, 100, GetSliceHash);
    ASSERT_OK(builder.status());
    for (uint32_t key_idx = 0; key_idx < num_items; ++key_idx) {
      builder.Add(Slice(keys[key_idx]), Slice(values[key_idx]));
      ASSERT_OK(builder.status());
      ASSERT_EQ(builder.NumEntries(), key_idx + 1);
    }
    ASSERT_OK(builder.Finish());
    ASSERT_EQ(num_items, builder.NumEntries());
    file_size = builder.FileSize();
    ASSERT_OK(writable_file->Close());

    // Check reader now.
    std::unique_ptr<RandomAccessFile> read_file;
    ASSERT_OK(env->NewRandomAccessFile(fname, &read_file, env_options));
    CuckooTableReader reader(
        options,
        std::move(read_file),
        file_size,
        GetSliceHash);
    ASSERT_OK(reader.status());
    for (uint32_t i = 0; i < num_items; ++i) {
      ValuesToAssert v(user_keys[i], values[i]);
      ASSERT_OK(reader.Get(
            ReadOptions(), Slice(keys[i]), &v, AssertValues, nullptr));
      ASSERT_EQ(1, v.call_count);
    }
  }
  void UpdateKeys(bool with_zero_seqno) {
    for (uint32_t i = 0; i < num_items; i++) {
      ParsedInternalKey ikey(user_keys[i],
          with_zero_seqno ? 0 : i + 1000, kTypeValue);
      keys[i].clear();
      AppendInternalKey(&keys[i], ikey);
    }
  }

  void CheckIterator() {
    std::unique_ptr<RandomAccessFile> read_file;
    ASSERT_OK(env->NewRandomAccessFile(fname, &read_file, env_options));
    CuckooTableReader reader(
        options,
        std::move(read_file),
        file_size,
        GetSliceHash);
    ASSERT_OK(reader.status());
    Iterator* it = reader.NewIterator(ReadOptions(), nullptr);
    ASSERT_OK(it->status());
    ASSERT_TRUE(!it->Valid());
    it->SeekToFirst();
    int cnt = 0;
    while (it->Valid()) {
      ASSERT_OK(it->status());
      ASSERT_TRUE(Slice(keys[cnt]) == it->key());
      ASSERT_TRUE(Slice(values[cnt]) == it->value());
      ++cnt;
      it->Next();
    }
    ASSERT_EQ(static_cast<uint32_t>(cnt), num_items);

    it->SeekToLast();
    cnt = num_items - 1;
    ASSERT_TRUE(it->Valid());
    while (it->Valid()) {
      ASSERT_OK(it->status());
      ASSERT_TRUE(Slice(keys[cnt]) == it->key());
      ASSERT_TRUE(Slice(values[cnt]) == it->value());
      --cnt;
      it->Prev();
    }
    ASSERT_EQ(cnt, -1);

    cnt = num_items / 2;
    it->Seek(keys[cnt]);
    while (it->Valid()) {
      ASSERT_OK(it->status());
      ASSERT_TRUE(Slice(keys[cnt]) == it->key());
      ASSERT_TRUE(Slice(values[cnt]) == it->value());
      ++cnt;
      it->Next();
    }
    ASSERT_EQ(static_cast<uint32_t>(cnt), num_items);
    delete it;

    Arena arena;
    it = reader.NewIterator(ReadOptions(), &arena);
    ASSERT_OK(it->status());
    ASSERT_TRUE(!it->Valid());
    it->Seek(keys[num_items/2]);
    ASSERT_TRUE(it->Valid());
    ASSERT_OK(it->status());
    ASSERT_TRUE(keys[num_items/2] == it->key());
    ASSERT_TRUE(values[num_items/2] == it->value());
    ASSERT_OK(it->status());
    it->~Iterator();
  }

  std::vector<std::string> keys;
  std::vector<std::string> user_keys;
  std::vector<std::string> values;
  uint64_t num_items;
  std::string fname;
  uint64_t file_size;
  Options options;
  Env* env;
  EnvOptions env_options;
};

TEST(CuckooReaderTest, WhenKeyExists) {
  SetUp(kNumHashFunc);
  fname = test::TmpDir() + "/CuckooReader_WhenKeyExists";
  for (uint64_t i = 0; i < num_items; i++) {
    user_keys[i] = "key" + NumToStr(i);
    ParsedInternalKey ikey(user_keys[i], i + 1000, kTypeValue);
    AppendInternalKey(&keys[i], ikey);
    values[i] = "value" + NumToStr(i);
    // Give disjoint hash values.
    AddHashLookups(user_keys[i], i, kNumHashFunc);
  }
  CreateCuckooFileAndCheckReader();
  // Last level file.
  UpdateKeys(true);
  CreateCuckooFileAndCheckReader();
  // Test with collision. Make all hash values collide.
  hash_map.clear();
  for (uint32_t i = 0; i < num_items; i++) {
    AddHashLookups(user_keys[i], 0, kNumHashFunc);
  }
  UpdateKeys(false);
  CreateCuckooFileAndCheckReader();
  // Last level file.
  UpdateKeys(true);
  CreateCuckooFileAndCheckReader();
}

TEST(CuckooReaderTest, CheckIterator) {
  SetUp(2*kNumHashFunc);
  fname = test::TmpDir() + "/CuckooReader_CheckIterator";
  for (uint64_t i = 0; i < num_items; i++) {
    user_keys[i] = "key" + NumToStr(i);
    ParsedInternalKey ikey(user_keys[i], 0, kTypeValue);
    AppendInternalKey(&keys[i], ikey);
    values[i] = "value" + NumToStr(i);
    // Give disjoint hash values, in reverse order.
    AddHashLookups(user_keys[i], num_items-i-1, kNumHashFunc);
  }
  CreateCuckooFileAndCheckReader();
  CheckIterator();
  // Last level file.
  UpdateKeys(true);
  CreateCuckooFileAndCheckReader();
  CheckIterator();
}

TEST(CuckooReaderTest, WhenKeyNotFound) {
  // Add keys with colliding hash values.
  SetUp(kNumHashFunc);
  fname = test::TmpDir() + "/CuckooReader_WhenKeyNotFound";
  for (uint64_t i = 0; i < num_items; i++) {
    user_keys[i] = "key" + NumToStr(i);
    ParsedInternalKey ikey(user_keys[i], i + 1000, kTypeValue);
    AppendInternalKey(&keys[i], ikey);
    values[i] = "value" + NumToStr(i);
    // Make all hash values collide.
    AddHashLookups(user_keys[i], 0, kNumHashFunc);
  }
  CreateCuckooFileAndCheckReader();
  std::unique_ptr<RandomAccessFile> read_file;
  ASSERT_OK(env->NewRandomAccessFile(fname, &read_file, env_options));
  CuckooTableReader reader(
      options,
      std::move(read_file),
      file_size,
      GetSliceHash);
  ASSERT_OK(reader.status());
  // Search for a key with colliding hash values.
  std::string not_found_user_key = "key" + NumToStr(num_items);
  std::string not_found_key;
  AddHashLookups(not_found_user_key, 0, kNumHashFunc);
  ParsedInternalKey ikey(not_found_user_key, 1000, kTypeValue);
  AppendInternalKey(&not_found_key, ikey);
  ValuesToAssert v("", "");
  ASSERT_OK(reader.Get(
        ReadOptions(), Slice(not_found_key), &v, AssertValues, nullptr));
  ASSERT_EQ(0, v.call_count);
  ASSERT_OK(reader.status());
  // Search for a key with an independent hash value.
  std::string not_found_user_key2 = "key" + NumToStr(num_items + 1);
  AddHashLookups(not_found_user_key2, kNumHashFunc, kNumHashFunc);
  ParsedInternalKey ikey2(not_found_user_key2, 1000, kTypeValue);
  std::string not_found_key2;
  AppendInternalKey(&not_found_key2, ikey2);
  ASSERT_OK(reader.Get(
        ReadOptions(), Slice(not_found_key2), &v, AssertValues, nullptr));
  ASSERT_EQ(0, v.call_count);
  ASSERT_OK(reader.status());

  // Test read with corrupted key.
  Slice corrupt_key("corrupt_ikey");
  ASSERT_TRUE(!ParseInternalKey(corrupt_key, &ikey));
  ASSERT_TRUE(reader.Get(
        ReadOptions(), corrupt_key, &v,
        AssertValues, nullptr).IsCorruption());
  ASSERT_EQ(0, v.call_count);
  ASSERT_OK(reader.status());

  // Test read with key of invalid length.
  IterKey k;
  k.SetInternalKey("very_long_key", 0, kTypeValue);
  ASSERT_TRUE(reader.Get(
        ReadOptions(), k.GetKey(), &v,
        AssertValues, nullptr).IsInvalidArgument());
  ASSERT_EQ(0, v.call_count);
  ASSERT_OK(reader.status());
  k.Clear();
  k.SetInternalKey("s", 0, kTypeValue);
  ASSERT_TRUE(reader.Get(
        ReadOptions(), k.GetKey(), &v,
        AssertValues, nullptr).IsInvalidArgument());
  ASSERT_EQ(0, v.call_count);
  ASSERT_OK(reader.status());

  // Test read when key is unused key.
  std::string unused_key =
    reader.GetTableProperties()->user_collected_properties.at(
    CuckooTablePropertyNames::kEmptyKey);
  // Add hash values that map to empty buckets.
  AddHashLookups(ExtractUserKey(unused_key).ToString(),
      kNumHashFunc, kNumHashFunc);
  ASSERT_OK(reader.Get(
        ReadOptions(), Slice(unused_key), &v, AssertValues, nullptr));
  ASSERT_EQ(0, v.call_count);
  ASSERT_OK(reader.status());
}

// Performance tests
namespace {
bool DoNothing(void* arg, const ParsedInternalKey& k, const Slice& v) {
  // Deliberately empty.
  return false;
}

bool CheckValue(void* cnt_ptr, const ParsedInternalKey& k, const Slice& v) {
  ++*reinterpret_cast<int*>(cnt_ptr);
  std::string expected_value;
  AppendInternalKey(&expected_value, k);
  ASSERT_EQ(0, v.compare(Slice(&expected_value[0], v.size())));
  return false;
}

void GetKeys(uint64_t num, std::vector<std::string>* keys) {
  IterKey k;
  k.SetInternalKey("", 0, kTypeValue);
  std::string internal_key_suffix = k.GetKey().ToString();
  ASSERT_EQ(static_cast<size_t>(8), internal_key_suffix.size());
  for (uint64_t key_idx = 0; key_idx < num; ++key_idx) {
    std::string new_key(reinterpret_cast<char*>(&key_idx), sizeof(key_idx));
    new_key += internal_key_suffix;
    keys->push_back(new_key);
  }
}

std::string GetFileName(uint64_t num, double hash_ratio) {
  if (FLAGS_file_dir.empty()) {
    FLAGS_file_dir = test::TmpDir();
  }
  return FLAGS_file_dir + "/cuckoo_read_benchmark" +
    std::to_string(num/1000000) + "Mratio" +
    std::to_string(static_cast<int>(100*hash_ratio));
}

// Create last level file as we are interested in measuring performance of
// last level file only.
void WriteFile(const std::vector<std::string>& keys,
    const uint64_t num, double hash_ratio) {
  Options options;
  options.allow_mmap_reads = true;
  Env* env = options.env;
  EnvOptions env_options = EnvOptions(options);
  std::string fname = GetFileName(num, hash_ratio);

  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env->NewWritableFile(fname, &writable_file, env_options));
  CuckooTableBuilder builder(
      writable_file.get(), hash_ratio,
      kMaxNumHashTable, 1000, GetSliceMurmurHash);
  ASSERT_OK(builder.status());
  for (uint64_t key_idx = 0; key_idx < num; ++key_idx) {
    // Value is just a part of key.
    builder.Add(Slice(keys[key_idx]), Slice(&keys[key_idx][0], 4));
    ASSERT_EQ(builder.NumEntries(), key_idx + 1);
    ASSERT_OK(builder.status());
  }
  ASSERT_OK(builder.Finish());
  ASSERT_EQ(num, builder.NumEntries());
  ASSERT_OK(writable_file->Close());

  uint64_t file_size;
  env->GetFileSize(fname, &file_size);
  std::unique_ptr<RandomAccessFile> read_file;
  ASSERT_OK(env->NewRandomAccessFile(fname, &read_file, env_options));

  CuckooTableReader reader(
      options, std::move(read_file), file_size, GetSliceMurmurHash);
  ASSERT_OK(reader.status());
  ReadOptions r_options;
  for (const auto& key : keys) {
    int cnt = 0;
    ASSERT_OK(reader.Get(r_options, Slice(key), &cnt, CheckValue, nullptr));
    if (cnt != 1) {
      fprintf(stderr, "%" PRIx64 " not found.\n",
          *reinterpret_cast<const uint64_t*>(key.data()));
      ASSERT_EQ(1, cnt);
    }
  }
}

void ReadKeys(const std::vector<std::string>& keys, uint64_t num,
    double hash_ratio, uint32_t batch_size) {
  Options options;
  options.allow_mmap_reads = true;
  Env* env = options.env;
  EnvOptions env_options = EnvOptions(options);
  std::string fname = GetFileName(num, hash_ratio);

  uint64_t file_size;
  env->GetFileSize(fname, &file_size);
  std::unique_ptr<RandomAccessFile> read_file;
  ASSERT_OK(env->NewRandomAccessFile(fname, &read_file, env_options));

  CuckooTableReader reader(
      options, std::move(read_file), file_size, GetSliceMurmurHash);
  ASSERT_OK(reader.status());
  const UserCollectedProperties user_props =
    reader.GetTableProperties()->user_collected_properties;
  const uint32_t num_hash_fun = *reinterpret_cast<const uint32_t*>(
      user_props.at(CuckooTablePropertyNames::kNumHashTable).data());
  fprintf(stderr, "With %" PRIu64 " items and hash table ratio %f, number of"
      " hash functions used: %u.\n", num, hash_ratio, num_hash_fun);
  ReadOptions r_options;

  uint64_t start_time = env->NowMicros();
  if (batch_size > 0) {
    for (uint64_t i = 0; i < num; i += batch_size) {
      for (uint64_t j = i; j < i+batch_size && j < num; ++j) {
        reader.Prepare(Slice(keys[j]));
      }
      for (uint64_t j = i; j < i+batch_size && j < num; ++j) {
        reader.Get(r_options, Slice(keys[j]), nullptr, DoNothing, nullptr);
      }
    }
  } else {
    for (uint64_t i = 0; i < num; i++) {
      reader.Get(r_options, Slice(keys[i]), nullptr, DoNothing, nullptr);
    }
  }
  float time_per_op = (env->NowMicros() - start_time) * 1.0 / num;
  fprintf(stderr,
      "Time taken per op is %.3fus (%.1f Mqps) with batch size of %u\n",
      time_per_op, 1.0 / time_per_op, batch_size);
}
}  // namespace.

TEST(CuckooReaderTest, TestReadPerformance) {
  uint64_t num = 1000*1000*100;
  if (!FLAGS_enable_perf) {
    return;
  }
#ifndef NDEBUG
  fprintf(stdout,
      "WARNING: Not compiled with DNDEBUG. Performance tests may be slow.\n");
#endif
  std::vector<std::string> keys;
  GetKeys(num, &keys);
  for (double hash_ratio : std::vector<double>({0.5, 0.6, 0.75, 0.9})) {
    if (FLAGS_write || !Env::Default()->FileExists(
          GetFileName(num, hash_ratio))) {
      WriteFile(keys, num, hash_ratio);
    }
    ReadKeys(keys, num, hash_ratio, 0);
    ReadKeys(keys, num, hash_ratio, 10);
    ReadKeys(keys, num, hash_ratio, 25);
    ReadKeys(keys, num, hash_ratio, 50);
    ReadKeys(keys, num, hash_ratio, 100);
    fprintf(stderr, "\n");
  }
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);
  rocksdb::test::RunAllTests();
  return 0;
}

#endif  // GFLAGS.
