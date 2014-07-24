// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <vector>
#include <string>
#include <map>
#include <utility>

#include "table/meta_blocks.h"
#include "table/cuckoo_table_builder.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

extern const uint64_t kCuckooTableMagicNumber;

namespace {
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
}  // namespace

class CuckooBuilderTest {
 public:
  CuckooBuilderTest() {
    env_ = Env::Default();
  }

  void CheckFileContents(const std::string& expected_data,
      std::string expected_unused_bucket, uint64_t expected_max_buckets,
      uint32_t expected_num_hash_fun) {
    // Read file
    unique_ptr<RandomAccessFile> read_file;
    ASSERT_OK(env_->NewRandomAccessFile(fname, &read_file, env_options_));
    uint64_t read_file_size;
    ASSERT_OK(env_->GetFileSize(fname, &read_file_size));

    // Assert Table Properties.
    TableProperties* props = nullptr;
    ASSERT_OK(ReadTableProperties(read_file.get(), read_file_size,
          kCuckooTableMagicNumber, env_, nullptr, &props));
    ASSERT_EQ(props->num_entries, num_items);
    ASSERT_EQ(props->fixed_key_len, key_length);

    // Check unused bucket.
    std::string unused_key = props->user_collected_properties[
      CuckooTablePropertyNames::kEmptyKey];
    ASSERT_EQ(expected_unused_bucket.substr(0, key_length), unused_key);

    uint32_t value_len_found =
      *reinterpret_cast<const uint32_t*>(props->user_collected_properties[
                CuckooTablePropertyNames::kValueLength].data());
    ASSERT_EQ(value_length, value_len_found);
    const uint64_t max_buckets =
      *reinterpret_cast<const uint64_t*>(props->user_collected_properties[
                CuckooTablePropertyNames::kMaxNumBuckets].data());
    ASSERT_EQ(expected_max_buckets, max_buckets);
    const uint32_t num_hash_fun_found =
      *reinterpret_cast<const uint32_t*>(props->user_collected_properties[
                CuckooTablePropertyNames::kNumHashTable].data());
    ASSERT_EQ(expected_num_hash_fun, num_hash_fun_found);
    delete props;
    // Check contents of the bucket.
    std::string read_data;
    read_data.resize(expected_data.size());
    Slice read_slice;
    ASSERT_OK(read_file->Read(0, expected_data.size(),
          &read_slice, &read_data[0]));
    ASSERT_EQ(expected_data, read_data);
  }

  Env* env_;
  const EnvOptions env_options_;
  std::string fname;
  uint64_t file_size = 100000;
  uint32_t num_items = 20;
  uint32_t num_hash_fun = 64;
  double hash_table_ratio = 0.9;
  uint32_t ikey_length;
  uint32_t user_key_length;
  uint32_t key_length;
  uint32_t value_length;
  uint32_t bucket_length;
};

TEST(CuckooBuilderTest, NoCollision) {
  hash_map.clear();
  uint32_t expected_num_hash_fun = 2;
  std::vector<std::string> user_keys(num_items);
  std::vector<std::string> keys(num_items);
  std::vector<std::string> values(num_items);
  uint64_t bucket_ids = 0;
  for (uint32_t i = 0; i < num_items; i++) {
    user_keys[i] = "keys" + std::to_string(i+100);
    ParsedInternalKey ikey(user_keys[i], i + 1000, kTypeValue);
    AppendInternalKey(&keys[i], ikey);
    values[i] = "value" + std::to_string(i+100);
    AddHashLookups(user_keys[i], bucket_ids, num_hash_fun);
    bucket_ids += num_hash_fun;
  }

  ikey_length = keys[0].size();
  key_length = ikey_length;
  value_length = values[0].size();
  bucket_length = ikey_length + value_length;
  uint64_t expected_max_buckets = file_size / bucket_length;
  std::string expected_unused_user_key = "keys10:";
  ParsedInternalKey ikey(expected_unused_user_key, 0, kTypeValue);
  std::string expected_unused_bucket;
  AppendInternalKey(&expected_unused_bucket, ikey);
  expected_unused_bucket.resize(bucket_length, 'a');
  unique_ptr<WritableFile> writable_file;
  fname = test::TmpDir() + "/BasicTest_writable_file";
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  CuckooTableBuilder cuckoo_builder(
      writable_file.get(), ikey_length,
      value_length, hash_table_ratio,
      file_size, num_hash_fun, 100, false, GetSliceHash);
  ASSERT_OK(cuckoo_builder.status());
  uint32_t key_idx = 0;
  std::string expected_file_data = "";
  for (uint32_t i = 0; i < expected_max_buckets; i++) {
    if (key_idx * num_hash_fun == i && key_idx < num_items) {
      cuckoo_builder.Add(Slice(keys[key_idx]), Slice(values[key_idx]));
      ASSERT_EQ(cuckoo_builder.NumEntries(), key_idx + 1);
      ASSERT_OK(cuckoo_builder.status());
      expected_file_data.append(keys[key_idx] + values[key_idx]);
      ++key_idx;
    } else {
      expected_file_data.append(expected_unused_bucket);
    }
  }
  ASSERT_OK(cuckoo_builder.Finish());
  writable_file->Close();
  CheckFileContents(expected_file_data, expected_unused_bucket,
      expected_max_buckets, expected_num_hash_fun);
}

TEST(CuckooBuilderTest, NoCollisionLastLevel) {
  hash_map.clear();
  uint32_t expected_num_hash_fun = 2;
  std::vector<std::string> user_keys(num_items);
  std::vector<std::string> keys(num_items);
  std::vector<std::string> values(num_items);
  uint64_t bucket_ids = 0;
  for (uint32_t i = 0; i < num_items; i++) {
    user_keys[i] = "keys" + std::to_string(i+100);
    // Set zero sequence number in all keys.
    ParsedInternalKey ikey(user_keys[i], 0, kTypeValue);
    AppendInternalKey(&keys[i], ikey);
    values[i] = "value" + std::to_string(i+100);
    AddHashLookups(user_keys[i], bucket_ids, num_hash_fun);
    bucket_ids += num_hash_fun;
  }
  ikey_length = keys[0].size();
  user_key_length = user_keys[0].size();
  key_length = user_key_length;
  value_length = values[0].size();
  bucket_length = key_length + value_length;
  uint64_t expected_max_buckets = file_size / bucket_length;
  std::string expected_unused_bucket = "keys10:";
  expected_unused_bucket.resize(bucket_length, 'a');
  unique_ptr<WritableFile> writable_file;
  fname = test::TmpDir() + "/NoCollisionLastLevel_writable_file";
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  CuckooTableBuilder cuckoo_builder(
      writable_file.get(), ikey_length,
      value_length, hash_table_ratio,
      file_size, num_hash_fun, 100, true, GetSliceHash);
  ASSERT_OK(cuckoo_builder.status());
  uint32_t key_idx = 0;
  std::string expected_file_data = "";
  for (uint32_t i = 0; i < expected_max_buckets; i++) {
    if (key_idx * num_hash_fun == i && key_idx < num_items) {
      cuckoo_builder.Add(Slice(keys[key_idx]), Slice(values[key_idx]));
      ASSERT_EQ(cuckoo_builder.NumEntries(), key_idx + 1);
      ASSERT_OK(cuckoo_builder.status());
      expected_file_data.append(user_keys[key_idx] + values[key_idx]);
      ++key_idx;
    } else {
      expected_file_data.append(expected_unused_bucket);
    }
  }
  ASSERT_OK(cuckoo_builder.Finish());
  writable_file->Close();
  CheckFileContents(expected_file_data, expected_unused_bucket,
      expected_max_buckets, expected_num_hash_fun);
}

TEST(CuckooBuilderTest, WithCollision) {
  // Take keys with colliding hash function values.
  hash_map.clear();
  num_hash_fun = 20;
  num_items = num_hash_fun;
  uint32_t expected_num_hash_fun = num_hash_fun;
  std::vector<std::string> user_keys(num_items);
  std::vector<std::string> keys(num_items);
  std::vector<std::string> values(num_items);
  for (uint32_t i = 0; i < num_items; i++) {
    user_keys[i] = "keys" + std::to_string(i+100);
    ParsedInternalKey ikey(user_keys[i], i + 1000, kTypeValue);
    AppendInternalKey(&keys[i], ikey);
    values[i] = "value" + std::to_string(i+100);
    // Make all hash values collide.
    AddHashLookups(user_keys[i], 0, num_hash_fun);
  }
  ikey_length = keys[0].size();
  value_length = values[0].size();
  key_length = ikey_length;
  bucket_length = key_length + value_length;
  uint64_t expected_max_buckets = file_size / bucket_length;
  std::string expected_unused_user_key = "keys10:";
  ParsedInternalKey ikey(expected_unused_user_key, 0, kTypeValue);
  std::string expected_unused_bucket;
  AppendInternalKey(&expected_unused_bucket, ikey);
  expected_unused_bucket.resize(bucket_length, 'a');
  unique_ptr<WritableFile> writable_file;
  fname = test::TmpDir() + "/WithCollision_writable_file";
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  CuckooTableBuilder cuckoo_builder(
      writable_file.get(), key_length, value_length, hash_table_ratio,
      file_size, num_hash_fun, 100, false, GetSliceHash);
  ASSERT_OK(cuckoo_builder.status());
  uint32_t key_idx = 0;
  std::string expected_file_data = "";
  for (uint32_t i = 0; i < expected_max_buckets; i++) {
    if (key_idx  == i && key_idx < num_items) {
      cuckoo_builder.Add(Slice(keys[key_idx]), Slice(values[key_idx]));
      ASSERT_EQ(cuckoo_builder.NumEntries(), key_idx + 1);
      ASSERT_OK(cuckoo_builder.status());
      expected_file_data.append(keys[key_idx] + values[key_idx]);
      ++key_idx;
    } else {
      expected_file_data.append(expected_unused_bucket);
    }
  }
  ASSERT_OK(cuckoo_builder.Finish());
  writable_file->Close();
  CheckFileContents(expected_file_data, expected_unused_bucket,
      expected_max_buckets, expected_num_hash_fun);
}

TEST(CuckooBuilderTest, FailWithTooManyCollisions) {
  // Take keys with colliding hash function values.
  // Take more keys than the number of hash functions.
  hash_map.clear();
  num_hash_fun = 20;
  num_items = num_hash_fun + 1;
  std::vector<std::string> user_keys(num_items);
  std::vector<std::string> keys(num_items);
  std::vector<std::string> values(num_items);
  for (uint32_t i = 0; i < num_items; i++) {
    user_keys[i] = "keys" + std::to_string(i+100);
    ParsedInternalKey ikey(user_keys[i], i + 1000, kTypeValue);
    AppendInternalKey(&keys[i], ikey);
    values[i] = "value" + std::to_string(i+100);
    // Make all hash values collide.
    AddHashLookups(user_keys[i], 0, num_hash_fun);
  }
  ikey_length = keys[0].size();
  value_length = values[0].size();
  unique_ptr<WritableFile> writable_file;
  fname = test::TmpDir() + "/FailWithTooManyCollisions_writable";
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  CuckooTableBuilder cuckoo_builder(
      writable_file.get(), ikey_length,
      value_length, hash_table_ratio, file_size, num_hash_fun,
      100, false, GetSliceHash);
  ASSERT_OK(cuckoo_builder.status());
  for (uint32_t key_idx = 0; key_idx < num_items-1; key_idx++) {
    cuckoo_builder.Add(Slice(keys[key_idx]), Slice(values[key_idx]));
    ASSERT_OK(cuckoo_builder.status());
    ASSERT_EQ(cuckoo_builder.NumEntries(), key_idx + 1);
  }
  cuckoo_builder.Add(Slice(keys.back()), Slice(values.back()));
  ASSERT_TRUE(cuckoo_builder.status().IsCorruption());
  cuckoo_builder.Abandon();
  writable_file->Close();
}

TEST(CuckooBuilderTest, FailWhenSameKeyInserted) {
  hash_map.clear();
  std::string user_key = "repeatedkey";
  AddHashLookups(user_key, 0, num_hash_fun);
  std::string key_to_reuse1, key_to_reuse2;
  ParsedInternalKey ikey1(user_key, 1000, kTypeValue);
  ParsedInternalKey ikey2(user_key, 1001, kTypeValue);
  AppendInternalKey(&key_to_reuse1, ikey1);
  AppendInternalKey(&key_to_reuse2, ikey2);
  std::string value = "value";
  ikey_length = key_to_reuse1.size();
  value_length = value.size();
  unique_ptr<WritableFile> writable_file;
  fname = test::TmpDir() + "/FailWhenSameKeyInserted_writable_file";
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  CuckooTableBuilder cuckoo_builder(
      writable_file.get(), ikey_length,
      value_length, hash_table_ratio, file_size, num_hash_fun,
      100, false, GetSliceHash);
  ASSERT_OK(cuckoo_builder.status());
  cuckoo_builder.Add(Slice(key_to_reuse1), Slice(value));
  ASSERT_OK(cuckoo_builder.status());
  ASSERT_EQ(cuckoo_builder.NumEntries(), 1U);
  cuckoo_builder.Add(Slice(key_to_reuse2), Slice(value));
  ASSERT_TRUE(cuckoo_builder.status().IsCorruption());
  cuckoo_builder.Abandon();
  writable_file->Close();
}

TEST(CuckooBuilderTest, WithACollisionPath) {
  hash_map.clear();
  // Have two hash functions. Insert elements with overlapping hashes.
  // Finally insert an element which will displace all the current elements.
  num_hash_fun = 2;
  uint32_t expected_num_hash_fun = num_hash_fun;
  uint32_t max_search_depth = 100;
  num_items = max_search_depth + 2;
  std::vector<std::string> user_keys(num_items);
  std::vector<std::string> keys(num_items);
  std::vector<std::string> values(num_items);
  std::vector<uint64_t> expected_bucket_id(num_items);
  for (uint32_t i = 0; i < num_items - 1; i++) {
    user_keys[i] = "keys" + std::to_string(i+100);
    ParsedInternalKey ikey(user_keys[i], i + 1000, kTypeValue);
    AppendInternalKey(&keys[i], ikey);
    values[i] = "value" + std::to_string(i+100);
    // Make all hash values collide with the next element.
    AddHashLookups(user_keys[i], i, num_hash_fun);
    expected_bucket_id[i] = i+1;
  }
  expected_bucket_id[0] = 0;
  user_keys.back() = "keys" + std::to_string(num_items + 99);
  ParsedInternalKey ikey(user_keys.back(), num_items + 1000, kTypeValue);
  AppendInternalKey(&keys.back(), ikey);
  values.back() = "value" + std::to_string(num_items+100);
  // Make both hash values collide with first element.
  AddHashLookups(user_keys.back(), 0, num_hash_fun);
  expected_bucket_id.back() = 1;

  ikey_length = keys[0].size();
  value_length = values[0].size();
  key_length = ikey_length;
  bucket_length = key_length + value_length;

  uint64_t expected_max_buckets = file_size / bucket_length;
  std::string expected_unused_user_key = "keys10:";
  ikey = ParsedInternalKey(expected_unused_user_key, 0, kTypeValue);
  std::string expected_unused_bucket;
  AppendInternalKey(&expected_unused_bucket, ikey);
  expected_unused_bucket.resize(bucket_length, 'a');
  std::string expected_file_data = "";
  for (uint32_t i = 0; i < expected_max_buckets; i++) {
    expected_file_data += expected_unused_bucket;
  }

  unique_ptr<WritableFile> writable_file;
  fname = test::TmpDir() + "/WithCollisionPath_writable_file";
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  CuckooTableBuilder cuckoo_builder(
      writable_file.get(), key_length,
      value_length, hash_table_ratio, file_size,
      num_hash_fun, max_search_depth, false, GetSliceHash);
  ASSERT_OK(cuckoo_builder.status());
  for (uint32_t key_idx = 0; key_idx < num_items; key_idx++) {
    cuckoo_builder.Add(Slice(keys[key_idx]), Slice(values[key_idx]));
    ASSERT_OK(cuckoo_builder.status());
    ASSERT_EQ(cuckoo_builder.NumEntries(), key_idx + 1);
    expected_file_data.replace(expected_bucket_id[key_idx]*bucket_length,
        bucket_length, keys[key_idx] + values[key_idx]);
  }
  ASSERT_OK(cuckoo_builder.Finish());
  writable_file->Close();
  CheckFileContents(expected_file_data, expected_unused_bucket,
      expected_max_buckets, expected_num_hash_fun);
}

TEST(CuckooBuilderTest, FailWhenCollisionPathTooLong) {
  hash_map.clear();
  // Have two hash functions. Insert elements with overlapping hashes.
  // Finally insert an element which will displace all the current elements.
  num_hash_fun = 2;

  uint32_t max_search_depth = 100;
  num_items = max_search_depth + 3;
  std::vector<std::string> user_keys(num_items);
  std::vector<std::string> keys(num_items);
  std::vector<std::string> values(num_items);
  for (uint32_t i = 0; i < num_items - 1; i++) {
    user_keys[i] = "keys" + std::to_string(i+100);
    ParsedInternalKey ikey(user_keys[i], i + 1000, kTypeValue);
    AppendInternalKey(&keys[i], ikey);
    values[i] = "value" + std::to_string(i+100);
    // Make all hash values collide with the next element.
    AddHashLookups(user_keys[i], i, num_hash_fun);
  }
  user_keys.back() = "keys" + std::to_string(num_items + 99);
  ParsedInternalKey ikey(user_keys.back(), num_items + 1000, kTypeValue);
  AppendInternalKey(&keys.back(), ikey);
  Slice(values.back()) = "value" + std::to_string(num_items+100);
  // Make both hash values collide with first element.
  AddHashLookups(user_keys.back(), 0, num_hash_fun);

  ikey_length = keys[0].size();
  value_length = values[0].size();
  unique_ptr<WritableFile> writable_file;
  fname = test::TmpDir() + "/FailWhenCollisionPathTooLong_writable";
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  CuckooTableBuilder cuckoo_builder(
      writable_file.get(), ikey_length,
      value_length, hash_table_ratio, file_size, num_hash_fun,
      max_search_depth, false, GetSliceHash);
  ASSERT_OK(cuckoo_builder.status());
  for (uint32_t key_idx = 0; key_idx < num_items-1; key_idx++) {
    cuckoo_builder.Add(Slice(keys[key_idx]), Slice(values[key_idx]));
    ASSERT_OK(cuckoo_builder.status());
    ASSERT_EQ(cuckoo_builder.NumEntries(), key_idx + 1);
  }
  cuckoo_builder.Add(Slice(keys.back()), Slice(values.back()));
  ASSERT_TRUE(cuckoo_builder.status().IsCorruption());
  cuckoo_builder.Abandon();
  writable_file->Close();
}

TEST(CuckooBuilderTest, FailWhenTableIsFull) {
  hash_map.clear();
  file_size = 160;

  num_items = 7;
  std::vector<std::string> user_keys(num_items);
  std::vector<std::string> keys(num_items);
  std::vector<std::string> values(num_items);
  for (uint32_t i = 0; i < num_items; i++) {
    user_keys[i] = "keys" + std::to_string(i+1000);
    ParsedInternalKey ikey(user_keys[i], i + 1000, kTypeValue);
    AppendInternalKey(&keys[i], ikey);
    values[i] = "value" + std::to_string(i+100);
    AddHashLookups(user_keys[i], i, num_hash_fun);
  }
  ikey_length = keys[0].size();
  value_length = values[0].size();
  bucket_length = ikey_length + value_length;
  // Check that number of items is tight.
  ASSERT_GT(bucket_length * num_items, file_size);
  ASSERT_LE(bucket_length * (num_items-1), file_size);

  unique_ptr<WritableFile> writable_file;
  fname = test::TmpDir() + "/FailWhenTabelIsFull_writable";
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  CuckooTableBuilder cuckoo_builder(
      writable_file.get(), ikey_length,
      value_length, hash_table_ratio, file_size, num_hash_fun,
      100, false, GetSliceHash);
  ASSERT_OK(cuckoo_builder.status());
  for (uint32_t key_idx = 0; key_idx < num_items-1; key_idx++) {
    cuckoo_builder.Add(Slice(keys[key_idx]), Slice(values[key_idx]));
    ASSERT_OK(cuckoo_builder.status());
    ASSERT_EQ(cuckoo_builder.NumEntries(), key_idx + 1);
  }
  cuckoo_builder.Add(Slice(keys.back()), Slice(values.back()));
  ASSERT_TRUE(cuckoo_builder.status().IsCorruption());
  cuckoo_builder.Abandon();
  writable_file->Close();
}
}  // namespace rocksdb

int main(int argc, char** argv) { return rocksdb::test::RunAllTests(); }
