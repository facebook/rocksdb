// Copyright (c) 2013, Facebook, Inc. All rights reserved.
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
std::unordered_map<std::string, std::vector<unsigned int>> hash_map;

void AddHashLookups(const std::string& s, unsigned int bucket_id,
    unsigned int num_hash_fun) {
  std::vector<unsigned int> v;
  for (unsigned int i = 0; i < num_hash_fun; i++) {
    v.push_back(bucket_id + i);
  }
  hash_map[s] = v;
  return;
}

unsigned int GetSliceHash(const Slice& s, unsigned int index,
    unsigned int max_num_buckets) {
  return hash_map[s.ToString()][index];
}
}  // namespace

class CuckooBuilderTest {
 public:
  CuckooBuilderTest() {
    env_ = Env::Default();
  }

  void CheckFileContents(const std::string& expected_data) {
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
    std::string unused_bucket = props->user_collected_properties[
      CuckooTablePropertyNames::kEmptyBucket];
    ASSERT_EQ(expected_unused_bucket, unused_bucket);

    unsigned int max_buckets;
    Slice max_buckets_slice = Slice(props->user_collected_properties[
                CuckooTablePropertyNames::kMaxNumBuckets]);
    GetVarint32(&max_buckets_slice, &max_buckets);
    ASSERT_EQ(expected_max_buckets, max_buckets);
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
  std::string expected_unused_bucket;
  unsigned int file_size = 100000;
  unsigned int num_items = 20;
  unsigned int num_hash_fun = 64;
  double hash_table_ratio = 0.9;
  unsigned int ikey_length;
  unsigned int user_key_length;
  unsigned int key_length;
  unsigned int value_length;
  unsigned int bucket_length;
  unsigned int expected_max_buckets;
};


TEST(CuckooBuilderTest, NoCollision) {
  hash_map.clear();
  num_items = 20;
  num_hash_fun = 64;
  std::vector<std::string> user_keys(num_items);
  std::vector<std::string> keys(num_items);
  std::vector<std::string> values(num_items);
  unsigned int bucket_ids = 0;
  for (unsigned int i = 0; i < num_items; i++) {
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
  expected_max_buckets = file_size / bucket_length;
  std::string expected_unused_user_key = "keys10:";
  ParsedInternalKey ikey(expected_unused_user_key, 0, kTypeValue);
  AppendInternalKey(&expected_unused_bucket, ikey);
  expected_unused_bucket.resize(bucket_length, 'a');
  unique_ptr<WritableFile> writable_file;
  fname = test::TmpDir() + "/BasicTest_writable_file";
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  CuckooTableBuilder* cuckoo_builder = new CuckooTableBuilder(
      writable_file.get(), ikey_length,
      value_length, hash_table_ratio,
      file_size, num_hash_fun, 100, GetSliceHash);
  ASSERT_OK(cuckoo_builder->status());
  unsigned int key_idx = 0;
  std::string expected_file_data = "";
  for (unsigned int i = 0; i < expected_max_buckets; i++) {
    if (key_idx * num_hash_fun == i && key_idx < num_items) {
      cuckoo_builder->Add(Slice(keys[key_idx]), Slice(values[key_idx]));
      ASSERT_EQ(cuckoo_builder->NumEntries(), key_idx + 1);
      ASSERT_OK(cuckoo_builder->status());
      expected_file_data.append(keys[key_idx] + values[key_idx]);
      ++key_idx;
    } else {
      expected_file_data.append(expected_unused_bucket);
    }
  }
  ASSERT_OK(cuckoo_builder->Finish());
  writable_file->Close();
  CheckFileContents(expected_file_data);
}

TEST(CuckooBuilderTest, NoCollisionLastLevel) {
  hash_map.clear();
  std::vector<std::string> user_keys(num_items);
  std::vector<std::string> keys(num_items);
  std::vector<std::string> values(num_items);
  unsigned int bucket_ids = 0;
  for (unsigned int i = 0; i < num_items; i++) {
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
  expected_max_buckets = file_size / bucket_length;
  expected_unused_bucket = "keys10:";
  expected_unused_bucket.resize(bucket_length, 'a');
  unique_ptr<WritableFile> writable_file;
  fname = test::TmpDir() + "/NoCollisionLastLevel_writable_file";
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  CuckooTableBuilder* cuckoo_builder = new CuckooTableBuilder(
      writable_file.get(), key_length,
      value_length, hash_table_ratio,
      file_size, num_hash_fun, 100, GetSliceHash);
  ASSERT_OK(cuckoo_builder->status());
  unsigned int key_idx = 0;
  std::string expected_file_data = "";
  for (unsigned int i = 0; i < expected_max_buckets; i++) {
    if (key_idx * num_hash_fun == i && key_idx < num_items) {
      cuckoo_builder->Add(Slice(keys[key_idx]), Slice(values[key_idx]));
      ASSERT_EQ(cuckoo_builder->NumEntries(), key_idx + 1);
      ASSERT_OK(cuckoo_builder->status());
      expected_file_data.append(user_keys[key_idx] + values[key_idx]);
      ++key_idx;
    } else {
      expected_file_data.append(expected_unused_bucket);
    }
  }
  ASSERT_OK(cuckoo_builder->Finish());
  writable_file->Close();
  CheckFileContents(expected_file_data);
}

TEST(CuckooBuilderTest, WithCollision) {
  // Take keys with colliding hash function values.
  hash_map.clear();
  num_hash_fun = 20;
  num_items = num_hash_fun;
  std::vector<std::string> user_keys(num_items);
  std::vector<std::string> keys(num_items);
  std::vector<std::string> values(num_items);
  for (unsigned int i = 0; i < num_items; i++) {
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
  expected_max_buckets = file_size / bucket_length;
  std::string expected_unused_user_key = "keys10:";
  ParsedInternalKey ikey(expected_unused_user_key, 0, kTypeValue);
  AppendInternalKey(&expected_unused_bucket, ikey);
  expected_unused_bucket.resize(bucket_length, 'a');
  unique_ptr<WritableFile> writable_file;
  fname = test::TmpDir() + "/WithCollision_writable_file";
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  CuckooTableBuilder* cuckoo_builder = new CuckooTableBuilder(
      writable_file.get(), key_length, value_length, hash_table_ratio,
      file_size, num_hash_fun, 100, GetSliceHash);
  ASSERT_OK(cuckoo_builder->status());
  unsigned int key_idx = 0;
  std::string expected_file_data = "";
  for (unsigned int i = 0; i < expected_max_buckets; i++) {
    if (key_idx  == i && key_idx < num_items) {
      cuckoo_builder->Add(Slice(keys[key_idx]), Slice(values[key_idx]));
      ASSERT_EQ(cuckoo_builder->NumEntries(), key_idx + 1);
      ASSERT_OK(cuckoo_builder->status());
      expected_file_data.append(keys[key_idx] + values[key_idx]);
      ++key_idx;
    } else {
      expected_file_data.append(expected_unused_bucket);
    }
  }
  ASSERT_OK(cuckoo_builder->Finish());
  writable_file->Close();
  CheckFileContents(expected_file_data);
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
  for (unsigned int i = 0; i < num_items; i++) {
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
  CuckooTableBuilder* cuckoo_builder = new CuckooTableBuilder(
      writable_file.get(), ikey_length,
      value_length, hash_table_ratio, file_size, num_hash_fun,
      100, GetSliceHash);
  ASSERT_OK(cuckoo_builder->status());
  for (unsigned int key_idx = 0; key_idx < num_items-1; key_idx++) {
    cuckoo_builder->Add(Slice(keys[key_idx]), Slice(values[key_idx]));
    ASSERT_OK(cuckoo_builder->status());
    ASSERT_EQ(cuckoo_builder->NumEntries(), key_idx + 1);
  }
  cuckoo_builder->Add(Slice(keys.back()), Slice(values.back()));
  ASSERT_TRUE(cuckoo_builder->status().IsCorruption());
  cuckoo_builder->Abandon();
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
  CuckooTableBuilder* cuckoo_builder = new CuckooTableBuilder(
      writable_file.get(), ikey_length,
      value_length, hash_table_ratio, file_size, num_hash_fun,
      100, GetSliceHash);
  ASSERT_OK(cuckoo_builder->status());
  cuckoo_builder->Add(Slice(key_to_reuse1), Slice(value));
  ASSERT_OK(cuckoo_builder->status());
  ASSERT_EQ(cuckoo_builder->NumEntries(), 1);
  cuckoo_builder->Add(Slice(key_to_reuse2), Slice(value));
  ASSERT_TRUE(cuckoo_builder->status().IsCorruption());
  cuckoo_builder->Abandon();
  writable_file->Close();
}

TEST(CuckooBuilderTest, WithACollisionPath) {
  hash_map.clear();
  // Have two hash functions. Insert elements with overlapping hashes.
  // Finally insert an element which will displace all the current elements.
  num_hash_fun = 2;

  unsigned int max_search_depth = 100;
  num_items = max_search_depth + 2;
  std::vector<std::string> user_keys(num_items);
  std::vector<std::string> keys(num_items);
  std::vector<std::string> values(num_items);
  std::vector<unsigned int> expected_bucket_id(num_items);
  for (unsigned int i = 0; i < num_items - 1; i++) {
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

  expected_max_buckets = file_size / bucket_length;
  std::string expected_unused_user_key = "keys10:";
  ikey = ParsedInternalKey(expected_unused_user_key, 0, kTypeValue);
  AppendInternalKey(&expected_unused_bucket, ikey);
  expected_unused_bucket.resize(bucket_length, 'a');
  std::string expected_file_data = "";
  for (unsigned int i = 0; i < expected_max_buckets; i++) {
    expected_file_data += expected_unused_bucket;
  }

  unique_ptr<WritableFile> writable_file;
  fname = test::TmpDir() + "/WithCollisionPath_writable_file";
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  CuckooTableBuilder* cuckoo_builder = new CuckooTableBuilder(
      writable_file.get(), key_length,
      value_length, hash_table_ratio, file_size,
      num_hash_fun, max_search_depth, GetSliceHash);
  ASSERT_OK(cuckoo_builder->status());
  for (unsigned int key_idx = 0; key_idx < num_items; key_idx++) {
    cuckoo_builder->Add(Slice(keys[key_idx]), Slice(values[key_idx]));
    ASSERT_OK(cuckoo_builder->status());
    ASSERT_EQ(cuckoo_builder->NumEntries(), key_idx + 1);
    expected_file_data.replace(expected_bucket_id[key_idx]*bucket_length,
        bucket_length, keys[key_idx] + values[key_idx]);
  }
  ASSERT_OK(cuckoo_builder->Finish());
  writable_file->Close();
  CheckFileContents(expected_file_data);
}

TEST(CuckooBuilderTest, FailWhenCollisionPathTooLong) {
  hash_map.clear();
  // Have two hash functions. Insert elements with overlapping hashes.
  // Finally insert an element which will displace all the current elements.
  num_hash_fun = 2;

  unsigned int max_search_depth = 100;
  num_items = max_search_depth + 3;
  std::vector<std::string> user_keys(num_items);
  std::vector<std::string> keys(num_items);
  std::vector<std::string> values(num_items);
  for (unsigned int i = 0; i < num_items - 1; i++) {
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
  CuckooTableBuilder* cuckoo_builder = new CuckooTableBuilder(
      writable_file.get(), ikey_length,
      value_length, hash_table_ratio, file_size, num_hash_fun,
      max_search_depth, GetSliceHash);
  ASSERT_OK(cuckoo_builder->status());
  for (unsigned int key_idx = 0; key_idx < num_items-1; key_idx++) {
    cuckoo_builder->Add(Slice(keys[key_idx]), Slice(values[key_idx]));
    ASSERT_OK(cuckoo_builder->status());
    ASSERT_EQ(cuckoo_builder->NumEntries(), key_idx + 1);
  }
  cuckoo_builder->Add(Slice(keys.back()), Slice(values.back()));
  ASSERT_TRUE(cuckoo_builder->status().IsCorruption());
  cuckoo_builder->Abandon();
  writable_file->Close();
}

TEST(CuckooBuilderTest, FailWhenTableIsFull) {
  hash_map.clear();
  file_size = 160;

  num_items = 7;
  std::vector<std::string> user_keys(num_items);
  std::vector<std::string> keys(num_items);
  std::vector<std::string> values(num_items);
  for (unsigned int i = 0; i < num_items; i++) {
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
  CuckooTableBuilder* cuckoo_builder = new CuckooTableBuilder(
      writable_file.get(), ikey_length,
      value_length, hash_table_ratio, file_size, num_hash_fun,
      100, GetSliceHash);
  ASSERT_OK(cuckoo_builder->status());
  for (unsigned int key_idx = 0; key_idx < num_items-1; key_idx++) {
    cuckoo_builder->Add(Slice(keys[key_idx]), Slice(values[key_idx]));
    ASSERT_OK(cuckoo_builder->status());
    ASSERT_EQ(cuckoo_builder->NumEntries(), key_idx + 1);
  }
  cuckoo_builder->Add(Slice(keys.back()), Slice(values.back()));
  ASSERT_TRUE(cuckoo_builder->status().IsCorruption());
  cuckoo_builder->Abandon();
  writable_file->Close();
}
}  // namespace rocksdb

int main(int argc, char** argv) { return rocksdb::test::RunAllTests(); }
