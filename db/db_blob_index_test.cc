//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/column_family.h"
#include "db/db_test_util.h"
#include "db/write_batch_internal.h"
#include "port/stack_trace.h"
#include "util/string_util.h"

namespace rocksdb {

// kTypeBlobIndex is a value type used by BlobDB only. The base rocksdb
// should accept the value type on write, and report not supported value
// for reads, unless caller request for it explicitly. The base rocksdb
// doesn't understand format of actual blob index (the value).
class DBBlobIndexTest : public DBTestBase {
 public:
  DBBlobIndexTest() : DBTestBase("/db_blob_index_test") {}

  ColumnFamilyHandle* cfh() { return dbfull()->DefaultColumnFamily(); }

  uint32_t column_family_id() {
    return reinterpret_cast<ColumnFamilyHandleImpl*>(cfh())->cfd()->GetID();
  }

  Status PutBlobIndex(WriteBatch* batch, const Slice& key,
                      const Slice& blob_index) {
    return WriteBatchInternal::PutBlobIndex(batch, column_family_id(), key,
                                            blob_index);
  }

  Status Write(WriteBatch* batch) {
    return dbfull()->Write(WriteOptions(), batch);
  }

  std::string GetImpl(const Slice& key, bool* is_blob_index = nullptr) {
    PinnableSlice value;
    auto s = dbfull()->GetImpl(ReadOptions(), cfh(), key, &value,
                               nullptr /*value_found*/, nullptr /*callback*/,
                               is_blob_index);
    if (s.IsNotFound()) {
      return "NOT_FOUND";
    }
    if (s.IsNotSupported()) {
      return "NOT_SUPPORTED";
    }
    if (!s.ok()) {
      return s.ToString();
    }
    return value.ToString();
  }

  std::string GetBlobIndex(const Slice& key) {
    bool is_blob_index = false;
    std::string value = GetImpl(key, &is_blob_index);
    if (!is_blob_index) {
      return "NOT_BLOB";
    }
    return value;
  }
};

// Should be able to write kTypeBlobIndex to memtables and SST files.
TEST_F(DBBlobIndexTest, Write) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  // Disable auto flushes.
  options.max_write_buffer_number = 10;
  options.min_write_buffer_number_to_merge = 10;
  options.num_levels = 2;

  auto check_value = [this]() {
    for (int i = 1; i <= 5; i++) {
      std::string index = ToString(i);
      ASSERT_EQ("blob" + index, GetBlobIndex("key" + index));
    }
  };

  // Write to memtables.
  DestroyAndReopen(options);
  for (int i = 1; i <= 5; i++) {
    std::string index = ToString(i);
    WriteBatch batch;
    ASSERT_OK(PutBlobIndex(&batch, "key" + index, "blob" + index));
    ASSERT_OK(Write(&batch));
    if (i < 5) {
      ASSERT_OK(dbfull()->TEST_SwitchMemtable());
    }
  }
  check_value();

  // Flush
  DestroyAndReopen(options);
  for (int i = 1; i <= 5; i++) {
    std::string index = ToString(i);
    WriteBatch batch;
    ASSERT_OK(PutBlobIndex(&batch, "key" + index, "blob" + index));
    ASSERT_OK(Write(&batch));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("5", FilesPerLevel());
  check_value();

  // Compaction
  // Trick to avoid trival move during compaction.
  ASSERT_OK(Put("key0", "v"));
  ASSERT_OK(Put("key6", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,1", FilesPerLevel());
  check_value();
}

// Should be able to get blob index if is_blob_index is provided, otherwise
// return Status::NotSupported status.
TEST_F(DBBlobIndexTest, Get) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  // Disable auto flushes.
  options.max_write_buffer_number = 10;
  options.min_write_buffer_number_to_merge = 10;
  Reopen(options);

  // Setup
  WriteBatch batch;
  ASSERT_OK(batch.Put("key_sst", "value_sst"));
  ASSERT_OK(PutBlobIndex(&batch, "blob_key_sst", "blob_sst"));
  ASSERT_OK(Write(&batch));
  ASSERT_OK(Flush());
  batch.Clear();
  ASSERT_OK(batch.Put("key_imm", "value_imm"));
  ASSERT_OK(PutBlobIndex(&batch, "blob_key_imm", "blob_imm"));
  ASSERT_OK(Write(&batch));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  batch.Clear();
  ASSERT_OK(batch.Put("key_mem", "value_mem"));
  ASSERT_OK(PutBlobIndex(&batch, "blob_key_mem", "blob_mem"));
  ASSERT_OK(Write(&batch));
  ASSERT_EQ("1", FilesPerLevel());

  auto verify_normal_key = [this](const std::string& key,
                                  const std::string& expected_value) {
    ASSERT_EQ(expected_value, Get(key));
    ASSERT_EQ(expected_value, GetImpl(key));
    bool is_blob_index = false;
    ASSERT_EQ(expected_value, GetImpl(key, &is_blob_index));
    ASSERT_FALSE(is_blob_index);
  };
  verify_normal_key("key_mem", "value_mem");
  verify_normal_key("key_imm", "value_imm");
  verify_normal_key("key_sst", "value_sst");

  auto verify_blob_key = [this](const std::string& key,
                                const std::string& expected_value) {
    PinnableSlice value;
    ASSERT_TRUE(Get(key, &value).IsNotSupported());
    ASSERT_EQ("NOT_SUPPORTED", GetImpl(key));
    bool is_blob_index = false;
    ASSERT_EQ(expected_value, GetImpl(key, &is_blob_index));
    ASSERT_TRUE(is_blob_index);
  };
  verify_blob_key("blob_key_mem", "blob_mem");
  verify_blob_key("blob_key_imm", "blob_imm");
  verify_blob_key("blob_key_sst", "blob_sst");
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
