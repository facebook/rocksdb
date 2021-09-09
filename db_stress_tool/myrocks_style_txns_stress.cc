//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef GFLAGS
#include "db_stress_tool/db_stress_common.h"
#ifndef NDEBUG
#include "utilities/fault_injection_fs.h"
#endif  // NDEBUG

namespace ROCKSDB_NAMESPACE {

class MyRocksRecord {
 public:
  static constexpr uint32_t kPrimaryIndexId = 1;
  static constexpr uint32_t kSecondaryIndexId = 2;

  // Used for generating search key to probe primary index.
  static std::string EncodePrimaryKey(uint32_t a);
  // Used for generating search prefix to probe secondary index.
  static std::string EncodeSecondaryKeyPrefix(uint32_t c);
  // Used for generating search key to probe secondary index.
  static std::string EncodeSecondaryKey(uint32_t c, uint32_t a);

  MyRocksRecord() = default;
  MyRocksRecord(uint32_t _a, uint32_t _b, uint32_t _c)
      : a_(_a), b_(_b), c_(_c) {}

  std::pair<std::string, std::string> EncodePrimaryIndexEntry() const;

  std::pair<std::string, std::string> EncodeSecondaryIndexEntry() const;

  Status DecodePrimaryIndexEntry(Slice primary_index_key,
                                 Slice primary_index_value);

  Status DecodeSecondaryIndexEntry(Slice secondary_index_key,
                                   Slice secondary_index_value);

  uint32_t a_value() const { return a_; }
  uint32_t b_value() const { return b_; }
  uint32_t c_value() const { return c_; }

  std::string ToString() const {
    std::string ret("(");
    ret.append(std::to_string(a_));
    ret.append(",");
    ret.append(std::to_string(c_));
    ret.append(",");
    ret.append(std::to_string(c_));
    ret.append(")");
    return ret;
  }

 private:
  static void Reverse(char* const begin, char* const end) {
    char* p1 = begin;
    char* p2 = end - 1;
    while (p1 < p2) {
      char ch = *p1;
      *p1 = *p2;
      *p2 = ch;
      p1++;
      p2--;
    }
  }

  uint32_t a_{0};
  uint32_t b_{0};
  uint32_t c_{0};
};

std::string MyRocksRecord::EncodePrimaryKey(uint32_t a) {
  char buf[8];
  EncodeFixed32(buf, kPrimaryIndexId);
  EncodeFixed32(buf + 4, a);
  Reverse(buf + 4, buf + 8);
  return std::string(buf, sizeof(buf));
}

std::string MyRocksRecord::EncodeSecondaryKeyPrefix(uint32_t c) {
  char buf[8];
  EncodeFixed32(buf, kSecondaryIndexId);
  EncodeFixed32(buf + 4, c);
  Reverse(buf + 4, buf + 8);
  return std::string(buf, sizeof(buf));
}

std::string MyRocksRecord::EncodeSecondaryKey(uint32_t c, uint32_t a) {
  char buf[12];
  EncodeFixed32(buf, kSecondaryIndexId);
  EncodeFixed32(buf + 4, c);
  EncodeFixed32(buf + 8, a);
  Reverse(buf + 4, buf + 8);
  Reverse(buf + 8, buf + 12);
  return std::string(buf, sizeof(buf));
}

std::pair<std::string, std::string> MyRocksRecord::EncodePrimaryIndexEntry()
    const {
  std::string primary_index_key;
  char buf[8];
  EncodeFixed32(buf, kPrimaryIndexId);
  EncodeFixed32(buf + 4, a_);
  Reverse(buf + 4, buf + 8);
  primary_index_key.assign(buf, sizeof(buf));

  std::string primary_index_value;
  EncodeFixed32(buf, b_);
  EncodeFixed32(buf + 4, c_);
  primary_index_value.assign(buf, sizeof(buf));
  return std::make_pair(primary_index_key, primary_index_value);
}

std::pair<std::string, std::string> MyRocksRecord::EncodeSecondaryIndexEntry()
    const {
  std::string secondary_index_key;
  char buf[12];
  EncodeFixed32(buf, kSecondaryIndexId);
  EncodeFixed32(buf + 4, c_);
  EncodeFixed32(buf + 8, a_);
  Reverse(buf + 4, buf + 8);
  Reverse(buf + 8, buf + 12);
  secondary_index_key.assign(buf, sizeof(buf));

  // Secondary index value is always 4-byte crc32 of the secondary key
  std::string secondary_index_value;
  uint32_t crc = crc32c::Value(buf, sizeof(buf));
  PutFixed32(&secondary_index_value, crc);
  return std::make_pair(secondary_index_key, secondary_index_value);
}

Status MyRocksRecord::DecodePrimaryIndexEntry(Slice primary_index_key,
                                              Slice primary_index_value) {
  if (primary_index_key.size() != 8) {
    assert(false);
    return Status::Corruption("Primary index key length is not 8");
  }
  uint32_t index_id = 0;
  bool result = GetFixed32(&primary_index_key, &index_id);
  assert(result);
  if (index_id != kPrimaryIndexId) {
    std::ostringstream oss;
    oss << "Unexpected primary index id: " << index_id;
    return Status::Corruption(oss.str());
  }
  const char* const buf = primary_index_key.data();
  a_ = static_cast<uint32_t>(buf[0]) << 24;
  a_ += static_cast<uint32_t>(buf[1]) << 16;
  a_ += static_cast<uint32_t>(buf[2]) << 8;
  a_ += static_cast<uint32_t>(buf[3]);

  if (primary_index_value.size() != 8) {
    return Status::Corruption("Primary index value length is not 8");
  }
  GetFixed32(&primary_index_value, &b_);
  GetFixed32(&primary_index_value, &c_);
  return Status::OK();
}

Status MyRocksRecord::DecodeSecondaryIndexEntry(Slice secondary_index_key,
                                                Slice secondary_index_value) {
  if (secondary_index_key.size() != 12) {
    return Status::Corruption("Secondary index key length is not 12");
  }
  uint32_t crc =
      crc32c::Value(secondary_index_key.data(), secondary_index_key.size());
  uint32_t index_id = 0;
  bool result = GetFixed32(&secondary_index_key, &index_id);
  assert(result);
  if (index_id != kSecondaryIndexId) {
    std::ostringstream oss;
    oss << "Unexpected secondary index id: " << index_id;
    return Status::Corruption(oss.str());
  }
  const char* const buf = secondary_index_key.data();
  c_ = static_cast<uint32_t>(buf[0]) << 24;
  c_ += static_cast<uint32_t>(buf[1]) << 16;
  c_ += static_cast<uint32_t>(buf[2]) << 8;
  c_ += static_cast<uint32_t>(buf[3]);

  a_ = static_cast<uint32_t>(buf[4]) << 24;
  a_ += static_cast<uint32_t>(buf[5]) << 16;
  a_ += static_cast<uint32_t>(buf[6]) << 8;
  a_ += static_cast<uint32_t>(buf[7]);

  if (secondary_index_value.size() != 4) {
    return Status::Corruption("Secondary index value length is not 4");
  }
  uint32_t val = 0;
  GetFixed32(&secondary_index_value, &val);
  if (val != crc) {
    std::ostringstream oss;
    oss << "Secondary index key checksum mismatch, stored: " << val
        << ", recomputed: " << crc;
    return Status::Corruption(oss.str());
  }
  return Status::OK();
}

class MyRocksStyleTxnsStressTest : public StressTest {
 public:
  MyRocksStyleTxnsStressTest() {}

  ~MyRocksStyleTxnsStressTest() override {}

  Status TestGet(ThreadState* thread, const ReadOptions& read_opts,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys) override;

  std::vector<Status> TestMultiGet(
      ThreadState* thread, const ReadOptions& read_opts,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) override;

  Status TestPrefixScan(ThreadState* thread, const ReadOptions& read_opts,
                        const std::vector<int>& rand_column_families,
                        const std::vector<int64_t>& rand_keys) override;

  // Given a key K, this creates an iterator which scans to K and then
  // does a random sequence of Next/Prev operations.
  Status TestIterate(ThreadState* thread, const ReadOptions& read_opts,
                     const std::vector<int>& rand_column_families,
                     const std::vector<int64_t>& rand_keys) override;

  Status TestPut(ThreadState* thread, WriteOptions& write_opts,
                 const ReadOptions& read_opts, const std::vector<int>& cf_ids,
                 const std::vector<int64_t>& keys, char (&value)[100],
                 std::unique_ptr<MutexLock>& lock) override;

  Status TestDelete(ThreadState* thread, WriteOptions& write_opts,
                    const std::vector<int>& rand_column_families,
                    const std::vector<int64_t>& rand_keys,
                    std::unique_ptr<MutexLock>& lock) override;

  Status TestDeleteRange(ThreadState* thread, WriteOptions& write_opts,
                         const std::vector<int>& rand_column_families,
                         const std::vector<int64_t>& rand_keys,
                         std::unique_ptr<MutexLock>& lock) override;

  void TestIngestExternalFile(ThreadState* thread,
                              const std::vector<int>& rand_column_families,
                              const std::vector<int64_t>& rand_keys,
                              std::unique_ptr<MutexLock>& lock) override;

  Status PrimaryKeyUpdateTxn(uint32_t old_pk, uint32_t new_pk);

  Status SecondaryKeyUpdateTxn(uint32_t old_sk, uint32_t new_sk);

  Status UpdatePrimaryIndexValue(uint32_t pk, uint32_t val_delta);

  void VerifyDb(ThreadState* thread) const override;
};

// Used for point-lookup transaction
Status MyRocksStyleTxnsStressTest::TestGet(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  (void)thread;
  (void)read_opts;
  (void)rand_column_families;
  (void)rand_keys;
  return Status::OK();
}

// Not used
std::vector<Status> MyRocksStyleTxnsStressTest::TestMultiGet(
    ThreadState* /*thread*/, const ReadOptions& /*read_opts*/,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/) {
  return std::vector<Status>{Status::NotSupported()};
}

Status MyRocksStyleTxnsStressTest::TestPrefixScan(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  (void)thread;
  (void)read_opts;
  (void)rand_column_families;
  (void)rand_keys;
  return Status::OK();
}

// Given a key K, this creates an iterator which scans to K and then
// does a random sequence of Next/Prev operations.
Status MyRocksStyleTxnsStressTest::TestIterate(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  (void)thread;
  (void)read_opts;
  (void)rand_column_families;
  (void)rand_keys;

  return Status::OK();
}

// Not intended for use.
Status MyRocksStyleTxnsStressTest::TestPut(
    ThreadState* /*thread*/, WriteOptions& /*write_opts*/,
    const ReadOptions& /*read_opts*/, const std::vector<int>& /*cf_ids*/,
    const std::vector<int64_t>& /*keys*/, char (&value)[100],
    std::unique_ptr<MutexLock>& /*lock*/) {
  (void)value;
  return Status::NotSupported();
}

// Not intended for use.
Status MyRocksStyleTxnsStressTest::TestDelete(
    ThreadState* /*thread*/, WriteOptions& /*write_opts*/,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/,
    std::unique_ptr<MutexLock>& /*lock*/) {
  return Status::NotSupported();
}

// Not intended for use.
Status MyRocksStyleTxnsStressTest::TestDeleteRange(
    ThreadState* /*thread*/, WriteOptions& /*write_opts*/,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/,
    std::unique_ptr<MutexLock>& /*lock*/) {
  return Status::NotSupported();
}

// Not intended for use.
void MyRocksStyleTxnsStressTest::TestIngestExternalFile(
    ThreadState* /*thread*/, const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/,
    std::unique_ptr<MutexLock>& /*lock*/) {}

void MyRocksStyleTxnsStressTest::VerifyDb(ThreadState* thread) const {
  (void)thread;
}

StressTest* CreateMyRocksStyleTxnsStressTest() {
  return new MyRocksStyleTxnsStressTest();
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
