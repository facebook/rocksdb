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
#include "rocksdb/utilities/write_batch_with_index.h"
#ifndef NDEBUG
#include "utilities/fault_injection_fs.h"
#endif  // NDEBUG

namespace ROCKSDB_NAMESPACE {

// TODO: move these to gflags.
static constexpr size_t kInitNumC = 1000;
static constexpr int kCARatio = 3;
static constexpr bool kDoPreload = true;

class MyRocksRecord {
 public:
  static constexpr uint32_t kPrimaryIndexId = 1;
  static constexpr uint32_t kSecondaryIndexId = 2;

  static_assert(kPrimaryIndexId < kSecondaryIndexId,
                "kPrimaryIndexId must be smaller than kSecondaryIndexId");

  // Used for generating search key to probe primary index.
  static std::string EncodePrimaryKey(uint32_t a);
  // Used for generating search prefix to probe secondary index.
  static std::string EncodeSecondaryKey(uint32_t c);
  // Used for generating search key to probe secondary index.
  static std::string EncodeSecondaryKey(uint32_t c, uint32_t a);

  static std::tuple<Status, uint32_t, uint32_t> DecodePrimaryIndexValue(
      Slice primary_index_value);

  static std::pair<Status, uint32_t> DecodeSecondaryIndexValue(
      Slice secondary_index_value);

  MyRocksRecord() = default;
  MyRocksRecord(uint32_t _a, uint32_t _b, uint32_t _c)
      : a_(_a), b_(_b), c_(_c) {}

  bool operator==(const MyRocksRecord& other) const {
    return a_ == other.a_ && b_ == other.b_ && c_ == other.c_;
  }

  bool operator!=(const MyRocksRecord& other) const {
    return !(*this == other);
  }

  std::pair<std::string, std::string> EncodePrimaryIndexEntry() const;

  std::string EncodePrimaryKey() const;

  std::string EncodePrimaryIndexValue() const;

  std::pair<std::string, std::string> EncodeSecondaryIndexEntry() const;

  std::string EncodeSecondaryKey() const;

  Status DecodePrimaryIndexEntry(Slice primary_index_key,
                                 Slice primary_index_value);

  Status DecodeSecondaryIndexEntry(Slice secondary_index_key,
                                   Slice secondary_index_value);

  uint32_t a_value() const { return a_; }
  uint32_t b_value() const { return b_; }
  uint32_t c_value() const { return c_; }

  void SetA(uint32_t _a) { a_ = _a; }
  void SetB(uint32_t _b) { b_ = _b; }
  void SetC(uint32_t _c) { c_ = _c; }

  std::string ToString() const {
    std::string ret("(");
    ret.append(std::to_string(a_));
    ret.append(",");
    ret.append(std::to_string(b_));
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
  Reverse(buf, buf + 4);
  EncodeFixed32(buf + 4, a);
  Reverse(buf + 4, buf + 8);
  return std::string(buf, sizeof(buf));
}

std::string MyRocksRecord::EncodeSecondaryKey(uint32_t c) {
  char buf[8];
  EncodeFixed32(buf, kSecondaryIndexId);
  Reverse(buf, buf + 4);
  EncodeFixed32(buf + 4, c);
  Reverse(buf + 4, buf + 8);
  return std::string(buf, sizeof(buf));
}

std::string MyRocksRecord::EncodeSecondaryKey(uint32_t c, uint32_t a) {
  char buf[12];
  EncodeFixed32(buf, kSecondaryIndexId);
  Reverse(buf, buf + 4);
  EncodeFixed32(buf + 4, c);
  EncodeFixed32(buf + 8, a);
  Reverse(buf + 4, buf + 8);
  Reverse(buf + 8, buf + 12);
  return std::string(buf, sizeof(buf));
}

std::tuple<Status, uint32_t, uint32_t> MyRocksRecord::DecodePrimaryIndexValue(
    Slice primary_index_value) {
  if (primary_index_value.size() != 8) {
    return std::tuple<Status, uint32_t, uint32_t>{Status::Corruption(""), 0, 0};
  }
  const char* const buf = primary_index_value.data();
  uint32_t b = static_cast<uint32_t>(static_cast<unsigned char>(buf[0])) << 24;
  b += static_cast<uint32_t>(static_cast<unsigned char>(buf[1])) << 16;
  b += static_cast<uint32_t>(static_cast<unsigned char>(buf[2])) << 8;
  b += static_cast<uint32_t>(static_cast<unsigned char>(buf[3]));

  uint32_t c = static_cast<uint32_t>(static_cast<unsigned char>(buf[4])) << 24;
  c += static_cast<uint32_t>(static_cast<unsigned char>(buf[5])) << 16;
  c += static_cast<uint32_t>(static_cast<unsigned char>(buf[6])) << 8;
  c += static_cast<uint32_t>(static_cast<unsigned char>(buf[7]));
  return std::tuple<Status, uint32_t, uint32_t>{Status::OK(), b, c};
}

std::pair<Status, uint32_t> MyRocksRecord::DecodeSecondaryIndexValue(
    Slice secondary_index_value) {
  if (secondary_index_value.size() != 4) {
    return std::make_pair(Status::Corruption(""), 0);
  }
  uint32_t crc = 0;
  bool result = GetFixed32(&secondary_index_value, &crc);
  assert(result);
  return std::make_pair(Status::OK(), crc);
}

std::pair<std::string, std::string> MyRocksRecord::EncodePrimaryIndexEntry()
    const {
  std::string primary_index_key;
  char buf[8];
  EncodeFixed32(buf, kPrimaryIndexId);
  Reverse(buf, buf + 4);
  EncodeFixed32(buf + 4, a_);
  Reverse(buf + 4, buf + 8);
  primary_index_key.assign(buf, sizeof(buf));

  std::string primary_index_value;
  EncodeFixed32(buf, b_);
  Reverse(buf, buf + 4);
  EncodeFixed32(buf + 4, c_);
  Reverse(buf + 4, buf + 8);
  primary_index_value.assign(buf, sizeof(buf));
  return std::make_pair(primary_index_key, primary_index_value);
}

std::string MyRocksRecord::EncodePrimaryKey() const {
  char buf[8];
  EncodeFixed32(buf, kPrimaryIndexId);
  Reverse(buf, buf + 4);
  EncodeFixed32(buf + 4, a_);
  Reverse(buf + 4, buf + 8);
  return std::string(buf, sizeof(buf));
}

std::string MyRocksRecord::EncodePrimaryIndexValue() const {
  char buf[8];
  EncodeFixed32(buf, b_);
  EncodeFixed32(buf + 4, c_);
  return std::string(buf, sizeof(buf));
}

std::pair<std::string, std::string> MyRocksRecord::EncodeSecondaryIndexEntry()
    const {
  std::string secondary_index_key;
  char buf[12];
  EncodeFixed32(buf, kSecondaryIndexId);
  Reverse(buf, buf + 4);
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

std::string MyRocksRecord::EncodeSecondaryKey() const {
  char buf[12];
  EncodeFixed32(buf, kSecondaryIndexId);
  Reverse(buf, buf + 4);
  EncodeFixed32(buf + 4, c_);
  EncodeFixed32(buf + 8, a_);
  Reverse(buf + 4, buf + 8);
  Reverse(buf + 8, buf + 12);
  return std::string(buf, sizeof(buf));
}

Status MyRocksRecord::DecodePrimaryIndexEntry(Slice primary_index_key,
                                              Slice primary_index_value) {
  if (primary_index_key.size() != 8) {
    assert(false);
    return Status::Corruption("Primary index key length is not 8");
  }

  const char* const index_id_buf = primary_index_key.data();
  uint32_t index_id =
      static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[0])) << 24;
  index_id += static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[1]))
              << 16;
  index_id += static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[2]))
              << 8;
  index_id +=
      static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[3]));
  primary_index_key.remove_prefix(sizeof(uint32_t));
  if (index_id != kPrimaryIndexId) {
    std::ostringstream oss;
    oss << "Unexpected primary index id: " << index_id;
    return Status::Corruption(oss.str());
  }

  const char* const buf = primary_index_key.data();
  a_ = static_cast<uint32_t>(static_cast<unsigned char>(buf[0])) << 24;
  a_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[1])) << 16;
  a_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[2])) << 8;
  a_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[3]));

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

  const char* const index_id_buf = secondary_index_key.data();
  uint32_t index_id =
      static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[0])) << 24;
  index_id += static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[1]))
              << 16;
  index_id += static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[2]))
              << 8;
  index_id +=
      static_cast<uint32_t>(static_cast<unsigned char>(index_id_buf[3]));
  secondary_index_key.remove_prefix(sizeof(uint32_t));
  if (index_id != kSecondaryIndexId) {
    std::ostringstream oss;
    oss << "Unexpected secondary index id: " << index_id;
    return Status::Corruption(oss.str());
  }

  const char* const buf = secondary_index_key.data();
  assert(secondary_index_key.size() == 8);
  c_ = static_cast<uint32_t>(static_cast<unsigned char>(buf[0])) << 24;
  c_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[1])) << 16;
  c_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[2])) << 8;
  c_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[3]));

  a_ = static_cast<uint32_t>(static_cast<unsigned char>(buf[4])) << 24;
  a_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[5])) << 16;
  a_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[6])) << 8;
  a_ += static_cast<uint32_t>(static_cast<unsigned char>(buf[7]));

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

  void FinishInitDb(SharedState*) override;

  void ReopenAndPreloadDb(SharedState* shared);

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

  void TestCompactRange(ThreadState* thread, int64_t rand_key,
                        const Slice& start_key,
                        ColumnFamilyHandle* column_family) override;

  Status TestBackupRestore(ThreadState* thread,
                           const std::vector<int>& rand_column_families,
                           const std::vector<int64_t>& rand_keys) override;

  Status TestCheckpoint(ThreadState* thread,
                        const std::vector<int>& rand_column_families,
                        const std::vector<int64_t>& rand_keys) override;

  Status TestApproximateSize(ThreadState* thread, uint64_t iteration,
                             const std::vector<int>& rand_column_families,
                             const std::vector<int64_t>& rand_keys) override;

  Status TestCustomOperations(
      ThreadState* thread,
      const std::vector<int>& rand_column_families) override;

  Status PrimaryKeyUpdateTxn(ThreadState* thread, uint32_t old_a,
                             uint32_t new_a);

  Status SecondaryKeyUpdateTxn(ThreadState* thread, uint32_t old_c,
                               uint32_t new_c);

  Status UpdatePrimaryIndexValueTxn(ThreadState* thread, uint32_t a,
                                    uint32_t b_delta);

  Status PointLookupTxn(ThreadState* thread, ReadOptions ropts, uint32_t a);

  Status RangeScanTxn(ThreadState* thread, ReadOptions ropts, uint32_t c);

  void VerifyDb(ThreadState* thread) const override;

 protected:
  uint32_t GeneratePrimaryIndexKeyForPointLookup(ThreadState* thread) const;

 private:
  void PreloadDb(SharedState* shared, size_t num_c);

  std::atomic<uint32_t> next_a_{0};

  std::atomic<uint32_t> min_a_{0};
};

void MyRocksStyleTxnsStressTest::FinishInitDb(SharedState* shared) {
  if (FLAGS_enable_compaction_filter) {
    // TODO (yanqin) enable compaction filter
  }
  if (kDoPreload) {
    ReopenAndPreloadDb(shared);
  }
}

void MyRocksStyleTxnsStressTest::ReopenAndPreloadDb(SharedState* shared) {
  (void)shared;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  for (const auto* handle : column_families_) {
    cf_descs.emplace_back(handle->GetName(), ColumnFamilyOptions(options_));
  }
  CancelAllBackgroundWork(db_, /*wait=*/true);
  for (auto* handle : column_families_) {
    delete handle;
  }
  column_families_.clear();
  delete db_;
  db_ = nullptr;
  txn_db_ = nullptr;

  TransactionDBOptions txn_db_opts;
  txn_db_opts.skip_concurrency_control = true;  // speed-up preloading
  Status s = TransactionDB::Open(options_, txn_db_opts, FLAGS_db, cf_descs,
                                 &column_families_, &txn_db_);
  if (s.ok()) {
    db_ = txn_db_;
  } else {
    fprintf(stderr, "Failed to open db: %s\n", s.ToString().c_str());
    exit(1);
  }

  PreloadDb(shared, kInitNumC);

  // Reopen
  CancelAllBackgroundWork(db_, /*wait=*/true);
  for (auto* handle : column_families_) {
    delete handle;
  }
  column_families_.clear();
  s = db_->Close();
  if (!s.ok()) {
    fprintf(stderr, "Error during closing db: %s\n", s.ToString().c_str());
    exit(1);
  }
  delete db_;
  db_ = nullptr;
  txn_db_ = nullptr;

  Open();
}

// Used for point-lookup transaction
Status MyRocksStyleTxnsStressTest::TestGet(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/) {
  uint32_t a = GeneratePrimaryIndexKeyForPointLookup(thread);
  return PointLookupTxn(thread, read_opts, a);
}

// Not used.
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
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/) {
  (void)thread;
  (void)read_opts;
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

void MyRocksStyleTxnsStressTest::TestIngestExternalFile(
    ThreadState* thread, const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& /*rand_keys*/,
    std::unique_ptr<MutexLock>& /*lock*/) {
  // TODO (yanqin)
  (void)thread;
  (void)rand_column_families;
}

void MyRocksStyleTxnsStressTest::TestCompactRange(
    ThreadState* thread, int64_t /*rand_key*/, const Slice& /*start_key*/,
    ColumnFamilyHandle* column_family) {
  // TODO (yanqin).
  // May use GetRangeHash() for validation before and after DB::CompactRange()
  // completes.
  (void)thread;
  (void)column_family;
}

Status MyRocksStyleTxnsStressTest::TestBackupRestore(
    ThreadState* thread, const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& /*rand_keys*/) {
  // TODO (yanqin)
  (void)thread;
  (void)rand_column_families;
  return Status::OK();
}

Status MyRocksStyleTxnsStressTest::TestCheckpoint(
    ThreadState* thread, const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& /*rand_keys*/) {
  // TODO (yanqin)
  (void)thread;
  (void)rand_column_families;
  return Status::OK();
}

Status MyRocksStyleTxnsStressTest::TestApproximateSize(
    ThreadState* thread, uint64_t iteration,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& /*rand_keys*/) {
  // TODO (yanqin)
  (void)thread;
  (void)iteration;
  (void)rand_column_families;
  return Status::OK();
}

Status MyRocksStyleTxnsStressTest::TestCustomOperations(
    ThreadState* thread, const std::vector<int>& rand_column_families) {
  // TODO (yanqin)
  (void)thread;
  (void)rand_column_families;
  return Status::OK();
}

Status MyRocksStyleTxnsStressTest::PrimaryKeyUpdateTxn(ThreadState* thread,
                                                       uint32_t old_a,
                                                       uint32_t new_a) {
  std::string old_pk = MyRocksRecord::EncodePrimaryKey(old_a);
  std::string new_pk = MyRocksRecord::EncodePrimaryKey(new_a);
  Transaction* txn = nullptr;
  WriteOptions wopts;
  Status s = NewTxn(wopts, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }
  txn->SetSnapshotOnNextOperation(/*notifier=*/nullptr);

  const auto defer([&s, thread, txn, this]() {
    if (s.ok()) {
      return;
    }
    if (s.IsNotFound()) {
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/0);
    } else if (s.IsBusy()) {
      // ignore.
    } else {
      thread->stats.AddErrors(1);
    }
    RollbackTxn(txn).PermitUncheckedError();
  });

  ReadOptions ropts;
  std::string value;
  s = txn->GetForUpdate(ropts, old_pk, &value);
  if (!s.ok()) {
    return s;
  }
  std::string empty_value;
  s = txn->GetForUpdate(ropts, new_pk, &empty_value);
  if (s.ok()) {
    s = Status::Busy();
    return s;
  }

  auto result = MyRocksRecord::DecodePrimaryIndexValue(value);
  s = std::get<0>(result);
  if (!s.ok()) {
    return s;
  }
  uint32_t b = std::get<1>(result);
  uint32_t c = std::get<2>(result);

  s = txn->Delete(old_pk);
  if (!s.ok()) {
    return s;
  }
  s = txn->Put(new_pk, value);
  if (!s.ok()) {
    return s;
  }

  auto* wb = txn->GetWriteBatch();
  assert(wb);

  std::string old_sk = MyRocksRecord::EncodeSecondaryKey(c, old_a);
  s = wb->SingleDelete(old_sk);
  if (!s.ok()) {
    return s;
  }

  MyRocksRecord record(new_a, b, c);
  std::string new_sk;
  std::string new_crc;
  std::tie(new_sk, new_crc) = record.EncodeSecondaryIndexEntry();
  s = wb->Put(new_sk, new_crc);
  if (!s.ok()) {
    return s;
  }

  s = CommitTxn(txn);
  return s;
}

Status MyRocksStyleTxnsStressTest::SecondaryKeyUpdateTxn(ThreadState* thread,
                                                         uint32_t old_c,
                                                         uint32_t new_c) {
  Transaction* txn = nullptr;
  WriteOptions wopts;
  Status s = NewTxn(wopts, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }
  Iterator* it = nullptr;
  const auto defer([&s, thread, it, txn, this]() {
    if (s.ok()) {
      return;
    }
    delete it;
    RollbackTxn(txn).PermitUncheckedError();
  });

  txn->SetSnapshotOnNextOperation(/*notifier=*/nullptr);
  std::string old_sk_prefix = MyRocksRecord::EncodeSecondaryKey(old_c);
  std::string iter_ub_str = MyRocksRecord::EncodeSecondaryKey(old_c + 1);
  Slice iter_ub = iter_ub_str;
  ReadOptions ropts;
  ropts.iterate_upper_bound = &iter_ub;
  it = txn->GetIterator(ropts);
  it->Seek(old_sk_prefix);
  if (!it->Valid()) {
    s = Status::NotFound();
    return s;
  }
  auto* wb = txn->GetWriteBatch();
  // it->Valid() is true here.
  do {
    MyRocksRecord record;
    s = record.DecodeSecondaryIndexEntry(it->key(), it->value());
    if (!s.ok()) {
      break;
    }
    // At this point, record.b is not known yet, thus we need to access
    // primary index.
    std::string pk = MyRocksRecord::EncodePrimaryKey(record.a_value());
    std::string value;
    s = txn->GetForUpdate(ReadOptions(), pk, &value);
    if (!s.ok()) {
      // We can also fail verification here.
      break;
    }
    auto result = MyRocksRecord::DecodePrimaryIndexValue(value);
    s = std::get<0>(result);
    if (!s.ok()) {
      break;
    }
    uint32_t b = std::get<1>(result);
    uint32_t c = std::get<2>(result);
    if (c != old_c) {
      s = Status::Corruption();
      break;
    }
    MyRocksRecord new_rec(record.a_value(), b, new_c);
    std::string new_primary_index_value = new_rec.EncodePrimaryIndexValue();
    s = txn->Put(pk, new_primary_index_value);
    if (!s.ok()) {
      break;
    }
    std::string old_sk = it->key().ToString(/*hex=*/false);
    std::string new_sk;
    std::string new_crc;
    std::tie(new_sk, new_crc) = new_rec.EncodeSecondaryIndexEntry();
    s = wb->SingleDelete(old_sk);
    if (!s.ok()) {
      break;
    }
    s = wb->Put(new_sk, new_crc);
    if (!s.ok()) {
      break;
    }

    it->Next();
  } while (it->Valid());
  if (!s.ok()) {
    return s;
  }
  s = CommitTxn(txn);
  return s;
}

Status MyRocksStyleTxnsStressTest::UpdatePrimaryIndexValueTxn(
    ThreadState* thread, uint32_t a, uint32_t b_delta) {
  std::string pk_str = MyRocksRecord::EncodePrimaryKey(a);
  Transaction* txn = nullptr;
  WriteOptions wopts;
  Status s = NewTxn(wopts, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }
  const auto defer([&s, thread, txn, this]() {
    if (s.ok()) {
      return;
    }
    if (s.IsNotFound()) {
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/0);
    } else if (s.IsInvalidArgument()) {
      // ignored.
    } else if (s.IsBusy() || s.IsTimedOut() || s.IsTryAgain() ||
               s.IsMergeInProgress()) {
      // ignored.
    } else {
      thread->stats.AddErrors(1);
    }
    RollbackTxn(txn).PermitUncheckedError();
  });
  ReadOptions ropts;
  std::string value;
  s = txn->GetForUpdate(ropts, pk_str, &value);
  if (!s.ok()) {
    return s;
  }
  auto result = MyRocksRecord::DecodePrimaryIndexValue(value);
  if (!std::get<0>(result).ok()) {
    return s;
  }
  uint32_t b = std::get<1>(result) + b_delta;
  uint32_t c = std::get<2>(result);
  MyRocksRecord record(a, b, c);
  std::string primary_index_value = record.EncodePrimaryIndexValue();
  s = txn->Put(pk_str, primary_index_value);
  if (!s.ok()) {
    return s;
  }
  s = CommitTxn(txn);
  if (s.ok()) {
    thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/1);
    thread->stats.AddBytesForWrites(
        /*nwrites=*/1, /*nbytes=*/pk_str.size() + primary_index_value.size());
  }
  return s;
}

Status MyRocksStyleTxnsStressTest::PointLookupTxn(ThreadState* thread,
                                                  ReadOptions ropts,
                                                  uint32_t a) {
  WriteOptions wopts;
  Transaction* txn = nullptr;
  Status s = NewTxn(wopts, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }
  const auto defer([&s, thread, txn, this]() {
    if (s.ok()) {
      return;
    }
    if (!s.ok() && !s.IsNotFound()) {
      thread->stats.AddErrors(1);
    } else if (s.IsNotFound()) {
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/0);
    }
    RollbackTxn(txn).PermitUncheckedError();
  });
  std::string pk_str = MyRocksRecord::EncodePrimaryKey(a);
  // pk may or may not exist
  std::string value;
  s = txn->Get(ropts, pk_str, &value);
  if (!s.ok()) {
    return s;
  }
  s = CommitTxn(txn);
  if (s.ok()) {
    thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/1);
  }
  return s;
}

Status MyRocksStyleTxnsStressTest::RangeScanTxn(ThreadState* thread,
                                                ReadOptions ropts, uint32_t c) {
  // TODO (yanqin)
  WriteOptions wopts;
  Transaction* txn = nullptr;
  Status s = NewTxn(wopts, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }
  const auto defer([&s, thread, txn, this]() {
    if (s.ok()) {
      return;
    }
    thread->stats.AddErrors(1);
    RollbackTxn(txn).PermitUncheckedError();
  });
  std::string sk = MyRocksRecord::EncodeSecondaryKey(c);
  std::unique_ptr<Iterator> iter(txn->GetIterator(ropts));
  iter->Seek(sk);
  if (!iter->status().ok()) {
    s = iter->status();
    return s;
  }
  thread->stats.AddIterations(1);
  s = CommitTxn(txn);
  return s;
}

void MyRocksStyleTxnsStressTest::VerifyDb(ThreadState* thread) const {
  if (thread->shared->HasVerificationFailedYet()) {
    return;
  }
  const Snapshot* const snapshot = db_->GetSnapshot();
  assert(snapshot);
  ManagedSnapshot snapshot_guard(db_, snapshot);

  // TODO (yanqin) with a probability, we can use either forward or backward
  // iterator in subsequent checks. We can also use more advanced features in
  // range scan. For now, let's just use simple forward iteration with
  // total_order_seek = true.

  // First, iterate primary index.
  size_t primary_index_entries_count = 0;
  {
    char buf[4];
    EncodeFixed32(buf, MyRocksRecord::kPrimaryIndexId + 1);
    std::reverse(buf, buf + sizeof(buf));
    std::string iter_ub_str(buf, sizeof(buf));
    Slice iter_ub = iter_ub_str;

    ReadOptions ropts;
    ropts.snapshot = snapshot;
    ropts.total_order_seek = true;
    ropts.iterate_upper_bound = &iter_ub;

    std::unique_ptr<Iterator> it(db_->NewIterator(ropts));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ++primary_index_entries_count;
    }
  }

  // Second, iterate secondary index.
  size_t secondary_index_entries_count = 0;
  {
    char buf[4];
    EncodeFixed32(buf, MyRocksRecord::kSecondaryIndexId);
    std::reverse(buf, buf + sizeof(buf));
    const std::string start_key(buf, sizeof(buf));

    ReadOptions ropts;
    ropts.snapshot = snapshot;
    ropts.total_order_seek = true;

    std::unique_ptr<Iterator> it(db_->NewIterator(ropts));
    for (it->Seek(start_key); it->Valid(); it->Next()) {
      ++secondary_index_entries_count;
      MyRocksRecord record;
      Status s = record.DecodeSecondaryIndexEntry(it->key(), it->value());
      if (!s.ok()) {
        VerificationAbort(thread->shared, "Cannot decode secondary index entry",
                          s);
        return;
      }
      // After decoding secondary index entry, we know a and c. Crc is verified
      // in decoding phase.
      //
      // Form a primary key and search in the primary index.
      std::string pk = MyRocksRecord::EncodePrimaryKey(record.a_value());
      std::string value;
      s = db_->Get(ReadOptions(), pk, &value);
      if (!s.ok()) {
        std::ostringstream oss;
        oss << "Error searching pk " << Slice(pk).ToString(/*hex=*/true) << ". "
            << s.ToString();
        VerificationAbort(thread->shared, oss.str(), s);
        return;
      }
      auto result = MyRocksRecord::DecodePrimaryIndexValue(value);
      s = std::get<0>(result);
      if (!s.ok()) {
        std::ostringstream oss;
        oss << "Error decoding primary index value "
            << Slice(value).ToString(/*hex=*/true) << ". " << s.ToString();
        VerificationAbort(thread->shared, oss.str(), s);
      }
      uint32_t c_in_primary = std::get<2>(result);
      if (c_in_primary != record.c_value()) {
        std::ostringstream oss;
        oss << "Pk/sk mismatch. pk: (c=" << c_in_primary
            << "), sk: (c=" << record.c_value() << ")";
        VerificationAbort(thread->shared, oss.str(), s);
      }
    }
  }

  if (secondary_index_entries_count != primary_index_entries_count) {
    std::ostringstream oss;
    oss << "Pk/sk mismatch: primary index has " << primary_index_entries_count
        << " entries. Secondary index has " << secondary_index_entries_count
        << " entries.";
    VerificationAbort(thread->shared, oss.str(), Status::OK());
  }
}

uint32_t MyRocksStyleTxnsStressTest::GeneratePrimaryIndexKeyForPointLookup(
    ThreadState* thread) const {
  Random& rand = thread->rand;
  uint32_t next_a = next_a_.load();
  uint32_t min_a = min_a_.load();
  assert(next_a > min_a);
  uint32_t rand_key = (rand.Next() % (next_a - min_a)) + min_a;
  return rand_key;
}

void MyRocksStyleTxnsStressTest::PreloadDb(SharedState* shared, size_t num_c) {
  WriteOptions wopts;
  wopts.disableWAL = true;
  wopts.sync = false;
  Random rnd(shared->GetSeed());
  for (size_t i = 0; i < num_c; ++i) {
    MyRocksRecord record(/*a=*/kCARatio * i, /*b=*/rnd.Next(), /*c=*/i);
    WriteBatch wb;
    const auto primary_index_entry = record.EncodePrimaryIndexEntry();
    Status s = wb.Put(primary_index_entry.first, primary_index_entry.second);
    assert(s.ok());
    const auto secondary_index_entry = record.EncodeSecondaryIndexEntry();
    s = wb.Put(secondary_index_entry.first, secondary_index_entry.second);
    assert(s.ok());
    s = txn_db_->Write(wopts, &wb);
    assert(s.ok());

    // TODO (yanqin): make the following check optional, especially when data
    // size is large.
    MyRocksRecord tmp_rec;
    tmp_rec.SetB(record.b_value());
    s = tmp_rec.DecodeSecondaryIndexEntry(secondary_index_entry.first,
                                          secondary_index_entry.second);
    assert(s.ok());
    assert(tmp_rec == record);
  }
  Status s = db_->Flush(FlushOptions());
  assert(s.ok());
  min_a_.store(0);
  next_a_.store(num_c * kCARatio);
}

StressTest* CreateMyRocksStyleTxnsStressTest() {
  return new MyRocksStyleTxnsStressTest();
}

void CheckAndSetOptionsForMyRocksStyleTxnStressTest() {
  if (FLAGS_test_batches_snapshots || FLAGS_test_cf_consistency) {
    fprintf(stderr,
            "-test_myrocks_txns is not compatible with "
            "-test_bathces_snapshots and -test_cf_consistency\n");
    exit(1);
  }
  if (!FLAGS_use_txn) {
    fprintf(stderr, "-use_txn must be true if -test_myrocks_txns\n");
    exit(1);
  }
  if (FLAGS_clear_column_family_one_in > 0) {
    fprintf(stderr,
            "-test_myrocks_txns is not compatible with clearing column "
            "families\n");
    exit(1);
  }
  if (FLAGS_column_families > 1) {
    // TODO (yanqin) support separating primary index and secondary index in
    // different column families.
    fprintf(stderr,
            "-test_myrocks_txns currently does not use more than one column "
            "family\n");
    exit(1);
  }
  if (FLAGS_writepercent > 0 || FLAGS_delpercent > 0 ||
      FLAGS_delrangepercent > 0) {
    fprintf(stderr,
            "-test_myrocks_txns requires that -writepercent, -delpercent and "
            "-delrangepercent be 0\n");
    exit(1);
  }
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
