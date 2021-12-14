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

namespace ROCKSDB_NAMESPACE {

// This file defines MultiOpsTxnsStress so that we can stress test RocksDB
// transactions on a simple, emulated relational table.
//
// The record format is similar to the example found at
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format.
//
// The table is created by
// ```
// create table t1 (
//   a int primary key,
//   b int,
//   c int,
//   key(c),
//   )
// ```
//
// (For simplicity, we use uint32_t for int here.)
//
// For this table, there is a primary index using `a`, as well as a secondary
// index using `c` and `a`.
//
// Primary key format:
// | index id | M(a) |
// Primary index value:
// | b | c |
// M(a) represents the big-endian format of a.
//
// Secondary key format:
// | index id | M(c) | M(a) |
// Secondary index value:
// | crc32 |
// Similarly to M(a), M(c) is the big-endian format of c.
//
// The in-memory representation of a record is defined in class
// MultiOpsTxnsStress:Record that includes a number of helper methods to
// encode/decode primary index keys, primary index values, secondary index keys,
// secondary index values, etc.
//
// Sometimes primary index and secondary index reside on different column
// families, but sometimes they colocate in the same column family. Current
// implementation puts them in the same (default) column family, and this is
// subject to future change if we find it interesting to test the other case.
//
// Class MultiOpsTxnsStressTest has the following transactions for testing.
//
// 1. Primary key update
// UPDATE t1 SET a = 3 WHERE a = 2;
// ```
// tx->GetForUpdate(primary key a=2)
// tx->GetForUpdate(primary key a=3)
// tx->Delete(primary key a=2)
// tx->Put(primary key a=3, value)
// tx->batch->SingleDelete(secondary key a=2)
// tx->batch->Put(secondary key a=3, value)
// tx->Prepare()
// Tx->Commit()
// ```
//
// 2. Secondary key update
// UPDATE t1 SET c = 3 WHERE c = 2;
// ```
// iter->Seek(secondary key)
// // Get corresponding primary key value(s) from iterator
// tx->GetForUpdate(primary key)
// tx->Put(primary key, value c=3)
// tx->batch->SingleDelete(secondary key c=2)
// tx->batch->Put(secondary key c=3)
// tx->Prepare()
// tx->Commit()
// ```
//
// 3. Primary index value update
// UPDATE t1 SET b = b + 1 WHERE a = 2;
// ```
// tx->GetForUpdate(primary key a=2)
// tx->Put(primary key a=2, value b=b+1)
// tx->Prepare()
// tx->Commit()
// ```
//
// 4. Point lookup
// SELECT * FROM t1 WHERE a = 3;
// ```
// tx->Get(primary key a=3)
// tx->Commit()
// ```
//
// 5. Range scan
// SELECT * FROM t1 WHERE c = 2;
// ```
// it = tx->GetIterator()
// it->Seek(secondary key c=2)
// tx->Commit()
// ```

class MultiOpsTxnsStressTest : public StressTest {
 public:
  class Record {
   public:
    static constexpr uint32_t kPrimaryIndexId = 1;
    static constexpr uint32_t kSecondaryIndexId = 2;

    static constexpr size_t kPrimaryIndexEntrySize = 8 + 8;
    static constexpr size_t kSecondaryIndexEntrySize = 12 + 4;

    static_assert(kPrimaryIndexId < kSecondaryIndexId,
                  "kPrimaryIndexId must be smaller than kSecondaryIndexId");

    static_assert(sizeof(kPrimaryIndexId) == sizeof(uint32_t),
                  "kPrimaryIndexId must be 4 bytes");
    static_assert(sizeof(kSecondaryIndexId) == sizeof(uint32_t),
                  "kSecondaryIndexId must be 4 bytes");

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

    Record() = default;
    Record(uint32_t _a, uint32_t _b, uint32_t _c) : a_(_a), b_(_b), c_(_c) {}

    bool operator==(const Record& other) const {
      return a_ == other.a_ && b_ == other.b_ && c_ == other.c_;
    }

    bool operator!=(const Record& other) const { return !(*this == other); }

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
    friend class InvariantChecker;

    uint32_t a_{0};
    uint32_t b_{0};
    uint32_t c_{0};
  };

  MultiOpsTxnsStressTest() {}

  ~MultiOpsTxnsStressTest() override {}

  void FinishInitDb(SharedState*) override;

  void ReopenAndPreloadDb(SharedState* shared);

  bool IsStateTracked() const override { return false; }

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

#ifndef ROCKSDB_LITE
  Status TestApproximateSize(ThreadState* thread, uint64_t iteration,
                             const std::vector<int>& rand_column_families,
                             const std::vector<int64_t>& rand_keys) override;
#endif  // !ROCKSDB_LITE

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
  uint32_t ChooseA(ThreadState* thread);

  uint32_t GenerateNextA();

 private:
  void PreloadDb(SharedState* shared, size_t num_c);

  // TODO (yanqin) encapsulate the selection of keys a separate class.
  std::atomic<uint32_t> next_a_{0};
};

class InvariantChecker {
 public:
  static_assert(sizeof(MultiOpsTxnsStressTest::Record().a_) == sizeof(uint32_t),
                "MultiOpsTxnsStressTest::Record::a_ must be 4 bytes");
  static_assert(sizeof(MultiOpsTxnsStressTest::Record().b_) == sizeof(uint32_t),
                "MultiOpsTxnsStressTest::Record::b_ must be 4 bytes");
  static_assert(sizeof(MultiOpsTxnsStressTest::Record().c_) == sizeof(uint32_t),
                "MultiOpsTxnsStressTest::Record::c_ must be 4 bytes");
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
