//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#include "db/arena_wrapped_db_iter.h"
#include "db_stress_tool/db_stress_common.h"
#ifndef NDEBUG
#include "test_util/fault_injection_test_fs.h"
#endif  // NDEBUG
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

// This test is to validate correctness for read after write.
// The idea is to construct a workload that is totally mathematically
// determine, so that the state of the DB can be calculated based on
// one single counter.
// Crash recovery is not implemented yet. It can potentially be
// implemented by scanning the DB to recover the counter.
class ReadAfterWriteStressTest : public StressTest {
 public:
  ReadAfterWriteStressTest()
      : updated_key_(kNumKeys), pre_update_key_(kNumKeys) {}

  virtual ~ReadAfterWriteStressTest() {}

  void VerifyDb(ThreadState*) const override {}

  void MaybeClearOneColumnFamily(ThreadState*) override {}

  bool ShouldAcquireMutexOnKey() const override { return false; }

  Status TestGet(ThreadState* thread, const ReadOptions& ro,
                 const std::vector<int>&,
                 const std::vector<int64_t>&) override {
    auto cfh = column_families_[0];

    std::string key_str =
        ConvertToKey(static_cast<int64_t>(thread->rand.Uniform(kNumKeys)));
    PinnableSlice value;
    const Snapshot* snapshot = nullptr;
    uint64_t key_cnt_low_bound = updated_key_.load();
    uint64_t key_cnt_upper_bound;
    ReadOptions my_read_opts = ro;
    if (thread->rand.OneInOpt(FLAGS_acquire_snapshot_one_in)) {
      snapshot = db_->GetSnapshot();
      my_read_opts.snapshot = snapshot;
      key_cnt_upper_bound = pre_update_key_.load(std::memory_order_acquire);
    }
    Status s = db_->Get(my_read_opts, cfh, key_str, &value);
    if (snapshot == nullptr) {
      key_cnt_upper_bound = pre_update_key_.load(std::memory_order_acquire);
    }

    if (key_cnt_low_bound > kNumKeys) {
      key_cnt_low_bound -= kNumKeys;
    } else {
      key_cnt_low_bound = 0;
    }
    uint64_t int_from_k = KeyConvertToCnt(key_str);
    uint64_t cnt_from_v = 0;
    bool succeed = true;
    if (s.ok()) {
      cnt_from_v = SliceToInt(value);
      if (!VerifyCount(int_from_k, cnt_from_v, key_cnt_upper_bound,
                       key_cnt_low_bound)) {
        succeed = false;
      }
    } else if (s.IsNotFound()) {
      uint64_t min_cnt = kNumKeys * ((int_from_k ^ (int_from_k - 1)) + 1) / 2;
      if (min_cnt < key_cnt_low_bound) {
        succeed = false;
      }
    } else {
      fprintf(stderr, "get error for key %s: %s\n",
              Slice(key_str).ToString(true).c_str(), s.ToString().c_str());
      thread->stats.AddErrors(1);
      std::terminate();
    }

    if (!succeed) {
      uint64_t sn = 0;
      if (snapshot != nullptr) {
        sn = snapshot->GetSequenceNumber();
      }
      fprintf(stderr,
              "TestGet failure: key %s converted to %" PRIu64
              " lower bound %" PRIu64 " upper bound %" PRIu64 " SeqNum %" PRIu64
              "\n",
              Slice(key_str).ToString(true).c_str(), int_from_k,
              key_cnt_low_bound, key_cnt_upper_bound, sn);

      if (s.IsNotFound()) {
        fprintf(stderr, "Result not found\n");
      } else {
        fprintf(stderr, "value %" PRIu64 "\n", cnt_from_v);
      }
      thread->stats.AddErrors(1);
      std::terminate();
    }
    if (snapshot != nullptr) {
      db_->ReleaseSnapshot(snapshot);
    }
    return Status::OK();
  }

  std::vector<Status> TestMultiGet(
      ThreadState*, const ReadOptions&, const std::vector<int>&,
      const std::vector<int64_t>& rand_keys) override {
    // Not yet implemented.
    std::vector<Status> ret_status(rand_keys.size());
    return ret_status;
  }

  Status TestPrefixScan(ThreadState* thread, const ReadOptions& read_opts,
                        const std::vector<int>&,
                        const std::vector<int64_t>&) override {
    auto cfh = column_families_[0];

    ReadOptions my_read_opts = read_opts;
    if (thread->rand.OneIn(1)) {
      my_read_opts.total_order_seek = true;
    } else {
      my_read_opts.auto_prefix_mode = true;
    }

    std::string key_str =
        ConvertToKey(static_cast<int64_t>(thread->rand.Uniform(kNumKeys)));
    uint32_t scan_cnt = thread->rand.Skewed(10);
    const Snapshot* snapshot = nullptr;
    uint64_t key_cnt_low_bound = updated_key_.load();
    uint64_t key_cnt_upper_bound;
    if (thread->rand.OneInOpt(FLAGS_acquire_snapshot_one_in)) {
      snapshot = db_->GetSnapshot();
      my_read_opts.snapshot = snapshot;
      key_cnt_upper_bound = pre_update_key_.load(std::memory_order_acquire);
    }
    Iterator* iter = db_->NewIterator(my_read_opts, cfh);
    if (snapshot == nullptr) {
      key_cnt_upper_bound = pre_update_key_.load(std::memory_order_acquire);
    }

    if (key_cnt_low_bound > kNumKeys) {
      key_cnt_low_bound -= kNumKeys;
    } else {
      key_cnt_low_bound = 0;
    }
    uint32_t count = 0;
    for (iter->Seek(key_str); iter->Valid() && count++ < scan_cnt;
         iter->Next()) {
      uint64_t cnt_from_v = SliceToInt(iter->value());
      uint64_t int_from_k = KeyConvertToCnt(iter->key());

      if (!VerifyCount(int_from_k, cnt_from_v, key_cnt_upper_bound,
                       key_cnt_low_bound)) {
        uint64_t sn;
        if (snapshot == nullptr) {
          ArenaWrappedDBIter* awdi =
              static_cast_with_check<ArenaWrappedDBIter>(iter);
          sn = awdi->TEST_get_seqnum();
        } else {
          sn = snapshot->GetSequenceNumber();
        }
        fprintf(stderr,
                "TestPrefixScan failure: key %s converted to %" PRIu64
                " count %" PRIu64 " lower bound %" PRIu64
                " upper bound %" PRIu64 " seek key: %s scanned keys: %" PRIu32
                " SeqNum %" PRIu64 "\n",
                iter->key().ToString(true).c_str(), int_from_k, cnt_from_v,
                key_cnt_low_bound, key_cnt_upper_bound,
                Slice(key_str).ToString(true).c_str(), count, sn);
        thread->stats.AddErrors(1);
        std::terminate();
        break;
      }
    }

    Status s = iter->status();
    if (iter->status().ok()) {
      thread->stats.AddPrefixes(1, count);
    } else {
      fprintf(stderr, "TestPrefixScan error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    }
    delete iter;

    if (snapshot != nullptr) {
      db_->ReleaseSnapshot(snapshot);
    }
    return s;
  }

  // The update count is translated to key by
  // (count * kMul1) % kNumKeys
  // (count % kNumKeys) can be recovered using (key * kMul2 ) % 582340
  // This is to make key updating randomly across the key space while
  // still keeping the order deterministic.
  static constexpr uint64_t kMul1 = 881;
  static constexpr uint64_t kMul2 = 8887;
  static constexpr uint64_t kNumKeys = kMul1 * kMul2 - 1;

  // Deterministially skip some key counts. This is to make sure in read
  // path some keys need to go deeper levels to be found.
  // if key_count % kNumKeys is multiplication of 2^n, the count only
  // inserted if key_count / kNumKeys is multipliciation of 2^n too.
  // This is to make sure that when read happens, some keys are
  // disportionally old.
  // This algorithm is arbitrary and can change if needed.
  bool ShouldInsertCount(uint64_t key_count) {
    uint64_t mod = key_count % kNumKeys;
    uint64_t div = key_count / kNumKeys;
    uint64_t m1 = (mod ^ (mod - 1));
    uint64_t m2 = (div ^ (div - 1));
    return ((m1 ^ m2) & m1) == 0;
  }

  // True if expected, false if not
  bool VerifyCount(uint64_t mod_from_key, uint64_t cnt_from_value,
                   uint64_t key_cnt_upper_bound, uint64_t key_cnt_low_bound) {
    if (!ShouldInsertCount(cnt_from_value)) {
      return false;
    } else if (mod_from_key != cnt_from_value % kNumKeys) {
      return false;
    } else if (cnt_from_value > key_cnt_upper_bound) {
      return false;
    }
    uint64_t m1 = mod_from_key ^ (mod_from_key - 1);
    uint64_t real_lower_bound =
        ((key_cnt_low_bound / kNumKeys) & ~m1) + (key_cnt_low_bound % kNumKeys);
    if (cnt_from_value < real_lower_bound) {
      return false;
    }
    return true;
  }

  std::string ConvertToKey(uint64_t key_count) {
    uint64_t k = ((key_count % kNumKeys) * kMul1) % kNumKeys;
    std::string ret_str;
    PutFixed64(&ret_str, k);
    return ret_str;
  }

  uint64_t SliceToInt(const Slice& slice) {
    uint64_t ret = 0;
    Slice copy_slice = slice;
    GetFixed64(&copy_slice, &ret);
    return ret;
  }

  uint64_t KeyConvertToCnt(const Slice& k) {
    uint64_t mod = SliceToInt(k);
    return (mod * kMul2) % kNumKeys;
  }

  Status TestPut(ThreadState* thread, WriteOptions& write_opts,
                 const ReadOptions&, const std::vector<int>&,
                 const std::vector<int64_t>&, char (&)[100],
                 std::unique_ptr<MutexLock>&) override {
    std::lock_guard<std::mutex> lock(write_lock_);
    ColumnFamilyHandle* cfh = column_families_[0];
    uint32_t num_entries = thread->rand.Skewed(8);
    uint64_t my_key_cnt = updated_key_.load(std::memory_order_relaxed);
    WriteBatch batch;
    for (uint32_t i = 0; i < num_entries;) {
      if (ShouldInsertCount(my_key_cnt)) {
        std::string key_str = ConvertToKey(my_key_cnt);
        std::string value_str;
        PutFixed64(&value_str, static_cast<uint64_t>(my_key_cnt));
        batch.Put(cfh, key_str, value_str);
      }
      my_key_cnt++;
      i++;
    }

    pre_update_key_.store(my_key_cnt, std::memory_order_release);
    Status s = db_->Write(write_opts, &batch);
    updated_key_.store(my_key_cnt, std::memory_order_release);
    if (!s.ok()) {
      fprintf(stderr, "write error: %s\n", s.ToString().c_str());
      std::terminate();
    }
    thread->stats.AddBytesForWrites(1, batch.GetDataSize());
    return s;
  }

  Status TestDelete(ThreadState*, WriteOptions&, const std::vector<int>&,
                    const std::vector<int64_t>&,
                    std::unique_ptr<MutexLock>&) override {
    return Status::OK();
  }

  Status TestDeleteRange(ThreadState*, WriteOptions&, const std::vector<int>&,
                         const std::vector<int64_t>&,
                         std::unique_ptr<MutexLock>&) override {
    return Status::OK();
  }

#ifdef ROCKSDB_LITE
  void TestIngestExternalFile(
      ThreadState* /* thread */,
      const std::vector<int>& /* rand_column_families */,
      const std::vector<int64_t>& /* rand_keys */,
      std::unique_ptr<MutexLock>& /* lock */) override {
    assert(false);
    fprintf(stderr,
            "RocksDB lite does not support "
            "TestIngestExternalFile\n");
    std::terminate();
  }
#else
  void TestIngestExternalFile(ThreadState*, const std::vector<int>&,
                              const std::vector<int64_t>&,
                              std::unique_ptr<MutexLock>&) override {}
#endif  // ROCKSDB_LITE
 private:
  std::atomic<int64_t> updated_key_;
  std::atomic<int64_t> pre_update_key_;
  std::mutex write_lock_;  // make write sequential.
};

StressTest* CreateReadAfterWriteStressTest() {
  return new ReadAfterWriteStressTest();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
