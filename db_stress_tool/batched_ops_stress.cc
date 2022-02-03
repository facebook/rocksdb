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
class BatchedOpsStressTest : public StressTest {
 public:
  BatchedOpsStressTest() {}
  virtual ~BatchedOpsStressTest() {}

  // Given a key K and value V, this puts ("0"+K, "0"+V), ("1"+K, "1"+V), ...
  // ("9"+K, "9"+V) in DB atomically i.e in a single batch.
  // Also refer BatchedOpsStressTest::TestGet
  Status TestPut(ThreadState* thread, WriteOptions& write_opts,
                 const ReadOptions& /* read_opts */,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys, char (&value)[100],
                 std::unique_ptr<MutexLock>& /* lock */) override {
    uint32_t value_base =
        thread->rand.Next() % thread->shared->UNKNOWN_SENTINEL;
    size_t sz = GenerateValue(value_base, value, sizeof(value));
    Slice v(value, sz);
    std::string keys[10] = {"9", "8", "7", "6", "5", "4", "3", "2", "1", "0"};
    std::string values[10] = {"9", "8", "7", "6", "5", "4", "3", "2", "1", "0"};
    Slice value_slices[10];
    WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */, 0 /* ts_sz */,
                     FLAGS_batch_protection_bytes_per_key);
    Status s;
    auto cfh = column_families_[rand_column_families[0]];
    std::string key_str = Key(rand_keys[0]);
    for (int i = 0; i < 10; i++) {
      keys[i] += key_str;
      values[i] += v.ToString();
      value_slices[i] = values[i];
      if (FLAGS_use_merge) {
        batch.Merge(cfh, keys[i], value_slices[i]);
      } else {
        batch.Put(cfh, keys[i], value_slices[i]);
      }
    }

    s = db_->Write(write_opts, &batch);
    if (!s.ok()) {
      fprintf(stderr, "multiput error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    } else {
      // we did 10 writes each of size sz + 1
      thread->stats.AddBytesForWrites(10, (sz + 1) * 10);
    }

    return s;
  }

  // Given a key K, this deletes ("0"+K), ("1"+K),... ("9"+K)
  // in DB atomically i.e in a single batch. Also refer MultiGet.
  Status TestDelete(ThreadState* thread, WriteOptions& writeoptions,
                    const std::vector<int>& rand_column_families,
                    const std::vector<int64_t>& rand_keys,
                    std::unique_ptr<MutexLock>& /* lock */) override {
    std::string keys[10] = {"9", "7", "5", "3", "1", "8", "6", "4", "2", "0"};

    WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */, 0 /* ts_sz */,
                     FLAGS_batch_protection_bytes_per_key);
    Status s;
    auto cfh = column_families_[rand_column_families[0]];
    std::string key_str = Key(rand_keys[0]);
    for (int i = 0; i < 10; i++) {
      keys[i] += key_str;
      batch.Delete(cfh, keys[i]);
    }

    s = db_->Write(writeoptions, &batch);
    if (!s.ok()) {
      fprintf(stderr, "multidelete error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    } else {
      thread->stats.AddDeletes(10);
    }

    return s;
  }

  Status TestDeleteRange(ThreadState* /* thread */,
                         WriteOptions& /* write_opts */,
                         const std::vector<int>& /* rand_column_families */,
                         const std::vector<int64_t>& /* rand_keys */,
                         std::unique_ptr<MutexLock>& /* lock */) override {
    assert(false);
    return Status::NotSupported(
        "BatchedOpsStressTest does not support "
        "TestDeleteRange");
  }

  void TestIngestExternalFile(
      ThreadState* /* thread */,
      const std::vector<int>& /* rand_column_families */,
      const std::vector<int64_t>& /* rand_keys */,
      std::unique_ptr<MutexLock>& /* lock */) override {
    assert(false);
    fprintf(stderr,
            "BatchedOpsStressTest does not support "
            "TestIngestExternalFile\n");
    std::terminate();
  }

  // Given a key K, this gets values for "0"+K, "1"+K,..."9"+K
  // in the same snapshot, and verifies that all the values are of the form
  // "0"+V, "1"+V,..."9"+V.
  // ASSUMES that BatchedOpsStressTest::TestPut was used to put (K, V) into
  // the DB.
  Status TestGet(ThreadState* thread, const ReadOptions& readoptions,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys) override {
    std::string keys[10] = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
    Slice key_slices[10];
    std::string values[10];
    ReadOptions readoptionscopy = readoptions;
    readoptionscopy.snapshot = db_->GetSnapshot();
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    auto cfh = column_families_[rand_column_families[0]];
    std::string from_db;
    Status s;
    for (int i = 0; i < 10; i++) {
      keys[i] += key.ToString();
      key_slices[i] = keys[i];
      s = db_->Get(readoptionscopy, cfh, key_slices[i], &from_db);
      if (!s.ok() && !s.IsNotFound()) {
        fprintf(stderr, "get error: %s\n", s.ToString().c_str());
        values[i] = "";
        thread->stats.AddErrors(1);
        // we continue after error rather than exiting so that we can
        // find more errors if any
      } else if (s.IsNotFound()) {
        values[i] = "";
        thread->stats.AddGets(1, 0);
      } else {
        values[i] = from_db;

        char expected_prefix = (keys[i])[0];
        char actual_prefix = (values[i])[0];
        if (actual_prefix != expected_prefix) {
          fprintf(stderr, "error expected prefix = %c actual = %c\n",
                  expected_prefix, actual_prefix);
        }
        (values[i])[0] = ' ';  // blank out the differing character
        thread->stats.AddGets(1, 1);
      }
    }
    db_->ReleaseSnapshot(readoptionscopy.snapshot);

    // Now that we retrieved all values, check that they all match
    for (int i = 1; i < 10; i++) {
      if (values[i] != values[0]) {
        fprintf(stderr, "error : inconsistent values for key %s: %s, %s\n",
                key.ToString(true).c_str(), StringToHex(values[0]).c_str(),
                StringToHex(values[i]).c_str());
        // we continue after error rather than exiting so that we can
        // find more errors if any
      }
    }

    return s;
  }

  std::vector<Status> TestMultiGet(
      ThreadState* thread, const ReadOptions& readoptions,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) override {
    size_t num_keys = rand_keys.size();
    std::vector<Status> ret_status(num_keys);
    std::array<std::string, 10> keys = {{"0", "1", "2", "3", "4",
                                         "5", "6", "7", "8", "9"}};
    size_t num_prefixes = keys.size();
    for (size_t rand_key = 0; rand_key < num_keys; ++rand_key) {
      std::vector<Slice> key_slices;
      std::vector<PinnableSlice> values(num_prefixes);
      std::vector<Status> statuses(num_prefixes);
      ReadOptions readoptionscopy = readoptions;
      readoptionscopy.snapshot = db_->GetSnapshot();
      std::vector<std::string> key_str;
      key_str.reserve(num_prefixes);
      key_slices.reserve(num_prefixes);
      std::string from_db;
      ColumnFamilyHandle* cfh = column_families_[rand_column_families[0]];

      for (size_t key = 0; key < num_prefixes; ++key) {
        key_str.emplace_back(keys[key] + Key(rand_keys[rand_key]));
        key_slices.emplace_back(key_str.back());
      }
      db_->MultiGet(readoptionscopy, cfh, num_prefixes, key_slices.data(),
                    values.data(), statuses.data());
      for (size_t i = 0; i < num_prefixes; i++) {
        Status s = statuses[i];
        if (!s.ok() && !s.IsNotFound()) {
          fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          thread->stats.AddErrors(1);
          ret_status[rand_key] = s;
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (s.IsNotFound()) {
          thread->stats.AddGets(1, 0);
          ret_status[rand_key] = s;
        } else {
          char expected_prefix = (keys[i])[0];
          char actual_prefix = (values[i])[0];
          if (actual_prefix != expected_prefix) {
            fprintf(stderr, "error expected prefix = %c actual = %c\n",
                    expected_prefix, actual_prefix);
          }
          std::string str;
          str.assign(values[i].data(), values[i].size());
          values[i].Reset();
          str[0] = ' ';  // blank out the differing character
          values[i].PinSelf(str);
          thread->stats.AddGets(1, 1);
        }
      }
      db_->ReleaseSnapshot(readoptionscopy.snapshot);

      // Now that we retrieved all values, check that they all match
      for (size_t i = 1; i < num_prefixes; i++) {
        if (values[i] != values[0]) {
          fprintf(stderr, "error : inconsistent values for key %s: %s, %s\n",
                  key_str[i].c_str(), StringToHex(values[0].ToString()).c_str(),
                  StringToHex(values[i].ToString()).c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        }
      }
    }

    return ret_status;
  }

  // Given a key, this does prefix scans for "0"+P, "1"+P,..."9"+P
  // in the same snapshot where P is the first FLAGS_prefix_size - 1 bytes
  // of the key. Each of these 10 scans returns a series of values;
  // each series should be the same length, and it is verified for each
  // index i that all the i'th values are of the form "0"+V, "1"+V,..."9"+V.
  // ASSUMES that MultiPut was used to put (K, V)
  Status TestPrefixScan(ThreadState* thread, const ReadOptions& readoptions,
                        const std::vector<int>& rand_column_families,
                        const std::vector<int64_t>& rand_keys) override {
    size_t prefix_to_use =
        (FLAGS_prefix_size < 0) ? 7 : static_cast<size_t>(FLAGS_prefix_size);
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    auto cfh = column_families_[rand_column_families[0]];
    std::string prefixes[10] = {"0", "1", "2", "3", "4",
                                "5", "6", "7", "8", "9"};
    Slice prefix_slices[10];
    ReadOptions readoptionscopy[10];
    const Snapshot* snapshot = db_->GetSnapshot();
    Iterator* iters[10];
    std::string upper_bounds[10];
    Slice ub_slices[10];
    Status s = Status::OK();
    for (int i = 0; i < 10; i++) {
      prefixes[i] += key.ToString();
      prefixes[i].resize(prefix_to_use);
      prefix_slices[i] = Slice(prefixes[i]);
      readoptionscopy[i] = readoptions;
      readoptionscopy[i].snapshot = snapshot;
      if (thread->rand.OneIn(2) &&
          GetNextPrefix(prefix_slices[i], &(upper_bounds[i]))) {
        // For half of the time, set the upper bound to the next prefix
        ub_slices[i] = Slice(upper_bounds[i]);
        readoptionscopy[i].iterate_upper_bound = &(ub_slices[i]);
      }
      iters[i] = db_->NewIterator(readoptionscopy[i], cfh);
      iters[i]->Seek(prefix_slices[i]);
    }

    long count = 0;
    while (iters[0]->Valid() && iters[0]->key().starts_with(prefix_slices[0])) {
      count++;
      std::string values[10];
      // get list of all values for this iteration
      for (int i = 0; i < 10; i++) {
        // no iterator should finish before the first one
        assert(iters[i]->Valid() &&
               iters[i]->key().starts_with(prefix_slices[i]));
        values[i] = iters[i]->value().ToString();

        char expected_first = (prefixes[i])[0];
        char actual_first = (values[i])[0];

        if (actual_first != expected_first) {
          fprintf(stderr, "error expected first = %c actual = %c\n",
                  expected_first, actual_first);
        }
        (values[i])[0] = ' ';  // blank out the differing character
      }
      // make sure all values are equivalent
      for (int i = 0; i < 10; i++) {
        if (values[i] != values[0]) {
          fprintf(stderr,
                  "error : %d, inconsistent values for prefix %s: %s, %s\n", i,
                  prefixes[i].c_str(), StringToHex(values[0]).c_str(),
                  StringToHex(values[i]).c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        }
        iters[i]->Next();
      }
    }

    // cleanup iterators and snapshot
    for (int i = 0; i < 10; i++) {
      // if the first iterator finished, they should have all finished
      assert(!iters[i]->Valid() ||
             !iters[i]->key().starts_with(prefix_slices[i]));
      assert(iters[i]->status().ok());
      delete iters[i];
    }
    db_->ReleaseSnapshot(snapshot);

    if (s.ok()) {
      thread->stats.AddPrefixes(1, count);
    } else {
      fprintf(stderr, "TestPrefixScan error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    }

    return s;
  }

  void VerifyDb(ThreadState* /* thread */) const override {}
};

StressTest* CreateBatchedOpsStressTest() { return new BatchedOpsStressTest(); }

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
