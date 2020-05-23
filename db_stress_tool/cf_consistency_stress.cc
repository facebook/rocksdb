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

namespace rocksdb {
class CfConsistencyStressTest : public StressTest {
 public:
  CfConsistencyStressTest() : batch_id_(0) {}

  ~CfConsistencyStressTest() override {}

  Status TestPut(ThreadState* thread, WriteOptions& write_opts,
                 const ReadOptions& /* read_opts */,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys, char (&value)[100],
                 std::unique_ptr<MutexLock>& /* lock */) override {
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    uint64_t value_base = batch_id_.fetch_add(1);
    size_t sz =
        GenerateValue(static_cast<uint32_t>(value_base), value, sizeof(value));
    Slice v(value, sz);
    WriteBatch batch;
    for (auto cf : rand_column_families) {
      ColumnFamilyHandle* cfh = column_families_[cf];
      if (FLAGS_use_merge) {
        batch.Merge(cfh, key, v);
      } else { /* !FLAGS_use_merge */
        batch.Put(cfh, key, v);
      }
    }
    Status s = db_->Write(write_opts, &batch);
    if (!s.ok()) {
      fprintf(stderr, "multi put or merge error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    } else {
      auto num = static_cast<long>(rand_column_families.size());
      thread->stats.AddBytesForWrites(num, (sz + 1) * num);
    }

    return s;
  }

  Status TestDelete(ThreadState* thread, WriteOptions& write_opts,
                    const std::vector<int>& rand_column_families,
                    const std::vector<int64_t>& rand_keys,
                    std::unique_ptr<MutexLock>& /* lock */) override {
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    WriteBatch batch;
    for (auto cf : rand_column_families) {
      ColumnFamilyHandle* cfh = column_families_[cf];
      batch.Delete(cfh, key);
    }
    Status s = db_->Write(write_opts, &batch);
    if (!s.ok()) {
      fprintf(stderr, "multidel error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    } else {
      thread->stats.AddDeletes(static_cast<long>(rand_column_families.size()));
    }
    return s;
  }

  Status TestDeleteRange(ThreadState* thread, WriteOptions& write_opts,
                         const std::vector<int>& rand_column_families,
                         const std::vector<int64_t>& rand_keys,
                         std::unique_ptr<MutexLock>& /* lock */) override {
    int64_t rand_key = rand_keys[0];
    auto shared = thread->shared;
    int64_t max_key = shared->GetMaxKey();
    if (rand_key > max_key - FLAGS_range_deletion_width) {
      rand_key =
          thread->rand.Next() % (max_key - FLAGS_range_deletion_width + 1);
    }
    std::string key_str = Key(rand_key);
    Slice key = key_str;
    std::string end_key_str = Key(rand_key + FLAGS_range_deletion_width);
    Slice end_key = end_key_str;
    WriteBatch batch;
    for (auto cf : rand_column_families) {
      ColumnFamilyHandle* cfh = column_families_[rand_column_families[cf]];
      batch.DeleteRange(cfh, key, end_key);
    }
    Status s = db_->Write(write_opts, &batch);
    if (!s.ok()) {
      fprintf(stderr, "multi del range error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    } else {
      thread->stats.AddRangeDeletions(
          static_cast<long>(rand_column_families.size()));
    }
    return s;
  }

  void TestIngestExternalFile(
      ThreadState* /* thread */,
      const std::vector<int>& /* rand_column_families */,
      const std::vector<int64_t>& /* rand_keys */,
      std::unique_ptr<MutexLock>& /* lock */) override {
    assert(false);
    fprintf(stderr,
            "CfConsistencyStressTest does not support TestIngestExternalFile "
            "because it's not possible to verify the result\n");
    std::terminate();
  }

  Status TestGet(ThreadState* thread, const ReadOptions& readoptions,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys) override {
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    Status s;
    bool is_consistent = true;

    if (thread->rand.OneIn(2)) {
      // 1/2 chance, does a random read from random CF
      auto cfh =
          column_families_[rand_column_families[thread->rand.Next() %
                                                rand_column_families.size()]];
      std::string from_db;
      s = db_->Get(readoptions, cfh, key, &from_db);
    } else {
      // 1/2 chance, comparing one key is the same across all CFs
      const Snapshot* snapshot = db_->GetSnapshot();
      ReadOptions readoptionscopy = readoptions;
      readoptionscopy.snapshot = snapshot;

      std::string value0;
      s = db_->Get(readoptionscopy, column_families_[rand_column_families[0]],
                   key, &value0);
      if (s.ok() || s.IsNotFound()) {
        bool found = s.ok();
        for (size_t i = 1; i < rand_column_families.size(); i++) {
          std::string value1;
          s = db_->Get(readoptionscopy,
                       column_families_[rand_column_families[i]], key, &value1);
          if (!s.ok() && !s.IsNotFound()) {
            break;
          }
          if (!found && s.ok()) {
            fprintf(stderr, "Get() return different results with key %s\n",
                    Slice(key_str).ToString(true).c_str());
            fprintf(stderr, "CF %s is not found\n",
                    column_family_names_[0].c_str());
            fprintf(stderr, "CF %s returns value %s\n",
                    column_family_names_[i].c_str(),
                    Slice(value1).ToString(true).c_str());
            is_consistent = false;
          } else if (found && s.IsNotFound()) {
            fprintf(stderr, "Get() return different results with key %s\n",
                    Slice(key_str).ToString(true).c_str());
            fprintf(stderr, "CF %s returns value %s\n",
                    column_family_names_[0].c_str(),
                    Slice(value0).ToString(true).c_str());
            fprintf(stderr, "CF %s is not found\n",
                    column_family_names_[i].c_str());
            is_consistent = false;
          } else if (s.ok() && value0 != value1) {
            fprintf(stderr, "Get() return different results with key %s\n",
                    Slice(key_str).ToString(true).c_str());
            fprintf(stderr, "CF %s returns value %s\n",
                    column_family_names_[0].c_str(),
                    Slice(value0).ToString(true).c_str());
            fprintf(stderr, "CF %s returns value %s\n",
                    column_family_names_[i].c_str(),
                    Slice(value1).ToString(true).c_str());
            is_consistent = false;
          }
          if (!is_consistent) {
            break;
          }
        }
      }

      db_->ReleaseSnapshot(snapshot);
    }
    if (!is_consistent) {
      fprintf(stderr, "TestGet error: is_consistent is false\n");
      thread->stats.AddErrors(1);
      // Fail fast to preserve the DB state.
      thread->shared->SetVerificationFailure();
    } else if (s.ok()) {
      thread->stats.AddGets(1, 1);
    } else if (s.IsNotFound()) {
      thread->stats.AddGets(1, 0);
    } else {
      fprintf(stderr, "TestGet error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    }
    return s;
  }

  std::vector<Status> TestMultiGet(
      ThreadState* thread, const ReadOptions& read_opts,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) override {
    size_t num_keys = rand_keys.size();
    std::vector<std::string> key_str;
    std::vector<Slice> keys;
    keys.reserve(num_keys);
    key_str.reserve(num_keys);
    std::vector<PinnableSlice> values(num_keys);
    std::vector<Status> statuses(num_keys);
    ColumnFamilyHandle* cfh = column_families_[rand_column_families[0]];

    for (size_t i = 0; i < num_keys; ++i) {
      key_str.emplace_back(Key(rand_keys[i]));
      keys.emplace_back(key_str.back());
    }
    db_->MultiGet(read_opts, cfh, num_keys, keys.data(), values.data(),
                  statuses.data());
    for (auto s : statuses) {
      if (s.ok()) {
        // found case
        thread->stats.AddGets(1, 1);
      } else if (s.IsNotFound()) {
        // not found case
        thread->stats.AddGets(1, 0);
      } else {
        // errors case
        fprintf(stderr, "MultiGet error: %s\n", s.ToString().c_str());
        thread->stats.AddErrors(1);
      }
    }
    return statuses;
  }

  Status TestPrefixScan(ThreadState* thread, const ReadOptions& readoptions,
                        const std::vector<int>& rand_column_families,
                        const std::vector<int64_t>& rand_keys) override {
    size_t prefix_to_use =
        (FLAGS_prefix_size < 0) ? 7 : static_cast<size_t>(FLAGS_prefix_size);

    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    Slice prefix = Slice(key.data(), prefix_to_use);

    std::string upper_bound;
    Slice ub_slice;
    ReadOptions ro_copy = readoptions;
    // Get the next prefix first and then see if we want to set upper bound.
    // We'll use the next prefix in an assertion later on
    if (GetNextPrefix(prefix, &upper_bound) && thread->rand.OneIn(2)) {
      ub_slice = Slice(upper_bound);
      ro_copy.iterate_upper_bound = &ub_slice;
    }
    auto cfh =
        column_families_[rand_column_families[thread->rand.Next() %
                                              rand_column_families.size()]];
    Iterator* iter = db_->NewIterator(ro_copy, cfh);
    unsigned long count = 0;
    for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix);
         iter->Next()) {
      ++count;
    }
    assert(prefix_to_use == 0 ||
           count <= GetPrefixKeyCount(prefix.ToString(), upper_bound));
    Status s = iter->status();
    if (s.ok()) {
      thread->stats.AddPrefixes(1, count);
    } else {
      fprintf(stderr, "TestPrefixScan error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    }
    delete iter;
    return s;
  }

  ColumnFamilyHandle* GetControlCfh(ThreadState* thread,
                                    int /*column_family_id*/
                                    ) override {
    // All column families should contain the same data. Randomly pick one.
    return column_families_[thread->rand.Next() % column_families_.size()];
  }

#ifdef ROCKSDB_LITE
  Status TestCheckpoint(ThreadState* /* thread */,
                        const std::vector<int>& /* rand_column_families */,
                        const std::vector<int64_t>& /* rand_keys */) override {
    assert(false);
    fprintf(stderr,
            "RocksDB lite does not support "
            "TestCheckpoint\n");
    std::terminate();
  }
#else
  Status TestCheckpoint(ThreadState* thread,
                        const std::vector<int>& /* rand_column_families */,
                        const std::vector<int64_t>& /* rand_keys */) override {
    std::string checkpoint_dir =
        FLAGS_db + "/.checkpoint" + ToString(thread->tid);

    // We need to clear DB including manifest files, so make a copy
    Options opt_copy = options_;
    opt_copy.env = db_stress_env->target();
    DestroyDB(checkpoint_dir, opt_copy);

    Checkpoint* checkpoint = nullptr;
    Status s = Checkpoint::Create(db_, &checkpoint);
    if (s.ok()) {
      s = checkpoint->CreateCheckpoint(checkpoint_dir);
    }
    std::vector<ColumnFamilyHandle*> cf_handles;
    DB* checkpoint_db = nullptr;
    if (s.ok()) {
      delete checkpoint;
      checkpoint = nullptr;
      Options options(options_);
      options.listeners.clear();
      std::vector<ColumnFamilyDescriptor> cf_descs;
      // TODO(ajkr): `column_family_names_` is not safe to access here when
      // `clear_column_family_one_in != 0`. But we can't easily switch to
      // `ListColumnFamilies` to get names because it won't necessarily give
      // the same order as `column_family_names_`.
      if (FLAGS_clear_column_family_one_in == 0) {
        for (const auto& name : column_family_names_) {
          cf_descs.emplace_back(name, ColumnFamilyOptions(options));
        }
        s = DB::OpenForReadOnly(DBOptions(options), checkpoint_dir, cf_descs,
                                &cf_handles, &checkpoint_db);
      }
    }
    if (checkpoint_db != nullptr) {
      for (auto cfh : cf_handles) {
        delete cfh;
      }
      cf_handles.clear();
      delete checkpoint_db;
      checkpoint_db = nullptr;
    }
    DestroyDB(checkpoint_dir, opt_copy);
    if (!s.ok()) {
      fprintf(stderr, "A checkpoint operation failed with: %s\n",
              s.ToString().c_str());
    }
    return s;
  }
#endif  // !ROCKSDB_LITE

  void VerifyDb(ThreadState* thread) const override {
    ReadOptions options(FLAGS_verify_checksum, true);
    // We must set total_order_seek to true because we are doing a SeekToFirst
    // on a column family whose memtables may support (by default) prefix-based
    // iterator. In this case, NewIterator with options.total_order_seek being
    // false returns a prefix-based iterator. Calling SeekToFirst using this
    // iterator causes the iterator to become invalid. That means we cannot
    // iterate the memtable using this iterator any more, although the memtable
    // contains the most up-to-date key-values.
    options.total_order_seek = true;
    const auto ss_deleter = [this](const Snapshot* ss) {
      db_->ReleaseSnapshot(ss);
    };
    std::unique_ptr<const Snapshot, decltype(ss_deleter)> snapshot_guard(
        db_->GetSnapshot(), ss_deleter);
    options.snapshot = snapshot_guard.get();
    assert(thread != nullptr);
    auto shared = thread->shared;
    std::vector<std::unique_ptr<Iterator>> iters(column_families_.size());
    for (size_t i = 0; i != column_families_.size(); ++i) {
      iters[i].reset(db_->NewIterator(options, column_families_[i]));
    }
    for (auto& iter : iters) {
      iter->SeekToFirst();
    }
    size_t num = column_families_.size();
    assert(num == iters.size());
    std::vector<Status> statuses(num, Status::OK());
    do {
      if (shared->HasVerificationFailedYet()) {
        break;
      }
      size_t valid_cnt = 0;
      size_t idx = 0;
      for (auto& iter : iters) {
        if (iter->Valid()) {
          ++valid_cnt;
        } else {
          statuses[idx] = iter->status();
        }
        ++idx;
      }
      if (valid_cnt == 0) {
        Status status;
        for (size_t i = 0; i != num; ++i) {
          const auto& s = statuses[i];
          if (!s.ok()) {
            status = s;
            fprintf(stderr, "Iterator on cf %s has error: %s\n",
                    column_families_[i]->GetName().c_str(),
                    s.ToString().c_str());
            shared->SetVerificationFailure();
          }
        }
        break;
      } else if (valid_cnt != iters.size()) {
        shared->SetVerificationFailure();
        for (size_t i = 0; i != num; ++i) {
          if (!iters[i]->Valid()) {
            if (statuses[i].ok()) {
              fprintf(stderr, "Finished scanning cf %s\n",
                      column_families_[i]->GetName().c_str());
            } else {
              fprintf(stderr, "Iterator on cf %s has error: %s\n",
                      column_families_[i]->GetName().c_str(),
                      statuses[i].ToString().c_str());
            }
          } else {
            fprintf(stderr, "cf %s has remaining data to scan\n",
                    column_families_[i]->GetName().c_str());
          }
        }
        break;
      }
      if (shared->HasVerificationFailedYet()) {
        break;
      }
      // If the program reaches here, then all column families' iterators are
      // still valid.
      if (shared->PrintingVerificationResults()) {
        continue;
      }
      Slice key;
      Slice value;
      int num_mismatched_cfs = 0;
      for (size_t i = 0; i != num; ++i) {
        if (i == 0) {
          key = iters[i]->key();
          value = iters[i]->value();
        } else {
          int cmp = key.compare(iters[i]->key());
          if (cmp != 0) {
            ++num_mismatched_cfs;
            if (1 == num_mismatched_cfs) {
              fprintf(stderr, "Verification failed\n");
              fprintf(stderr, "Latest Sequence Number: %" PRIu64 "\n",
                      db_->GetLatestSequenceNumber());
              fprintf(stderr, "[%s] %s => %s\n",
                      column_families_[0]->GetName().c_str(),
                      key.ToString(true /* hex */).c_str(),
                      value.ToString(true /* hex */).c_str());
            }
            fprintf(stderr, "[%s] %s => %s\n",
                    column_families_[i]->GetName().c_str(),
                    iters[i]->key().ToString(true /* hex */).c_str(),
                    iters[i]->value().ToString(true /* hex */).c_str());
#ifndef ROCKSDB_LITE
            Slice begin_key;
            Slice end_key;
            if (cmp < 0) {
              begin_key = key;
              end_key = iters[i]->key();
            } else {
              begin_key = iters[i]->key();
              end_key = key;
            }
            std::vector<KeyVersion> versions;
            const size_t kMaxNumIKeys = 8;
            const auto print_key_versions = [&](ColumnFamilyHandle* cfh) {
              Status s = GetAllKeyVersions(db_, cfh, begin_key, end_key,
                                           kMaxNumIKeys, &versions);
              if (!s.ok()) {
                fprintf(stderr, "%s\n", s.ToString().c_str());
                return;
              }
              assert(nullptr != cfh);
              fprintf(stderr,
                      "Internal keys in CF '%s', [%s, %s] (max %" ROCKSDB_PRIszt
                      ")\n",
                      cfh->GetName().c_str(),
                      begin_key.ToString(true /* hex */).c_str(),
                      end_key.ToString(true /* hex */).c_str(), kMaxNumIKeys);
              for (const KeyVersion& kv : versions) {
                fprintf(stderr, "  key %s seq %" PRIu64 " type %d\n",
                        Slice(kv.user_key).ToString(true).c_str(), kv.sequence,
                        kv.type);
              }
            };
            if (1 == num_mismatched_cfs) {
              print_key_versions(column_families_[0]);
            }
            print_key_versions(column_families_[i]);
#endif  // ROCKSDB_LITE
            shared->SetVerificationFailure();
          }
        }
      }
      shared->FinishPrintingVerificationResults();
      for (auto& iter : iters) {
        iter->Next();
      }
    } while (true);
  }

#ifndef ROCKSDB_LITE
  void ContinuouslyVerifyDb(ThreadState* thread) const override {
    assert(thread);
    Status status;

    DB* db_ptr = cmp_db_ ? cmp_db_ : db_;
    const auto& cfhs = cmp_db_ ? cmp_cfhs_ : column_families_;
    const auto ss_deleter = [&](const Snapshot* ss) {
      db_ptr->ReleaseSnapshot(ss);
    };
    std::unique_ptr<const Snapshot, decltype(ss_deleter)> snapshot_guard(
        db_ptr->GetSnapshot(), ss_deleter);
    if (cmp_db_) {
      status = cmp_db_->TryCatchUpWithPrimary();
    }
    SharedState* shared = thread->shared;
    assert(shared);
    if (!status.ok()) {
      shared->SetShouldStopTest();
      return;
    }
    assert(cmp_db_ || snapshot_guard.get());
    const auto checksum_column_family = [](Iterator* iter,
                                           uint32_t* checksum) -> Status {
      assert(nullptr != checksum);
      uint32_t ret = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ret = crc32c::Extend(ret, iter->key().data(), iter->key().size());
        ret = crc32c::Extend(ret, iter->value().data(), iter->value().size());
      }
      *checksum = ret;
      return iter->status();
    };
    ReadOptions ropts;
    ropts.total_order_seek = true;
    ropts.snapshot = snapshot_guard.get();
    uint32_t crc = 0;
    {
      // Compute crc for all key-values of default column family.
      std::unique_ptr<Iterator> it(db_ptr->NewIterator(ropts));
      status = checksum_column_family(it.get(), &crc);
    }
    uint32_t tmp_crc = 0;
    if (status.ok()) {
      for (ColumnFamilyHandle* cfh : cfhs) {
        if (cfh == db_ptr->DefaultColumnFamily()) {
          continue;
        }
        std::unique_ptr<Iterator> it(db_ptr->NewIterator(ropts, cfh));
        status = checksum_column_family(it.get(), &tmp_crc);
        if (!status.ok() || tmp_crc != crc) {
          break;
        }
      }
    }
    if (!status.ok() || tmp_crc != crc) {
      shared->SetShouldStopTest();
    }
  }
#endif  // !ROCKSDB_LITE

  std::vector<int> GenerateColumnFamilies(
      const int /* num_column_families */,
      int /* rand_column_family */) const override {
    std::vector<int> ret;
    int num = static_cast<int>(column_families_.size());
    int k = 0;
    std::generate_n(back_inserter(ret), num, [&k]() -> int { return k++; });
    return ret;
  }

 private:
  std::atomic<int64_t> batch_id_;
};

StressTest* CreateCfConsistencyStressTest() {
  return new CfConsistencyStressTest();
}

}  // namespace rocksdb
#endif  // GFLAGS
