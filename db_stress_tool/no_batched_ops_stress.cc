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
#endif // NDEBUG

namespace ROCKSDB_NAMESPACE {
class NonBatchedOpsStressTest : public StressTest {
 public:
  NonBatchedOpsStressTest() {}

  virtual ~NonBatchedOpsStressTest() {}

  void VerifyDb(ThreadState* thread) const override {
    // This `ReadOptions` is for validation purposes. Ignore
    // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
    ReadOptions options(FLAGS_verify_checksum, true);
    std::string ts_str;
    Slice ts;
    if (FLAGS_user_timestamp_size > 0) {
      ts_str = GenerateTimestampForRead();
      ts = ts_str;
      options.timestamp = &ts;
    }
    auto shared = thread->shared;
    const int64_t max_key = shared->GetMaxKey();
    const int64_t keys_per_thread = max_key / shared->GetNumThreads();
    int64_t start = keys_per_thread * thread->tid;
    int64_t end = start + keys_per_thread;
    uint64_t prefix_to_use =
        (FLAGS_prefix_size < 0) ? 1 : static_cast<size_t>(FLAGS_prefix_size);
    if (thread->tid == shared->GetNumThreads() - 1) {
      end = max_key;
    }
    for (size_t cf = 0; cf < column_families_.size(); ++cf) {
      if (thread->shared->HasVerificationFailedYet()) {
        break;
      }
      if (thread->rand.OneIn(3)) {
        // 1/3 chance use iterator to verify this range
        Slice prefix;
        std::string seek_key = Key(start);
        std::unique_ptr<Iterator> iter(
            db_->NewIterator(options, column_families_[cf]));
        iter->Seek(seek_key);
        prefix = Slice(seek_key.data(), prefix_to_use);
        for (auto i = start; i < end; i++) {
          if (thread->shared->HasVerificationFailedYet()) {
            break;
          }
          std::string from_db;
          std::string keystr = Key(i);
          Slice k = keystr;
          Slice pfx = Slice(keystr.data(), prefix_to_use);
          // Reseek when the prefix changes
          if (prefix_to_use > 0 && prefix.compare(pfx) != 0) {
            iter->Seek(k);
            seek_key = keystr;
            prefix = Slice(seek_key.data(), prefix_to_use);
          }
          Status s = iter->status();
          if (iter->Valid()) {
            Slice iter_key = iter->key();
            if (iter->key().compare(k) > 0) {
              s = Status::NotFound(Slice());
            } else if (iter->key().compare(k) == 0) {
              from_db = iter->value().ToString();
              iter->Next();
            } else if (iter_key.compare(k) < 0) {
              VerificationAbort(shared, "An out of range key was found",
                                static_cast<int>(cf), i);
            }
          } else {
            // The iterator found no value for the key in question, so do not
            // move to the next item in the iterator
            s = Status::NotFound();
          }
          VerifyValue(static_cast<int>(cf), i, options, shared, from_db, s,
                      true);
          if (from_db.length()) {
            PrintKeyValue(static_cast<int>(cf), static_cast<uint32_t>(i),
                          from_db.data(), from_db.length());
          }
        }
      } else if (thread->rand.OneIn(2)) {
        // 1/3 chance use Get to verify this range
        for (auto i = start; i < end; i++) {
          if (thread->shared->HasVerificationFailedYet()) {
            break;
          }
          std::string from_db;
          std::string keystr = Key(i);
          Slice k = keystr;
          Status s = db_->Get(options, column_families_[cf], k, &from_db);
          VerifyValue(static_cast<int>(cf), i, options, shared, from_db, s,
                      true);
          if (from_db.length()) {
            PrintKeyValue(static_cast<int>(cf), static_cast<uint32_t>(i),
                          from_db.data(), from_db.length());
          }
        }
      } else {
        // 1/3 chance use MultiGet to verify this range
        for (auto i = start; i < end;) {
          if (thread->shared->HasVerificationFailedYet()) {
            break;
          }
          // Keep the batch size to some reasonable value
          size_t batch_size = thread->rand.Uniform(128) + 1;
          batch_size = std::min<size_t>(batch_size, end - i);
          std::vector<std::string> keystrs(batch_size);
          std::vector<Slice> keys(batch_size);
          std::vector<PinnableSlice> values(batch_size);
          std::vector<Status> statuses(batch_size);
          for (size_t j = 0; j < batch_size; ++j) {
            keystrs[j] = Key(i + j);
            keys[j] = Slice(keystrs[j].data(), keystrs[j].length());
          }
          db_->MultiGet(options, column_families_[cf], batch_size, keys.data(),
                        values.data(), statuses.data());
          for (size_t j = 0; j < batch_size; ++j) {
            Status s = statuses[j];
            std::string from_db = values[j].ToString();
            VerifyValue(static_cast<int>(cf), i + j, options, shared, from_db,
                        s, true);
            if (from_db.length()) {
              PrintKeyValue(static_cast<int>(cf), static_cast<uint32_t>(i + j),
                            from_db.data(), from_db.length());
            }
          }

          i += batch_size;
        }
      }
    }
  }

  void MaybeClearOneColumnFamily(ThreadState* thread) override {
    if (FLAGS_column_families > 1) {
      if (thread->rand.OneInOpt(FLAGS_clear_column_family_one_in)) {
        // drop column family and then create it again (can't drop default)
        int cf = thread->rand.Next() % (FLAGS_column_families - 1) + 1;
        std::string new_name = ToString(new_column_family_name_.fetch_add(1));
        {
          MutexLock l(thread->shared->GetMutex());
          fprintf(
              stdout,
              "[CF %d] Dropping and recreating column family. new name: %s\n",
              cf, new_name.c_str());
        }
        thread->shared->LockColumnFamily(cf);
        Status s = db_->DropColumnFamily(column_families_[cf]);
        delete column_families_[cf];
        if (!s.ok()) {
          fprintf(stderr, "dropping column family error: %s\n",
                  s.ToString().c_str());
          std::terminate();
        }
        s = db_->CreateColumnFamily(ColumnFamilyOptions(options_), new_name,
                                    &column_families_[cf]);
        column_family_names_[cf] = new_name;
        thread->shared->ClearColumnFamily(cf);
        if (!s.ok()) {
          fprintf(stderr, "creating column family error: %s\n",
                  s.ToString().c_str());
          std::terminate();
        }
        thread->shared->UnlockColumnFamily(cf);
      }
    }
  }

  bool ShouldAcquireMutexOnKey() const override { return true; }

  bool IsStateTracked() const override { return true; }

  Status TestGet(ThreadState* thread, const ReadOptions& read_opts,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys) override {
    auto cfh = column_families_[rand_column_families[0]];
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    std::string from_db;
    int error_count = 0;

#ifndef NDEBUG
    if (fault_fs_guard) {
      fault_fs_guard->EnableErrorInjection();
      SharedState::ignore_read_error = false;
    }
#endif // NDEBUG
    Status s = db_->Get(read_opts, cfh, key, &from_db);
#ifndef NDEBUG
    if (fault_fs_guard) {
      error_count = fault_fs_guard->GetAndResetErrorCount();
    }
#endif // NDEBUG
    if (s.ok()) {
#ifndef NDEBUG
      if (fault_fs_guard) {
        if (error_count && !SharedState::ignore_read_error) {
          // Grab mutex so multiple thread don't try to print the
          // stack trace at the same time
          MutexLock l(thread->shared->GetMutex());
          fprintf(stderr, "Didn't get expected error from Get\n");
          fprintf(stderr, "Callstack that injected the fault\n");
          fault_fs_guard->PrintFaultBacktrace();
          std::terminate();
        }
      }
#endif // NDEBUG
      // found case
      thread->stats.AddGets(1, 1);
    } else if (s.IsNotFound()) {
      // not found case
      thread->stats.AddGets(1, 0);
    } else {
      if (error_count == 0) {
        // errors case
        thread->stats.AddErrors(1);
      } else {
        thread->stats.AddVerifiedErrors(1);
      }
    }
#ifndef NDEBUG
    if (fault_fs_guard) {
      fault_fs_guard->DisableErrorInjection();
    }
#endif // NDEBUG
    return s;
  }

  std::vector<Status> TestMultiGet(
      ThreadState* thread, const ReadOptions& read_opts,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) override {
    size_t num_keys = rand_keys.size();
    std::vector<std::string> key_str;
    std::vector<Slice> keys;
    key_str.reserve(num_keys);
    keys.reserve(num_keys);
    std::vector<PinnableSlice> values(num_keys);
    std::vector<Status> statuses(num_keys);
    ColumnFamilyHandle* cfh = column_families_[rand_column_families[0]];
    int error_count = 0;
    // Do a consistency check between Get and MultiGet. Don't do it too
    // often as it will slow db_stress down
    bool do_consistency_check = thread->rand.OneIn(4);

    ReadOptions readoptionscopy = read_opts;
    if (do_consistency_check) {
      readoptionscopy.snapshot = db_->GetSnapshot();
    }

    // To appease clang analyzer
    const bool use_txn = FLAGS_use_txn;

    // Create a transaction in order to write some data. The purpose is to
    // exercise WriteBatchWithIndex::MultiGetFromBatchAndDB. The transaction
    // will be rolled back once MultiGet returns.
#ifndef ROCKSDB_LITE
    Transaction* txn = nullptr;
    if (use_txn) {
      WriteOptions wo;
      Status s = NewTxn(wo, &txn);
      if (!s.ok()) {
        fprintf(stderr, "NewTxn: %s\n", s.ToString().c_str());
        std::terminate();
      }
    }
#endif
    for (size_t i = 0; i < num_keys; ++i) {
      key_str.emplace_back(Key(rand_keys[i]));
      keys.emplace_back(key_str.back());
#ifndef ROCKSDB_LITE
      if (use_txn) {
        // With a 1 in 10 probability, insert the just added key in the batch
        // into the transaction. This will create an overlap with the MultiGet
        // keys and exercise some corner cases in the code
        if (thread->rand.OneIn(10)) {
          int op = thread->rand.Uniform(2);
          Status s;
          switch (op) {
            case 0:
            case 1: {
              uint32_t value_base =
                  thread->rand.Next() % thread->shared->UNKNOWN_SENTINEL;
              char value[100];
              size_t sz = GenerateValue(value_base, value, sizeof(value));
              Slice v(value, sz);
              if (op == 0) {
                s = txn->Put(cfh, keys.back(), v);
              } else {
                s = txn->Merge(cfh, keys.back(), v);
              }
              break;
            }
            case 2:
              s = txn->Delete(cfh, keys.back());
              break;
            default:
              assert(false);
          }
          if (!s.ok()) {
            fprintf(stderr, "Transaction put: %s\n", s.ToString().c_str());
            std::terminate();
          }
        }
      }
#endif
    }

    if (!use_txn) {
#ifndef NDEBUG
      if (fault_fs_guard) {
        fault_fs_guard->EnableErrorInjection();
        SharedState::ignore_read_error = false;
      }
#endif // NDEBUG
      db_->MultiGet(readoptionscopy, cfh, num_keys, keys.data(), values.data(),
                    statuses.data());
#ifndef NDEBUG
      if (fault_fs_guard) {
        error_count = fault_fs_guard->GetAndResetErrorCount();
      }
#endif // NDEBUG
    } else {
#ifndef ROCKSDB_LITE
      txn->MultiGet(readoptionscopy, cfh, num_keys, keys.data(), values.data(),
                    statuses.data());
#endif
    }

#ifndef NDEBUG
    if (fault_fs_guard && error_count && !SharedState::ignore_read_error) {
      int stat_nok = 0;
      for (const auto& s : statuses) {
        if (!s.ok() && !s.IsNotFound()) {
          stat_nok++;
        }
      }

      if (stat_nok < error_count) {
        // Grab mutex so multiple thread don't try to print the
        // stack trace at the same time
        MutexLock l(thread->shared->GetMutex());
        fprintf(stderr, "Didn't get expected error from MultiGet. \n");
        fprintf(stderr, "num_keys %zu Expected %d errors, seen %d\n", num_keys,
                error_count, stat_nok);
        fprintf(stderr, "Callstack that injected the fault\n");
        fault_fs_guard->PrintFaultBacktrace();
        std::terminate();
      }
    }
    if (fault_fs_guard) {
      fault_fs_guard->DisableErrorInjection();
    }
#endif // NDEBUG

    for (size_t i = 0; i < statuses.size(); ++i) {
      Status s = statuses[i];
      bool is_consistent = true;
      // Only do the consistency check if no error was injected and MultiGet
      // didn't return an unexpected error
      if (do_consistency_check && !error_count && (s.ok() || s.IsNotFound())) {
        Status tmp_s;
        std::string value;

        if (use_txn) {
#ifndef ROCKSDB_LITE
          tmp_s = txn->Get(readoptionscopy, cfh, keys[i], &value);
#endif  // ROCKSDB_LITE
        } else {
          tmp_s = db_->Get(readoptionscopy, cfh, keys[i], &value);
        }
        if (!tmp_s.ok() && !tmp_s.IsNotFound()) {
          fprintf(stderr, "Get error: %s\n", s.ToString().c_str());
          is_consistent = false;
        } else if (!s.ok() && tmp_s.ok()) {
          fprintf(stderr, "MultiGet returned different results with key %s\n",
                  keys[i].ToString(true).c_str());
          fprintf(stderr, "Get returned ok, MultiGet returned not found\n");
          is_consistent = false;
        } else if (s.ok() && tmp_s.IsNotFound()) {
          fprintf(stderr, "MultiGet returned different results with key %s\n",
                  keys[i].ToString(true).c_str());
          fprintf(stderr, "MultiGet returned ok, Get returned not found\n");
          is_consistent = false;
        } else if (s.ok() && value != values[i].ToString()) {
          fprintf(stderr, "MultiGet returned different results with key %s\n",
                  keys[i].ToString(true).c_str());
          fprintf(stderr, "MultiGet returned value %s\n",
                  values[i].ToString(true).c_str());
          fprintf(stderr, "Get returned value %s\n", value.c_str());
          is_consistent = false;
        }
      }

      if (!is_consistent) {
        fprintf(stderr, "TestMultiGet error: is_consistent is false\n");
        thread->stats.AddErrors(1);
        // Fail fast to preserve the DB state
        thread->shared->SetVerificationFailure();
        break;
      } else if (s.ok()) {
        // found case
        thread->stats.AddGets(1, 1);
      } else if (s.IsNotFound()) {
        // not found case
        thread->stats.AddGets(1, 0);
      } else if (s.IsMergeInProgress() && use_txn) {
        // With txn this is sometimes expected.
        thread->stats.AddGets(1, 1);
      } else {
        if (error_count == 0) {
          // errors case
          fprintf(stderr, "MultiGet error: %s\n", s.ToString().c_str());
          thread->stats.AddErrors(1);
        } else {
          thread->stats.AddVerifiedErrors(1);
        }
      }
    }

    if (readoptionscopy.snapshot) {
      db_->ReleaseSnapshot(readoptionscopy.snapshot);
    }
    if (use_txn) {
#ifndef ROCKSDB_LITE
      RollbackTxn(txn);
#endif
    }
    return statuses;
  }

  Status TestPrefixScan(ThreadState* thread, const ReadOptions& read_opts,
                        const std::vector<int>& rand_column_families,
                        const std::vector<int64_t>& rand_keys) override {
    auto cfh = column_families_[rand_column_families[0]];
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    Slice prefix = Slice(key.data(), FLAGS_prefix_size);

    std::string upper_bound;
    Slice ub_slice;
    ReadOptions ro_copy = read_opts;
    // Get the next prefix first and then see if we want to set upper bound.
    // We'll use the next prefix in an assertion later on
    if (GetNextPrefix(prefix, &upper_bound) && thread->rand.OneIn(2)) {
      // For half of the time, set the upper bound to the next prefix
      ub_slice = Slice(upper_bound);
      ro_copy.iterate_upper_bound = &ub_slice;
    }

    Iterator* iter = db_->NewIterator(ro_copy, cfh);
    unsigned long count = 0;
    for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix);
         iter->Next()) {
      ++count;
    }

    assert(count <= GetPrefixKeyCount(prefix.ToString(), upper_bound));

    Status s = iter->status();
    if (iter->status().ok()) {
      thread->stats.AddPrefixes(1, count);
    } else {
      fprintf(stderr, "TestPrefixScan error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    }
    delete iter;
    return s;
  }

  Status TestPut(ThreadState* thread, WriteOptions& write_opts,
                 const ReadOptions& read_opts,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys, char (&value)[100],
                 std::unique_ptr<MutexLock>& lock) override {
    auto shared = thread->shared;
    int64_t max_key = shared->GetMaxKey();
    int64_t rand_key = rand_keys[0];
    int rand_column_family = rand_column_families[0];
    std::string write_ts_str;
    Slice write_ts;
    while (!shared->AllowsOverwrite(rand_key) &&
           (FLAGS_use_merge || shared->Exists(rand_column_family, rand_key))) {
      lock.reset();
      rand_key = thread->rand.Next() % max_key;
      rand_column_family = thread->rand.Next() % FLAGS_column_families;
      lock.reset(
          new MutexLock(shared->GetMutexForKey(rand_column_family, rand_key)));
      if (FLAGS_user_timestamp_size > 0) {
        write_ts_str = NowNanosStr();
        write_ts = write_ts_str;
      }
    }
    if (write_ts.size() == 0 && FLAGS_user_timestamp_size) {
      write_ts_str = NowNanosStr();
      write_ts = write_ts_str;
    }

    std::string key_str = Key(rand_key);
    Slice key = key_str;
    ColumnFamilyHandle* cfh = column_families_[rand_column_family];

    if (FLAGS_verify_before_write) {
      std::string key_str2 = Key(rand_key);
      Slice k = key_str2;
      std::string from_db;
      Status s = db_->Get(read_opts, cfh, k, &from_db);
      if (!VerifyValue(rand_column_family, rand_key, read_opts, shared, from_db,
                       s, true)) {
        return s;
      }
    }
    uint32_t value_base = thread->rand.Next() % shared->UNKNOWN_SENTINEL;
    size_t sz = GenerateValue(value_base, value, sizeof(value));
    Slice v(value, sz);
    shared->Put(rand_column_family, rand_key, value_base, true /* pending */);
    Status s;
    if (FLAGS_use_merge) {
      if (!FLAGS_use_txn) {
        s = db_->Merge(write_opts, cfh, key, v);
      } else {
#ifndef ROCKSDB_LITE
        Transaction* txn;
        s = NewTxn(write_opts, &txn);
        if (s.ok()) {
          s = txn->Merge(cfh, key, v);
          if (s.ok()) {
            s = CommitTxn(txn);
          }
        }
#endif
      }
    } else {
      if (!FLAGS_use_txn) {
        if (FLAGS_user_timestamp_size == 0) {
          s = db_->Put(write_opts, cfh, key, v);
        } else {
          s = db_->Put(write_opts, cfh, key, write_ts, v);
        }
      } else {
#ifndef ROCKSDB_LITE
        Transaction* txn;
        s = NewTxn(write_opts, &txn);
        if (s.ok()) {
          s = txn->Put(cfh, key, v);
          if (s.ok()) {
            s = CommitTxn(txn);
          }
        }
#endif
      }
    }
    shared->Put(rand_column_family, rand_key, value_base, false /* pending */);
    if (!s.ok()) {
      if (FLAGS_injest_error_severity >= 2) {
        if (!is_db_stopped_ && s.severity() >= Status::Severity::kFatalError) {
          is_db_stopped_ = true;
        } else if (!is_db_stopped_ ||
                   s.severity() < Status::Severity::kFatalError) {
          fprintf(stderr, "put or merge error: %s\n", s.ToString().c_str());
          std::terminate();
        }
      } else {
        fprintf(stderr, "put or merge error: %s\n", s.ToString().c_str());
        std::terminate();
      }
    }
    thread->stats.AddBytesForWrites(1, sz);
    PrintKeyValue(rand_column_family, static_cast<uint32_t>(rand_key), value,
                  sz);
    return s;
  }

  Status TestDelete(ThreadState* thread, WriteOptions& write_opts,
                    const std::vector<int>& rand_column_families,
                    const std::vector<int64_t>& rand_keys,
                    std::unique_ptr<MutexLock>& lock) override {
    int64_t rand_key = rand_keys[0];
    int rand_column_family = rand_column_families[0];
    auto shared = thread->shared;
    int64_t max_key = shared->GetMaxKey();

    // OPERATION delete
    // If the chosen key does not allow overwrite and it does not exist,
    // choose another key.
    std::string write_ts_str;
    Slice write_ts;
    while (!shared->AllowsOverwrite(rand_key) &&
           !shared->Exists(rand_column_family, rand_key)) {
      lock.reset();
      rand_key = thread->rand.Next() % max_key;
      rand_column_family = thread->rand.Next() % FLAGS_column_families;
      lock.reset(
          new MutexLock(shared->GetMutexForKey(rand_column_family, rand_key)));
      if (FLAGS_user_timestamp_size > 0) {
        write_ts_str = NowNanosStr();
        write_ts = write_ts_str;
      }
    }
    if (write_ts.size() == 0 && FLAGS_user_timestamp_size) {
      write_ts_str = NowNanosStr();
      write_ts = write_ts_str;
    }

    std::string key_str = Key(rand_key);
    Slice key = key_str;
    auto cfh = column_families_[rand_column_family];

    // Use delete if the key may be overwritten and a single deletion
    // otherwise.
    Status s;
    if (shared->AllowsOverwrite(rand_key)) {
      shared->Delete(rand_column_family, rand_key, true /* pending */);
      if (!FLAGS_use_txn) {
        if (FLAGS_user_timestamp_size == 0) {
          s = db_->Delete(write_opts, cfh, key);
        } else {
          s = db_->Delete(write_opts, cfh, key, write_ts);
        }
      } else {
#ifndef ROCKSDB_LITE
        Transaction* txn;
        s = NewTxn(write_opts, &txn);
        if (s.ok()) {
          s = txn->Delete(cfh, key);
          if (s.ok()) {
            s = CommitTxn(txn);
          }
        }
#endif
      }
      shared->Delete(rand_column_family, rand_key, false /* pending */);
      thread->stats.AddDeletes(1);
      if (!s.ok()) {
        if (FLAGS_injest_error_severity >= 2) {
          if (!is_db_stopped_ &&
              s.severity() >= Status::Severity::kFatalError) {
            is_db_stopped_ = true;
          } else if (!is_db_stopped_ ||
                     s.severity() < Status::Severity::kFatalError) {
            fprintf(stderr, "delete error: %s\n", s.ToString().c_str());
            std::terminate();
          }
        } else {
          fprintf(stderr, "delete error: %s\n", s.ToString().c_str());
          std::terminate();
        }
      }
    } else {
      shared->SingleDelete(rand_column_family, rand_key, true /* pending */);
      if (!FLAGS_use_txn) {
        if (FLAGS_user_timestamp_size == 0) {
          s = db_->SingleDelete(write_opts, cfh, key);
        } else {
          s = db_->SingleDelete(write_opts, cfh, key, write_ts);
        }
      } else {
#ifndef ROCKSDB_LITE
        Transaction* txn;
        s = NewTxn(write_opts, &txn);
        if (s.ok()) {
          s = txn->SingleDelete(cfh, key);
          if (s.ok()) {
            s = CommitTxn(txn);
          }
        }
#endif
      }
      shared->SingleDelete(rand_column_family, rand_key, false /* pending */);
      thread->stats.AddSingleDeletes(1);
      if (!s.ok()) {
        if (FLAGS_injest_error_severity >= 2) {
          if (!is_db_stopped_ &&
              s.severity() >= Status::Severity::kFatalError) {
            is_db_stopped_ = true;
          } else if (!is_db_stopped_ ||
                     s.severity() < Status::Severity::kFatalError) {
            fprintf(stderr, "single delete error: %s\n", s.ToString().c_str());
            std::terminate();
          }
        } else {
          fprintf(stderr, "single delete error: %s\n", s.ToString().c_str());
          std::terminate();
        }
      }
    }
    return s;
  }

  Status TestDeleteRange(ThreadState* thread, WriteOptions& write_opts,
                         const std::vector<int>& rand_column_families,
                         const std::vector<int64_t>& rand_keys,
                         std::unique_ptr<MutexLock>& lock) override {
    // OPERATION delete range
    std::vector<std::unique_ptr<MutexLock>> range_locks;
    // delete range does not respect disallowed overwrites. the keys for
    // which overwrites are disallowed are randomly distributed so it
    // could be expensive to find a range where each key allows
    // overwrites.
    int64_t rand_key = rand_keys[0];
    int rand_column_family = rand_column_families[0];
    auto shared = thread->shared;
    int64_t max_key = shared->GetMaxKey();
    if (rand_key > max_key - FLAGS_range_deletion_width) {
      lock.reset();
      rand_key =
          thread->rand.Next() % (max_key - FLAGS_range_deletion_width + 1);
      range_locks.emplace_back(
          new MutexLock(shared->GetMutexForKey(rand_column_family, rand_key)));
    } else {
      range_locks.emplace_back(std::move(lock));
    }
    for (int j = 1; j < FLAGS_range_deletion_width; ++j) {
      if (((rand_key + j) & ((1 << FLAGS_log2_keys_per_lock) - 1)) == 0) {
        range_locks.emplace_back(new MutexLock(
            shared->GetMutexForKey(rand_column_family, rand_key + j)));
      }
    }
    shared->DeleteRange(rand_column_family, rand_key,
                        rand_key + FLAGS_range_deletion_width,
                        true /* pending */);

    std::string keystr = Key(rand_key);
    Slice key = keystr;
    auto cfh = column_families_[rand_column_family];
    std::string end_keystr = Key(rand_key + FLAGS_range_deletion_width);
    Slice end_key = end_keystr;
    Status s = db_->DeleteRange(write_opts, cfh, key, end_key);
    if (!s.ok()) {
      if (FLAGS_injest_error_severity >= 2) {
        if (!is_db_stopped_ && s.severity() >= Status::Severity::kFatalError) {
          is_db_stopped_ = true;
        } else if (!is_db_stopped_ ||
                   s.severity() < Status::Severity::kFatalError) {
          fprintf(stderr, "delete range error: %s\n", s.ToString().c_str());
          std::terminate();
        }
      } else {
        fprintf(stderr, "delete range error: %s\n", s.ToString().c_str());
        std::terminate();
      }
    }
    int covered = shared->DeleteRange(rand_column_family, rand_key,
                                      rand_key + FLAGS_range_deletion_width,
                                      false /* pending */);
    thread->stats.AddRangeDeletions(1);
    thread->stats.AddCoveredByRangeDeletions(covered);
    return s;
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
  void TestIngestExternalFile(ThreadState* thread,
                              const std::vector<int>& rand_column_families,
                              const std::vector<int64_t>& rand_keys,
                              std::unique_ptr<MutexLock>& lock) override {
    const std::string sst_filename =
        FLAGS_db + "/." + ToString(thread->tid) + ".sst";
    Status s;
    if (db_stress_env->FileExists(sst_filename).ok()) {
      // Maybe we terminated abnormally before, so cleanup to give this file
      // ingestion a clean slate
      s = db_stress_env->DeleteFile(sst_filename);
    }

    SstFileWriter sst_file_writer(EnvOptions(options_), options_);
    if (s.ok()) {
      s = sst_file_writer.Open(sst_filename);
    }
    int64_t key_base = rand_keys[0];
    int column_family = rand_column_families[0];
    std::vector<std::unique_ptr<MutexLock>> range_locks;
    std::vector<uint32_t> values;
    SharedState* shared = thread->shared;

    // Grab locks, set pending state on expected values, and add keys
    for (int64_t key = key_base;
         s.ok() && key < std::min(key_base + FLAGS_ingest_external_file_width,
                                  shared->GetMaxKey());
         ++key) {
      if (key == key_base) {
        range_locks.emplace_back(std::move(lock));
      } else if ((key & ((1 << FLAGS_log2_keys_per_lock) - 1)) == 0) {
        range_locks.emplace_back(
            new MutexLock(shared->GetMutexForKey(column_family, key)));
      }

      uint32_t value_base = thread->rand.Next() % shared->UNKNOWN_SENTINEL;
      values.push_back(value_base);
      shared->Put(column_family, key, value_base, true /* pending */);

      char value[100];
      size_t value_len = GenerateValue(value_base, value, sizeof(value));
      auto key_str = Key(key);
      s = sst_file_writer.Put(Slice(key_str), Slice(value, value_len));
    }

    if (s.ok()) {
      s = sst_file_writer.Finish();
    }
    if (s.ok()) {
      s = db_->IngestExternalFile(column_families_[column_family],
                                  {sst_filename}, IngestExternalFileOptions());
    }
    if (!s.ok()) {
      fprintf(stderr, "file ingestion error: %s\n", s.ToString().c_str());
      std::terminate();
    }
    int64_t key = key_base;
    for (int32_t value : values) {
      shared->Put(column_family, key, value, false /* pending */);
      ++key;
    }
  }
#endif  // ROCKSDB_LITE

  bool VerifyValue(int cf, int64_t key, const ReadOptions& /*opts*/,
                   SharedState* shared, const std::string& value_from_db,
                   const Status& s, bool strict = false) const {
    if (shared->HasVerificationFailedYet()) {
      return false;
    }
    // compare value_from_db with the value in the shared state
    char value[kValueMaxLen];
    uint32_t value_base = shared->Get(cf, key);
    if (value_base == SharedState::UNKNOWN_SENTINEL) {
      return true;
    }
    if (value_base == SharedState::DELETION_SENTINEL && !strict) {
      return true;
    }

    if (s.ok()) {
      if (value_base == SharedState::DELETION_SENTINEL) {
        VerificationAbort(shared, "Unexpected value found", cf, key);
        return false;
      }
      size_t sz = GenerateValue(value_base, value, sizeof(value));
      if (value_from_db.length() != sz) {
        VerificationAbort(shared, "Length of value read is not equal", cf, key);
        return false;
      }
      if (memcmp(value_from_db.data(), value, sz) != 0) {
        VerificationAbort(shared, "Contents of value read don't match", cf,
                          key);
        return false;
      }
    } else {
      if (value_base != SharedState::DELETION_SENTINEL) {
        VerificationAbort(shared, "Value not found: " + s.ToString(), cf, key);
        return false;
      }
    }
    return true;
  }
};

StressTest* CreateNonBatchedOpsStressTest() {
  return new NonBatchedOpsStressTest();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
