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
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/fault_injection_fs.h"

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
      ts_str = GetNowNanos();
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
      if (thread->rand.OneIn(4)) {
        // 1/4 chance use iterator to verify this range
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
          VerifyOrSyncValue(static_cast<int>(cf), i, options, shared, from_db,
                            s, true);
          if (from_db.length()) {
            PrintKeyValue(static_cast<int>(cf), static_cast<uint32_t>(i),
                          from_db.data(), from_db.length());
          }
        }
      } else if (thread->rand.OneIn(3)) {
        // 1/4 chance use Get to verify this range
        for (auto i = start; i < end; i++) {
          if (thread->shared->HasVerificationFailedYet()) {
            break;
          }
          std::string from_db;
          std::string keystr = Key(i);
          Slice k = keystr;
          Status s = db_->Get(options, column_families_[cf], k, &from_db);
          VerifyOrSyncValue(static_cast<int>(cf), i, options, shared, from_db,
                            s, true);
          if (from_db.length()) {
            PrintKeyValue(static_cast<int>(cf), static_cast<uint32_t>(i),
                          from_db.data(), from_db.length());
          }
        }
      } else if (thread->rand.OneIn(2)) {
        // 1/4 chance use MultiGet to verify this range
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
            VerifyOrSyncValue(static_cast<int>(cf), i + j, options, shared,
                              from_db, s, true);
            if (from_db.length()) {
              PrintKeyValue(static_cast<int>(cf), static_cast<uint32_t>(i + j),
                            from_db.data(), from_db.length());
            }
          }

          i += batch_size;
        }
      } else {
        // 1/4 chance use GetMergeOperand to verify this range
        // Start off with small size that will be increased later if necessary
        std::vector<PinnableSlice> values(4);
        GetMergeOperandsOptions merge_operands_info;
        merge_operands_info.expected_max_number_of_operands =
            static_cast<int>(values.size());
        for (auto i = start; i < end; i++) {
          if (thread->shared->HasVerificationFailedYet()) {
            break;
          }
          std::string from_db;
          std::string keystr = Key(i);
          Slice k = keystr;
          int number_of_operands = 0;
          Status s = db_->GetMergeOperands(options, column_families_[cf], k,
                                           values.data(), &merge_operands_info,
                                           &number_of_operands);
          if (s.IsIncomplete()) {
            // Need to resize values as there are more than values.size() merge
            // operands on this key. Should only happen a few times when we
            // encounter a key that had more merge operands than any key seen so
            // far
            values.resize(number_of_operands);
            merge_operands_info.expected_max_number_of_operands =
                static_cast<int>(number_of_operands);
            s = db_->GetMergeOperands(options, column_families_[cf], k,
                                      values.data(), &merge_operands_info,
                                      &number_of_operands);
          }
          // Assumed here that GetMergeOperands always sets number_of_operand
          if (number_of_operands) {
            from_db = values[number_of_operands - 1].ToString();
          }
          VerifyOrSyncValue(static_cast<int>(cf), i, options, shared, from_db,
                            s, true);
          if (from_db.length()) {
            PrintKeyValue(static_cast<int>(cf), static_cast<uint32_t>(i),
                          from_db.data(), from_db.length());
          }
        }
      }
    }
  }

#ifndef ROCKSDB_LITE
  void ContinuouslyVerifyDb(ThreadState* thread) const override {
    if (!cmp_db_) {
      return;
    }
    assert(cmp_db_);
    assert(!cmp_cfhs_.empty());
    Status s = cmp_db_->TryCatchUpWithPrimary();
    if (!s.ok()) {
      assert(false);
      exit(1);
    }

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

    auto* shared = thread->shared;
    assert(shared);
    const int64_t max_key = shared->GetMaxKey();
    ReadOptions read_opts(FLAGS_verify_checksum, true);
    std::string ts_str;
    Slice ts;
    if (FLAGS_user_timestamp_size > 0) {
      ts_str = GetNowNanos();
      ts = ts_str;
      read_opts.timestamp = &ts;
    }

    static Random64 rand64(shared->GetSeed());

    {
      uint32_t crc = 0;
      std::unique_ptr<Iterator> it(cmp_db_->NewIterator(read_opts));
      s = checksum_column_family(it.get(), &crc);
      if (!s.ok()) {
        fprintf(stderr, "Computing checksum of default cf: %s\n",
                s.ToString().c_str());
        assert(false);
      }
    }

    for (auto* handle : cmp_cfhs_) {
      if (thread->rand.OneInOpt(3)) {
        // Use Get()
        uint64_t key = rand64.Uniform(static_cast<uint64_t>(max_key));
        std::string key_str = Key(key);
        std::string value;
        std::string key_ts;
        s = cmp_db_->Get(read_opts, handle, key_str, &value,
                         FLAGS_user_timestamp_size > 0 ? &key_ts : nullptr);
        s.PermitUncheckedError();
      } else {
        // Use range scan
        std::unique_ptr<Iterator> iter(cmp_db_->NewIterator(read_opts, handle));
        uint32_t rnd = (thread->rand.Next()) % 4;
        if (0 == rnd) {
          // SeekToFirst() + Next()*5
          read_opts.total_order_seek = true;
          iter->SeekToFirst();
          for (int i = 0; i < 5 && iter->Valid(); ++i, iter->Next()) {
          }
        } else if (1 == rnd) {
          // SeekToLast() + Prev()*5
          read_opts.total_order_seek = true;
          iter->SeekToLast();
          for (int i = 0; i < 5 && iter->Valid(); ++i, iter->Prev()) {
          }
        } else if (2 == rnd) {
          // Seek() +Next()*5
          uint64_t key = rand64.Uniform(static_cast<uint64_t>(max_key));
          std::string key_str = Key(key);
          iter->Seek(key_str);
          for (int i = 0; i < 5 && iter->Valid(); ++i, iter->Next()) {
          }
        } else {
          // SeekForPrev() + Prev()*5
          uint64_t key = rand64.Uniform(static_cast<uint64_t>(max_key));
          std::string key_str = Key(key);
          iter->SeekForPrev(key_str);
          for (int i = 0; i < 5 && iter->Valid(); ++i, iter->Prev()) {
          }
        }
      }
    }
  }
#else
  void ContinuouslyVerifyDb(ThreadState* /*thread*/) const override {}
#endif  // ROCKSDB_LITE

  void MaybeClearOneColumnFamily(ThreadState* thread) override {
    if (FLAGS_column_families > 1) {
      if (thread->rand.OneInOpt(FLAGS_clear_column_family_one_in)) {
        // drop column family and then create it again (can't drop default)
        int cf = thread->rand.Next() % (FLAGS_column_families - 1) + 1;
        std::string new_name =
            std::to_string(new_column_family_name_.fetch_add(1));
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

    if (fault_fs_guard) {
      fault_fs_guard->EnableErrorInjection();
      SharedState::ignore_read_error = false;
    }

    std::unique_ptr<MutexLock> lock(new MutexLock(
        thread->shared->GetMutexForKey(rand_column_families[0], rand_keys[0])));

    ReadOptions read_opts_copy = read_opts;
    std::string read_ts_str;
    Slice read_ts_slice;
    MaybeUseOlderTimestampForPointLookup(thread, read_ts_str, read_ts_slice,
                                         read_opts_copy);

    Status s = db_->Get(read_opts_copy, cfh, key, &from_db);
    if (fault_fs_guard) {
      error_count = fault_fs_guard->GetAndResetErrorCount();
    }
    if (s.ok()) {
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
      // found case
      thread->stats.AddGets(1, 1);
      // we only have the latest expected state
      if (!FLAGS_skip_verifydb && !read_opts_copy.timestamp &&
          thread->shared->Get(rand_column_families[0], rand_keys[0]) ==
              SharedState::DELETION_SENTINEL) {
        thread->shared->SetVerificationFailure();
        fprintf(stderr,
                "error : inconsistent values for key %s: Get returns %s, "
                "expected state does not have the key.\n",
                key.ToString(true).c_str(), StringToHex(from_db).c_str());
      }
    } else if (s.IsNotFound()) {
      // not found case
      thread->stats.AddGets(1, 0);
      if (!FLAGS_skip_verifydb && !read_opts_copy.timestamp) {
        auto expected =
            thread->shared->Get(rand_column_families[0], rand_keys[0]);
        if (expected != SharedState::DELETION_SENTINEL &&
            expected != SharedState::UNKNOWN_SENTINEL) {
          thread->shared->SetVerificationFailure();
          fprintf(stderr,
                  "error : inconsistent values for key %s: expected state has "
                  "the key, Get() returns NotFound.\n",
                  key.ToString(true).c_str());
        }
      }
    } else {
      if (error_count == 0) {
        // errors case
        thread->stats.AddErrors(1);
      } else {
        thread->stats.AddVerifiedErrors(1);
      }
    }
    if (fault_fs_guard) {
      fault_fs_guard->DisableErrorInjection();
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

    std::string read_ts_str;
    Slice read_ts_slice;
    MaybeUseOlderTimestampForPointLookup(thread, read_ts_str, read_ts_slice,
                                         readoptionscopy);

    readoptionscopy.rate_limiter_priority =
        FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;

    // To appease clang analyzer
    const bool use_txn = FLAGS_use_txn;

    // Create a transaction in order to write some data. The purpose is to
    // exercise WriteBatchWithIndex::MultiGetFromBatchAndDB. The transaction
    // will be rolled back once MultiGet returns.
#ifndef ROCKSDB_LITE
    Transaction* txn = nullptr;
    if (use_txn) {
      WriteOptions wo;
      if (FLAGS_rate_limit_auto_wal_flush) {
        wo.rate_limiter_priority = Env::IO_USER;
      }
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
      if (fault_fs_guard) {
        fault_fs_guard->EnableErrorInjection();
        SharedState::ignore_read_error = false;
      }
      db_->MultiGet(readoptionscopy, cfh, num_keys, keys.data(), values.data(),
                    statuses.data());
      if (fault_fs_guard) {
        error_count = fault_fs_guard->GetAndResetErrorCount();
      }
    } else {
#ifndef ROCKSDB_LITE
      txn->MultiGet(readoptionscopy, cfh, num_keys, keys.data(), values.data(),
                    statuses.data());
#endif
    }

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

    std::string read_ts_str;
    Slice read_ts_slice;
    MaybeUseOlderTimestampForRangeScan(thread, read_ts_str, read_ts_slice,
                                       ro_copy);

    Iterator* iter = db_->NewIterator(ro_copy, cfh);
    unsigned long count = 0;
    for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix);
         iter->Next()) {
      ++count;
    }

    if (ro_copy.iter_start_ts == nullptr) {
      assert(count <= GetPrefixKeyCount(prefix.ToString(), upper_bound));
    }

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
                 const std::vector<int64_t>& rand_keys,
                 char (&value)[100]) override {
    auto shared = thread->shared;
    int64_t max_key = shared->GetMaxKey();
    int64_t rand_key = rand_keys[0];
    int rand_column_family = rand_column_families[0];
    std::string write_ts_str;
    Slice write_ts;
    std::unique_ptr<MutexLock> lock(
        new MutexLock(shared->GetMutexForKey(rand_column_family, rand_key)));
    while (!shared->AllowsOverwrite(rand_key) &&
           (FLAGS_use_merge || shared->Exists(rand_column_family, rand_key))) {
      lock.reset();
      rand_key = thread->rand.Next() % max_key;
      rand_column_family = thread->rand.Next() % FLAGS_column_families;
      lock.reset(
          new MutexLock(shared->GetMutexForKey(rand_column_family, rand_key)));
      if (FLAGS_user_timestamp_size > 0) {
        write_ts_str = GetNowNanos();
        write_ts = write_ts_str;
      }
    }
    if (write_ts.size() == 0 && FLAGS_user_timestamp_size) {
      write_ts_str = GetNowNanos();
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
      if (!VerifyOrSyncValue(rand_column_family, rand_key, read_opts, shared,
                             from_db, s, true)) {
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
            s = CommitTxn(txn, thread);
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
            s = CommitTxn(txn, thread);
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
                    const std::vector<int64_t>& rand_keys) override {
    int64_t rand_key = rand_keys[0];
    int rand_column_family = rand_column_families[0];
    auto shared = thread->shared;

    std::unique_ptr<MutexLock> lock(
        new MutexLock(shared->GetMutexForKey(rand_column_family, rand_key)));

    // OPERATION delete
    std::string write_ts_str = GetNowNanos();
    Slice write_ts = write_ts_str;

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
            s = CommitTxn(txn, thread);
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
            s = CommitTxn(txn, thread);
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
                         const std::vector<int64_t>& rand_keys) override {
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
      rand_key =
          thread->rand.Next() % (max_key - FLAGS_range_deletion_width + 1);
    }
    for (int j = 0; j < FLAGS_range_deletion_width; ++j) {
      if (j == 0 ||
          ((rand_key + j) & ((1 << FLAGS_log2_keys_per_lock) - 1)) == 0) {
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
      const std::vector<int64_t>& /* rand_keys */) override {
    assert(false);
    fprintf(stderr,
            "RocksDB lite does not support "
            "TestIngestExternalFile\n");
    std::terminate();
  }
#else
  void TestIngestExternalFile(ThreadState* thread,
                              const std::vector<int>& rand_column_families,
                              const std::vector<int64_t>& rand_keys) override {
    const std::string sst_filename =
        FLAGS_db + "/." + std::to_string(thread->tid) + ".sst";
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
    range_locks.reserve(FLAGS_ingest_external_file_width);
    std::vector<int64_t> keys;
    keys.reserve(FLAGS_ingest_external_file_width);
    std::vector<uint32_t> values;
    values.reserve(FLAGS_ingest_external_file_width);
    SharedState* shared = thread->shared;

    assert(FLAGS_nooverwritepercent < 100);
    // Grab locks, set pending state on expected values, and add keys
    for (int64_t key = key_base;
         s.ok() && key < shared->GetMaxKey() &&
         static_cast<int32_t>(keys.size()) < FLAGS_ingest_external_file_width;
         ++key) {
      if (key == key_base ||
          (key & ((1 << FLAGS_log2_keys_per_lock) - 1)) == 0) {
        range_locks.emplace_back(
            new MutexLock(shared->GetMutexForKey(column_family, key)));
      }
      if (!shared->AllowsOverwrite(key)) {
        // We could alternatively include `key` on the condition its current
        // value is `DELETION_SENTINEL`.
        continue;
      }
      keys.push_back(key);

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
    for (size_t i = 0; i < keys.size(); ++i) {
      shared->Put(column_family, keys[i], values[i], false /* pending */);
    }
  }
#endif  // ROCKSDB_LITE

  // Given a key K, this creates an iterator which scans the range
  // [K, K + FLAGS_num_iterations) forward and backward.
  // Then does a random sequence of Next/Prev operations.
  Status TestIterateAgainstExpected(
      ThreadState* thread, const ReadOptions& read_opts,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) override {
    // Lock the whole range over which we might iterate to ensure it doesn't
    // change under us.
    std::vector<std::unique_ptr<MutexLock>> range_locks;
    int64_t lb = rand_keys[0];
    int rand_column_family = rand_column_families[0];
    auto shared = thread->shared;
    int64_t max_key = shared->GetMaxKey();
    if (static_cast<uint64_t>(lb) > max_key - FLAGS_num_iterations) {
      lb = thread->rand.Next() % (max_key - FLAGS_num_iterations + 1);
    }
    for (int j = 0; j < static_cast<int>(FLAGS_num_iterations); ++j) {
      if (j == 0 || ((lb + j) & ((1 << FLAGS_log2_keys_per_lock) - 1)) == 0) {
        range_locks.emplace_back(
            new MutexLock(shared->GetMutexForKey(rand_column_family, lb + j)));
      }
    }
    int64_t ub = lb + FLAGS_num_iterations;
    // Locks acquired for [lb, ub)
    ReadOptions readoptscopy(read_opts);
    std::string read_ts_str;
    Slice read_ts;
    if (FLAGS_user_timestamp_size > 0) {
      read_ts_str = GetNowNanos();
      read_ts = read_ts_str;
      readoptscopy.timestamp = &read_ts;
    }
    readoptscopy.total_order_seek = true;
    std::string max_key_str;
    Slice max_key_slice;
    if (!FLAGS_destroy_db_initially) {
      max_key_str = Key(max_key);
      max_key_slice = max_key_str;
      // to restrict iterator from reading keys written in batched_op_stress
      // that do not have expected state updated and may not be parseable by
      // GetIntVal().
      readoptscopy.iterate_upper_bound = &max_key_slice;
    }
    auto cfh = column_families_[rand_column_family];
    std::string op_logs;
    std::unique_ptr<Iterator> iter(db_->NewIterator(readoptscopy, cfh));

    auto check_no_key_in_range = [&](int64_t start, int64_t end) {
      for (auto j = std::max(start, lb); j < std::min(end, ub); ++j) {
        auto expected_value =
            shared->Get(rand_column_family, static_cast<int64_t>(j));
        if (expected_value != shared->DELETION_SENTINEL &&
            expected_value != shared->UNKNOWN_SENTINEL) {
          // Fail fast to preserve the DB state.
          thread->shared->SetVerificationFailure();
          if (iter->Valid()) {
            fprintf(stderr,
                    "Expected state has key %s, iterator is at key %s\n",
                    Slice(Key(j)).ToString(true).c_str(),
                    iter->key().ToString(true).c_str());
          } else {
            fprintf(stderr, "Expected state has key %s, iterator is invalid\n",
                    Slice(Key(j)).ToString(true).c_str());
          }
          fprintf(stderr, "Column family: %s, op_logs: %s\n",
                  cfh->GetName().c_str(), op_logs.c_str());
          thread->stats.AddErrors(1);
          return false;
        }
      }
      return true;
    };

    // Forward and backward scan to ensure we cover the entire range [lb, ub).
    // The random sequence Next and Prev test below tends to be very short
    // ranged.
    int64_t last_key = lb - 1;
    std::string key_str = Key(lb);
    iter->Seek(Slice(key_str));
    op_logs += "S " + Slice(key_str).ToString(true) + " ";
    uint64_t curr;
    while (true) {
      if (!iter->Valid()) {
        if (!iter->status().ok()) {
          thread->shared->SetVerificationFailure();
          fprintf(stderr, "TestIterate against expected state error: %s\n",
                  iter->status().ToString().c_str());
          fprintf(stderr, "Column family: %s, op_logs: %s\n",
                  cfh->GetName().c_str(), op_logs.c_str());
          thread->stats.AddErrors(1);
          return iter->status();
        }
        if (!check_no_key_in_range(last_key + 1, static_cast<int64_t>(ub))) {
          // error reported in check_no_key_in_range()
          return Status::OK();
        }
        break;
      }
      // iter is valid, the range (last_key, current key) was skipped
      GetIntVal(iter->key().ToString(), &curr);
      if (!check_no_key_in_range(last_key + 1, static_cast<int64_t>(curr))) {
        return Status::OK();
      }
      last_key = static_cast<int64_t>(curr);
      if (last_key >= ub - 1) {
        break;
      }
      iter->Next();
      op_logs += "N";
    }

    // backward scan
    key_str = Key(ub - 1);
    iter->SeekForPrev(Slice(key_str));
    op_logs += " SFP " + Slice(key_str).ToString(true) + " ";
    last_key = ub;
    while (true) {
      if (!iter->Valid()) {
        if (!iter->status().ok()) {
          thread->shared->SetVerificationFailure();
          fprintf(stderr, "TestIterate against expected state error: %s\n",
                  iter->status().ToString().c_str());
          fprintf(stderr, "Column family: %s, op_logs: %s\n",
                  cfh->GetName().c_str(), op_logs.c_str());
          thread->stats.AddErrors(1);
          return iter->status();
        }
        if (!check_no_key_in_range(lb, last_key)) {
          return Status::OK();
        }
        break;
      }
      // the range (current key, last key) was skipped
      GetIntVal(iter->key().ToString(), &curr);
      if (!check_no_key_in_range(static_cast<int64_t>(curr + 1), last_key)) {
        return Status::OK();
      }
      last_key = static_cast<int64_t>(curr);
      if (last_key <= lb) {
        break;
      }
      iter->Prev();
      op_logs += "P";
    }

    // start from middle of [lb, ub) otherwise it is easy to iterate out of
    // locked range
    int64_t mid = lb + static_cast<int64_t>(FLAGS_num_iterations / 2);
    key_str = Key(mid);
    Slice key = key_str;
    if (thread->rand.OneIn(2)) {
      iter->Seek(key);
      op_logs += " S " + key.ToString(true) + " ";
      if (!iter->Valid() && iter->status().ok()) {
        if (!check_no_key_in_range(mid, ub)) {
          return Status::OK();
        }
      }
    } else {
      iter->SeekForPrev(key);
      op_logs += " SFP " + key.ToString(true) + " ";
      if (!iter->Valid() && iter->status().ok()) {
        // iterator says nothing <= mid
        if (!check_no_key_in_range(lb, mid + 1)) {
          return Status::OK();
        }
      }
    }

    for (uint64_t i = 0; i < FLAGS_num_iterations && iter->Valid(); i++) {
      GetIntVal(iter->key().ToString(), &curr);
      if (curr < static_cast<uint64_t>(lb)) {
        iter->Next();
        op_logs += "N";
      } else if (curr >= static_cast<uint64_t>(ub)) {
        iter->Prev();
        op_logs += "P";
      } else {
        uint32_t expected_value =
            shared->Get(rand_column_family, static_cast<int64_t>(curr));
        if (expected_value == shared->DELETION_SENTINEL) {
          // Fail fast to preserve the DB state.
          thread->shared->SetVerificationFailure();
          fprintf(stderr, "Iterator has key %s, but expected state does not.\n",
                  iter->key().ToString(true).c_str());
          fprintf(stderr, "Column family: %s, op_logs: %s\n",
                  cfh->GetName().c_str(), op_logs.c_str());
          thread->stats.AddErrors(1);
          break;
        }
        if (thread->rand.OneIn(2)) {
          iter->Next();
          op_logs += "N";
          if (!iter->Valid()) {
            break;
          }
          uint64_t next;
          GetIntVal(iter->key().ToString(), &next);
          if (!check_no_key_in_range(static_cast<int64_t>(curr + 1),
                                     static_cast<int64_t>(next))) {
            return Status::OK();
          }
        } else {
          iter->Prev();
          op_logs += "P";
          if (!iter->Valid()) {
            break;
          }
          uint64_t prev;
          GetIntVal(iter->key().ToString(), &prev);
          if (!check_no_key_in_range(static_cast<int64_t>(prev + 1),
                                     static_cast<int64_t>(curr))) {
            return Status::OK();
          }
        }
      }
    }
    if (!iter->status().ok()) {
      thread->shared->SetVerificationFailure();
      fprintf(stderr, "TestIterate against expected state error: %s\n",
              iter->status().ToString().c_str());
      fprintf(stderr, "Column family: %s, op_logs: %s\n",
              cfh->GetName().c_str(), op_logs.c_str());
      thread->stats.AddErrors(1);
      return iter->status();
    }
    thread->stats.AddIterations(1);
    return Status::OK();
  }

  bool VerifyOrSyncValue(int cf, int64_t key, const ReadOptions& /*opts*/,
                         SharedState* shared, const std::string& value_from_db,
                         const Status& s, bool strict = false) const {
    if (shared->HasVerificationFailedYet()) {
      return false;
    }
    // compare value_from_db with the value in the shared state
    uint32_t value_base = shared->Get(cf, key);
    if (value_base == SharedState::UNKNOWN_SENTINEL) {
      if (s.ok()) {
        // Value exists in db, update state to reflect that
        Slice slice(value_from_db);
        value_base = GetValueBase(slice);
        shared->Put(cf, key, value_base, false);
      } else if (s.IsNotFound()) {
        // Value doesn't exist in db, update state to reflect that
        shared->SingleDelete(cf, key, false);
      }
      return true;
    }
    if (value_base == SharedState::DELETION_SENTINEL && !strict) {
      return true;
    }

    if (s.ok()) {
      char value[kValueMaxLen];
      if (value_base == SharedState::DELETION_SENTINEL) {
        VerificationAbort(shared, "Unexpected value found", cf, key,
                          value_from_db, "");
        return false;
      }
      size_t sz = GenerateValue(value_base, value, sizeof(value));
      if (value_from_db.length() != sz) {
        VerificationAbort(shared, "Length of value read is not equal", cf, key,
                          value_from_db, Slice(value, sz));
        return false;
      }
      if (memcmp(value_from_db.data(), value, sz) != 0) {
        VerificationAbort(shared, "Contents of value read don't match", cf, key,
                          value_from_db, Slice(value, sz));
        return false;
      }
    } else {
      if (value_base != SharedState::DELETION_SENTINEL) {
        char value[kValueMaxLen];
        size_t sz = GenerateValue(value_base, value, sizeof(value));
        VerificationAbort(shared, "Value not found: " + s.ToString(), cf, key,
                          "", Slice(value, sz));
        return false;
      }
    }
    return true;
  }

#ifndef ROCKSDB_LITE
  void PrepareTxnDbOptions(SharedState* shared,
                           TransactionDBOptions& txn_db_opts) override {
    txn_db_opts.rollback_deletion_type_callback =
        [shared](TransactionDB*, ColumnFamilyHandle*, const Slice& key) {
          assert(shared);
          uint64_t key_num = 0;
          bool ok = GetIntVal(key.ToString(), &key_num);
          assert(ok);
          (void)ok;
          return !shared->AllowsOverwrite(key_num);
        };
  }
#endif  // ROCKSDB_LITE
};

StressTest* CreateNonBatchedOpsStressTest() {
  return new NonBatchedOpsStressTest();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
