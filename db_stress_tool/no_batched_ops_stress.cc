//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db_stress_tool/expected_state.h"
#ifdef GFLAGS
#include "db/wide/wide_columns_helper.h"
#include "db_stress_tool/db_stress_common.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {
class NonBatchedOpsStressTest : public StressTest {
 public:
  NonBatchedOpsStressTest() = default;

  virtual ~NonBatchedOpsStressTest() = default;

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

      enum class VerificationMethod {
        kIterator,
        kGet,
        kGetEntity,
        kMultiGet,
        kMultiGetEntity,
        kGetMergeOperands,
        // Add any new items above kNumberOfMethods
        kNumberOfMethods
      };

      constexpr int num_methods =
          static_cast<int>(VerificationMethod::kNumberOfMethods);

      const VerificationMethod method =
          static_cast<VerificationMethod>(thread->rand.Uniform(
              (FLAGS_user_timestamp_size > 0) ? num_methods - 1 : num_methods));

      if (method == VerificationMethod::kIterator) {
        std::unique_ptr<Iterator> iter(
            db_->NewIterator(options, column_families_[cf]));

        std::string seek_key = Key(start);
        iter->Seek(seek_key);

        Slice prefix(seek_key.data(), prefix_to_use);

        for (int64_t i = start; i < end; ++i) {
          if (thread->shared->HasVerificationFailedYet()) {
            break;
          }

          const std::string key = Key(i);
          const Slice k(key);
          const Slice pfx(key.data(), prefix_to_use);

          // Reseek when the prefix changes
          if (prefix_to_use > 0 && prefix.compare(pfx) != 0) {
            iter->Seek(k);
            seek_key = key;
            prefix = Slice(seek_key.data(), prefix_to_use);
          }

          Status s = iter->status();

          std::string from_db;

          if (iter->Valid()) {
            const int diff = iter->key().compare(k);

            if (diff > 0) {
              s = Status::NotFound();
            } else if (diff == 0) {
              if (!VerifyWideColumns(iter->value(), iter->columns())) {
                VerificationAbort(shared, static_cast<int>(cf), i,
                                  iter->value(), iter->columns());
              }

              from_db = iter->value().ToString();
              iter->Next();
            } else {
              assert(diff < 0);

              VerificationAbort(shared, "An out of range key was found",
                                static_cast<int>(cf), i);
            }
          } else {
            // The iterator found no value for the key in question, so do not
            // move to the next item in the iterator
            s = Status::NotFound();
          }

          VerifyOrSyncValue(static_cast<int>(cf), i, options, shared, from_db,
                            /* msg_prefix */ "Iterator verification", s);

          if (!from_db.empty()) {
            PrintKeyValue(static_cast<int>(cf), static_cast<uint32_t>(i),
                          from_db.data(), from_db.size());
          }
        }
      } else if (method == VerificationMethod::kGet) {
        for (int64_t i = start; i < end; ++i) {
          if (thread->shared->HasVerificationFailedYet()) {
            break;
          }

          const std::string key = Key(i);
          std::string from_db;

          Status s = db_->Get(options, column_families_[cf], key, &from_db);

          VerifyOrSyncValue(static_cast<int>(cf), i, options, shared, from_db,
                            /* msg_prefix */ "Get verification", s);

          if (!from_db.empty()) {
            PrintKeyValue(static_cast<int>(cf), static_cast<uint32_t>(i),
                          from_db.data(), from_db.size());
          }
        }
      } else if (method == VerificationMethod::kGetEntity) {
        for (int64_t i = start; i < end; ++i) {
          if (thread->shared->HasVerificationFailedYet()) {
            break;
          }

          const std::string key = Key(i);
          PinnableWideColumns result;

          Status s =
              db_->GetEntity(options, column_families_[cf], key, &result);

          std::string from_db;

          if (s.ok()) {
            const WideColumns& columns = result.columns();

            if (WideColumnsHelper::HasDefaultColumn(columns)) {
              from_db = WideColumnsHelper::GetDefaultColumn(columns).ToString();
            }

            if (!VerifyWideColumns(columns)) {
              VerificationAbort(shared, static_cast<int>(cf), i, from_db,
                                columns);
            }
          }

          VerifyOrSyncValue(static_cast<int>(cf), i, options, shared, from_db,
                            /* msg_prefix */ "GetEntity verification", s);

          if (!from_db.empty()) {
            PrintKeyValue(static_cast<int>(cf), static_cast<uint32_t>(i),
                          from_db.data(), from_db.size());
          }
        }
      } else if (method == VerificationMethod::kMultiGet) {
        for (int64_t i = start; i < end;) {
          if (thread->shared->HasVerificationFailedYet()) {
            break;
          }

          // Keep the batch size to some reasonable value
          size_t batch_size = thread->rand.Uniform(128) + 1;
          batch_size = std::min<size_t>(batch_size, end - i);

          std::vector<std::string> key_strs(batch_size);
          std::vector<Slice> keys(batch_size);
          std::vector<PinnableSlice> values(batch_size);
          std::vector<Status> statuses(batch_size);

          for (size_t j = 0; j < batch_size; ++j) {
            key_strs[j] = Key(i + j);
            keys[j] = Slice(key_strs[j]);
          }

          db_->MultiGet(options, column_families_[cf], batch_size, keys.data(),
                        values.data(), statuses.data());

          for (size_t j = 0; j < batch_size; ++j) {
            const std::string from_db = values[j].ToString();

            VerifyOrSyncValue(static_cast<int>(cf), i + j, options, shared,
                              from_db, /* msg_prefix */ "MultiGet verification",
                              statuses[j]);

            if (!from_db.empty()) {
              PrintKeyValue(static_cast<int>(cf), static_cast<uint32_t>(i + j),
                            from_db.data(), from_db.size());
            }
          }

          i += batch_size;
        }
      } else if (method == VerificationMethod::kMultiGetEntity) {
        for (int64_t i = start; i < end;) {
          if (thread->shared->HasVerificationFailedYet()) {
            break;
          }

          // Keep the batch size to some reasonable value
          size_t batch_size = thread->rand.Uniform(128) + 1;
          batch_size = std::min<size_t>(batch_size, end - i);

          std::vector<std::string> key_strs(batch_size);
          std::vector<Slice> keys(batch_size);
          std::vector<PinnableWideColumns> results(batch_size);
          std::vector<Status> statuses(batch_size);

          for (size_t j = 0; j < batch_size; ++j) {
            key_strs[j] = Key(i + j);
            keys[j] = Slice(key_strs[j]);
          }

          db_->MultiGetEntity(options, column_families_[cf], batch_size,
                              keys.data(), results.data(), statuses.data());

          for (size_t j = 0; j < batch_size; ++j) {
            std::string from_db;

            if (statuses[j].ok()) {
              const WideColumns& columns = results[j].columns();

              if (WideColumnsHelper::HasDefaultColumn(columns)) {
                from_db =
                    WideColumnsHelper::GetDefaultColumn(columns).ToString();
              }

              if (!VerifyWideColumns(columns)) {
                VerificationAbort(shared, static_cast<int>(cf), i, from_db,
                                  columns);
              }
            }

            VerifyOrSyncValue(
                static_cast<int>(cf), i + j, options, shared, from_db,
                /* msg_prefix */ "MultiGetEntity verification", statuses[j]);

            if (!from_db.empty()) {
              PrintKeyValue(static_cast<int>(cf), static_cast<uint32_t>(i + j),
                            from_db.data(), from_db.size());
            }
          }

          i += batch_size;
        }
      } else {
        assert(method == VerificationMethod::kGetMergeOperands);

        // Start off with small size that will be increased later if necessary
        std::vector<PinnableSlice> values(4);

        GetMergeOperandsOptions merge_operands_info;
        merge_operands_info.expected_max_number_of_operands =
            static_cast<int>(values.size());

        for (int64_t i = start; i < end; ++i) {
          if (thread->shared->HasVerificationFailedYet()) {
            break;
          }

          const std::string key = Key(i);
          const Slice k(key);
          std::string from_db;
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
                            /* msg_prefix */ "GetMergeOperands verification",
                            s);

          if (!from_db.empty()) {
            PrintKeyValue(static_cast<int>(cf), static_cast<uint32_t>(i),
                          from_db.data(), from_db.size());
          }
        }
      }
    }
  }

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
          thread->shared->SafeTerminate();
        }
        s = db_->CreateColumnFamily(ColumnFamilyOptions(options_), new_name,
                                    &column_families_[cf]);
        column_family_names_[cf] = new_name;
        thread->shared->ClearColumnFamily(cf);
        if (!s.ok()) {
          fprintf(stderr, "creating column family error: %s\n",
                  s.ToString().c_str());
          thread->shared->SafeTerminate();
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

    ReadOptions read_opts_copy = read_opts;
    std::string read_ts_str;
    Slice read_ts_slice;
    if (FLAGS_user_timestamp_size > 0) {
      read_ts_str = GetNowNanos();
      read_ts_slice = read_ts_str;
      read_opts_copy.timestamp = &read_ts_slice;
    }
    bool read_older_ts = MaybeUseOlderTimestampForPointLookup(
        thread, read_ts_str, read_ts_slice, read_opts_copy);

    const ExpectedValue pre_read_expected_value =
        thread->shared->Get(rand_column_families[0], rand_keys[0]);
    Status s = db_->Get(read_opts_copy, cfh, key, &from_db);
    const ExpectedValue post_read_expected_value =
        thread->shared->Get(rand_column_families[0], rand_keys[0]);
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
      if (!FLAGS_skip_verifydb && !read_older_ts) {
        if (ExpectedValueHelper::MustHaveNotExisted(pre_read_expected_value,
                                                    post_read_expected_value)) {
          thread->shared->SetVerificationFailure();
          fprintf(stderr,
                  "error : inconsistent values for key %s: Get returns %s, "
                  "but expected state is \"deleted\".\n",
                  key.ToString(true).c_str(), StringToHex(from_db).c_str());
        }
        Slice from_db_slice(from_db);
        uint32_t value_base_from_db = GetValueBase(from_db_slice);
        if (!ExpectedValueHelper::InExpectedValueBaseRange(
                value_base_from_db, pre_read_expected_value,
                post_read_expected_value)) {
          thread->shared->SetVerificationFailure();
          fprintf(stderr,
                  "error : inconsistent values for key %s: Get returns %s with "
                  "value base %d that falls out of expected state's value base "
                  "range.\n",
                  key.ToString(true).c_str(), StringToHex(from_db).c_str(),
                  value_base_from_db);
        }
      }
    } else if (s.IsNotFound()) {
      // not found case
      thread->stats.AddGets(1, 0);
      if (!FLAGS_skip_verifydb && !read_older_ts) {
        if (ExpectedValueHelper::MustHaveExisted(pre_read_expected_value,
                                                 post_read_expected_value)) {
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
    // When Flags_use_txn is enabled, we also do a read your write check.
    std::vector<std::optional<ExpectedValue>> ryw_expected_values;
    ryw_expected_values.reserve(num_keys);

    SharedState* shared = thread->shared;

    int column_family = rand_column_families[0];
    ColumnFamilyHandle* cfh = column_families_[column_family];
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
    std::unique_ptr<Transaction> txn;
    if (use_txn) {
      WriteOptions wo;
      if (FLAGS_rate_limit_auto_wal_flush) {
        wo.rate_limiter_priority = Env::IO_USER;
      }
      Status s = NewTxn(wo, &txn);
      if (!s.ok()) {
        fprintf(stderr, "NewTxn error: %s\n", s.ToString().c_str());
        thread->shared->SafeTerminate();
      }
    }
    for (size_t i = 0; i < num_keys; ++i) {
      uint64_t rand_key = rand_keys[i];
      key_str.emplace_back(Key(rand_key));
      keys.emplace_back(key_str.back());
      if (use_txn) {
        if (!shared->AllowsOverwrite(rand_key) &&
            shared->Exists(column_family, rand_key)) {
          // Just do read your write checks for keys that allow overwrites.
          ryw_expected_values.emplace_back(std::nullopt);
          continue;
        }
        // With a 1 in 10 probability, insert the just added key in the batch
        // into the transaction. This will create an overlap with the MultiGet
        // keys and exercise some corner cases in the code
        if (thread->rand.OneIn(10)) {
          int op = thread->rand.Uniform(2);
          Status s;
          assert(txn);
          switch (op) {
            case 0:
            case 1: {
              ExpectedValue put_value;
              put_value.Put(false /* pending */);
              ryw_expected_values.emplace_back(put_value);
              char value[100];
              size_t sz =
                  GenerateValue(put_value.GetValueBase(), value, sizeof(value));
              Slice v(value, sz);
              if (op == 0) {
                s = txn->Put(cfh, keys.back(), v);
              } else {
                s = txn->Merge(cfh, keys.back(), v);
              }
              break;
            }
            case 2: {
              ExpectedValue delete_value;
              delete_value.Delete(false /* pending */);
              ryw_expected_values.emplace_back(delete_value);
              s = txn->Delete(cfh, keys.back());
              break;
            }
            default:
              assert(false);
          }
          if (!s.ok()) {
            fprintf(stderr, "Transaction put error: %s\n",
                    s.ToString().c_str());
            thread->shared->SafeTerminate();
          }
        } else {
          ryw_expected_values.emplace_back(std::nullopt);
        }
      }
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
      assert(txn);
      txn->MultiGet(readoptionscopy, cfh, num_keys, keys.data(), values.data(),
                    statuses.data());
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

    auto ryw_check =
        [](const Slice& key, const PinnableSlice& value, const Status& s,
           const std::optional<ExpectedValue>& ryw_expected_value) -> bool {
      if (!ryw_expected_value.has_value()) {
        return true;
      }
      const ExpectedValue& expected = ryw_expected_value.value();
      char expected_value[100];
      if (s.ok() &&
          ExpectedValueHelper::MustHaveNotExisted(expected, expected)) {
        fprintf(stderr,
                "MultiGet returned value different from what was "
                "written for key %s\n",
                key.ToString(true).c_str());
        fprintf(stderr,
                "MultiGet returned ok, transaction has non-committed "
                "delete.\n");
        return false;
      } else if (s.IsNotFound() &&
                 ExpectedValueHelper::MustHaveExisted(expected, expected)) {
        fprintf(stderr,
                "MultiGet returned value different from what was "
                "written for key %s\n",
                key.ToString(true).c_str());
        fprintf(stderr,
                "MultiGet returned not found, transaction has "
                "non-committed value.\n");
        return false;
      } else if (s.ok() &&
                 ExpectedValueHelper::MustHaveExisted(expected, expected)) {
        Slice from_txn_slice(value);
        size_t sz = GenerateValue(expected.GetValueBase(), expected_value,
                                  sizeof(expected_value));
        Slice expected_value_slice(expected_value, sz);
        if (expected_value_slice.compare(from_txn_slice) == 0) {
          return true;
        }
        fprintf(stderr,
                "MultiGet returned value different from what was "
                "written for key %s\n",
                key.ToString(true /* hex */).c_str());
        fprintf(stderr, "MultiGet returned value %s\n",
                from_txn_slice.ToString(true /* hex */).c_str());
        fprintf(stderr, "Transaction has non-committed value %s\n",
                expected_value_slice.ToString(true /* hex */).c_str());
        return false;
      }
      return true;
    };

    auto check_multiget =
        [&](const Slice& key, const PinnableSlice& expected_value,
            const Status& s,
            const std::optional<ExpectedValue>& ryw_expected_value) -> bool {
      bool is_consistent = true;
      bool is_ryw_correct = true;
      // Only do the consistency check if no error was injected and
      // MultiGet didn't return an unexpected error. If test does not use
      // transaction, the consistency check for each key included check results
      // from db `Get` and db `MultiGet` are consistent.
      // If test use transaction, after consistency check, also do a read your
      // own write check.
      if (do_consistency_check && !error_count && (s.ok() || s.IsNotFound())) {
        Status tmp_s;
        std::string value;

        if (use_txn) {
          assert(txn);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_GET);
          tmp_s = txn->Get(readoptionscopy, cfh, key, &value);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_MULTIGET);
        } else {
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_GET);
          tmp_s = db_->Get(readoptionscopy, cfh, key, &value);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_MULTIGET);
        }
        if (!tmp_s.ok() && !tmp_s.IsNotFound()) {
          fprintf(stderr, "Get error: %s\n", s.ToString().c_str());
          is_consistent = false;
        } else if (!s.ok() && tmp_s.ok()) {
          fprintf(stderr, "MultiGet returned different results with key %s\n",
                  key.ToString(true).c_str());
          fprintf(stderr, "Get returned ok, MultiGet returned not found\n");
          is_consistent = false;
        } else if (s.ok() && tmp_s.IsNotFound()) {
          fprintf(stderr, "MultiGet returned different results with key %s\n",
                  key.ToString(true).c_str());
          fprintf(stderr, "MultiGet returned ok, Get returned not found\n");
          is_consistent = false;
        } else if (s.ok() && value != expected_value.ToString()) {
          fprintf(stderr, "MultiGet returned different results with key %s\n",
                  key.ToString(true).c_str());
          fprintf(stderr, "MultiGet returned value %s\n",
                  expected_value.ToString(true).c_str());
          fprintf(stderr, "Get returned value %s\n",
                  Slice(value).ToString(true /* hex */).c_str());
          is_consistent = false;
        }
      }

      // If test uses transaction, continue to do a read your own write check.
      if (is_consistent && use_txn) {
        is_ryw_correct = ryw_check(key, expected_value, s, ryw_expected_value);
      }

      if (!is_consistent) {
        fprintf(stderr, "TestMultiGet error: is_consistent is false\n");
        thread->stats.AddErrors(1);
        // Fail fast to preserve the DB state
        thread->shared->SetVerificationFailure();
        return false;
      } else if (!is_ryw_correct) {
        fprintf(stderr, "TestMultiGet error: is_ryw_correct is false\n");
        thread->stats.AddErrors(1);
        // Fail fast to preserve the DB state
        thread->shared->SetVerificationFailure();
        return false;
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
      return true;
    };

    size_t num_of_keys = keys.size();
    assert(values.size() == num_of_keys);
    assert(statuses.size() == num_of_keys);
    for (size_t i = 0; i < num_of_keys; ++i) {
      bool check_result = true;
      if (use_txn) {
        assert(ryw_expected_values.size() == num_of_keys);
        check_result = check_multiget(keys[i], values[i], statuses[i],
                                      ryw_expected_values[i]);
      } else {
        check_result = check_multiget(keys[i], values[i], statuses[i],
                                      std::nullopt /* ryw_expected_value */);
      }
      if (!check_result) {
        break;
      }
    }

    if (readoptionscopy.snapshot) {
      db_->ReleaseSnapshot(readoptionscopy.snapshot);
    }
    if (use_txn) {
      txn->Rollback().PermitUncheckedError();
    }
    return statuses;
  }

  void TestGetEntity(ThreadState* thread, const ReadOptions& read_opts,
                     const std::vector<int>& rand_column_families,
                     const std::vector<int64_t>& rand_keys) override {
    if (fault_fs_guard) {
      fault_fs_guard->EnableErrorInjection();
      SharedState::ignore_read_error = false;
    }

    assert(thread);

    SharedState* const shared = thread->shared;
    assert(shared);

    assert(!rand_column_families.empty());
    assert(!rand_keys.empty());

    std::unique_ptr<MutexLock> lock(new MutexLock(
        shared->GetMutexForKey(rand_column_families[0], rand_keys[0])));

    assert(rand_column_families[0] >= 0);
    assert(rand_column_families[0] < static_cast<int>(column_families_.size()));

    ColumnFamilyHandle* const cfh = column_families_[rand_column_families[0]];
    assert(cfh);

    const std::string key = Key(rand_keys[0]);

    PinnableWideColumns from_db;

    const Status s = db_->GetEntity(read_opts, cfh, key, &from_db);

    int error_count = 0;

    if (fault_fs_guard) {
      error_count = fault_fs_guard->GetAndResetErrorCount();
    }

    if (s.ok()) {
      if (fault_fs_guard) {
        if (error_count && !SharedState::ignore_read_error) {
          // Grab mutex so multiple threads don't try to print the
          // stack trace at the same time
          MutexLock l(shared->GetMutex());
          fprintf(stderr, "Didn't get expected error from GetEntity\n");
          fprintf(stderr, "Call stack that injected the fault\n");
          fault_fs_guard->PrintFaultBacktrace();
          std::terminate();
        }
      }

      thread->stats.AddGets(1, 1);

      if (!FLAGS_skip_verifydb) {
        const WideColumns& columns = from_db.columns();
        ExpectedValue expected =
            shared->Get(rand_column_families[0], rand_keys[0]);
        if (!VerifyWideColumns(columns)) {
          shared->SetVerificationFailure();
          fprintf(stderr,
                  "error : inconsistent columns returned by GetEntity for key "
                  "%s: %s\n",
                  StringToHex(key).c_str(), WideColumnsToHex(columns).c_str());
        } else if (ExpectedValueHelper::MustHaveNotExisted(expected,
                                                           expected)) {
          shared->SetVerificationFailure();
          fprintf(
              stderr,
              "error : inconsistent values for key %s: GetEntity returns %s, "
              "expected state does not have the key.\n",
              StringToHex(key).c_str(), WideColumnsToHex(columns).c_str());
        }
      }
    } else if (s.IsNotFound()) {
      thread->stats.AddGets(1, 0);

      if (!FLAGS_skip_verifydb) {
        ExpectedValue expected =
            shared->Get(rand_column_families[0], rand_keys[0]);
        if (ExpectedValueHelper::MustHaveExisted(expected, expected)) {
          shared->SetVerificationFailure();
          fprintf(stderr,
                  "error : inconsistent values for key %s: expected state has "
                  "the key, GetEntity returns NotFound.\n",
                  StringToHex(key).c_str());
        }
      }
    } else {
      if (error_count == 0) {
        thread->stats.AddErrors(1);
      } else {
        thread->stats.AddVerifiedErrors(1);
      }
    }

    if (fault_fs_guard) {
      fault_fs_guard->DisableErrorInjection();
    }
  }

  void TestMultiGetEntity(ThreadState* thread, const ReadOptions& read_opts,
                          const std::vector<int>& rand_column_families,
                          const std::vector<int64_t>& rand_keys) override {
    assert(thread);

    ManagedSnapshot snapshot_guard(db_);

    ReadOptions read_opts_copy(read_opts);
    read_opts_copy.snapshot = snapshot_guard.snapshot();

    assert(!rand_column_families.empty());
    assert(rand_column_families[0] >= 0);
    assert(rand_column_families[0] < static_cast<int>(column_families_.size()));

    ColumnFamilyHandle* const cfh = column_families_[rand_column_families[0]];
    assert(cfh);

    assert(!rand_keys.empty());

    const size_t num_keys = rand_keys.size();

    std::vector<std::string> keys(num_keys);
    std::vector<Slice> key_slices(num_keys);

    for (size_t i = 0; i < num_keys; ++i) {
      keys[i] = Key(rand_keys[i]);
      key_slices[i] = keys[i];
    }

    std::vector<PinnableWideColumns> results(num_keys);
    std::vector<Status> statuses(num_keys);

    if (fault_fs_guard) {
      fault_fs_guard->EnableErrorInjection();
      SharedState::ignore_read_error = false;
    }

    db_->MultiGetEntity(read_opts_copy, cfh, num_keys, key_slices.data(),
                        results.data(), statuses.data());

    int error_count = 0;

    if (fault_fs_guard) {
      error_count = fault_fs_guard->GetAndResetErrorCount();

      if (error_count && !SharedState::ignore_read_error) {
        int stat_nok = 0;
        for (const auto& s : statuses) {
          if (!s.ok() && !s.IsNotFound()) {
            stat_nok++;
          }
        }

        if (stat_nok < error_count) {
          // Grab mutex so multiple threads don't try to print the
          // stack trace at the same time
          assert(thread->shared);
          MutexLock l(thread->shared->GetMutex());

          fprintf(stderr, "Didn't get expected error from MultiGetEntity\n");
          fprintf(stderr, "num_keys %zu Expected %d errors, seen %d\n",
                  num_keys, error_count, stat_nok);
          fprintf(stderr, "Call stack that injected the fault\n");
          fault_fs_guard->PrintFaultBacktrace();
          std::terminate();
        }
      }

      fault_fs_guard->DisableErrorInjection();
    }

    const bool check_get_entity = !error_count && thread->rand.OneIn(4);

    for (size_t i = 0; i < num_keys; ++i) {
      const Status& s = statuses[i];

      bool is_consistent = true;

      if (s.ok() && !VerifyWideColumns(results[i].columns())) {
        fprintf(
            stderr,
            "error : inconsistent columns returned by MultiGetEntity for key "
            "%s: %s\n",
            StringToHex(keys[i]).c_str(),
            WideColumnsToHex(results[i].columns()).c_str());
        is_consistent = false;
      } else if (check_get_entity && (s.ok() || s.IsNotFound())) {
        PinnableWideColumns cmp_result;
        ThreadStatusUtil::SetThreadOperation(
            ThreadStatus::OperationType::OP_GETENTITY);
        const Status cmp_s =
            db_->GetEntity(read_opts_copy, cfh, key_slices[i], &cmp_result);

        if (!cmp_s.ok() && !cmp_s.IsNotFound()) {
          fprintf(stderr, "GetEntity error: %s\n", cmp_s.ToString().c_str());
          is_consistent = false;
        } else if (cmp_s.IsNotFound()) {
          if (s.ok()) {
            fprintf(stderr,
                    "Inconsistent results for key %s: MultiGetEntity returned "
                    "ok, GetEntity returned not found\n",
                    StringToHex(keys[i]).c_str());
            is_consistent = false;
          }
        } else {
          assert(cmp_s.ok());

          if (s.IsNotFound()) {
            fprintf(stderr,
                    "Inconsistent results for key %s: MultiGetEntity returned "
                    "not found, GetEntity returned ok\n",
                    StringToHex(keys[i]).c_str());
            is_consistent = false;
          } else {
            assert(s.ok());

            if (results[i] != cmp_result) {
              fprintf(
                  stderr,
                  "Inconsistent results for key %s: MultiGetEntity returned "
                  "%s, GetEntity returned %s\n",
                  StringToHex(keys[i]).c_str(),
                  WideColumnsToHex(results[i].columns()).c_str(),
                  WideColumnsToHex(cmp_result.columns()).c_str());
              is_consistent = false;
            }
          }
        }
      }

      if (!is_consistent) {
        fprintf(stderr,
                "TestMultiGetEntity error: results are not consistent\n");
        thread->stats.AddErrors(1);
        // Fail fast to preserve the DB state
        thread->shared->SetVerificationFailure();
        break;
      } else if (s.ok()) {
        thread->stats.AddGets(1, 1);
      } else if (s.IsNotFound()) {
        thread->stats.AddGets(1, 0);
      } else {
        if (error_count == 0) {
          fprintf(stderr, "MultiGetEntity error: %s\n", s.ToString().c_str());
          thread->stats.AddErrors(1);
        } else {
          thread->stats.AddVerifiedErrors(1);
        }
      }
    }
  }

  Status TestPrefixScan(ThreadState* thread, const ReadOptions& read_opts,
                        const std::vector<int>& rand_column_families,
                        const std::vector<int64_t>& rand_keys) override {
    assert(!rand_column_families.empty());
    assert(!rand_keys.empty());

    ColumnFamilyHandle* const cfh = column_families_[rand_column_families[0]];
    assert(cfh);

    const std::string key = Key(rand_keys[0]);
    const Slice prefix(key.data(), FLAGS_prefix_size);

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

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro_copy, cfh));

    uint64_t count = 0;
    Status s;

    if (fault_fs_guard) {
      fault_fs_guard->EnableErrorInjection();
      SharedState::ignore_read_error = false;
    }

    for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix);
         iter->Next()) {
      ++count;

      // When iter_start_ts is set, iterator exposes internal keys, including
      // tombstones; however, we want to perform column validation only for
      // value-like types.
      if (ro_copy.iter_start_ts) {
        const ValueType value_type = ExtractValueType(iter->key());
        if (value_type != kTypeValue && value_type != kTypeBlobIndex &&
            value_type != kTypeWideColumnEntity) {
          continue;
        }
      }

      if (!VerifyWideColumns(iter->value(), iter->columns())) {
        s = Status::Corruption("Value and columns inconsistent",
                               DebugString(iter->value(), iter->columns()));
        break;
      }
    }

    if (ro_copy.iter_start_ts == nullptr) {
      assert(count <= GetPrefixKeyCount(prefix.ToString(), upper_bound));
    }

    if (s.ok()) {
      s = iter->status();
    }

    uint64_t error_count = 0;
    if (fault_fs_guard) {
      error_count = fault_fs_guard->GetAndResetErrorCount();
    }
    if (!s.ok() && (!fault_fs_guard || (fault_fs_guard && !error_count))) {
      fprintf(stderr, "TestPrefixScan error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);

      return s;
    }

    if (fault_fs_guard) {
      fault_fs_guard->DisableErrorInjection();
    }
    thread->stats.AddPrefixes(1, count);

    return Status::OK();
  }

  Status TestPut(ThreadState* thread, WriteOptions& write_opts,
                 const ReadOptions& read_opts,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys,
                 char (&value)[100]) override {
    assert(!rand_column_families.empty());
    assert(!rand_keys.empty());

    auto shared = thread->shared;
    assert(shared);

    const int64_t max_key = shared->GetMaxKey();

    int64_t rand_key = rand_keys[0];
    int rand_column_family = rand_column_families[0];
    std::string write_ts;

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
        write_ts = GetNowNanos();
      }
    }

    if (write_ts.empty() && FLAGS_user_timestamp_size) {
      write_ts = GetNowNanos();
    }

    const std::string k = Key(rand_key);

    ColumnFamilyHandle* const cfh = column_families_[rand_column_family];
    assert(cfh);

    if (FLAGS_verify_before_write) {
      std::string from_db;
      Status s = db_->Get(read_opts, cfh, k, &from_db);
      if (!VerifyOrSyncValue(rand_column_family, rand_key, read_opts, shared,
                             /* msg_prefix */ "Pre-Put Get verification",
                             from_db, s)) {
        return s;
      }
    }

    PendingExpectedValue pending_expected_value =
        shared->PreparePut(rand_column_family, rand_key);
    const uint32_t value_base = pending_expected_value.GetFinalValueBase();
    const size_t sz = GenerateValue(value_base, value, sizeof(value));
    const Slice v(value, sz);

    Status s;

    if (FLAGS_use_put_entity_one_in > 0 &&
        (value_base % FLAGS_use_put_entity_one_in) == 0) {
      s = db_->PutEntity(write_opts, cfh, k,
                         GenerateWideColumns(value_base, v));
    } else if (FLAGS_use_merge) {
      if (!FLAGS_use_txn) {
        if (FLAGS_user_timestamp_size == 0) {
          s = db_->Merge(write_opts, cfh, k, v);
        } else {
          s = db_->Merge(write_opts, cfh, k, write_ts, v);
        }
      } else {
        s = ExecuteTransaction(write_opts, thread, [&](Transaction& txn) {
          return txn.Merge(cfh, k, v);
        });
      }
    } else {
      if (!FLAGS_use_txn) {
        if (FLAGS_user_timestamp_size == 0) {
          s = db_->Put(write_opts, cfh, k, v);
        } else {
          s = db_->Put(write_opts, cfh, k, write_ts, v);
        }
      } else {
        s = ExecuteTransaction(write_opts, thread, [&](Transaction& txn) {
          return txn.Put(cfh, k, v);
        });
      }
    }

    if (!s.ok()) {
      if (FLAGS_inject_error_severity >= 2) {
        if (!is_db_stopped_ && s.severity() >= Status::Severity::kFatalError) {
          is_db_stopped_ = true;
        } else if (!is_db_stopped_ ||
                   s.severity() < Status::Severity::kFatalError) {
          fprintf(stderr, "put or merge error: %s\n", s.ToString().c_str());
          thread->shared->SafeTerminate();
        }
      } else {
        fprintf(stderr, "put or merge error: %s\n", s.ToString().c_str());
        thread->shared->SafeTerminate();
      }
    }
    pending_expected_value.Commit();
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
      PendingExpectedValue pending_expected_value =
          shared->PrepareDelete(rand_column_family, rand_key);
      if (!FLAGS_use_txn) {
        if (FLAGS_user_timestamp_size == 0) {
          s = db_->Delete(write_opts, cfh, key);
        } else {
          s = db_->Delete(write_opts, cfh, key, write_ts);
        }
      } else {
        s = ExecuteTransaction(write_opts, thread, [&](Transaction& txn) {
          return txn.Delete(cfh, key);
        });
      }

      if (!s.ok()) {
        if (FLAGS_inject_error_severity >= 2) {
          if (!is_db_stopped_ &&
              s.severity() >= Status::Severity::kFatalError) {
            is_db_stopped_ = true;
          } else if (!is_db_stopped_ ||
                     s.severity() < Status::Severity::kFatalError) {
            fprintf(stderr, "delete error: %s\n", s.ToString().c_str());
            thread->shared->SafeTerminate();
          }
        } else {
          fprintf(stderr, "delete error: %s\n", s.ToString().c_str());
          thread->shared->SafeTerminate();
        }
      }
      pending_expected_value.Commit();
      thread->stats.AddDeletes(1);
    } else {
      PendingExpectedValue pending_expected_value =
          shared->PrepareSingleDelete(rand_column_family, rand_key);
      if (!FLAGS_use_txn) {
        if (FLAGS_user_timestamp_size == 0) {
          s = db_->SingleDelete(write_opts, cfh, key);
        } else {
          s = db_->SingleDelete(write_opts, cfh, key, write_ts);
        }
      } else {
        s = ExecuteTransaction(write_opts, thread, [&](Transaction& txn) {
          return txn.SingleDelete(cfh, key);
        });
      }

      if (!s.ok()) {
        if (FLAGS_inject_error_severity >= 2) {
          if (!is_db_stopped_ &&
              s.severity() >= Status::Severity::kFatalError) {
            is_db_stopped_ = true;
          } else if (!is_db_stopped_ ||
                     s.severity() < Status::Severity::kFatalError) {
            fprintf(stderr, "single delete error: %s\n", s.ToString().c_str());
            thread->shared->SafeTerminate();
          }
        } else {
          fprintf(stderr, "single delete error: %s\n", s.ToString().c_str());
          thread->shared->SafeTerminate();
        }
      }
      pending_expected_value.Commit();
      thread->stats.AddSingleDeletes(1);
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
    std::vector<PendingExpectedValue> pending_expected_values =
        shared->PrepareDeleteRange(rand_column_family, rand_key,
                                   rand_key + FLAGS_range_deletion_width);
    const int covered = static_cast<int>(pending_expected_values.size());
    std::string keystr = Key(rand_key);
    Slice key = keystr;
    auto cfh = column_families_[rand_column_family];
    std::string end_keystr = Key(rand_key + FLAGS_range_deletion_width);
    Slice end_key = end_keystr;
    std::string write_ts_str;
    Slice write_ts;
    Status s;
    if (FLAGS_user_timestamp_size) {
      write_ts_str = GetNowNanos();
      write_ts = write_ts_str;
      s = db_->DeleteRange(write_opts, cfh, key, end_key, write_ts);
    } else {
      s = db_->DeleteRange(write_opts, cfh, key, end_key);
    }
    if (!s.ok()) {
      if (FLAGS_inject_error_severity >= 2) {
        if (!is_db_stopped_ && s.severity() >= Status::Severity::kFatalError) {
          is_db_stopped_ = true;
        } else if (!is_db_stopped_ ||
                   s.severity() < Status::Severity::kFatalError) {
          fprintf(stderr, "delete range error: %s\n", s.ToString().c_str());
          thread->shared->SafeTerminate();
        }
      } else {
        fprintf(stderr, "delete range error: %s\n", s.ToString().c_str());
        thread->shared->SafeTerminate();
      }
    }
    for (PendingExpectedValue& pending_expected_value :
         pending_expected_values) {
      pending_expected_value.Commit();
    }
    thread->stats.AddRangeDeletions(1);
    thread->stats.AddCoveredByRangeDeletions(covered);
    return s;
  }

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
    std::vector<PendingExpectedValue> pending_expected_values;
    pending_expected_values.reserve(FLAGS_ingest_external_file_width);
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
        // We could alternatively include `key` that is deleted.
        continue;
      }
      keys.push_back(key);

      PendingExpectedValue pending_expected_value =
          shared->PreparePut(column_family, key);
      const uint32_t value_base = pending_expected_value.GetFinalValueBase();
      values.push_back(value_base);
      pending_expected_values.push_back(pending_expected_value);

      char value[100];
      auto key_str = Key(key);
      const size_t value_len = GenerateValue(value_base, value, sizeof(value));
      const Slice k(key_str);
      const Slice v(value, value_len);

      if (FLAGS_use_put_entity_one_in > 0 &&
          (value_base % FLAGS_use_put_entity_one_in) == 0) {
        WideColumns columns = GenerateWideColumns(value_base, v);
        s = sst_file_writer.PutEntity(k, columns);
      } else {
        s = sst_file_writer.Put(k, v);
      }
    }

    if (s.ok() && keys.empty()) {
      return;
    }

    if (s.ok()) {
      s = sst_file_writer.Finish();
    }
    if (s.ok()) {
      s = db_->IngestExternalFile(column_families_[column_family],
                                  {sst_filename}, IngestExternalFileOptions());
    }
    if (!s.ok()) {
      if (!s.IsIOError() || !std::strstr(s.getState(), "injected")) {
        fprintf(stderr, "file ingestion error: %s\n", s.ToString().c_str());
        thread->shared->SafeTerminate();
      } else {
        fprintf(stdout, "file ingestion error: %s\n", s.ToString().c_str());
      }
    } else {
      for (size_t i = 0; i < pending_expected_values.size(); ++i) {
        pending_expected_values[i].Commit();
      }
    }
  }

  // Given a key K, this creates an iterator which scans the range
  // [K, K + FLAGS_num_iterations) forward and backward.
  // Then does a random sequence of Next/Prev operations.
  Status TestIterateAgainstExpected(
      ThreadState* thread, const ReadOptions& read_opts,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) override {
    assert(thread);
    assert(!rand_column_families.empty());
    assert(!rand_keys.empty());

    auto shared = thread->shared;
    assert(shared);

    int64_t max_key = shared->GetMaxKey();

    const int64_t num_iter = static_cast<int64_t>(FLAGS_num_iterations);

    int64_t lb = rand_keys[0];
    if (lb > max_key - num_iter) {
      lb = thread->rand.Next() % (max_key - num_iter + 1);
    }

    const int64_t ub = lb + num_iter;

    const int rand_column_family = rand_column_families[0];

    // Testing parallel read and write to the same key with user timestamp
    // is not currently supported
    std::vector<std::unique_ptr<MutexLock>> range_locks;
    if (FLAGS_user_timestamp_size > 0) {
      range_locks = shared->GetLocksForKeyRange(rand_column_family, lb, ub);
    }

    ReadOptions ro(read_opts);
    if (FLAGS_prefix_size > 0) {
      ro.total_order_seek = true;
    }

    std::string read_ts_str;
    Slice read_ts;
    if (FLAGS_user_timestamp_size > 0) {
      read_ts_str = GetNowNanos();
      read_ts = read_ts_str;
      ro.timestamp = &read_ts;
    }

    std::string max_key_str;
    Slice max_key_slice;
    if (!FLAGS_destroy_db_initially) {
      max_key_str = Key(max_key);
      max_key_slice = max_key_str;
      // to restrict iterator from reading keys written in batched_op_stress
      // that do not have expected state updated and may not be parseable by
      // GetIntVal().
      ro.iterate_upper_bound = &max_key_slice;
    }

    ColumnFamilyHandle* const cfh = column_families_[rand_column_family];
    assert(cfh);

    const std::size_t expected_values_size = static_cast<std::size_t>(ub - lb);
    std::vector<ExpectedValue> pre_read_expected_values;
    std::vector<ExpectedValue> post_read_expected_values;

    for (int64_t i = 0; i < static_cast<int64_t>(expected_values_size); ++i) {
      pre_read_expected_values.push_back(
          shared->Get(rand_column_family, i + lb));
    }
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro, cfh));
    for (int64_t i = 0; i < static_cast<int64_t>(expected_values_size); ++i) {
      post_read_expected_values.push_back(
          shared->Get(rand_column_family, i + lb));
    }

    assert(pre_read_expected_values.size() == expected_values_size &&
           pre_read_expected_values.size() == post_read_expected_values.size());

    std::string op_logs;

    auto check_columns = [&]() {
      assert(iter);
      assert(iter->Valid());

      if (!VerifyWideColumns(iter->value(), iter->columns())) {
        shared->SetVerificationFailure();

        fprintf(stderr,
                "Verification failed for key %s: "
                "Value and columns inconsistent: value: %s, columns: %s\n",
                Slice(iter->key()).ToString(/* hex */ true).c_str(),
                iter->value().ToString(/* hex */ true).c_str(),
                WideColumnsToHex(iter->columns()).c_str());
        fprintf(stderr, "Column family: %s, op_logs: %s\n",
                cfh->GetName().c_str(), op_logs.c_str());

        thread->stats.AddErrors(1);

        return false;
      }

      return true;
    };

    auto check_no_key_in_range = [&](int64_t start, int64_t end) {
      assert(start <= end);
      for (auto j = std::max(start, lb); j < std::min(end, ub); ++j) {
        std::size_t index = static_cast<std::size_t>(j - lb);
        assert(index < pre_read_expected_values.size() &&
               index < post_read_expected_values.size());
        const ExpectedValue pre_read_expected_value =
            pre_read_expected_values[index];
        const ExpectedValue post_read_expected_value =
            post_read_expected_values[index];
        if (ExpectedValueHelper::MustHaveExisted(pre_read_expected_value,
                                                 post_read_expected_value)) {
          // Fail fast to preserve the DB state.
          thread->shared->SetVerificationFailure();
          if (iter->Valid()) {
            fprintf(stderr,
                    "Verification failed. Expected state has key %s, iterator "
                    "is at key %s\n",
                    Slice(Key(j)).ToString(true).c_str(),
                    iter->key().ToString(true).c_str());
          } else {
            fprintf(stderr,
                    "Verification failed. Expected state has key %s, iterator "
                    "is invalid\n",
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
    iter->Seek(key_str);

    op_logs += "S " + Slice(key_str).ToString(true) + " ";

    uint64_t curr = 0;
    while (true) {
      assert(last_key < ub);
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
        if (!check_no_key_in_range(last_key + 1, ub)) {
          return Status::OK();
        }
        break;
      }

      if (!check_columns()) {
        return Status::OK();
      }

      // iter is valid, the range (last_key, current key) was skipped
      GetIntVal(iter->key().ToString(), &curr);
      if (static_cast<int64_t>(curr) <= last_key) {
        thread->shared->SetVerificationFailure();
        fprintf(stderr,
                "TestIterateAgainstExpected failed: found unexpectedly small "
                "key\n");
        fprintf(stderr, "Column family: %s, op_logs: %s\n",
                cfh->GetName().c_str(), op_logs.c_str());
        fprintf(stderr, "Last op found key: %s, expected at least: %s\n",
                Slice(Key(curr)).ToString(true).c_str(),
                Slice(Key(last_key + 1)).ToString(true).c_str());
        thread->stats.AddErrors(1);
        return Status::OK();
      }
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
    iter->SeekForPrev(key_str);

    op_logs += " SFP " + Slice(key_str).ToString(true) + " ";

    last_key = ub;
    while (true) {
      assert(lb < last_key);
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

      if (!check_columns()) {
        return Status::OK();
      }

      // the range (current key, last key) was skipped
      GetIntVal(iter->key().ToString(), &curr);
      if (last_key <= static_cast<int64_t>(curr)) {
        thread->shared->SetVerificationFailure();
        fprintf(stderr,
                "TestIterateAgainstExpected failed: found unexpectedly large "
                "key\n");
        fprintf(stderr, "Column family: %s, op_logs: %s\n",
                cfh->GetName().c_str(), op_logs.c_str());
        fprintf(stderr, "Last op found key: %s, expected at most: %s\n",
                Slice(Key(curr)).ToString(true).c_str(),
                Slice(Key(last_key - 1)).ToString(true).c_str());
        thread->stats.AddErrors(1);
        return Status::OK();
      }
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

    // Write-prepared and Write-unprepared do not support Refresh() yet.
    if (!(FLAGS_use_txn && FLAGS_txn_write_policy != 0) &&
        thread->rand.OneIn(2)) {
      pre_read_expected_values.clear();
      post_read_expected_values.clear();
      // Refresh after forward/backward scan to allow higher chance of SV
      // change.
      for (int64_t i = 0; i < static_cast<int64_t>(expected_values_size); ++i) {
        pre_read_expected_values.push_back(
            shared->Get(rand_column_family, i + lb));
      }
      Status rs = iter->Refresh();
      assert(rs.ok());
      op_logs += "Refresh ";
      for (int64_t i = 0; i < static_cast<int64_t>(expected_values_size); ++i) {
        post_read_expected_values.push_back(
            shared->Get(rand_column_family, i + lb));
      }

      assert(pre_read_expected_values.size() == expected_values_size &&
             pre_read_expected_values.size() ==
                 post_read_expected_values.size());
    }

    // start from middle of [lb, ub) otherwise it is easy to iterate out of
    // locked range
    const int64_t mid = lb + num_iter / 2;

    key_str = Key(mid);
    const Slice key(key_str);

    if (thread->rand.OneIn(2)) {
      iter->Seek(key);
      op_logs += " S " + key.ToString(true) + " ";
      if (!iter->Valid() && iter->status().ok()) {
        if (!check_no_key_in_range(mid, ub)) {
          return Status::OK();
        }
      } else if (iter->Valid()) {
        GetIntVal(iter->key().ToString(), &curr);
        if (static_cast<int64_t>(curr) < mid) {
          thread->shared->SetVerificationFailure();
          fprintf(stderr,
                  "TestIterateAgainstExpected failed: found unexpectedly small "
                  "key\n");
          fprintf(stderr, "Column family: %s, op_logs: %s\n",
                  cfh->GetName().c_str(), op_logs.c_str());
          fprintf(stderr, "Last op found key: %s, expected at least: %s\n",
                  Slice(Key(curr)).ToString(true).c_str(),
                  Slice(Key(mid)).ToString(true).c_str());
          thread->stats.AddErrors(1);
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
      } else if (iter->Valid()) {
        GetIntVal(iter->key().ToString(), &curr);
        if (mid < static_cast<int64_t>(curr)) {
          thread->shared->SetVerificationFailure();
          fprintf(stderr,
                  "TestIterateAgainstExpected failed: found unexpectedly large "
                  "key\n");
          fprintf(stderr, "Column family: %s, op_logs: %s\n",
                  cfh->GetName().c_str(), op_logs.c_str());
          fprintf(stderr, "Last op found key: %s, expected at most: %s\n",
                  Slice(Key(curr)).ToString(true).c_str(),
                  Slice(Key(mid)).ToString(true).c_str());
          thread->stats.AddErrors(1);
          return Status::OK();
        }
      }
    }

    for (int64_t i = 0; i < num_iter && iter->Valid(); ++i) {
      if (!check_columns()) {
        return Status::OK();
      }

      GetIntVal(iter->key().ToString(), &curr);
      if (static_cast<int64_t>(curr) < lb) {
        iter->Next();
        op_logs += "N";
      } else if (static_cast<int64_t>(curr) >= ub) {
        iter->Prev();
        op_logs += "P";
      } else {
        const uint32_t value_base_from_db = GetValueBase(iter->value());
        std::size_t index = static_cast<std::size_t>(curr - lb);
        assert(index < pre_read_expected_values.size() &&
               index < post_read_expected_values.size());
        const ExpectedValue pre_read_expected_value =
            pre_read_expected_values[index];
        const ExpectedValue post_read_expected_value =
            post_read_expected_values[index];
        if (ExpectedValueHelper::MustHaveNotExisted(pre_read_expected_value,
                                                    post_read_expected_value) ||
            !ExpectedValueHelper::InExpectedValueBaseRange(
                value_base_from_db, pre_read_expected_value,
                post_read_expected_value)) {
          // Fail fast to preserve the DB state.
          thread->shared->SetVerificationFailure();
          fprintf(stderr,
                  "Verification failed: iterator has key %s, but expected "
                  "state does not.\n",
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
          uint64_t next = 0;
          GetIntVal(iter->key().ToString(), &next);
          if (next <= curr) {
            thread->shared->SetVerificationFailure();
            fprintf(stderr,
                    "TestIterateAgainstExpected failed: found unexpectedly "
                    "small key\n");
            fprintf(stderr, "Column family: %s, op_logs: %s\n",
                    cfh->GetName().c_str(), op_logs.c_str());
            fprintf(stderr, "Last op found key: %s, expected at least: %s\n",
                    Slice(Key(next)).ToString(true).c_str(),
                    Slice(Key(curr + 1)).ToString(true).c_str());
            thread->stats.AddErrors(1);
            return Status::OK();
          }
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
          uint64_t prev = 0;
          GetIntVal(iter->key().ToString(), &prev);
          if (curr <= prev) {
            thread->shared->SetVerificationFailure();
            fprintf(stderr,
                    "TestIterateAgainstExpected failed: found unexpectedly "
                    "large key\n");
            fprintf(stderr, "Column family: %s, op_logs: %s\n",
                    cfh->GetName().c_str(), op_logs.c_str());
            fprintf(stderr, "Last op found key: %s, expected at most: %s\n",
                    Slice(Key(prev)).ToString(true).c_str(),
                    Slice(Key(curr - 1)).ToString(true).c_str());
            thread->stats.AddErrors(1);
            return Status::OK();
          }
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
                         std::string msg_prefix, const Status& s) const {
    if (shared->HasVerificationFailedYet()) {
      return false;
    }
    const ExpectedValue expected_value = shared->Get(cf, key);

    if (expected_value.PendingWrite() || expected_value.PendingDelete()) {
      if (s.ok()) {
        // Value exists in db, update state to reflect that
        Slice slice(value_from_db);
        uint32_t value_base = GetValueBase(slice);
        shared->SyncPut(cf, key, value_base);
      } else if (s.IsNotFound()) {
        // Value doesn't exist in db, update state to reflect that
        shared->SyncDelete(cf, key);
      }
      return true;
    }

    // compare value_from_db with the value in the shared state
    if (s.ok()) {
      const Slice slice(value_from_db);
      const uint32_t value_base_from_db = GetValueBase(slice);
      if (ExpectedValueHelper::MustHaveNotExisted(expected_value,
                                                  expected_value)) {
        VerificationAbort(shared, msg_prefix + ": Unexpected value found", cf,
                          key, value_from_db, "");
        return false;
      }
      char expected_value_data[kValueMaxLen];
      size_t expected_value_data_size =
          GenerateValue(expected_value.GetValueBase(), expected_value_data,
                        sizeof(expected_value_data));
      if (!ExpectedValueHelper::InExpectedValueBaseRange(
              value_base_from_db, expected_value, expected_value)) {
        VerificationAbort(shared, msg_prefix + ": Unexpected value found", cf,
                          key, value_from_db,
                          Slice(expected_value_data, expected_value_data_size));
        return false;
      }
      // TODO: are the length/memcmp() checks repetitive?
      if (value_from_db.length() != expected_value_data_size) {
        VerificationAbort(shared,
                          msg_prefix + ": Length of value read is not equal",
                          cf, key, value_from_db,
                          Slice(expected_value_data, expected_value_data_size));
        return false;
      }
      if (memcmp(value_from_db.data(), expected_value_data,
                 expected_value_data_size) != 0) {
        VerificationAbort(shared,
                          msg_prefix + ": Contents of value read don't match",
                          cf, key, value_from_db,
                          Slice(expected_value_data, expected_value_data_size));
        return false;
      }
    } else if (s.IsNotFound()) {
      if (ExpectedValueHelper::MustHaveExisted(expected_value,
                                               expected_value)) {
        char expected_value_data[kValueMaxLen];
        size_t expected_value_data_size =
            GenerateValue(expected_value.GetValueBase(), expected_value_data,
                          sizeof(expected_value_data));
        VerificationAbort(
            shared, msg_prefix + ": Value not found: " + s.ToString(), cf, key,
            "", Slice(expected_value_data, expected_value_data_size));
        return false;
      }
    } else {
      assert(false);
    }
    return true;
  }

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
};

StressTest* CreateNonBatchedOpsStressTest() {
  return new NonBatchedOpsStressTest();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
