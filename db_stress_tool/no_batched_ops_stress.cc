//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"
#include "db_stress_tool/db_stress_listener.h"
#include "db_stress_tool/db_stress_shared_state.h"
#include "db_stress_tool/expected_state.h"
#include "rocksdb/status.h"
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

  void TestKeyMayExist(ThreadState* thread, const ReadOptions& read_opts,
                       const std::vector<int>& rand_column_families,
                       const std::vector<int64_t>& rand_keys) override {
    auto cfh = column_families_[rand_column_families[0]];
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    std::string ignore;
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
    bool key_may_exist = db_->KeyMayExist(read_opts_copy, cfh, key, &ignore);
    const ExpectedValue post_read_expected_value =
        thread->shared->Get(rand_column_families[0], rand_keys[0]);

    if (!key_may_exist && !FLAGS_skip_verifydb && !read_older_ts) {
      if (ExpectedValueHelper::MustHaveExisted(pre_read_expected_value,
                                               post_read_expected_value)) {
        thread->shared->SetVerificationFailure();
        fprintf(stderr,
                "error : inconsistent values for key %s: expected state has "
                "the key, TestKeyMayExist() returns false indicating the key "
                "must not exist.\n",
                key.ToString(true).c_str());
      }
    }
  }

  Status TestGet(ThreadState* thread, const ReadOptions& read_opts,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys) override {
    auto cfh = column_families_[rand_column_families[0]];
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    std::string from_db;

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

    if (fault_fs_guard) {
      fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
          FaultInjectionIOType::kRead);
      fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
          FaultInjectionIOType::kMetadataRead);
      SharedState::ignore_read_error = false;
    }

    const ExpectedValue pre_read_expected_value =
        thread->shared->Get(rand_column_families[0], rand_keys[0]);
    Status s = db_->Get(read_opts_copy, cfh, key, &from_db);
    const ExpectedValue post_read_expected_value =
        thread->shared->Get(rand_column_families[0], rand_keys[0]);

    int injected_error_count = 0;
    if (fault_fs_guard) {
      injected_error_count = GetMinInjectedErrorCount(
          fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
              FaultInjectionIOType::kRead),
          fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
              FaultInjectionIOType::kMetadataRead));
      if (!SharedState::ignore_read_error && injected_error_count > 0 &&
          (s.ok() || s.IsNotFound())) {
        // Grab mutex so multiple thread don't try to print the
        // stack trace at the same time
        MutexLock l(thread->shared->GetMutex());
        fprintf(stderr, "Didn't get expected error from Get\n");
        fprintf(stderr, "Callstack that injected the fault\n");
        fault_fs_guard->PrintInjectedThreadLocalErrorBacktrace(
            FaultInjectionIOType::kRead);
        fault_fs_guard->PrintInjectedThreadLocalErrorBacktrace(
            FaultInjectionIOType::kMetadataRead);
        std::terminate();
      }
    }

    if (s.ok()) {
      // found case
      thread->stats.AddGets(1, 1);
      // we only have the latest expected state
      if (!FLAGS_skip_verifydb && !read_older_ts) {
        if (ExpectedValueHelper::MustHaveNotExisted(pre_read_expected_value,
                                                    post_read_expected_value)) {
          thread->shared->SetVerificationFailure();
          fprintf(stderr,
                  "error : inconsistent values for key %s (%" PRIi64
                  "): Get returns %s, "
                  "but expected state is \"deleted\".\n",
                  key.ToString(true).c_str(), rand_keys[0],
                  StringToHex(from_db).c_str());
        }
        Slice from_db_slice(from_db);
        uint32_t value_base_from_db = GetValueBase(from_db_slice);
        if (!ExpectedValueHelper::InExpectedValueBaseRange(
                value_base_from_db, pre_read_expected_value,
                post_read_expected_value)) {
          thread->shared->SetVerificationFailure();
          fprintf(stderr,
                  "error : inconsistent values for key %s (%" PRIi64
                  "): Get returns %s with "
                  "value base %d that falls out of expected state's value base "
                  "range.\n",
                  key.ToString(true).c_str(), rand_keys[0],
                  StringToHex(from_db).c_str(), value_base_from_db);
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
                  "error : inconsistent values for key %s (%" PRIi64
                  "): expected state has "
                  "the key, Get() returns NotFound.\n",
                  key.ToString(true).c_str(), rand_keys[0]);
        }
      }
    } else if (injected_error_count == 0 || !IsErrorInjectedAndRetryable(s)) {
      thread->shared->SetVerificationFailure();
      fprintf(stderr, "error : Get() returns %s for key: %s (%" PRIi64 ").\n",
              s.ToString().c_str(), key.ToString(true).c_str(), rand_keys[0]);
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
    std::unordered_map<std::string, ExpectedValue> ryw_expected_values;

    SharedState* shared = thread->shared;
    assert(shared);

    int column_family = rand_column_families[0];
    ColumnFamilyHandle* cfh = column_families_[column_family];

    bool do_consistency_check = FLAGS_check_multiget_consistency;

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
      // TODO(hx235): test fault injection with MultiGet() with transactions
      if (fault_fs_guard) {
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }
      WriteOptions wo;
      if (FLAGS_rate_limit_auto_wal_flush) {
        wo.rate_limiter_priority = Env::IO_USER;
      }
      Status s = NewTxn(wo, thread, &txn);
      if (!s.ok()) {
        fprintf(stderr, "NewTxn error: %s\n", s.ToString().c_str());
        shared->SafeTerminate();
      }
    }
    for (size_t i = 0; i < num_keys; ++i) {
      uint64_t rand_key = rand_keys[i];
      key_str.emplace_back(Key(rand_key));
      keys.emplace_back(key_str.back());
      if (use_txn) {
        MaybeAddKeyToTxnForRYW(thread, column_family, rand_key, txn.get(),
                               ryw_expected_values);
      }
    }

    int injected_error_count = 0;

    if (!use_txn) {
      if (fault_fs_guard) {
        fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
            FaultInjectionIOType::kRead);
        fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
            FaultInjectionIOType::kMetadataRead);
        SharedState::ignore_read_error = false;
      }
      db_->MultiGet(readoptionscopy, cfh, num_keys, keys.data(), values.data(),
                    statuses.data());
      if (fault_fs_guard) {
        injected_error_count = GetMinInjectedErrorCount(
            fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
                FaultInjectionIOType::kRead),
            fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
                FaultInjectionIOType::kMetadataRead));

        if (injected_error_count > 0) {
          int stat_nok_nfound = 0;
          for (const auto& s : statuses) {
            if (!s.ok() && !s.IsNotFound()) {
              stat_nok_nfound++;
            }
          }
          if (!SharedState::ignore_read_error &&
              stat_nok_nfound < injected_error_count) {
            // Grab mutex so multiple thread don't try to print the
            // stack trace at the same time
            MutexLock l(shared->GetMutex());
            fprintf(stderr, "Didn't get expected error from MultiGet. \n");
            fprintf(stderr,
                    "num_keys %zu Expected %d errors, seen at least %d\n",
                    num_keys, injected_error_count, stat_nok_nfound);
            fprintf(stderr, "Callstack that injected the fault\n");
            fault_fs_guard->PrintInjectedThreadLocalErrorBacktrace(
                FaultInjectionIOType::kRead);
            fault_fs_guard->PrintInjectedThreadLocalErrorBacktrace(
                FaultInjectionIOType::kMetadataRead);
            std::terminate();
          }
        }
      }
    } else {
      assert(txn);
      txn->MultiGet(readoptionscopy, cfh, num_keys, keys.data(), values.data(),
                    statuses.data());
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
      //  Temporarily disable error injection for verification
      if (fault_fs_guard) {
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }

      bool check_multiget_res = true;
      bool is_consistent = true;
      bool is_ryw_correct = true;

      // If test does not use transaction, the consistency check for each key
      // included check results from db `Get` and db `MultiGet` are consistent.
      // If test use transaction, after consistency check, also do a read your
      // own write check.
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
        fprintf(stderr,
                "MultiGet(%d) returned different results with key %s. "
                "Snapshot Seq No: %" PRIu64 "\n",
                column_family, key.ToString(true).c_str(),
                readoptionscopy.snapshot->GetSequenceNumber());
        fprintf(stderr, "Get returned ok, MultiGet returned not found\n");
        is_consistent = false;
      } else if (s.ok() && tmp_s.IsNotFound()) {
        fprintf(stderr,
                "MultiGet(%d) returned different results with key %s. "
                "Snapshot Seq No: %" PRIu64 "\n",
                column_family, key.ToString(true).c_str(),
                readoptionscopy.snapshot->GetSequenceNumber());
        fprintf(stderr, "MultiGet returned ok, Get returned not found\n");
        is_consistent = false;
      } else if (s.ok() && value != expected_value.ToString()) {
        fprintf(stderr,
                "MultiGet(%d) returned different results with key %s. "
                "Snapshot Seq No: %" PRIu64 "\n",
                column_family, key.ToString(true).c_str(),
                readoptionscopy.snapshot->GetSequenceNumber());
        fprintf(stderr, "MultiGet returned value %s\n",
                expected_value.ToString(true).c_str());
        fprintf(stderr, "Get returned value %s\n",
                Slice(value).ToString(true /* hex */).c_str());
        is_consistent = false;
      }

      // If test uses transaction, continue to do a read your own write check.
      if (is_consistent && use_txn) {
        is_ryw_correct = ryw_check(key, expected_value, s, ryw_expected_value);
      }

      if (!is_consistent) {
        fprintf(stderr, "TestMultiGet error: is_consistent is false\n");
        thread->stats.AddErrors(1);
        check_multiget_res = false;
        // Fail fast to preserve the DB state
        shared->SetVerificationFailure();
      } else if (!is_ryw_correct) {
        fprintf(stderr, "TestMultiGet error: is_ryw_correct is false\n");
        thread->stats.AddErrors(1);
        check_multiget_res = false;
        // Fail fast to preserve the DB state
        shared->SetVerificationFailure();
      } else if (s.ok()) {
        // found case
        thread->stats.AddGets(1, 1);
      } else if (s.IsNotFound()) {
        // not found case
        thread->stats.AddGets(1, 0);
      } else if (s.IsMergeInProgress() && use_txn) {
        // With txn this is sometimes expected.
        thread->stats.AddGets(1, 1);
      } else if (injected_error_count == 0 || !IsErrorInjectedAndRetryable(s)) {
        fprintf(stderr, "MultiGet error: %s\n", s.ToString().c_str());
        thread->stats.AddErrors(1);
        shared->SetVerificationFailure();
      }

      // Enable back error injection disbled for checking results
      if (fault_fs_guard) {
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }
      return check_multiget_res;
    };

    // Consistency check
    if (do_consistency_check && injected_error_count == 0) {
      size_t num_of_keys = keys.size();
      assert(values.size() == num_of_keys);
      assert(statuses.size() == num_of_keys);
      for (size_t i = 0; i < num_of_keys; ++i) {
        bool check_result = true;
        if (use_txn) {
          std::optional<ExpectedValue> ryw_expected_value;

          const auto it = ryw_expected_values.find(key_str[i]);
          if (it != ryw_expected_values.end()) {
            ryw_expected_value = it->second;
          }

          check_result = check_multiget(keys[i], values[i], statuses[i],
                                        ryw_expected_value);
        } else {
          check_result = check_multiget(keys[i], values[i], statuses[i],
                                        std::nullopt /* ryw_expected_value */);
        }
        if (!check_result) {
          break;
        }
      }
    }

    if (readoptionscopy.snapshot) {
      db_->ReleaseSnapshot(readoptionscopy.snapshot);
    }
    if (use_txn) {
      txn->Rollback().PermitUncheckedError();
      // Enable back error injection disbled for transactions
      if (fault_fs_guard) {
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }
    }
    return statuses;
  }

  void TestGetEntity(ThreadState* thread, const ReadOptions& read_opts,
                     const std::vector<int>& rand_column_families,
                     const std::vector<int64_t>& rand_keys) override {
    assert(thread);

    SharedState* const shared = thread->shared;
    assert(shared);

    assert(!rand_column_families.empty());

    const int column_family = rand_column_families[0];

    assert(column_family >= 0);
    assert(column_family < static_cast<int>(column_families_.size()));

    ColumnFamilyHandle* const cfh = column_families_[column_family];
    assert(cfh);

    assert(!rand_keys.empty());

    const int64_t key = rand_keys[0];
    const std::string key_str = Key(key);

    PinnableWideColumns columns_from_db;
    PinnableAttributeGroups attribute_groups_from_db;

    ReadOptions read_opts_copy = read_opts;
    std::string read_ts_str;
    Slice read_ts_slice;
    if (FLAGS_user_timestamp_size > 0) {
      read_ts_str = GetNowNanos();
      read_ts_slice = read_ts_str;
      read_opts_copy.timestamp = &read_ts_slice;
    }
    const bool read_older_ts = MaybeUseOlderTimestampForPointLookup(
        thread, read_ts_str, read_ts_slice, read_opts_copy);

    const ExpectedValue pre_read_expected_value =
        thread->shared->Get(column_family, key);

    if (fault_fs_guard) {
      fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
          FaultInjectionIOType::kRead);
      fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
          FaultInjectionIOType::kMetadataRead);
      SharedState::ignore_read_error = false;
    }

    Status s;
    if (FLAGS_use_attribute_group) {
      attribute_groups_from_db.emplace_back(cfh);
      s = db_->GetEntity(read_opts_copy, key_str, &attribute_groups_from_db);
      if (s.ok()) {
        s = attribute_groups_from_db.back().status();
      }
    } else {
      s = db_->GetEntity(read_opts_copy, cfh, key_str, &columns_from_db);
    }

    const ExpectedValue post_read_expected_value =
        thread->shared->Get(column_family, key);

    int injected_error_count = 0;
    if (fault_fs_guard) {
      injected_error_count = GetMinInjectedErrorCount(
          fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
              FaultInjectionIOType::kRead),
          fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
              FaultInjectionIOType::kMetadataRead));
      if (!SharedState::ignore_read_error && injected_error_count > 0 &&
          (s.ok() || s.IsNotFound())) {
        // Grab mutex so multiple thread don't try to print the
        // stack trace at the same time
        MutexLock l(thread->shared->GetMutex());
        fprintf(stderr, "Didn't get expected error from GetEntity\n");
        fprintf(stderr, "Callstack that injected the fault\n");
        fault_fs_guard->PrintInjectedThreadLocalErrorBacktrace(
            FaultInjectionIOType::kRead);
        fault_fs_guard->PrintInjectedThreadLocalErrorBacktrace(
            FaultInjectionIOType::kMetadataRead);
        std::terminate();
      }
    }

    if (s.ok()) {
      thread->stats.AddGets(1, 1);

      if (!FLAGS_skip_verifydb && !read_older_ts) {
        if (FLAGS_use_attribute_group) {
          assert(!attribute_groups_from_db.empty());
        }
        const WideColumns& columns =
            FLAGS_use_attribute_group
                ? attribute_groups_from_db.back().columns()
                : columns_from_db.columns();
        if (!VerifyWideColumns(columns)) {
          shared->SetVerificationFailure();
          fprintf(stderr,
                  "error : inconsistent columns returned by GetEntity for key "
                  "%s (%" PRIi64 "): %s\n",
                  StringToHex(key_str).c_str(), rand_keys[0],
                  WideColumnsToHex(columns).c_str());
        } else if (ExpectedValueHelper::MustHaveNotExisted(
                       pre_read_expected_value, post_read_expected_value)) {
          shared->SetVerificationFailure();
          fprintf(stderr,
                  "error : inconsistent values for key %s (%" PRIi64
                  "): GetEntity returns %s, "
                  "expected state does not have the key.\n",
                  StringToHex(key_str).c_str(), rand_keys[0],
                  WideColumnsToHex(columns).c_str());
        } else {
          const uint32_t value_base_from_db =
              GetValueBase(WideColumnsHelper::GetDefaultColumn(columns));
          if (!ExpectedValueHelper::InExpectedValueBaseRange(
                  value_base_from_db, pre_read_expected_value,
                  post_read_expected_value)) {
            shared->SetVerificationFailure();
            fprintf(
                stderr,
                "error : inconsistent values for key %s (%" PRIi64
                "): GetEntity returns %s "
                "with value base %d that falls out of expected state's value "
                "base range.\n",
                StringToHex(key_str).c_str(), rand_keys[0],
                WideColumnsToHex(columns).c_str(), value_base_from_db);
          }
        }
      }
    } else if (s.IsNotFound()) {
      thread->stats.AddGets(1, 0);

      if (!FLAGS_skip_verifydb && !read_older_ts) {
        if (ExpectedValueHelper::MustHaveExisted(pre_read_expected_value,
                                                 post_read_expected_value)) {
          shared->SetVerificationFailure();
          fprintf(stderr,
                  "error : inconsistent values for key %s (%" PRIi64
                  "): expected state has "
                  "the key, GetEntity returns NotFound.\n",
                  StringToHex(key_str).c_str(), rand_keys[0]);
        }
      }
    } else if (injected_error_count == 0 || !IsErrorInjectedAndRetryable(s)) {
      fprintf(stderr,
              "error : GetEntity() returns %s for key: %s (%" PRIi64 ").\n",
              s.ToString().c_str(), StringToHex(key_str).c_str(), rand_keys[0]);
      thread->shared->SetVerificationFailure();
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

    const int column_family = rand_column_families[0];

    assert(column_family >= 0);
    assert(column_family < static_cast<int>(column_families_.size()));

    ColumnFamilyHandle* const cfh = column_families_[column_family];
    assert(cfh);

    assert(!rand_keys.empty());

    const size_t num_keys = rand_keys.size();

    std::unique_ptr<Transaction> txn;

    if (FLAGS_use_txn) {
      // TODO(hx235): test fault injection with MultiGetEntity() with
      // transactions
      if (fault_fs_guard) {
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }
      WriteOptions write_options;
      if (FLAGS_rate_limit_auto_wal_flush) {
        write_options.rate_limiter_priority = Env::IO_USER;
      }

      const Status s = NewTxn(write_options, thread, &txn);
      if (!s.ok()) {
        fprintf(stderr, "NewTxn error: %s\n", s.ToString().c_str());
        thread->shared->SafeTerminate();
      }
    }

    std::vector<std::string> keys(num_keys);
    std::vector<Slice> key_slices(num_keys);
    std::unordered_map<std::string, ExpectedValue> ryw_expected_values;

    for (size_t i = 0; i < num_keys; ++i) {
      const int64_t key = rand_keys[i];

      keys[i] = Key(key);
      key_slices[i] = keys[i];

      if (FLAGS_use_txn) {
        MaybeAddKeyToTxnForRYW(thread, column_family, key, txn.get(),
                               ryw_expected_values);
      }
    }

    int injected_error_count = 0;

    auto verify_expected_errors = [&](auto get_status) {
      assert(fault_fs_guard);
      injected_error_count = GetMinInjectedErrorCount(
          fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
              FaultInjectionIOType::kRead),
          fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
              FaultInjectionIOType::kMetadataRead));
      if (injected_error_count) {
        int stat_nok_nfound = 0;
        for (size_t i = 0; i < num_keys; ++i) {
          const Status& s = get_status(i);
          if (!s.ok() && !s.IsNotFound()) {
            ++stat_nok_nfound;
          }
        }

        if (!SharedState::ignore_read_error &&
            stat_nok_nfound < injected_error_count) {
          // Grab mutex so multiple threads don't try to print the
          // stack trace at the same time
          assert(thread->shared);
          MutexLock l(thread->shared->GetMutex());

          fprintf(stderr, "Didn't get expected error from MultiGetEntity\n");
          fprintf(stderr, "num_keys %zu Expected %d errors, seen %d\n",
                  num_keys, injected_error_count, stat_nok_nfound);
          fprintf(stderr, "Call stack that injected the fault\n");
          fault_fs_guard->PrintInjectedThreadLocalErrorBacktrace(
              FaultInjectionIOType::kRead);
          fault_fs_guard->PrintInjectedThreadLocalErrorBacktrace(
              FaultInjectionIOType::kMetadataRead);
          std::terminate();
        }
      }
    };

    auto check_results = [&](auto get_columns, auto get_status,
                             auto do_extra_check, auto call_get_entity) {
      // Temporarily disable error injection for checking results
      if (fault_fs_guard) {
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }
      const bool check_get_entity =
          !injected_error_count && FLAGS_check_multiget_entity_consistency;

      for (size_t i = 0; i < num_keys; ++i) {
        const WideColumns& columns = get_columns(i);
        const Status& s = get_status(i);

        bool is_consistent = true;

        if (s.ok() && !VerifyWideColumns(columns)) {
          fprintf(
              stderr,
              "error : inconsistent columns returned by MultiGetEntity for key "
              "%s: %s\n",
              StringToHex(keys[i]).c_str(), WideColumnsToHex(columns).c_str());
          is_consistent = false;
        } else if (s.ok() || s.IsNotFound()) {
          if (!do_extra_check(keys[i], columns, s)) {
            is_consistent = false;
          } else if (check_get_entity) {
            PinnableWideColumns cmp_result;
            ThreadStatusUtil::SetThreadOperation(
                ThreadStatus::OperationType::OP_GETENTITY);
            const Status cmp_s = call_get_entity(key_slices[i], &cmp_result);

            if (!cmp_s.ok() && !cmp_s.IsNotFound()) {
              fprintf(stderr, "GetEntity error: %s\n",
                      cmp_s.ToString().c_str());
              is_consistent = false;
            } else if (cmp_s.IsNotFound()) {
              if (s.ok()) {
                fprintf(
                    stderr,
                    "Inconsistent results for key %s: MultiGetEntity returned "
                    "ok, GetEntity returned not found\n",
                    StringToHex(keys[i]).c_str());
                is_consistent = false;
              }
            } else {
              assert(cmp_s.ok());

              if (s.IsNotFound()) {
                fprintf(
                    stderr,
                    "Inconsistent results for key %s: MultiGetEntity returned "
                    "not found, GetEntity returned ok\n",
                    StringToHex(keys[i]).c_str());
                is_consistent = false;
              } else {
                assert(s.ok());

                const WideColumns& cmp_columns = cmp_result.columns();

                if (columns != cmp_columns) {
                  fprintf(stderr,
                          "Inconsistent results for key %s: MultiGetEntity "
                          "returned "
                          "%s, GetEntity returned %s\n",
                          StringToHex(keys[i]).c_str(),
                          WideColumnsToHex(columns).c_str(),
                          WideColumnsToHex(cmp_columns).c_str());
                  is_consistent = false;
                }
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
        } else if (injected_error_count == 0 ||
                   !IsErrorInjectedAndRetryable(s)) {
          fprintf(stderr, "MultiGetEntity error: %s\n", s.ToString().c_str());
          thread->stats.AddErrors(1);
          thread->shared->SetVerificationFailure();
        }
      }
      // Enable back error injection disbled for checking results
      if (fault_fs_guard) {
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }
    };

    if (FLAGS_use_txn) {
      // Transactional/read-your-own-writes MultiGetEntity verification
      std::vector<PinnableWideColumns> results(num_keys);
      std::vector<Status> statuses(num_keys);

      assert(txn);
      txn->MultiGetEntity(read_opts_copy, cfh, num_keys, key_slices.data(),
                          results.data(), statuses.data());

      auto ryw_check = [&](const std::string& key, const WideColumns& columns,
                           const Status& s) -> bool {
        const auto it = ryw_expected_values.find(key);
        if (it == ryw_expected_values.end()) {
          return true;
        }

        const auto& ryw_expected_value = it->second;

        if (s.ok()) {
          if (ryw_expected_value.IsDeleted()) {
            fprintf(
                stderr,
                "MultiGetEntity failed the read-your-own-write check for key "
                "%s\n",
                Slice(key).ToString(true).c_str());
            fprintf(stderr,
                    "MultiGetEntity returned ok, transaction has non-committed "
                    "delete\n");
            return false;
          } else {
            const uint32_t value_base = ryw_expected_value.GetValueBase();
            char expected_value[100];
            const size_t sz = GenerateValue(value_base, expected_value,
                                            sizeof(expected_value));
            const Slice expected_slice(expected_value, sz);
            const WideColumns expected_columns =
                GenerateExpectedWideColumns(value_base, expected_slice);

            if (columns != expected_columns) {
              fprintf(
                  stderr,
                  "MultiGetEntity failed the read-your-own-write check for key "
                  "%s\n",
                  Slice(key).ToString(true).c_str());
              fprintf(stderr, "MultiGetEntity returned %s\n",
                      WideColumnsToHex(columns).c_str());
              fprintf(stderr, "Transaction has non-committed write %s\n",
                      WideColumnsToHex(expected_columns).c_str());
              return false;
            }

            return true;
          }
        }

        assert(s.IsNotFound());
        if (!ryw_expected_value.IsDeleted()) {
          fprintf(stderr,
                  "MultiGetEntity failed the read-your-own-write check for key "
                  "%s\n",
                  Slice(key).ToString(true).c_str());
          fprintf(stderr,
                  "MultiGetEntity returned not found, transaction has "
                  "non-committed write\n");
          return false;
        }

        return true;
      };

      check_results([&](size_t i) { return results[i].columns(); },
                    [&](size_t i) { return statuses[i]; }, ryw_check,
                    [&](const Slice& key, PinnableWideColumns* result) {
                      return txn->GetEntity(read_opts_copy, cfh, key, result);
                    });

      txn->Rollback().PermitUncheckedError();
      // Enable back error injection disbled for transactions
      if (fault_fs_guard) {
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }
    } else if (FLAGS_use_attribute_group) {
      // AttributeGroup MultiGetEntity verification

      if (fault_fs_guard) {
        fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
            FaultInjectionIOType::kRead);
        fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
            FaultInjectionIOType::kMetadataRead);
        SharedState::ignore_read_error = false;
      }

      std::vector<PinnableAttributeGroups> results;
      results.reserve(num_keys);
      for (size_t i = 0; i < num_keys; ++i) {
        PinnableAttributeGroups attribute_groups;
        attribute_groups.emplace_back(cfh);
        results.emplace_back(std::move(attribute_groups));
      }

      db_->MultiGetEntity(read_opts_copy, num_keys, key_slices.data(),
                          results.data());

      if (fault_fs_guard) {
        verify_expected_errors(
            [&](size_t i) { return results[i][0].status(); });
      }

      // Compare against non-attribute-group GetEntity result
      check_results([&](size_t i) { return results[i][0].columns(); },
                    [&](size_t i) { return results[i][0].status(); },
                    [](const Slice& /* key */, const WideColumns& /* columns */,
                       const Status& /* s */) { return true; },
                    [&](const Slice& key, PinnableWideColumns* result) {
                      return db_->GetEntity(read_opts_copy, cfh, key, result);
                    });
    } else {
      // Non-AttributeGroup MultiGetEntity verification

      if (fault_fs_guard) {
        fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
            FaultInjectionIOType::kRead);
        fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
            FaultInjectionIOType::kMetadataRead);
        SharedState::ignore_read_error = false;
      }

      std::vector<PinnableWideColumns> results(num_keys);
      std::vector<Status> statuses(num_keys);

      db_->MultiGetEntity(read_opts_copy, cfh, num_keys, key_slices.data(),
                          results.data(), statuses.data());

      if (fault_fs_guard) {
        verify_expected_errors([&](size_t i) { return statuses[i]; });
      }

      check_results([&](size_t i) { return results[i].columns(); },
                    [&](size_t i) { return statuses[i]; },
                    [](const Slice& /* key */, const WideColumns& /* columns */,
                       const Status& /* s */) { return true; },
                    [&](const Slice& key, PinnableWideColumns* result) {
                      return db_->GetEntity(read_opts_copy, cfh, key, result);
                    });
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

    // Randomly test with `iterate_upper_bound` and `prefix_same_as_start`
    //
    // Get the next prefix first and then see if we want to set it to be the
    // upper bound. We'll use the next prefix in an assertion later on
    if (GetNextPrefix(prefix, &upper_bound) && thread->rand.OneIn(2)) {
      // For half of the time, set the upper bound to the next prefix
      ub_slice = Slice(upper_bound);
      ro_copy.iterate_upper_bound = &ub_slice;
      if (FLAGS_use_sqfc_for_range_queries) {
        ro_copy.table_filter =
            sqfc_factory_->GetTableFilterForRangeQuery(prefix, ub_slice);
      }
    } else if (options_.prefix_extractor && thread->rand.OneIn(2)) {
      ro_copy.prefix_same_as_start = true;
    }

    std::string read_ts_str;
    Slice read_ts_slice;
    MaybeUseOlderTimestampForRangeScan(thread, read_ts_str, read_ts_slice,
                                       ro_copy);

    if (fault_fs_guard) {
      fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
          FaultInjectionIOType::kRead);
      fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
          FaultInjectionIOType::kMetadataRead);
      SharedState::ignore_read_error = false;
    }

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro_copy, cfh));

    uint64_t count = 0;
    Status s;

    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
      // If upper or prefix bounds is specified, only keys of the target
      // prefix should show up. Otherwise, we need to manual exit the loop when
      // we see the first key that is not in the target prefix show up.
      if (ro_copy.iterate_upper_bound != nullptr ||
          ro_copy.prefix_same_as_start) {
        assert(iter->key().starts_with(prefix));
      } else if (!iter->key().starts_with(prefix)) {
        break;
      }
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

      if (ro_copy.allow_unprepared_value) {
        if (!iter->PrepareValue()) {
          s = iter->status();
          break;
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

    int injected_error_count = 0;
    if (fault_fs_guard) {
      injected_error_count = GetMinInjectedErrorCount(
          fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
              FaultInjectionIOType::kRead),
          fault_fs_guard->GetAndResetInjectedThreadLocalErrorCount(
              FaultInjectionIOType::kMetadataRead));
      if (!SharedState::ignore_read_error && injected_error_count > 0 &&
          s.ok()) {
        // Grab mutex so multiple thread don't try to print the
        // stack trace at the same time
        MutexLock l(thread->shared->GetMutex());
        fprintf(stderr, "Didn't get expected error from PrefixScan\n");
        fprintf(stderr, "Callstack that injected the fault\n");
        fault_fs_guard->PrintInjectedThreadLocalErrorBacktrace(
            FaultInjectionIOType::kRead);
        fault_fs_guard->PrintInjectedThreadLocalErrorBacktrace(
            FaultInjectionIOType::kMetadataRead);
        std::terminate();
      }
    }

    if (s.ok()) {
      thread->stats.AddPrefixes(1, count);
    } else if (injected_error_count == 0 || !IsErrorInjectedAndRetryable(s)) {
      fprintf(stderr,
              "TestPrefixScan error: %s with ReadOptions::iterate_upper_bound: "
              "%s, prefix_same_as_start: %s \n",
              s.ToString().c_str(),
              ro_copy.iterate_upper_bound
                  ? ro_copy.iterate_upper_bound->ToString(true).c_str()
                  : "nullptr",
              ro_copy.prefix_same_as_start ? "true" : "false");
      thread->shared->SetVerificationFailure();
    }

    return s;
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
      // Temporarily disable error injection for preparation
      if (fault_fs_guard) {
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }

      std::string from_db;
      Status s = db_->Get(read_opts, cfh, k, &from_db);
      bool res = VerifyOrSyncValue(
          rand_column_family, rand_key, read_opts, shared,
          /* msg_prefix */ "Pre-Put Get verification", from_db, s);

      // Enable back error injection disabled for preparation
      if (fault_fs_guard) {
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }
      if (!res) {
        return s;
      }
    }

    // To track the final write status
    Status s;
    // To track the initial write status
    Status initial_write_s;
    // To track whether WAL write may have succeeded during the initial failed
    // write
    bool initial_wal_write_may_succeed = true;
    bool commit_bypass_memtable = false;

    PendingExpectedValue pending_expected_value =
        shared->PreparePut(rand_column_family, rand_key);

    const uint32_t value_base = pending_expected_value.GetFinalValueBase();
    const size_t sz = GenerateValue(value_base, value, sizeof(value));
    const Slice v(value, sz);

    uint64_t wait_for_recover_start_time = 0;
    do {
      // In order to commit the expected state for the initial write failed with
      // injected retryable error and successful WAL write, retry the write
      // until it succeeds after the recovery finishes
      if (!s.ok() && IsErrorInjectedAndRetryable(s) &&
          initial_wal_write_may_succeed) {
        std::this_thread::sleep_for(std::chrono::microseconds(1 * 1000 * 1000));
      }
      if (FLAGS_use_put_entity_one_in > 0 &&
          (value_base % FLAGS_use_put_entity_one_in) == 0) {
        if (!FLAGS_use_txn) {
          if (FLAGS_use_attribute_group) {
            s = db_->PutEntity(write_opts, k,
                               GenerateAttributeGroups({cfh}, value_base, v));
          } else {
            s = db_->PutEntity(write_opts, cfh, k,
                               GenerateWideColumns(value_base, v));
          }
        } else {
          s = ExecuteTransaction(write_opts, thread, [&](Transaction& txn) {
            return txn.PutEntity(cfh, k, GenerateWideColumns(value_base, v));
          });
        }
      } else if (FLAGS_use_timed_put_one_in > 0 &&
                 ((value_base + kLargePrimeForCommonFactorSkew) %
                  FLAGS_use_timed_put_one_in) == 0) {
        WriteBatch wb;
        uint64_t write_unix_time = GetWriteUnixTime(thread);
        s = wb.TimedPut(cfh, k, v, write_unix_time);
        if (s.ok()) {
          s = db_->Write(write_opts, &wb);
        }
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
          s = ExecuteTransaction(
              write_opts, thread,
              [&](Transaction& txn) { return txn.Put(cfh, k, v); },
              &commit_bypass_memtable);
        }
      }
      UpdateIfInitialWriteFails(db_stress_env, s, &initial_write_s,
                                &initial_wal_write_may_succeed,
                                &wait_for_recover_start_time);

    } while (!s.ok() && IsErrorInjectedAndRetryable(s) &&
             initial_wal_write_may_succeed);

    if (!s.ok()) {
      pending_expected_value.Rollback();
      if (IsErrorInjectedAndRetryable(s)) {
        assert(!initial_wal_write_may_succeed);
        return s;
      } else if (FLAGS_inject_error_severity == 2) {
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
    } else {
      PrintWriteRecoveryWaitTimeIfNeeded(
          db_stress_env, initial_write_s, initial_wal_write_may_succeed,
          wait_for_recover_start_time, "TestPut");
      pending_expected_value.Commit();
      thread->stats.AddBytesForWrites(1, sz);
      PrintKeyValue(rand_column_family, static_cast<uint32_t>(rand_key), value,
                    sz);
    }
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

    // To track the final write status
    Status s;
    // To track the initial write status
    Status initial_write_s;
    // To track whether WAL write may have succeeded during the initial failed
    // write
    bool initial_wal_write_may_succeed = true;
    bool commit_bypass_memtable = false;

    // Use delete if the key may be overwritten and a single deletion
    // otherwise.
    if (shared->AllowsOverwrite(rand_key)) {
      PendingExpectedValue pending_expected_value =
          shared->PrepareDelete(rand_column_family, rand_key);

      uint64_t wait_for_recover_start_time = 0;
      do {
        // In order to commit the expected state for the initial write failed
        // with injected retryable error and successful WAL write, retry the
        // write until it succeeds after the recovery finishes
        if (!s.ok() && IsErrorInjectedAndRetryable(s) &&
            initial_wal_write_may_succeed) {
          std::this_thread::sleep_for(
              std::chrono::microseconds(1 * 1000 * 1000));
        }
        if (!FLAGS_use_txn) {
          if (FLAGS_user_timestamp_size == 0) {
            s = db_->Delete(write_opts, cfh, key);
          } else {
            s = db_->Delete(write_opts, cfh, key, write_ts);
          }
        } else {
          s = ExecuteTransaction(
              write_opts, thread,
              [&](Transaction& txn) { return txn.Delete(cfh, key); },
              &commit_bypass_memtable);
        }
        UpdateIfInitialWriteFails(
            db_stress_env, s, &initial_write_s, &initial_wal_write_may_succeed,
            &wait_for_recover_start_time, commit_bypass_memtable);
      } while (!s.ok() && IsErrorInjectedAndRetryable(s) &&
               initial_wal_write_may_succeed);

      if (!s.ok()) {
        pending_expected_value.Rollback();
        if (IsErrorInjectedAndRetryable(s)) {
          assert(!initial_wal_write_may_succeed);
          return s;
        } else if (FLAGS_inject_error_severity == 2) {
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
      } else {
        PrintWriteRecoveryWaitTimeIfNeeded(
            db_stress_env, initial_write_s, initial_wal_write_may_succeed,
            wait_for_recover_start_time, "TestDelete");
        pending_expected_value.Commit();
        thread->stats.AddDeletes(1);
      }
    } else {
      PendingExpectedValue pending_expected_value =
          shared->PrepareSingleDelete(rand_column_family, rand_key);

      uint64_t wait_for_recover_start_time = 0;
      do {
        // In order to commit the expected state for the initial write failed
        // with injected retryable error and successful WAL write, retry the
        // write until it succeeds after the recovery finishes
        if (!s.ok() && IsErrorInjectedAndRetryable(s) &&
            initial_wal_write_may_succeed) {
          std::this_thread::sleep_for(
              std::chrono::microseconds(1 * 1000 * 1000));
        }
        if (!FLAGS_use_txn) {
          if (FLAGS_user_timestamp_size == 0) {
            s = db_->SingleDelete(write_opts, cfh, key);
          } else {
            s = db_->SingleDelete(write_opts, cfh, key, write_ts);
          }
        } else {
          s = ExecuteTransaction(
              write_opts, thread,
              [&](Transaction& txn) { return txn.SingleDelete(cfh, key); },
              &commit_bypass_memtable);
        }
        UpdateIfInitialWriteFails(
            db_stress_env, s, &initial_write_s, &initial_wal_write_may_succeed,
            &wait_for_recover_start_time, commit_bypass_memtable);
      } while (!s.ok() && IsErrorInjectedAndRetryable(s) &&
               initial_wal_write_may_succeed);

      if (!s.ok()) {
        pending_expected_value.Rollback();
        if (IsErrorInjectedAndRetryable(s)) {
          assert(!initial_wal_write_may_succeed);
          return s;
        } else if (FLAGS_inject_error_severity == 2) {
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
      } else {
        PrintWriteRecoveryWaitTimeIfNeeded(
            db_stress_env, initial_write_s, initial_wal_write_may_succeed,
            wait_for_recover_start_time, "TestDelete");
        pending_expected_value.Commit();
        thread->stats.AddSingleDeletes(1);
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
    GetDeleteRangeKeyLocks(thread, rand_column_family, rand_key, &range_locks);

    // To track the final write status
    Status s;
    // To track the initial write status
    Status initial_write_s;
    // To track whether WAL write may have succeeded during the initial failed
    // write
    bool initial_wal_write_may_succeed = true;

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
    uint64_t wait_for_recover_start_time = 0;

    do {
      // In order to commit the expected state for the initial write failed with
      // injected retryable error and successful WAL write, retry the write
      // until it succeeds after the recovery finishes
      if (!s.ok() && IsErrorInjectedAndRetryable(s) &&
          initial_wal_write_may_succeed) {
        std::this_thread::sleep_for(std::chrono::microseconds(1 * 1000 * 1000));
      }
      if (FLAGS_user_timestamp_size) {
        write_ts_str = GetNowNanos();
        write_ts = write_ts_str;
        s = db_->DeleteRange(write_opts, cfh, key, end_key, write_ts);
      } else {
        s = db_->DeleteRange(write_opts, cfh, key, end_key);
      }
      UpdateIfInitialWriteFails(db_stress_env, s, &initial_write_s,
                                &initial_wal_write_may_succeed,
                                &wait_for_recover_start_time);
    } while (!s.ok() && IsErrorInjectedAndRetryable(s) &&
             initial_wal_write_may_succeed);

    if (!s.ok()) {
      for (PendingExpectedValue& pending_expected_value :
           pending_expected_values) {
        pending_expected_value.Rollback();
      }
      if (IsErrorInjectedAndRetryable(s)) {
        assert(!initial_wal_write_may_succeed);
        return s;
      } else if (FLAGS_inject_error_severity == 2) {
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
    } else {
      PrintWriteRecoveryWaitTimeIfNeeded(
          db_stress_env, initial_write_s, initial_wal_write_may_succeed,
          wait_for_recover_start_time, "TestDeleteRange");
      for (PendingExpectedValue& pending_expected_value :
           pending_expected_values) {
        pending_expected_value.Commit();
      }
      thread->stats.AddRangeDeletions(1);
      thread->stats.AddCoveredByRangeDeletions(covered);
    }
    return s;
  }

  void TestIngestExternalFile(ThreadState* thread,
                              const std::vector<int>& rand_column_families,
                              const std::vector<int64_t>& rand_keys) override {
    // When true, we create two sst files, the first one with regular puts for
    // a continuous range of keys, the second one with a standalone range
    // deletion for all the keys. This is to exercise the standalone range
    // deletion file's compaction input optimization.
    bool test_standalone_range_deletion = thread->rand.OneInOpt(
        FLAGS_test_ingest_standalone_range_deletion_one_in);
    std::vector<std::string> external_files;
    const std::string sst_filename =
        FLAGS_db + "/." + std::to_string(thread->tid) + ".sst";
    external_files.push_back(sst_filename);
    std::string standalone_rangedel_filename;
    if (test_standalone_range_deletion) {
      standalone_rangedel_filename = FLAGS_db + "/." +
                                     std::to_string(thread->tid) +
                                     "_standalone_rangedel.sst";
      external_files.push_back(standalone_rangedel_filename);
    }
    Status s;
    std::ostringstream ingest_options_oss;

    // Temporarily disable error injection for preparation
    if (fault_fs_guard) {
      fault_fs_guard->DisableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataRead);
      fault_fs_guard->DisableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataWrite);
    }

    for (const auto& filename : external_files) {
      if (db_stress_env->FileExists(filename).ok()) {
        // Maybe we terminated abnormally before, so cleanup to give this file
        // ingestion a clean slate
        s = db_stress_env->DeleteFile(filename);
      }
      if (!s.ok()) {
        return;
      }
    }

    if (fault_fs_guard) {
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataRead);
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataWrite);
    }

    SstFileWriter sst_file_writer(EnvOptions(options_), options_);
    SstFileWriter standalone_rangedel_sst_file_writer(EnvOptions(options_),
                                                      options_);
    if (s.ok()) {
      s = sst_file_writer.Open(sst_filename);
    }
    if (s.ok() && test_standalone_range_deletion) {
      s = standalone_rangedel_sst_file_writer.Open(
          standalone_rangedel_filename);
    }
    if (!s.ok()) {
      return;
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

    // Grab locks, add keys
    assert(FLAGS_nooverwritepercent < 100);
    for (int64_t key = key_base;
         key < shared->GetMaxKey() &&
         key < key_base + FLAGS_ingest_external_file_width;
         ++key) {
      if (key == key_base ||
          (key & ((1 << FLAGS_log2_keys_per_lock) - 1)) == 0) {
        range_locks.emplace_back(
            new MutexLock(shared->GetMutexForKey(column_family, key)));
      }
      if (test_standalone_range_deletion) {
        // Testing standalone range deletion needs a continuous range of keys.
        if (shared->AllowsOverwrite(key)) {
          if (keys.empty() || (!keys.empty() && keys.back() == key - 1)) {
            keys.push_back(key);
          } else {
            keys.clear();
            keys.push_back(key);
          }
        } else {
          if (keys.size() > 0) {
            break;
          } else {
            continue;
          }
        }
      } else {
        if (!shared->AllowsOverwrite(key)) {
          // We could alternatively include `key` that is deleted.
          continue;
        }
        keys.push_back(key);
      }
    }

    if (s.ok() && keys.empty()) {
      return;
    }

    // set pending state on expected values, create and ingest files.
    size_t total_keys = keys.size();
    for (size_t i = 0; s.ok() && i < total_keys; i++) {
      int64_t key = keys.at(i);
      char value[100];
      auto key_str = Key(key);
      const Slice k(key_str);
      Slice v;
      if (test_standalone_range_deletion) {
        assert(i == 0 || keys.at(i - 1) == key - 1);
        s = sst_file_writer.Put(k, v);
      } else {
        PendingExpectedValue pending_expected_value =
            shared->PreparePut(column_family, key);
        const uint32_t value_base = pending_expected_value.GetFinalValueBase();
        const size_t value_len =
            GenerateValue(value_base, value, sizeof(value));
        v = Slice(value, value_len);
        values.push_back(value_base);
        pending_expected_values.push_back(pending_expected_value);
        if (FLAGS_use_put_entity_one_in > 0 &&
            (value_base % FLAGS_use_put_entity_one_in) == 0) {
          WideColumns columns = GenerateWideColumns(values.back(), v);
          s = sst_file_writer.PutEntity(k, columns);
        } else {
          s = sst_file_writer.Put(k, v);
        }
      }
    }
    if (s.ok() && !keys.empty()) {
      s = sst_file_writer.Finish();
    }

    if (s.ok() && total_keys != 0 && test_standalone_range_deletion) {
      int64_t start_key = keys.at(0);
      int64_t end_key = keys.back() + 1;
      pending_expected_values =
          shared->PrepareDeleteRange(column_family, start_key, end_key);
      auto start_key_str = Key(start_key);
      const Slice start_key_slice(start_key_str);
      auto end_key_str = Key(end_key);
      const Slice end_key_slice(end_key_str);
      s = standalone_rangedel_sst_file_writer.DeleteRange(start_key_slice,
                                                          end_key_slice);
      if (s.ok()) {
        s = standalone_rangedel_sst_file_writer.Finish();
      }
    }
    if (s.ok()) {
      IngestExternalFileOptions ingest_options;
      ingest_options.move_files = thread->rand.OneInOpt(2);
      ingest_options.verify_checksums_before_ingest = thread->rand.OneInOpt(2);
      ingest_options.verify_checksums_readahead_size =
          thread->rand.OneInOpt(2) ? 1024 * 1024 : 0;
      ingest_options.fill_cache = thread->rand.OneInOpt(4);
      ingest_options_oss << "move_files: " << ingest_options.move_files
                         << ", verify_checksums_before_ingest: "
                         << ingest_options.verify_checksums_before_ingest
                         << ", verify_checksums_readahead_size: "
                         << ingest_options.verify_checksums_readahead_size
                         << ", fill_cache: " << ingest_options.fill_cache
                         << ", test_standalone_range_deletion: "
                         << test_standalone_range_deletion;
      s = db_->IngestExternalFile(column_families_[column_family],
                                  external_files, ingest_options);
    }
    if (!s.ok()) {
      for (PendingExpectedValue& pending_expected_value :
           pending_expected_values) {
        pending_expected_value.Rollback();
      }

      if (!IsErrorInjectedAndRetryable(s)) {
        fprintf(stderr,
                "file ingestion error: %s under specified "
                "IngestExternalFileOptions: %s (Empty string or "
                "missing field indicates default option or value is used)\n",
                s.ToString().c_str(), ingest_options_oss.str().c_str());
        thread->shared->SafeTerminate();
      }
    } else {
      for (PendingExpectedValue& pending_expected_value :
           pending_expected_values) {
        pending_expected_value.Commit();
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
    std::string ub_str, lb_str;
    if (FLAGS_use_sqfc_for_range_queries) {
      ub_str = Key(ub);
      lb_str = Key(lb);
      ro.table_filter =
          sqfc_factory_->GetTableFilterForRangeQuery(lb_str, ub_str);
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
    std::unique_ptr<Iterator> iter;
    if (FLAGS_use_multi_cf_iterator) {
      std::vector<ColumnFamilyHandle*> cfhs;
      cfhs.reserve(rand_column_families.size());
      for (auto cf_index : rand_column_families) {
        cfhs.emplace_back(column_families_[cf_index]);
      }
      assert(!cfhs.empty());
      iter = db_->NewCoalescingIterator(ro, cfhs);
    } else {
      iter = std::unique_ptr<Iterator>(db_->NewIterator(ro, cfh));
    }

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

      if (iter->Valid() && ro.allow_unprepared_value) {
        op_logs += "*";

        if (!iter->PrepareValue()) {
          assert(!iter->Valid());
          assert(!iter->status().ok());
        }
      }

      if (!iter->Valid()) {
        if (!iter->status().ok()) {
          if (IsErrorInjectedAndRetryable(iter->status())) {
            return iter->status();
          } else {
            thread->shared->SetVerificationFailure();
            fprintf(stderr, "TestIterate against expected state error: %s\n",
                    iter->status().ToString().c_str());
            fprintf(stderr, "Column family: %s, op_logs: %s\n",
                    cfh->GetName().c_str(), op_logs.c_str());
            thread->stats.AddErrors(1);
            return iter->status();
          }
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

      if (iter->Valid() && ro.allow_unprepared_value) {
        op_logs += "*";

        if (!iter->PrepareValue()) {
          assert(!iter->Valid());
          assert(!iter->status().ok());
        }
      }

      if (!iter->Valid()) {
        if (!iter->status().ok()) {
          if (IsErrorInjectedAndRetryable(iter->status())) {
            return iter->status();
          } else {
            thread->shared->SetVerificationFailure();
            fprintf(stderr, "TestIterate against expected state error: %s\n",
                    iter->status().ToString().c_str());
            fprintf(stderr, "Column family: %s, op_logs: %s\n",
                    cfh->GetName().c_str(), op_logs.c_str());
            thread->stats.AddErrors(1);
            return iter->status();
          }
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

    // Write-prepared/write-unprepared transactions and multi-CF iterator do not
    // support Refresh() yet.
    if (!(FLAGS_use_txn && FLAGS_txn_write_policy != 0) &&
        !FLAGS_use_multi_cf_iterator && thread->rand.OneIn(2)) {
      pre_read_expected_values.clear();
      post_read_expected_values.clear();
      // Refresh after forward/backward scan to allow higher chance of SV
      // change.
      for (int64_t i = 0; i < static_cast<int64_t>(expected_values_size); ++i) {
        pre_read_expected_values.push_back(
            shared->Get(rand_column_family, i + lb));
      }
      Status rs = iter->Refresh();
      if (!rs.ok() && IsErrorInjectedAndRetryable(rs)) {
        return rs;
      }
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
      if (ro.allow_unprepared_value) {
        op_logs += "*";

        if (!iter->PrepareValue()) {
          assert(!iter->Valid());
          assert(!iter->status().ok());
          break;
        }
      }

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
      if (IsErrorInjectedAndRetryable(iter->status())) {
        return iter->status();
      } else {
        thread->shared->SetVerificationFailure();
        fprintf(stderr, "TestIterate against expected state error: %s\n",
                iter->status().ToString().c_str());
        fprintf(stderr, "Column family: %s, op_logs: %s\n",
                cfh->GetName().c_str(), op_logs.c_str());
        thread->stats.AddErrors(1);
        return iter->status();
      }
    }

    thread->stats.AddIterations(1);

    return Status::OK();
  }

  bool VerifyOrSyncValue(int cf, int64_t key, const ReadOptions& opts,
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
        return true;
      } else if (s.IsNotFound()) {
        // Value doesn't exist in db, update state to reflect that
        shared->SyncDelete(cf, key);
        return true;
      } else {
        assert(false);
      }
    }
    char expected_value_data[kValueMaxLen];
    size_t expected_value_data_size =
        GenerateValue(expected_value.GetValueBase(), expected_value_data,
                      sizeof(expected_value_data));

    std::ostringstream read_u64ts;
    if (opts.timestamp) {
      read_u64ts << " while read with timestamp: ";
      uint64_t read_ts;
      if (DecodeU64Ts(*opts.timestamp, &read_ts).ok()) {
        read_u64ts << std::to_string(read_ts) << ", ";
      } else {
        read_u64ts << s.ToString()
                   << " Encoded read timestamp: " << opts.timestamp->ToString()
                   << ", ";
      }
    }

    // compare value_from_db with the value in the shared state
    if (s.ok()) {
      const Slice slice(value_from_db);
      const uint32_t value_base_from_db = GetValueBase(slice);
      if (ExpectedValueHelper::MustHaveNotExisted(expected_value,
                                                  expected_value)) {
        VerificationAbort(
            shared, msg_prefix + ": Unexpected value found" + read_u64ts.str(),
            cf, key, value_from_db, "");
        return false;
      }
      if (!ExpectedValueHelper::InExpectedValueBaseRange(
              value_base_from_db, expected_value, expected_value)) {
        VerificationAbort(
            shared, msg_prefix + ": Unexpected value found" + read_u64ts.str(),
            cf, key, value_from_db,
            Slice(expected_value_data, expected_value_data_size));
        return false;
      }
      // TODO: are the length/memcmp() checks repetitive?
      if (value_from_db.length() != expected_value_data_size) {
        VerificationAbort(shared,
                          msg_prefix + ": Length of value read is not equal" +
                              read_u64ts.str(),
                          cf, key, value_from_db,
                          Slice(expected_value_data, expected_value_data_size));
        return false;
      }
      if (memcmp(value_from_db.data(), expected_value_data,
                 expected_value_data_size) != 0) {
        VerificationAbort(shared,
                          msg_prefix + ": Contents of value read don't match" +
                              read_u64ts.str(),
                          cf, key, value_from_db,
                          Slice(expected_value_data, expected_value_data_size));
        return false;
      }
    } else if (s.IsNotFound()) {
      if (ExpectedValueHelper::MustHaveExisted(expected_value,
                                               expected_value)) {
        VerificationAbort(
            shared,
            msg_prefix + ": Value not found " + read_u64ts.str() + s.ToString(),
            cf, key, "", Slice(expected_value_data, expected_value_data_size));
        return false;
      }
    } else {
      VerificationAbort(
          shared,
          msg_prefix + "Non-OK status " + read_u64ts.str() + s.ToString(), cf,
          key, "", Slice(expected_value_data, expected_value_data_size));
      return false;
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

  void MaybeAddKeyToTxnForRYW(
      ThreadState* thread, int column_family, int64_t key, Transaction* txn,
      std::unordered_map<std::string, ExpectedValue>& ryw_expected_values) {
    assert(thread);
    assert(txn);

    SharedState* const shared = thread->shared;
    assert(shared);

    const ExpectedValue expected_value =
        thread->shared->Get(column_family, key);
    bool may_exist = !ExpectedValueHelper::MustHaveNotExisted(expected_value,
                                                              expected_value);
    if (!shared->AllowsOverwrite(key) && may_exist) {
      // Just do read your write checks for keys that allow overwrites.
      return;
    }

    // With a 1 in 10 probability, insert the just added key in the batch
    // into the transaction. This will create an overlap with the MultiGet
    // keys and exercise some corner cases in the code
    if (thread->rand.OneIn(10)) {
      assert(column_family >= 0);
      assert(column_family < static_cast<int>(column_families_.size()));

      ColumnFamilyHandle* const cfh = column_families_[column_family];
      assert(cfh);

      const std::string k = Key(key);

      enum class Op {
        PutOrPutEntity,
        Merge,
        Delete,
        // add new operations above this line
        NumberOfOps
      };

      const Op op = static_cast<Op>(
          thread->rand.Uniform(static_cast<int>(Op::NumberOfOps)));

      Status s;

      switch (op) {
        case Op::PutOrPutEntity:
        case Op::Merge: {
          ExpectedValue put_value;
          put_value.SyncPut(static_cast<uint32_t>(thread->rand.Uniform(
              static_cast<int>(ExpectedValue::GetValueBaseMask()))));
          ryw_expected_values[k] = put_value;

          const uint32_t value_base = put_value.GetValueBase();

          char value[100];
          const size_t sz = GenerateValue(value_base, value, sizeof(value));
          const Slice v(value, sz);

          if (op == Op::PutOrPutEntity || !FLAGS_use_merge) {
            if (FLAGS_use_put_entity_one_in > 0 &&
                (value_base % FLAGS_use_put_entity_one_in) == 0) {
              s = txn->PutEntity(cfh, k, GenerateWideColumns(value_base, v));
            } else {
              s = txn->Put(cfh, k, v);
            }
          } else {
            s = txn->Merge(cfh, k, v);
          }

          break;
        }
        case Op::Delete: {
          ExpectedValue delete_value;
          delete_value.SyncDelete();
          ryw_expected_values[k] = delete_value;

          s = txn->Delete(cfh, k);
          break;
        }
        default:
          assert(false);
      }

      if (!s.ok()) {
        fprintf(stderr,
                "Transaction write error in read-your-own-write test: %s\n",
                s.ToString().c_str());
        shared->SafeTerminate();
      }
    }
  }
};

StressTest* CreateNonBatchedOpsStressTest() {
  return new NonBatchedOpsStressTest();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
