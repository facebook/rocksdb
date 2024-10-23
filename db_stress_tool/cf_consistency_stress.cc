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
#include "file/file_util.h"

namespace ROCKSDB_NAMESPACE {
class CfConsistencyStressTest : public StressTest {
 public:
  CfConsistencyStressTest() : batch_id_(0) {}

  ~CfConsistencyStressTest() override = default;

  bool IsStateTracked() const override { return false; }

  Status TestPut(ThreadState* thread, WriteOptions& write_opts,
                 const ReadOptions& /* read_opts */,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys,
                 char (&value)[100]) override {
    assert(!rand_column_families.empty());
    assert(!rand_keys.empty());

    const std::string k = Key(rand_keys[0]);

    const uint32_t value_base = batch_id_.fetch_add(1);
    const size_t sz = GenerateValue(value_base, value, sizeof(value));
    const Slice v(value, sz);

    WriteBatch batch;

    Status status;
    if (FLAGS_use_attribute_group && FLAGS_use_put_entity_one_in > 0 &&
        (value_base % FLAGS_use_put_entity_one_in) == 0) {
      std::vector<ColumnFamilyHandle*> cfhs;
      cfhs.reserve(rand_column_families.size());
      for (auto cf : rand_column_families) {
        cfhs.push_back(column_families_[cf]);
      }
      status = batch.PutEntity(k, GenerateAttributeGroups(cfhs, value_base, v));
    } else {
      for (auto cf : rand_column_families) {
        ColumnFamilyHandle* const cfh = column_families_[cf];
        assert(cfh);

        if (FLAGS_use_put_entity_one_in > 0 &&
            (value_base % FLAGS_use_put_entity_one_in) == 0) {
          status = batch.PutEntity(cfh, k, GenerateWideColumns(value_base, v));
        } else if (FLAGS_use_timed_put_one_in > 0 &&
                   ((value_base + kLargePrimeForCommonFactorSkew) %
                    FLAGS_use_timed_put_one_in) == 0) {
          uint64_t write_unix_time = GetWriteUnixTime(thread);
          status = batch.TimedPut(cfh, k, v, write_unix_time);
        } else if (FLAGS_use_merge) {
          status = batch.Merge(cfh, k, v);
        } else {
          status = batch.Put(cfh, k, v);
        }
        if (!status.ok()) {
          break;
        }
      }
    }

    if (status.ok()) {
      status = db_->Write(write_opts, &batch);
    }

    if (status.ok()) {
      auto num = static_cast<long>(rand_column_families.size());
      thread->stats.AddBytesForWrites(num, (sz + 1) * num);
    } else if (!IsErrorInjectedAndRetryable(status)) {
      fprintf(stderr, "multi put or merge error: %s\n",
              status.ToString().c_str());
      thread->stats.AddErrors(1);
    }

    return status;
  }

  Status TestDelete(ThreadState* thread, WriteOptions& write_opts,
                    const std::vector<int>& rand_column_families,
                    const std::vector<int64_t>& rand_keys) override {
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    WriteBatch batch;
    for (auto cf : rand_column_families) {
      ColumnFamilyHandle* cfh = column_families_[cf];
      batch.Delete(cfh, key);
    }
    Status s = db_->Write(write_opts, &batch);
    if (s.ok()) {
      thread->stats.AddDeletes(static_cast<long>(rand_column_families.size()));
    } else if (!IsErrorInjectedAndRetryable(s)) {
      fprintf(stderr, "multidel error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    }
    return s;
  }

  Status TestDeleteRange(ThreadState* thread, WriteOptions& write_opts,
                         const std::vector<int>& rand_column_families,
                         const std::vector<int64_t>& rand_keys) override {
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
    if (s.ok()) {
      thread->stats.AddRangeDeletions(
          static_cast<long>(rand_column_families.size()));
    } else if (!IsErrorInjectedAndRetryable(s)) {
      fprintf(stderr, "multi del range error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    }
    return s;
  }

  void TestIngestExternalFile(
      ThreadState* /* thread */,
      const std::vector<int>& /* rand_column_families */,
      const std::vector<int64_t>& /* rand_keys */) override {
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

      // Temporarily disable error injection for verification
      if (fault_fs_guard) {
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }

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

      //  Enable back error injection disabled for verification
      if (fault_fs_guard) {
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
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
    } else if (!IsErrorInjectedAndRetryable(s)) {
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
    ReadOptions readoptionscopy = read_opts;
    readoptionscopy.rate_limiter_priority =
        FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;

    for (size_t i = 0; i < num_keys; ++i) {
      key_str.emplace_back(Key(rand_keys[i]));
      keys.emplace_back(key_str.back());
    }
    db_->MultiGet(readoptionscopy, cfh, num_keys, keys.data(), values.data(),
                  statuses.data());
    for (const auto& s : statuses) {
      if (s.ok()) {
        // found case
        thread->stats.AddGets(1, 1);
      } else if (s.IsNotFound()) {
        // not found case
        thread->stats.AddGets(1, 0);
      } else if (!IsErrorInjectedAndRetryable(s)) {
        // errors case
        fprintf(stderr, "MultiGet error: %s\n", s.ToString().c_str());
        thread->stats.AddErrors(1);
      }
    }
    return statuses;
  }

  void TestGetEntity(ThreadState* thread, const ReadOptions& read_opts,
                     const std::vector<int>& rand_column_families,
                     const std::vector<int64_t>& rand_keys) override {
    assert(thread);
    assert(!rand_column_families.empty());
    assert(!rand_keys.empty());

    const std::string key = Key(rand_keys[0]);

    Status s;
    bool is_consistent = true;

    if (thread->rand.OneIn(2)) {
      // With a 1/2 chance, do a random read from a random CF
      const size_t cf_id = thread->rand.Next() % rand_column_families.size();

      assert(rand_column_families[cf_id] >= 0);
      assert(rand_column_families[cf_id] <
             static_cast<int>(column_families_.size()));

      ColumnFamilyHandle* const cfh =
          column_families_[rand_column_families[cf_id]];
      assert(cfh);

      PinnableWideColumns result;
      s = db_->GetEntity(read_opts, cfh, key, &result);

      if (s.ok()) {
        if (!VerifyWideColumns(result.columns())) {
          fprintf(
              stderr,
              "GetEntity error: inconsistent columns for key %s, entity %s\n",
              StringToHex(key).c_str(),
              WideColumnsToHex(result.columns()).c_str());
          is_consistent = false;
        }
      }
    } else {
      // With a 1/2 chance, compare one key across all CFs
      ManagedSnapshot snapshot_guard(db_);

      ReadOptions read_opts_copy = read_opts;
      read_opts_copy.snapshot = snapshot_guard.snapshot();

      assert(rand_column_families[0] >= 0);
      assert(rand_column_families[0] <
             static_cast<int>(column_families_.size()));

      PinnableWideColumns cmp_result;
      s = db_->GetEntity(read_opts_copy,
                         column_families_[rand_column_families[0]], key,
                         &cmp_result);

      //  Temporarily disable error injection for verification
      if (fault_fs_guard) {
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }

      if (s.ok() || s.IsNotFound()) {
        const bool cmp_found = s.ok();

        if (cmp_found) {
          if (!VerifyWideColumns(cmp_result.columns())) {
            fprintf(stderr,
                    "GetEntity error: inconsistent columns for key %s, "
                    "entity %s\n",
                    StringToHex(key).c_str(),
                    WideColumnsToHex(cmp_result.columns()).c_str());
            is_consistent = false;
          }
        }

        if (is_consistent) {
          if (FLAGS_use_attribute_group) {
            PinnableAttributeGroups result;
            result.reserve(rand_column_families.size());
            for (size_t i = 1; i < rand_column_families.size(); ++i) {
              assert(rand_column_families[i] >= 0);
              assert(rand_column_families[i] <
                     static_cast<int>(column_families_.size()));

              result.emplace_back(column_families_[rand_column_families[i]]);
            }
            s = db_->GetEntity(read_opts_copy, key, &result);
            if (s.ok()) {
              for (auto& attribute_group : result) {
                s = attribute_group.status();
                if (!s.ok() && !s.IsNotFound()) {
                  break;
                }

                const bool found = s.ok();

                if (!cmp_found && found) {
                  fprintf(
                      stderr,
                      "Non-AttributeGroup GetEntity returns different results "
                      "than AttributeGroup GetEntity for key %s: CF %s "
                      "returns not found, CF %s returns entity %s \n",
                      StringToHex(key).c_str(), column_family_names_[0].c_str(),
                      attribute_group.column_family()->GetName().c_str(),
                      WideColumnsToHex(attribute_group.columns()).c_str());
                  is_consistent = false;
                  break;
                }
                if (cmp_found && !found) {
                  fprintf(
                      stderr,
                      "Non-AttributeGroup GetEntity returns different results "
                      "than AttributeGroup GetEntity for key %s: CF %s "
                      "returns entity %s, CF %s returns not found  \n",
                      StringToHex(key).c_str(), column_family_names_[0].c_str(),
                      WideColumnsToHex(cmp_result.columns()).c_str(),
                      attribute_group.column_family()->GetName().c_str());
                  is_consistent = false;
                  break;
                }
                if (found &&
                    attribute_group.columns() != cmp_result.columns()) {
                  fprintf(
                      stderr,
                      "Non-AttributeGroup GetEntity returns different results "
                      "than AttributeGroup GetEntity for key %s: CF %s "
                      "returns entity %s, CF %s returns entity %s\n",
                      StringToHex(key).c_str(), column_family_names_[0].c_str(),
                      WideColumnsToHex(cmp_result.columns()).c_str(),
                      attribute_group.column_family()->GetName().c_str(),
                      WideColumnsToHex(attribute_group.columns()).c_str());
                  is_consistent = false;
                  break;
                }
              }
            }
          } else {
            for (size_t i = 1; i < rand_column_families.size(); ++i) {
              assert(rand_column_families[i] >= 0);
              assert(rand_column_families[i] <
                     static_cast<int>(column_families_.size()));

              PinnableWideColumns result;
              s = db_->GetEntity(read_opts_copy,
                                 column_families_[rand_column_families[i]], key,
                                 &result);

              if (!s.ok() && !s.IsNotFound()) {
                break;
              }

              const bool found = s.ok();

              assert(!column_family_names_.empty());
              assert(i < column_family_names_.size());

              if (!cmp_found && found) {
                fprintf(stderr,
                        "GetEntity returns different results for key %s: CF %s "
                        "returns not found, CF %s returns entity %s\n",
                        StringToHex(key).c_str(),
                        column_family_names_[0].c_str(),
                        column_family_names_[i].c_str(),
                        WideColumnsToHex(result.columns()).c_str());
                is_consistent = false;
                break;
              }

              if (cmp_found && !found) {
                fprintf(stderr,
                        "GetEntity returns different results for key %s: CF %s "
                        "returns entity %s, CF %s returns not found\n",
                        StringToHex(key).c_str(),
                        column_family_names_[0].c_str(),
                        WideColumnsToHex(cmp_result.columns()).c_str(),
                        column_family_names_[i].c_str());
                is_consistent = false;
                break;
              }

              if (found && result != cmp_result) {
                fprintf(stderr,
                        "GetEntity returns different results for key %s: CF %s "
                        "returns entity %s, CF %s returns entity %s\n",
                        StringToHex(key).c_str(),
                        column_family_names_[0].c_str(),
                        WideColumnsToHex(cmp_result.columns()).c_str(),
                        column_family_names_[i].c_str(),
                        WideColumnsToHex(result.columns()).c_str());
                is_consistent = false;
                break;
              }
            }
          }
        }
      }

      //  Enable back error injection disabled for verification
      if (fault_fs_guard) {
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }
    }

    if (!is_consistent) {
      fprintf(stderr, "TestGetEntity error: results are not consistent\n");
      thread->stats.AddErrors(1);
      // Fail fast to preserve the DB state.
      thread->shared->SetVerificationFailure();
    } else if (s.ok()) {
      thread->stats.AddGets(1, 1);
    } else if (s.IsNotFound()) {
      thread->stats.AddGets(1, 0);
    } else if (!IsErrorInjectedAndRetryable(s)) {
      fprintf(stderr, "TestGetEntity error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    }
  }

  void TestMultiGetEntity(ThreadState* thread, const ReadOptions& read_opts,
                          const std::vector<int>& rand_column_families,
                          const std::vector<int64_t>& rand_keys) override {
    assert(thread);
    assert(thread->shared);
    assert(!rand_column_families.empty());
    assert(!rand_keys.empty());

    ManagedSnapshot snapshot_guard(db_);

    ReadOptions read_opts_copy = read_opts;
    read_opts_copy.snapshot = snapshot_guard.snapshot();

    const size_t num_cfs = rand_column_families.size();

    std::vector<ColumnFamilyHandle*> cfhs;
    cfhs.reserve(num_cfs);

    for (size_t j = 0; j < num_cfs; ++j) {
      assert(rand_column_families[j] >= 0);
      assert(rand_column_families[j] <
             static_cast<int>(column_families_.size()));

      ColumnFamilyHandle* const cfh = column_families_[rand_column_families[j]];
      assert(cfh);

      cfhs.emplace_back(cfh);
    }

    const size_t num_keys = rand_keys.size();

    if (FLAGS_use_attribute_group) {
      // AttributeGroup MultiGetEntity verification

      std::vector<PinnableAttributeGroups> results;
      std::vector<Slice> key_slices;
      std::vector<std::string> key_strs;
      results.reserve(num_keys);
      key_slices.reserve(num_keys);
      key_strs.reserve(num_keys);

      for (size_t i = 0; i < num_keys; ++i) {
        key_strs.emplace_back(Key(rand_keys[i]));
        key_slices.emplace_back(key_strs.back());
        PinnableAttributeGroups attribute_groups;
        for (auto* cfh : cfhs) {
          attribute_groups.emplace_back(cfh);
        }
        results.emplace_back(std::move(attribute_groups));
      }
      db_->MultiGetEntity(read_opts_copy, num_keys, key_slices.data(),
                          results.data());

      bool is_consistent = true;

      for (size_t i = 0; i < num_keys; ++i) {
        const auto& result = results[i];
        const Status& cmp_s = result[0].status();
        const WideColumns& cmp_columns = result[0].columns();

        bool has_error = false;

        for (size_t j = 0; j < num_cfs; ++j) {
          const Status& s = result[j].status();
          const WideColumns& columns = result[j].columns();
          if (!s.ok() && IsErrorInjectedAndRetryable(s)) {
            break;
          } else if (!s.ok() && !s.IsNotFound()) {
            fprintf(stderr, "TestMultiGetEntity (AttributeGroup) error: %s\n",
                    s.ToString().c_str());
            thread->stats.AddErrors(1);
            has_error = true;
            break;
          }

          assert(cmp_s.ok() || cmp_s.IsNotFound());

          if (s.IsNotFound()) {
            if (cmp_s.ok()) {
              fprintf(stderr,
                      "MultiGetEntity (AttributeGroup) returns different "
                      "results for key %s: CF %s "
                      "returns entity %s, CF %s returns not found\n",
                      key_slices[i].ToString(true).c_str(),
                      column_family_names_[0].c_str(),
                      WideColumnsToHex(cmp_columns).c_str(),
                      column_family_names_[j].c_str());
              is_consistent = false;
              break;
            }

            continue;
          }

          assert(s.ok());
          if (cmp_s.IsNotFound()) {
            fprintf(stderr,
                    "MultiGetEntity (AttributeGroup) returns different results "
                    "for key %s: CF %s "
                    "returns not found, CF %s returns entity %s\n",
                    key_slices[i].ToString(true).c_str(),
                    column_family_names_[0].c_str(),
                    column_family_names_[j].c_str(),
                    WideColumnsToHex(columns).c_str());
            is_consistent = false;
            break;
          }

          if (columns != cmp_columns) {
            fprintf(stderr,
                    "MultiGetEntity (AttributeGroup) returns different results "
                    "for key %s: CF %s "
                    "returns entity %s, CF %s returns entity %s\n",
                    key_slices[i].ToString(true).c_str(),
                    column_family_names_[0].c_str(),
                    WideColumnsToHex(cmp_columns).c_str(),
                    column_family_names_[j].c_str(),
                    WideColumnsToHex(columns).c_str());
            is_consistent = false;
            break;
          }

          if (!VerifyWideColumns(columns)) {
            fprintf(stderr,
                    "MultiGetEntity (AttributeGroup) error: inconsistent "
                    "columns for key %s, "
                    "entity %s\n",
                    key_slices[i].ToString(true).c_str(),
                    WideColumnsToHex(columns).c_str());
            is_consistent = false;
            break;
          }
        }
        if (has_error) {
          break;
        } else if (!is_consistent) {
          fprintf(stderr,
                  "TestMultiGetEntity (AttributeGroup) error: results are not "
                  "consistent\n");
          thread->stats.AddErrors(1);
          // Fail fast to preserve the DB state.
          thread->shared->SetVerificationFailure();
          break;
        } else if (cmp_s.ok()) {
          thread->stats.AddGets(1, 1);
        } else if (cmp_s.IsNotFound()) {
          thread->stats.AddGets(1, 0);
        }
      }

    } else {
      // Non-AttributeGroup MultiGetEntity verification

      for (size_t i = 0; i < num_keys; ++i) {
        const std::string key = Key(rand_keys[i]);

        std::vector<Slice> key_slices(num_cfs, key);
        std::vector<PinnableWideColumns> results(num_cfs);
        std::vector<Status> statuses(num_cfs);

        db_->MultiGetEntity(read_opts_copy, num_cfs, cfhs.data(),
                            key_slices.data(), results.data(), statuses.data());

        bool is_consistent = true;

        const Status& cmp_s = statuses[0];
        const WideColumns& cmp_columns = results[0].columns();

        for (size_t j = 0; j < num_cfs; ++j) {
          const Status& s = statuses[j];
          const WideColumns& columns = results[j].columns();

          if (!s.ok() && IsErrorInjectedAndRetryable(s)) {
            break;
          } else if (!s.ok() && !s.IsNotFound()) {
            fprintf(stderr, "TestMultiGetEntity error: %s\n",
                    s.ToString().c_str());
            thread->stats.AddErrors(1);
            break;
          }

          assert(cmp_s.ok() || cmp_s.IsNotFound());

          if (s.IsNotFound()) {
            if (cmp_s.ok()) {
              fprintf(
                  stderr,
                  "MultiGetEntity returns different results for key %s: CF %s "
                  "returns entity %s, CF %s returns not found\n",
                  StringToHex(key).c_str(), column_family_names_[0].c_str(),
                  WideColumnsToHex(cmp_columns).c_str(),
                  column_family_names_[j].c_str());
              is_consistent = false;
              break;
            }

            continue;
          }

          assert(s.ok());
          if (cmp_s.IsNotFound()) {
            fprintf(
                stderr,
                "MultiGetEntity returns different results for key %s: CF %s "
                "returns not found, CF %s returns entity %s\n",
                StringToHex(key).c_str(), column_family_names_[0].c_str(),
                column_family_names_[j].c_str(),
                WideColumnsToHex(columns).c_str());
            is_consistent = false;
            break;
          }

          if (columns != cmp_columns) {
            fprintf(
                stderr,
                "MultiGetEntity returns different results for key %s: CF %s "
                "returns entity %s, CF %s returns entity %s\n",
                StringToHex(key).c_str(), column_family_names_[0].c_str(),
                WideColumnsToHex(cmp_columns).c_str(),
                column_family_names_[j].c_str(),
                WideColumnsToHex(columns).c_str());
            is_consistent = false;
            break;
          }

          if (!VerifyWideColumns(columns)) {
            fprintf(stderr,
                    "MultiGetEntity error: inconsistent columns for key %s, "
                    "entity %s\n",
                    StringToHex(key).c_str(),
                    WideColumnsToHex(columns).c_str());
            is_consistent = false;
            break;
          }
        }

        if (!is_consistent) {
          fprintf(stderr,
                  "TestMultiGetEntity error: results are not consistent\n");
          thread->stats.AddErrors(1);
          // Fail fast to preserve the DB state.
          thread->shared->SetVerificationFailure();
          break;
        } else if (statuses[0].ok()) {
          thread->stats.AddGets(1, 1);
        } else if (statuses[0].IsNotFound()) {
          thread->stats.AddGets(1, 0);
        }
      }
    }
  }

  Status TestPrefixScan(ThreadState* thread, const ReadOptions& readoptions,
                        const std::vector<int>& rand_column_families,
                        const std::vector<int64_t>& rand_keys) override {
    assert(!rand_column_families.empty());
    assert(!rand_keys.empty());

    const std::string key = Key(rand_keys[0]);

    const size_t prefix_to_use =
        (FLAGS_prefix_size < 0) ? 7 : static_cast<size_t>(FLAGS_prefix_size);

    const Slice prefix(key.data(), prefix_to_use);

    std::string upper_bound;
    Slice ub_slice;

    ReadOptions ro_copy = readoptions;

    // Get the next prefix first and then see if we want to set upper bound.
    // We'll use the next prefix in an assertion later on
    if (GetNextPrefix(prefix, &upper_bound) && thread->rand.OneIn(2)) {
      ub_slice = Slice(upper_bound);
      ro_copy.iterate_upper_bound = &ub_slice;
      if (FLAGS_use_sqfc_for_range_queries) {
        ro_copy.table_filter =
            sqfc_factory_->GetTableFilterForRangeQuery(prefix, ub_slice);
      }
    }

    ColumnFamilyHandle* const cfh =
        column_families_[rand_column_families[thread->rand.Uniform(
            static_cast<int>(rand_column_families.size()))]];
    assert(cfh);

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro_copy, cfh));

    uint64_t count = 0;
    Status s;

    for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix);
         iter->Next()) {
      ++count;

      if (!VerifyWideColumns(iter->value(), iter->columns())) {
        s = Status::Corruption("Value and columns inconsistent",
                               DebugString(iter->value(), iter->columns()));
        break;
      }
    }

    assert(prefix_to_use == 0 ||
           count <= GetPrefixKeyCount(prefix.ToString(), upper_bound));

    if (s.ok()) {
      s = iter->status();
    }

    if (!s.ok() && !IsErrorInjectedAndRetryable(s)) {
      fprintf(stderr, "TestPrefixScan error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);

      return s;
    }

    thread->stats.AddPrefixes(1, count);

    return Status::OK();
  }

  ColumnFamilyHandle* GetControlCfh(ThreadState* thread,
                                    int /*column_family_id*/
                                    ) override {
    // All column families should contain the same data. Randomly pick one.
    return column_families_[thread->rand.Next() % column_families_.size()];
  }

  void VerifyDb(ThreadState* thread) const override {
    // This `ReadOptions` is for validation purposes. Ignore
    // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
    ReadOptions options(FLAGS_verify_checksum, true);

    // We must set total_order_seek to true because we are doing a SeekToFirst
    // on a column family whose memtables may support (by default) prefix-based
    // iterator. In this case, NewIterator with options.total_order_seek being
    // false returns a prefix-based iterator. Calling SeekToFirst using this
    // iterator causes the iterator to become invalid. That means we cannot
    // iterate the memtable using this iterator any more, although the memtable
    // contains the most up-to-date key-values.
    options.total_order_seek = true;

    ManagedSnapshot snapshot_guard(db_);
    options.snapshot = snapshot_guard.snapshot();

    const size_t num = column_families_.size();

    std::vector<std::unique_ptr<Iterator>> iters;
    iters.reserve(num);

    for (size_t i = 0; i < num; ++i) {
      iters.emplace_back(db_->NewIterator(options, column_families_[i]));
      iters.back()->SeekToFirst();
    }

    std::vector<Status> statuses(num, Status::OK());

    assert(thread);

    auto shared = thread->shared;
    assert(shared);

    do {
      if (shared->HasVerificationFailedYet()) {
        break;
      }

      size_t valid_cnt = 0;

      for (size_t i = 0; i < num; ++i) {
        const auto& iter = iters[i];
        assert(iter);

        if (iter->Valid()) {
          if (!VerifyWideColumns(iter->value(), iter->columns())) {
            statuses[i] =
                Status::Corruption("Value and columns inconsistent",
                                   DebugString(iter->value(), iter->columns()));
          } else {
            ++valid_cnt;
          }
        } else {
          statuses[i] = iter->status();
        }
      }

      if (valid_cnt == 0) {
        for (size_t i = 0; i < num; ++i) {
          const auto& s = statuses[i];
          if (!s.ok()) {
            fprintf(stderr, "Iterator on cf %s has error: %s\n",
                    column_families_[i]->GetName().c_str(),
                    s.ToString().c_str());
            shared->SetVerificationFailure();
          }
        }

        break;
      }

      if (valid_cnt < num) {
        shared->SetVerificationFailure();

        for (size_t i = 0; i < num; ++i) {
          assert(iters[i]);

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
      assert(valid_cnt == num);

      if (shared->PrintingVerificationResults()) {
        continue;
      }

      assert(iters[0]);

      const Slice key = iters[0]->key();
      const Slice value = iters[0]->value();

      int num_mismatched_cfs = 0;

      for (size_t i = 1; i < num; ++i) {
        assert(iters[i]);

        const int cmp = key.compare(iters[i]->key());

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

          Slice begin_key;
          Slice end_key;
          if (cmp < 0) {
            begin_key = key;
            end_key = iters[i]->key();
          } else {
            begin_key = iters[i]->key();
            end_key = key;
          }

          const auto print_key_versions = [&](ColumnFamilyHandle* cfh) {
            constexpr size_t kMaxNumIKeys = 8;

            std::vector<KeyVersion> versions;
            const Status s = GetAllKeyVersions(db_, cfh, begin_key, end_key,
                                               kMaxNumIKeys, &versions);
            if (!s.ok()) {
              fprintf(stderr, "%s\n", s.ToString().c_str());
              return;
            }

            assert(cfh);

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

          shared->SetVerificationFailure();
        }
      }

      shared->FinishPrintingVerificationResults();

      for (auto& iter : iters) {
        assert(iter);
        iter->Next();
      }
    } while (true);
  }

  void ContinuouslyVerifyDb(ThreadState* thread) const override {
    assert(thread);
    Status status;

    DB* db_ptr = cmp_db_ ? cmp_db_ : db_;
    const auto& cfhs = cmp_db_ ? cmp_cfhs_ : column_families_;

    // Take a snapshot to preserve the state of primary db.
    ManagedSnapshot snapshot_guard(db_);

    SharedState* shared = thread->shared;
    assert(shared);

    if (cmp_db_) {
      status = cmp_db_->TryCatchUpWithPrimary();
      if (!status.ok()) {
        fprintf(stderr, "TryCatchUpWithPrimary: %s\n",
                status.ToString().c_str());
        shared->SetShouldStopTest();
        assert(false);
        return;
      }
    }

    const auto checksum_column_family = [](Iterator* iter,
                                           uint32_t* checksum) -> Status {
      assert(nullptr != checksum);

      uint32_t ret = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ret = crc32c::Extend(ret, iter->key().data(), iter->key().size());
        ret = crc32c::Extend(ret, iter->value().data(), iter->value().size());

        for (const auto& column : iter->columns()) {
          ret = crc32c::Extend(ret, column.name().data(), column.name().size());
          ret =
              crc32c::Extend(ret, column.value().data(), column.value().size());
        }
      }

      *checksum = ret;
      return iter->status();
    };
    // This `ReadOptions` is for validation purposes. Ignore
    // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
    ReadOptions ropts(FLAGS_verify_checksum, true);
    ropts.total_order_seek = true;
    if (nullptr == cmp_db_) {
      ropts.snapshot = snapshot_guard.snapshot();
    }
    uint32_t crc = 0;
    {
      // Compute crc for all key-values of default column family.
      std::unique_ptr<Iterator> it(db_ptr->NewIterator(ropts));
      status = checksum_column_family(it.get(), &crc);
      if (!status.ok()) {
        fprintf(stderr, "Computing checksum of default cf: %s\n",
                status.ToString().c_str());
        assert(false);
      }
    }
    // Since we currently intentionally disallow reading from the secondary
    // instance with snapshot, we cannot achieve cross-cf consistency if WAL is
    // enabled because there is no guarantee that secondary instance replays
    // the primary's WAL to a consistent point where all cfs have the same
    // data.
    if (status.ok() && FLAGS_disable_wal) {
      uint32_t tmp_crc = 0;
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
      if (!status.ok()) {
        fprintf(stderr, "status: %s\n", status.ToString().c_str());
        shared->SetShouldStopTest();
        assert(false);
      } else if (tmp_crc != crc) {
        fprintf(stderr, "tmp_crc=%" PRIu32 " crc=%" PRIu32 "\n", tmp_crc, crc);
        shared->SetShouldStopTest();
        assert(false);
      }
    }
  }

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
  std::atomic<uint32_t> batch_id_;
};

StressTest* CreateCfConsistencyStressTest() {
  return new CfConsistencyStressTest();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
