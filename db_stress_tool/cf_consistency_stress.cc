//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef GFLAGS
#include <algorithm>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

#include "db/wide/wide_columns_helper.h"
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
    DebugOpKind debug_op = DebugOpKind::kPut;
    uint64_t write_unix_time = 0;

    WriteBatch batch;

    Status status;
    if (FLAGS_use_attribute_group && FLAGS_use_put_entity_one_in > 0 &&
        (value_base % FLAGS_use_put_entity_one_in) == 0) {
      debug_op = DebugOpKind::kAttributeGroupPutEntity;
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
          debug_op = DebugOpKind::kPutEntity;
          status = batch.PutEntity(cfh, k, GenerateWideColumns(value_base, v));
        } else if (FLAGS_use_timed_put_one_in > 0 &&
                   ((value_base + kLargePrimeForCommonFactorSkew) %
                    FLAGS_use_timed_put_one_in) == 0) {
          debug_op = DebugOpKind::kTimedPut;
          write_unix_time = GetWriteUnixTime(thread);
          status = batch.TimedPut(cfh, k, v, write_unix_time);
        } else if (FLAGS_use_merge) {
          debug_op = DebugOpKind::kMerge;
          status = batch.Merge(cfh, k, v);
        } else {
          status = batch.Put(cfh, k, v);
        }
        if (!status.ok()) {
          break;
        }
      }
    }

    const SequenceNumber latest_seq_before = db_->GetLatestSequenceNumber();
    const uint32_t batch_count = batch.Count();

    if (status.ok()) {
      status = db_->Write(write_opts, &batch);
    }

    if (status.ok()) {
      const SequenceNumber latest_seq_after = db_->GetLatestSequenceNumber();
      RecordDebugEvent({debug_op, k,
                        /* end_key */ "", latest_seq_before, latest_seq_after,
                        batch_count, value_base, write_unix_time,
                        rand_column_families});
      if (latest_seq_after <
          latest_seq_before + static_cast<SequenceNumber>(batch_count)) {
        ReportInvalidWriteSequenceBounds(thread, k, latest_seq_before,
                                         latest_seq_after, batch_count,
                                         value_base);
      }
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
    const SequenceNumber latest_seq_before = db_->GetLatestSequenceNumber();
    Status s = db_->Write(write_opts, &batch);
    if (s.ok()) {
      const SequenceNumber latest_seq_after = db_->GetLatestSequenceNumber();
      RecordDebugEvent({DebugOpKind::kDelete, key_str,
                        /* end_key */ "", latest_seq_before, latest_seq_after,
                        batch.Count(),
                        /* value_base */ 0,
                        /* write_unix_time */ 0, rand_column_families});
      if (latest_seq_after <
          latest_seq_before + static_cast<SequenceNumber>(batch.Count())) {
        ReportInvalidWriteSequenceBounds(thread, key_str, latest_seq_before,
                                         latest_seq_after, batch.Count(),
                                         /* value_base */ 0);
      }
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
    const SequenceNumber latest_seq_before = db_->GetLatestSequenceNumber();
    Status s = db_->Write(write_opts, &batch);
    if (s.ok()) {
      const SequenceNumber latest_seq_after = db_->GetLatestSequenceNumber();
      RecordDebugEvent({DebugOpKind::kDeleteRange, key_str, end_key_str,
                        latest_seq_before, latest_seq_after, batch.Count(),
                        /* value_base */ 0,
                        /* write_unix_time */ 0, rand_column_families});
      if (latest_seq_after <
          latest_seq_before + static_cast<SequenceNumber>(batch.Count())) {
        ReportInvalidWriteSequenceBounds(thread, key_str, latest_seq_before,
                                         latest_seq_after, batch.Count(),
                                         /* value_base */ 0);
      }
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
      const SequenceNumber snapshot_seq =
          snapshot_guard.snapshot()->GetSequenceNumber();

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
                        "returns not found, CF %s returns entity %s"
                        " [snapshot_seq=%" PRIu64 " latest_seq=%" PRIu64 "]\n",
                        StringToHex(key).c_str(),
                        column_family_names_[0].c_str(),
                        column_family_names_[i].c_str(),
                        WideColumnsToHex(result.columns()).c_str(),
                        snapshot_seq, db_->GetLatestSequenceNumber());
                DumpGetEntityMismatchDebug(key, read_opts_copy, snapshot_seq,
                                           rand_column_families,
                                           /* cmp_cf_idx */ 0,
                                           /* mismatch_cf_idx */ i);
                is_consistent = false;
                break;
              }

              if (cmp_found && !found) {
                fprintf(stderr,
                        "GetEntity returns different results for key %s: CF %s "
                        "returns entity %s, CF %s returns not found"
                        " [snapshot_seq=%" PRIu64 " latest_seq=%" PRIu64 "]\n",
                        StringToHex(key).c_str(),
                        column_family_names_[0].c_str(),
                        WideColumnsToHex(cmp_result.columns()).c_str(),
                        column_family_names_[i].c_str(), snapshot_seq,
                        db_->GetLatestSequenceNumber());
                DumpGetEntityMismatchDebug(key, read_opts_copy, snapshot_seq,
                                           rand_column_families,
                                           /* cmp_cf_idx */ 0,
                                           /* mismatch_cf_idx */ i);
                is_consistent = false;
                break;
              }

              if (found && result != cmp_result) {
                fprintf(stderr,
                        "GetEntity returns different results for key %s: CF %s "
                        "returns entity %s, CF %s returns entity %s"
                        " [snapshot_seq=%" PRIu64 " latest_seq=%" PRIu64 "]\n",
                        StringToHex(key).c_str(),
                        column_family_names_[0].c_str(),
                        WideColumnsToHex(cmp_result.columns()).c_str(),
                        column_family_names_[i].c_str(),
                        WideColumnsToHex(result.columns()).c_str(),
                        snapshot_seq, db_->GetLatestSequenceNumber());
                DumpGetEntityMismatchDebug(key, read_opts_copy, snapshot_seq,
                                           rand_column_families,
                                           /* cmp_cf_idx */ 0,
                                           /* mismatch_cf_idx */ i);
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
                  "returns entity %s, CF %s returns not found"
                  " [snapshot_seq=%" PRIu64 " latest_seq=%" PRIu64 "]\n",
                  StringToHex(key).c_str(), column_family_names_[0].c_str(),
                  WideColumnsToHex(cmp_columns).c_str(),
                  column_family_names_[j].c_str(),
                  snapshot_guard.snapshot()->GetSequenceNumber(),
                  db_->GetLatestSequenceNumber());
              DumpGetEntityMismatchDebug(
                  key, read_opts_copy,
                  snapshot_guard.snapshot()->GetSequenceNumber(),
                  rand_column_families, /* cmp_cf_idx */ 0,
                  /* mismatch_cf_idx */ j);
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
                "returns not found, CF %s returns entity %s"
                " [snapshot_seq=%" PRIu64 " latest_seq=%" PRIu64 "]\n",
                StringToHex(key).c_str(), column_family_names_[0].c_str(),
                column_family_names_[j].c_str(),
                WideColumnsToHex(columns).c_str(),
                snapshot_guard.snapshot()->GetSequenceNumber(),
                db_->GetLatestSequenceNumber());
            DumpGetEntityMismatchDebug(
                key, read_opts_copy,
                snapshot_guard.snapshot()->GetSequenceNumber(),
                rand_column_families, /* cmp_cf_idx */ 0,
                /* mismatch_cf_idx */ j);
            is_consistent = false;
            break;
          }

          if (columns != cmp_columns) {
            fprintf(
                stderr,
                "MultiGetEntity returns different results for key %s: CF %s "
                "returns entity %s, CF %s returns entity %s"
                " [snapshot_seq=%" PRIu64 " latest_seq=%" PRIu64 "]\n",
                StringToHex(key).c_str(), column_family_names_[0].c_str(),
                WideColumnsToHex(cmp_columns).c_str(),
                column_family_names_[j].c_str(),
                WideColumnsToHex(columns).c_str(),
                snapshot_guard.snapshot()->GetSequenceNumber(),
                db_->GetLatestSequenceNumber());
            DumpGetEntityMismatchDebug(
                key, read_opts_copy,
                snapshot_guard.snapshot()->GetSequenceNumber(),
                rand_column_families, /* cmp_cf_idx */ 0,
                /* mismatch_cf_idx */ j);
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
    std::unique_ptr<ManagedSnapshot> snapshot = nullptr;
    if (ro_copy.auto_refresh_iterator_with_snapshot) {
      snapshot = std::make_unique<ManagedSnapshot>(db_);
      ro_copy.snapshot = snapshot->snapshot();
    }

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
    options.auto_refresh_iterator_with_snapshot =
        FLAGS_auto_refresh_iterator_with_snapshot;

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

    DB* db_ptr = secondary_db_ ? secondary_db_.get() : db_;
    const auto& cfhs = secondary_db_ ? secondary_cfhs_ : column_families_;

    // Take a snapshot to preserve the state of primary db.
    ManagedSnapshot snapshot_guard(db_);

    SharedState* shared = thread->shared;
    assert(shared);

    if (secondary_db_) {
      status = secondary_db_->TryCatchUpWithPrimary();
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
    if (nullptr == secondary_db_ || FLAGS_auto_refresh_iterator_with_snapshot) {
      ropts.snapshot = snapshot_guard.snapshot();
      ropts.auto_refresh_iterator_with_snapshot = true;
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
  enum class DebugOpKind : uint8_t {
    kPut,
    kPutEntity,
    kAttributeGroupPutEntity,
    kTimedPut,
    kMerge,
    kDelete,
    kDeleteRange,
  };

  struct DebugEvent {
    DebugOpKind op;
    std::string key;
    std::string end_key;
    // [seq_before, seq_after] is the observed latest-sequence window around
    // the write. Exact per-CF sequence mapping is only available when
    // seq_after == seq_before + batch_count.
    SequenceNumber seq_before;
    SequenceNumber seq_after;
    uint32_t batch_count;
    uint32_t value_base;
    uint64_t write_unix_time;
    std::vector<int> cfs;
  };

  static constexpr size_t kMaxRecentDebugEvents = 512;
  static const char* DebugOpKindName(DebugOpKind op) {
    switch (op) {
      case DebugOpKind::kPut:
        return "Put";
      case DebugOpKind::kPutEntity:
        return "PutEntity";
      case DebugOpKind::kAttributeGroupPutEntity:
        return "AttributeGroupPutEntity";
      case DebugOpKind::kTimedPut:
        return "TimedPut";
      case DebugOpKind::kMerge:
        return "Merge";
      case DebugOpKind::kDelete:
        return "Delete";
      case DebugOpKind::kDeleteRange:
        return "DeleteRange";
    }
    return "Unknown";
  }

  std::string DebugCfName(int cf) const {
    if (cf >= 0 && static_cast<size_t>(cf) < column_family_names_.size()) {
      return column_family_names_[cf] + "#" + std::to_string(cf);
    }
    return "cf#" + std::to_string(cf);
  }

  void RecordDebugEvent(DebugEvent event) {
    std::lock_guard<std::mutex> lock(debug_mu_);
    recent_debug_events_.emplace_back(std::move(event));
    if (recent_debug_events_.size() > kMaxRecentDebugEvents) {
      recent_debug_events_.pop_front();
    }
  }

  static bool EventTouchesKey(const DebugEvent& event, const std::string& key) {
    if (event.op == DebugOpKind::kDeleteRange) {
      return event.key <= key && key < event.end_key;
    }
    return event.key == key;
  }

  void ReportInvalidWriteSequenceBounds(ThreadState* thread,
                                        const std::string& key,
                                        SequenceNumber latest_seq_before,
                                        SequenceNumber latest_seq_after,
                                        uint32_t batch_count,
                                        uint32_t value_base) {
    fprintf(stderr,
            "Write sequence bounds invalid for key %s: latest_before=%" PRIu64
            " batch_count=%u latest_after=%" PRIu64 " value_base=%u\n",
            Slice(key).ToString(true).c_str(), latest_seq_before, batch_count,
            latest_seq_after, value_base);
    thread->stats.AddErrors(1);
    thread->shared->SetVerificationFailure();
  }

  static bool HasExactCfSequenceMapping(const DebugEvent& event) {
    return event.seq_after ==
           event.seq_before + static_cast<SequenceNumber>(event.batch_count);
  }

  bool TryGetEventSequenceForCf(const DebugEvent& event, int cf,
                                SequenceNumber* seq) const {
    if (!HasExactCfSequenceMapping(event)) {
      return false;
    }
    const size_t count =
        std::min(event.cfs.size(), static_cast<size_t>(event.batch_count));
    for (size_t i = 0; i < count; ++i) {
      if (event.cfs[i] == cf) {
        *seq = event.seq_before + static_cast<SequenceNumber>(i + 1);
        return true;
      }
    }
    return false;
  }

  std::string FormatCfSequenceSummary(const DebugEvent& event,
                                      SequenceNumber snapshot_seq) const {
    if (!HasExactCfSequenceMapping(event)) {
      return "interleaved-with-other-writes";
    }
    const size_t count =
        std::min(event.cfs.size(), static_cast<size_t>(event.batch_count));
    std::string summary;
    for (size_t i = 0; i < count; ++i) {
      if (!summary.empty()) {
        summary.append(", ");
      }
      const SequenceNumber seq =
          event.seq_before + static_cast<SequenceNumber>(i + 1);
      summary.append(DebugCfName(event.cfs[i]));
      summary.push_back('@');
      summary.append(std::to_string(seq));
      summary.append(seq <= snapshot_seq ? ":visible" : ":hidden");
    }
    if (event.batch_count != event.cfs.size()) {
      if (!summary.empty()) {
        summary.push_back(' ');
      }
      summary.append("(batch_count=");
      summary.append(std::to_string(event.batch_count));
      summary.append(", cf_count=");
      summary.append(std::to_string(event.cfs.size()));
      summary.push_back(')');
    }
    return summary;
  }

  static std::string FormatValueResult(const Status& status,
                                       const std::string& value) {
    if (status.ok()) {
      return Slice(value).ToString(true);
    }
    if (status.IsNotFound()) {
      return "not found";
    }
    return status.ToString();
  }

  static std::string FormatEntityResult(const Status& status,
                                        const PinnableWideColumns& columns) {
    if (status.ok()) {
      return WideColumnsToHex(columns.columns());
    }
    if (status.IsNotFound()) {
      return "not found";
    }
    return status.ToString();
  }

  void DumpRecentDebugEvents(const std::string& key,
                             SequenceNumber snapshot_seq, int cmp_cf,
                             int mismatch_cf) const {
    std::vector<DebugEvent> matched_events;
    {
      std::lock_guard<std::mutex> lock(debug_mu_);
      for (const auto& event : recent_debug_events_) {
        if (EventTouchesKey(event, key)) {
          matched_events.push_back(event);
        }
      }
    }

    fprintf(stdout,
            "[cf_consistency_debug] recent_events key=%s snapshot_seq=%" PRIu64
            " matched_events=%" ROCKSDB_PRIszt "\n",
            StringToHex(key).c_str(), snapshot_seq, matched_events.size());

    for (const auto& event : matched_events) {
      SequenceNumber cmp_seq = 0;
      SequenceNumber mismatch_seq = 0;
      const bool has_cmp_seq =
          TryGetEventSequenceForCf(event, cmp_cf, &cmp_seq);
      const bool has_mismatch_seq =
          TryGetEventSequenceForCf(event, mismatch_cf, &mismatch_seq);
      const bool snapshot_splits_focus_cfs = has_cmp_seq && has_mismatch_seq &&
                                             cmp_seq <= snapshot_seq &&
                                             snapshot_seq < mismatch_seq;

      fprintf(stdout,
              "[cf_consistency_debug] event op=%s key=%s end_key=%s "
              "seq_before=%" PRIu64 " seq_after=%" PRIu64
              " batch_count=%u exact_mapping=%s value_base=%u "
              "write_unix_time=%" PRIu64
              " focus_split=%s focus_cmp_seq=%s "
              "focus_mismatch_seq=%s per_cf=%s\n",
              DebugOpKindName(event.op), StringToHex(event.key).c_str(),
              event.end_key.empty() ? "-" : StringToHex(event.end_key).c_str(),
              event.seq_before, event.seq_after, event.batch_count,
              HasExactCfSequenceMapping(event) ? "true" : "false",
              event.value_base, event.write_unix_time,
              snapshot_splits_focus_cfs ? "true" : "false",
              has_cmp_seq ? std::to_string(cmp_seq).c_str() : "-",
              has_mismatch_seq ? std::to_string(mismatch_seq).c_str() : "-",
              FormatCfSequenceSummary(event, snapshot_seq).c_str());
    }
  }

  void DumpGetEntityMismatchDebug(const std::string& key,
                                  const ReadOptions& snapshot_read_opts,
                                  SequenceNumber snapshot_seq,
                                  const std::vector<int>& rand_column_families,
                                  size_t cmp_cf_idx, size_t mismatch_cf_idx) {
    fprintf(stdout,
            "[cf_consistency_debug] begin key=%s snapshot_seq=%" PRIu64
            " latest_seq=%" PRIu64 " cmp_cf=%s mismatch_cf=%s\n",
            StringToHex(key).c_str(), snapshot_seq,
            db_->GetLatestSequenceNumber(),
            DebugCfName(rand_column_families[cmp_cf_idx]).c_str(),
            DebugCfName(rand_column_families[mismatch_cf_idx]).c_str());

    ReadOptions latest_read_opts(snapshot_read_opts);
    latest_read_opts.snapshot = nullptr;

    for (int cf : rand_column_families) {
      ColumnFamilyHandle* const cfh = column_families_[cf];

      PinnableWideColumns snapshot_entity;
      const Status snapshot_entity_status =
          db_->GetEntity(snapshot_read_opts, cfh, key, &snapshot_entity);
      std::string snapshot_value;
      const Status snapshot_value_status =
          db_->Get(snapshot_read_opts, cfh, key, &snapshot_value);

      PinnableWideColumns latest_entity;
      const Status latest_entity_status =
          db_->GetEntity(latest_read_opts, cfh, key, &latest_entity);
      std::string latest_value;
      const Status latest_value_status =
          db_->Get(latest_read_opts, cfh, key, &latest_value);

      std::string snapshot_verify = "n/a";
      if (snapshot_entity_status.ok()) {
        snapshot_verify =
            VerifyWideColumns(snapshot_entity.columns()) ? "true" : "false";
      }

      std::string latest_verify = "n/a";
      if (latest_entity_status.ok()) {
        latest_verify =
            VerifyWideColumns(latest_entity.columns()) ? "true" : "false";
      }

      std::string snapshot_default_matches_get = "n/a";
      if (snapshot_entity_status.ok() && snapshot_value_status.ok()) {
        snapshot_default_matches_get =
            WideColumnsHelper::GetDefaultColumn(snapshot_entity.columns()) ==
                    Slice(snapshot_value)
                ? "true"
                : "false";
      }

      std::string latest_default_matches_get = "n/a";
      if (latest_entity_status.ok() && latest_value_status.ok()) {
        latest_default_matches_get =
            WideColumnsHelper::GetDefaultColumn(latest_entity.columns()) ==
                    Slice(latest_value)
                ? "true"
                : "false";
      }

      fprintf(
          stdout,
          "[cf_consistency_debug] cf=%s snapshot_get_entity=%s "
          "snapshot_entity_verify=%s snapshot_get=%s "
          "snapshot_default_matches_get=%s latest_get_entity=%s "
          "latest_entity_verify=%s latest_get=%s "
          "latest_default_matches_get=%s\n",
          DebugCfName(cf).c_str(),
          FormatEntityResult(snapshot_entity_status, snapshot_entity).c_str(),
          snapshot_verify.c_str(),
          FormatValueResult(snapshot_value_status, snapshot_value).c_str(),
          snapshot_default_matches_get.c_str(),
          FormatEntityResult(latest_entity_status, latest_entity).c_str(),
          latest_verify.c_str(),
          FormatValueResult(latest_value_status, latest_value).c_str(),
          latest_default_matches_get.c_str());
    }

    DumpRecentDebugEvents(key, snapshot_seq, rand_column_families[cmp_cf_idx],
                          rand_column_families[mismatch_cf_idx]);
    fprintf(stdout, "[cf_consistency_debug] end key=%s\n",
            StringToHex(key).c_str());
    fflush(stdout);
  }

  std::atomic<uint32_t> batch_id_;
  mutable std::mutex debug_mu_;
  std::deque<DebugEvent> recent_debug_events_;
};

StressTest* CreateCfConsistencyStressTest() {
  return new CfConsistencyStressTest();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
