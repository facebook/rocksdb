//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#ifdef GFLAGS
#include "db_stress_tool/db_stress_common.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {
void ThreadBody(void* v) {
  ThreadStatusUtil::RegisterThread(db_stress_env, ThreadStatus::USER);
  ThreadState* thread = reinterpret_cast<ThreadState*>(v);
  SharedState* shared = thread->shared;

  if (!FLAGS_skip_verifydb && shared->ShouldVerifyAtBeginning()) {
    thread->shared->GetStressTest()->VerifyDb(thread);
  }
  {
    MutexLock l(shared->GetMutex());
    shared->IncInitialized();
    if (shared->AllInitialized()) {
      shared->GetCondVar()->SignalAll();
    }
  }
  if (!FLAGS_verification_only) {
    {
      MutexLock l(shared->GetMutex());
      while (!shared->Started()) {
        shared->GetCondVar()->Wait();
      }
    }
    thread->shared->GetStressTest()->OperateDb(thread);
    {
      MutexLock l(shared->GetMutex());
      shared->IncOperated();
      if (shared->AllOperated()) {
        shared->GetCondVar()->SignalAll();
      }
      while (!shared->VerifyStarted()) {
        shared->GetCondVar()->Wait();
      }
    }

    if (!FLAGS_skip_verifydb) {
      thread->shared->GetStressTest()->VerifyDb(thread);
    }

    {
      MutexLock l(shared->GetMutex());
      shared->IncDone();
      if (shared->AllDone()) {
        shared->GetCondVar()->SignalAll();
      }
    }
  }

  ThreadStatusUtil::UnregisterThread();
}
bool RunStressTestImpl(SharedState* shared) {
  SystemClock* clock = db_stress_env->GetSystemClock().get();
  StressTest* stress = shared->GetStressTest();

  if (shared->ShouldVerifyAtBeginning() && FLAGS_preserve_unverified_changes) {
    Status s = InitUnverifiedSubdir(FLAGS_db);
    if (s.ok() && !FLAGS_expected_values_dir.empty()) {
      s = InitUnverifiedSubdir(FLAGS_expected_values_dir);
    }
    if (!s.ok()) {
      fprintf(stderr, "Failed to setup unverified state dir: %s\n",
              s.ToString().c_str());
      exit(1);
    }
  }

  stress->InitDb(shared);
  stress->FinishInitDb(shared);

  if (FLAGS_write_fault_one_in) {
    if (!FLAGS_sync_fault_injection) {
      // unsynced WAL loss is not supported without sync_fault_injection
      fault_fs_guard->SetDirectWritableTypes({kWalFile});
    }
    IOStatus error_msg;
    if (FLAGS_inject_error_severity <= 1 || FLAGS_inject_error_severity > 2) {
      error_msg = IOStatus::IOError("Retryable injected write error");
      error_msg.SetRetryable(true);
    } else if (FLAGS_inject_error_severity == 2) {
      error_msg = IOStatus::IOError("Fatal injected write error");
      error_msg.SetDataLoss(true);
    }
    // TODO: inject write error for other file types including
    //  MANIFEST, CURRENT, and WAL files.
    fault_fs_guard->SetRandomWriteError(
        shared->GetSeed(), FLAGS_write_fault_one_in, error_msg,
        /*inject_for_all_file_types=*/false, {FileType::kTableFile});
    fault_fs_guard->SetFilesystemDirectWritable(false);
    fault_fs_guard->EnableWriteErrorInjection();
  }
  if (FLAGS_sync_fault_injection) {
    fault_fs_guard->SetFilesystemDirectWritable(false);
  }

  uint32_t n = FLAGS_threads;
  uint64_t now = clock->NowMicros();
  fprintf(stdout, "%s Initializing worker threads\n",
          clock->TimeToString(now / 1000000).c_str());

  shared->SetThreads(n);

  if (FLAGS_compaction_thread_pool_adjust_interval > 0) {
    shared->IncBgThreads();
  }

  if (FLAGS_continuous_verification_interval > 0) {
    shared->IncBgThreads();
  }

  if (FLAGS_compressed_secondary_cache_size > 0 ||
      FLAGS_compressed_secondary_cache_ratio > 0.0) {
    shared->IncBgThreads();
  }

  std::vector<ThreadState*> threads(n);
  for (uint32_t i = 0; i < n; i++) {
    threads[i] = new ThreadState(i, shared);
    db_stress_env->StartThread(ThreadBody, threads[i]);
  }

  ThreadState bg_thread(0, shared);
  if (FLAGS_compaction_thread_pool_adjust_interval > 0) {
    db_stress_env->StartThread(PoolSizeChangeThread, &bg_thread);
  }

  ThreadState continuous_verification_thread(0, shared);
  if (FLAGS_continuous_verification_interval > 0) {
    db_stress_env->StartThread(DbVerificationThread,
                               &continuous_verification_thread);
  }

  ThreadState compressed_cache_set_capacity_thread(0, shared);
  if (FLAGS_compressed_secondary_cache_size > 0 ||
      FLAGS_compressed_secondary_cache_ratio > 0.0) {
    db_stress_env->StartThread(CompressedCacheSetCapacityThread,
                               &compressed_cache_set_capacity_thread);
  }

  // Each thread goes through the following states:
  // initializing -> wait for others to init -> read/populate/depopulate
  // wait for others to operate -> verify -> done

  {
    MutexLock l(shared->GetMutex());
    while (!shared->AllInitialized()) {
      shared->GetCondVar()->Wait();
    }
    if (shared->ShouldVerifyAtBeginning()) {
      if (shared->HasVerificationFailedYet()) {
        fprintf(stderr, "Crash-recovery verification failed :(\n");
      } else {
        fprintf(stdout, "Crash-recovery verification passed :)\n");
        Status s = DestroyUnverifiedSubdir(FLAGS_db);
        if (s.ok() && !FLAGS_expected_values_dir.empty()) {
          s = DestroyUnverifiedSubdir(FLAGS_expected_values_dir);
        }
        if (!s.ok()) {
          fprintf(stderr, "Failed to cleanup unverified state dir: %s\n",
                  s.ToString().c_str());
          exit(1);
        }
      }
    }

    if (!FLAGS_verification_only) {
      // This is after the verification step to avoid making all those `Get()`s
      // and `MultiGet()`s contend on the DB-wide trace mutex.
      if (!FLAGS_expected_values_dir.empty()) {
        stress->TrackExpectedState(shared);
      }
      now = clock->NowMicros();
      fprintf(stdout, "%s Starting database operations\n",
              clock->TimeToString(now / 1000000).c_str());

      shared->SetStart();
      shared->GetCondVar()->SignalAll();
      while (!shared->AllOperated()) {
        shared->GetCondVar()->Wait();
      }

      now = clock->NowMicros();
      if (FLAGS_test_batches_snapshots) {
        fprintf(stdout, "%s Limited verification already done during gets\n",
                clock->TimeToString((uint64_t)now / 1000000).c_str());
      } else if (FLAGS_skip_verifydb) {
        fprintf(stdout, "%s Verification skipped\n",
                clock->TimeToString((uint64_t)now / 1000000).c_str());
      } else {
        fprintf(stdout, "%s Starting verification\n",
                clock->TimeToString((uint64_t)now / 1000000).c_str());
      }

      shared->SetStartVerify();
      shared->GetCondVar()->SignalAll();
      while (!shared->AllDone()) {
        shared->GetCondVar()->Wait();
      }
    }
  }

  // If we are running verification_only
  // stats will be empty and trying to report them will
  // emit no ops or writes error. To avoid this, merging and reporting stats
  // are not executed when running with verification_only
  // TODO: We need to create verification stats (e.g. how many keys
  // are verified by which method) and report them here instead of operation
  // stats.
  if (!FLAGS_verification_only) {
    for (unsigned int i = 1; i < n; i++) {
      threads[0]->stats.Merge(threads[i]->stats);
    }
    threads[0]->stats.Report("Stress Test");
  }

  for (unsigned int i = 0; i < n; i++) {
    delete threads[i];
    threads[i] = nullptr;
  }
  now = clock->NowMicros();
  if (!FLAGS_skip_verifydb && !FLAGS_test_batches_snapshots &&
      !shared->HasVerificationFailedYet()) {
    fprintf(stdout, "%s Verification successful\n",
            clock->TimeToString(now / 1000000).c_str());
  }

  if (!FLAGS_verification_only) {
    stress->PrintStatistics();
  }

  if (FLAGS_compaction_thread_pool_adjust_interval > 0 ||
      FLAGS_continuous_verification_interval > 0 ||
      FLAGS_compressed_secondary_cache_size > 0 ||
      FLAGS_compressed_secondary_cache_ratio > 0.0) {
    MutexLock l(shared->GetMutex());
    shared->SetShouldStopBgThread();
    while (!shared->BgThreadsFinished()) {
      shared->GetCondVar()->Wait();
    }
  }

  if (shared->HasVerificationFailedYet()) {
    fprintf(stderr, "Verification failed :(\n");
    return false;
  }
  return true;
}
bool RunStressTest(SharedState* shared) {
  ThreadStatusUtil::RegisterThread(db_stress_env, ThreadStatus::USER);
  bool result = RunStressTestImpl(shared);
  ThreadStatusUtil::UnregisterThread();
  return result;
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
