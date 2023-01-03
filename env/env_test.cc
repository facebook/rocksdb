//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef OS_WIN
#include <sys/ioctl.h>
#endif

#if defined(ROCKSDB_IOURING_PRESENT)
#include <liburing.h>
#include <sys/uio.h>
#endif

#include <sys/types.h>

#include <atomic>
#include <list>
#include <mutex>
#include <unordered_set>

#ifdef OS_LINUX
#include <fcntl.h>
#include <linux/fs.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#ifdef ROCKSDB_FALLOCATE_PRESENT
#include <errno.h>
#endif

#include "db/db_impl/db_impl.h"
#include "env/emulated_clock.h"
#include "env/env_chroot.h"
#include "env/env_encryption_ctr.h"
#include "env/fs_readonly.h"
#include "env/mock_env.h"
#include "env/unique_id_gen.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "options/options_helper.h"
#include "port/malloc.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/env_encryption.h"
#include "rocksdb/file_system.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/utilities/object_registry.h"
#include "test_util/mock_time_env.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/counted_fs.h"
#include "utilities/env_timed.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

using port::kPageSize;

static const int kDelayMicros = 100000;

struct Deleter {
  explicit Deleter(void (*fn)(void*)) : fn_(fn) {}

  void operator()(void* ptr) {
    assert(fn_);
    assert(ptr);
    (*fn_)(ptr);
  }

  void (*fn_)(void*);
};

extern "C" bool RocksDbIOUringEnable() { return true; }

std::unique_ptr<char, Deleter> NewAligned(const size_t size, const char ch) {
  char* ptr = nullptr;
#ifdef OS_WIN
  if (nullptr ==
      (ptr = reinterpret_cast<char*>(_aligned_malloc(size, kPageSize)))) {
    return std::unique_ptr<char, Deleter>(nullptr, Deleter(_aligned_free));
  }
  std::unique_ptr<char, Deleter> uptr(ptr, Deleter(_aligned_free));
#else
  if (posix_memalign(reinterpret_cast<void**>(&ptr), kPageSize, size) != 0) {
    return std::unique_ptr<char, Deleter>(nullptr, Deleter(free));
  }
  std::unique_ptr<char, Deleter> uptr(ptr, Deleter(free));
#endif
  memset(uptr.get(), ch, size);
  return uptr;
}

class EnvPosixTest : public testing::Test {
 private:
  port::Mutex mu_;
  std::string events_;

 public:
  Env* env_;
  bool direct_io_;
  EnvPosixTest() : env_(Env::Default()), direct_io_(false) {}
  ~EnvPosixTest() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->LoadDependency({});
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
};

class EnvPosixTestWithParam
    : public EnvPosixTest,
      public ::testing::WithParamInterface<std::pair<Env*, bool>> {
 public:
  EnvPosixTestWithParam() {
    std::pair<Env*, bool> param_pair = GetParam();
    env_ = param_pair.first;
    direct_io_ = param_pair.second;
  }

  void WaitThreadPoolsEmpty() {
    // Wait until the thread pools are empty.
    while (env_->GetThreadPoolQueueLen(Env::Priority::LOW) != 0) {
      Env::Default()->SleepForMicroseconds(kDelayMicros);
    }
    while (env_->GetThreadPoolQueueLen(Env::Priority::HIGH) != 0) {
      Env::Default()->SleepForMicroseconds(kDelayMicros);
    }
  }

  ~EnvPosixTestWithParam() override { WaitThreadPoolsEmpty(); }
};

static void SetBool(void* ptr) {
  reinterpret_cast<std::atomic<bool>*>(ptr)->store(true);
}

TEST_F(EnvPosixTest, DISABLED_RunImmediately) {
  for (int pri = Env::BOTTOM; pri < Env::TOTAL; ++pri) {
    std::atomic<bool> called(false);
    env_->SetBackgroundThreads(1, static_cast<Env::Priority>(pri));
    env_->Schedule(&SetBool, &called, static_cast<Env::Priority>(pri));
    Env::Default()->SleepForMicroseconds(kDelayMicros);
    ASSERT_TRUE(called.load());
  }
}

TEST_F(EnvPosixTest, RunEventually) {
  std::atomic<bool> called(false);
  env_->StartThread(&SetBool, &called);
  env_->WaitForJoin();
  ASSERT_TRUE(called.load());
}

#ifdef OS_WIN
TEST_F(EnvPosixTest, AreFilesSame) {
  {
    bool tmp;
    if (env_->AreFilesSame("", "", &tmp).IsNotSupported()) {
      fprintf(stderr,
              "skipping EnvBasicTestWithParam.AreFilesSame due to "
              "unsupported Env::AreFilesSame\n");
      return;
    }
  }

  const EnvOptions soptions;
  auto* env = Env::Default();
  std::string same_file_name = test::PerThreadDBPath(env, "same_file");
  std::string same_file_link_name = same_file_name + "_link";

  std::unique_ptr<WritableFile> same_file;
  ASSERT_OK(env->NewWritableFile(same_file_name, &same_file, soptions));
  same_file->Append("random_data");
  ASSERT_OK(same_file->Flush());
  same_file.reset();

  ASSERT_OK(env->LinkFile(same_file_name, same_file_link_name));
  bool result = false;
  ASSERT_OK(env->AreFilesSame(same_file_name, same_file_link_name, &result));
  ASSERT_TRUE(result);
}
#endif

#ifdef OS_LINUX
TEST_F(EnvPosixTest, DISABLED_FilePermission) {
  // Only works for Linux environment
  if (env_ == Env::Default()) {
    EnvOptions soptions;
    std::vector<std::string> fileNames{
        test::PerThreadDBPath(env_, "testfile"),
        test::PerThreadDBPath(env_, "testfile1")};
    std::unique_ptr<WritableFile> wfile;
    ASSERT_OK(env_->NewWritableFile(fileNames[0], &wfile, soptions));
    ASSERT_OK(env_->NewWritableFile(fileNames[1], &wfile, soptions));
    wfile.reset();
    std::unique_ptr<RandomRWFile> rwfile;
    ASSERT_OK(env_->NewRandomRWFile(fileNames[1], &rwfile, soptions));

    struct stat sb;
    for (const auto& filename : fileNames) {
      if (::stat(filename.c_str(), &sb) == 0) {
        ASSERT_EQ(sb.st_mode & 0777, 0644);
      }
      ASSERT_OK(env_->DeleteFile(filename));
    }

    env_->SetAllowNonOwnerAccess(false);
    ASSERT_OK(env_->NewWritableFile(fileNames[0], &wfile, soptions));
    ASSERT_OK(env_->NewWritableFile(fileNames[1], &wfile, soptions));
    wfile.reset();
    ASSERT_OK(env_->NewRandomRWFile(fileNames[1], &rwfile, soptions));

    for (const auto& filename : fileNames) {
      if (::stat(filename.c_str(), &sb) == 0) {
        ASSERT_EQ(sb.st_mode & 0777, 0600);
      }
      ASSERT_OK(env_->DeleteFile(filename));
    }
  }
}

TEST_F(EnvPosixTest, LowerThreadPoolCpuPriority) {
  std::atomic<CpuPriority> from_priority(CpuPriority::kNormal);
  std::atomic<CpuPriority> to_priority(CpuPriority::kNormal);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ThreadPoolImpl::BGThread::BeforeSetCpuPriority", [&](void* pri) {
        from_priority.store(*reinterpret_cast<CpuPriority*>(pri));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ThreadPoolImpl::BGThread::AfterSetCpuPriority", [&](void* pri) {
        to_priority.store(*reinterpret_cast<CpuPriority*>(pri));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  env_->SetBackgroundThreads(1, Env::BOTTOM);
  env_->SetBackgroundThreads(1, Env::HIGH);

  auto RunTask = [&](Env::Priority pool) {
    std::atomic<bool> called(false);
    env_->Schedule(&SetBool, &called, pool);
    for (int i = 0; i < kDelayMicros; i++) {
      if (called.load()) {
        break;
      }
      Env::Default()->SleepForMicroseconds(1);
    }
    ASSERT_TRUE(called.load());
  };

  {
    // Same priority, no-op.
    env_->LowerThreadPoolCPUPriority(Env::Priority::BOTTOM,
                                     CpuPriority::kNormal)
        .PermitUncheckedError();
    RunTask(Env::Priority::BOTTOM);
    ASSERT_EQ(from_priority, CpuPriority::kNormal);
    ASSERT_EQ(to_priority, CpuPriority::kNormal);
  }

  {
    // Higher priority, no-op.
    env_->LowerThreadPoolCPUPriority(Env::Priority::BOTTOM, CpuPriority::kHigh)
        .PermitUncheckedError();
    RunTask(Env::Priority::BOTTOM);
    ASSERT_EQ(from_priority, CpuPriority::kNormal);
    ASSERT_EQ(to_priority, CpuPriority::kNormal);
  }

  {
    // Lower priority from kNormal -> kLow.
    env_->LowerThreadPoolCPUPriority(Env::Priority::BOTTOM, CpuPriority::kLow)
        .PermitUncheckedError();
    RunTask(Env::Priority::BOTTOM);
    ASSERT_EQ(from_priority, CpuPriority::kNormal);
    ASSERT_EQ(to_priority, CpuPriority::kLow);
  }

  {
    // Lower priority from kLow -> kIdle.
    env_->LowerThreadPoolCPUPriority(Env::Priority::BOTTOM, CpuPriority::kIdle)
        .PermitUncheckedError();
    RunTask(Env::Priority::BOTTOM);
    ASSERT_EQ(from_priority, CpuPriority::kLow);
    ASSERT_EQ(to_priority, CpuPriority::kIdle);
  }

  {
    // Lower priority from kNormal -> kIdle for another pool.
    env_->LowerThreadPoolCPUPriority(Env::Priority::HIGH, CpuPriority::kIdle)
        .PermitUncheckedError();
    RunTask(Env::Priority::HIGH);
    ASSERT_EQ(from_priority, CpuPriority::kNormal);
    ASSERT_EQ(to_priority, CpuPriority::kIdle);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif

TEST_F(EnvPosixTest, MemoryMappedFileBuffer) {
  const int kFileBytes = 1 << 15;  // 32 KB
  std::string expected_data;
  std::string fname = test::PerThreadDBPath(env_, "testfile");
  {
    std::unique_ptr<WritableFile> wfile;
    const EnvOptions soptions;
    ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));

    Random rnd(301);
    expected_data = rnd.RandomString(kFileBytes);
    ASSERT_OK(wfile->Append(expected_data));
  }

  std::unique_ptr<MemoryMappedFileBuffer> mmap_buffer;
  Status status = env_->NewMemoryMappedFileBuffer(fname, &mmap_buffer);
  // it should be supported at least on linux
#if !defined(OS_LINUX)
  if (status.IsNotSupported()) {
    fprintf(stderr,
            "skipping EnvPosixTest.MemoryMappedFileBuffer due to "
            "unsupported Env::NewMemoryMappedFileBuffer\n");
    return;
  }
#endif  // !defined(OS_LINUX)

  ASSERT_OK(status);
  ASSERT_NE(nullptr, mmap_buffer.get());
  ASSERT_NE(nullptr, mmap_buffer->GetBase());
  ASSERT_EQ(kFileBytes, mmap_buffer->GetLen());
  std::string actual_data(reinterpret_cast<const char*>(mmap_buffer->GetBase()),
                          mmap_buffer->GetLen());
  ASSERT_EQ(expected_data, actual_data);
}

#ifndef ROCKSDB_NO_DYNAMIC_EXTENSION
TEST_F(EnvPosixTest, LoadRocksDBLibrary) {
  std::shared_ptr<DynamicLibrary> library;
  std::function<void*(void*, const char*)> function;
  Status status = env_->LoadLibrary("no-such-library", "", &library);
  ASSERT_NOK(status);
  ASSERT_EQ(nullptr, library.get());
  status = env_->LoadLibrary("rocksdb", "", &library);
  if (status.ok()) {  // If we have can find a rocksdb shared library
    ASSERT_NE(nullptr, library.get());
    ASSERT_OK(library->LoadFunction("rocksdb_create_default_env",
                                    &function));  // from C definition
    ASSERT_NE(nullptr, function);
    ASSERT_NOK(library->LoadFunction("no-such-method", &function));
    ASSERT_EQ(nullptr, function);
    ASSERT_OK(env_->LoadLibrary(library->Name(), "", &library));
  } else {
    ASSERT_EQ(nullptr, library.get());
  }
}
#endif  // !ROCKSDB_NO_DYNAMIC_EXTENSION

#if !defined(OS_WIN) && !defined(ROCKSDB_NO_DYNAMIC_EXTENSION)
TEST_F(EnvPosixTest, LoadRocksDBLibraryWithSearchPath) {
  std::shared_ptr<DynamicLibrary> library;
  std::function<void*(void*, const char*)> function;
  ASSERT_NOK(env_->LoadLibrary("no-such-library", "/tmp", &library));
  ASSERT_EQ(nullptr, library.get());
  ASSERT_NOK(env_->LoadLibrary("dl", "/tmp", &library));
  ASSERT_EQ(nullptr, library.get());
  Status status = env_->LoadLibrary("rocksdb", "/tmp:./", &library);
  if (status.ok()) {
    ASSERT_NE(nullptr, library.get());
    ASSERT_OK(env_->LoadLibrary(library->Name(), "", &library));
  }
  char buff[1024];
  std::string cwd = getcwd(buff, sizeof(buff));

  status = env_->LoadLibrary("rocksdb", "/tmp:" + cwd, &library);
  if (status.ok()) {
    ASSERT_NE(nullptr, library.get());
    ASSERT_OK(env_->LoadLibrary(library->Name(), "", &library));
  }
}
#endif  // !OS_WIN && !ROCKSDB_NO_DYNAMIC_EXTENSION

TEST_P(EnvPosixTestWithParam, UnSchedule) {
  std::atomic<bool> called(false);
  env_->SetBackgroundThreads(1, Env::LOW);

  /* Block the low priority queue */
  test::SleepingBackgroundTask sleeping_task, sleeping_task1;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                 Env::Priority::LOW);

  /* Schedule another task */
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task1,
                 Env::Priority::LOW, &sleeping_task1);

  /* Remove it with a different tag  */
  ASSERT_EQ(0, env_->UnSchedule(&called, Env::Priority::LOW));

  /* Remove it from the queue with the right tag */
  ASSERT_EQ(1, env_->UnSchedule(&sleeping_task1, Env::Priority::LOW));

  // Unblock background thread
  sleeping_task.WakeUp();

  /* Schedule another task */
  env_->Schedule(&SetBool, &called);
  for (int i = 0; i < kDelayMicros; i++) {
    if (called.load()) {
      break;
    }
    Env::Default()->SleepForMicroseconds(1);
  }
  ASSERT_TRUE(called.load());

  ASSERT_TRUE(!sleeping_task.IsSleeping() && !sleeping_task1.IsSleeping());
  WaitThreadPoolsEmpty();
}

// This tests assumes that the last scheduled
// task will run last. In fact, in the allotted
// sleeping time nothing may actually run or they may
// run in any order. The purpose of the test is unclear.
#ifndef OS_WIN
TEST_P(EnvPosixTestWithParam, RunMany) {
  env_->SetBackgroundThreads(1, Env::LOW);
  std::atomic<int> last_id(0);

  struct CB {
    std::atomic<int>* last_id_ptr;  // Pointer to shared slot
    int id;                         // Order# for the execution of this callback

    CB(std::atomic<int>* p, int i) : last_id_ptr(p), id(i) {}

    static void Run(void* v) {
      CB* cb = reinterpret_cast<CB*>(v);
      int cur = cb->last_id_ptr->load();
      ASSERT_EQ(cb->id - 1, cur);
      cb->last_id_ptr->store(cb->id);
    }
  };

  // Schedule in different order than start time
  CB cb1(&last_id, 1);
  CB cb2(&last_id, 2);
  CB cb3(&last_id, 3);
  CB cb4(&last_id, 4);
  env_->Schedule(&CB::Run, &cb1);
  env_->Schedule(&CB::Run, &cb2);
  env_->Schedule(&CB::Run, &cb3);
  env_->Schedule(&CB::Run, &cb4);
  // thread-pool pops a thread function and then run the function, which may
  // cause threadpool is empty but the last function is still running. Add a
  // dummy function at the end, to make sure the last callback is finished
  // before threadpool is empty.
  struct DummyCB {
    static void Run(void*) {}
  };
  env_->Schedule(&DummyCB::Run, nullptr);

  WaitThreadPoolsEmpty();
  ASSERT_EQ(4, last_id.load(std::memory_order_acquire));
}
#endif

struct State {
  port::Mutex mu;
  int val;
  int num_running;
};

static void ThreadBody(void* arg) {
  State* s = reinterpret_cast<State*>(arg);
  s->mu.Lock();
  s->val += 1;
  s->num_running -= 1;
  s->mu.Unlock();
}

TEST_P(EnvPosixTestWithParam, StartThread) {
  State state;
  state.val = 0;
  state.num_running = 3;
  for (int i = 0; i < 3; i++) {
    env_->StartThread(&ThreadBody, &state);
  }
  while (true) {
    state.mu.Lock();
    int num = state.num_running;
    state.mu.Unlock();
    if (num == 0) {
      break;
    }
    Env::Default()->SleepForMicroseconds(kDelayMicros);
  }
  ASSERT_EQ(state.val, 3);
  WaitThreadPoolsEmpty();
}

TEST_P(EnvPosixTestWithParam, TwoPools) {
  // Data structures to signal tasks to run.
  port::Mutex mutex;
  port::CondVar cv(&mutex);
  bool should_start = false;

  class CB {
   public:
    CB(const std::string& pool_name, int pool_size, port::Mutex* trigger_mu,
       port::CondVar* trigger_cv, bool* _should_start)
        : mu_(),
          num_running_(0),
          num_finished_(0),
          pool_size_(pool_size),
          pool_name_(pool_name),
          trigger_mu_(trigger_mu),
          trigger_cv_(trigger_cv),
          should_start_(_should_start) {}

    static void Run(void* v) {
      CB* cb = reinterpret_cast<CB*>(v);
      cb->Run();
    }

    void Run() {
      {
        MutexLock l(&mu_);
        num_running_++;
        // make sure we don't have more than pool_size_ jobs running.
        ASSERT_LE(num_running_, pool_size_.load());
      }

      {
        MutexLock l(trigger_mu_);
        while (!(*should_start_)) {
          trigger_cv_->Wait();
        }
      }

      {
        MutexLock l(&mu_);
        num_running_--;
        num_finished_++;
      }
    }

    int NumFinished() {
      MutexLock l(&mu_);
      return num_finished_;
    }

    void Reset(int pool_size) {
      pool_size_.store(pool_size);
      num_finished_ = 0;
    }

   private:
    port::Mutex mu_;
    int num_running_;
    int num_finished_;
    std::atomic<int> pool_size_;
    std::string pool_name_;
    port::Mutex* trigger_mu_;
    port::CondVar* trigger_cv_;
    bool* should_start_;
  };

  const int kLowPoolSize = 2;
  const int kHighPoolSize = 4;
  const int kJobs = 8;

  CB low_pool_job("low", kLowPoolSize, &mutex, &cv, &should_start);
  CB high_pool_job("high", kHighPoolSize, &mutex, &cv, &should_start);

  env_->SetBackgroundThreads(kLowPoolSize);
  env_->SetBackgroundThreads(kHighPoolSize, Env::Priority::HIGH);

  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::LOW));
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));

  // schedule same number of jobs in each pool
  for (int i = 0; i < kJobs; i++) {
    env_->Schedule(&CB::Run, &low_pool_job);
    env_->Schedule(&CB::Run, &high_pool_job, Env::Priority::HIGH);
  }
  // Wait a short while for the jobs to be dispatched.
  int sleep_count = 0;
  while ((unsigned int)(kJobs - kLowPoolSize) !=
             env_->GetThreadPoolQueueLen(Env::Priority::LOW) ||
         (unsigned int)(kJobs - kHighPoolSize) !=
             env_->GetThreadPoolQueueLen(Env::Priority::HIGH)) {
    env_->SleepForMicroseconds(kDelayMicros);
    if (++sleep_count > 100) {
      break;
    }
  }

  ASSERT_EQ((unsigned int)(kJobs - kLowPoolSize),
            env_->GetThreadPoolQueueLen());
  ASSERT_EQ((unsigned int)(kJobs - kLowPoolSize),
            env_->GetThreadPoolQueueLen(Env::Priority::LOW));
  ASSERT_EQ((unsigned int)(kJobs - kHighPoolSize),
            env_->GetThreadPoolQueueLen(Env::Priority::HIGH));

  // Trigger jobs to run.
  {
    MutexLock l(&mutex);
    should_start = true;
    cv.SignalAll();
  }

  // wait for all jobs to finish
  while (low_pool_job.NumFinished() < kJobs ||
         high_pool_job.NumFinished() < kJobs) {
    env_->SleepForMicroseconds(kDelayMicros);
  }

  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::LOW));
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));

  // Hold jobs to schedule;
  should_start = false;

  // call IncBackgroundThreadsIfNeeded to two pools. One increasing and
  // the other decreasing
  env_->IncBackgroundThreadsIfNeeded(kLowPoolSize - 1, Env::Priority::LOW);
  env_->IncBackgroundThreadsIfNeeded(kHighPoolSize + 1, Env::Priority::HIGH);
  high_pool_job.Reset(kHighPoolSize + 1);
  low_pool_job.Reset(kLowPoolSize);

  // schedule same number of jobs in each pool
  for (int i = 0; i < kJobs; i++) {
    env_->Schedule(&CB::Run, &low_pool_job);
    env_->Schedule(&CB::Run, &high_pool_job, Env::Priority::HIGH);
  }
  // Wait a short while for the jobs to be dispatched.
  sleep_count = 0;
  while ((unsigned int)(kJobs - kLowPoolSize) !=
             env_->GetThreadPoolQueueLen(Env::Priority::LOW) ||
         (unsigned int)(kJobs - (kHighPoolSize + 1)) !=
             env_->GetThreadPoolQueueLen(Env::Priority::HIGH)) {
    env_->SleepForMicroseconds(kDelayMicros);
    if (++sleep_count > 100) {
      break;
    }
  }
  ASSERT_EQ((unsigned int)(kJobs - kLowPoolSize),
            env_->GetThreadPoolQueueLen());
  ASSERT_EQ((unsigned int)(kJobs - kLowPoolSize),
            env_->GetThreadPoolQueueLen(Env::Priority::LOW));
  ASSERT_EQ((unsigned int)(kJobs - (kHighPoolSize + 1)),
            env_->GetThreadPoolQueueLen(Env::Priority::HIGH));

  // Trigger jobs to run.
  {
    MutexLock l(&mutex);
    should_start = true;
    cv.SignalAll();
  }

  // wait for all jobs to finish
  while (low_pool_job.NumFinished() < kJobs ||
         high_pool_job.NumFinished() < kJobs) {
    env_->SleepForMicroseconds(kDelayMicros);
  }

  env_->SetBackgroundThreads(kHighPoolSize, Env::Priority::HIGH);
  WaitThreadPoolsEmpty();
}

TEST_P(EnvPosixTestWithParam, DecreaseNumBgThreads) {
  constexpr int kWaitMicros = 60000000;  // 1min

  std::vector<test::SleepingBackgroundTask> tasks(10);

  // Set number of thread to 1 first.
  env_->SetBackgroundThreads(1, Env::Priority::HIGH);

  // Schedule 3 tasks. 0 running; Task 1, 2 waiting.
  for (size_t i = 0; i < 3; i++) {
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &tasks[i],
                   Env::Priority::HIGH);
  }
  ASSERT_FALSE(tasks[0].TimedWaitUntilSleeping(kWaitMicros));
  ASSERT_EQ(2U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[0].IsSleeping());
  ASSERT_TRUE(!tasks[1].IsSleeping());
  ASSERT_TRUE(!tasks[2].IsSleeping());

  // Increase to 2 threads. Task 0, 1 running; 2 waiting
  env_->SetBackgroundThreads(2, Env::Priority::HIGH);
  ASSERT_FALSE(tasks[1].TimedWaitUntilSleeping(kWaitMicros));
  ASSERT_EQ(1U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[0].IsSleeping());
  ASSERT_TRUE(tasks[1].IsSleeping());
  ASSERT_TRUE(!tasks[2].IsSleeping());

  // Shrink back to 1 thread. Still task 0, 1 running, 2 waiting
  env_->SetBackgroundThreads(1, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ(1U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[0].IsSleeping());
  ASSERT_TRUE(tasks[1].IsSleeping());
  ASSERT_TRUE(!tasks[2].IsSleeping());

  // The last task finishes. Task 0 running, 2 waiting.
  tasks[1].WakeUp();
  ASSERT_FALSE(tasks[1].TimedWaitUntilDone(kWaitMicros));
  ASSERT_EQ(1U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[0].IsSleeping());
  ASSERT_TRUE(!tasks[1].IsSleeping());
  ASSERT_TRUE(!tasks[2].IsSleeping());

  // Increase to 5 threads. Task 0 and 2 running.
  env_->SetBackgroundThreads(5, Env::Priority::HIGH);
  ASSERT_FALSE(tasks[2].TimedWaitUntilSleeping(kWaitMicros));
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[0].IsSleeping());
  ASSERT_TRUE(!tasks[1].IsSleeping());
  ASSERT_TRUE(tasks[2].IsSleeping());

  // Change number of threads a couple of times while there is no sufficient
  // tasks.
  env_->SetBackgroundThreads(7, Env::Priority::HIGH);
  tasks[2].WakeUp();
  ASSERT_FALSE(tasks[2].TimedWaitUntilDone(kWaitMicros));
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  env_->SetBackgroundThreads(3, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  env_->SetBackgroundThreads(4, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  env_->SetBackgroundThreads(5, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  env_->SetBackgroundThreads(4, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));

  Env::Default()->SleepForMicroseconds(kDelayMicros * 50);

  // Enqueue 5 more tasks. Thread pool size now is 4.
  // Task 0, 3, 4, 5 running;6, 7 waiting.
  for (size_t i = 3; i < 8; i++) {
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &tasks[i],
                   Env::Priority::HIGH);
  }
  for (size_t i = 3; i <= 5; i++) {
    ASSERT_FALSE(tasks[i].TimedWaitUntilSleeping(kWaitMicros));
  }
  ASSERT_EQ(2U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[0].IsSleeping());
  ASSERT_TRUE(!tasks[1].IsSleeping());
  ASSERT_TRUE(!tasks[2].IsSleeping());
  ASSERT_TRUE(tasks[3].IsSleeping());
  ASSERT_TRUE(tasks[4].IsSleeping());
  ASSERT_TRUE(tasks[5].IsSleeping());
  ASSERT_TRUE(!tasks[6].IsSleeping());
  ASSERT_TRUE(!tasks[7].IsSleeping());

  // Wake up task 0, 3 and 4. Task 5, 6, 7 running.
  tasks[0].WakeUp();
  tasks[3].WakeUp();
  tasks[4].WakeUp();

  for (size_t i = 5; i < 8; i++) {
    ASSERT_FALSE(tasks[i].TimedWaitUntilSleeping(kWaitMicros));
  }
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  for (size_t i = 5; i < 8; i++) {
    ASSERT_TRUE(tasks[i].IsSleeping());
  }

  // Shrink back to 1 thread. Still task 5, 6, 7 running
  env_->SetBackgroundThreads(1, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_TRUE(tasks[5].IsSleeping());
  ASSERT_TRUE(tasks[6].IsSleeping());
  ASSERT_TRUE(tasks[7].IsSleeping());

  // Wake up task  6. Task 5, 7 running
  tasks[6].WakeUp();
  ASSERT_FALSE(tasks[6].TimedWaitUntilDone(kWaitMicros));
  ASSERT_TRUE(tasks[5].IsSleeping());
  ASSERT_TRUE(!tasks[6].IsSleeping());
  ASSERT_TRUE(tasks[7].IsSleeping());

  // Wake up threads 7. Task 5 running
  tasks[7].WakeUp();
  ASSERT_FALSE(tasks[7].TimedWaitUntilDone(kWaitMicros));
  ASSERT_TRUE(!tasks[7].IsSleeping());

  // Enqueue thread 8 and 9. Task 5 running; one of 8, 9 might be running.
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &tasks[8],
                 Env::Priority::HIGH);
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &tasks[9],
                 Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_GT(env_->GetThreadPoolQueueLen(Env::Priority::HIGH), (unsigned int)0);
  ASSERT_TRUE(!tasks[8].IsSleeping() || !tasks[9].IsSleeping());

  // Increase to 4 threads. Task 5, 8, 9 running.
  env_->SetBackgroundThreads(4, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ((unsigned int)0, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[8].IsSleeping());
  ASSERT_TRUE(tasks[9].IsSleeping());

  // Shrink to 1 thread
  env_->SetBackgroundThreads(1, Env::Priority::HIGH);

  // Wake up thread 9.
  tasks[9].WakeUp();
  ASSERT_FALSE(tasks[9].TimedWaitUntilDone(kWaitMicros));
  ASSERT_TRUE(!tasks[9].IsSleeping());
  ASSERT_TRUE(tasks[8].IsSleeping());

  // Wake up thread 8
  tasks[8].WakeUp();
  ASSERT_FALSE(tasks[8].TimedWaitUntilDone(kWaitMicros));
  ASSERT_TRUE(!tasks[8].IsSleeping());

  // Wake up the last thread
  tasks[5].WakeUp();
  ASSERT_FALSE(tasks[5].TimedWaitUntilDone(kWaitMicros));
  WaitThreadPoolsEmpty();
}

TEST_P(EnvPosixTestWithParam, ReserveThreads) {
  // Initialize the background thread to 1 in case other threads exist
  // from the last unit test
  env_->SetBackgroundThreads(1, Env::Priority::HIGH);
  ASSERT_EQ(env_->GetBackgroundThreads(Env::HIGH), 1);
  constexpr int kWaitMicros = 10000000;  // 10seconds
  std::vector<test::SleepingBackgroundTask> tasks(4);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  // Set the sync point to ensure thread 0 can terminate
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"ThreadPoolImpl::BGThread::Termination:th0",
        "EnvTest::ReserveThreads:0"}});
  // Empty the thread pool to ensure all the threads can start later
  env_->SetBackgroundThreads(0, Env::Priority::HIGH);
  TEST_SYNC_POINT("EnvTest::ReserveThreads:0");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  // Set the sync point to ensure threads start and pass the sync point
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"ThreadPoolImpl::BGThread::Start:th0", "EnvTest::ReserveThreads:1"},
       {"ThreadPoolImpl::BGThread::Start:th1", "EnvTest::ReserveThreads:2"},
       {"ThreadPoolImpl::BGThread::Start:th2", "EnvTest::ReserveThreads:3"},
       {"ThreadPoolImpl::BGThread::Start:th3", "EnvTest::ReserveThreads:4"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Set number of thread to 3 first.
  env_->SetBackgroundThreads(3, Env::Priority::HIGH);
  ASSERT_EQ(env_->GetBackgroundThreads(Env::HIGH), 3);
  // Add sync points to ensure all 3 threads start
  TEST_SYNC_POINT("EnvTest::ReserveThreads:1");
  TEST_SYNC_POINT("EnvTest::ReserveThreads:2");
  TEST_SYNC_POINT("EnvTest::ReserveThreads:3");
  // Reserve 2 threads
  ASSERT_EQ(2, env_->ReserveThreads(2, Env::Priority::HIGH));

  // Schedule 3 tasks. Task 0 running (in this context, doing
  // SleepingBackgroundTask); Task 1, 2 waiting; 3 reserved threads.
  for (size_t i = 0; i < 3; i++) {
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &tasks[i],
                   Env::Priority::HIGH);
  }
  ASSERT_FALSE(tasks[0].TimedWaitUntilSleeping(kWaitMicros));
  ASSERT_EQ(2U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[0].IsSleeping());
  ASSERT_TRUE(!tasks[1].IsSleeping());
  ASSERT_TRUE(!tasks[2].IsSleeping());

  // Release 2 threads. Task 0, 1, 2 running; 0 reserved thread.
  ASSERT_EQ(2, env_->ReleaseThreads(2, Env::Priority::HIGH));
  ASSERT_FALSE(tasks[1].TimedWaitUntilSleeping(kWaitMicros));
  ASSERT_FALSE(tasks[2].TimedWaitUntilSleeping(kWaitMicros));
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[1].IsSleeping());
  ASSERT_TRUE(tasks[2].IsSleeping());
  // No more threads can be reserved
  ASSERT_EQ(0, env_->ReserveThreads(3, Env::Priority::HIGH));
  // Expand the number of background threads so that the last thread
  // is waiting
  env_->SetBackgroundThreads(4, Env::Priority::HIGH);
  // Add sync point to ensure the 4th thread starts
  TEST_SYNC_POINT("EnvTest::ReserveThreads:4");
  // As the thread pool is expanded, we can reserve one more thread
  ASSERT_EQ(1, env_->ReserveThreads(3, Env::Priority::HIGH));
  // No more threads can be reserved
  ASSERT_EQ(0, env_->ReserveThreads(3, Env::Priority::HIGH));

  // Reset the sync points for the next iteration in BGThread or the
  // next time Submit() is called
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"ThreadPoolImpl::BGThread::WaitingThreadsInc",
        "EnvTest::ReserveThreads:5"},
       {"ThreadPoolImpl::BGThread::Termination", "EnvTest::ReserveThreads:6"},
       {"ThreadPoolImpl::Submit::Enqueue", "EnvTest::ReserveThreads:7"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  tasks[0].WakeUp();
  ASSERT_FALSE(tasks[0].TimedWaitUntilDone(kWaitMicros));
  // Add sync point to ensure the number of waiting threads increases
  TEST_SYNC_POINT("EnvTest::ReserveThreads:5");
  // 1 more thread can be reserved
  ASSERT_EQ(1, env_->ReserveThreads(3, Env::Priority::HIGH));
  // 2 reserved threads now

  // Currently, two threads are blocked since the number of waiting
  // threads is equal to the number of reserved threads (i.e., 2).
  // If we reduce the number of background thread to 1, at least one thread
  // will be the last excessive thread (here we have no control over the
  // number of excessive threads because thread order does not
  // necessarily follows the schedule order, but we ensure that the last thread
  // shall not run any task by expanding the thread pool after we schedule
  // the tasks), and thus they(it) become(s) unblocked, the number of waiting
  // threads decreases to 0 or 1, but the number of reserved threads is still 2
  env_->SetBackgroundThreads(1, Env::Priority::HIGH);

  // Task 1,2 running; 2 reserved threads, however, in fact, we only have
  // 0 or 1 waiting thread in the thread pool, proved by the
  // following test, we CANNOT reserve 2 threads even though we just
  // release 2
  TEST_SYNC_POINT("EnvTest::ReserveThreads:6");
  ASSERT_EQ(2, env_->ReleaseThreads(2, Env::Priority::HIGH));
  ASSERT_GT(2, env_->ReserveThreads(2, Env::Priority::HIGH));

  // Every new task will be put into the queue at this point
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &tasks[3],
                 Env::Priority::HIGH);
  TEST_SYNC_POINT("EnvTest::ReserveThreads:7");
  ASSERT_EQ(1U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(!tasks[3].IsSleeping());

  // Set the number of threads to 3 so that Task 3 can dequeue
  env_->SetBackgroundThreads(3, Env::Priority::HIGH);
  // Wakup Task 1
  tasks[1].WakeUp();
  ASSERT_FALSE(tasks[1].TimedWaitUntilDone(kWaitMicros));
  // Task 2, 3 running (Task 3 dequeue); 0 or 1 reserved thread
  ASSERT_FALSE(tasks[3].TimedWaitUntilSleeping(kWaitMicros));
  ASSERT_TRUE(tasks[3].IsSleeping());
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));

  // At most 1 thread can be released
  ASSERT_GT(2, env_->ReleaseThreads(3, Env::Priority::HIGH));
  tasks[2].WakeUp();
  ASSERT_FALSE(tasks[2].TimedWaitUntilDone(kWaitMicros));
  tasks[3].WakeUp();
  ASSERT_FALSE(tasks[3].TimedWaitUntilDone(kWaitMicros));
  WaitThreadPoolsEmpty();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

#if (defined OS_LINUX || defined OS_WIN)
namespace {
bool IsSingleVarint(const std::string& s) {
  Slice slice(s);

  uint64_t v;
  if (!GetVarint64(&slice, &v)) {
    return false;
  }

  return slice.size() == 0;
}

bool IsUniqueIDValid(const std::string& s) {
  return !s.empty() && !IsSingleVarint(s);
}

const size_t MAX_ID_SIZE = 100;
char temp_id[MAX_ID_SIZE];

}  // namespace

// Determine whether we can use the FS_IOC_GETVERSION ioctl
// on a file in directory DIR.  Create a temporary file therein,
// try to apply the ioctl (save that result), cleanup and
// return the result.  Return true if it is supported, and
// false if anything fails.
// Note that this function "knows" that dir has just been created
// and is empty, so we create a simply-named test file: "f".
bool ioctl_support__FS_IOC_GETVERSION(const std::string& dir) {
#ifdef OS_WIN
  return true;
#else
  const std::string file = dir + "/f";
  int fd;
  do {
    fd = open(file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
  } while (fd < 0 && errno == EINTR);
  long int version;
  bool ok = (fd >= 0 && ioctl(fd, FS_IOC_GETVERSION, &version) >= 0);

  close(fd);
  unlink(file.c_str());

  return ok;
#endif
}

// To ensure that Env::GetUniqueId-related tests work correctly, the files
// should be stored in regular storage like "hard disk" or "flash device",
// and not on a tmpfs file system (like /dev/shm and /tmp on some systems).
// Otherwise we cannot get the correct id.
//
// This function serves as the replacement for test::TmpDir(), which may be
// customized to be on a file system that doesn't work with GetUniqueId().

class IoctlFriendlyTmpdir {
 public:
  explicit IoctlFriendlyTmpdir() {
    char dir_buf[100];

    const char* fmt = "%s/rocksdb.XXXXXX";
    const char* tmp = getenv("TEST_IOCTL_FRIENDLY_TMPDIR");

#ifdef OS_WIN
#define rmdir _rmdir
    if (tmp == nullptr) {
      tmp = getenv("TMP");
    }

    snprintf(dir_buf, sizeof dir_buf, fmt, tmp);
    auto result = _mktemp(dir_buf);
    assert(result != nullptr);
    BOOL ret = CreateDirectory(dir_buf, NULL);
    assert(ret == TRUE);
    dir_ = dir_buf;
#else
    std::list<std::string> candidate_dir_list = {"/var/tmp", "/tmp"};

    // If $TEST_IOCTL_FRIENDLY_TMPDIR/rocksdb.XXXXXX fits, use
    // $TEST_IOCTL_FRIENDLY_TMPDIR; subtract 2 for the "%s", and
    // add 1 for the trailing NUL byte.
    if (tmp && strlen(tmp) + strlen(fmt) - 2 + 1 <= sizeof dir_buf) {
      // use $TEST_IOCTL_FRIENDLY_TMPDIR value
      candidate_dir_list.push_front(tmp);
    }

    for (const std::string& d : candidate_dir_list) {
      snprintf(dir_buf, sizeof dir_buf, fmt, d.c_str());
      if (mkdtemp(dir_buf)) {
        if (ioctl_support__FS_IOC_GETVERSION(dir_buf)) {
          dir_ = dir_buf;
          return;
        } else {
          // Diagnose ioctl-related failure only if this is the
          // directory specified via that envvar.
          if (tmp && tmp == d) {
            fprintf(stderr,
                    "TEST_IOCTL_FRIENDLY_TMPDIR-specified directory is "
                    "not suitable: %s\n",
                    d.c_str());
          }
          rmdir(dir_buf);  // ignore failure
        }
      } else {
        // mkdtemp failed: diagnose it, but don't give up.
        fprintf(stderr, "mkdtemp(%s/...) failed: %s\n", d.c_str(),
                errnoStr(errno).c_str());
      }
    }

    // check if it's running test within a docker container, in which case, the
    // file system inside `overlayfs` may not support FS_IOC_GETVERSION
    // skip the tests
    struct stat buffer;
    if (stat("/.dockerenv", &buffer) == 0) {
      is_supported_ = false;
      return;
    }

    fprintf(stderr,
            "failed to find an ioctl-friendly temporary directory;"
            " specify one via the TEST_IOCTL_FRIENDLY_TMPDIR envvar\n");
    std::abort();
#endif
  }

  ~IoctlFriendlyTmpdir() { rmdir(dir_.c_str()); }

  const std::string& name() const { return dir_; }

  bool is_supported() const { return is_supported_; }

 private:
  std::string dir_;

  bool is_supported_ = true;
};

#ifndef ROCKSDB_LITE
TEST_F(EnvPosixTest, PositionedAppend) {
  std::unique_ptr<WritableFile> writable_file;
  EnvOptions options;
  options.use_direct_writes = true;
  options.use_mmap_writes = false;
  std::string fname = test::PerThreadDBPath(env_, "positioned_append");
  SetupSyncPointsToMockDirectIO();

  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, options));
  const size_t kBlockSize = 4096;
  const size_t kDataSize = kPageSize;
  // Write a page worth of 'a'
  auto data_ptr = NewAligned(kDataSize, 'a');
  Slice data_a(data_ptr.get(), kDataSize);
  ASSERT_OK(writable_file->PositionedAppend(data_a, 0U));
  // Write a page worth of 'b' right after the first sector
  data_ptr = NewAligned(kDataSize, 'b');
  Slice data_b(data_ptr.get(), kDataSize);
  ASSERT_OK(writable_file->PositionedAppend(data_b, kBlockSize));
  ASSERT_OK(writable_file->Close());
  // The file now has 1 sector worth of a followed by a page worth of b

  // Verify the above
  std::unique_ptr<SequentialFile> seq_file;
  ASSERT_OK(env_->NewSequentialFile(fname, &seq_file, options));
  size_t scratch_len = kPageSize * 2;
  std::unique_ptr<char[]> scratch(new char[scratch_len]);
  Slice result;
  ASSERT_OK(seq_file->Read(scratch_len, &result, scratch.get()));
  ASSERT_EQ(kPageSize + kBlockSize, result.size());
  ASSERT_EQ('a', result[kBlockSize - 1]);
  ASSERT_EQ('b', result[kBlockSize]);
}
#endif  // !ROCKSDB_LITE

// `GetUniqueId()` temporarily returns zero on Windows. `BlockBasedTable` can
// handle a return value of zero but this test case cannot.
#ifndef OS_WIN
TEST_P(EnvPosixTestWithParam, RandomAccessUniqueID) {
  // Create file.
  if (env_ == Env::Default()) {
    EnvOptions soptions;
    soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;
    IoctlFriendlyTmpdir ift;
    if (!ift.is_supported()) {
      ROCKSDB_GTEST_BYPASS(
          "FS_IOC_GETVERSION is not supported by the filesystem");
      return;
    }
    std::string fname = ift.name() + "/testfile";
    std::unique_ptr<WritableFile> wfile;
    ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));

    std::unique_ptr<RandomAccessFile> file;

    // Get Unique ID
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
    size_t id_size = file->GetUniqueId(temp_id, MAX_ID_SIZE);
    ASSERT_TRUE(id_size > 0);
    std::string unique_id1(temp_id, id_size);
    ASSERT_TRUE(IsUniqueIDValid(unique_id1));

    // Get Unique ID again
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
    id_size = file->GetUniqueId(temp_id, MAX_ID_SIZE);
    ASSERT_TRUE(id_size > 0);
    std::string unique_id2(temp_id, id_size);
    ASSERT_TRUE(IsUniqueIDValid(unique_id2));

    // Get Unique ID again after waiting some time.
    env_->SleepForMicroseconds(1000000);
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
    id_size = file->GetUniqueId(temp_id, MAX_ID_SIZE);
    ASSERT_TRUE(id_size > 0);
    std::string unique_id3(temp_id, id_size);
    ASSERT_TRUE(IsUniqueIDValid(unique_id3));

    // Check IDs are the same.
    ASSERT_EQ(unique_id1, unique_id2);
    ASSERT_EQ(unique_id2, unique_id3);

    // Delete the file
    ASSERT_OK(env_->DeleteFile(fname));
  }
}
#endif  // !defined(OS_WIN)

// only works in linux platforms
#ifdef ROCKSDB_FALLOCATE_PRESENT
TEST_P(EnvPosixTestWithParam, AllocateTest) {
  if (env_ == Env::Default()) {
    SetupSyncPointsToMockDirectIO();
    std::string fname = test::PerThreadDBPath(env_, "preallocate_testfile");
    // Try fallocate in a file to see whether the target file system supports
    // it.
    // Skip the test if fallocate is not supported.
    std::string fname_test_fallocate =
        test::PerThreadDBPath(env_, "preallocate_testfile_2");
    int fd = -1;
    do {
      fd = open(fname_test_fallocate.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    } while (fd < 0 && errno == EINTR);
    ASSERT_GT(fd, 0);

    int alloc_status = fallocate(fd, 0, 0, 1);

    int err_number = 0;
    if (alloc_status != 0) {
      err_number = errno;
      fprintf(stderr, "Warning: fallocate() fails, %s\n",
              errnoStr(err_number).c_str());
    }
    close(fd);
    ASSERT_OK(env_->DeleteFile(fname_test_fallocate));
    if (alloc_status != 0 && err_number == EOPNOTSUPP) {
      // The filesystem containing the file does not support fallocate
      return;
    }

    EnvOptions soptions;
    soptions.use_mmap_writes = false;
    soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;
    std::unique_ptr<WritableFile> wfile;
    ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));

    // allocate 100 MB
    size_t kPreallocateSize = 100 * 1024 * 1024;
    size_t kBlockSize = 512;
    size_t kDataSize = 1024 * 1024;
    auto data_ptr = NewAligned(kDataSize, 'A');
    Slice data(data_ptr.get(), kDataSize);
    wfile->SetPreallocationBlockSize(kPreallocateSize);
    wfile->PrepareWrite(wfile->GetFileSize(), kDataSize);
    ASSERT_OK(wfile->Append(data));
    ASSERT_OK(wfile->Flush());

    struct stat f_stat;
    ASSERT_EQ(stat(fname.c_str(), &f_stat), 0);
    ASSERT_EQ((unsigned int)kDataSize, f_stat.st_size);
    // verify that blocks are preallocated
    // Note here that we don't check the exact number of blocks preallocated --
    // we only require that number of allocated blocks is at least what we
    // expect.
    // It looks like some FS give us more blocks that we asked for. That's fine.
    // It might be worth investigating further.
    ASSERT_LE((unsigned int)(kPreallocateSize / kBlockSize), f_stat.st_blocks);

    // close the file, should deallocate the blocks
    wfile.reset();

    stat(fname.c_str(), &f_stat);
    ASSERT_EQ((unsigned int)kDataSize, f_stat.st_size);
    // verify that preallocated blocks were deallocated on file close
    // Because the FS might give us more blocks, we add a full page to the size
    // and expect the number of blocks to be less or equal to that.
    ASSERT_GE((f_stat.st_size + kPageSize + kBlockSize - 1) / kBlockSize,
              (unsigned int)f_stat.st_blocks);
  }
}
#endif  // ROCKSDB_FALLOCATE_PRESENT

// Returns true if any of the strings in ss are the prefix of another string.
bool HasPrefix(const std::unordered_set<std::string>& ss) {
  for (const std::string& s : ss) {
    if (s.empty()) {
      return true;
    }
    for (size_t i = 1; i < s.size(); ++i) {
      if (ss.count(s.substr(0, i)) != 0) {
        return true;
      }
    }
  }
  return false;
}

// `GetUniqueId()` temporarily returns zero on Windows. `BlockBasedTable` can
// handle a return value of zero but this test case cannot.
#ifndef OS_WIN
TEST_P(EnvPosixTestWithParam, RandomAccessUniqueIDConcurrent) {
  if (env_ == Env::Default()) {
    // Check whether a bunch of concurrently existing files have unique IDs.
    EnvOptions soptions;
    soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;

    // Create the files
    IoctlFriendlyTmpdir ift;
    if (!ift.is_supported()) {
      ROCKSDB_GTEST_BYPASS(
          "FS_IOC_GETVERSION is not supported by the filesystem");
      return;
    }
    std::vector<std::string> fnames;
    for (int i = 0; i < 1000; ++i) {
      fnames.push_back(ift.name() + "/" + "testfile" + std::to_string(i));

      // Create file.
      std::unique_ptr<WritableFile> wfile;
      ASSERT_OK(env_->NewWritableFile(fnames[i], &wfile, soptions));
    }

    // Collect and check whether the IDs are unique.
    std::unordered_set<std::string> ids;
    for (const std::string& fname : fnames) {
      std::unique_ptr<RandomAccessFile> file;
      std::string unique_id;
      ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
      size_t id_size = file->GetUniqueId(temp_id, MAX_ID_SIZE);
      ASSERT_TRUE(id_size > 0);
      unique_id = std::string(temp_id, id_size);
      ASSERT_TRUE(IsUniqueIDValid(unique_id));

      ASSERT_TRUE(ids.count(unique_id) == 0);
      ids.insert(unique_id);
    }

    // Delete the files
    for (const std::string& fname : fnames) {
      ASSERT_OK(env_->DeleteFile(fname));
    }

    ASSERT_TRUE(!HasPrefix(ids));
  }
}

// TODO: Disable the flaky test, it's a known issue that ext4 may return same
// key after file deletion. The issue is tracked in #7405, #7470.
TEST_P(EnvPosixTestWithParam, DISABLED_RandomAccessUniqueIDDeletes) {
  if (env_ == Env::Default()) {
    EnvOptions soptions;
    soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;

    IoctlFriendlyTmpdir ift;
    if (!ift.is_supported()) {
      ROCKSDB_GTEST_BYPASS(
          "FS_IOC_GETVERSION is not supported by the filesystem");
      return;
    }
    std::string fname = ift.name() + "/" + "testfile";

    // Check that after file is deleted we don't get same ID again in a new
    // file.
    std::unordered_set<std::string> ids;
    for (int i = 0; i < 1000; ++i) {
      // Create file.
      {
        std::unique_ptr<WritableFile> wfile;
        ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));
      }

      // Get Unique ID
      std::string unique_id;
      {
        std::unique_ptr<RandomAccessFile> file;
        ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
        size_t id_size = file->GetUniqueId(temp_id, MAX_ID_SIZE);
        ASSERT_TRUE(id_size > 0);
        unique_id = std::string(temp_id, id_size);
      }

      ASSERT_TRUE(IsUniqueIDValid(unique_id));
      ASSERT_TRUE(ids.count(unique_id) == 0);
      ids.insert(unique_id);

      // Delete the file
      ASSERT_OK(env_->DeleteFile(fname));
    }

    ASSERT_TRUE(!HasPrefix(ids));
  }
}
#endif  // !defined(OS_WIN)

TEST_P(EnvPosixTestWithParam, MultiRead) {
  EnvOptions soptions;
  soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;
  std::string fname = test::PerThreadDBPath(env_, "testfile");

  const size_t kSectorSize = 4096;
  const size_t kNumSectors = 8;

  // Create file.
  {
    std::unique_ptr<WritableFile> wfile;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && \
    !defined(OS_AIX)
    if (soptions.use_direct_writes) {
      soptions.use_direct_writes = false;
    }
#endif
    ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));
    for (size_t i = 0; i < kNumSectors; ++i) {
      auto data = NewAligned(kSectorSize * 8, static_cast<char>(i + 1));
      Slice slice(data.get(), kSectorSize);
      ASSERT_OK(wfile->Append(slice));
    }
    ASSERT_OK(wfile->Close());
  }

  // More attempts to simulate more partial result sequences.
  for (uint32_t attempt = 0; attempt < 20; attempt++) {
    // Random Read
    Random rnd(301 + attempt);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "UpdateResults::io_uring_result", [&](void* arg) {
          if (attempt > 0) {
            // No failure in the first attempt.
            size_t& bytes_read = *static_cast<size_t*>(arg);
            if (rnd.OneIn(4)) {
              bytes_read = 0;
            } else if (rnd.OneIn(3)) {
              bytes_read = static_cast<size_t>(
                  rnd.Uniform(static_cast<int>(bytes_read)));
            }
          }
        });

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    std::unique_ptr<RandomAccessFile> file;
    std::vector<ReadRequest> reqs(3);
    std::vector<std::unique_ptr<char, Deleter>> data;
    uint64_t offset = 0;
    for (size_t i = 0; i < reqs.size(); ++i) {
      reqs[i].offset = offset;
      offset += 2 * kSectorSize;
      reqs[i].len = kSectorSize;
      data.emplace_back(NewAligned(kSectorSize, 0));
      reqs[i].scratch = data.back().get();
    }
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && \
    !defined(OS_AIX)
    if (soptions.use_direct_reads) {
      soptions.use_direct_reads = false;
    }
#endif
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
    ASSERT_OK(file->MultiRead(reqs.data(), reqs.size()));
    for (size_t i = 0; i < reqs.size(); ++i) {
      auto buf = NewAligned(kSectorSize * 8, static_cast<char>(i * 2 + 1));
      ASSERT_OK(reqs[i].status);
      ASSERT_EQ(memcmp(reqs[i].scratch, buf.get(), kSectorSize), 0);
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(EnvPosixTest, MultiReadNonAlignedLargeNum) {
  // In this test we don't do aligned read, so it doesn't work for
  // direct I/O case.
  EnvOptions soptions;
  soptions.use_direct_reads = soptions.use_direct_writes = false;
  std::string fname = test::PerThreadDBPath(env_, "testfile");

  const size_t kTotalSize = 81920;
  Random rnd(301);
  std::string expected_data = rnd.RandomString(kTotalSize);

  // Create file.
  {
    std::unique_ptr<WritableFile> wfile;
    ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));
    ASSERT_OK(wfile->Append(expected_data));
    ASSERT_OK(wfile->Close());
  }

  // More attempts to simulate more partial result sequences.
  for (uint32_t attempt = 0; attempt < 25; attempt++) {
    // Right now kIoUringDepth is hard coded as 256, so we need very large
    // number of keys to cover the case of multiple rounds of submissions.
    // Right now the test latency is still acceptable. If it ends up with
    // too long, we can modify the io uring depth with SyncPoint here.
    const int num_reads = rnd.Uniform(512) + 1;

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "UpdateResults::io_uring_result", [&](void* arg) {
          if (attempt > 5) {
            // Improve partial result rates in second half of the run to
            // cover the case of repeated partial results.
            int odd = (attempt < 15) ? num_reads / 2 : 4;
            // No failure in first several attempts.
            size_t& bytes_read = *static_cast<size_t*>(arg);
            if (rnd.OneIn(odd)) {
              bytes_read = 0;
            } else if (rnd.OneIn(odd / 2)) {
              bytes_read = static_cast<size_t>(
                  rnd.Uniform(static_cast<int>(bytes_read)));
            }
          }
        });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    // Generate (offset, len) pairs
    std::set<int> start_offsets;
    for (int i = 0; i < num_reads; i++) {
      int rnd_off;
      // No repeat offsets.
      while (start_offsets.find(rnd_off = rnd.Uniform(81920)) !=
             start_offsets.end()) {
      }
      start_offsets.insert(rnd_off);
    }
    std::vector<size_t> offsets;
    std::vector<size_t> lens;
    // std::set already sorted the offsets.
    for (int so : start_offsets) {
      offsets.push_back(so);
    }
    for (size_t i = 0; i + 1 < offsets.size(); i++) {
      lens.push_back(static_cast<size_t>(
          rnd.Uniform(static_cast<int>(offsets[i + 1] - offsets[i])) + 1));
    }
    lens.push_back(static_cast<size_t>(
        rnd.Uniform(static_cast<int>(kTotalSize - offsets.back())) + 1));
    ASSERT_EQ(num_reads, lens.size());

    // Create requests
    std::vector<std::string> scratches;
    scratches.reserve(num_reads);
    std::vector<ReadRequest> reqs(num_reads);
    for (size_t i = 0; i < reqs.size(); ++i) {
      reqs[i].offset = offsets[i];
      reqs[i].len = lens[i];
      scratches.emplace_back(reqs[i].len, ' ');
      reqs[i].scratch = const_cast<char*>(scratches.back().data());
    }

    // Query the data
    std::unique_ptr<RandomAccessFile> file;
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
    ASSERT_OK(file->MultiRead(reqs.data(), reqs.size()));

    // Validate results
    for (int i = 0; i < num_reads; ++i) {
      ASSERT_OK(reqs[i].status);
      ASSERT_EQ(
          Slice(expected_data.data() + offsets[i], lens[i]).ToString(true),
          reqs[i].result.ToString(true));
    }

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

#ifndef ROCKSDB_LITE
TEST_F(EnvPosixTest, NonAlignedDirectIOMultiReadBeyondFileSize) {
  EnvOptions soptions;
  soptions.use_direct_reads = true;
  soptions.use_direct_writes = false;
  std::string fname = test::PerThreadDBPath(env_, "testfile");

  Random rnd(301);
  std::unique_ptr<WritableFile> wfile;
  size_t alignment = 0;
  // Create file.
  {
    ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));
    auto data_ptr = NewAligned(4095, 'b');
    Slice data_b(data_ptr.get(), 4095);
    ASSERT_OK(wfile->PositionedAppend(data_b, 0U));
    ASSERT_OK(wfile->Close());
  }

#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && \
    !defined(OS_AIX) && !defined(OS_OPENBSD) && !defined(OS_FREEBSD)
  if (soptions.use_direct_reads) {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "NewRandomAccessFile:O_DIRECT", [&](void* arg) {
          int* val = static_cast<int*>(arg);
          *val &= ~O_DIRECT;
        });
  }
#endif
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  const int num_reads = 2;
  // Create requests
  std::vector<std::string> scratches;
  scratches.reserve(num_reads);
  std::vector<ReadRequest> reqs(num_reads);

  std::unique_ptr<RandomAccessFile> file;
  ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
  alignment = file->GetRequiredBufferAlignment();
  ASSERT_EQ(num_reads, reqs.size());

  std::vector<std::unique_ptr<char, Deleter>> data;

  std::vector<size_t> offsets = {0, 2047};
  std::vector<size_t> lens = {2047, 4096 - 2047};

  for (size_t i = 0; i < num_reads; i++) {
    // Do alignment
    reqs[i].offset = static_cast<uint64_t>(TruncateToPageBoundary(
        alignment, static_cast<size_t>(/*offset=*/offsets[i])));
    reqs[i].len =
        Roundup(static_cast<size_t>(/*offset=*/offsets[i]) + /*length=*/lens[i],
                alignment) -
        reqs[i].offset;

    size_t new_capacity = Roundup(reqs[i].len, alignment);
    data.emplace_back(NewAligned(new_capacity, 0));
    reqs[i].scratch = data.back().get();
  }

  // Query the data
  ASSERT_OK(file->MultiRead(reqs.data(), reqs.size()));

  // Validate results
  for (size_t i = 0; i < num_reads; ++i) {
    ASSERT_OK(reqs[i].status);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}
#endif  // ROCKSDB_LITE

#if defined(ROCKSDB_IOURING_PRESENT)
void GenerateFilesAndRequest(Env* env, const std::string& fname,
                             std::vector<ReadRequest>* ret_reqs,
                             std::vector<std::string>* scratches) {
  const size_t kTotalSize = 81920;
  Random rnd(301);
  std::string expected_data = rnd.RandomString(kTotalSize);

  // Create file.
  {
    std::unique_ptr<WritableFile> wfile;
    ASSERT_OK(env->NewWritableFile(fname, &wfile, EnvOptions()));
    ASSERT_OK(wfile->Append(expected_data));
    ASSERT_OK(wfile->Close());
  }

  // Right now kIoUringDepth is hard coded as 256, so we need very large
  // number of keys to cover the case of multiple rounds of submissions.
  // Right now the test latency is still acceptable. If it ends up with
  // too long, we can modify the io uring depth with SyncPoint here.
  const int num_reads = 3;
  std::vector<size_t> offsets = {10000, 20000, 30000};
  std::vector<size_t> lens = {3000, 200, 100};

  // Create requests
  scratches->reserve(num_reads);
  std::vector<ReadRequest>& reqs = *ret_reqs;
  reqs.resize(num_reads);
  for (int i = 0; i < num_reads; ++i) {
    reqs[i].offset = offsets[i];
    reqs[i].len = lens[i];
    scratches->emplace_back(reqs[i].len, ' ');
    reqs[i].scratch = const_cast<char*>(scratches->back().data());
  }
}

TEST_F(EnvPosixTest, MultiReadIOUringError) {
  // In this test we don't do aligned read, so we can't do direct I/O.
  EnvOptions soptions;
  soptions.use_direct_reads = soptions.use_direct_writes = false;
  std::string fname = test::PerThreadDBPath(env_, "testfile");

  std::vector<std::string> scratches;
  std::vector<ReadRequest> reqs;
  GenerateFilesAndRequest(env_, fname, &reqs, &scratches);
  // Query the data
  std::unique_ptr<RandomAccessFile> file;
  ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));

  bool io_uring_wait_cqe_called = false;
  SyncPoint::GetInstance()->SetCallBack(
      "PosixRandomAccessFile::MultiRead:io_uring_wait_cqe:return",
      [&](void* arg) {
        if (!io_uring_wait_cqe_called) {
          io_uring_wait_cqe_called = true;
          ssize_t& ret = *(static_cast<ssize_t*>(arg));
          ret = 1;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = file->MultiRead(reqs.data(), reqs.size());
  if (io_uring_wait_cqe_called) {
    ASSERT_NOK(s);
  } else {
    s.PermitUncheckedError();
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(EnvPosixTest, MultiReadIOUringError2) {
  // In this test we don't do aligned read, so we can't do direct I/O.
  EnvOptions soptions;
  soptions.use_direct_reads = soptions.use_direct_writes = false;
  std::string fname = test::PerThreadDBPath(env_, "testfile");

  std::vector<std::string> scratches;
  std::vector<ReadRequest> reqs;
  GenerateFilesAndRequest(env_, fname, &reqs, &scratches);
  // Query the data
  std::unique_ptr<RandomAccessFile> file;
  ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));

  bool io_uring_submit_and_wait_called = false;
  SyncPoint::GetInstance()->SetCallBack(
      "PosixRandomAccessFile::MultiRead:io_uring_submit_and_wait:return1",
      [&](void* arg) {
        io_uring_submit_and_wait_called = true;
        ssize_t* ret = static_cast<ssize_t*>(arg);
        (*ret)--;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "PosixRandomAccessFile::MultiRead:io_uring_submit_and_wait:return2",
      [&](void* arg) {
        struct io_uring* iu = static_cast<struct io_uring*>(arg);
        struct io_uring_cqe* cqe;
        assert(io_uring_wait_cqe(iu, &cqe) == 0);
        io_uring_cqe_seen(iu, cqe);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = file->MultiRead(reqs.data(), reqs.size());
  if (io_uring_submit_and_wait_called) {
    ASSERT_NOK(s);
  } else {
    s.PermitUncheckedError();
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif  // ROCKSDB_IOURING_PRESENT

// Only works in linux platforms
#ifdef OS_WIN
TEST_P(EnvPosixTestWithParam, DISABLED_InvalidateCache) {
#else
TEST_P(EnvPosixTestWithParam, InvalidateCache) {
#endif
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  EnvOptions soptions;
  soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;
  std::string fname = test::PerThreadDBPath(env_, "testfile");

  const size_t kSectorSize = 512;
  auto data = NewAligned(kSectorSize, 0);
  Slice slice(data.get(), kSectorSize);

  // Create file.
  {
    std::unique_ptr<WritableFile> wfile;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && \
    !defined(OS_AIX)
    if (soptions.use_direct_writes) {
      soptions.use_direct_writes = false;
    }
#endif
    ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));
    ASSERT_OK(wfile->Append(slice));
    ASSERT_OK(wfile->InvalidateCache(0, 0));
    ASSERT_OK(wfile->Close());
  }

  // Random Read
  {
    std::unique_ptr<RandomAccessFile> file;
    auto scratch = NewAligned(kSectorSize, 0);
    Slice result;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && \
    !defined(OS_AIX)
    if (soptions.use_direct_reads) {
      soptions.use_direct_reads = false;
    }
#endif
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
    ASSERT_OK(file->Read(0, kSectorSize, &result, scratch.get()));
    ASSERT_EQ(memcmp(scratch.get(), data.get(), kSectorSize), 0);
    ASSERT_OK(file->InvalidateCache(0, 11));
    ASSERT_OK(file->InvalidateCache(0, 0));
  }

  // Sequential Read
  {
    std::unique_ptr<SequentialFile> file;
    auto scratch = NewAligned(kSectorSize, 0);
    Slice result;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && \
    !defined(OS_AIX)
    if (soptions.use_direct_reads) {
      soptions.use_direct_reads = false;
    }
#endif
    ASSERT_OK(env_->NewSequentialFile(fname, &file, soptions));
    if (file->use_direct_io()) {
      ASSERT_OK(file->PositionedRead(0, kSectorSize, &result, scratch.get()));
    } else {
      ASSERT_OK(file->Read(kSectorSize, &result, scratch.get()));
    }
    ASSERT_EQ(memcmp(scratch.get(), data.get(), kSectorSize), 0);
    ASSERT_OK(file->InvalidateCache(0, 11));
    ASSERT_OK(file->InvalidateCache(0, 0));
  }
  // Delete the file
  ASSERT_OK(env_->DeleteFile(fname));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
}
#endif  // OS_LINUX || OS_WIN

class TestLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    log_count++;

    char new_format[550];
    std::fill_n(new_format, sizeof(new_format), '2');
    {
      va_list backup_ap;
      va_copy(backup_ap, ap);
      int n = vsnprintf(new_format, sizeof(new_format) - 1, format, backup_ap);
      // 48 bytes for extra information + bytes allocated

// When we have n == -1 there is not a terminating zero expected
#ifdef OS_WIN
      if (n < 0) {
        char_0_count++;
      }
#endif

      if (new_format[0] == '[') {
        // "[DEBUG] "
        ASSERT_TRUE(n <= 56 + (512 - static_cast<int>(sizeof(port::TimeVal))));
      } else {
        ASSERT_TRUE(n <= 48 + (512 - static_cast<int>(sizeof(port::TimeVal))));
      }
      va_end(backup_ap);
    }

    for (size_t i = 0; i < sizeof(new_format); i++) {
      if (new_format[i] == 'x') {
        char_x_count++;
      } else if (new_format[i] == '\0') {
        char_0_count++;
      }
    }
  }
  int log_count;
  int char_x_count;
  int char_0_count;
};

TEST_P(EnvPosixTestWithParam, LogBufferTest) {
  TestLogger test_logger;
  test_logger.SetInfoLogLevel(InfoLogLevel::INFO_LEVEL);
  test_logger.log_count = 0;
  test_logger.char_x_count = 0;
  test_logger.char_0_count = 0;
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, &test_logger);
  LogBuffer log_buffer_debug(DEBUG_LEVEL, &test_logger);

  char bytes200[200];
  std::fill_n(bytes200, sizeof(bytes200), '1');
  bytes200[sizeof(bytes200) - 1] = '\0';
  char bytes600[600];
  std::fill_n(bytes600, sizeof(bytes600), '1');
  bytes600[sizeof(bytes600) - 1] = '\0';
  char bytes9000[9000];
  std::fill_n(bytes9000, sizeof(bytes9000), '1');
  bytes9000[sizeof(bytes9000) - 1] = '\0';

  ROCKS_LOG_BUFFER(&log_buffer, "x%sx", bytes200);
  ROCKS_LOG_BUFFER(&log_buffer, "x%sx", bytes600);
  ROCKS_LOG_BUFFER(&log_buffer, "x%sx%sx%sx", bytes200, bytes200, bytes200);
  ROCKS_LOG_BUFFER(&log_buffer, "x%sx%sx", bytes200, bytes600);
  ROCKS_LOG_BUFFER(&log_buffer, "x%sx%sx", bytes600, bytes9000);

  ROCKS_LOG_BUFFER(&log_buffer_debug, "x%sx", bytes200);
  test_logger.SetInfoLogLevel(DEBUG_LEVEL);
  ROCKS_LOG_BUFFER(&log_buffer_debug, "x%sx%sx%sx", bytes600, bytes9000,
                   bytes200);

  ASSERT_EQ(0, test_logger.log_count);
  log_buffer.FlushBufferToLog();
  log_buffer_debug.FlushBufferToLog();
  ASSERT_EQ(6, test_logger.log_count);
  ASSERT_EQ(6, test_logger.char_0_count);
  ASSERT_EQ(10, test_logger.char_x_count);
}

class TestLogger2 : public Logger {
 public:
  explicit TestLogger2(size_t max_log_size) : max_log_size_(max_log_size) {}
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    char new_format[2000];
    std::fill_n(new_format, sizeof(new_format), '2');
    {
      va_list backup_ap;
      va_copy(backup_ap, ap);
      int n = vsnprintf(new_format, sizeof(new_format) - 1, format, backup_ap);
      // 48 bytes for extra information + bytes allocated
      ASSERT_TRUE(n <=
                  48 + static_cast<int>(max_log_size_ - sizeof(port::TimeVal)));
      ASSERT_TRUE(n > static_cast<int>(max_log_size_ - sizeof(port::TimeVal)));
      va_end(backup_ap);
    }
  }
  size_t max_log_size_;
};

TEST_P(EnvPosixTestWithParam, LogBufferMaxSizeTest) {
  char bytes9000[9000];
  std::fill_n(bytes9000, sizeof(bytes9000), '1');
  bytes9000[sizeof(bytes9000) - 1] = '\0';

  for (size_t max_log_size = 256; max_log_size <= 1024;
       max_log_size += 1024 - 256) {
    TestLogger2 test_logger(max_log_size);
    test_logger.SetInfoLogLevel(InfoLogLevel::INFO_LEVEL);
    LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, &test_logger);
    ROCKS_LOG_BUFFER_MAX_SZ(&log_buffer, max_log_size, "%s", bytes9000);
    log_buffer.FlushBufferToLog();
  }
}

TEST_P(EnvPosixTestWithParam, Preallocation) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  const std::string src = test::PerThreadDBPath(env_, "testfile");
  std::unique_ptr<WritableFile> srcfile;
  EnvOptions soptions;
  soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && \
    !defined(OS_AIX) && !defined(OS_OPENBSD) && !defined(OS_FREEBSD)
  if (soptions.use_direct_writes) {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "NewWritableFile:O_DIRECT", [&](void* arg) {
          int* val = static_cast<int*>(arg);
          *val &= ~O_DIRECT;
        });
  }
#endif
  ASSERT_OK(env_->NewWritableFile(src, &srcfile, soptions));
  srcfile->SetPreallocationBlockSize(1024 * 1024);

  // No writes should mean no preallocation
  size_t block_size, last_allocated_block;
  srcfile->GetPreallocationStatus(&block_size, &last_allocated_block);
  ASSERT_EQ(last_allocated_block, 0UL);

  // Small write should preallocate one block
  size_t kStrSize = 4096;
  auto data = NewAligned(kStrSize, 'A');
  Slice str(data.get(), kStrSize);
  srcfile->PrepareWrite(srcfile->GetFileSize(), kStrSize);
  ASSERT_OK(srcfile->Append(str));
  srcfile->GetPreallocationStatus(&block_size, &last_allocated_block);
  ASSERT_EQ(last_allocated_block, 1UL);

  // Write an entire preallocation block, make sure we increased by two.
  {
    auto buf_ptr = NewAligned(block_size, ' ');
    Slice buf(buf_ptr.get(), block_size);
    srcfile->PrepareWrite(srcfile->GetFileSize(), block_size);
    ASSERT_OK(srcfile->Append(buf));
    srcfile->GetPreallocationStatus(&block_size, &last_allocated_block);
    ASSERT_EQ(last_allocated_block, 2UL);
  }

  // Write five more blocks at once, ensure we're where we need to be.
  {
    auto buf_ptr = NewAligned(block_size * 5, ' ');
    Slice buf = Slice(buf_ptr.get(), block_size * 5);
    srcfile->PrepareWrite(srcfile->GetFileSize(), buf.size());
    ASSERT_OK(srcfile->Append(buf));
    srcfile->GetPreallocationStatus(&block_size, &last_allocated_block);
    ASSERT_EQ(last_allocated_block, 7UL);
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
}

// Test that the two ways to get children file attributes (in bulk or
// individually) behave consistently.
TEST_P(EnvPosixTestWithParam, ConsistentChildrenAttributes) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  EnvOptions soptions;
  soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;
  const int kNumChildren = 10;

  std::string data;
  std::string test_base_dir = test::PerThreadDBPath(env_, "env_test_chr_attr");
  env_->CreateDir(test_base_dir).PermitUncheckedError();
  for (int i = 0; i < kNumChildren; ++i) {
    const std::string path = test_base_dir + "/testfile_" + std::to_string(i);
    std::unique_ptr<WritableFile> file;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && \
    !defined(OS_AIX) && !defined(OS_OPENBSD) && !defined(OS_FREEBSD)
    if (soptions.use_direct_writes) {
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "NewWritableFile:O_DIRECT", [&](void* arg) {
            int* val = static_cast<int*>(arg);
            *val &= ~O_DIRECT;
          });
    }
#endif
    ASSERT_OK(env_->NewWritableFile(path, &file, soptions));
    auto buf_ptr = NewAligned(data.size(), 'T');
    Slice buf(buf_ptr.get(), data.size());
    ASSERT_OK(file->Append(buf));
    data.append(std::string(4096, 'T'));
  }

  std::vector<Env::FileAttributes> file_attrs;
  ASSERT_OK(env_->GetChildrenFileAttributes(test_base_dir, &file_attrs));
  for (int i = 0; i < kNumChildren; ++i) {
    const std::string name = "testfile_" + std::to_string(i);
    const std::string path = test_base_dir + "/" + name;

    auto file_attrs_iter = std::find_if(
        file_attrs.begin(), file_attrs.end(),
        [&name](const Env::FileAttributes& fm) { return fm.name == name; });
    ASSERT_TRUE(file_attrs_iter != file_attrs.end());
    uint64_t size;
    ASSERT_OK(env_->GetFileSize(path, &size));
    ASSERT_EQ(size, 4096 * i);
    ASSERT_EQ(size, file_attrs_iter->size_bytes);
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
}

// Test that all WritableFileWrapper forwards all calls to WritableFile.
TEST_P(EnvPosixTestWithParam, WritableFileWrapper) {
  class Base : public WritableFile {
   public:
    mutable int* step_;

    void inc(int x) const { EXPECT_EQ(x, (*step_)++); }

    explicit Base(int* step) : step_(step) { inc(0); }

    Status Append(const Slice& /*data*/) override {
      inc(1);
      return Status::OK();
    }

    Status Append(
        const Slice& /*data*/,
        const DataVerificationInfo& /* verification_info */) override {
      inc(1);
      return Status::OK();
    }

    Status PositionedAppend(const Slice& /*data*/,
                            uint64_t /*offset*/) override {
      inc(2);
      return Status::OK();
    }

    Status PositionedAppend(
        const Slice& /*data*/, uint64_t /*offset*/,
        const DataVerificationInfo& /* verification_info */) override {
      inc(2);
      return Status::OK();
    }

    Status Truncate(uint64_t /*size*/) override {
      inc(3);
      return Status::OK();
    }

    Status Close() override {
      inc(4);
      return Status::OK();
    }

    Status Flush() override {
      inc(5);
      return Status::OK();
    }

    Status Sync() override {
      inc(6);
      return Status::OK();
    }

    Status Fsync() override {
      inc(7);
      return Status::OK();
    }

    bool IsSyncThreadSafe() const override {
      inc(8);
      return true;
    }

    bool use_direct_io() const override {
      inc(9);
      return true;
    }

    size_t GetRequiredBufferAlignment() const override {
      inc(10);
      return 0;
    }

    void SetIOPriority(Env::IOPriority /*pri*/) override { inc(11); }

    Env::IOPriority GetIOPriority() override {
      inc(12);
      return Env::IOPriority::IO_LOW;
    }

    void SetWriteLifeTimeHint(Env::WriteLifeTimeHint /*hint*/) override {
      inc(13);
    }

    Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
      inc(14);
      return Env::WriteLifeTimeHint::WLTH_NOT_SET;
    }

    uint64_t GetFileSize() override {
      inc(15);
      return 0;
    }

    void SetPreallocationBlockSize(size_t /*size*/) override { inc(16); }

    void GetPreallocationStatus(size_t* /*block_size*/,
                                size_t* /*last_allocated_block*/) override {
      inc(17);
    }

    size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const override {
      inc(18);
      return 0;
    }

    Status InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
      inc(19);
      return Status::OK();
    }

    Status RangeSync(uint64_t /*offset*/, uint64_t /*nbytes*/) override {
      inc(20);
      return Status::OK();
    }

    void PrepareWrite(size_t /*offset*/, size_t /*len*/) override { inc(21); }

    Status Allocate(uint64_t /*offset*/, uint64_t /*len*/) override {
      inc(22);
      return Status::OK();
    }

   public:
    ~Base() override { inc(23); }
  };

  class Wrapper : public WritableFileWrapper {
   public:
    explicit Wrapper(WritableFile* target) : WritableFileWrapper(target) {}
  };

  int step = 0;

  {
    Base b(&step);
    Wrapper w(&b);
    ASSERT_OK(w.Append(Slice()));
    ASSERT_OK(w.PositionedAppend(Slice(), 0));
    ASSERT_OK(w.Truncate(0));
    ASSERT_OK(w.Close());
    ASSERT_OK(w.Flush());
    ASSERT_OK(w.Sync());
    ASSERT_OK(w.Fsync());
    w.IsSyncThreadSafe();
    w.use_direct_io();
    w.GetRequiredBufferAlignment();
    w.SetIOPriority(Env::IOPriority::IO_HIGH);
    w.GetIOPriority();
    w.SetWriteLifeTimeHint(Env::WriteLifeTimeHint::WLTH_NOT_SET);
    w.GetWriteLifeTimeHint();
    w.GetFileSize();
    w.SetPreallocationBlockSize(0);
    w.GetPreallocationStatus(nullptr, nullptr);
    w.GetUniqueId(nullptr, 0);
    ASSERT_OK(w.InvalidateCache(0, 0));
    ASSERT_OK(w.RangeSync(0, 0));
    w.PrepareWrite(0, 0);
    ASSERT_OK(w.Allocate(0, 0));
  }

  EXPECT_EQ(24, step);
}

TEST_P(EnvPosixTestWithParam, PosixRandomRWFile) {
  const std::string path = test::PerThreadDBPath(env_, "random_rw_file");

  env_->DeleteFile(path).PermitUncheckedError();

  std::unique_ptr<RandomRWFile> file;

  // Cannot open non-existing file.
  ASSERT_NOK(env_->NewRandomRWFile(path, &file, EnvOptions()));

  // Create the file using WritableFile
  {
    std::unique_ptr<WritableFile> wf;
    ASSERT_OK(env_->NewWritableFile(path, &wf, EnvOptions()));
  }

  ASSERT_OK(env_->NewRandomRWFile(path, &file, EnvOptions()));

  char buf[10000];
  Slice read_res;

  ASSERT_OK(file->Write(0, "ABCD"));
  ASSERT_OK(file->Read(0, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ABCD");

  ASSERT_OK(file->Write(2, "XXXX"));
  ASSERT_OK(file->Read(0, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ABXXXX");

  ASSERT_OK(file->Write(10, "ZZZ"));
  ASSERT_OK(file->Read(10, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ZZZ");

  ASSERT_OK(file->Write(11, "Y"));
  ASSERT_OK(file->Read(10, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ZYZ");

  ASSERT_OK(file->Write(200, "FFFFF"));
  ASSERT_OK(file->Read(200, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "FFFFF");

  ASSERT_OK(file->Write(205, "XXXX"));
  ASSERT_OK(file->Read(200, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "FFFFFXXXX");

  ASSERT_OK(file->Write(5, "QQQQ"));
  ASSERT_OK(file->Read(0, 9, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ABXXXQQQQ");

  ASSERT_OK(file->Read(2, 4, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "XXXQ");

  // Close file and reopen it
  ASSERT_OK(file->Close());
  ASSERT_OK(env_->NewRandomRWFile(path, &file, EnvOptions()));

  ASSERT_OK(file->Read(0, 9, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ABXXXQQQQ");

  ASSERT_OK(file->Read(10, 3, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ZYZ");

  ASSERT_OK(file->Read(200, 9, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "FFFFFXXXX");

  ASSERT_OK(file->Write(4, "TTTTTTTTTTTTTTTT"));
  ASSERT_OK(file->Read(0, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ABXXTTTTTT");

  // Clean up
  ASSERT_OK(env_->DeleteFile(path));
}

class RandomRWFileWithMirrorString {
 public:
  explicit RandomRWFileWithMirrorString(RandomRWFile* _file) : file_(_file) {}

  void Write(size_t offset, const std::string& data) {
    // Write to mirror string
    StringWrite(offset, data);

    // Write to file
    Status s = file_->Write(offset, data);
    ASSERT_OK(s) << s.ToString();
  }

  void Read(size_t offset = 0, size_t n = 1000000) {
    Slice str_res(nullptr, 0);
    if (offset < file_mirror_.size()) {
      size_t str_res_sz = std::min(file_mirror_.size() - offset, n);
      str_res = Slice(file_mirror_.data() + offset, str_res_sz);
      StopSliceAtNull(&str_res);
    }

    Slice file_res;
    Status s = file_->Read(offset, n, &file_res, buf_);
    ASSERT_OK(s) << s.ToString();
    StopSliceAtNull(&file_res);

    ASSERT_EQ(str_res.ToString(), file_res.ToString()) << offset << " " << n;
  }

  void SetFile(RandomRWFile* _file) { file_ = _file; }

 private:
  void StringWrite(size_t offset, const std::string& src) {
    if (offset + src.size() > file_mirror_.size()) {
      file_mirror_.resize(offset + src.size(), '\0');
    }

    char* pos = const_cast<char*>(file_mirror_.data() + offset);
    memcpy(pos, src.data(), src.size());
  }

  void StopSliceAtNull(Slice* slc) {
    for (size_t i = 0; i < slc->size(); i++) {
      if ((*slc)[i] == '\0') {
        *slc = Slice(slc->data(), i);
        break;
      }
    }
  }

  char buf_[10000];
  RandomRWFile* file_;
  std::string file_mirror_;
};

TEST_P(EnvPosixTestWithParam, PosixRandomRWFileRandomized) {
  const std::string path = test::PerThreadDBPath(env_, "random_rw_file_rand");
  env_->DeleteFile(path).PermitUncheckedError();

  std::unique_ptr<RandomRWFile> file;

#ifdef OS_LINUX
  // Cannot open non-existing file.
  ASSERT_NOK(env_->NewRandomRWFile(path, &file, EnvOptions()));
#endif

  // Create the file using WritableFile
  {
    std::unique_ptr<WritableFile> wf;
    ASSERT_OK(env_->NewWritableFile(path, &wf, EnvOptions()));
  }

  ASSERT_OK(env_->NewRandomRWFile(path, &file, EnvOptions()));
  RandomRWFileWithMirrorString file_with_mirror(file.get());

  Random rnd(301);
  std::string buf;
  for (int i = 0; i < 10000; i++) {
    // Genrate random data
    buf = rnd.RandomString(10);

    // Pick random offset for write
    size_t write_off = rnd.Next() % 1000;
    file_with_mirror.Write(write_off, buf);

    // Pick random offset for read
    size_t read_off = rnd.Next() % 1000;
    size_t read_sz = rnd.Next() % 20;
    file_with_mirror.Read(read_off, read_sz);

    if (i % 500 == 0) {
      // Reopen the file every 500 iters
      ASSERT_OK(env_->NewRandomRWFile(path, &file, EnvOptions()));
      file_with_mirror.SetFile(file.get());
    }
  }

  // clean up
  ASSERT_OK(env_->DeleteFile(path));
}

class TestEnv : public EnvWrapper {
 public:
  explicit TestEnv() : EnvWrapper(Env::Default()), close_count(0) {}
  const char* Name() const override { return "TestEnv"; }
  class TestLogger : public Logger {
   public:
    using Logger::Logv;
    explicit TestLogger(TestEnv* env_ptr) : Logger() { env = env_ptr; }
    ~TestLogger() override {
      if (!closed_) {
        Status s = CloseHelper();
        s.PermitUncheckedError();
      }
    }
    void Logv(const char* /*format*/, va_list /*ap*/) override {}

   protected:
    Status CloseImpl() override { return CloseHelper(); }

   private:
    Status CloseHelper() {
      env->CloseCountInc();
      return Status::OK();
    }
    TestEnv* env;
  };

  void CloseCountInc() { close_count++; }

  int GetCloseCount() { return close_count; }

  Status NewLogger(const std::string& /*fname*/,
                   std::shared_ptr<Logger>* result) override {
    result->reset(new TestLogger(this));
    return Status::OK();
  }

 private:
  int close_count;
};

class EnvTest : public testing::Test {
 public:
  EnvTest() : test_directory_(test::PerThreadDBPath("env_test")) {}

 protected:
  const std::string test_directory_;
};

TEST_F(EnvTest, Close) {
  TestEnv* env = new TestEnv();
  std::shared_ptr<Logger> logger;
  Status s;

  s = env->NewLogger("", &logger);
  ASSERT_OK(s);
  ASSERT_OK(logger.get()->Close());
  ASSERT_EQ(env->GetCloseCount(), 1);
  // Call Close() again. CloseHelper() should not be called again
  ASSERT_OK(logger.get()->Close());
  ASSERT_EQ(env->GetCloseCount(), 1);
  logger.reset();
  ASSERT_EQ(env->GetCloseCount(), 1);

  s = env->NewLogger("", &logger);
  ASSERT_OK(s);
  logger.reset();
  ASSERT_EQ(env->GetCloseCount(), 2);

  delete env;
}

class LogvWithInfoLogLevelLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const InfoLogLevel /* log_level */, const char* /* format */,
            va_list /* ap */) override {}
};

TEST_F(EnvTest, LogvWithInfoLogLevel) {
  // Verifies the log functions work on a `Logger` that only overrides the
  // `Logv()` overload including `InfoLogLevel`.
  const std::string kSampleMessage("sample log message");
  LogvWithInfoLogLevelLogger logger;
  ROCKS_LOG_HEADER(&logger, "%s", kSampleMessage.c_str());
  ROCKS_LOG_DEBUG(&logger, "%s", kSampleMessage.c_str());
  ROCKS_LOG_INFO(&logger, "%s", kSampleMessage.c_str());
  ROCKS_LOG_WARN(&logger, "%s", kSampleMessage.c_str());
  ROCKS_LOG_ERROR(&logger, "%s", kSampleMessage.c_str());
  ROCKS_LOG_FATAL(&logger, "%s", kSampleMessage.c_str());
}

INSTANTIATE_TEST_CASE_P(DefaultEnvWithoutDirectIO, EnvPosixTestWithParam,
                        ::testing::Values(std::pair<Env*, bool>(Env::Default(),
                                                                false)));
#if !defined(ROCKSDB_LITE)
INSTANTIATE_TEST_CASE_P(DefaultEnvWithDirectIO, EnvPosixTestWithParam,
                        ::testing::Values(std::pair<Env*, bool>(Env::Default(),
                                                                true)));
#endif  // !defined(ROCKSDB_LITE)

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)
static Env* GetChrootEnv() {
  static std::unique_ptr<Env> chroot_env(
      NewChrootEnv(Env::Default(), test::TmpDir(Env::Default())));
  return chroot_env.get();
}
INSTANTIATE_TEST_CASE_P(ChrootEnvWithoutDirectIO, EnvPosixTestWithParam,
                        ::testing::Values(std::pair<Env*, bool>(GetChrootEnv(),
                                                                false)));
INSTANTIATE_TEST_CASE_P(ChrootEnvWithDirectIO, EnvPosixTestWithParam,
                        ::testing::Values(std::pair<Env*, bool>(GetChrootEnv(),
                                                                true)));
#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)

class EnvFSTestWithParam
    : public ::testing::Test,
      public ::testing::WithParamInterface<std::tuple<bool, bool, bool>> {
 public:
  EnvFSTestWithParam() {
    bool env_non_null = std::get<0>(GetParam());
    bool env_default = std::get<1>(GetParam());
    bool fs_default = std::get<2>(GetParam());

    env_ = env_non_null ? (env_default ? Env::Default() : nullptr) : nullptr;
    fs_ = fs_default
              ? FileSystem::Default()
              : std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
    if (env_non_null && env_default && !fs_default) {
      env_ptr_ = NewCompositeEnv(fs_);
    }
    if (env_non_null && !env_default && fs_default) {
      env_ptr_ =
          std::unique_ptr<Env>(new FaultInjectionTestEnv(Env::Default()));
      fs_.reset();
    }
    if (env_non_null && !env_default && !fs_default) {
      env_ptr_.reset(new FaultInjectionTestEnv(Env::Default()));
      composite_env_ptr_.reset(new CompositeEnvWrapper(env_ptr_.get(), fs_));
      env_ = composite_env_ptr_.get();
    } else {
      env_ = env_ptr_.get();
    }

    dbname1_ = test::PerThreadDBPath("env_fs_test1");
    dbname2_ = test::PerThreadDBPath("env_fs_test2");
  }

  ~EnvFSTestWithParam() = default;

  Env* env_;
  std::unique_ptr<Env> env_ptr_;
  std::unique_ptr<Env> composite_env_ptr_;
  std::shared_ptr<FileSystem> fs_;
  std::string dbname1_;
  std::string dbname2_;
};

TEST_P(EnvFSTestWithParam, OptionsTest) {
  Options opts;
  opts.env = env_;
  opts.create_if_missing = true;
  std::string dbname = dbname1_;

  if (env_) {
    if (fs_) {
      ASSERT_EQ(fs_.get(), env_->GetFileSystem().get());
    } else {
      ASSERT_NE(FileSystem::Default().get(), env_->GetFileSystem().get());
    }
  }
  for (int i = 0; i < 2; ++i) {
    DB* db;
    Status s = DB::Open(opts, dbname, &db);
    ASSERT_OK(s);

    WriteOptions wo;
    ASSERT_OK(db->Put(wo, "a", "a"));
    ASSERT_OK(db->Flush(FlushOptions()));
    ASSERT_OK(db->Put(wo, "b", "b"));
    ASSERT_OK(db->Flush(FlushOptions()));
    ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    std::string val;
    ASSERT_OK(db->Get(ReadOptions(), "a", &val));
    ASSERT_EQ("a", val);
    ASSERT_OK(db->Get(ReadOptions(), "b", &val));
    ASSERT_EQ("b", val);

    ASSERT_OK(db->Close());
    delete db;
    ASSERT_OK(DestroyDB(dbname, opts));

    dbname = dbname2_;
  }
}

// The parameters are as follows -
// 1. True means Options::env is non-null, false means null
// 2. True means use Env::Default, false means custom
// 3. True means use FileSystem::Default, false means custom
INSTANTIATE_TEST_CASE_P(EnvFSTest, EnvFSTestWithParam,
                        ::testing::Combine(::testing::Bool(), ::testing::Bool(),
                                           ::testing::Bool()));
// This test ensures that default Env and those allocated by
// NewCompositeEnv() all share the same threadpool
TEST_F(EnvTest, MultipleCompositeEnv) {
  std::shared_ptr<FaultInjectionTestFS> fs1 =
      std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
  std::shared_ptr<FaultInjectionTestFS> fs2 =
      std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
  std::unique_ptr<Env> env1 = NewCompositeEnv(fs1);
  std::unique_ptr<Env> env2 = NewCompositeEnv(fs2);
  Env::Default()->SetBackgroundThreads(8, Env::HIGH);
  Env::Default()->SetBackgroundThreads(16, Env::LOW);
  ASSERT_EQ(env1->GetBackgroundThreads(Env::LOW), 16);
  ASSERT_EQ(env1->GetBackgroundThreads(Env::HIGH), 8);
  ASSERT_EQ(env2->GetBackgroundThreads(Env::LOW), 16);
  ASSERT_EQ(env2->GetBackgroundThreads(Env::HIGH), 8);
}

TEST_F(EnvTest, IsDirectory) {
  Status s = Env::Default()->CreateDirIfMissing(test_directory_);
  ASSERT_OK(s);
  const std::string test_sub_dir = test_directory_ + "sub1";
  const std::string test_file_path = test_directory_ + "file1";
  ASSERT_OK(Env::Default()->CreateDirIfMissing(test_sub_dir));
  bool is_dir = false;
  ASSERT_OK(Env::Default()->IsDirectory(test_sub_dir, &is_dir));
  ASSERT_TRUE(is_dir);
  {
    std::unique_ptr<FSWritableFile> wfile;
    s = Env::Default()->GetFileSystem()->NewWritableFile(
        test_file_path, FileOptions(), &wfile, /*dbg=*/nullptr);
    ASSERT_OK(s);
    std::unique_ptr<WritableFileWriter> fwriter;
    fwriter.reset(new WritableFileWriter(std::move(wfile), test_file_path,
                                         FileOptions(),
                                         SystemClock::Default().get()));
    constexpr char buf[] = "test";
    s = fwriter->Append(buf);
    ASSERT_OK(s);
  }
  ASSERT_OK(Env::Default()->IsDirectory(test_file_path, &is_dir));
  ASSERT_FALSE(is_dir);
}

TEST_F(EnvTest, EnvWriteVerificationTest) {
  Status s = Env::Default()->CreateDirIfMissing(test_directory_);
  const std::string test_file_path = test_directory_ + "file1";
  ASSERT_OK(s);
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::unique_ptr<WritableFile> file;
  s = fault_fs_env->NewWritableFile(test_file_path, &file, EnvOptions());
  ASSERT_OK(s);

  DataVerificationInfo v_info;
  std::string test_data = "test";
  std::string checksum;
  uint32_t v_crc32c = crc32c::Extend(0, test_data.c_str(), test_data.size());
  PutFixed32(&checksum, v_crc32c);
  v_info.checksum = Slice(checksum);
  s = file->Append(Slice(test_data), v_info);
  ASSERT_OK(s);
}

class CreateEnvTest : public testing::Test {
 public:
  CreateEnvTest() {
    config_options_.ignore_unknown_options = false;
    config_options_.ignore_unsupported_options = false;
  }
  ConfigOptions config_options_;
};

#ifndef ROCKSDB_LITE
TEST_F(CreateEnvTest, LoadCTRProvider) {
  config_options_.invoke_prepare_options = false;
  std::string CTR = CTREncryptionProvider::kClassName();
  std::shared_ptr<EncryptionProvider> provider;
  // Test a provider with no cipher
  ASSERT_OK(
      EncryptionProvider::CreateFromString(config_options_, CTR, &provider));
  ASSERT_NE(provider, nullptr);
  ASSERT_EQ(provider->Name(), CTR);
  ASSERT_NOK(provider->PrepareOptions(config_options_));
  ASSERT_NOK(provider->ValidateOptions(DBOptions(), ColumnFamilyOptions()));
  auto cipher = provider->GetOptions<std::shared_ptr<BlockCipher>>("Cipher");
  ASSERT_NE(cipher, nullptr);
  ASSERT_EQ(cipher->get(), nullptr);
  provider.reset();

  ASSERT_OK(EncryptionProvider::CreateFromString(config_options_,
                                                 CTR + "://test", &provider));
  ASSERT_NE(provider, nullptr);
  ASSERT_EQ(provider->Name(), CTR);
  ASSERT_OK(provider->PrepareOptions(config_options_));
  ASSERT_OK(provider->ValidateOptions(DBOptions(), ColumnFamilyOptions()));
  cipher = provider->GetOptions<std::shared_ptr<BlockCipher>>("Cipher");
  ASSERT_NE(cipher, nullptr);
  ASSERT_NE(cipher->get(), nullptr);
  ASSERT_STREQ(cipher->get()->Name(), "ROT13");
  provider.reset();

  ASSERT_OK(EncryptionProvider::CreateFromString(config_options_, "1://test",
                                                 &provider));
  ASSERT_NE(provider, nullptr);
  ASSERT_EQ(provider->Name(), CTR);
  ASSERT_OK(provider->PrepareOptions(config_options_));
  ASSERT_OK(provider->ValidateOptions(DBOptions(), ColumnFamilyOptions()));
  cipher = provider->GetOptions<std::shared_ptr<BlockCipher>>("Cipher");
  ASSERT_NE(cipher, nullptr);
  ASSERT_NE(cipher->get(), nullptr);
  ASSERT_STREQ(cipher->get()->Name(), "ROT13");
  provider.reset();

  ASSERT_OK(EncryptionProvider::CreateFromString(
      config_options_, "id=" + CTR + "; cipher=ROT13", &provider));
  ASSERT_NE(provider, nullptr);
  ASSERT_EQ(provider->Name(), CTR);
  cipher = provider->GetOptions<std::shared_ptr<BlockCipher>>("Cipher");
  ASSERT_NE(cipher, nullptr);
  ASSERT_NE(cipher->get(), nullptr);
  ASSERT_STREQ(cipher->get()->Name(), "ROT13");
  provider.reset();
}

TEST_F(CreateEnvTest, LoadROT13Cipher) {
  std::shared_ptr<BlockCipher> cipher;
  // Test a provider with no cipher
  ASSERT_OK(BlockCipher::CreateFromString(config_options_, "ROT13", &cipher));
  ASSERT_NE(cipher, nullptr);
  ASSERT_STREQ(cipher->Name(), "ROT13");
}
#endif  // ROCKSDB_LITE

TEST_F(CreateEnvTest, CreateDefaultSystemClock) {
  std::shared_ptr<SystemClock> clock, copy;
  ASSERT_OK(SystemClock::CreateFromString(config_options_,
                                          SystemClock::kDefaultName(), &clock));
  ASSERT_NE(clock, nullptr);
  ASSERT_EQ(clock, SystemClock::Default());
#ifndef ROCKSDB_LITE
  std::string opts_str = clock->ToString(config_options_);
  std::string mismatch;
  ASSERT_OK(SystemClock::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(clock->AreEquivalent(config_options_, copy.get(), &mismatch));
#endif  // ROCKSDB_LITE
}

#ifndef ROCKSDB_LITE
TEST_F(CreateEnvTest, CreateMockSystemClock) {
  std::shared_ptr<SystemClock> mock, copy;

  config_options_.registry->AddLibrary("test")->AddFactory<SystemClock>(
      MockSystemClock::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<SystemClock>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockSystemClock(nullptr));
        return guard->get();
      });
  ASSERT_OK(SystemClock::CreateFromString(
      config_options_, EmulatedSystemClock::kClassName(), &mock));
  ASSERT_NE(mock, nullptr);
  ASSERT_STREQ(mock->Name(), EmulatedSystemClock::kClassName());
  ASSERT_EQ(mock->Inner(), SystemClock::Default().get());
  std::string opts_str = mock->ToString(config_options_);
  std::string mismatch;
  ASSERT_OK(SystemClock::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(mock->AreEquivalent(config_options_, copy.get(), &mismatch));

  std::string id = std::string("id=") + EmulatedSystemClock::kClassName() +
                   ";target=" + MockSystemClock::kClassName();

  ASSERT_OK(SystemClock::CreateFromString(config_options_, id, &mock));
  ASSERT_NE(mock, nullptr);
  ASSERT_STREQ(mock->Name(), EmulatedSystemClock::kClassName());
  ASSERT_NE(mock->Inner(), nullptr);
  ASSERT_STREQ(mock->Inner()->Name(), MockSystemClock::kClassName());
  ASSERT_EQ(mock->Inner()->Inner(), SystemClock::Default().get());
  opts_str = mock->ToString(config_options_);
  ASSERT_OK(SystemClock::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(mock->AreEquivalent(config_options_, copy.get(), &mismatch));
  ASSERT_OK(SystemClock::CreateFromString(
      config_options_, EmulatedSystemClock::kClassName(), &mock));
}

TEST_F(CreateEnvTest, CreateReadOnlyFileSystem) {
  std::shared_ptr<FileSystem> fs, copy;

  ASSERT_OK(FileSystem::CreateFromString(
      config_options_, ReadOnlyFileSystem::kClassName(), &fs));
  ASSERT_NE(fs, nullptr);
  ASSERT_STREQ(fs->Name(), ReadOnlyFileSystem::kClassName());
  ASSERT_EQ(fs->Inner(), FileSystem::Default().get());

  std::string opts_str = fs->ToString(config_options_);
  std::string mismatch;

  ASSERT_OK(FileSystem::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(fs->AreEquivalent(config_options_, copy.get(), &mismatch));

  ASSERT_OK(FileSystem::CreateFromString(
      config_options_,
      std::string("id=") + ReadOnlyFileSystem::kClassName() +
          "; target=" + TimedFileSystem::kClassName(),
      &fs));
  ASSERT_NE(fs, nullptr);
  opts_str = fs->ToString(config_options_);
  ASSERT_STREQ(fs->Name(), ReadOnlyFileSystem::kClassName());
  ASSERT_NE(fs->Inner(), nullptr);
  ASSERT_STREQ(fs->Inner()->Name(), TimedFileSystem::kClassName());
  ASSERT_EQ(fs->Inner()->Inner(), FileSystem::Default().get());
  ASSERT_OK(FileSystem::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(fs->AreEquivalent(config_options_, copy.get(), &mismatch));
}

TEST_F(CreateEnvTest, CreateTimedFileSystem) {
  std::shared_ptr<FileSystem> fs, copy;

  ASSERT_OK(FileSystem::CreateFromString(config_options_,
                                         TimedFileSystem::kClassName(), &fs));
  ASSERT_NE(fs, nullptr);
  ASSERT_STREQ(fs->Name(), TimedFileSystem::kClassName());
  ASSERT_EQ(fs->Inner(), FileSystem::Default().get());

  std::string opts_str = fs->ToString(config_options_);
  std::string mismatch;

  ASSERT_OK(FileSystem::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(fs->AreEquivalent(config_options_, copy.get(), &mismatch));

  ASSERT_OK(FileSystem::CreateFromString(
      config_options_,
      std::string("id=") + TimedFileSystem::kClassName() +
          "; target=" + ReadOnlyFileSystem::kClassName(),
      &fs));
  ASSERT_NE(fs, nullptr);
  opts_str = fs->ToString(config_options_);
  ASSERT_STREQ(fs->Name(), TimedFileSystem::kClassName());
  ASSERT_NE(fs->Inner(), nullptr);
  ASSERT_STREQ(fs->Inner()->Name(), ReadOnlyFileSystem::kClassName());
  ASSERT_EQ(fs->Inner()->Inner(), FileSystem::Default().get());
  ASSERT_OK(FileSystem::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(fs->AreEquivalent(config_options_, copy.get(), &mismatch));
}

TEST_F(CreateEnvTest, CreateCountedFileSystem) {
  std::shared_ptr<FileSystem> fs, copy;

  ASSERT_OK(FileSystem::CreateFromString(config_options_,
                                         CountedFileSystem::kClassName(), &fs));
  ASSERT_NE(fs, nullptr);
  ASSERT_STREQ(fs->Name(), CountedFileSystem::kClassName());
  ASSERT_EQ(fs->Inner(), FileSystem::Default().get());

  std::string opts_str = fs->ToString(config_options_);
  std::string mismatch;

  ASSERT_OK(FileSystem::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(fs->AreEquivalent(config_options_, copy.get(), &mismatch));

  ASSERT_OK(FileSystem::CreateFromString(
      config_options_,
      std::string("id=") + CountedFileSystem::kClassName() +
          "; target=" + ReadOnlyFileSystem::kClassName(),
      &fs));
  ASSERT_NE(fs, nullptr);
  opts_str = fs->ToString(config_options_);
  ASSERT_STREQ(fs->Name(), CountedFileSystem::kClassName());
  ASSERT_NE(fs->Inner(), nullptr);
  ASSERT_STREQ(fs->Inner()->Name(), ReadOnlyFileSystem::kClassName());
  ASSERT_EQ(fs->Inner()->Inner(), FileSystem::Default().get());
  ASSERT_OK(FileSystem::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(fs->AreEquivalent(config_options_, copy.get(), &mismatch));
}

#ifndef OS_WIN
TEST_F(CreateEnvTest, CreateChrootFileSystem) {
  std::shared_ptr<FileSystem> fs, copy;
  auto tmp_dir = test::TmpDir(Env::Default());
  // The Chroot FileSystem has a required "chroot_dir" option.
  ASSERT_NOK(FileSystem::CreateFromString(config_options_,
                                          ChrootFileSystem::kClassName(), &fs));

  // ChrootFileSystem fails with an invalid directory
  ASSERT_NOK(FileSystem::CreateFromString(
      config_options_,
      std::string("chroot_dir=/No/Such/Directory; id=") +
          ChrootFileSystem::kClassName(),
      &fs));
  std::string chroot_opts = std::string("chroot_dir=") + tmp_dir +
                            std::string("; id=") +
                            ChrootFileSystem::kClassName();

  // Create a valid ChrootFileSystem with an inner Default
  ASSERT_OK(FileSystem::CreateFromString(config_options_, chroot_opts, &fs));
  ASSERT_NE(fs, nullptr);
  ASSERT_STREQ(fs->Name(), ChrootFileSystem::kClassName());
  ASSERT_EQ(fs->Inner(), FileSystem::Default().get());
  std::string opts_str = fs->ToString(config_options_);
  std::string mismatch;
  ASSERT_OK(FileSystem::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(fs->AreEquivalent(config_options_, copy.get(), &mismatch));

  // Create a valid ChrootFileSystem with an inner TimedFileSystem
  ASSERT_OK(FileSystem::CreateFromString(
      config_options_,
      chroot_opts + "; target=" + TimedFileSystem::kClassName(), &fs));
  ASSERT_NE(fs, nullptr);
  ASSERT_STREQ(fs->Name(), ChrootFileSystem::kClassName());
  ASSERT_NE(fs->Inner(), nullptr);
  ASSERT_STREQ(fs->Inner()->Name(), TimedFileSystem::kClassName());
  ASSERT_EQ(fs->Inner()->Inner(), FileSystem::Default().get());
  opts_str = fs->ToString(config_options_);
  ASSERT_OK(FileSystem::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(fs->AreEquivalent(config_options_, copy.get(), &mismatch));

  // Create a TimedFileSystem with an inner ChrootFileSystem
  ASSERT_OK(FileSystem::CreateFromString(
      config_options_,
      "target={" + chroot_opts + "}; id=" + TimedFileSystem::kClassName(),
      &fs));
  ASSERT_NE(fs, nullptr);
  ASSERT_STREQ(fs->Name(), TimedFileSystem::kClassName());
  ASSERT_NE(fs->Inner(), nullptr);
  ASSERT_STREQ(fs->Inner()->Name(), ChrootFileSystem::kClassName());
  ASSERT_EQ(fs->Inner()->Inner(), FileSystem::Default().get());
  opts_str = fs->ToString(config_options_);
  ASSERT_OK(FileSystem::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(fs->AreEquivalent(config_options_, copy.get(), &mismatch));
}
#endif  // OS_WIN

TEST_F(CreateEnvTest, CreateEncryptedFileSystem) {
  std::shared_ptr<FileSystem> fs, copy;

  std::string base_opts =
      std::string("provider=1://test; id=") + EncryptedFileSystem::kClassName();
  // The EncryptedFileSystem requires a "provider" option.
  ASSERT_NOK(FileSystem::CreateFromString(
      config_options_, EncryptedFileSystem::kClassName(), &fs));

  ASSERT_OK(FileSystem::CreateFromString(config_options_, base_opts, &fs));

  ASSERT_NE(fs, nullptr);
  ASSERT_STREQ(fs->Name(), EncryptedFileSystem::kClassName());
  ASSERT_EQ(fs->Inner(), FileSystem::Default().get());
  std::string opts_str = fs->ToString(config_options_);
  std::string mismatch;
  ASSERT_OK(FileSystem::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(fs->AreEquivalent(config_options_, copy.get(), &mismatch));
  ASSERT_OK(FileSystem::CreateFromString(
      config_options_, base_opts + "; target=" + TimedFileSystem::kClassName(),
      &fs));
  ASSERT_NE(fs, nullptr);
  ASSERT_STREQ(fs->Name(), EncryptedFileSystem::kClassName());
  ASSERT_NE(fs->Inner(), nullptr);
  ASSERT_STREQ(fs->Inner()->Name(), TimedFileSystem::kClassName());
  ASSERT_EQ(fs->Inner()->Inner(), FileSystem::Default().get());
  opts_str = fs->ToString(config_options_);
  ASSERT_OK(FileSystem::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(fs->AreEquivalent(config_options_, copy.get(), &mismatch));
}

#endif  // ROCKSDB_LITE

namespace {

constexpr size_t kThreads = 8;
constexpr size_t kIdsPerThread = 1000;

// This is a mini-stress test to check for duplicates in functions like
// GenerateUniqueId()
template <typename IdType, class Hash = std::hash<IdType>>
struct NoDuplicateMiniStressTest {
  std::unordered_set<IdType, Hash> ids;
  std::mutex mutex;
  Env* env;

  NoDuplicateMiniStressTest() { env = Env::Default(); }

  virtual ~NoDuplicateMiniStressTest() {}

  void Run() {
    std::array<std::thread, kThreads> threads;
    for (size_t i = 0; i < kThreads; ++i) {
      threads[i] = std::thread([&]() { ThreadFn(); });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    // All must be unique
    ASSERT_EQ(ids.size(), kThreads * kIdsPerThread);
  }

  void ThreadFn() {
    std::array<IdType, kIdsPerThread> my_ids;
    // Generate in parallel threads as fast as possible
    for (size_t i = 0; i < kIdsPerThread; ++i) {
      my_ids[i] = Generate();
    }
    // Now collate
    std::lock_guard<std::mutex> lock(mutex);
    for (auto& id : my_ids) {
      ids.insert(id);
    }
  }

  virtual IdType Generate() = 0;
};

void VerifyRfcUuids(const std::unordered_set<std::string>& uuids) {
  if (uuids.empty()) {
    return;
  }
}

using uint64_pair_t = std::pair<uint64_t, uint64_t>;
struct HashUint64Pair {
  std::size_t operator()(
      std::pair<uint64_t, uint64_t> const& u) const noexcept {
    // Assume suitable distribution already
    return static_cast<size_t>(u.first ^ u.second);
  }
};

}  // namespace

TEST_F(EnvTest, GenerateUniqueId) {
  struct MyStressTest : public NoDuplicateMiniStressTest<std::string> {
    std::string Generate() override { return env->GenerateUniqueId(); }
  };

  MyStressTest t;
  t.Run();

  // Basically verify RFC-4122 format
  for (auto& uuid : t.ids) {
    ASSERT_EQ(36U, uuid.size());
    ASSERT_EQ('-', uuid[8]);
    ASSERT_EQ('-', uuid[13]);
    ASSERT_EQ('-', uuid[18]);
    ASSERT_EQ('-', uuid[23]);
  }
}

TEST_F(EnvTest, GenerateDbSessionId) {
  struct MyStressTest : public NoDuplicateMiniStressTest<std::string> {
    std::string Generate() override { return DBImpl::GenerateDbSessionId(env); }
  };

  MyStressTest t;
  t.Run();

  // Basically verify session ID
  for (auto& id : t.ids) {
    ASSERT_EQ(20U, id.size());
  }
}

constexpr bool kRequirePortGenerateRfcUuid =
#if defined(OS_LINUX) || defined(OS_ANDROID) || defined(OS_WIN)
    true;
#else
    false;
#endif

TEST_F(EnvTest, PortGenerateRfcUuid) {
  if (!kRequirePortGenerateRfcUuid) {
    ROCKSDB_GTEST_SKIP("Not supported/expected on this platform");
    return;
  }
  struct MyStressTest : public NoDuplicateMiniStressTest<std::string> {
    std::string Generate() override {
      std::string u;
      assert(port::GenerateRfcUuid(&u));
      return u;
    }
  };

  MyStressTest t;
  t.Run();

  // Extra verification on versions and variants
  VerifyRfcUuids(t.ids);
}

// Test the atomic, linear generation of GenerateRawUuid
TEST_F(EnvTest, GenerateRawUniqueId) {
  struct MyStressTest
      : public NoDuplicateMiniStressTest<uint64_pair_t, HashUint64Pair> {
    uint64_pair_t Generate() override {
      uint64_pair_t p;
      GenerateRawUniqueId(&p.first, &p.second);
      return p;
    }
  };

  MyStressTest t;
  t.Run();
}

// Test that each entropy source ("track") is at least adequate
TEST_F(EnvTest, GenerateRawUniqueIdTrackPortUuidOnly) {
  if (!kRequirePortGenerateRfcUuid) {
    ROCKSDB_GTEST_SKIP("Not supported/expected on this platform");
    return;
  }

  struct MyStressTest
      : public NoDuplicateMiniStressTest<uint64_pair_t, HashUint64Pair> {
    uint64_pair_t Generate() override {
      uint64_pair_t p;
      TEST_GenerateRawUniqueId(&p.first, &p.second, false, true, true);
      return p;
    }
  };

  MyStressTest t;
  t.Run();
}

TEST_F(EnvTest, GenerateRawUniqueIdTrackEnvDetailsOnly) {
  struct MyStressTest
      : public NoDuplicateMiniStressTest<uint64_pair_t, HashUint64Pair> {
    uint64_pair_t Generate() override {
      uint64_pair_t p;
      TEST_GenerateRawUniqueId(&p.first, &p.second, true, false, true);
      return p;
    }
  };

  MyStressTest t;
  t.Run();
}

TEST_F(EnvTest, GenerateRawUniqueIdTrackRandomDeviceOnly) {
  struct MyStressTest
      : public NoDuplicateMiniStressTest<uint64_pair_t, HashUint64Pair> {
    uint64_pair_t Generate() override {
      uint64_pair_t p;
      TEST_GenerateRawUniqueId(&p.first, &p.second, true, true, false);
      return p;
    }
  };

  MyStressTest t;
  t.Run();
}

TEST_F(EnvTest, SemiStructuredUniqueIdGenTest) {
  // Must be thread safe and usable as a static
  static SemiStructuredUniqueIdGen gen;

  struct MyStressTest
      : public NoDuplicateMiniStressTest<uint64_pair_t, HashUint64Pair> {
    uint64_pair_t Generate() override {
      uint64_pair_t p;
      gen.GenerateNext(&p.first, &p.second);
      return p;
    }
  };

  MyStressTest t;
  t.Run();
}

TEST_F(EnvTest, FailureToCreateLockFile) {
  auto env = Env::Default();
  auto fs = env->GetFileSystem();
  std::string dir = test::PerThreadDBPath(env, "lockdir");
  std::string file = dir + "/lockfile";

  // Ensure directory doesn't exist
  ASSERT_OK(DestroyDir(env, dir));

  // Make sure that we can acquire a file lock after the first attempt fails
  FileLock* lock = nullptr;
  ASSERT_NOK(fs->LockFile(file, IOOptions(), &lock, /*dbg*/ nullptr));
  ASSERT_FALSE(lock);

  ASSERT_OK(fs->CreateDir(dir, IOOptions(), /*dbg*/ nullptr));
  ASSERT_OK(fs->LockFile(file, IOOptions(), &lock, /*dbg*/ nullptr));
  ASSERT_OK(fs->UnlockFile(lock, IOOptions(), /*dbg*/ nullptr));

  // Clean up
  ASSERT_OK(DestroyDir(env, dir));
}

TEST_F(CreateEnvTest, CreateDefaultEnv) {
  ConfigOptions options;
  options.ignore_unsupported_options = false;

  std::shared_ptr<Env> guard;
  Env* env = nullptr;
  ASSERT_OK(Env::CreateFromString(options, "", &env));
  ASSERT_EQ(env, Env::Default());

  env = nullptr;
  ASSERT_OK(Env::CreateFromString(options, Env::kDefaultName(), &env));
  ASSERT_EQ(env, Env::Default());

  env = nullptr;
  ASSERT_OK(Env::CreateFromString(options, "", &env, &guard));
  ASSERT_EQ(env, Env::Default());
  ASSERT_EQ(guard, nullptr);

  env = nullptr;
  ASSERT_OK(Env::CreateFromString(options, Env::kDefaultName(), &env, &guard));
  ASSERT_EQ(env, Env::Default());
  ASSERT_EQ(guard, nullptr);

#ifndef ROCKSDB_LITE
  std::string opt_str = env->ToString(options);
  ASSERT_OK(Env::CreateFromString(options, opt_str, &env));
  ASSERT_EQ(env, Env::Default());
  ASSERT_OK(Env::CreateFromString(options, opt_str, &env, &guard));
  ASSERT_EQ(env, Env::Default());
  ASSERT_EQ(guard, nullptr);
#endif  // ROCKSDB_LITE
}

#ifndef ROCKSDB_LITE
namespace {
class WrappedEnv : public EnvWrapper {
 public:
  explicit WrappedEnv(Env* t) : EnvWrapper(t) {}
  explicit WrappedEnv(const std::shared_ptr<Env>& t) : EnvWrapper(t) {}
  static const char* kClassName() { return "WrappedEnv"; }
  const char* Name() const override { return kClassName(); }
  static void Register(ObjectLibrary& lib, const std::string& /*arg*/) {
    lib.AddFactory<Env>(
        WrappedEnv::kClassName(),
        [](const std::string& /*uri*/, std::unique_ptr<Env>* guard,
           std::string* /* errmsg */) {
          guard->reset(new WrappedEnv(nullptr));
          return guard->get();
        });
  }
};
}  // namespace
TEST_F(CreateEnvTest, CreateMockEnv) {
  ConfigOptions options;
  options.ignore_unsupported_options = false;
  WrappedEnv::Register(*(options.registry->AddLibrary("test")), "");
  std::shared_ptr<Env> guard, copy;
  std::string opt_str;

  Env* env = nullptr;
  ASSERT_NOK(Env::CreateFromString(options, MockEnv::kClassName(), &env));
  ASSERT_OK(
      Env::CreateFromString(options, MockEnv::kClassName(), &env, &guard));
  ASSERT_NE(env, nullptr);
  ASSERT_NE(env, Env::Default());
  opt_str = env->ToString(options);
  ASSERT_OK(Env::CreateFromString(options, opt_str, &env, &copy));
  ASSERT_NE(copy, guard);
  std::string mismatch;
  ASSERT_TRUE(guard->AreEquivalent(options, copy.get(), &mismatch));
  guard.reset(MockEnv::Create(Env::Default(), SystemClock::Default()));
  opt_str = guard->ToString(options);
  ASSERT_OK(Env::CreateFromString(options, opt_str, &env, &copy));
  std::unique_ptr<Env> wrapped_env(new WrappedEnv(Env::Default()));
  guard.reset(MockEnv::Create(wrapped_env.get(), SystemClock::Default()));
  opt_str = guard->ToString(options);
  ASSERT_OK(Env::CreateFromString(options, opt_str, &env, &copy));
  opt_str = copy->ToString(options);
}

TEST_F(CreateEnvTest, CreateWrappedEnv) {
  ConfigOptions options;
  options.ignore_unsupported_options = false;
  WrappedEnv::Register(*(options.registry->AddLibrary("test")), "");
  Env* env = nullptr;
  std::shared_ptr<Env> guard, copy;
  std::string opt_str;
  std::string mismatch;

  ASSERT_NOK(Env::CreateFromString(options, WrappedEnv::kClassName(), &env));
  ASSERT_OK(
      Env::CreateFromString(options, WrappedEnv::kClassName(), &env, &guard));
  ASSERT_NE(env, nullptr);
  ASSERT_NE(env, Env::Default());
  ASSERT_FALSE(guard->AreEquivalent(options, Env::Default(), &mismatch));

  opt_str = env->ToString(options);
  ASSERT_OK(Env::CreateFromString(options, opt_str, &env, &copy));
  ASSERT_NE(copy, guard);
  ASSERT_TRUE(guard->AreEquivalent(options, copy.get(), &mismatch));

  guard.reset(new WrappedEnv(std::make_shared<WrappedEnv>(Env::Default())));
  ASSERT_NE(guard.get(), env);
  opt_str = guard->ToString(options);
  ASSERT_OK(Env::CreateFromString(options, opt_str, &env, &copy));
  ASSERT_NE(copy, guard);
  ASSERT_TRUE(guard->AreEquivalent(options, copy.get(), &mismatch));

  guard.reset(new WrappedEnv(std::make_shared<WrappedEnv>(
      std::make_shared<WrappedEnv>(Env::Default()))));
  ASSERT_NE(guard.get(), env);
  opt_str = guard->ToString(options);
  ASSERT_OK(Env::CreateFromString(options, opt_str, &env, &copy));
  ASSERT_NE(copy, guard);
  ASSERT_TRUE(guard->AreEquivalent(options, copy.get(), &mismatch));
}

TEST_F(CreateEnvTest, CreateCompositeEnv) {
  ConfigOptions options;
  options.ignore_unsupported_options = false;
  std::shared_ptr<Env> guard, copy;
  Env* env = nullptr;
  std::string mismatch, opt_str;

  WrappedEnv::Register(*(options.registry->AddLibrary("test")), "");
  std::unique_ptr<Env> base(NewCompositeEnv(FileSystem::Default()));
  std::unique_ptr<Env> wrapped(new WrappedEnv(Env::Default()));
  std::shared_ptr<FileSystem> timed_fs =
      std::make_shared<TimedFileSystem>(FileSystem::Default());
  std::shared_ptr<SystemClock> clock =
      std::make_shared<EmulatedSystemClock>(SystemClock::Default());

  opt_str = base->ToString(options);
  ASSERT_NOK(Env::CreateFromString(options, opt_str, &env));
  ASSERT_OK(Env::CreateFromString(options, opt_str, &env, &guard));
  ASSERT_NE(env, nullptr);
  ASSERT_NE(env, Env::Default());
  ASSERT_EQ(env->GetFileSystem(), FileSystem::Default());
  ASSERT_EQ(env->GetSystemClock(), SystemClock::Default());

  base = NewCompositeEnv(timed_fs);
  opt_str = base->ToString(options);
  ASSERT_NOK(Env::CreateFromString(options, opt_str, &env));
  ASSERT_OK(Env::CreateFromString(options, opt_str, &env, &guard));
  ASSERT_NE(env, nullptr);
  ASSERT_NE(env, Env::Default());
  ASSERT_NE(env->GetFileSystem(), FileSystem::Default());
  ASSERT_EQ(env->GetSystemClock(), SystemClock::Default());

  env = nullptr;
  guard.reset(new CompositeEnvWrapper(wrapped.get(), timed_fs));
  opt_str = guard->ToString(options);
  ASSERT_OK(Env::CreateFromString(options, opt_str, &env, &copy));
  ASSERT_NE(env, nullptr);
  ASSERT_NE(env, Env::Default());
  ASSERT_TRUE(guard->AreEquivalent(options, copy.get(), &mismatch));

  env = nullptr;
  guard.reset(new CompositeEnvWrapper(wrapped.get(), clock));
  opt_str = guard->ToString(options);
  ASSERT_OK(Env::CreateFromString(options, opt_str, &env, &copy));
  ASSERT_NE(env, nullptr);
  ASSERT_NE(env, Env::Default());
  ASSERT_TRUE(guard->AreEquivalent(options, copy.get(), &mismatch));

  env = nullptr;
  guard.reset(new CompositeEnvWrapper(wrapped.get(), timed_fs, clock));
  opt_str = guard->ToString(options);
  ASSERT_OK(Env::CreateFromString(options, opt_str, &env, &copy));
  ASSERT_NE(env, nullptr);
  ASSERT_NE(env, Env::Default());
  ASSERT_TRUE(guard->AreEquivalent(options, copy.get(), &mismatch));

  guard.reset(new CompositeEnvWrapper(nullptr, timed_fs, clock));
  ColumnFamilyOptions cf_opts;
  DBOptions db_opts;
  db_opts.env = guard.get();
  auto comp = db_opts.env->CheckedCast<CompositeEnvWrapper>();
  ASSERT_NE(comp, nullptr);
  ASSERT_EQ(comp->Inner(), nullptr);
  ASSERT_NOK(ValidateOptions(db_opts, cf_opts));
  ASSERT_OK(db_opts.env->PrepareOptions(options));
  ASSERT_NE(comp->Inner(), nullptr);
  ASSERT_OK(ValidateOptions(db_opts, cf_opts));
}
#endif  // ROCKSDB_LITE

// Forward declaration
class ReadAsyncFS;

struct MockIOHandle {
  std::function<void(const FSReadRequest&, void*)> cb;
  void* cb_arg;
  bool create_io_error;
};

// ReadAsyncFS and ReadAsyncRandomAccessFile mocks the FS doing asynchronous
// reads by creating threads that submit read requests and then calling Poll API
// to obtain those results.
class ReadAsyncRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
 public:
  ReadAsyncRandomAccessFile(ReadAsyncFS& fs,
                            std::unique_ptr<FSRandomAccessFile>& file)
      : FSRandomAccessFileOwnerWrapper(std::move(file)), fs_(fs) {}

  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(const FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override;

 private:
  ReadAsyncFS& fs_;
  std::unique_ptr<FSRandomAccessFile> file_;
  int counter = 0;
};

class ReadAsyncFS : public FileSystemWrapper {
 public:
  explicit ReadAsyncFS(const std::shared_ptr<FileSystem>& wrapped)
      : FileSystemWrapper(wrapped) {}

  static const char* kClassName() { return "ReadAsyncFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s = target()->NewRandomAccessFile(fname, opts, &file, dbg);
    EXPECT_OK(s);
    result->reset(new ReadAsyncRandomAccessFile(*this, file));
    return s;
  }

  IOStatus Poll(std::vector<void*>& io_handles,
                size_t /*min_completions*/) override {
    // Wait for the threads completion.
    for (auto& t : workers) {
      t.join();
    }

    for (size_t i = 0; i < io_handles.size(); i++) {
      MockIOHandle* handle = static_cast<MockIOHandle*>(io_handles[i]);
      if (handle->create_io_error) {
        FSReadRequest req;
        req.status = IOStatus::IOError();
        handle->cb(req, handle->cb_arg);
      }
    }
    return IOStatus::OK();
  }

  std::vector<std::thread> workers;
};

IOStatus ReadAsyncRandomAccessFile::ReadAsync(
    FSReadRequest& req, const IOOptions& opts,
    std::function<void(const FSReadRequest&, void*)> cb, void* cb_arg,
    void** io_handle, IOHandleDeleter* del_fn, IODebugContext* dbg) {
  IOHandleDeleter deletefn = [](void* args) -> void {
    delete (static_cast<MockIOHandle*>(args));
    args = nullptr;
  };
  *del_fn = deletefn;

  // Allocate and populate io_handle.
  MockIOHandle* mock_handle = new MockIOHandle();
  bool create_io_error = false;
  if (counter % 2) {
    create_io_error = true;
  }
  mock_handle->create_io_error = create_io_error;
  mock_handle->cb = cb;
  mock_handle->cb_arg = cb_arg;
  *io_handle = static_cast<void*>(mock_handle);
  counter++;

  // Submit read request asynchronously.
  std::function<void(FSReadRequest)> submit_request =
      [&opts, cb, cb_arg, dbg, create_io_error, this](FSReadRequest _req) {
        if (!create_io_error) {
          _req.status = target()->Read(_req.offset, _req.len, opts,
                                       &(_req.result), _req.scratch, dbg);
          cb(_req, cb_arg);
        }
      };

  fs_.workers.emplace_back(submit_request, req);
  return IOStatus::OK();
}

class TestAsyncRead : public testing::Test {
 public:
  TestAsyncRead() { env_ = Env::Default(); }
  Env* env_;
};

// Tests the default implementation of ReadAsync API.
TEST_F(TestAsyncRead, ReadAsync) {
  EnvOptions soptions;
  std::shared_ptr<ReadAsyncFS> fs =
      std::make_shared<ReadAsyncFS>(env_->GetFileSystem());

  std::string fname = test::PerThreadDBPath(env_, "testfile");

  const size_t kSectorSize = 4096;
  const size_t kNumSectors = 8;

  // 1. create & write to a file.
  {
    std::unique_ptr<FSWritableFile> wfile;
    ASSERT_OK(
        fs->NewWritableFile(fname, FileOptions(), &wfile, nullptr /*dbg*/));

    for (size_t i = 0; i < kNumSectors; ++i) {
      auto data = NewAligned(kSectorSize * 8, static_cast<char>(i + 1));
      Slice slice(data.get(), kSectorSize);
      ASSERT_OK(wfile->Append(slice, IOOptions(), nullptr));
    }
    ASSERT_OK(wfile->Close(IOOptions(), nullptr));
  }
  // 2. Read file
  {
    std::unique_ptr<FSRandomAccessFile> file;
    ASSERT_OK(fs->NewRandomAccessFile(fname, FileOptions(), &file, nullptr));

    IOOptions opts;
    std::vector<void*> io_handles(kNumSectors);
    std::vector<FSReadRequest> reqs(kNumSectors);
    std::vector<std::unique_ptr<char, Deleter>> data;
    std::vector<size_t> vals;
    IOHandleDeleter del_fn;
    uint64_t offset = 0;

    // Initialize read requests
    for (size_t i = 0; i < kNumSectors; i++) {
      reqs[i].offset = offset;
      reqs[i].len = kSectorSize;
      data.emplace_back(NewAligned(kSectorSize, 0));
      reqs[i].scratch = data.back().get();
      vals.push_back(i);
      offset += kSectorSize;
    }

    // callback function passed to async read.
    std::function<void(const FSReadRequest&, void*)> callback =
        [&](const FSReadRequest& req, void* cb_arg) {
          assert(cb_arg != nullptr);
          size_t i = *(reinterpret_cast<size_t*>(cb_arg));
          reqs[i].offset = req.offset;
          reqs[i].result = req.result;
          reqs[i].status = req.status;
        };

    // Submit asynchronous read requests.
    for (size_t i = 0; i < kNumSectors; i++) {
      void* cb_arg = static_cast<void*>(&(vals[i]));
      ASSERT_OK(file->ReadAsync(reqs[i], opts, callback, cb_arg,
                                &(io_handles[i]), &del_fn, nullptr));
    }

    // Poll for the submitted requests.
    fs->Poll(io_handles, kNumSectors);

    // Check the status of read requests.
    for (size_t i = 0; i < kNumSectors; i++) {
      if (i % 2) {
        ASSERT_EQ(reqs[i].status, IOStatus::IOError());
      } else {
        auto buf = NewAligned(kSectorSize * 8, static_cast<char>(i + 1));
        Slice expected_data(buf.get(), kSectorSize);

        ASSERT_EQ(reqs[i].offset, i * kSectorSize);
        ASSERT_OK(reqs[i].status);
        ASSERT_EQ(expected_data.ToString(), reqs[i].result.ToString());
      }
    }

    // Delete io_handles.
    for (size_t i = 0; i < io_handles.size(); i++) {
      del_fn(io_handles[i]);
    }
  }
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
