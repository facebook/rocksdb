//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.


#include <iostream>
#include <unordered_set>

#include "rocksdb/env.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "util/testharness.h"

namespace rocksdb {

static const int kDelayMicros = 100000;

class EnvPosixTest {
 private:
  port::Mutex mu_;
  std::string events_;

 public:
  Env* env_;
  EnvPosixTest() : env_(Env::Default()) { }
};

static void SetBool(void* ptr) {
  reinterpret_cast<port::AtomicPointer*>(ptr)->NoBarrier_Store(ptr);
}

TEST(EnvPosixTest, RunImmediately) {
  port::AtomicPointer called (nullptr);
  env_->Schedule(&SetBool, &called);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_TRUE(called.NoBarrier_Load() != nullptr);
}

TEST(EnvPosixTest, RunMany) {
  port::AtomicPointer last_id (nullptr);

  struct CB {
    port::AtomicPointer* last_id_ptr;   // Pointer to shared slot
    uintptr_t id;             // Order# for the execution of this callback

    CB(port::AtomicPointer* p, int i) : last_id_ptr(p), id(i) { }

    static void Run(void* v) {
      CB* cb = reinterpret_cast<CB*>(v);
      void* cur = cb->last_id_ptr->NoBarrier_Load();
      ASSERT_EQ(cb->id-1, reinterpret_cast<uintptr_t>(cur));
      cb->last_id_ptr->Release_Store(reinterpret_cast<void*>(cb->id));
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

  Env::Default()->SleepForMicroseconds(kDelayMicros);
  void* cur = last_id.Acquire_Load();
  ASSERT_EQ(4U, reinterpret_cast<uintptr_t>(cur));
}

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

TEST(EnvPosixTest, StartThread) {
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
}

TEST(EnvPosixTest, TwoPools) {

  class CB {
   public:
    CB(const std::string& pool_name, int pool_size)
        : mu_(),
          num_running_(0),
          num_finished_(0),
          pool_size_(pool_size),
          pool_name_(pool_name) { }

    static void Run(void* v) {
      CB* cb = reinterpret_cast<CB*>(v);
      cb->Run();
    }

    void Run() {
      {
        MutexLock l(&mu_);
        num_running_++;
        std::cout << "Pool " << pool_name_ << ": "
                  << num_running_ << " running threads.\n";
        // make sure we don't have more than pool_size_ jobs running.
        ASSERT_LE(num_running_, pool_size_);
      }

      // sleep for 1 sec
      Env::Default()->SleepForMicroseconds(1000000);

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

   private:
    port::Mutex mu_;
    int num_running_;
    int num_finished_;
    int pool_size_;
    std::string pool_name_;
  };

  const int kLowPoolSize = 2;
  const int kHighPoolSize = 4;
  const int kJobs = 8;

  CB low_pool_job("low", kLowPoolSize);
  CB high_pool_job("high", kHighPoolSize);

  env_->SetBackgroundThreads(kLowPoolSize);
  env_->SetBackgroundThreads(kHighPoolSize, Env::Priority::HIGH);

  // schedule same number of jobs in each pool
  for (int i = 0; i < kJobs; i++) {
    env_->Schedule(&CB::Run, &low_pool_job);
    env_->Schedule(&CB::Run, &high_pool_job, Env::Priority::HIGH);
  }

  // wait for all jobs to finish
  while (low_pool_job.NumFinished() < kJobs ||
         high_pool_job.NumFinished() < kJobs) {
    env_->SleepForMicroseconds(kDelayMicros);
  }
}

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

TEST(EnvPosixTest, RandomAccessUniqueID) {
  // Create file.
  const EnvOptions soptions;
  std::string fname = test::TmpDir() + "/" + "testfile";
  unique_ptr<WritableFile> wfile;
  ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));

  unique_ptr<RandomAccessFile> file;

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
  env_->DeleteFile(fname);
}

// Returns true if any of the strings in ss are the prefix of another string.
bool HasPrefix(const std::unordered_set<std::string>& ss) {
  for (const std::string& s: ss) {
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

TEST(EnvPosixTest, RandomAccessUniqueIDConcurrent) {
  // Check whether a bunch of concurrently existing files have unique IDs.
  const EnvOptions soptions;

  // Create the files
  std::vector<std::string> fnames;
  for (int i = 0; i < 1000; ++i) {
    fnames.push_back(test::TmpDir() + "/" + "testfile" + std::to_string(i));

    // Create file.
    unique_ptr<WritableFile> wfile;
    ASSERT_OK(env_->NewWritableFile(fnames[i], &wfile, soptions));
  }

  // Collect and check whether the IDs are unique.
  std::unordered_set<std::string> ids;
  for (const std::string fname: fnames) {
    unique_ptr<RandomAccessFile> file;
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
  for (const std::string fname: fnames) {
    ASSERT_OK(env_->DeleteFile(fname));
  }

  ASSERT_TRUE(!HasPrefix(ids));
}

TEST(EnvPosixTest, RandomAccessUniqueIDDeletes) {
  const EnvOptions soptions;
  std::string fname = test::TmpDir() + "/" + "testfile";

  // Check that after file is deleted we don't get same ID again in a new file.
  std::unordered_set<std::string> ids;
  for (int i = 0; i < 1000; ++i) {
    // Create file.
    {
      unique_ptr<WritableFile> wfile;
      ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));
    }

    // Get Unique ID
    std::string unique_id;
    {
      unique_ptr<RandomAccessFile> file;
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

TEST(EnvPosixTest, InvalidateCache) {
  const EnvOptions soptions;
  std::string fname = test::TmpDir() + "/" + "testfile";

  // Create file.
  {
    unique_ptr<WritableFile> wfile;
    ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));
    ASSERT_OK(wfile.get()->Append(Slice("Hello world")));
    ASSERT_OK(wfile.get()->InvalidateCache(0, 0));
    ASSERT_OK(wfile.get()->Close());
  }

  // Random Read
  {
    unique_ptr<RandomAccessFile> file;
    char scratch[100];
    Slice result;
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
    ASSERT_OK(file.get()->Read(0, 11, &result, scratch));
    ASSERT_EQ(memcmp(scratch, "Hello world", 11), 0);
    ASSERT_OK(file.get()->InvalidateCache(0, 11));
    ASSERT_OK(file.get()->InvalidateCache(0, 0));
  }

  // Sequential Read
  {
    unique_ptr<SequentialFile> file;
    char scratch[100];
    Slice result;
    ASSERT_OK(env_->NewSequentialFile(fname, &file, soptions));
    ASSERT_OK(file.get()->Read(11, &result, scratch));
    ASSERT_EQ(memcmp(scratch, "Hello world", 11), 0);
    ASSERT_OK(file.get()->InvalidateCache(0, 11));
    ASSERT_OK(file.get()->InvalidateCache(0, 0));
  }
  // Delete the file
  ASSERT_OK(env_->DeleteFile(fname));
}

TEST(EnvPosixTest, PosixRandomRWFileTest) {
  EnvOptions soptions;
  soptions.use_mmap_writes = soptions.use_mmap_reads = false;
  std::string fname = test::TmpDir() + "/" + "testfile";

  unique_ptr<RandomRWFile> file;
  ASSERT_OK(env_->NewRandomRWFile(fname, &file, soptions));
  // If you run the unit test on tmpfs, then tmpfs might not
  // support fallocate. It is still better to trigger that
  // code-path instead of eliminating it completely.
  file.get()->Allocate(0, 10*1024*1024);
  ASSERT_OK(file.get()->Write(100, Slice("Hello world")));
  ASSERT_OK(file.get()->Write(105, Slice("Hello world")));
  ASSERT_OK(file.get()->Sync());
  ASSERT_OK(file.get()->Fsync());
  char scratch[100];
  Slice result;
  ASSERT_OK(file.get()->Read(100, 16, &result, scratch));
  ASSERT_EQ(result.compare("HelloHello world"), 0);
  ASSERT_OK(file.get()->Close());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}
