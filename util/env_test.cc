// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"

#include <unordered_set>
#include "port/port.h"
#include "util/coding.h"
#include "util/testharness.h"

namespace leveldb {

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
  port::AtomicPointer called (NULL);
  env_->Schedule(&SetBool, &called);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_TRUE(called.NoBarrier_Load() != NULL);
}

TEST(EnvPosixTest, RunMany) {
  port::AtomicPointer last_id (NULL);

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
  std::string fname = test::TmpDir() + "/" + "testfile";
  unique_ptr<WritableFile> wfile;
  ASSERT_OK(env_->NewWritableFile(fname, &wfile));

  unique_ptr<RandomAccessFile> file;

  // Get Unique ID
  ASSERT_OK(env_->NewRandomAccessFile(fname, &file));
  size_t id_size = file->GetUniqueId(temp_id, MAX_ID_SIZE);
  ASSERT_TRUE(id_size > 0);
  std::string unique_id1(temp_id, id_size);
  ASSERT_TRUE(IsUniqueIDValid(unique_id1));

  // Get Unique ID again
  ASSERT_OK(env_->NewRandomAccessFile(fname, &file));
  id_size = file->GetUniqueId(temp_id, MAX_ID_SIZE);
  ASSERT_TRUE(id_size > 0);
  std::string unique_id2(temp_id, id_size);
  ASSERT_TRUE(IsUniqueIDValid(unique_id2));

  // Get Unique ID again after waiting some time.
  env_->SleepForMicroseconds(1000000);
  ASSERT_OK(env_->NewRandomAccessFile(fname, &file));
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

  // Create the files
  std::vector<std::string> fnames;
  for (int i = 0; i < 1000; ++i) {
    fnames.push_back(test::TmpDir() + "/" + "testfile" + std::to_string(i));

    // Create file.
    unique_ptr<WritableFile> wfile;
    ASSERT_OK(env_->NewWritableFile(fnames[i], &wfile));
  }

  // Collect and check whether the IDs are unique.
  std::unordered_set<std::string> ids;
  for (const std::string fname: fnames) {
    unique_ptr<RandomAccessFile> file;
    std::string unique_id;
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file));
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
  std::string fname = test::TmpDir() + "/" + "testfile";

  // Check that after file is deleted we don't get same ID again in a new file.
  std::unordered_set<std::string> ids;
  for (int i = 0; i < 1000; ++i) {
    // Create file.
    {
      unique_ptr<WritableFile> wfile;
      ASSERT_OK(env_->NewWritableFile(fname, &wfile));
    }

    // Get Unique ID
    std::string unique_id;
    {
      unique_ptr<RandomAccessFile> file;
      ASSERT_OK(env_->NewRandomAccessFile(fname, &file));
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

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
