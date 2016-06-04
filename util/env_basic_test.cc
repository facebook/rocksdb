// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/utilities/env_registry.h"
#include "util/mock_env.h"
#include "util/testharness.h"

namespace rocksdb {

class EnvBasicTestWithParam : public testing::Test,
                              public ::testing::WithParamInterface<Env*> {
 public:
  Env* env_;
  const EnvOptions soptions_;
  std::string test_dir_;

  EnvBasicTestWithParam() {
    env_ = GetParam();
    test_dir_ = test::TmpDir(env_) + "/env_basic_test";
  }

  void SetUp() {
    env_->CreateDirIfMissing(test_dir_);
    std::vector<std::string> files;
    env_->GetChildren(test_dir_, &files);
    for (const auto& file : files) {
      env_->DeleteFile(test_dir_ + "/" + file);
    }
  }
};

static std::unique_ptr<Env> mock_env(new MockEnv(Env::Default()));
INSTANTIATE_TEST_CASE_P(MockEnv, EnvBasicTestWithParam,
                        ::testing::Values(mock_env.get()));
#ifndef ROCKSDB_LITE
static std::unique_ptr<Env> mem_env(NewMemEnv(Env::Default()));
INSTANTIATE_TEST_CASE_P(MemEnv, EnvBasicTestWithParam,
                        ::testing::Values(mem_env.get()));

namespace {

// Returns a vector of 0 or 1 Env*, depending whether an Env is registered for
// TEST_ENV_URI.
//
// The purpose of returning an empty vector (instead of nullptr) is that gtest
// ValuesIn() will skip running tests when given an empty collection.
std::vector<Env*> GetCustomEnvs() {
  static Env* custom_env;
  static std::unique_ptr<Env> custom_env_guard;
  static bool init = false;
  if (!init) {
    init = true;
    const char* uri = getenv("TEST_ENV_URI");
    if (uri != nullptr) {
      custom_env = NewEnvFromUri(uri, &custom_env_guard);
    }
  }

  std::vector<Env*> res;
  if (custom_env != nullptr) {
    res.emplace_back(custom_env);
  }
  return res;
}

}  // anonymous namespace

INSTANTIATE_TEST_CASE_P(CustomEnv, EnvBasicTestWithParam,
                        ::testing::ValuesIn(GetCustomEnvs()));
#endif  // ROCKSDB_LITE

TEST_P(EnvBasicTestWithParam, Basics) {
  uint64_t file_size;
  unique_ptr<WritableFile> writable_file;
  std::vector<std::string> children;

  // Check that the directory is empty.
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/non_existent"));
  ASSERT_TRUE(!env_->GetFileSize(test_dir_ + "/non_existent", &file_size).ok());
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_EQ(0U, children.size());

  // Create a file.
  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/f", &writable_file, soptions_));
  writable_file.reset();

  // Check that the file exists.
  ASSERT_OK(env_->FileExists(test_dir_ + "/f"));
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/f", &file_size));
  ASSERT_EQ(0U, file_size);
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_EQ(1U, children.size());
  ASSERT_EQ("f", children[0]);

  // Write to the file.
  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("abc"));
  writable_file.reset();

  // Check for expected size.
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/f", &file_size));
  ASSERT_EQ(3U, file_size);

  // Check that renaming works.
  ASSERT_TRUE(!env_->RenameFile(test_dir_ + "/non_existent", "/g").ok());
  ASSERT_OK(env_->RenameFile(test_dir_ + "/f", test_dir_ + "/g"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/f"));
  ASSERT_OK(env_->FileExists(test_dir_ + "/g"));
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/g", &file_size));
  ASSERT_EQ(3U, file_size);

  // Check that opening non-existent file fails.
  unique_ptr<SequentialFile> seq_file;
  unique_ptr<RandomAccessFile> rand_file;
  ASSERT_TRUE(!env_->NewSequentialFile(test_dir_ + "/non_existent", &seq_file,
                                       soptions_)
                   .ok());
  ASSERT_TRUE(!seq_file);
  ASSERT_TRUE(!env_->NewRandomAccessFile(test_dir_ + "/non_existent",
                                         &rand_file, soptions_)
                   .ok());
  ASSERT_TRUE(!rand_file);

  // Check that deleting works.
  ASSERT_TRUE(!env_->DeleteFile(test_dir_ + "/non_existent").ok());
  ASSERT_OK(env_->DeleteFile(test_dir_ + "/g"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/g"));
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_EQ(0U, children.size());
  ASSERT_OK(env_->DeleteDir(test_dir_));
}

TEST_P(EnvBasicTestWithParam, ReadWrite) {
  unique_ptr<WritableFile> writable_file;
  unique_ptr<SequentialFile> seq_file;
  unique_ptr<RandomAccessFile> rand_file;
  Slice result;
  char scratch[100];

  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("hello "));
  ASSERT_OK(writable_file->Append("world"));
  writable_file.reset();

  // Read sequentially.
  ASSERT_OK(env_->NewSequentialFile(test_dir_ + "/f", &seq_file, soptions_));
  ASSERT_OK(seq_file->Read(5, &result, scratch));  // Read "hello".
  ASSERT_EQ(0, result.compare("hello"));
  ASSERT_OK(seq_file->Skip(1));
  ASSERT_OK(seq_file->Read(1000, &result, scratch));  // Read "world".
  ASSERT_EQ(0, result.compare("world"));
  ASSERT_OK(seq_file->Read(1000, &result, scratch));  // Try reading past EOF.
  ASSERT_EQ(0U, result.size());
  ASSERT_OK(seq_file->Skip(100));  // Try to skip past end of file.
  ASSERT_OK(seq_file->Read(1000, &result, scratch));
  ASSERT_EQ(0U, result.size());

  // Random reads.
  ASSERT_OK(env_->NewRandomAccessFile(test_dir_ + "/f", &rand_file, soptions_));
  ASSERT_OK(rand_file->Read(6, 5, &result, scratch));  // Read "world".
  ASSERT_EQ(0, result.compare("world"));
  ASSERT_OK(rand_file->Read(0, 5, &result, scratch));  // Read "hello".
  ASSERT_EQ(0, result.compare("hello"));
  ASSERT_OK(rand_file->Read(10, 100, &result, scratch));  // Read "d".
  ASSERT_EQ(0, result.compare("d"));

  // Too high offset.
  ASSERT_TRUE(rand_file->Read(1000, 5, &result, scratch).ok());
}

TEST_P(EnvBasicTestWithParam, Locks) {
  FileLock* lock;

  // only test they return success.
  // TODO(andrewkr): verify functionality
  ASSERT_OK(env_->LockFile(test_dir_ + "lock_file", &lock));
  ASSERT_OK(env_->UnlockFile(lock));
}

TEST_P(EnvBasicTestWithParam, Misc) {
  std::string test_dir;
  ASSERT_OK(env_->GetTestDirectory(&test_dir));
  ASSERT_TRUE(!test_dir.empty());

  unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/b", &writable_file, soptions_));

  // These are no-ops, but we test they return success.
  ASSERT_OK(writable_file->Sync());
  ASSERT_OK(writable_file->Flush());
  ASSERT_OK(writable_file->Close());
  writable_file.reset();
}

TEST_P(EnvBasicTestWithParam, LargeWrite) {
  const size_t kWriteSize = 300 * 1024;
  char* scratch = new char[kWriteSize * 2];

  std::string write_data;
  for (size_t i = 0; i < kWriteSize; ++i) {
    write_data.append(1, static_cast<char>(i));
  }

  unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("foo"));
  ASSERT_OK(writable_file->Append(write_data));
  writable_file.reset();

  unique_ptr<SequentialFile> seq_file;
  Slice result;
  ASSERT_OK(env_->NewSequentialFile(test_dir_ + "/f", &seq_file, soptions_));
  ASSERT_OK(seq_file->Read(3, &result, scratch));  // Read "foo".
  ASSERT_EQ(0, result.compare("foo"));

  size_t read = 0;
  std::string read_data;
  while (read < kWriteSize) {
    ASSERT_OK(seq_file->Read(kWriteSize - read, &result, scratch));
    read_data.append(result.data(), result.size());
    read += result.size();
  }
  ASSERT_TRUE(write_data == read_data);
  delete [] scratch;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
