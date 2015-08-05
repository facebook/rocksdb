//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <atomic>
#include <thread>
#include <vector>

#include "rocksdb/delete_scheduler.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "util/delete_scheduler_impl.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"

namespace rocksdb {

class DeleteSchedulerTest : public testing::Test {
 public:
  DeleteSchedulerTest() : env_(Env::Default()) {
    dummy_files_dir_ = test::TmpDir(env_) + "/dummy_data_dir";
    DestroyAndCreateDir(dummy_files_dir_);
    trash_dir_ = test::TmpDir(env_) + "/trash";
    DestroyAndCreateDir(trash_dir_);
  }

  ~DeleteSchedulerTest() {
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
    rocksdb::SyncPoint::GetInstance()->LoadDependency({});
    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
    DestroyDir(dummy_files_dir_);
    if (delete_scheduler_ != nullptr) {
      delete delete_scheduler_;
      delete_scheduler_ = nullptr;
    }
  }

  void WaitForEmptyTrash() {
    reinterpret_cast<DeleteSchedulerImpl*>(delete_scheduler_)
        ->TEST_WaitForEmptyTrash();
  }

  void DestroyDir(const std::string& dir) {
    if (env_->FileExists(dir).IsNotFound()) {
      return;
    }
    std::vector<std::string> files_in_dir;
    EXPECT_OK(env_->GetChildren(dir, &files_in_dir));
    for (auto& file_in_dir : files_in_dir) {
      if (file_in_dir == "." || file_in_dir == "..") {
        continue;
      }
      EXPECT_OK(env_->DeleteFile(dir + "/" + file_in_dir));
    }
    EXPECT_OK(env_->DeleteDir(dir));
  }

  void DestroyAndCreateDir(const std::string& dir) {
    DestroyDir(dir);
    EXPECT_OK(env_->CreateDir(dir));
  }

  int CountFilesInDir(const std::string& dir) {
    std::vector<std::string> files_in_dir;
    EXPECT_OK(env_->GetChildren(dir, &files_in_dir));
    // Ignore "." and ".."
    return static_cast<int>(files_in_dir.size()) - 2;
  }

  std::string NewDummyFile(const std::string& file_name, uint64_t size = 1024) {
    std::string file_path = dummy_files_dir_ + "/" + file_name;
    std::unique_ptr<WritableFile> f;
    env_->NewWritableFile(file_path, &f, EnvOptions());
    std::string data(size, 'A');
    EXPECT_OK(f->Append(data));
    EXPECT_OK(f->Close());
    return file_path;
  }

  Env* env_;
  std::string dummy_files_dir_;
  std::string trash_dir_;
  int64_t rate_bytes_per_sec_;
  DeleteScheduler* delete_scheduler_;
};

// Test the basic functionality of DeleteScheduler (Rate Limiting).
// 1- Create 100 dummy files
// 2- Delete the 100 dummy files using DeleteScheduler
// 3- Wait for DeleteScheduler to delete all files in trash
// 4- Measure time spent in step 2,3 and make sure it matches the expected
//    time from a rate limited delete
// 5- Make sure that all created files were completely deleted
TEST_F(DeleteSchedulerTest, BasicRateLimiting) {
  int num_files = 100;  // 100 files
  uint64_t file_size = 1024;  // every file is 1 kb
  std::vector<uint64_t> delete_kbs_per_sec = {512, 200, 100, 50, 25};

  for (size_t t = 0; t < delete_kbs_per_sec.size(); t++) {
    DestroyAndCreateDir(dummy_files_dir_);
    rate_bytes_per_sec_ = delete_kbs_per_sec[t] * 1024;
    delete_scheduler_ =
        NewDeleteScheduler(env_, trash_dir_, rate_bytes_per_sec_);

    // Create 100 dummy files, every file is 1 Kb
    std::vector<std::string> generated_files;
    uint64_t total_files_size = 0;
    for (int i = 0; i < num_files; i++) {
      std::string file_name = "file" + ToString(i) + ".data";
      generated_files.push_back(NewDummyFile(file_name, file_size));
      total_files_size += file_size;
    }

    // Delete dummy files and measure time spent to empty trash
    uint64_t delete_start_time = env_->NowMicros();
    for (int i = 0; i < num_files; i++) {
      ASSERT_OK(delete_scheduler_->DeleteFile(generated_files[i]));
    }
    ASSERT_EQ(CountFilesInDir(dummy_files_dir_), 0);

    WaitForEmptyTrash();
    uint64_t time_spent_deleting = env_->NowMicros() - delete_start_time;
    uint64_t expected_delete_time =
        ((total_files_size * 1000000) / rate_bytes_per_sec_);
    ASSERT_GT(time_spent_deleting, expected_delete_time * 0.9);
    ASSERT_LT(time_spent_deleting, expected_delete_time * 1.1);
    printf("Delete time = %" PRIu64 ", Expected delete time = %" PRIu64
           ", Ratio %f\n",
           time_spent_deleting, expected_delete_time,
           static_cast<double>(time_spent_deleting) / expected_delete_time);

    ASSERT_EQ(CountFilesInDir(trash_dir_), 0);
    auto bg_errors = delete_scheduler_->GetBackgroundErrors();
    ASSERT_EQ(bg_errors.size(), 0);
  }
}

// Same as the BasicRateLimiting test but delete files in multiple threads.
// 1- Create 100 dummy files
// 2- Delete the 100 dummy files using DeleteScheduler using 10 threads
// 3- Wait for DeleteScheduler to delete all files in queue
// 4- Measure time spent in step 2,3 and make sure it matches the expected
//    time from a rate limited delete
// 5- Make sure that all created files were completely deleted
TEST_F(DeleteSchedulerTest, RateLimitingMultiThreaded) {
  int thread_cnt = 10;
  int num_files = 10;  // 10 files per thread
  uint64_t file_size = 1024;  // every file is 1 kb
  std::vector<uint64_t> delete_kbs_per_sec = {512, 200, 100, 50, 25};

  for (size_t t = 0; t < delete_kbs_per_sec.size(); t++) {
    DestroyAndCreateDir(dummy_files_dir_);
    rate_bytes_per_sec_ = delete_kbs_per_sec[t] * 1024;
    delete_scheduler_ =
        NewDeleteScheduler(env_, trash_dir_, rate_bytes_per_sec_);

    // Create 100 dummy files, every file is 1 Kb
    std::vector<std::string> generated_files;
    uint64_t total_files_size = 0;
    for (int i = 0; i < num_files * thread_cnt; i++) {
      std::string file_name = "file" + ToString(i) + ".data";
      generated_files.push_back(NewDummyFile(file_name, file_size));
      total_files_size += file_size;
    }

    // Delete dummy files using 10 threads and measure time spent to empty trash
    uint64_t delete_start_time = env_->NowMicros();
    std::atomic<int> thread_num(0);
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_cnt; i++) {
      threads.emplace_back([&]() {
        int idx = thread_num.fetch_add(1);
        int range_start = idx * num_files;
        int range_end = range_start + num_files;
        for (int j = range_start; j < range_end; j++){
          ASSERT_OK(delete_scheduler_->DeleteFile(generated_files[j]));
        }
      });
    }

    for (size_t i = 0; i < threads.size(); i++) {
      threads[i].join();
    }
    ASSERT_EQ(CountFilesInDir(dummy_files_dir_), 0);

    WaitForEmptyTrash();
    uint64_t time_spent_deleting = env_->NowMicros() - delete_start_time;
    uint64_t expected_delete_time =
        ((total_files_size * 1000000) / rate_bytes_per_sec_);
    ASSERT_GT(time_spent_deleting, expected_delete_time * 0.9);
    ASSERT_LT(time_spent_deleting, expected_delete_time * 1.1);
    printf("Delete time = %" PRIu64 ", Expected delete time = %" PRIu64
           ", Ratio %f\n",
           time_spent_deleting, expected_delete_time,
           static_cast<double>(time_spent_deleting) / expected_delete_time);

    ASSERT_EQ(CountFilesInDir(trash_dir_), 0);
    auto bg_errors = delete_scheduler_->GetBackgroundErrors();
    ASSERT_EQ(bg_errors.size(), 0);
  }
}

// Disable rate limiting by setting rate_bytes_per_sec_ to 0 and make sure
// that when DeleteScheduler delete a file it delete it immediately and dont
// move it to trash
TEST_F(DeleteSchedulerTest, DisableRateLimiting) {
  int bg_delete_file = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteSchedulerImpl::DeleteTrashFile:DeleteFile",
      [&](void* arg) { bg_delete_file++; });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  delete_scheduler_ = NewDeleteScheduler(env_, "", 0);

  for (int i = 0; i < 10; i++) {
    // Every file we delete will be deleted immediately
    std::string dummy_file = NewDummyFile("dummy.data");
    ASSERT_OK(delete_scheduler_->DeleteFile(dummy_file));
    ASSERT_TRUE(env_->FileExists(dummy_file).IsNotFound());
    ASSERT_EQ(CountFilesInDir(dummy_files_dir_), 0);
    ASSERT_EQ(CountFilesInDir(trash_dir_), 0);
  }

  ASSERT_EQ(bg_delete_file, 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

// Testing that moving files to trash with the same name is not a problem
// 1- Create 10 files with the same name "conflict.data"
// 2- Delete the 10 files using DeleteScheduler
// 3- Make sure that trash directory contain 10 files ("conflict.data" x 10)
// --- Hold DeleteSchedulerImpl::BackgroundEmptyTrash ---
// 4- Make sure that files are deleted from trash
TEST_F(DeleteSchedulerTest, ConflictNames) {
  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::ConflictNames:1",
       "DeleteSchedulerImpl::BackgroundEmptyTrash"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 Mb/sec
  delete_scheduler_ = NewDeleteScheduler(env_, trash_dir_, rate_bytes_per_sec_);

  // Create "conflict.data" and move it to trash 10 times
  for (int i = 0; i < 10; i++) {
    std::string dummy_file = NewDummyFile("conflict.data");
    ASSERT_OK(delete_scheduler_->DeleteFile(dummy_file));
  }
  ASSERT_EQ(CountFilesInDir(dummy_files_dir_), 0);
  // 10 files ("conflict.data" x 10) in trash
  ASSERT_EQ(CountFilesInDir(trash_dir_), 10);

  // Hold BackgroundEmptyTrash
  TEST_SYNC_POINT("DeleteSchedulerTest::ConflictNames:1");
  WaitForEmptyTrash();
  ASSERT_EQ(CountFilesInDir(trash_dir_), 0);

  auto bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(bg_errors.size(), 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

// 1- Create 10 dummy files
// 2- Delete the 10 files using DeleteScheduler (move them to trsah)
// 3- Delete the 10 files directly (using env_->DeleteFile)
// --- Hold DeleteSchedulerImpl::BackgroundEmptyTrash ---
// 4- Make sure that DeleteScheduler failed to delete the 10 files and
//    reported 10 background errors
TEST_F(DeleteSchedulerTest, BackgroundError) {
  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::BackgroundError:1",
       "DeleteSchedulerImpl::BackgroundEmptyTrash"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 Mb/sec
  delete_scheduler_ = NewDeleteScheduler(env_, trash_dir_, rate_bytes_per_sec_);

  // Generate 10 dummy files and move them to trash
  for (int i = 0; i < 10; i++) {
    std::string file_name = "data_" + ToString(i) + ".data";
    ASSERT_OK(delete_scheduler_->DeleteFile(NewDummyFile(file_name)));
  }
  ASSERT_EQ(CountFilesInDir(dummy_files_dir_), 0);
  ASSERT_EQ(CountFilesInDir(trash_dir_), 10);

  // Delete 10 files from trash, this will cause background errors in
  // BackgroundEmptyTrash since we already deleted the files it was
  // goind to delete
  for (int i = 0; i < 10; i++) {
    std::string file_name = "data_" + ToString(i) + ".data";
    ASSERT_OK(env_->DeleteFile(trash_dir_ + "/" + file_name));
  }

  // Hold BackgroundEmptyTrash
  TEST_SYNC_POINT("DeleteSchedulerTest::BackgroundError:1");
  WaitForEmptyTrash();
  auto bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(bg_errors.size(), 10);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

// 1- Create 10 files in trash
// 2- Create a DeleteScheduler with delete_exisitng_trash = true
// 3- Wait for DeleteScheduler to delete all files in queue
// 4- Make sure that all files in trash directory were deleted
TEST_F(DeleteSchedulerTest, TrashWithExistingFiles) {
  std::vector<std::string> dummy_files;
  for (int i = 0; i < 10; i++) {
    std::string file_name = "data_" + ToString(i) + ".data";
    std::string trash_path = trash_dir_ + "/" + file_name;
    env_->RenameFile(NewDummyFile(file_name), trash_path);
  }
  ASSERT_EQ(CountFilesInDir(trash_dir_), 10);

  Status s;
  rate_bytes_per_sec_ = 1024 * 1024;  // 1 Mb/sec
  delete_scheduler_ = NewDeleteScheduler(env_, trash_dir_, rate_bytes_per_sec_,
                                         nullptr, true, &s);
  ASSERT_OK(s);

  WaitForEmptyTrash();
  ASSERT_EQ(CountFilesInDir(trash_dir_), 0);

  auto bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(bg_errors.size(), 0);
}

// 1- Create 10 dummy files
// 2- Delete 10 dummy files using DeleteScheduler
// 3- Wait for DeleteScheduler to delete all files in queue
// 4- Make sure all files in trash directory were deleted
// 5- Repeat previous steps 5 times
TEST_F(DeleteSchedulerTest, StartBGEmptyTrashMultipleTimes) {
  int bg_delete_file = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteSchedulerImpl::DeleteTrashFile:DeleteFile",
      [&](void* arg) { bg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / sec
  delete_scheduler_ = NewDeleteScheduler(env_, trash_dir_, rate_bytes_per_sec_);

  // Move files to trash, wait for empty trash, start again
  for (int run = 1; run <= 5; run++) {
    // Generate 10 dummy files and move them to trash
    for (int i = 0; i < 10; i++) {
      std::string file_name = "data_" + ToString(i) + ".data";
      ASSERT_OK(delete_scheduler_->DeleteFile(NewDummyFile(file_name)));
    }
    ASSERT_EQ(CountFilesInDir(dummy_files_dir_), 0);
    WaitForEmptyTrash();
    ASSERT_EQ(bg_delete_file, 10 * run);
    ASSERT_EQ(CountFilesInDir(trash_dir_), 0);

    auto bg_errors = delete_scheduler_->GetBackgroundErrors();
    ASSERT_EQ(bg_errors.size(), 0);
  }

  ASSERT_EQ(bg_delete_file, 50);
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
}

// 1- Create a DeleteScheduler with very slow rate limit (1 Byte / sec)
// 2- Delete 100 files using DeleteScheduler
// 3- Delete the DeleteScheduler (call the destructor while queue is not empty)
// 4- Make sure that not all files were deleted from trash and that
//    DeleteScheduler background thread did not delete all files
TEST_F(DeleteSchedulerTest, DestructorWithNonEmptyQueue) {
  int bg_delete_file = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteSchedulerImpl::DeleteTrashFile:DeleteFile",
      [&](void* arg) { bg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1;  // 1 Byte / sec
  delete_scheduler_ = NewDeleteScheduler(env_, trash_dir_, rate_bytes_per_sec_);

  for (int i = 0; i < 100; i++) {
    std::string file_name = "data_" + ToString(i) + ".data";
    ASSERT_OK(delete_scheduler_->DeleteFile(NewDummyFile(file_name)));
  }

  // Deleting 100 files will need >28 hours to delete
  // we will delete the DeleteScheduler while delete queue is not empty
  delete delete_scheduler_;
  delete_scheduler_ = nullptr;

  ASSERT_LT(bg_delete_file, 100);
  ASSERT_GT(CountFilesInDir(trash_dir_), 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

// 1- Delete the trash directory
// 2- Delete 10 files using DeleteScheduler
// 3- Make sure that the 10 files were deleted immediately since DeleteScheduler
//    failed to move them to trash directory
TEST_F(DeleteSchedulerTest, MoveToTrashError) {
  int bg_delete_file = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteSchedulerImpl::DeleteTrashFile:DeleteFile",
      [&](void* arg) { bg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024;  // 1 Kb / sec
  delete_scheduler_ = NewDeleteScheduler(env_, trash_dir_, rate_bytes_per_sec_);

  // We will delete the trash directory, that mean that DeleteScheduler wont
  // be able to move files to trash and will delete files them immediately.
  DestroyDir(trash_dir_);
  for (int i = 0; i < 10; i++) {
    std::string file_name = "data_" + ToString(i) + ".data";
    ASSERT_OK(delete_scheduler_->DeleteFile(NewDummyFile(file_name)));
  }

  ASSERT_EQ(CountFilesInDir(dummy_files_dir_), 0);
  ASSERT_EQ(bg_delete_file, 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
