//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <string>
#include <vector>

#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "rocksdb/db.h"
#include "rocksdb/transaction_log.h"
#include "util/file_util.h"

namespace rocksdb {

class RepairTest : public DBTestBase {
 public:
  RepairTest() : DBTestBase("/repair_test") {}

  std::string GetFirstSstPath() {
    uint64_t manifest_size;
    std::vector<std::string> files;
    db_->GetLiveFiles(files, &manifest_size);
    auto sst_iter =
        std::find_if(files.begin(), files.end(), [](const std::string& file) {
          uint64_t number;
          FileType type;
          bool ok = ParseFileName(file, &number, &type);
          return ok && type == kTableFile;
        });
    return sst_iter == files.end() ? "" : dbname_ + *sst_iter;
  }
};

TEST_F(RepairTest, LostManifest) {
  // Add a couple SST files, delete the manifest, and verify RepairDB() saves
  // the day.
  Put("key", "val");
  Flush();
  Put("key2", "val2");
  Flush();
  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  RepairDB(dbname_, CurrentOptions());
  Reopen(CurrentOptions());

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "val2");
}

TEST_F(RepairTest, CorruptManifest) {
  // Manifest is in an invalid format. Expect a full recovery.
  Put("key", "val");
  Flush();
  Put("key2", "val2");
  Flush();
  // Need to get path before Close() deletes db_, but overwrite it after Close()
  // to ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  CreateFile(env_, manifest_path, "blah");
  RepairDB(dbname_, CurrentOptions());
  Reopen(CurrentOptions());

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "val2");
}

TEST_F(RepairTest, IncompleteManifest) {
  // In this case, the manifest is valid but does not reference all of the SST
  // files. Expect a full recovery.
  Put("key", "val");
  Flush();
  std::string orig_manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  CopyFile(orig_manifest_path, orig_manifest_path + ".tmp");
  Put("key2", "val2");
  Flush();
  // Need to get path before Close() deletes db_, but overwrite it after Close()
  // to ensure Close() didn't change the manifest.
  std::string new_manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(new_manifest_path));
  // Replace the manifest with one that is only aware of the first SST file.
  CopyFile(orig_manifest_path + ".tmp", new_manifest_path);
  RepairDB(dbname_, CurrentOptions());
  Reopen(CurrentOptions());

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "val2");
}

TEST_F(RepairTest, LostSst) {
  // Delete one of the SST files but preserve the manifest that refers to it,
  // then verify the DB is still usable for the intact SST.
  Put("key", "val");
  Flush();
  Put("key2", "val2");
  Flush();
  auto sst_path = GetFirstSstPath();
  ASSERT_FALSE(sst_path.empty());
  ASSERT_OK(env_->DeleteFile(sst_path));

  Close();
  RepairDB(dbname_, CurrentOptions());
  Reopen(CurrentOptions());

  // Exactly one of the key-value pairs should be in the DB now.
  ASSERT_TRUE((Get("key") == "val") != (Get("key2") == "val2"));
}

TEST_F(RepairTest, CorruptSst) {
  // Corrupt one of the SST files but preserve the manifest that refers to it,
  // then verify the DB is still usable for the intact SST.
  Put("key", "val");
  Flush();
  Put("key2", "val2");
  Flush();
  auto sst_path = GetFirstSstPath();
  ASSERT_FALSE(sst_path.empty());
  CreateFile(env_, sst_path, "blah");

  Close();
  RepairDB(dbname_, CurrentOptions());
  Reopen(CurrentOptions());

  // Exactly one of the key-value pairs should be in the DB now.
  ASSERT_TRUE((Get("key") == "val") != (Get("key2") == "val2"));
}

TEST_F(RepairTest, UnflushedSst) {
  // This test case invokes repair while some data is unflushed, then verifies
  // that data is in the db.
  Put("key", "val");
  VectorLogPtr wal_files;
  ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
  ASSERT_EQ(wal_files.size(), 1);
  uint64_t total_ssts_size;
  GetAllSSTFiles(&total_ssts_size);
  ASSERT_EQ(total_ssts_size, 0);
  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  RepairDB(dbname_, CurrentOptions());
  Reopen(CurrentOptions());

  ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
  ASSERT_EQ(wal_files.size(), 0);
  GetAllSSTFiles(&total_ssts_size);
  ASSERT_GT(total_ssts_size, 0);
  ASSERT_EQ(Get("key"), "val");
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
