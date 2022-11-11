// Copyright (c) 2017 Rockset

#include "cloud/cloud_manifest.h"

#include "env/composite_env_wrapper.h"
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class CloudManifestTest : public testing::Test {
 public:
  CloudManifestTest() {
    tmp_dir_ = test::TmpDir() + "/cloud_manifest_test";
    env_ = Env::Default();
    env_->CreateDir(tmp_dir_);
  }

 protected:
  std::string tmp_dir_;
  Env* env_;
};

TEST_F(CloudManifestTest, BasicTest) {
  {
    std::unique_ptr<CloudManifest> manifest;
    ASSERT_OK(CloudManifest::CreateForEmptyDatabase("firstEpoch", &manifest));

    ASSERT_EQ(manifest->GetEpoch(130), "firstEpoch");
  }
  {
    std::unique_ptr<CloudManifest> manifest;
    ASSERT_OK(CloudManifest::CreateForEmptyDatabase("firstEpoch", &manifest));
    EXPECT_TRUE(manifest->AddEpoch(10, "secondEpoch"));
    EXPECT_TRUE(manifest->AddEpoch(10, "thirdEpoch"));
    EXPECT_TRUE(manifest->AddEpoch(40, "fourthEpoch"));

    for (int iter = 0; iter < 2; ++iter) {
      ASSERT_EQ(manifest->GetEpoch(0), "firstEpoch");
      ASSERT_EQ(manifest->GetEpoch(5), "firstEpoch");
      ASSERT_EQ(manifest->GetEpoch(10), "thirdEpoch");
      ASSERT_EQ(manifest->GetEpoch(11), "thirdEpoch");
      ASSERT_EQ(manifest->GetEpoch(39), "thirdEpoch");
      ASSERT_EQ(manifest->GetEpoch(40), "fourthEpoch");
      ASSERT_EQ(manifest->GetEpoch(41), "fourthEpoch");

      // serialize and deserialize
      auto tmpfile = tmp_dir_ + "/cloudmanifest";
      {
        std::unique_ptr<WritableFileWriter> writer;
        ASSERT_OK(WritableFileWriter::Create(env_->GetFileSystem(), tmpfile,
                                             FileOptions(), &writer, nullptr));
        ASSERT_OK(manifest->WriteToLog(std::move(writer)));
      }

      manifest.reset();
      {
        std::unique_ptr<SequentialFileReader> reader;
        ASSERT_OK(SequentialFileReader::Create(
            env_->GetFileSystem(), tmpfile, FileOptions(), &reader, nullptr));
        ASSERT_OK(CloudManifest::LoadFromLog(std::move(reader), &manifest));
      }
    }
  }
}

TEST_F(CloudManifestTest, IdempotencyTest) {
  std::unique_ptr<CloudManifest> manifest;
  ASSERT_OK(CloudManifest::CreateForEmptyDatabase("epoch1", &manifest));
  EXPECT_TRUE(manifest->AddEpoch(10, "epoch2"));
  // file number goes back in time
  EXPECT_FALSE(manifest->AddEpoch(9, "epoch3"));
  // same file number, same epoch
  EXPECT_FALSE(manifest->AddEpoch(10, "epoch2"));
  // same file number, different epoch
  EXPECT_TRUE(manifest->AddEpoch(10, "epoch3"));
}

}  //  namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
