// Copyright (c) 2017 Rockset

#include "cloud/cloud_manifest.h"

#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "util/file_reader_writer.h"

namespace rocksdb {

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
    manifest->Finalize();

    ASSERT_EQ(manifest->GetEpoch(130).ToString(), "firstEpoch");
  }
  {
    std::unique_ptr<CloudManifest> manifest;
    ASSERT_OK(CloudManifest::CreateForEmptyDatabase("firstEpoch", &manifest));
    manifest->AddEpoch(10, "secondEpoch");
    manifest->AddEpoch(10, "thirdEpoch");
    manifest->AddEpoch(40, "fourthEpoch");
    manifest->Finalize();

    for (int iter = 0; iter < 2; ++iter) {
      ASSERT_EQ(manifest->GetEpoch(0).ToString(), "firstEpoch");
      ASSERT_EQ(manifest->GetEpoch(5).ToString(), "firstEpoch");
      ASSERT_EQ(manifest->GetEpoch(10).ToString(), "thirdEpoch");
      ASSERT_EQ(manifest->GetEpoch(11).ToString(), "thirdEpoch");
      ASSERT_EQ(manifest->GetEpoch(39).ToString(), "thirdEpoch");
      ASSERT_EQ(manifest->GetEpoch(40).ToString(), "fourthEpoch");
      ASSERT_EQ(manifest->GetEpoch(41).ToString(), "fourthEpoch");

      // serialize and deserialize
      auto tmpfile = tmp_dir_ + "/cloudmanifest";
      {
        std::unique_ptr<WritableFile> file;
        ASSERT_OK(env_->NewWritableFile(tmpfile, &file, EnvOptions()));
        ASSERT_OK(manifest->WriteToLog(std::unique_ptr<WritableFileWriter>(
            new WritableFileWriter(std::move(file), tmpfile, EnvOptions()))));
      }

      manifest.reset();
      {
        std::unique_ptr<SequentialFile> file;
        ASSERT_OK(env_->NewSequentialFile(tmpfile, &file, EnvOptions()));
        CloudManifest::LoadFromLog(
            std::unique_ptr<SequentialFileReader>(
                new SequentialFileReader(std::move(file), tmpfile)),
            &manifest);
      }
    }
  }
}

}  //  namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
