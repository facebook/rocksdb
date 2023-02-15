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
  Status DumpToRandomFile(const CloudManifest* manifest, std::string *filepath) {
    Random rnd(301);
    std::string filename = "CLOUDMANIFEST" + rnd.RandomString(7);
    *filepath = tmp_dir_ + "/" + filename;
    std::unique_ptr<WritableFileWriter> writer;
    Status st = WritableFileWriter::Create(env_->GetFileSystem(), *filepath,
                                         FileOptions(), &writer, nullptr);
    if (!st.ok()) {
      return st;
    }

    st = manifest->WriteToLog(std::move(writer));
    return st;
  }

  Status LoadFromFile(const std::string& filepath,
                      std::unique_ptr<CloudManifest>* manifest) {
    std::unique_ptr<SequentialFileReader> reader;
    Status st =
        SequentialFileReader::Create(env_->GetFileSystem(), filepath,
                                     FileOptions(), &reader, nullptr, nullptr);
    if (!st.ok()) {
      return st;
    }
    st = CloudManifest::LoadFromLog(std::move(reader), manifest);
    return st;
  }

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

      std::string filepath;
      ASSERT_OK(DumpToRandomFile(manifest.get(), &filepath));
      manifest.reset();
      ASSERT_OK(LoadFromFile(filepath, &manifest));
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

  EXPECT_EQ(manifest->GetCurrentEpoch(), "epoch2");

  // same file number, different epoch
  EXPECT_TRUE(manifest->AddEpoch(10, "epoch3"));
  EXPECT_EQ(manifest->GetCurrentEpoch(), "epoch3");

  // idempotency for old cm delta
  EXPECT_FALSE(manifest->AddEpoch(10, "epoch2"));
  EXPECT_EQ(manifest->GetCurrentEpoch(), "epoch3");

  EXPECT_TRUE(manifest->AddEpoch(11, "epoch4"));

  EXPECT_EQ(manifest->GetCurrentEpoch(), "epoch4");

  std::vector<std::pair<uint64_t, std::string>> pastEpochs{{10, "epoch1"},
                                                           {10, "epoch2"},
                                                           {11, "epoch3"}};
  EXPECT_EQ(manifest->TEST_GetPastEpochs(), pastEpochs);

  EXPECT_EQ(manifest->GetEpoch(9), "epoch1");
  EXPECT_EQ(manifest->GetEpoch(10), "epoch3");

  std::string filepath;
  ASSERT_OK(DumpToRandomFile(manifest.get(), &filepath));
  manifest.reset();
  ASSERT_OK(LoadFromFile(filepath, &manifest));
}

}  //  namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
