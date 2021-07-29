// Copyright (c) 2017 Rockset

#include "cloud/cloud_log_controller_impl.h"
#include "cloud/cloud_storage_provider_impl.h"
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
class CloudEnvTest : public testing::Test {
public:
  std::unique_ptr<CloudEnv> cenv_;

  virtual ~CloudEnvTest() {
  }
  
};

TEST_F(CloudEnvTest, TestBucket) {
  CloudEnvOptions copts;
  copts.src_bucket.SetRegion("North");
  copts.src_bucket.SetBucketName("Input", "src.");
  ASSERT_FALSE(copts.src_bucket.IsValid());
  copts.src_bucket.SetObjectPath("Here");
  ASSERT_TRUE(copts.src_bucket.IsValid());
  
  copts.dest_bucket.SetRegion("South");
  copts.dest_bucket.SetObjectPath("There");
  ASSERT_FALSE(copts.dest_bucket.IsValid());  
  copts.dest_bucket.SetBucketName("Output", "dest.");
  ASSERT_TRUE(copts.dest_bucket.IsValid());  
}

TEST_F(CloudEnvTest, ConfigureOptions) {
  ConfigOptions config_options;
  CloudEnvOptions copts, copy;
  copts.keep_local_sst_files = false;
  copts.keep_local_log_files = false;
  copts.create_bucket_if_missing = false;
  copts.validate_filesize = false;
  copts.skip_dbid_verification = false;
  copts.ephemeral_resync_on_open = false;
  copts.skip_cloud_files_in_getchildren = false;
  copts.constant_sst_file_size_in_sst_file_manager = 100;
  copts.run_purger = false;
  copts.purger_periodicity_millis = 101;
  
  std::string str;
  ASSERT_OK(copts.Serialize(config_options, &str));
  ASSERT_OK(copy.Configure(config_options, str));
  ASSERT_FALSE(copy.keep_local_sst_files);
  ASSERT_FALSE(copy.keep_local_log_files);
  ASSERT_FALSE(copy.create_bucket_if_missing);
  ASSERT_FALSE(copy.validate_filesize);
  ASSERT_FALSE(copy.skip_dbid_verification);
  ASSERT_FALSE(copy.ephemeral_resync_on_open);
  ASSERT_FALSE(copy.skip_cloud_files_in_getchildren);
  ASSERT_FALSE(copy.run_purger);
  ASSERT_EQ(copy.constant_sst_file_size_in_sst_file_manager, 100);
  ASSERT_EQ(copy.purger_periodicity_millis, 101);

  // Now try a different value
  copts.keep_local_sst_files = true;
  copts.keep_local_log_files = true;
  copts.create_bucket_if_missing = true;
  copts.validate_filesize = true;
  copts.skip_dbid_verification = true;
  copts.ephemeral_resync_on_open = true;
  copts.skip_cloud_files_in_getchildren = true;
  copts.constant_sst_file_size_in_sst_file_manager = 200;
  copts.run_purger = true;
  copts.purger_periodicity_millis = 201;
  
  ASSERT_OK(copts.Serialize(config_options, &str));
  ASSERT_OK(copy.Configure(config_options, str));
  ASSERT_TRUE(copy.keep_local_sst_files);
  ASSERT_TRUE(copy.keep_local_log_files);
  ASSERT_TRUE(copy.create_bucket_if_missing);
  ASSERT_TRUE(copy.validate_filesize);
  ASSERT_TRUE(copy.skip_dbid_verification);
  ASSERT_TRUE(copy.ephemeral_resync_on_open);
  ASSERT_TRUE(copy.skip_cloud_files_in_getchildren);
  ASSERT_TRUE(copy.run_purger);
  ASSERT_EQ(copy.constant_sst_file_size_in_sst_file_manager, 200);
  ASSERT_EQ(copy.purger_periodicity_millis, 201);
}
  
TEST_F(CloudEnvTest, ConfigureBucketOptions) {
  ConfigOptions config_options;
  CloudEnvOptions copts, copy;
  std::string str;
  copts.src_bucket.SetBucketName("source", "src.");
  copts.src_bucket.SetObjectPath("foo");
  copts.src_bucket.SetRegion("north");
  copts.dest_bucket.SetBucketName("dest");
  copts.dest_bucket.SetObjectPath("bar");
  ASSERT_OK(copts.Serialize(config_options, &str));
  
  ASSERT_OK(copy.Configure(config_options, str));
  ASSERT_EQ(copts.src_bucket.GetBucketName(), copy.src_bucket.GetBucketName());
  ASSERT_EQ(copts.src_bucket.GetObjectPath(), copy.src_bucket.GetObjectPath());
  ASSERT_EQ(copts.src_bucket.GetRegion(), copy.src_bucket.GetRegion());

  ASSERT_EQ(copts.dest_bucket.GetBucketName(), copy.dest_bucket.GetBucketName());
  ASSERT_EQ(copts.dest_bucket.GetObjectPath(), copy.dest_bucket.GetObjectPath());
  ASSERT_EQ(copts.dest_bucket.GetRegion(), copy.dest_bucket.GetRegion());
}
  
TEST_F(CloudEnvTest, ConfigureEnv) {
  std::unique_ptr<CloudEnv> cenv;
  
  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(CloudEnv::CreateFromString(config_options, "keep_local_sst_files=true", &cenv));
  ASSERT_NE(cenv, nullptr);
  ASSERT_STREQ(cenv->Name(), "cloud");
  auto copts = cenv->GetOptions<CloudEnvOptions>();
  ASSERT_NE(copts, nullptr);
  ASSERT_TRUE(copts->keep_local_sst_files);
}

TEST_F(CloudEnvTest, TestInitialize) {
  std::unique_ptr<CloudEnv> cenv;
  BucketOptions bucket;
  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(CloudEnv::CreateFromString(
      config_options, "id=cloud; TEST=cloudenvtest:/test/path", &cenv));
  ASSERT_NE(cenv, nullptr);
  ASSERT_STREQ(cenv->Name(), "cloud");

  ASSERT_TRUE(StartsWith(cenv->GetSrcBucketName(),
                         bucket.GetBucketPrefix() + "cloudenvtest."));
  ASSERT_EQ(cenv->GetSrcObjectPath(), "/test/path");
  ASSERT_TRUE(cenv->SrcMatchesDest());

  ASSERT_OK(CloudEnv::CreateFromString(
      config_options, "id=cloud; TEST=cloudenvtest2:/test/path2?here", &cenv));
  ASSERT_NE(cenv, nullptr);
  ASSERT_STREQ(cenv->Name(), "cloud");
  ASSERT_TRUE(StartsWith(cenv->GetSrcBucketName(),
                         bucket.GetBucketPrefix() + "cloudenvtest2."));
  ASSERT_EQ(cenv->GetSrcObjectPath(), "/test/path2");
  ASSERT_EQ(cenv->GetCloudEnvOptions().src_bucket.GetRegion(), "here");
  ASSERT_TRUE(cenv->SrcMatchesDest());

  ASSERT_OK(
      CloudEnv::CreateFromString(config_options,
                                 "id=cloud; TEST=cloudenvtest3:/test/path3; "
                                 "src.bucket=my_bucket; dest.object=/my_path",
                                 &cenv));
  ASSERT_NE(cenv, nullptr);
  ASSERT_STREQ(cenv->Name(), "cloud");
  ASSERT_EQ(cenv->GetSrcBucketName(), bucket.GetBucketPrefix() + "my_bucket");
  ASSERT_EQ(cenv->GetSrcObjectPath(), "/test/path3");
  ASSERT_TRUE(StartsWith(cenv->GetDestBucketName(),
                         bucket.GetBucketPrefix() + "cloudenvtest3."));
  ASSERT_EQ(cenv->GetDestObjectPath(), "/my_path");
}

TEST_F(CloudEnvTest, ConfigureAwsEnv) {
  std::unique_ptr<CloudEnv> cenv;
  
  ConfigOptions config_options;
  Status s = CloudEnv::CreateFromString(config_options, "id=aws; keep_local_sst_files=true", &cenv);
#ifdef USE_AWS
  ASSERT_OK(s);
  ASSERT_NE(cenv, nullptr);
  ASSERT_STREQ(cenv->Name(), "aws");
  auto copts = cenv->GetOptions<CloudEnvOptions>();
  ASSERT_NE(copts, nullptr);
  ASSERT_TRUE(copts->keep_local_sst_files);
  ASSERT_NE(cenv->GetStorageProvider(), nullptr);
  ASSERT_STREQ(cenv->GetStorageProvider()->Name(),
               CloudStorageProviderImpl::kS3());
#else
  ASSERT_NOK(s);
  ASSERT_EQ(cenv, nullptr);
#endif
}
  
TEST_F(CloudEnvTest, ConfigureS3Provider) {
  std::unique_ptr<CloudEnv> cenv;
  
  ConfigOptions config_options;
  Status s = CloudEnv::CreateFromString(config_options, "provider=s3", &cenv);
  ASSERT_NOK(s);
  ASSERT_EQ(cenv, nullptr);
  
#ifdef USE_AWS
  ASSERT_OK(CloudEnv::CreateFromString(config_options, "id=aws; provider=s3", &cenv));
  ASSERT_STREQ(cenv->Name(), "aws");
  ASSERT_NE(cenv->GetStorageProvider(), nullptr);
  ASSERT_STREQ(cenv->GetStorageProvider()->Name(),
               CloudStorageProviderImpl::kS3());
#endif
}

// Test is disabled until we have a mock provider and authentication issues are
// resolved
TEST_F(CloudEnvTest, DISABLED_ConfigureKinesisController) {
  std::unique_ptr<CloudEnv> cenv;
  
  ConfigOptions config_options;
  Status s = CloudEnv::CreateFromString(config_options, "provider=mock; controller=kinesis", &cenv);
  ASSERT_NOK(s);
  ASSERT_EQ(cenv, nullptr);
  
#ifdef USE_AWS
  ASSERT_OK(CloudEnv::CreateFromString(
      config_options, "id=aws; controller=kinesis; TEST=dbcloud:/test", &cenv));
  ASSERT_STREQ(cenv->Name(), "aws");
  ASSERT_NE(cenv->GetLogController(), nullptr);
  ASSERT_STREQ(cenv->GetLogController()->Name(),
               CloudLogControllerImpl::kKinesis());
#endif
}

TEST_F(CloudEnvTest, ConfigureKafkaController) {
  std::unique_ptr<CloudEnv> cenv;
  
  ConfigOptions config_options;
  Status s = CloudEnv::CreateFromString(config_options, "provider=mock; controller=kafka", &cenv);
#ifdef USE_KAFKA
  ASSERT_OK(s);
  ASSERT_NE(cenv, nullptr);
  ASSERT_NE(cenv->GetLogController(), nullptr);
  ASSERT_STREQ(cenv->GetLogController()->Name(),
               CloudLogControllerImpl::kKafka());
#else
  ASSERT_NOK(s);
  ASSERT_EQ(cenv, nullptr);
#endif
}

  
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
  
