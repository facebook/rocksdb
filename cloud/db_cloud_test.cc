// Copyright (c) 2017 Rockset

#ifndef ROCKSDB_LITE

#ifdef USE_AWS

#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "rocksdb/cloud/db_cloud.h"
#include "util/testharness.h"
#include "util/logging.h"
#include "cloud/aws/aws_env.h"
#include "aws/aws_file.h"
#ifndef OS_WIN
#include <unistd.h>
#endif

namespace rocksdb {

class CloudTest : public testing::Test {
 public:
  CloudTest() {
    dbname_ = test::TmpDir() + "/db_cloud";
    cloud_storage_bucket_prefix_ = "dbcloud." + AwsEnv::GetTestBucketSuffix();
    options_.create_if_missing = true;
    db_ = nullptr;
    aenv_ = nullptr;
    DestroyDB(dbname_, Options());
    CreateLoggerFromOptions(dbname_, options_, &options_.info_log);

    // Get cloud credentials
    AwsEnv::GetTestCredentials(
              &cloud_env_options_.credentials.access_key_id,
              &cloud_env_options_.credentials.secret_key,
              &cloud_env_options_.region);
    EmptyBucket();
  }

  void EmptyBucket() {
    ASSERT_TRUE(!aenv_);
    // create a dummy aws env 
    ASSERT_OK(CloudEnv::NewAwsEnv(Env::Default(),
			          cloud_storage_bucket_prefix_,
		                  cloud_env_options_,
				  options_.info_log,
				  &aenv_));
    // delete all pre-existing contents from the bucket
    ASSERT_OK(aenv_->EmptyBucket());
    delete aenv_;
    aenv_ = nullptr;
  }

  virtual ~CloudTest() {
    CloseDB();
    // XXX DestroyDB(dbname_, Options());
  }

  // Open database via the cloud interface
  void OpenDB() {
    ASSERT_NE(cloud_env_options_.credentials.access_key_id.size(), 0);
    ASSERT_NE(cloud_env_options_.credentials.secret_key.size(), 0);

    // Create new AWS env
    ASSERT_OK(CloudEnv::NewAwsEnv(Env::Default(),
			          cloud_storage_bucket_prefix_,
		                  cloud_env_options_,
				  options_.info_log,
				  &aenv_));
    options_.env = aenv_;

    // default column family
    ColumnFamilyOptions cfopt = options_;
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cfopt));
    std::vector<ColumnFamilyHandle*> handles;

    ASSERT_TRUE(db_ == nullptr);
    ASSERT_OK(DBCloud::Open(options_, dbname_,
			    column_families, &handles,
			    &db_));
    // Delete the handle for the default column family because the DBImpl
    // always holds a reference to it.
    ASSERT_TRUE(handles.size() > 0);
    delete handles[0];
  }

  // Creates and Opens a clone
  void CloneDB(const std::string& clone_name,
	       std::unique_ptr<DBCloud>* cloud_db,
	       std::unique_ptr<CloudEnv>* cloud_env) {

    // The local directory where the clone resides
    std::string cname = test::TmpDir() + "/" + clone_name;

    CloudEnv* cenv;
    DBCloud* clone_db;

    // Create new AWS env
    ASSERT_OK(CloudEnv::NewAwsEnv(Env::Default(),
			          cloud_storage_bucket_prefix_,
		                  cloud_env_options_,
				  options_.info_log,
				  &cenv));

    // sets the cloud env to be used by the env wrapper
    options_.env = cenv;

    // Returns the cloud env that was created
    cloud_env->reset(cenv);

    // default column family
    ColumnFamilyOptions cfopt = options_;

    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cfopt));
    std::vector<ColumnFamilyHandle*> handles;

    ASSERT_OK(DBCloud::OpenClone(options_, cname,
			        column_families, &handles,
			        &clone_db));
    cloud_db->reset(clone_db);

    // Delete the handle for the default column family because the DBImpl
    // always holds a reference to it.
    ASSERT_TRUE(handles.size() > 0);
    delete handles[0];
  }

  void CloseDB() {
    if (db_) {
      db_->Flush(FlushOptions());  // convert pending writes to sst files
      delete db_;
      db_ = nullptr;
    }
    if (aenv_) {
      delete aenv_;
      aenv_ = nullptr;
    }
  }

 protected:
  Options options_;
  std::string dbname_;
  std::string cloud_storage_bucket_prefix_;
  CloudEnvOptions cloud_env_options_;
  DBCloud* db_;
  CloudEnv* aenv_;
};

//
// Most basic test. Create DB, write one key, close it and then check to see
// that the key exists.
//
TEST_F(CloudTest, BasicTest) {

  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  CloseDB();
  value.clear();

  // Reopen and validate
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");
  CloseDB();
}

//
// Create and read from a clone.
//
TEST_F(CloudTest, BasicClone) {

  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  CloseDB();
  value.clear();

  // Create and Open clone
  std::unique_ptr<CloudEnv> cloud_env;
  std::unique_ptr<DBCloud> cloud_db;
  //CloneDB("clone1", &cloud_db, &cloud_env);
  //Status stax = cloud_db->Get(ReadOptions(), "Hello", &value);

  //ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
  //ASSERT_TRUE(value.compare("World") == 0);
}

} //  namespace rocksdb

// A black-box test for the cloud wrapper around rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else // USE_AWS

#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr,
          "SKIPPED as DBCloud is supported only when USE_AWS is defined.\n");
  return 0;
}
#endif

#else // ROCKSDB_LITE

#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr, "SKIPPED as DBCloud is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
