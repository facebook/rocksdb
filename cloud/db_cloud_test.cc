// Copyright (c) 2017 Rockset

#ifndef ROCKSDB_LITE

#ifdef USE_AWS

#include "rocksdb/cloud/db_cloud.h"
#include "cloud/aws/aws_env.h"
#include "cloud/db_cloud_impl.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "util/logging.h"
#include "util/testharness.h"
#include <algorithm>
#include <chrono>
#ifndef OS_WIN
#include <unistd.h>
#endif

namespace rocksdb {

class CloudTest : public testing::Test {
 public:
  CloudTest() {
    base_env_ = Env::Default();
    dbname_ = test::TmpDir() + "/db_cloud";
    clone_dir_ = test::TmpDir() + "/ctest";
    src_bucket_prefix_ = "dbcloud." + AwsEnv::GetTestBucketSuffix();
    src_object_prefix_ = dbname_;
    options_.create_if_missing = true;
    db_ = nullptr;
    aenv_ = nullptr;
    persistent_cache_path_ = "";
    persistent_cache_size_gb_ = 0;

    DestroyDB(dbname_, Options());
    CreateLoggerFromOptions(dbname_, options_, &options_.info_log);

    // Get cloud credentials
    AwsEnv::GetTestCredentials(&cloud_env_options_.credentials.access_key_id,
                               &cloud_env_options_.credentials.secret_key,
                               &region_);
    Cleanup();
  }

  void Cleanup() {
    ASSERT_TRUE(!aenv_);

    // create a dummy aws env
    ASSERT_OK(CloudEnv::NewAwsEnv(
        base_env_, src_bucket_prefix_, src_object_prefix_, region_,
        dest_bucket_prefix_, dest_object_prefix_, region_,
        cloud_env_options_, options_.info_log, &aenv_));
    // delete all pre-existing contents from the bucket
    Status st = aenv_->EmptyBucket(src_bucket_prefix_);
    ASSERT_TRUE(st.ok() || st.IsNotFound());
    delete aenv_;
    aenv_ = nullptr;

    // delete and create directory where clones reside
    DestroyDir(clone_dir_);
    ASSERT_OK(base_env_->CreateDir(clone_dir_));
  }

  void DestroyDir(const std::string& dir) {
    std::string cmd = "rm -rf " + dir;
    int rc = system(cmd.c_str());
    ASSERT_EQ(rc, 0);
  }

  virtual ~CloudTest() {
    CloseDB();
    DestroyDB(dbname_, Options());
    DestroyDir(clone_dir_);
  }

  void CreateAwsEnv() {
    ASSERT_OK(CloudEnv::NewAwsEnv(
        base_env_, src_bucket_prefix_, src_object_prefix_, region_,
        src_bucket_prefix_, src_object_prefix_, region_,
        cloud_env_options_, options_.info_log, &aenv_));
  }

  // Open database via the cloud interface
  void OpenDB() {
    ASSERT_NE(cloud_env_options_.credentials.access_key_id.size(), 0);
    ASSERT_NE(cloud_env_options_.credentials.secret_key.size(), 0);

    // Create new AWS env
    CreateAwsEnv();
    options_.env = aenv_;

    // default column family
    ColumnFamilyOptions cfopt = options_;
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(
        ColumnFamilyDescriptor(kDefaultColumnFamilyName, cfopt));
    std::vector<ColumnFamilyHandle*> handles;

    ASSERT_TRUE(db_ == nullptr);
    ASSERT_OK(DBCloud::Open(options_, dbname_, column_families,
                            persistent_cache_path_, persistent_cache_size_gb_,
                            &handles, &db_));
    ASSERT_OK(db_->GetDbIdentity(dbid_));

    // Delete the handle for the default column family because the DBImpl
    // always holds a reference to it.
    ASSERT_TRUE(handles.size() > 0);
    delete handles[0];
  }

  // Creates and Opens a clone
  void CloneDB(const std::string& clone_name, const std::string& src_bucket,
               const std::string& src_object_path,
               const std::string& dest_bucket,
               const std::string& dest_object_path,
               std::unique_ptr<DBCloud>* cloud_db,
               std::unique_ptr<CloudEnv>* cloud_env) {
    // The local directory where the clone resides
    std::string cname = clone_dir_ + "/" + clone_name;

    CloudEnv* cenv;
    DBCloud* clone_db;

    // If there is no destination bucket, then the clone needs to copy
    // all sst fies from source bucket to local dir
    CloudEnvOptions copt = cloud_env_options_;
    if (dest_bucket.empty()) {
      copt.keep_local_sst_files = true;
    }

    // Create new AWS env
    ASSERT_OK(CloudEnv::NewAwsEnv(
        base_env_,
        src_bucket, src_object_path, region_,
        dest_bucket, dest_object_path, region_,
        copt, options_.info_log, &cenv));

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

    ASSERT_OK(DBCloud::Open(options_, cname, column_families,
                            persistent_cache_path_, persistent_cache_size_gb_,
                            &handles, &clone_db));
    cloud_db->reset(clone_db);

    // Delete the handle for the default column family because the DBImpl
    // always holds a reference to it.
    ASSERT_TRUE(handles.size() > 0);
    delete handles[0];

    // verify that a clone created with an empty destination directory
    // creates a db object using the local env. We wrap the local env
    // with a CloudEnvWrapper, so its type has to be kNone.
    if (dest_bucket.empty()) {
      CloudEnvImpl* c = static_cast<CloudEnvImpl*>
                            (clone_db->GetBaseDB()->GetEnv());
      ASSERT_TRUE(c->GetCloudType() == CloudType::kNone);
    }
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

  void SetPersistentCache(const std::string& path, uint64_t size_gb) {
    persistent_cache_path_ = path;
    persistent_cache_size_gb_ = size_gb;
  }

 protected:
  Env* base_env_;
  Options options_;
  std::string dbname_;
  std::string clone_dir_;
  std::string src_bucket_prefix_;
  std::string src_object_prefix_;
  std::string dest_bucket_prefix_;
  std::string dest_object_prefix_;
  CloudEnvOptions cloud_env_options_;
  std::string region_;
  std::string dbid_;
  std::string persistent_cache_path_;
  uint64_t persistent_cache_size_gb_;
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
TEST_F(CloudTest, Newdb) {
  std::string master_dbid;
  std::string newdb1_dbid;
  std::string newdb2_dbid;

  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  ASSERT_OK(db_->GetDbIdentity(master_dbid));
  CloseDB();
  value.clear();

  {
    // Create and Open  a new instance
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("newdb1", src_bucket_prefix_, src_object_prefix_,
            dest_bucket_prefix_, dest_object_prefix_, &cloud_db, &cloud_env);

    // Retrieve the id of the first reopen
    ASSERT_OK(cloud_db->GetDbIdentity(newdb1_dbid));

    // This reopen has the same src and destination paths, so it is
    // not a clone, but just a reopen.
    ASSERT_EQ(newdb1_dbid, master_dbid);

    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);

    // Open master and write one more kv to it. The dest bukcet is emty,
    // so writes go to local dir only.
    OpenDB();
    ASSERT_OK(db_->Put(WriteOptions(), "Dhruba", "Borthakur"));

    // check that the newly written kv exists
    value.clear();
    ASSERT_OK(db_->Get(ReadOptions(), "Dhruba", &value));
    ASSERT_TRUE(value.compare("Borthakur") == 0);

    // check that the earlier kv exists too
    value.clear();
    ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);
    CloseDB();

    // Assert  that newdb1 cannot see the second kv because the second kv
    // was written to local dir only.
    ASSERT_TRUE(cloud_db->Get(ReadOptions(), "Dhruba", &value).IsNotFound());
  }
  {
    // Create another instance using a different local dir but the same two
    // buckets as newdb1. This should be identical in contents with newdb1.
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("newdb2", src_bucket_prefix_, src_object_prefix_,
            dest_bucket_prefix_, dest_object_prefix_, &cloud_db, &cloud_env);

    // Retrieve the id of the second clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb2_dbid));

    // Since we used the same src and destination buckets & paths for both
    // newdb1 and newdb2, we should get the same dbid as newdb1
    ASSERT_EQ(newdb1_dbid, newdb2_dbid);

    // check that both the kvs appear in the clone
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Dhruba", &value));
    ASSERT_TRUE(value.compare("Borthakur") == 0);
  }
}

//
// Create and read from a clone.
//
TEST_F(CloudTest, TrueClone) {
  std::string master_dbid;
  std::string newdb1_dbid;
  std::string newdb2_dbid;
  std::string newdb3_dbid;

  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  ASSERT_OK(db_->GetDbIdentity(master_dbid));
  CloseDB();
  value.clear();
  {
    // Create a new instance with different src and destination paths.
    // This is true clone and should have all the contents of the masterdb
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath1", src_bucket_prefix_, src_object_prefix_,
            src_bucket_prefix_, "clone1_path", &cloud_db, &cloud_env);

    // Retrieve the id of the clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb1_dbid));

    // Since we used the different src and destination paths for both
    // the master and clone1, the clone should have its own identity.
    ASSERT_NE(master_dbid, newdb1_dbid);

    // check that the original kv appears in the clone
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);

    // write a new value to the clone
    ASSERT_OK(cloud_db->Put(WriteOptions(), "Hello", "Clone1"));
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("Clone1") == 0);
  }
  {
    // Reopen clone1 with a different local path
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath2", src_bucket_prefix_, src_object_prefix_,
            src_bucket_prefix_, "clone1_path", &cloud_db, &cloud_env);

    // Retrieve the id of the clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb2_dbid));
    ASSERT_EQ(newdb1_dbid, newdb2_dbid);
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("Clone1") == 0);
  }
  {
    // Create clone2
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath3",  // xxx try with localpath2
            src_bucket_prefix_, src_object_prefix_, src_bucket_prefix_,
            "clone2_path", &cloud_db, &cloud_env);

    // Retrieve the id of the clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb3_dbid));
    ASSERT_NE(newdb2_dbid, newdb3_dbid);

    // verify that data is still as it was in the original db.
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);
  }
}

//
// verify that dbid registry is appropriately handled
//
TEST_F(CloudTest, DbidRegistry) {
  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);

  // Assert that there is one db in the registry
  while (true) {
    DbidList dbs;
    ASSERT_OK(aenv_->GetDbidList(src_bucket_prefix_, &dbs));
    if (dbs.size() == 0) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  CloseDB();
}

TEST_F(CloudTest, KeepLocalFiles) {
  cloud_env_options_.keep_local_sst_files = true;
  // Create two files
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "Hello2", "World2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  CloseDB();

  DestroyDB(dbname_, Options());

  OpenDB();

  std::vector<std::string> files;
  ASSERT_OK(Env::Default()->GetChildren(dbname_, &files));
  int sst_files =
      std::count_if(files.begin(), files.end(), [](const std::string& file) {
        return file.find("sst") != std::string::npos;
      });
  ASSERT_EQ(sst_files, 2);

  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");
  ASSERT_OK(db_->Get(ReadOptions(), "Hello2", &value));
  ASSERT_EQ(value, "World2");
  CloseDB();
}

TEST_F(CloudTest, CopyToFromS3) {
  std::string fname = dbname_ + "/100000.sst";

  // Create aws env
  cloud_env_options_.keep_local_sst_files = true;
  OpenDB();
  ASSERT_NE(aenv_, nullptr);
  char buffer[1 * 1024 * 1024];

  // create a 10 MB file and upload it to cloud
  {
    unique_ptr<WritableFile> writer;
    ASSERT_OK(aenv_->NewWritableFile(fname, &writer, EnvOptions()));

    for (int i = 0; i < 10; i++) {
      ASSERT_OK(writer->Append(Slice(buffer, sizeof(buffer))));
    }
    // sync and close file
  }

  // delete the file manually.
  ASSERT_OK(base_env_->DeleteFile(fname));

  // reopen file for reading. It should be refetched from cloud storage.
  {
    unique_ptr<RandomAccessFile> reader;
    ASSERT_OK(aenv_->NewRandomAccessFile(fname, &reader, EnvOptions()));

    uint64_t offset = 0;
    for (int i = 0; i < 10; i++) {
      Slice result;
      char* scratch = &buffer[0];
      ASSERT_OK(reader->Read(offset, sizeof(buffer), &result, scratch));
      ASSERT_EQ(result.size(), sizeof(buffer));
      offset += sizeof(buffer);
    }
  }
  CloseDB();
}

TEST_F(CloudTest, DelayFileDeletion) {
  std::string fname = dbname_ + "/igor.sst";

  // Create aws env
  cloud_env_options_.keep_local_sst_files = true;
  CreateAwsEnv();
  ((AwsEnv*)aenv_)->TEST_SetFileDeletionDelay(std::chrono::seconds(2));

  // create a file
  {
    unique_ptr<WritableFile> writer;
    ASSERT_OK(aenv_->NewWritableFile(fname, &writer, EnvOptions()));

    for (int i = 0; i < 10; i++) {
      ASSERT_OK(writer->Append("igor"));
    }
    // sync and close file
  }
  // delete the file
  ASSERT_OK(aenv_->DeleteFile(fname));

  // file should still be there
  ASSERT_OK(aenv_->FileExists(fname));

  // file should be deleted after 2 seconds
  std::this_thread::sleep_for(std::chrono::seconds(3));
  auto st = aenv_->FileExists(fname);
  ASSERT_TRUE(st.IsNotFound());
}


#ifdef AWS_DO_NOT_RUN
//
// Verify that we can cache data from S3 in persistent cache.
//
TEST_F(CloudTest, PersistentCache) {
  std::string pcache = test::TmpDir() + "/persistent_cache";
  SetPersistentCache(pcache, 1);

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
#endif /* AWS_DO_NOT_RUN */

}  //  namespace rocksdb

// A black-box test for the cloud wrapper around rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else  // USE_AWS

#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr,
          "SKIPPED as DBCloud is supported only when USE_AWS is defined.\n");
  return 0;
}
#endif

#else  // ROCKSDB_LITE

#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr, "SKIPPED as DBCloud is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
