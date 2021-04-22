// Copyright (c) 2019 Rockset

#ifndef ROCKSDB_LITE

#ifdef USE_AWS

#include <algorithm>
#include <chrono>
#include <cinttypes>

#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "db/db_impl/db_impl.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "test_util/testharness.h"
#include "util/string_util.h"
#ifndef OS_WIN
#include <unistd.h>
#endif

namespace ROCKSDB_NAMESPACE {

class RemoteCompactionTest : public testing::Test {
 public:
  RemoteCompactionTest() {
    base_env_ = Env::Default();
    dbname_ = test::TmpDir() + "/db_cloud";
    clone_dir_ = test::TmpDir() + "/ctest";
    cloud_env_options_.TEST_Initialize("dbcloud.", dbname_);
    cloud_env_options_.keep_local_sst_files = true;

    options_.create_if_missing = true;
    options_.create_missing_column_families = true;
    persistent_cache_path_ = "";
    persistent_cache_size_gb_ = 0;
    db_ = nullptr;

    DestroyDir(dbname_);
    base_env_->CreateDirIfMissing(dbname_);
    base_env_->NewLogger(test::TmpDir(base_env_) + "/rocksdb-cloud.log",
                         &options_.info_log);
    options_.info_log->SetInfoLogLevel(InfoLogLevel::DEBUG_LEVEL);

    Cleanup();
  }

  void Cleanup() {
    ASSERT_TRUE(!aenv_);

    // check cloud credentials
    ASSERT_TRUE(cloud_env_options_.credentials.HasValid().ok());

    CloudEnv* aenv;
    // create a dummy aws env
    ASSERT_OK(CloudEnv::NewAwsEnv(base_env_, cloud_env_options_,
                                  options_.info_log, &aenv));
    aenv_.reset(aenv);
    // delete all pre-existing contents from the bucket
    Status st =
        aenv_->GetStorageProvider()->EmptyBucket(aenv_->GetSrcBucketName(), "");
    ASSERT_TRUE(st.ok() || st.IsNotFound());
    aenv_.reset();

    // delete and create directory where clones reside
    DestroyDir(clone_dir_);
    ASSERT_OK(base_env_->CreateDir(clone_dir_));
  }

  std::set<std::string> GetSSTFiles(std::string name) {
    std::vector<std::string> files;
    aenv_->GetBaseEnv()->GetChildren(name, &files);
    std::set<std::string> sst_files;
    for (auto& f : files) {
      if (IsSstFile(RemoveEpoch(f))) {
        sst_files.insert(f);
      }
    }
    return sst_files;
  }

  void DestroyDir(const std::string& dir) {
    std::string cmd = "rm -rf " + dir;
    int rc = system(cmd.c_str());
    ASSERT_EQ(rc, 0);
  }

  virtual ~RemoteCompactionTest() { CloseDB(); }

  void CreateCloudEnv() {
    CloudEnv* cenv;
    ASSERT_OK(CloudEnv::NewAwsEnv(base_env_, cloud_env_options_,
                                  options_.info_log, &cenv));
    // To catch any possible file deletion bugs, we set file deletion delay to
    // smallest possible
    CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(cenv);
    cimpl->TEST_SetFileDeletionDelay(std::chrono::seconds(0));
    aenv_.reset(cenv);
  }

  // Open database via the cloud interface
  void OpenDB() {
    ASSERT_TRUE(cloud_env_options_.credentials.HasValid().ok());

    // Create new cloud env
    CreateCloudEnv();
    options_.env = aenv_.get();

    // default column family
    ColumnFamilyOptions cfopt = options_;
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(
        ColumnFamilyDescriptor(kDefaultColumnFamilyName, cfopt));
    column_families.emplace_back(ColumnFamilyDescriptor("T", cfopt));

    ASSERT_TRUE(db_ == nullptr);
    ASSERT_OK(DBCloud::Open(options_, dbname_, column_families,
                            persistent_cache_path_, persistent_cache_size_gb_,
                            &cf_handles_, &db_));
    ASSERT_OK(db_->GetDbIdentity(dbid_));
    db_->DisableFileDeletions();

    // Delete the handle for the default column family because the DBImpl
    // always holds a reference to it.
    ASSERT_TRUE(cf_handles_.size() > 0);
  }

  // returns the name of the local directory where this clone resides
  // e.g /tmp/rocksdbtest-10001/ctest/localpath1/
  std::string GetCloneLocalDir(const std::string& clone_name) {
    return clone_dir_ + "/" + clone_name;
  }

  // Creates and Opens a clone
  void CloneDB(const std::string& clone_local_dir,
               const std::string& dest_bucket_name,
               const std::string& dest_object_path,
               std::unique_ptr<DBCloud>* cloud_db,
               std::unique_ptr<CloudEnv>* cloud_env) {
    CloudEnv* cenv;
    DBCloud* clone_db;

    // If there is no destination bucket, then the clone needs to copy
    // all sst fies from source bucket to local dir
    CloudEnvOptions copt = cloud_env_options_;
    if (dest_bucket_name == copt.src_bucket.GetBucketName()) {
      copt.dest_bucket = copt.src_bucket;
    } else {
      copt.dest_bucket.SetBucketName(dest_bucket_name);
    }
    copt.dest_bucket.SetObjectPath(dest_object_path);
    if (!copt.dest_bucket.IsValid()) {
      copt.keep_local_sst_files = true;
    }
    // Create new AWS env
    ASSERT_OK(CloudEnv::NewAwsEnv(base_env_, copt, options_.info_log, &cenv));
    // To catch any possible file deletion bugs, we set file deletion delay to
    // smallest possible
    CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(cenv);
    cimpl->TEST_SetFileDeletionDelay(std::chrono::seconds(0));
    // sets the cloud env to be used by the env wrapper
    options_.env = cenv;

    // Returns the cloud env that was created
    cloud_env->reset(cenv);

    // default column family
    ColumnFamilyOptions cfopt = options_;

    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(
        ColumnFamilyDescriptor(kDefaultColumnFamilyName, cfopt));
    column_families.emplace_back(ColumnFamilyDescriptor("T", cfopt));
    std::vector<ColumnFamilyHandle*> handles;

    ASSERT_OK(DBCloud::Open(options_, clone_local_dir, column_families,
                            persistent_cache_path_, persistent_cache_size_gb_,
                            &handles, &clone_db));
    clone_db->DisableFileDeletions();
    cloud_db->reset(clone_db);

    // Delete the handle for the default column family because the DBImpl
    // always holds a reference to it.
    ASSERT_TRUE(handles.size() > 0);
    delete handles[0];
    delete handles[1];
  }

  void CloseDB() {
    if (db_) {
      db_->Flush(FlushOptions());  // convert pending writes to sst files

      // Delete the column families first before deleting the db.
      for (auto h : cf_handles_) {
        delete h;
      }
      cf_handles_.clear();

      delete db_;
      db_ = nullptr;
    }
  }

  Status GetCloudLiveFilesSrc(std::set<uint64_t>* list) {
    std::unique_ptr<ManifestReader> manifest(new ManifestReader(
        options_.info_log, aenv_.get(), aenv_->GetSrcBucketName()));
    return manifest->GetLiveFiles(aenv_->GetSrcObjectPath(), list);
  }

  // Returns the DBImpl of the underlying rocksdb instance
  DBImpl* GetDBImpl() {
    DB* rocksdb = db_->GetBaseDB();
    return dynamic_cast<DBImpl*>(rocksdb);
  }

  /**
   * define a pluggable compaction service. It accepts compaction requests from
   * the DB and then invoke's the DB's ExecuteRemoteCompactionReques to execute
   * it.
   */
  class TestPluggableCompactionService : public PluggableCompactionService {
   public:
    TestPluggableCompactionService(CloudEnv* _cloud_env, DB* _clone) {
      clone = _clone;
      cloud_env = static_cast<CloudEnvImpl*>(_cloud_env);
    }
    ~TestPluggableCompactionService() {}

    // Run the remote compaction on a clone database
    Status Run(const PluggableCompactionParam& job,
               PluggableCompactionResult* result) override {
      return clone->ExecuteRemoteCompactionRequest(job, result, false);
    }

    std::vector<Status> InstallFiles(
        const std::vector<std::string>& remote_paths,
        const std::vector<std::string>& local_paths,
        const EnvOptions& env_options, Env* local_env) override {
      assert(remote_paths.size() == local_paths.size());
      std::vector<Status> statuses;
      for (uint32_t i = 0; i < remote_paths.size(); ++i) {
        statuses.push_back(InstallFile(remote_paths[i], local_paths[i],
                                       env_options, local_env));
      }

      return statuses;
    }

    // Install the remote file into the local db
    Status InstallFile(const std::string& remote_path,
                       const std::string& destination_path,
                       const EnvOptions& env_options, Env* local_env) {
      // create destination file
      std::unique_ptr<WritableFile> writable_file;
      Status status = local_env->NewWritableFile(destination_path,
                                                 &writable_file, env_options);
      if (!status.ok()) {
        return status;
      }

      // open source file
      std::unique_ptr<SequentialFile> readable_file;
      status = local_env->NewSequentialFile(remote_path, &readable_file,
                                            env_options);
      if (!status.ok()) {
        return status;
      }

      // copy contents of source file  to destination file
      Slice result;
      char scratch[16 * 1024];
      while (true) {
        status = readable_file->Read(sizeof(scratch), &result, &scratch[0]);
        if (!status.ok()) {
          return status;
        }
        if (result.size() == 0) {
          break;
        }
        status = writable_file->Append(result);
        if (!status.ok()) {
          return status;
        }
      }
      writable_file->Fsync();
      writable_file->Close();
      return status;
    }

   private:
    CloudEnvImpl* cloud_env;
    DB* clone;
  };

  // Wire up all compaction requests through our pluggable service
  Status SetupPluggableCompaction(CloudEnv* cloud_env, DB* clone) {
    // create a service object
    std::unique_ptr<PluggableCompactionService> service;
    service.reset(new TestPluggableCompactionService(cloud_env, clone));

    // Setup our local DB to invoke a custom service. This will ensure that
    // all compaction requests will flow through the service object. The service
    // object forwards the compaction requests to the clone.
    Status status = db_->RegisterPluggableCompactionService(std::move(service));
    Log(options_.info_log, "Setup pluggable compaction %s",
        status.ToString().c_str());
    return status;
  }

  void CleanupPluggableCompaction() {
    db_->UnRegisterPluggableCompactionService();
  }

 protected:
  Env* base_env_;
  Options options_;
  std::string dbname_;
  std::string clone_dir_;
  CloudEnvOptions cloud_env_options_;
  std::string dbid_;
  std::string persistent_cache_path_;
  uint64_t persistent_cache_size_gb_;
  DBCloud* db_;
  std::unique_ptr<CloudEnv> aenv_;
  std::vector<ColumnFamilyHandle*> cf_handles_;
};

//
// Most basic test. Create DB, write two keys into two L0 files.
// Create a clone and setup a compaction service so that compactions
// on the DB translates into a compaction request on the clone.
//
TEST_F(RemoteCompactionTest, BasicTest) {
  OpenDB();

  std::string value;

  // create two files on storage
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "WorldOne"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "WorldNextGen"));
  ASSERT_OK(db_->Put(WriteOptions(), "Aurora", "Borealis"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // validate that there are 2 files
  std::vector<LiveFileMetaData> files;
  db_->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 2);

  {
    // local directory for a clone
    std::string clone_local_dir = GetCloneLocalDir("localpath1");

    // Create a new instance with different src and destination paths.
    // This is true clone and should have all the contents of the masterdb
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB(clone_local_dir, cloud_env_options_.src_bucket.GetBucketName(),
            "clone1_path", &cloud_db, &cloud_env);

    // check that the original kv appears in the clone
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("WorldNextGen") == 0);

    // Make all compactions flow through the clone database
    ASSERT_OK(SetupPluggableCompaction(cloud_env.get(), cloud_db.get()));

    // compact main db and do not allow trivial file moves
    ASSERT_OK(
        GetDBImpl()->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));

    // check that compact made it all into one file
    files.clear();
    db_->GetLiveFilesMetaData(&files);
    ASSERT_EQ(1, files.size());

    // validate
    value.clear();
    ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("WorldNextGen") == 0);
    value.clear();
    ASSERT_OK(db_->Get(ReadOptions(), "Aurora", &value));
    ASSERT_TRUE(value.compare("Borealis") == 0);

    // The clone db will get freed up at the end of this code block.
    // Cleanup the connection between the main db and the clone.
    CleanupPluggableCompaction();
  }
  CloseDB();
  value.clear();

  // Reopen main db and validate
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "WorldNextGen");
  ASSERT_OK(db_->Get(ReadOptions(), "Aurora", &value));
  ASSERT_EQ(value, "Borealis");

  std::set<uint64_t> live_files;
  ASSERT_OK(GetCloudLiveFilesSrc(&live_files));
  ASSERT_GT(live_files.size(), 0);
  CloseDB();
}

TEST_F(RemoteCompactionTest, ColumnFamilyTest) {
  OpenDB();

  std::string value;

  // create two files on storage
  ASSERT_OK(db_->Put(WriteOptions(), cf_handles_[1], "Hello", "WorldOne"));
  ASSERT_OK(db_->Flush(FlushOptions(), cf_handles_[1]));
  ASSERT_OK(db_->Put(WriteOptions(), cf_handles_[1], "Hello", "WorldNextGen"));
  ASSERT_OK(db_->Put(WriteOptions(), cf_handles_[1], "Aurora", "Borealis"));
  ASSERT_OK(db_->Flush(FlushOptions(), cf_handles_[1]));
  ASSERT_OK(db_->Put(WriteOptions(), "Default", "Abc"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // validate that there are 3 files, 2 for `T` column family, and 1 for default
  // column family
  std::vector<LiveFileMetaData> files;
  db_->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 3);

  {
    // local directory for a clone
    std::string clone_local_dir = GetCloneLocalDir("localpath1");

    // Create a new instance with different src and destination paths.
    // This is true clone and should have all the contents of the masterdb
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB(clone_local_dir, cloud_env_options_.src_bucket.GetBucketName(),
            "clone1_path", &cloud_db, &cloud_env);

    // check that the original kv appears in the clone
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), cf_handles_[1], "Hello", &value));
    ASSERT_TRUE(value.compare("WorldNextGen") == 0);

    // Make all compactions flow through the clone database
    ASSERT_OK(SetupPluggableCompaction(cloud_env.get(), cloud_db.get()));

    // compact main db and do not allow trivial file moves
    ASSERT_OK(GetDBImpl()->TEST_CompactRange(0, nullptr, nullptr,
                                             cf_handles_[1], true));

    // check that compact made it all into 1 file in T column family, and there
    // is 1 file for default column family
    files.clear();
    db_->GetLiveFilesMetaData(&files);
    ASSERT_EQ(2, files.size());
    std::sort(files.begin(), files.end(),
              [](const LiveFileMetaData& lhs, const LiveFileMetaData& rhs) {
                return lhs.column_family_name < rhs.column_family_name;
              });
    ASSERT_EQ(files[0].column_family_name, "T");
    ASSERT_EQ(files[1].column_family_name, "default");

    // validate
    value.clear();
    ASSERT_OK(db_->Get(ReadOptions(), cf_handles_[1], "Hello", &value));
    ASSERT_TRUE(value.compare("WorldNextGen") == 0);
    value.clear();
    ASSERT_OK(db_->Get(ReadOptions(), cf_handles_[1], "Aurora", &value));
    ASSERT_TRUE(value.compare("Borealis") == 0);

    // The clone db will get freed up at the end of this code block.
    // Cleanup the connection between the main db and the clone.
    CleanupPluggableCompaction();
  }
  CloseDB();
  value.clear();

  // Reopen main db and validate
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), cf_handles_[1], "Hello", &value));
  ASSERT_EQ(value, "WorldNextGen");
  ASSERT_OK(db_->Get(ReadOptions(), cf_handles_[1], "Aurora", &value));
  ASSERT_EQ(value, "Borealis");
  ASSERT_OK(db_->Get(ReadOptions(), "Default", &value));
  ASSERT_EQ(value, "Abc");

  std::set<uint64_t> live_files;
  ASSERT_OK(GetCloudLiveFilesSrc(&live_files));
  ASSERT_GT(live_files.size(), 0);
  CloseDB();
}

}  //  namespace ROCKSDB_NAMESPACE

// Run all pluggable compaction tests
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else  // USE_AWS

#include <stdio.h>

int main(int, char**) {
  fprintf(stderr,
          "SKIPPED as DBCloud is supported only when USE_AWS is defined.\n");
  return 0;
}
#endif

#else  // ROCKSDB_LITE

#include <stdio.h>

int main(int, char**) {
  fprintf(stderr, "SKIPPED as DBCloud is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
