// Copyright (c) 2017-present, Rockset, Inc.  All rights reserved.
#include <cstdio>
#include <iostream>
#include <string>

#include "rocksdb/options.h"
#include "rocksdb/cloud/db_cloud.h"

using namespace rocksdb;

// This is the local directory where the db is stored. The same
// path name is used to store data inside the specified cloud
// storage bucket.
std::string kDBPath = "/tmp/rocksdb_main_db";

// This is the local directory where the clone is stored. The same
// pathname is used to store data in the specified cloud bucket.
std::string kClonePath = "/tmp/rocksdb_clone_db";

//
// This is the name of the cloud storage bucket where the db
// is made durable. If you are using AWS, you have to manually
// ensure that this bucket name is unique to you and does not
// conflict with any other S3 users who might have already created
// this bucket name.
// In this example, the database and its clone are both stored in
// the same bucket (obviously with different pathnames).
//
std::string kBucketSuffix = "cloud.clone.example.";

//
// Creates and Opens a clone
//
Status CloneDB(const std::string& clone_name,
               const std::string& src_bucket,
	       const std::string& src_object_path,
	       const std::string& dest_bucket,
	       const std::string& dest_object_path,
	       const CloudEnvOptions& cloud_env_options,
	       std::unique_ptr<DB>* cloud_db,
	       std::unique_ptr<CloudEnv>* cloud_env) {

  // The local directory where the clone resides
  std::string cname =  kClonePath + "/" + clone_name;

  // Create new AWS env
  CloudEnv* cenv;
  Status st = CloudEnv::NewAwsEnv(Env::Default(),
			          src_bucket,
			          src_object_path,
			          dest_bucket,
			          dest_object_path,
		                  cloud_env_options,
				  nullptr,
				  &cenv);
  if (!st.ok()) {
    fprintf(stderr, "Unable to create an AWS environment with "
            "bucket %s",
	    src_bucket.c_str());
    return st;
  }
  cloud_env->reset(cenv);

  // Create options and use the AWS env that we created earlier
  Options options;
  options.env = cloud_env->get();

  // open clone
  DB* db;
  st = DBCloud::Open(options, kClonePath, &db);
  if (!st.ok()) {
    fprintf(stderr, "Unable to open clone at path %s with bucket %s. %s\n",
            kClonePath.c_str(), kBucketSuffix.c_str(), st.ToString().c_str());
    return st;
  }
  cloud_db->reset(db);
  return Status::OK();
}

int main() {
  // cloud environment config options here
  CloudEnvOptions cloud_env_options;

  // Store a reference to a cloud env. A new cloud env object should be associated
  // with every new cloud-db.
  std::unique_ptr<CloudEnv> cloud_env;

  // Retrieve aws access keys from env
  char* keyid = getenv("AWS_ACCESS_KEY_ID");
  char* secret = getenv("AWS_SECRET_ACCESS_KEY");
  if (keyid == nullptr || secret == nullptr) {
    fprintf(stderr, "Please set env variables "
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY with cloud credentials");
    return -1;
  }
  cloud_env_options.credentials.access_key_id.assign(keyid);
  cloud_env_options.credentials.secret_key.assign(secret);
  cloud_env_options.region = "us-west-2";

  // Append the user name to the bucket name in an attempt to make it
  // globally unique. S3 bucket-namess need to be goblly unique.
  // If you want to rerun this example, then unique user-name suffix here.
  char* user = getenv("USER");
  kBucketSuffix.append(user);

  // Create a new AWS cloud env Status
  CloudEnv* cenv;
  Status s = CloudEnv::NewAwsEnv(Env::Default(),
                                 kBucketSuffix,
                                 kDBPath,
                                 kBucketSuffix,
                                 kDBPath,
                                 cloud_env_options,
                                 nullptr,
                                 &cenv);
  if (!s.ok()) {
    fprintf(stderr, "Unable to create cloud env bucket suffix %s. %s\n",
            kBucketSuffix.c_str(), s.ToString().c_str());
    return -1;
  }
  cloud_env.reset(cenv);

  // Create options and use the AWS env that we created earlier
  Options options;
  options.env = cloud_env.get();
  options.create_if_missing = true;

  // open DB
  DB* db;
  s = DBCloud::Open(options, kDBPath, &db);
  if (!s.ok()) {
    fprintf(stderr, "Unable to open db at path %s with bucket %s. %s\n",
            kDBPath.c_str(), kBucketSuffix.c_str(), s.ToString().c_str());
    return -1;
  }

  // Put key-value into main db
  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;

  // get value from main db
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // Flush all data from main db to sst files.
  db->Flush(FlushOptions());

  // create a clone of the db and and verify that all's well
  std::unique_ptr<DB> clone_db;
  std::unique_ptr<CloudEnv> clone_env;

  s = CloneDB("clone1",
               kBucketSuffix,
	       kDBPath,
	       kBucketSuffix,
	       kClonePath,
	       cloud_env_options,
	       &clone_db,
	       &clone_env);
  if (!s.ok()) {
    fprintf(stderr, "Unable to clone db at path %s with bucket %s. %s\n",
            kDBPath.c_str(), kBucketSuffix.c_str(), s.ToString().c_str());
    return -1;
  }

  // insert a key-value in the clone.
  s = clone_db->Put(WriteOptions(), "name", "dhruba");
  assert(s.ok());


  // assert that values from the main db appears in the clone
  s = clone_db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // assert that the write to the clone does not appear in the main db
  s = db->Get(ReadOptions(), "name", &value);
  assert(s.IsNotFound());
	       
  // Flush all data from clone db to sst files. Release clone.
  clone_db->Flush(FlushOptions());
  clone_db.release();
  delete db;

  fprintf(stdout, "Successfully used db at %s and clone at %s in bucket %s.\n",
          kDBPath.c_str(), kClonePath.c_str(), kBucketSuffix.c_str());
  return 0;
}
