## RocksDB-Cloud on Amazon Web Services (AWS)

This directory contains the extensions needed to make rocksdb store
files in AWS environment.

The compilation process assumes that the AWS c++ SDK is installed in
the default location of /usr/local.

If you want to compile rocksdb with AWS support, please set the following
environment variables 

   USE_AWS=1
   make clean all db_bench

To run dbbench,
   db_bench --env_uri="s3://" --aws_access_id=xxx and --aws_secret_key=yyy


