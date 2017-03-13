## RocksDB-Cloud on Amazon Web Services (AWS)

This directory contains the extensions needed to make rocksdb store
files in AWS environment.

The compilation process assumes that the AWS c++ SDK is installed in
the default location of /usr/local. You can follow the steps listed
here https://github.com/aws/aws-sdk-cpp to install the c++ AWS sdk.

If you want to compile rocksdb with AWS support, please set the following
environment variables:

   USE_AWS=1
   make clean all db_bench

Here is an [example](https://github.com/rockset/rocksdb-cloud/blob/master/cloud/examples/cloud_durable_example.cc)  of code that uses rockdb-cloud.

The cloud unit tests need a AWS S3 bucket to store files. Please set the
following environment variables to run the cloud unit tests:

AWS_ACCESS_KEY_ID     : your aws access credentials
AWS_SECRET_ACCESS_KEY : your secret key
AWS_BUCKET_NAME       : the name of your S3 test bucket

To run dbbench,
   db_bench --env_uri="s3://" --aws_access_id=xxx and --aws_secret_key=yyy



