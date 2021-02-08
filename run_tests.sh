#!/bin/bash

set -eu

args=$(getopt -o '+hj:' -l 'help,parallel_jobs:' -n "$0" -- "$@")
eval set -- "$args"

set +e
read -r -d '' usage_str <<END
Usage:
  $0 [options...]

Entry-point for the RocksDB-Cloud test script.

Options:
  -h, --help               Help
  -j, --parallel_jobs      Quiet
END
set -e

die_usage() {
  echo "$@" >&2
  echo >&2
  echo "$usage_str" >&2
  exit 1
}

while :; do
  case "$1" in
      -h|--help)
          echo "$usage_str"
          exit 0
          ;;
      -j|--parallel_jobs)
          PARALLEL_JOBS="$2"
          shift
          ;;
      --)
          shift
          break
          ;;
      *)
          die "Internal error: unknown option $1"
  esac
  shift
done

echo "Running with $PARALLEL_JOBS parallel jobs"

echo "Pulling base image..."
docker pull rockset/rocksdb_cloud_runtime:test

echo "Checking AWS access keys"
if [[ -z ${AWS_ACCESS_KEY_ID+x} ]]; then
  AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id) || die_error "AWS access key ID not found"
fi
if [[ -z ${AWS_SECRET_ACCESS_KEY+x} ]]; then
  AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key) || die_error "AWS secret access key not found"
fi

export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY

export SRC_ROOT=$(git rev-parse --show-toplevel)

echo $UID

echo "Building tests..."
docker run -v $SRC_ROOT:/opt/rocksdb-cloud/src -w /opt/rocksdb-cloud/src \
    -u $UID -e V=1 -e USE_AWS=1 -e USE_KAFKA=1 \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
    --rm rockset/rocksdb_cloud_runtime:test \
    /bin/bash -c "make -j4 db_test db_test2 db_basic_test env_basic_test db_cloud_test cloud_manifest_test"

echo "Running db_test. This test might take a while. Get some coffee :)"
docker run -v $SRC_ROOT:/opt/rocksdb-cloud/src -w /opt/rocksdb-cloud/src \
    -u $UID -e V=1 -e USE_AWS=1 -e USE_KAFKA=1 \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
    --rm rockset/rocksdb_cloud_runtime:test \
    /bin/bash -c "./db_test"

echo "Running db_test2, db_basic_test and env_basic_test"
docker run -v $SRC_ROOT:/opt/rocksdb-cloud/src -w /opt/rocksdb-cloud/src \
    -u $UID -e V=1 -e USE_AWS=1 -e USE_KAFKA=1 \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
    --rm rockset/rocksdb_cloud_runtime:test \
    /bin/bash -c "./db_test2 && ./db_basic_test && ./env_basic_test"

echo "Running cloud tests..."
docker run -v $SRC_ROOT:/opt/rocksdb-cloud/src -w /opt/rocksdb-cloud/src \
    -u $UID -e V=1 -e USE_AWS=1 -e USE_KAFKA=1 \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
    --rm rockset/rocksdb_cloud_runtime:test \
    /bin/bash -c "./cloud_manifest_test && ./db_cloud_test --gtest_filter=-CloudTest.KeepLocalLogKafka"
