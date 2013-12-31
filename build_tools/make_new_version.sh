#!/bin/bash
#  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree. An additional grant
#  of patent rights can be found in the PATENTS file in the same directory.

set -e
# Print out the colored progress info so that it can be brainlessly 
# distinguished by users.
function title() {
  echo -e "\033[1;32m$*\033[0m"
}

usage="Create new rocksdb version and prepare it for the release process\n"
usage+="USAGE: ./make_new_version.sh <version>"

# -- Pre-check
if [[ $# < 1 ]]; then
  echo -e $usage
  exit 1
fi

ROCKSDB_VERSION=$1

GIT_BRANCH=`git rev-parse --abbrev-ref HEAD`
if [ $GIT_BRANCH != "master" ]; then
  echo "Error: Current branch is '$GIT_BRANCH', Please switch to master branch."
fi

# --Step 1: cutting new tag
title "Adding new tag for this release ..."
git tag -a "$ROCKSDB_VERSION.fb" -m "Rocksdb $ROCKSDB_VERSION"

# Setting up the proxy for remote repo access
export http_proxy=http://172.31.255.99:8080
export https_proxy="$http_proxy";

title "Pushing new tag to remote repo ..."
proxycmd.sh git push origin --tags

# --Step 2: Update README.fb
title "Updating the latest version info in README.fb ..."
sed -i "s/Latest release is [0-9]\+.[0-9]\+.fb/Latest release is $ROCKSDB_VERSION.fb/" README.fb
git commit README.fb -m "update the latest version in README.fb to $ROCKSDB_VERSION"
proxycmd.sh git push

# --Step 3: Prepare this repo for 3rd release
title "Cleaning up repo ..."
make clean
git clean -fxd

title "Generating the build info ..."
# Comment out the call of `build_detection_version` so that the SHA number and build date of this
# release will remain constant. Otherwise everytime we run "make" util/build_version.cc will be 
# overridden.
sed -i 's/^\$PWD\/build_tools\/build_detect_version$//' build_tools/build_detect_platform

# Generate util/build_version.cc
build_tools/build_detect_version

title "Done!"
