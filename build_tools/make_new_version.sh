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

title "Adding new tag for this release ..."
git tag -a "$ROCKSDB_VERSION.fb" -m "Rocksdb $ROCKSDB_VERSION"

# Setting up the proxy for remote repo access
export http_proxy=http://172.31.255.99:8080
export https_proxy="$http_proxy";

title "Pushing new tag to remote repo ..."
proxycmd.sh git push origin --tags

title "Done!"
