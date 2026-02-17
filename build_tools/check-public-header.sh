#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates. All Rights Reserved.
#
# Check for some simple mistakes in public headers (on the command line)
# that should prevent commit or push

BAD=""

# Look for potential for ODR violations caused by public headers depending on
# build parameters that could vary between RocksDB build and application build.
# * Cases like ROCKSDB_NAMESPACE, and ROCKSDB_ASSERT_STATUS_CHECKED are
#   intentional, hard to avoid. (We expect definitions to change and the user
#   should also.)
# * Cases like _WIN32, OS_WIN, and __cplusplus are essentially ODR-safe.
# * Cases like
#   #ifdef BLAH  // ODR-SAFE
#   #undef BLAH
#   #endif
#   that should not cause ODR violations can be exempted with the ODR-SAFE
#   marker recognized here.

grep -nHE '^#if' -- "$@" | grep -vE 'ROCKSDB_NAMESPACE|ROCKSDB_ASSERT_STATUS_CHECKED|_WIN32|OS_WIN|ODR-SAFE|__cplusplus|ROCKSDB_DLL|ROCKSDB_LIBRARY_EXPORTS'
if [ "$?" != "1" ]; then
  echo "^^^^^ #if in public API could cause an ODR violation."
  echo "      Add // ODR-SAFE if verified safe."
  BAD=1
fi

if [ "$BAD" ]; then
  exit 1
fi
