#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
# If clang_format_diff.py command is not specfied, we assume we are able to
# access directly without any path.

TGT_DIFF=`git diff TARGETS | head -n 1`

if [ ! -z "$TGT_DIFF" ]
then
  echo "TARGETS file has uncommitted changes. Skip this check."
  exit 0
fi

echo Backup original TARGETS file.

cp TARGETS TARGETS.bkp

${PYTHON:-python3} buckifier/buckify_rocksdb.py

TGT_DIFF=`git diff TARGETS | head -n 1`

if [ -z "$TGT_DIFF" ]
then
  mv TARGETS.bkp TARGETS
  exit 0
else
  echo "Please run '${PYTHON:-python3} buckifier/buckify_rocksdb.py' to update TARGETS file."
  echo "Do not manually update TARGETS file."
  ${PYTHON:-python3} --version
  mv TARGETS.bkp TARGETS
  exit 1
fi
