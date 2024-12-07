#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

TGT_DIFF=`git diff BUCK | head -n 1`

if [ ! -z "$TGT_DIFF" ]
then
  echo "BUCK file has uncommitted changes. Skip this check."
  exit 0
fi

echo Backup original BUCK file.

cp BUCK BUCK.bkp

${PYTHON:-python3} buckifier/buckify_rocksdb.py

if [[ ! -f "BUCK" ]]
then
  echo "buckifier/buckify_rocksdb.py was expected to (re)generate BUCK file."
  echo "BUCK file is missing!"
  exit 1
fi

TGT_DIFF=`git diff BUCK | head -n 1`

if [ -z "$TGT_DIFF" ]
then
  mv BUCK.bkp BUCK
  exit 0
else
  echo "Please run '${PYTHON:-python3} buckifier/buckify_rocksdb.py' to update BUCK file."
  echo "Do not manually update BUCK file."
  ${PYTHON:-python3} --version
  mv BUCK.bkp BUCK
  exit 1
fi
