#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

if [[ ! -f "BUCK" ]]
then
  echo "BUCK file is missing!"
  echo "Please do not remove / rename BUCK file in your commit(s)."
  exit 1
fi

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
  echo "BUCK file went missing after running buckifier/buckify_rocksdb.py!"
  echo "Please do not remove the BUCK file."
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
