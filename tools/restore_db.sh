#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
#

if [ "$#" -lt 2 ]; then
  echo "usage: ${BASH_SOURCE[0]} <Backup Dir> <DB Path>"
  exit 1
fi

backup_dir="$1"
db_dir="$2"

echo "== Restoring latest from $backup_dir to $db_dir"
./ldb restore --db="$db_dir" --backup_dir="$backup_dir"
