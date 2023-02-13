#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
#

if [ "$#" -lt 2 ]; then
  echo "usage: ${BASH_SOURCE[0]} <DB Path> <Backup Dir>"
  exit 1
fi

db_dir="$1"
backup_dir="$2"

echo "== Backing up DB $db_dir to $backup_dir"
./ldb backup --db="$db_dir" --backup_dir="$backup_dir"
