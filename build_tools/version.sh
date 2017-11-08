#!/bin/sh
if [ "$#" = "0" ]; then
  echo "Usage: $0 major|minor|patch|full"
  exit 1
fi

if [ "$1" = "major" ]; then
  awk '/MAJOR/ {print $3; exit}' include/rocksdb/version.h
fi
if [ "$1" = "minor" ]; then
  awk '/MINOR/ {print $3; exit}' include/rocksdb/version.h
fi
if [ "$1" = "patch" ]; then
  awk '/PATCH/ {print $3; exit}' include/rocksdb/version.h
fi
if [ "$1" = "full" ]; then
  awk '/#define ROCKSDB/ { env[$2] = $3 }
       END { printf "%s.%s.%s\n", env["ROCKSDB_MAJOR"],
                                  env["ROCKSDB_MINOR"],
                                  env["ROCKSDB_PATCH"] }'  \
      include/rocksdb/version.h
fi
