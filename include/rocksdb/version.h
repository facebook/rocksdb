// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <string>
#include <unordered_map>

#include "rocksdb/rocksdb_namespace.h"

#define ROCKSDB_MAJOR 6
#define ROCKSDB_MINOR 26
#define ROCKSDB_PATCH 0

// Do not use these. We made the mistake of declaring macros starting with
// double underscore. Now we have to live with our choice. We'll deprecate these
// at some point
#define __ROCKSDB_MAJOR__ ROCKSDB_MAJOR
#define __ROCKSDB_MINOR__ ROCKSDB_MINOR
#define __ROCKSDB_PATCH__ ROCKSDB_PATCH

namespace ROCKSDB_NAMESPACE {
// Structure containing the values that represent a RocksDB version
struct RocksVersion {
  uint8_t major = ROCKSDB_MAJOR;
  uint8_t minor = ROCKSDB_MINOR;
  uint8_t patch = ROCKSDB_PATCH;

  // Compares the current version to "other" version returning:
  //   > 0, if the "other" version is older than the current one;
  //   < 0, if the "other" version is newer than the current one;
  //   = 0, if the versions are equivalent.
  // If "with_patch" is false, only the major and minor versions are compared;
  // If "with_patch" is true, the patch version is also compared.
  int Compare(const RocksVersion& other, bool with_patch = false) const;

  // Converts the version to a string of the form major.minor.patch.
  // If with_patch is true, the patch release info is included;
  // If with_patch is false, only the major and minor version is returned
  std::string AsString(bool with_patch = true) const;
};

// Returns a set of properties indicating how/when/where this version of RocksDB
// was created.
const std::unordered_map<std::string, std::string>& GetRocksBuildProperties();

// Returns the current version of RocksDB as a string (e.g. "6.16.0").
// If with_patch is true, the patch is included (6.16.x).
// Otherwise, only major and minor version is included (6.16)
std::string GetRocksVersionAsString(bool with_patch = true);

// Gets the set of build properties (@see GetRocksBuildProperties) into a
// string. Properties are returned one-per-line, with the first line being:
// "<program> from RocksDB <version>.
// If verbose is true, the full set of properties is
// printed. If verbose is false, only the version information (@see
// GetRocksVersionString) is printed.
std::string GetRocksBuildInfoAsString(const std::string& program,
                                      bool verbose = false);
}  // namespace ROCKSDB_NAMESPACE
