// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "rocksdb/slice.h"
#include <string>

namespace rocksdb {

// Column family's name is translated to ColumnFamilyHandle at DB open or column
// family open time. Clients use ColumnFamilyHandle to comunicate with the DB
//
// Column family names that start with "." (a dot) are system specific and
// should not be used by the clients

struct ColumnFamilyHandle {
  uint32_t id;
  // default
  ColumnFamilyHandle() : id() {}
  explicit ColumnFamilyHandle(uint32_t _id) : id(_id) {}
};

const ColumnFamilyHandle default_column_family = ColumnFamilyHandle();
extern const std::string default_column_family_name;

}  // namespace rocksdb
