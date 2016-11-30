//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <string>
#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace rocksdb {
// EXPERIMENAL ONLY
// A wrapped database which puts values of KV pairs in a separate log
// and store location to the log in the underlying DB.
// It lacks lots of importatant functionalities, e.g. DB restarts,
// garbage collection, iterators, etc.
//
// The factory needs to be moved to include/rocksdb/utilities to allow
// users to use blob DB.
extern Status NewBlobDB(Options options, std::string dbname, DB** blob_db);
}  // namespace rocksdb
