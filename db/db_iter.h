// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>
#include "rocksdb/db.h"
#include "db/dbformat.h"

namespace rocksdb {

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
extern Iterator* NewDBIterator(
    const std::string* dbname,
    Env* env,
    const Options& options,
    const Comparator *user_key_comparator,
    Iterator* internal_iter,
    const SequenceNumber& sequence);

}  // namespace rocksdb
