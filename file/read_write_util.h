//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <atomic>
#include "rocksdb/env.h"

namespace rocksdb {
// Returns a WritableFile.
//
// env     : the Env.
// fname   : the file name.
// result  : output arg. A WritableFile based on `fname` returned.
// options : the Env Options.
extern Status NewWritableFile(Env* env, const std::string& fname,
                              std::unique_ptr<WritableFile>* result,
                              const EnvOptions& options);

// Read a single line from a file.
bool ReadOneLine(std::istringstream* iss, SequentialFile* seq_file,
                 std::string* output, bool* has_data, Status* result);

#ifndef NDEBUG
bool IsFileSectorAligned(const size_t off, size_t sector_size);
#endif  // NDEBUG
}  // namespace rocksdb
