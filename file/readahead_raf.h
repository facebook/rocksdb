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

namespace ROCKSDB_NAMESPACE {
// This file provides the following main abstractions:
// SequentialFileReader : wrapper over Env::SequentialFile
// RandomAccessFileReader : wrapper over Env::RandomAccessFile
// WritableFileWriter : wrapper over Env::WritableFile
// In addition, it also exposed NewReadaheadRandomAccessFile, NewWritableFile,
// and ReadOneLine primitives.

// NewReadaheadRandomAccessFile provides a wrapper over RandomAccessFile to
// always prefetch additional data with every read. This is mainly used in
// Compaction Table Readers.
std::unique_ptr<RandomAccessFile> NewReadaheadRandomAccessFile(
    std::unique_ptr<RandomAccessFile>&& file, size_t readahead_size);
}  // namespace ROCKSDB_NAMESPACE
