// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_BUILDER_H_
#define STORAGE_LEVELDB_DB_BUILDER_H_

#include "rocksdb/comparator.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace leveldb {

struct Options;
struct FileMetaData;

class Env;
class EnvOptions;
class Iterator;
class TableCache;
class VersionEdit;

// Build a Table file from the contents of *iter.  The generated file
// will be named according to meta->number.  On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
extern Status BuildTable(const std::string& dbname,
                         Env* env,
                         const Options& options,
                         const EnvOptions& soptions,
                         TableCache* table_cache,
                         Iterator* iter,
                         FileMetaData* meta,
                         const Comparator* user_comparator,
                         const SequenceNumber newest_snapshot,
                         const SequenceNumber earliest_seqno_in_memtable);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_BUILDER_H_
