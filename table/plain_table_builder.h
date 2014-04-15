// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// IndexedTable is a simple table format for UNIT TEST ONLY. It is not built
// as production quality.

#ifndef ROCKSDB_LITE
#pragma once
#include <stdint.h>
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/table_builder.h"
#include "rocksdb/table_properties.h"

namespace rocksdb {

class BlockBuilder;
class BlockHandle;
class WritableFile;
class TableBuilder;

class PlainTableBuilder: public TableBuilder {
public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish(). The output file
  // will be part of level specified by 'level'.  A value of -1 means
  // that the caller does not know which level the output file will reside.
  PlainTableBuilder(const Options& options, WritableFile* file,
                    uint32_t user_key_size);

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~PlainTableBuilder();

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  // Return non-ok iff some error has been detected.
  Status status() const override;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

private:
  Options options_;
  WritableFile* file_;
  uint64_t offset_ = 0;
  Status status_;
  TableProperties properties_;

  const size_t user_key_len_;
  bool closed_ = false;  // Either Finish() or Abandon() has been called.

  bool IsFixedLength() const {
    return user_key_len_ > 0;
  }

  // No copying allowed
  PlainTableBuilder(const PlainTableBuilder&) = delete;
  void operator=(const PlainTableBuilder&) = delete;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
