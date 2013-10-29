// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// IndexedTable is a simple table format for UNIT TEST ONLY. It is not built
// as production quality.

#pragma once
#include <stdint.h>
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
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
                    int user_key_size, int key_prefix_len);

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~PlainTableBuilder();

  // Change the options used by this builder.  Note: only some of the
  // option fields can be changed after construction.  If a field is
  // not allowed to change dynamically and its value in the structure
  // passed to the constructor is different from its value in the
  // structure passed to this method, this method will return an error
  // without changing any fields.
  Status ChangeOptions(const Options& options);

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
  uint64_t num_entries_ = 0;

  const size_t user_key_size_;
  const size_t key_prefix_len_;
  bool closed_ = false;  // Either Finish() or Abandon() has been called.

  int GetInternalKeyLength() {
    return user_key_size_ + 8;
  }

  // No copying allowed
  PlainTableBuilder(const PlainTableBuilder&) = delete;
  void operator=(const PlainTableBuilder&) = delete;
};

}  // namespace rocksdb

