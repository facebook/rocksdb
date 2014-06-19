//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE
#include <stdint.h>
#include <vector>
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/table_builder.h"
#include "table/plain_table_key_coding.h"
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
                    uint32_t user_key_size, EncodingType encoding_type,
                    size_t index_sparseness);

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
  std::vector<std::unique_ptr<TablePropertiesCollector>>
      table_properties_collectors_;
  WritableFile* file_;
  uint64_t offset_ = 0;
  Status status_;
  TableProperties properties_;
  PlainTableKeyEncoder encoder_;

  bool closed_ = false;  // Either Finish() or Abandon() has been called.

  // No copying allowed
  PlainTableBuilder(const PlainTableBuilder&) = delete;
  void operator=(const PlainTableBuilder&) = delete;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
