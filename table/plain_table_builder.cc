// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/plain_table_builder.h"

#include <assert.h>
#include <map>

#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"

namespace rocksdb {

PlainTableBuilder::PlainTableBuilder(const Options& options,
                                     WritableFile* file,
                                     int user_key_size, int key_prefix_len) :
    options_(options), file_(file), user_key_size_(user_key_size),
    key_prefix_len_(key_prefix_len) {
  std::string version;
  PutFixed32(&version, 1 | 0x80000000);
  file_->Append(Slice(version));
  offset_ = 4;
}

PlainTableBuilder::~PlainTableBuilder() {
}

Status PlainTableBuilder::ChangeOptions(const Options& options) {
  return Status::OK();
}

void PlainTableBuilder::Add(const Slice& key, const Slice& value) {
  assert((int) key.size() == GetInternalKeyLength());

  // Write key-value pair
  file_->Append(key);
  offset_ += GetInternalKeyLength();

  std::string size;
  int value_size = value.size();
  PutVarint32(&size, value_size);
  Slice sizeSlice(size);
  file_->Append(sizeSlice);
  file_->Append(value);
  offset_ += value_size + size.length();

  num_entries_++;
}

Status PlainTableBuilder::status() const {
  return Status::OK();
}

Status PlainTableBuilder::Finish() {
  assert(!closed_);
  closed_ = true;
  return Status::OK();
}

void PlainTableBuilder::Abandon() {
  closed_ = true;
}

uint64_t PlainTableBuilder::NumEntries() const {
  return num_entries_;
}

uint64_t PlainTableBuilder::FileSize() const {
  return offset_;
}

}  // namespace rocksdb
