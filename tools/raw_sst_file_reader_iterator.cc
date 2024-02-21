//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "tools/raw_sst_file_reader_iterator.h"

#include <chrono>
#include <cinttypes>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_builder.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/plain/plain_table_factory.h"
#include "table/table_reader.h"
#include "util/compression.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

RawSstFileReaderIterator::RawSstFileReaderIterator(InternalIterator* iterator,
                             bool has_from, Slice* from_key,
                             bool has_to, Slice* to_key)
    : iter_(iterator),
      ikey(new ParsedInternalKey()),
      has_to_(has_to),
      to_key_(to_key) {
  if (has_from) {
    InternalKey k;
    k.SetMinPossibleForUserKey(*from_key);
    iter_->Seek(k.Encode());
  } else {
    iter_->SeekToFirst();
  }
  initKey();
}

bool RawSstFileReaderIterator::has_next() const {
  return iter_->Valid() && (!has_to_ ||
                            BytewiseComparator()->Compare(
                                getKey(), *to_key_) < 0);
}

void RawSstFileReaderIterator::initKey() {
  if (iter_->Valid()) {
    ParseInternalKey(iter_->key(), ikey, true /* log_err_key */);
  }
}
void RawSstFileReaderIterator::next() {
  iter_->Next();
  initKey();

}

Slice RawSstFileReaderIterator::getKey() const {
  return ikey->user_key;
}

uint64_t RawSstFileReaderIterator::getSequenceNumber() const {
    return ikey->sequence;
}

uint32_t RawSstFileReaderIterator::getType() const {
    return static_cast<int>(ikey->type);
}

Slice RawSstFileReaderIterator::getValue() const {
    return iter_->value();
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
