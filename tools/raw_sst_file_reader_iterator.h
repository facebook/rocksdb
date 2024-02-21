// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE

#include <memory>
#include <string>
#include "file/writable_file_writer.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/raw_iterator.h"

namespace ROCKSDB_NAMESPACE {

class RawSstFileReaderIterator : public RawIterator {
 public:
  explicit RawSstFileReaderIterator(InternalIterator* iterator,
                                    bool has_from,
                                    Slice* from_key,
                                    bool has_to,
                                    Slice* to_key);

  bool has_next() const override;
  Slice getKey() const override;
  Slice getValue() const override;
  uint64_t getSequenceNumber() const override;
  uint32_t getType() const override;
  void next() final override;

  ~RawSstFileReaderIterator(){
    delete iter_;
  }

 private:
  void initKey();
  InternalIterator* iter_;
  ParsedInternalKey* ikey;
  bool has_to_;
  Slice* to_key_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
