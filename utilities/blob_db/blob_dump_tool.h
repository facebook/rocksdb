//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
#pragma once
#ifndef ROCKSDB_LITE

#include <memory>
#include <string>
#include <utility>
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/file_reader_writer.h"
#include "utilities/blob_db/blob_log_format.h"

namespace rocksdb {
namespace blob_db {

class BlobDumpTool {
 public:
  enum class DisplayType {
    kNone,
    kRaw,
    kHex,
    kDetail,
  };

  BlobDumpTool();

  Status Run(const std::string& filename, DisplayType key_type,
             DisplayType blob_type);

 private:
  std::unique_ptr<RandomAccessFileReader> reader_;
  std::unique_ptr<char> buffer_;
  size_t buffer_size_;

  Status Read(uint64_t offset, size_t size, Slice* result);
  Status DumpBlobLogHeader(uint64_t* offset);
  Status DumpBlobLogFooter(uint64_t file_size, uint64_t* footer_offset);
  Status DumpRecord(DisplayType show_key, DisplayType show_blob,
                    uint64_t* offset);
  void DumpSlice(const Slice s, DisplayType type);

  template <class T>
  std::string GetString(std::pair<T, T> p);
};

}  // namespace blob_db
}  // namespace rocksdb

#endif  // ROCKSDB_LITE
