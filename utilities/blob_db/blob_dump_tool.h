//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "utilities/blob_db/blob_log_reader.h"

namespace rocksdb {
namespace blob_db {

class BlobDumpTool {
 public:
  int Run(int argc, char** argv);

 private:
  Status DumpBlobLogHeader(Reader& reader);
  Status DumpRecord(Reader& reader);
  void DumpSlice(const Slice& s);
  std::string GetString(ttlrange_t ttl);
  std::string GetString(tsrange_t ts);
};

}  // namespace blob_db
}  // namespace rocksdb

#endif  // ROCKSDB_LITE
