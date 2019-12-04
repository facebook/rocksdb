// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cassert>
#include <memory>
#include <string>
#include <vector>

namespace rocksdb {

class Slice;
class SliceTransform;

// SstFileChecksum generate the checksum for each SST file when the file is
// written to the file system. The checksum is stored in the Manifest
class SstFileChecksum {
 public:

  virtual ~SstFileChecksum() {}
  // Return the checksum of concat (A, data[0,n-1]) where init_checksum is the
  // returned value of some string A. It is used to maintain the checksum of a
  // stream of data
  virtual uint32_t Extend(uint32_t init_checksum, const char* data, size_t n) = 0;

  // Return the checksum value of data[0,n-1]
  virtual uint32_t Value(const char* data, size_t n) = 0;

  // Return a processed value of the checksum for store in somewhere
  virtual uint32_t ProcessChecksum(const uint32_t checksum) = 0;

  // Returns a name that identifies the current file checksum method.
  virtual const char* Name() const = 0;
};

}  // namespace rocksdb
