// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Arena class defines memory allocation methods. It's used by memtable and
// skiplist.

#ifndef STORAGE_ROCKSDB_INCLUDE_ARENA_H_
#define STORAGE_ROCKSDB_INCLUDE_ARENA_H_

#include <limits>
#include <memory>

namespace rocksdb {

class Arena {
 public:
  Arena() {};
  virtual ~Arena() {};

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  virtual char* Allocate(size_t bytes) = 0;

  // Allocate memory with the normal alignment guarantees provided by malloc.
  virtual char* AllocateAligned(size_t bytes) = 0;

  // Returns an estimate of the total memory used by arena.
  virtual const size_t ApproximateMemoryUsage() = 0;

  // Returns the total number of bytes in all blocks allocated so far.
  virtual const size_t MemoryAllocatedBytes() = 0;

 private:
  // No copying allowed
  Arena(const Arena&);
  void operator=(const Arena&);
};

}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_INCLUDE_ARENA_H_
