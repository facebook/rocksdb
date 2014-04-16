// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE
#pragma once
#include "rocksdb/slice_transform.h"
#include "rocksdb/memtablerep.h"

namespace rocksdb {

class HashLinkListRepFactory : public MemTableRepFactory {
 public:
  explicit HashLinkListRepFactory(size_t bucket_count)
      : bucket_count_(bucket_count) { }

  virtual ~HashLinkListRepFactory() {}

  virtual MemTableRep* CreateMemTableRep(
      const MemTableRep::KeyComparator& compare, Arena* arena,
      const SliceTransform* transform) override;

  virtual const char* Name() const override {
    return "HashLinkListRepFactory";
  }

 private:
  const size_t bucket_count_;
};

}
#endif  // ROCKSDB_LITE
