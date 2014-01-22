// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "rocksdb/slice_transform.h"
#include "rocksdb/memtablerep.h"

namespace rocksdb {

class HashSkipListRepFactory : public MemTableRepFactory {
 public:
  explicit HashSkipListRepFactory(const SliceTransform* transform,
      size_t bucket_count = 1000000)
    : transform_(transform),
      bucket_count_(bucket_count) { }

  virtual ~HashSkipListRepFactory() { delete transform_; }

  virtual MemTableRep* CreateMemTableRep(MemTableRep::KeyComparator& compare,
                                         Arena* arena) override;

  virtual const char* Name() const override {
    return "HashSkipListRepFactory";
  }

  const SliceTransform* GetTransform() { return transform_; }

 private:
  const SliceTransform* transform_;
  const size_t bucket_count_;
};

}
