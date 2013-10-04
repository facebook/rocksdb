// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/slice_transform.h"
#include "rocksdb/slice.h"

namespace rocksdb {

namespace {

class FixedPrefixTransform : public SliceTransform {
 private:
  size_t prefix_len_;

 public:
  explicit FixedPrefixTransform(size_t prefix_len) : prefix_len_(prefix_len) { }

  virtual const char* Name() const {
    return "rocksdb.FixedPrefix";
  }

  virtual Slice Transform(const Slice& src) const {
    assert(InDomain(src));
    return Slice(src.data(), prefix_len_);
  }

  virtual bool InDomain(const Slice& src) const {
    return (src.size() >= prefix_len_);
  }

  virtual bool InRange(const Slice& dst) const {
    return (dst.size() == prefix_len_);
  }
};

class NoopTransform : public SliceTransform {
 public:
  explicit NoopTransform() { }

  virtual const char* Name() const {
    return "rocksdb.Noop";
  }

  virtual Slice Transform(const Slice& src) const {
    return src;
  }

  virtual bool InDomain(const Slice& src) const {
    return true;
  }

  virtual bool InRange(const Slice& dst) const {
    return true;
  }
};

}

const SliceTransform* NewFixedPrefixTransform(size_t prefix_len) {
  return new FixedPrefixTransform(prefix_len);
}

const SliceTransform* NewNoopTransform() {
  return new NoopTransform;
}

}  // namespace rocksdb
