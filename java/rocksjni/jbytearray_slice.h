// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.

#pragma once

#include<iostream>
#include <jni.h>
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class CharArrayPinnableSlice : public PinnableSlice {
 public:
  explicit CharArrayPinnableSlice(char* jval, jint jval_len)
      : jval_(jval), jval_len_(jval_len){};

  inline virtual void PinSelf(const Slice& slice) {
    assert(!pinned_);

    size_ = slice.size();
    std::cout << "CharArrayPinnableSlice PinSelf size_ " << size_ << std::endl;
    const jint copy_size = std::min(jval_len_, static_cast<jint>(size_));
    std::cout << "CharArrayPinnableSlice PinSelf copy_size " << copy_size << std::endl;

    memcpy(jval_, slice.data(), copy_size);
    assert(!pinned_);
  };

  inline virtual void PinSelf() { 
    assert(!pinned_); 
    std::cout << "CharArrayPinnableSlice PinSelf() !!! ???" << size_ << std::endl;
  };

 private:
  char* jval_;
  jint jval_len_;
};

class JByteArrayPinnableSlice : public PinnableSlice {
 public:
  explicit JByteArrayPinnableSlice(JNIEnv* jenv, jbyteArray jval,
    jint jval_off, jint jval_len) : jenv_(jenv), jval_(jval), jval_off_(jval_off), jval_len_(jval_len) {};

  inline virtual void PinSelf(const Slice& slice) {
    assert(!pinned_);

    size_ = slice.size();
    std::cout << "JByteArrayPinnableSlice size_ " << size_ << std::endl;
    const jint copy_size = std::min(jval_len_, static_cast<jint>(size_));
    std::cout << "JByteArrayPinnableSlice copy_size " << copy_size << std::endl;

    jenv_->SetByteArrayRegion(
        jval_, jval_off_, copy_size,
        const_cast<jbyte*>(reinterpret_cast<const jbyte*>(slice.data())));

    assert(!pinned_);
  };

  inline virtual void PinSelf() {
    assert(!pinned_);
    std::cout << "JByteArrayPinnableSlice PinSelf() !!! ???" << size_ << std::endl;
  };

 private:
  JNIEnv* jenv_;
  jbyteArray jval_;
  jint jval_off_;
  jint jval_len_;
};
}