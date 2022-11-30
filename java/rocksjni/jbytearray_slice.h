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

#include <jni.h>

#include <iostream>

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class CharArrayValueSink : public ValueSink {
 private:
  char* jval_;
  jint jval_len_;

  jint pos_ = 0;
  jint size_ = 0;

  public:
   explicit CharArrayValueSink(char* jval, jint jval_len)
      : jval_(jval), jval_len_(jval_len){};

  inline void Assign(const char* data, size_t size) { 
    const jint copy_size = std::min(jval_len_, static_cast<jint>(size));
    memcpy(jval_, data, copy_size);
    pos_ = 0;
    size_ = copy_size;
  };

  inline void Move(const std::string&& buf) {
    // Move to this cannot std::move, but must assign, as this is not a std::string
    Assign(buf.data(), buf.size());
  };

  inline void RemovePrefix(size_t len) {
    pos_ += std::min(size_, static_cast<jint>(len));
    size_ -= std::min(size_, static_cast<jint>(len));
  };

  inline void RemoveSuffix(size_t len) {
    size_ -= std::min(size_, static_cast<jint>(len));
  };

  inline bool IsEmpty() { return false; };

  inline size_t Size() { return size_; };

  inline const char* Data() { return jval_ + pos_; };

};

class JByteArrayValueSink : public ValueSink {
 private:
  JNIEnv* jenv_;
  jbyteArray jval_;
  jint jval_off_;
  jint jval_len_;

  jint pos_ = 0;
  jint size_ = 0;

 public:
  explicit JByteArrayValueSink(JNIEnv* jenv, jbyteArray jval, jint jval_off,
                                   jint jval_len)
      : jenv_(jenv), jval_(jval), jval_off_(jval_off), jval_len_(jval_len){};

  inline void Assign(const char* data, size_t size) { 
    const jint copy_size = std::min(jval_len_, static_cast<jint>(size));
    jenv_->SetByteArrayRegion(
        jval_, jval_off_, copy_size,
        const_cast<jbyte*>(reinterpret_cast<const jbyte*>(data)));
    pos_ = 0;
    size_ = copy_size;
  };

  inline void Move(const std::string&& buf) {
    // Move to this cannot std::move, but must assign, as this is not a std::string
    Assign(buf.data(), buf.size());
  };

  inline void RemovePrefix(size_t len) {
    pos_ += std::min(size_, static_cast<jint>(len));
    size_ -= std::min(size_, static_cast<jint>(len));
  };

  inline void RemoveSuffix(size_t len) {
    size_ -= std::min(size_, static_cast<jint>(len));
  };

  inline bool IsEmpty() { return false; };

  inline size_t Size() { return size_; };

  //TODO (AP) is this ever needed ? let's find out..
  inline const char* Data() { 
    assert(false);
    return nullptr; 
  };
};

}  // namespace ROCKSDB_NAMESPACE