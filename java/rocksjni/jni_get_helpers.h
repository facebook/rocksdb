//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <jni.h>

#include <functional>

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE {

typedef std::function<ROCKSDB_NAMESPACE::Status(
    // const ROCKSDB_NAMESPACE::ReadOptions&,
    const ROCKSDB_NAMESPACE::Slice&, ROCKSDB_NAMESPACE::PinnableSlice*)>
    FnGet;

jbyteArray rocksjni_get_helper(JNIEnv* env,
                               const ROCKSDB_NAMESPACE::FnGet& fn_get,
                               jbyteArray jkey, jint jkey_off, jint jkey_len);

jint rocksjni_get_helper(JNIEnv* env, const ROCKSDB_NAMESPACE::FnGet& fn_get,
                         jbyteArray jkey, jint jkey_off, jint jkey_len,
                         jbyteArray jval, jint jval_off, jint jval_len,
                         bool* has_exception);

};  // namespace ROCKSDB_NAMESPACE
