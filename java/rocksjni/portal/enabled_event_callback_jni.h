// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#pragma once

#include <jni.h>

#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
// The portal class for org.rocksdb.AbstractListener.EnabledEventCallback
class EnabledEventCallbackJni {
 public:
  // Returns the set of equivalent C++
  // ROCKSDB_NAMESPACE::EnabledEventCallbackJni::EnabledEventCallback enums for
  // the provided Java jenabled_event_callback_values
  static std::set<EnabledEventCallback> toCppEnabledEventCallbacks(
      jlong jenabled_event_callback_values) {
    std::set<EnabledEventCallback> enabled_event_callbacks;
    for (size_t i = 0; i < EnabledEventCallback::NUM_ENABLED_EVENT_CALLBACK;
         ++i) {
      if (((1ULL << i) & jenabled_event_callback_values) > 0) {
        enabled_event_callbacks.emplace(static_cast<EnabledEventCallback>(i));
      }
    }
    return enabled_event_callbacks;
  }
};
}  // namespace ROCKSDB_NAMESPACE
