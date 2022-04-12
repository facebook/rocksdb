//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::EventListener.

#include <jni.h>

#include <memory>

#include "include/org_rocksdb_AbstractEventListener.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/event_listener_jnicallback.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_AbstractEventListener
 * Method:    createNewEventListener
 * Signature: (J)J
 */
jlong Java_org_rocksdb_AbstractEventListener_createNewEventListener(
    JNIEnv* env, jobject jobj, jlong jenabled_event_callback_values) {
  auto enabled_event_callbacks =
      ROCKSDB_NAMESPACE::EnabledEventCallbackJni::toCppEnabledEventCallbacks(
          jenabled_event_callback_values);
  auto* sptr_event_listener =
      new std::shared_ptr<ROCKSDB_NAMESPACE::EventListener>(
          new ROCKSDB_NAMESPACE::EventListenerJniCallback(
              env, jobj, enabled_event_callbacks));
  return GET_CPLUSPLUS_POINTER(sptr_event_listener);
}

/*
 * Class:     org_rocksdb_AbstractEventListener
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_AbstractEventListener_disposeInternal(JNIEnv*, jobject,
                                                            jlong jhandle) {
  delete reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::EventListener>*>(
      jhandle);
}
