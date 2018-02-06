//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::CompactionFilter.

#include <iostream>
#include <jni.h>

#include "rocksjni/compaction_filter_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {

/**
 * Main initialization function. Caches a bunch of class and method references to speed up access later.
 *
 * @param env                    JNI environment
 * @param jCompactionFilter      Reference to the CompactionFilter Java object that is being initialized.
 * @return                       Success (true) or failure (false)
 */
bool CompactionFilterJniCallback::Initialize(JNIEnv *env, jobject jCompactionFilterLocalRef) {

  // Cache a reference to the JVM. We can then use it to safely spawn JNIEnv references from multiple threads while in
  // the FilterV2 method, which in turn will let us call back into Java.
  const jint rs = env->GetJavaVM(&javaVM_);
  if(rs != JNI_OK) {
    // exception thrown
    return false;
  }

  // Cache the CompactionFilter instance reference
  assert(jCompactionFilterLocalRef != nullptr);
  jCompactionFilter_ = env->NewGlobalRef(jCompactionFilterLocalRef);
  if(jCompactionFilter_ == nullptr) {
    // exception thrown: OutOfMemoryError
    return false;
  }

  // The name of a CompactionFilter will not change during its lifetime,
  // so we call and cache it in a global var
  jmethodID jNameMethodId = AbstractJavaCompactionFilterJni::getNameMethodId(env);
  if(jNameMethodId == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return false;
  }
  jstring jsName = (jstring)env->CallObjectMethod(jCompactionFilter_, jNameMethodId);
  if(env->ExceptionCheck()) {
    // exception thrown
    return false;
  }
  jboolean has_exception = JNI_FALSE;
  name_ = JniUtil::copyStdString(env, jsName,
                                 &has_exception);  // also releases jsName
  if (has_exception == JNI_TRUE) {
    // exception thrown
    return false;
  }

  // Cache the CompactionFilter#FilterV2Internal method reference
  jCompactionFilterFilterV2InternalMethodId_ = AbstractJavaCompactionFilterJni::getFilterV2InternalMethodId(env);
  if(jCompactionFilterFilterV2InternalMethodId_ == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return false;
  }

  // Cache the CompactionOutput class reference
  jCompactionOutputClass_ = CompactionOutputJni::getJClass(env);
  if(jCompactionOutputClass_ == nullptr) {
    // exception thrown: ClassNotFoundException or OutOfMemoryError
    return false;
  }

  // Cache the CompactionOutput#decisionValue field reference
  jCompactionOutputDecisionValueFieldId_ = CompactionOutputJni::getDecisionValueFieldId(env);
  if(jCompactionOutputDecisionValueFieldId_ == nullptr) {
    // exception thrown: NoSuchFieldException or OutOfMemoryError
    return false;
  }

  // Cache the CompactionOutput#newValue field reference
  jCompactionOutputNewValueFieldId_ = CompactionOutputJni::getNewValueFieldId(env);
  if(jCompactionOutputNewValueFieldId_ == nullptr) {
    // exception thrown: NoSuchFieldException or OutOfMemoryError
    return false;
  }

  // Cache the CompactionOutput#skipUntil field reference
  jCompactionOutputSkipUntilFieldId_ = CompactionOutputJni::getSkipUntilFieldId(env);
  if(jCompactionOutputSkipUntilFieldId_ == nullptr) {
    // exception thrown: NoSuchFieldException or OutOfMemoryError
    return false;
  }

  // Success
  return true;

}

/**
 * Destructor. Releases all the global references.
 */
CompactionFilterJniCallback::~CompactionFilterJniCallback() {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = JniUtil::getJniEnv(javaVM_, &attached_thread);
  assert(env != nullptr);
  if(jCompactionFilter_ != nullptr) {
    env->DeleteGlobalRef(jCompactionFilter_);
  }
  // This MUST be last!
  if(javaVM_ != nullptr) {
    JniUtil::releaseJniEnv(javaVM_, attached_thread);
  }
}

/**
 * Accessor for the name of this CompactionFilter, which was cached from the Java side in Initialize()
 *
 * @return  Name string
 */
const char* CompactionFilterJniCallback::Name() const {
  return name_.c_str();
}

/**
 * Main filtering method. Translates from C++'s CompactionFilter::FilterV2() signature:
 *      FilterV2(int level, Slice &key, ValueType value_type, Slice &existing_value, string *new_value, string *skip_until)
 * to Java's AbstractJavaCompactionFilter.FilterV2() signature:
 *      CompactionOutput FilterV2(int level, Slice key, ValueType valueType, DirectSlice existingValue)
 * and then massages return types appropriately.
 *
 * See CompactionFilter::FilterV2() for parameter description.
 */
CompactionFilter::Decision CompactionFilterJniCallback::FilterV2(
    int level, const Slice &key, CompactionFilter::ValueType value_type,
    const Slice &existing_value, std::string *new_value,
    std::string *skip_until) const {

  // Grab a fresh JNI env by attaching the current thread to the Java VM. This is a no-op if we're already attached.
  // NOTE: In the current implementation we never detach, because we assume the threads are long-lived.
  //
  // TODO(benclay): We need a lifecycle hook on thread death in order to safely detach without the huge performance
  //                impact of detaching on every call.
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = JniUtil::getJniEnv(javaVM_, &attached_thread);
  assert(env != nullptr);

  // Wrap with a try / catch for handling errors coming out of Java calls
  CompactionFilter::Decision decision = CompactionFilter::Decision::kKeep;
  try {

    // Push a local frame to make sure all the thread-local JNI handles that are allocated during this method are
    // eventually reclaimed.
    if(env->PushLocalFrame(1) < 0) {
      throw CompactionFilterJniCallbackException("Pushing local frame failed");
    }

    // Issue the CompactionFilter#FilterV2Internal call over to Java
    jlong jKeyHandle = reinterpret_cast<jlong>(&key);
    jlong jExistingValueHandle = reinterpret_cast<jlong>(&existing_value);
    jobject jCompactionOutput =
        env->CallObjectMethod(jCompactionFilter_, jCompactionFilterFilterV2InternalMethodId_, level, jKeyHandle,
                              value_type, jExistingValueHandle);
    if (env->ExceptionCheck()) {
      throw CompactionFilterJniCallbackException("Exception encountered in Java FilterV2Internal method");
    }
    if(jCompactionOutput == nullptr) {
      throw CompactionFilterJniCallbackException("Java FilterV2Internal method returned null");
    }

    // Extract decision value from the result object
    jbyte jDecisionValue = env->GetByteField(jCompactionOutput, jCompactionOutputDecisionValueFieldId_);
    decision = static_cast<CompactionFilter::Decision>(jDecisionValue);

    // Extract new_value from the result object if we have one
    if(decision == CompactionFilter::Decision::kChangeValue) {
      jbyteArray jNewValue = (jbyteArray) env->GetObjectField(jCompactionOutput, jCompactionOutputNewValueFieldId_);
      if (env->ExceptionCheck()) {
        throw CompactionFilterJniCallbackException("Exception accessing CompactionOutput#newValue");
      }
      if (jNewValue != nullptr) {
        jboolean has_exception = JNI_FALSE;
        std::string incoming_new_value = rocksdb::JniUtil::byteString<std::string>(  // also releases jNewValue
            env, jNewValue, [](const char* str, const size_t len) { return std::string(str, len); }, &has_exception);
        if (has_exception == JNI_TRUE) {
          throw CompactionFilterJniCallbackException(
              "Java exception encountered when copying new_value byte array from Java -> native");
        }
        new_value->assign(incoming_new_value);
      }
    }

      // Extract skip_until from the result object
    else if(decision == CompactionFilter::Decision::kRemoveAndSkipUntil) {
      jbyteArray jSkipUntil = (jbyteArray) env->GetObjectField(jCompactionOutput, jCompactionOutputSkipUntilFieldId_);
      if (env->ExceptionCheck()) {
        throw CompactionFilterJniCallbackException("Exception accessing CompactionOutput#skipUntil");
      }
      if (jSkipUntil != nullptr) {
        jboolean has_exception = JNI_FALSE;
        std::string incoming_skip_until = rocksdb::JniUtil::byteString<std::string>(  // also releases jSkipUntil
            env, jSkipUntil, [](const char* str, const size_t len) { return std::string(str, len); }, &has_exception);
        if (has_exception == JNI_TRUE) {
          throw CompactionFilterJniCallbackException(
              "Java exception encountered when copying skip_until byte array from Java -> native");
        }
        skip_until->assign(incoming_skip_until);
      }
    }

  } catch (CompactionFilterJniCallbackException e) {
    // Print a warning to stderr
    std::cerr << "Error condition encountered in CompactionFilterJniCallback::FilterV2: " << e.what() << std::endl;
    // Print any Java exception to stderr
    if(env->ExceptionCheck()) {
      env->ExceptionDescribe();
    }
  }

  // Pop the frame to make sure all the thread-local JNI handles that are allocated during this method are
  // eventually reclaimed.
  env->PopLocalFrame(NULL);

  // Return
  return decision;

}

}  // namespace rocksdb
