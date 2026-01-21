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
#include "rocksjni/portal/common.h"
#include "rocksjni/portal/integer_jni.h"
#include "rocksjni/portal/java_class.h"
#include "rocksjni/portal/jni_util.h"
#include "rocksjni/portal/long_jni.h"
#include "rocksjni/portal/map_jni.h"

namespace ROCKSDB_NAMESPACE {
class HashMapJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.util.HashMap
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/HashMap");
  }

  /**
   * Create a new Java java.util.HashMap object.
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java java.util.HashMap object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const uint32_t initial_capacity = 16) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(I)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jhash_map =
        env->NewObject(jclazz, mid, static_cast<jint>(initial_capacity));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jhash_map;
  }

  /**
   * A function which maps a std::pair<K,V> to a std::pair<JK, JV>
   *
   * @return Either a pointer to a std::pair<jobject, jobject>, or nullptr
   *     if an error occurs during the mapping
   */
  template <typename K, typename V, typename JK, typename JV>
  using FnMapKV =
      std::function<std::unique_ptr<std::pair<JK, JV>>(const std::pair<K, V>&)>;

  // template <class I, typename K, typename V, typename K1, typename V1,
  // typename std::enable_if<std::is_same<typename
  // std::iterator_traits<I>::value_type, std::pair<const K,V>>::value,
  // int32_t>::type = 0> static void putAll(JNIEnv* env, const jobject
  // jhash_map, I iterator, const FnMapKV<const K,V,K1,V1> &fn_map_kv) {
  /**
   * Returns true if it succeeds, false if an error occurs
   */
  template <class iterator_type, typename K, typename V>
  static bool putAll(JNIEnv* env, const jobject jhash_map,
                     iterator_type iterator, iterator_type end,
                     const FnMapKV<K, V, jobject, jobject>& fn_map_kv) {
    const jmethodID jmid_put =
        ROCKSDB_NAMESPACE::MapJni::getMapPutMethodId(env);
    if (jmid_put == nullptr) {
      return false;
    }

    for (auto it = iterator; it != end; ++it) {
      const std::unique_ptr<std::pair<jobject, jobject>> result =
          fn_map_kv(*it);
      if (result == nullptr) {
        // an error occurred during fn_map_kv
        return false;
      }
      env->CallObjectMethod(jhash_map, jmid_put, result->first, result->second);
      if (env->ExceptionCheck()) {
        // exception occurred
        env->DeleteLocalRef(result->second);
        env->DeleteLocalRef(result->first);
        return false;
      }

      // release local references
      env->DeleteLocalRef(result->second);
      env->DeleteLocalRef(result->first);
    }

    return true;
  }

  /**
   * Creates a java.util.Map<String, String> from a std::map<std::string,
   * std::string>
   *
   * @param env A pointer to the Java environment
   * @param map the Cpp map
   *
   * @return a reference to the Java java.util.Map object, or nullptr if an
   * exception occcurred
   */
  static jobject fromCppMap(JNIEnv* env,
                            const std::map<std::string, std::string>* map) {
    if (map == nullptr) {
      return nullptr;
    }

    jobject jhash_map = construct(env, static_cast<uint32_t>(map->size()));
    if (jhash_map == nullptr) {
      // exception occurred
      return nullptr;
    }

    const ROCKSDB_NAMESPACE::HashMapJni::FnMapKV<
        const std::string, const std::string, jobject, jobject>
        fn_map_kv =
            [env](const std::pair<const std::string, const std::string>& kv) {
              jstring jkey = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
                  env, &(kv.first), false);
              if (env->ExceptionCheck()) {
                // an error occurred
                return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
              }

              jstring jvalue = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
                  env, &(kv.second), true);
              if (env->ExceptionCheck()) {
                // an error occurred
                env->DeleteLocalRef(jkey);
                return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
              }

              return std::unique_ptr<std::pair<jobject, jobject>>(
                  new std::pair<jobject, jobject>(
                      static_cast<jobject>(jkey),
                      static_cast<jobject>(jvalue)));
            };

    if (!putAll(env, jhash_map, map->begin(), map->end(), fn_map_kv)) {
      // exception occurred
      return nullptr;
    }

    return jhash_map;
  }

  /**
   * Creates a java.util.Map<String, Long> from a std::map<std::string,
   * uint32_t>
   *
   * @param env A pointer to the Java environment
   * @param map the Cpp map
   *
   * @return a reference to the Java java.util.Map object, or nullptr if an
   * exception occcurred
   */
  static jobject fromCppMap(JNIEnv* env,
                            const std::map<std::string, uint32_t>* map) {
    if (map == nullptr) {
      return nullptr;
    }

    if (map == nullptr) {
      return nullptr;
    }

    jobject jhash_map = construct(env, static_cast<uint32_t>(map->size()));
    if (jhash_map == nullptr) {
      // exception occurred
      return nullptr;
    }

    const ROCKSDB_NAMESPACE::HashMapJni::FnMapKV<
        const std::string, const uint32_t, jobject, jobject>
        fn_map_kv =
            [env](const std::pair<const std::string, const uint32_t>& kv) {
              jstring jkey = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
                  env, &(kv.first), false);
              if (env->ExceptionCheck()) {
                // an error occurred
                return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
              }

              jobject jvalue = ROCKSDB_NAMESPACE::IntegerJni::valueOf(
                  env, static_cast<jint>(kv.second));
              if (env->ExceptionCheck()) {
                // an error occurred
                env->DeleteLocalRef(jkey);
                return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
              }

              return std::unique_ptr<std::pair<jobject, jobject>>(
                  new std::pair<jobject, jobject>(static_cast<jobject>(jkey),
                                                  jvalue));
            };

    if (!putAll(env, jhash_map, map->begin(), map->end(), fn_map_kv)) {
      // exception occurred
      return nullptr;
    }

    return jhash_map;
  }

  /**
   * Creates a java.util.Map<String, Long> from a std::map<std::string,
   * uint64_t>
   *
   * @param env A pointer to the Java environment
   * @param map the Cpp map
   *
   * @return a reference to the Java java.util.Map object, or nullptr if an
   * exception occcurred
   */
  static jobject fromCppMap(JNIEnv* env,
                            const std::map<std::string, uint64_t>* map) {
    if (map == nullptr) {
      return nullptr;
    }

    jobject jhash_map = construct(env, static_cast<uint32_t>(map->size()));
    if (jhash_map == nullptr) {
      // exception occurred
      return nullptr;
    }

    const ROCKSDB_NAMESPACE::HashMapJni::FnMapKV<
        const std::string, const uint64_t, jobject, jobject>
        fn_map_kv =
            [env](const std::pair<const std::string, const uint64_t>& kv) {
              jstring jkey = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
                  env, &(kv.first), false);
              if (env->ExceptionCheck()) {
                // an error occurred
                return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
              }

              jobject jvalue = ROCKSDB_NAMESPACE::LongJni::valueOf(
                  env, static_cast<jlong>(kv.second));
              if (env->ExceptionCheck()) {
                // an error occurred
                env->DeleteLocalRef(jkey);
                return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
              }

              return std::unique_ptr<std::pair<jobject, jobject>>(
                  new std::pair<jobject, jobject>(static_cast<jobject>(jkey),
                                                  jvalue));
            };

    if (!putAll(env, jhash_map, map->begin(), map->end(), fn_map_kv)) {
      // exception occurred
      return nullptr;
    }

    return jhash_map;
  }

  /**
   * Creates a java.util.Map<String, Long> from a std::map<uint32_t, uint64_t>
   *
   * @param env A pointer to the Java environment
   * @param map the Cpp map
   *
   * @return a reference to the Java java.util.Map object, or nullptr if an
   * exception occcurred
   */
  static jobject fromCppMap(JNIEnv* env,
                            const std::map<uint32_t, uint64_t>* map) {
    if (map == nullptr) {
      return nullptr;
    }

    jobject jhash_map = construct(env, static_cast<uint32_t>(map->size()));
    if (jhash_map == nullptr) {
      // exception occurred
      return nullptr;
    }

    const ROCKSDB_NAMESPACE::HashMapJni::FnMapKV<const uint32_t, const uint64_t,
                                                 jobject, jobject>
        fn_map_kv = [env](const std::pair<const uint32_t, const uint64_t>& kv) {
          jobject jkey = ROCKSDB_NAMESPACE::IntegerJni::valueOf(
              env, static_cast<jint>(kv.first));
          if (env->ExceptionCheck()) {
            // an error occurred
            return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
          }

          jobject jvalue = ROCKSDB_NAMESPACE::LongJni::valueOf(
              env, static_cast<jlong>(kv.second));
          if (env->ExceptionCheck()) {
            // an error occurred
            env->DeleteLocalRef(jkey);
            return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
          }

          return std::unique_ptr<std::pair<jobject, jobject>>(
              new std::pair<jobject, jobject>(static_cast<jobject>(jkey),
                                              jvalue));
        };

    if (!putAll(env, jhash_map, map->begin(), map->end(), fn_map_kv)) {
      // exception occurred
      return nullptr;
    }

    return jhash_map;
  }
};

}  // namespace ROCKSDB_NAMESPACE
