// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::Comparator

#ifndef JAVA_ROCKSJNI_COMPARATORJNICALLBACK_H_
#define JAVA_ROCKSJNI_COMPARATORJNICALLBACK_H_

#include <jni.h>

#include <memory>
#include <string>

#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksjni/jnicallback.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

enum ReusedSynchronisationType {
  /**
   * Standard mutex.
   */
  MUTEX,

  /**
   * Use adaptive mutex, which spins in the user space before resorting
   * to kernel. This could reduce context switch when the mutex is not
   * heavily contended. However, if the mutex is hot, we could end up
   * wasting spin time.
   */
  ADAPTIVE_MUTEX,

  /**
   * There is a reused buffer per-thread.
   */
  THREAD_LOCAL
};

struct ComparatorJniCallbackOptions {
  // Set the synchronisation type used to guard the reused buffers.
  // Only used if max_reused_buffer_size > 0.
  // Default: ADAPTIVE_MUTEX
  ReusedSynchronisationType reused_synchronisation_type =
      ReusedSynchronisationType::ADAPTIVE_MUTEX;

  // Indicates if a direct byte buffer (i.e. outside of the normal
  // garbage-collected heap) is used for the callbacks to Java,
  // as opposed to a non-direct byte buffer which is a wrapper around
  // an on-heap byte[].
  // Default: true
  bool direct_buffer = true;

  // Maximum size of a buffer (in bytes) that will be reused.
  // Comparators will use 5 of these buffers,
  // so the retained memory size will be 5 * max_reused_buffer_size.
  // When a buffer is needed for transferring data to a callback,
  // if it requires less than max_reused_buffer_size, then an
  // existing buffer will be reused, else a new buffer will be
  // allocated just for that callback. -1 to disable.
  // Default: 64 bytes
  int32_t max_reused_buffer_size = 64;
};

/**
 * This class acts as a bridge between C++
 * and Java. The methods in this class will be
 * called back from the RocksDB storage engine (C++)
 * we then callback to the appropriate Java method
 * this enables Comparators to be implemented in Java.
 *
 * The design of this Comparator caches the Java Slice
 * objects that are used in the compare and findShortestSeparator
 * method callbacks. Instead of creating new objects for each callback
 * of those functions, by reuse via setHandle we are a lot
 * faster; Unfortunately this means that we have to
 * introduce independent locking in regions of each of those methods
 * via the mutexs mtx_compare and mtx_findShortestSeparator respectively
 */
class ComparatorJniCallback : public JniCallback, public Comparator {
 public:
  ComparatorJniCallback(JNIEnv* env, jobject jcomparator,
                        const ComparatorJniCallbackOptions* options);
  ~ComparatorJniCallback();
  virtual const char* Name() const;
  virtual int Compare(const Slice& a, const Slice& b) const;
  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const;
  virtual void FindShortSuccessor(std::string* key) const;
  const ComparatorJniCallbackOptions* m_options;

 private:
  struct ThreadLocalBuf {
    ThreadLocalBuf(JavaVM* _jvm, bool _direct_buffer, jobject _jbuf)
        : jvm(_jvm), direct_buffer(_direct_buffer), jbuf(_jbuf) {}
    JavaVM* jvm;
    bool direct_buffer;
    jobject jbuf;
  };
  inline void MaybeLockForReuse(const std::unique_ptr<port::Mutex>& mutex,
                                const bool cond) const;
  inline void MaybeUnlockForReuse(const std::unique_ptr<port::Mutex>& mutex,
                                  const bool cond) const;
  jobject GetBuffer(JNIEnv* env, const Slice& src, bool reuse_buffer,
                    ThreadLocalPtr* tl_buf, jobject jreuse_buffer) const;
  jobject ReuseBuffer(JNIEnv* env, const Slice& src,
                      jobject jreuse_buffer) const;
  jobject NewBuffer(JNIEnv* env, const Slice& src) const;
  void DeleteBuffer(JNIEnv* env, jobject jbuffer) const;
  // used for synchronisation in compare method
  std::unique_ptr<port::Mutex> mtx_compare;
  // used for synchronisation in findShortestSeparator method
  std::unique_ptr<port::Mutex> mtx_shortest;
  // used for synchronisation in findShortSuccessor method
  std::unique_ptr<port::Mutex> mtx_short;
  std::unique_ptr<const char[]> m_name;
  jclass m_abstract_comparator_jni_bridge_clazz;  // TODO(AR) could we make this
                                                  // static somehow?
  jclass m_jbytebuffer_clazz;  // TODO(AR) we could cache this globally for the
                               // entire VM if we switch more APIs to use
                               // ByteBuffer // TODO(AR) could we make this
                               // static somehow?
  jmethodID m_jcompare_mid;    // TODO(AR) could we make this static somehow?
  jmethodID m_jshortest_mid;   // TODO(AR) could we make this static somehow?
  jmethodID m_jshort_mid;      // TODO(AR) could we make this static somehow?
  jobject m_jcompare_buf_a;
  jobject m_jcompare_buf_b;
  jobject m_jshortest_buf_start;
  jobject m_jshortest_buf_limit;
  jobject m_jshort_buf_key;
  ThreadLocalPtr* m_tl_buf_a;
  ThreadLocalPtr* m_tl_buf_b;
};
}  // namespace ROCKSDB_NAMESPACE

#endif  // JAVA_ROCKSJNI_COMPARATORJNICALLBACK_H_
