// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Comparator and rocksdb::DirectComparator.

#ifndef JAVA_ROCKSJNI_COMPARATORJNICALLBACK_H_
#define JAVA_ROCKSJNI_COMPARATORJNICALLBACK_H_

#include <jni.h>
#include <memory>
#include <string>
#include <map>
#include "rocksjni/jnicallback.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "port/port.h"


using namespace std;

namespace rocksdb {

struct ComparatorJniCallbackOptions {
  // Use adaptive mutex, which spins in the user space before resorting
  // to kernel. This could reduce context switch when the mutex is not
  // heavily contended. However, if the mutex is hot, we could end up
  // wasting spin time.
  // Default: false
  bool use_adaptive_mutex;

  ComparatorJniCallbackOptions() : use_adaptive_mutex(false) {
  }
};

//
//wgao Thread Local class that hold jobject
//
class ThreadLocalJObject {
public:
	ThreadLocalJObject() {
		lInited = 1;

	}

	~ThreadLocalJObject()
	{
		
		lInited = 0;
		if (lObjAssigned == 1 && m_jSlice != nullptr && m_jvm!=null)
		{
			jboolean attached_thread = JNI_FALSE;

			JNIEnv* env = JniUtil::getJniEnv(m_jvm, &attached_thread);
			assert(env != nullptr);
			// free ASAP after thread detach this thread local obj
			env->DeleteLocalRef(m_jSlice);	

			JniUtil::releaseJniEnv(m_jvm, attached_thread);

			delete m_jvm;
			m_jvm = null;

		}
	}

	volatile long lInited = 0;
	volatile long lObjAssigned = 0;
	JavaVM* m_jvm = null;
	jobject m_jSlice = nullptr;
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
class BaseComparatorJniCallback : public JniCallback, public Comparator {
 public:
    BaseComparatorJniCallback(
      JNIEnv* env, jobject jComparator,
      const ComparatorJniCallbackOptions* copt);
    virtual const char* Name() const;
    virtual int Compare(const Slice& a, const Slice& b) const;
	virtual int CompareNoLock(JNIEnv* env, const jobject& jSliceA, const jobject& jSliceB,
		const Slice& a, const Slice& b,
		jboolean attached_thread) const;
    virtual void FindShortestSeparator(
      std::string* start, const Slice& limit) const;
    virtual void FindShortSuccessor(std::string* key) const;

 private:
    // used for synchronisation in compare method
    std::unique_ptr<port::Mutex> mtx_compare;
    // used for synchronisation in findShortestSeparator method
    std::unique_ptr<port::Mutex> mtx_findShortestSeparator;
    std::unique_ptr<const char[]> m_name;
    jmethodID m_jCompareMethodId;
    jmethodID m_jFindShortestSeparatorMethodId;
    jmethodID m_jFindShortSuccessorMethodId;

	// wgao used for synchronisation in thread local map method
	std::unique_ptr<port::Mutex> mtx_threadMap;
	map<std::string, int> mapThread2SliceObjectA;
	map<std::string, jobject> mapThread2SliceObjectB;

 protected:
    jobject m_jSliceA;
    jobject m_jSliceB;
    jobject m_jSliceLimit;
};

class ComparatorJniCallback : public BaseComparatorJniCallback {
 public:
      ComparatorJniCallback(
        JNIEnv* env, jobject jComparator,
        const ComparatorJniCallbackOptions* copt);
      ~ComparatorJniCallback();
};

class DirectComparatorJniCallback : public BaseComparatorJniCallback {
 public:
      DirectComparatorJniCallback(
        JNIEnv* env, jobject jComparator,
        const ComparatorJniCallbackOptions* copt);
      ~DirectComparatorJniCallback();
};
}  // namespace rocksdb

#endif  // JAVA_ROCKSJNI_COMPARATORJNICALLBACK_H_
