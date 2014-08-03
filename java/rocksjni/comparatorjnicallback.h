// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Comparator and rocksdb::DirectComparator.

#ifndef JAVA_ROCKSJNI_COMPARATORJNICALLBACK_H_
#define JAVA_ROCKSJNI_COMPARATORJNICALLBACK_H_

#include <jni.h>
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"

namespace rocksdb {
class BaseComparatorJniCallback : public Comparator {
  public:
    BaseComparatorJniCallback(JNIEnv* env, jobject jComparator);
    virtual ~BaseComparatorJniCallback();
    virtual const char* Name() const;
    virtual int Compare(const Slice& a, const Slice& b) const;
    virtual void FindShortestSeparator(std::string* start, const Slice& limit) const;
    virtual void FindShortSuccessor(std::string* key) const;

  private:
    JavaVM* m_jvm;
    jobject m_jComparator;
    std::string m_name;
    jmethodID m_jCompareMethodId;
    jmethodID m_jFindShortestSeparatorMethodId;
    jmethodID m_jFindShortSuccessorMethodId;
    JNIEnv* getJniEnv() const;

  protected:
    jobject m_jSliceA;
    jobject m_jSliceB;
    jobject m_jSliceLimit;
};

class ComparatorJniCallback : public BaseComparatorJniCallback {
    public:
      ComparatorJniCallback(JNIEnv* env, jobject jComparator);
};

class DirectComparatorJniCallback : public BaseComparatorJniCallback {
    public:
      DirectComparatorJniCallback(JNIEnv* env, jobject jComparator);
};
}  // namespace rocksdb

#endif  // JAVA_ROCKSJNI_COMPARATORJNICALLBACK_H_
