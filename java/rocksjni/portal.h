// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#ifndef JAVA_ROCKSJNI_PORTAL_H_
#define JAVA_ROCKSJNI_PORTAL_H_

#include <jni.h>
#include <limits>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/writebatchhandlerjnicallback.h"

namespace rocksdb {

// detect if jlong overflows size_t
inline Status check_if_jlong_fits_size_t(const jlong& jvalue) {
  Status s = Status::OK();
  if (static_cast<uint64_t>(jvalue) > std::numeric_limits<size_t>::max()) {
    s = Status::InvalidArgument(Slice("jlong overflows 32 bit value."));
  }
  return s;
}

// The portal class for org.rocksdb.RocksDB
class RocksDBJni {
 public:
  // Get the java class id of org.rocksdb.RocksDB.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/RocksDB");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.RocksDB
  // that stores the pointer to rocksdb::DB.
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::DB of the specified org.rocksdb.RocksDB.
  static rocksdb::DB* getHandle(JNIEnv* env, jobject jdb) {
    return reinterpret_cast<rocksdb::DB*>(
        env->GetLongField(jdb, getHandleFieldID(env)));
  }

  // Pass the rocksdb::DB pointer to the java side.
  static void setHandle(JNIEnv* env, jobject jdb, rocksdb::DB* db) {
    env->SetLongField(
        jdb, getHandleFieldID(env),
        reinterpret_cast<jlong>(db));
  }
};

// The portal class for org.rocksdb.RocksDBException
class RocksDBExceptionJni {
 public:
  // Get the jclass of org.rocksdb.RocksDBException
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/RocksDBException");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Create and throw a java exception by converting the input
  // Status to an RocksDBException.
  //
  // In case s.ok() is true, then this function will not throw any
  // exception.
  static void ThrowNew(JNIEnv* env, Status s) {
    if (s.ok()) {
      return;
    }
    jstring msg = env->NewStringUTF(s.ToString().c_str());
    // get the constructor id of org.rocksdb.RocksDBException
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "<init>", "(Ljava/lang/String;)V");
    assert(mid != nullptr);

    env->Throw((jthrowable)env->NewObject(getJClass(env), mid, msg));
  }
};

class OptionsJni {
 public:
  // Get the java class id of org.rocksdb.Options.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/Options");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.Options
  // that stores the pointer to rocksdb::Options
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::Options
  static rocksdb::Options* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::Options*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::Options pointer to the java side.
  static void setHandle(JNIEnv* env, jobject jobj, rocksdb::Options* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class DBOptionsJni {
 public:
  // Get the java class id of org.rocksdb.DBOptions.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/DBOptions");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.DBOptions
  // that stores the pointer to rocksdb::DBOptions
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::DBOptions
  static rocksdb::DBOptions* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::DBOptions*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::DBOptions pointer to the java side.
  static void setHandle(JNIEnv* env, jobject jobj, rocksdb::DBOptions* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class ColumnFamilyDescriptorJni {
 public:
  // Get the java class id of org.rocksdb.ColumnFamilyDescriptor
  static jclass getColumnFamilyDescriptorClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/ColumnFamilyDescriptor");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the java method id of columnFamilyName
  static jmethodID getColumnFamilyNameMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getColumnFamilyDescriptorClass(env),
        "columnFamilyName", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of columnFamilyOptions
  static jmethodID getColumnFamilyOptionsMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getColumnFamilyDescriptorClass(env),
        "columnFamilyOptions", "()Lorg/rocksdb/ColumnFamilyOptions;");
    assert(mid != nullptr);
    return mid;
  }
};

class ColumnFamilyOptionsJni {
 public:
  // Get the java class id of org.rocksdb.ColumnFamilyOptions.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/ColumnFamilyOptions");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.DBOptions
  // that stores the pointer to rocksdb::ColumnFamilyOptions
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::ColumnFamilyOptions
  static rocksdb::ColumnFamilyOptions* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::ColumnFamilyOptions pointer to the java side.
  static void setHandle(JNIEnv* env, jobject jobj,
      rocksdb::ColumnFamilyOptions* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class WriteOptionsJni {
 public:
  // Get the java class id of org.rocksdb.WriteOptions.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/WriteOptions");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.WriteOptions
  // that stores the pointer to rocksdb::WriteOptions
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::WriteOptions
  static rocksdb::WriteOptions* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::WriteOptions*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::WriteOptions pointer to the java side.
  static void setHandle(JNIEnv* env, jobject jobj, rocksdb::WriteOptions* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};


class ReadOptionsJni {
 public:
  // Get the java class id of org.rocksdb.ReadOptions.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/ReadOptions");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.ReadOptions
  // that stores the pointer to rocksdb::ReadOptions
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::ReadOptions
  static rocksdb::ReadOptions* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::ReadOptions*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::ReadOptions pointer to the java side.
  static void setHandle(JNIEnv* env, jobject jobj,
                        rocksdb::ReadOptions* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};


class WriteBatchJni {
 public:
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/WriteBatch");
    assert(jclazz != nullptr);
    return jclazz;
  }

  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::WriteBatch of the specified
  // org.rocksdb.WriteBatch.
  static rocksdb::WriteBatch* getHandle(JNIEnv* env, jobject jwb) {
    return reinterpret_cast<rocksdb::WriteBatch*>(
        env->GetLongField(jwb, getHandleFieldID(env)));
  }

  // Pass the rocksdb::WriteBatch pointer to the java side.
  static void setHandle(JNIEnv* env, jobject jwb, rocksdb::WriteBatch* wb) {
    env->SetLongField(
        jwb, getHandleFieldID(env),
        reinterpret_cast<jlong>(wb));
  }
};

class WriteBatchHandlerJni {
 public:
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/WriteBatch$Handler");
    assert(jclazz != nullptr);
    return jclazz;
  }

  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the java method `put` of org.rocksdb.WriteBatch.Handler.
  static jmethodID getPutMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "put", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `merge` of org.rocksdb.WriteBatch.Handler.
  static jmethodID getMergeMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "merge", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `delete` of org.rocksdb.WriteBatch.Handler.
  static jmethodID getDeleteMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "delete", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `logData` of org.rocksdb.WriteBatch.Handler.
  static jmethodID getLogDataMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "logData", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `shouldContinue` of org.rocksdb.WriteBatch.Handler.
  static jmethodID getContinueMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "shouldContinue", "()Z");
    assert(mid != nullptr);
    return mid;
  }

  // Get the pointer to rocksdb::WriteBatchHandlerJniCallback of the specified
  // org.rocksdb.WriteBatchHandler.
  static rocksdb::WriteBatchHandlerJniCallback* getHandle(
      JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::WriteBatchHandlerJniCallback*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::WriteBatchHandlerJniCallback pointer to the java side.
  static void setHandle(
      JNIEnv* env, jobject jobj,
      const rocksdb::WriteBatchHandlerJniCallback* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class HistogramDataJni {
 public:
  static jmethodID getConstructorMethodId(JNIEnv* env, jclass jclazz) {
    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "(DDDDD)V");
    assert(mid != nullptr);
    return mid;
  }
};

class BackupableDBOptionsJni {
 public:
  // Get the java class id of org.rocksdb.BackupableDBOptions.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/BackupableDBOptions");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.BackupableDBOptions
  // that stores the pointer to rocksdb::BackupableDBOptions
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::BackupableDBOptions
  static rocksdb::BackupableDBOptions* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::BackupableDBOptions*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::BackupableDBOptions pointer to the java side.
  static void setHandle(
      JNIEnv* env, jobject jobj, rocksdb::BackupableDBOptions* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class IteratorJni {
 public:
  // Get the java class id of org.rocksdb.Iteartor.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/RocksIterator");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.Iterator
  // that stores the pointer to rocksdb::Iterator.
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::Iterator.
  static rocksdb::Iterator* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::Iterator*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::Iterator pointer to the java side.
  static void setHandle(
      JNIEnv* env, jobject jobj, rocksdb::Iterator* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class FilterJni {
 public:
  // Get the java class id of org.rocksdb.FilterPolicy.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/Filter");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.Filter
  // that stores the pointer to rocksdb::FilterPolicy.
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::FilterPolicy.
  static std::shared_ptr<rocksdb::FilterPolicy>* getHandle(
      JNIEnv* env, jobject jobj) {
    return reinterpret_cast
        <std::shared_ptr<rocksdb::FilterPolicy> *>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::FilterPolicy pointer to the java side.
  static void setHandle(
      JNIEnv* env, jobject jobj, std::shared_ptr<rocksdb::FilterPolicy>* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class ColumnFamilyHandleJni {
 public:
  // Get the java class id of org.rocksdb.ColumnFamilyHandle.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/ColumnFamilyHandle");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.ColumnFamilyHandle.
  // that stores the pointer to rocksdb::ColumnFamilyHandle.
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::ColumnFamilyHandle.
  static rocksdb::ColumnFamilyHandle* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::ColumnFamilyHandle*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::ColumnFamilyHandle pointer to the java side.
  static void setHandle(
      JNIEnv* env, jobject jobj, const rocksdb::ColumnFamilyHandle* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class FlushOptionsJni {
 public:
    // Get the java class id of org.rocksdb.FlushOptions.
    static jclass getJClass(JNIEnv* env) {
      jclass jclazz = env->FindClass("org/rocksdb/FlushOptions");
      assert(jclazz != nullptr);
      return jclazz;
    }

    // Get the field id of the member variable of org.rocksdb.FlushOptions
    // that stores the pointer to rocksdb::FlushOptions.
    static jfieldID getHandleFieldID(JNIEnv* env) {
      static jfieldID fid = env->GetFieldID(
          getJClass(env), "nativeHandle_", "J");
      assert(fid != nullptr);
      return fid;
    }

    // Pass the FlushOptions pointer to the java side.
    static void setHandle(
      JNIEnv* env, jobject jobj,
      const rocksdb::FlushOptions* op) {
      env->SetLongField(
          jobj, getHandleFieldID(env),
          reinterpret_cast<jlong>(op));
    }
};

class ComparatorOptionsJni {
 public:
    // Get the java class id of org.rocksdb.ComparatorOptions.
    static jclass getJClass(JNIEnv* env) {
      jclass jclazz = env->FindClass("org/rocksdb/ComparatorOptions");
      assert(jclazz != nullptr);
      return jclazz;
    }

    // Get the field id of the member variable of org.rocksdb.ComparatorOptions
    // that stores the pointer to rocksdb::ComparatorJniCallbackOptions.
    static jfieldID getHandleFieldID(JNIEnv* env) {
      static jfieldID fid = env->GetFieldID(
          getJClass(env), "nativeHandle_", "J");
      assert(fid != nullptr);
      return fid;
    }

    // Pass the ComparatorJniCallbackOptions pointer to the java side.
    static void setHandle(
      JNIEnv* env, jobject jobj,
      const rocksdb::ComparatorJniCallbackOptions* op) {
      env->SetLongField(
          jobj, getHandleFieldID(env),
          reinterpret_cast<jlong>(op));
    }
};

class AbstractComparatorJni {
 public:
  // Get the java class id of org.rocksdb.Comparator.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/AbstractComparator");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.Comparator
  // that stores the pointer to rocksdb::Comparator.
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the java method `name` of org.rocksdb.Comparator.
  static jmethodID getNameMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "name", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `compare` of org.rocksdb.Comparator.
  static jmethodID getCompareMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(getJClass(env),
      "compare",
      "(Lorg/rocksdb/AbstractSlice;Lorg/rocksdb/AbstractSlice;)I");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `findShortestSeparator` of org.rocksdb.Comparator.
  static jmethodID getFindShortestSeparatorMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(getJClass(env),
      "findShortestSeparator",
      "(Ljava/lang/String;Lorg/rocksdb/AbstractSlice;)Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `findShortSuccessor` of org.rocksdb.Comparator.
  static jmethodID getFindShortSuccessorMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(getJClass(env),
      "findShortSuccessor",
      "(Ljava/lang/String;)Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  // Get the pointer to ComparatorJniCallback.
  static rocksdb::BaseComparatorJniCallback* getHandle(
    JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::BaseComparatorJniCallback*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the ComparatorJniCallback pointer to the java side.
  static void setHandle(
    JNIEnv* env, jobject jobj, const rocksdb::BaseComparatorJniCallback* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class AbstractSliceJni {
 public:
  // Get the java class id of org.rocksdb.Slice.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/AbstractSlice");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.Slice
  // that stores the pointer to rocksdb::Slice.
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to Slice.
  static rocksdb::Slice* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::Slice*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the Slice pointer to the java side.
  static void setHandle(
      JNIEnv* env, jobject jobj, const rocksdb::Slice* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class SliceJni {
 public:
  // Get the java class id of org.rocksdb.Slice.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/Slice");
    assert(jclazz != nullptr);
    return jclazz;
  }

  static jobject construct0(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(getJClass(env), "<init>", "()V");
    assert(mid != nullptr);
    return env->NewObject(getJClass(env), mid);
  }
};

class DirectSliceJni {
 public:
  // Get the java class id of org.rocksdb.DirectSlice.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/DirectSlice");
    assert(jclazz != nullptr);
    return jclazz;
  }

  static jobject construct0(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(getJClass(env), "<init>", "()V");
    assert(mid != nullptr);
    return env->NewObject(getJClass(env), mid);
  }
};

class ListJni {
 public:
  // Get the java class id of java.util.List.
  static jclass getListClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("java/util/List");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the java class id of java.util.ArrayList.
  static jclass getArrayListClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("java/util/ArrayList");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the java class id of java.util.Iterator.
  static jclass getIteratorClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("java/util/Iterator");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the java method id of java.util.List.iterator().
  static jmethodID getIteratorMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getListClass(env), "iterator", "()Ljava/util/Iterator;");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of java.util.Iterator.hasNext().
  static jmethodID getHasNextMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getIteratorClass(env), "hasNext", "()Z");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of java.util.Iterator.next().
  static jmethodID getNextMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getIteratorClass(env), "next", "()Ljava/lang/Object;");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of arrayList constructor.
  static jmethodID getArrayListConstructorMethodId(JNIEnv* env, jclass jclazz) {
    static jmethodID mid = env->GetMethodID(
        jclazz, "<init>", "(I)V");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of java.util.List.add().
  static jmethodID getListAddMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getListClass(env), "add", "(Ljava/lang/Object;)Z");
    assert(mid != nullptr);
    return mid;
  }
};

class BackupInfoJni {
 public:
  // Get the java class id of org.rocksdb.BackupInfo.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/BackupInfo");
    assert(jclazz != nullptr);
    return jclazz;
  }

  static jobject construct0(JNIEnv* env, uint32_t backup_id, int64_t timestamp,
      uint64_t size, uint32_t number_files) {
    static jmethodID mid = env->GetMethodID(getJClass(env), "<init>",
        "(IJJI)V");
    assert(mid != nullptr);
    return env->NewObject(getJClass(env), mid,
        backup_id, timestamp, size, number_files);
  }
};

class BackupInfoListJni {
 public:
  static jobject getBackupInfo(JNIEnv* env,
      std::vector<BackupInfo> backup_infos) {
    jclass jclazz = env->FindClass("java/util/ArrayList");
    jmethodID mid = rocksdb::ListJni::getArrayListConstructorMethodId(
        env, jclazz);
    jobject jbackup_info_handle_list = env->NewObject(jclazz, mid,
        backup_infos.size());
    // insert in java list
    for (std::vector<rocksdb::BackupInfo>::size_type i = 0;
        i != backup_infos.size(); i++) {
      rocksdb::BackupInfo backup_info = backup_infos[i];
      jobject obj = rocksdb::BackupInfoJni::construct0(env,
          backup_info.backup_id,
          backup_info.timestamp,
          backup_info.size,
          backup_info.number_files);
      env->CallBooleanMethod(jbackup_info_handle_list,
          rocksdb::ListJni::getListAddMethodId(env), obj);
    }
    return jbackup_info_handle_list;
  }
};

class JniUtil {
 public:
    /**
     * Copies a jstring to a std::string
     * and releases the original jstring
     */
    static std::string copyString(JNIEnv* env, jstring js) {
      const char *utf = env->GetStringUTFChars(js, NULL);
      std::string name(utf);
      env->ReleaseStringUTFChars(js, utf);
      return name;
    }
};

}  // namespace rocksdb
#endif  // JAVA_ROCKSJNI_PORTAL_H_
