// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::Options.

#include "rocksdb/options.h"

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include <memory>
#include <vector>

#include "include/org_rocksdb_ColumnFamilyOptions.h"
#include "include/org_rocksdb_ComparatorOptions.h"
#include "include/org_rocksdb_DBOptions.h"
#include "include/org_rocksdb_FlushOptions.h"
#include "include/org_rocksdb_Options.h"
#include "include/org_rocksdb_ReadOptions.h"
#include "include/org_rocksdb_WriteOptions.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/portal.h"
#include "rocksjni/statisticsjni.h"
#include "rocksjni/table_filter_jnicallback.h"
#include "utilities/merge_operators.h"

/*
 * Class:     org_rocksdb_Options
 * Method:    newOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_Options_newOptions__(
    JNIEnv*, jclass) {
  auto* op = new ROCKSDB_NAMESPACE::Options();
  return reinterpret_cast<jlong>(op);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    newOptions
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_Options_newOptions__JJ(
    JNIEnv*, jclass, jlong jdboptions, jlong jcfoptions) {
  auto* dbOpt =
      reinterpret_cast<const ROCKSDB_NAMESPACE::DBOptions*>(jdboptions);
  auto* cfOpt = reinterpret_cast<const ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(
      jcfoptions);
  auto* op = new ROCKSDB_NAMESPACE::Options(*dbOpt, *cfOpt);
  return reinterpret_cast<jlong>(op);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    copyOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_copyOptions(
    JNIEnv*, jclass, jlong jhandle) {
  auto new_opt = new ROCKSDB_NAMESPACE::Options(
      *(reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Options_disposeInternal(
    JNIEnv*, jobject, jlong handle) {
  auto* op = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(handle);
  assert(op != nullptr);
  delete op;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setIncreaseParallelism
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setIncreaseParallelism(
    JNIEnv*, jobject, jlong jhandle, jint totalThreads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->IncreaseParallelism(
      static_cast<int>(totalThreads));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCreateIfMissing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setCreateIfMissing(
    JNIEnv*, jobject, jlong jhandle, jboolean flag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->create_if_missing =
      flag;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    createIfMissing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_createIfMissing(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->create_if_missing;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCreateMissingColumnFamilies
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setCreateMissingColumnFamilies(
    JNIEnv*, jobject, jlong jhandle, jboolean flag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->create_missing_column_families = flag;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    createMissingColumnFamilies
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_createMissingColumnFamilies(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->create_missing_column_families;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setComparatorHandle
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setComparatorHandle__JI(
    JNIEnv*, jobject, jlong jhandle, jint builtinComparator) {
  switch (builtinComparator) {
    case 1:
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->comparator =
          ROCKSDB_NAMESPACE::ReverseBytewiseComparator();
      break;
    default:
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->comparator =
          ROCKSDB_NAMESPACE::BytewiseComparator();
      break;
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setComparatorHandle
 * Signature: (JJB)V
 */
void Java_org_rocksdb_Options_setComparatorHandle__JJB(
    JNIEnv*, jobject, jlong jopt_handle, jlong jcomparator_handle,
    jbyte jcomparator_type) {
  ROCKSDB_NAMESPACE::Comparator* comparator = nullptr;
  switch (jcomparator_type) {
    // JAVA_COMPARATOR
    case 0x0:
      comparator = reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallback*>(
          jcomparator_handle);
      break;

    // JAVA_NATIVE_COMPARATOR_WRAPPER
    case 0x1:
      comparator =
          reinterpret_cast<ROCKSDB_NAMESPACE::Comparator*>(jcomparator_handle);
      break;
  }
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jopt_handle);
  opt->comparator = comparator;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMergeOperatorName
 * Signature: (JJjava/lang/String)V
 */
void Java_org_rocksdb_Options_setMergeOperatorName(
    JNIEnv* env, jobject, jlong jhandle, jstring jop_name) {
  const char* op_name = env->GetStringUTFChars(jop_name, nullptr);
  if (op_name == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  options->merge_operator =
      ROCKSDB_NAMESPACE::MergeOperators::CreateFromStringId(op_name);

  env->ReleaseStringUTFChars(jop_name, op_name);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMergeOperator
 * Signature: (JJjava/lang/String)V
 */
void Java_org_rocksdb_Options_setMergeOperator(
    JNIEnv*, jobject, jlong jhandle, jlong mergeOperatorHandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->merge_operator =
      *(reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::MergeOperator>*>(
          mergeOperatorHandle));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionFilterHandle
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionFilterHandle(
    JNIEnv*, jobject, jlong jopt_handle,
    jlong jcompactionfilter_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jopt_handle)
      ->compaction_filter =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionFilter*>(
          jcompactionfilter_handle);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionFilterFactoryHandle
 * Signature: (JJ)V
 */
void JNICALL Java_org_rocksdb_Options_setCompactionFilterFactoryHandle(
    JNIEnv*, jobject, jlong jopt_handle,
    jlong jcompactionfilterfactory_handle) {
  auto* cff_factory = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::CompactionFilterFactory>*>(
      jcompactionfilterfactory_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jopt_handle)
      ->compaction_filter_factory = *cff_factory;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteBufferSize
 * Signature: (JJ)I
 */
void Java_org_rocksdb_Options_setWriteBufferSize(
    JNIEnv* env, jobject, jlong jhandle, jlong jwrite_buffer_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jwrite_buffer_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->write_buffer_size =
        jwrite_buffer_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteBufferManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWriteBufferManager(
    JNIEnv*, jobject, jlong joptions_handle,
    jlong jwrite_buffer_manager_handle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jwrite_buffer_manager_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(joptions_handle)
      ->write_buffer_manager = *write_buffer_manager;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writeBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writeBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->write_buffer_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxWriteBufferNumber
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxWriteBufferNumber(
    JNIEnv*, jobject, jlong jhandle,
    jint jmax_write_buffer_number) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_write_buffer_number = jmax_write_buffer_number;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setStatistics
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setStatistics(
    JNIEnv*, jobject, jlong jhandle, jlong jstatistics_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* pSptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::StatisticsJni>*>(
          jstatistics_handle);
  opt->statistics = *pSptr;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    statistics
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_statistics(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  std::shared_ptr<ROCKSDB_NAMESPACE::Statistics> sptr = opt->statistics;
  if (sptr == nullptr) {
    return 0;
  } else {
    std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>* pSptr =
        new std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>(sptr);
    return reinterpret_cast<jlong>(pSptr);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxWriteBufferNumber
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxWriteBufferNumber(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_write_buffer_number;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    errorIfExists
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_errorIfExists(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->error_if_exists;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setErrorIfExists
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setErrorIfExists(
    JNIEnv*, jobject, jlong jhandle, jboolean error_if_exists) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->error_if_exists =
      static_cast<bool>(error_if_exists);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    paranoidChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_paranoidChecks(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->paranoid_checks;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setParanoidChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setParanoidChecks(
    JNIEnv*, jobject, jlong jhandle, jboolean paranoid_checks) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->paranoid_checks =
      static_cast<bool>(paranoid_checks);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnv
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setEnv(
    JNIEnv*, jobject, jlong jhandle, jlong jenv) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->env =
      reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jenv);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxTotalWalSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxTotalWalSize(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_total_wal_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_total_wal_size =
      static_cast<jlong>(jmax_total_wal_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxTotalWalSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxTotalWalSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_total_wal_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxOpenFiles
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxOpenFiles(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_open_files;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxOpenFiles
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxOpenFiles(
    JNIEnv*, jobject, jlong jhandle, jint max_open_files) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_open_files =
      static_cast<int>(max_open_files);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxFileOpeningThreads
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxFileOpeningThreads(
    JNIEnv*, jobject, jlong jhandle, jint jmax_file_opening_threads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_file_opening_threads = static_cast<int>(jmax_file_opening_threads);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxFileOpeningThreads
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxFileOpeningThreads(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<int>(opt->max_file_opening_threads);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useFsync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useFsync(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->use_fsync;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseFsync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseFsync(
    JNIEnv*, jobject, jlong jhandle, jboolean use_fsync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->use_fsync =
      static_cast<bool>(use_fsync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_Options_setDbPaths(
    JNIEnv* env, jobject, jlong jhandle, jobjectArray jpaths,
    jlongArray jtarget_sizes) {
  std::vector<ROCKSDB_NAMESPACE::DbPath> db_paths;
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, nullptr);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jboolean has_exception = JNI_FALSE;
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    jobject jpath =
        reinterpret_cast<jstring>(env->GetObjectArrayElement(jpaths, i));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    std::string path = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
        env, static_cast<jstring>(jpath), &has_exception);
    env->DeleteLocalRef(jpath);

    if (has_exception == JNI_TRUE) {
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    jlong jtarget_size = ptr_jtarget_size[i];

    db_paths.push_back(
        ROCKSDB_NAMESPACE::DbPath(path, static_cast<uint64_t>(jtarget_size)));
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->db_paths = db_paths;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dbPathsLen
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_dbPathsLen(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->db_paths.size());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_Options_dbPaths(
    JNIEnv* env, jobject, jlong jhandle, jobjectArray jpaths,
    jlongArray jtarget_sizes) {
  jboolean is_copy;
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, &is_copy);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    ROCKSDB_NAMESPACE::DbPath db_path = opt->db_paths[i];

    jstring jpath = env->NewStringUTF(db_path.path.c_str());
    if (jpath == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    env->SetObjectArrayElement(jpaths, i, jpath);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jpath);
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    ptr_jtarget_size[i] = static_cast<jint>(db_path.target_size);
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size,
                                is_copy == JNI_TRUE ? 0 : JNI_ABORT);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dbLogDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_dbLogDir(
    JNIEnv* env, jobject, jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
          ->db_log_dir.c_str());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDbLogDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_Options_setDbLogDir(
    JNIEnv* env, jobject, jlong jhandle, jstring jdb_log_dir) {
  const char* log_dir = env->GetStringUTFChars(jdb_log_dir, nullptr);
  if (log_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->db_log_dir.assign(
      log_dir);
  env->ReleaseStringUTFChars(jdb_log_dir, log_dir);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_walDir(
    JNIEnv* env, jobject, jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->wal_dir.c_str());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_Options_setWalDir(
    JNIEnv* env, jobject, jlong jhandle, jstring jwal_dir) {
  const char* wal_dir = env->GetStringUTFChars(jwal_dir, nullptr);
  if (wal_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->wal_dir.assign(
      wal_dir);
  env->ReleaseStringUTFChars(jwal_dir, wal_dir);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    deleteObsoleteFilesPeriodMicros
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_deleteObsoleteFilesPeriodMicros(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->delete_obsolete_files_period_micros;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDeleteObsoleteFilesPeriodMicros
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setDeleteObsoleteFilesPeriodMicros(
    JNIEnv*, jobject, jlong jhandle, jlong micros) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->delete_obsolete_files_period_micros = static_cast<int64_t>(micros);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBaseBackgroundCompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setBaseBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle, jint max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->base_background_compactions = static_cast<int>(max);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    baseBackgroundCompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_baseBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->base_background_compactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBackgroundCompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_background_compactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBackgroundCompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle, jint max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_background_compactions = static_cast<int>(max);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxSubcompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxSubcompactions(
    JNIEnv*, jobject, jlong jhandle, jint max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_subcompactions =
      static_cast<int32_t>(max);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxSubcompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxSubcompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_subcompactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBackgroundFlushes
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBackgroundFlushes(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_background_flushes;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBackgroundFlushes
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBackgroundFlushes(
    JNIEnv*, jobject, jlong jhandle, jint max_background_flushes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_background_flushes = static_cast<int>(max_background_flushes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBackgroundJobs
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBackgroundJobs(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_background_jobs;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBackgroundJobs
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBackgroundJobs(
    JNIEnv*, jobject, jlong jhandle, jint max_background_jobs) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_background_jobs =
      static_cast<int>(max_background_jobs);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxLogFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxLogFileSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_log_file_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxLogFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxLogFileSize(
    JNIEnv* env, jobject, jlong jhandle, jlong max_log_file_size) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(max_log_file_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_log_file_size =
        max_log_file_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    logFileTimeToRoll
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_logFileTimeToRoll(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->log_file_time_to_roll;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLogFileTimeToRoll
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setLogFileTimeToRoll(
    JNIEnv* env, jobject, jlong jhandle, jlong log_file_time_to_roll) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      log_file_time_to_roll);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
        ->log_file_time_to_roll = log_file_time_to_roll;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    keepLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_keepLogFileNum(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->keep_log_file_num;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setKeepLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setKeepLogFileNum(
    JNIEnv* env, jobject, jlong jhandle, jlong keep_log_file_num) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(keep_log_file_num);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->keep_log_file_num =
        keep_log_file_num;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    recycleLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_recycleLogFileNum(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->recycle_log_file_num;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setRecycleLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setRecycleLogFileNum(
    JNIEnv* env, jobject, jlong jhandle, jlong recycle_log_file_num) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      recycle_log_file_num);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
        ->recycle_log_file_num = recycle_log_file_num;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxManifestFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxManifestFileSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_manifest_file_size;
}

/*
 * Method:    memTableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_memTableFactoryName(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  ROCKSDB_NAMESPACE::MemTableRepFactory* tf = opt->memtable_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  // temporarly fix for the historical typo
  if (strcmp(tf->Name(), "HashLinkListRepFactory") == 0) {
    return env->NewStringUTF("HashLinkedListRepFactory");
  }

  return env->NewStringUTF(tf->Name());
}

static std::vector<ROCKSDB_NAMESPACE::DbPath>
rocksdb_convert_cf_paths_from_java_helper(JNIEnv* env, jobjectArray path_array,
                                          jlongArray size_array,
                                          jboolean* has_exception) {
  jboolean copy_str_has_exception;
  std::vector<std::string> paths = ROCKSDB_NAMESPACE::JniUtil::copyStrings(
      env, path_array, &copy_str_has_exception);
  if (JNI_TRUE == copy_str_has_exception) {
    // Exception thrown
    *has_exception = JNI_TRUE;
    return {};
  }

  if (static_cast<size_t>(env->GetArrayLength(size_array)) != paths.size()) {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(
        env,
        ROCKSDB_NAMESPACE::Status::InvalidArgument(
            ROCKSDB_NAMESPACE::Slice("There should be a corresponding target "
                                     "size for every path and vice versa.")));
    *has_exception = JNI_TRUE;
    return {};
  }

  jlong* size_array_ptr = env->GetLongArrayElements(size_array, nullptr);
  if (nullptr == size_array_ptr) {
    // exception thrown: OutOfMemoryError
    *has_exception = JNI_TRUE;
    return {};
  }
  std::vector<ROCKSDB_NAMESPACE::DbPath> cf_paths;
  for (size_t i = 0; i < paths.size(); ++i) {
    jlong target_size = size_array_ptr[i];
    if (target_size < 0) {
      ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(
          env,
          ROCKSDB_NAMESPACE::Status::InvalidArgument(ROCKSDB_NAMESPACE::Slice(
              "Path target size has to be positive.")));
      *has_exception = JNI_TRUE;
      env->ReleaseLongArrayElements(size_array, size_array_ptr, JNI_ABORT);
      return {};
    }
    cf_paths.push_back(ROCKSDB_NAMESPACE::DbPath(
        paths[i], static_cast<uint64_t>(target_size)));
  }

  env->ReleaseLongArrayElements(size_array, size_array_ptr, JNI_ABORT);

  return cf_paths;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCfPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_Options_setCfPaths(JNIEnv* env, jclass, jlong jhandle,
                                         jobjectArray path_array,
                                         jlongArray size_array) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  jboolean has_exception = JNI_FALSE;
  std::vector<ROCKSDB_NAMESPACE::DbPath> cf_paths =
      rocksdb_convert_cf_paths_from_java_helper(env, path_array, size_array,
                                                &has_exception);
  if (JNI_FALSE == has_exception) {
    options->cf_paths = std::move(cf_paths);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    cfPathsLen
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_cfPathsLen(JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->cf_paths.size());
}

template <typename T>
static void rocksdb_convert_cf_paths_to_java_helper(JNIEnv* env, jlong jhandle,
                                                    jobjectArray jpaths,
                                                    jlongArray jtarget_sizes) {
  jboolean is_copy;
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, &is_copy);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* opt = reinterpret_cast<T*>(jhandle);
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    ROCKSDB_NAMESPACE::DbPath cf_path = opt->cf_paths[i];

    jstring jpath = env->NewStringUTF(cf_path.path.c_str());
    if (jpath == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    env->SetObjectArrayElement(jpaths, i, jpath);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jpath);
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    ptr_jtarget_size[i] = static_cast<jint>(cf_path.target_size);

    env->DeleteLocalRef(jpath);
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size,
                                is_copy ? 0 : JNI_ABORT);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    cfPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_Options_cfPaths(JNIEnv* env, jclass, jlong jhandle,
                                      jobjectArray jpaths,
                                      jlongArray jtarget_sizes) {
  rocksdb_convert_cf_paths_to_java_helper<ROCKSDB_NAMESPACE::Options>(
      env, jhandle, jpaths, jtarget_sizes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxManifestFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxManifestFileSize(
    JNIEnv*, jobject, jlong jhandle, jlong max_manifest_file_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_manifest_file_size = static_cast<int64_t>(max_manifest_file_size);
}

/*
 * Method:    setMemTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMemTableFactory(
    JNIEnv*, jobject, jlong jhandle, jlong jfactory_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->memtable_factory.reset(
          reinterpret_cast<ROCKSDB_NAMESPACE::MemTableRepFactory*>(
              jfactory_handle));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setRateLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setRateLimiter(
    JNIEnv*, jobject, jlong jhandle, jlong jrate_limiter_handle) {
  std::shared_ptr<ROCKSDB_NAMESPACE::RateLimiter>* pRateLimiter =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::RateLimiter>*>(
          jrate_limiter_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->rate_limiter =
      *pRateLimiter;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setSstFileManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setSstFileManager(
    JNIEnv*, jobject, jlong jhandle, jlong jsst_file_manager_handle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jsst_file_manager_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->sst_file_manager =
      *sptr_sst_file_manager;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLogger
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setLogger(
    JNIEnv*, jobject, jlong jhandle, jlong jlogger_handle) {
  std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>* pLogger =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>*>(
          jlogger_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->info_log = *pLogger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setInfoLogLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setInfoLogLevel(
    JNIEnv*, jobject, jlong jhandle, jbyte jlog_level) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->info_log_level =
      static_cast<ROCKSDB_NAMESPACE::InfoLogLevel>(jlog_level);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    infoLogLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_infoLogLevel(
    JNIEnv*, jobject, jlong jhandle) {
  return static_cast<jbyte>(
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->info_log_level);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    tableCacheNumshardbits
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_tableCacheNumshardbits(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->table_cache_numshardbits;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTableCacheNumshardbits
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setTableCacheNumshardbits(
    JNIEnv*, jobject, jlong jhandle, jint table_cache_numshardbits) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->table_cache_numshardbits = static_cast<int>(table_cache_numshardbits);
}

/*
 * Method:    useFixedLengthPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_useFixedLengthPrefixExtractor(
    JNIEnv*, jobject, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->prefix_extractor.reset(ROCKSDB_NAMESPACE::NewFixedPrefixTransform(
          static_cast<int>(jprefix_length)));
}

/*
 * Method:    useCappedPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_useCappedPrefixExtractor(
    JNIEnv*, jobject, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->prefix_extractor.reset(ROCKSDB_NAMESPACE::NewCappedPrefixTransform(
          static_cast<int>(jprefix_length)));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_walTtlSeconds(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->WAL_ttl_seconds;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalTtlSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalTtlSeconds(
    JNIEnv*, jobject, jlong jhandle, jlong WAL_ttl_seconds) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->WAL_ttl_seconds =
      static_cast<int64_t>(WAL_ttl_seconds);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_walSizeLimitMB(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->WAL_size_limit_MB;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalSizeLimitMB
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalSizeLimitMB(
    JNIEnv*, jobject, jlong jhandle, jlong WAL_size_limit_MB) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->WAL_size_limit_MB =
      static_cast<int64_t>(WAL_size_limit_MB);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxWriteBatchGroupSizeBytes
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxWriteBatchGroupSizeBytes(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_write_batch_group_size_bytes) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->max_write_batch_group_size_bytes =
      static_cast<uint64_t>(jmax_write_batch_group_size_bytes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxWriteBatchGroupSizeBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxWriteBatchGroupSizeBytes(JNIEnv*, jclass,
                                                           jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->max_write_batch_group_size_bytes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    manifestPreallocationSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_manifestPreallocationSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->manifest_preallocation_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setManifestPreallocationSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setManifestPreallocationSize(
    JNIEnv* env, jobject, jlong jhandle, jlong preallocation_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      preallocation_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
        ->manifest_preallocation_size = preallocation_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Method:    setTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setTableFactory(
    JNIEnv*, jobject, jlong jhandle, jlong jtable_factory_handle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* table_factory =
      reinterpret_cast<ROCKSDB_NAMESPACE::TableFactory*>(jtable_factory_handle);
  options->table_factory.reset(table_factory);
}

/*
 * Method:    setSstPartitionerFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setSstPartitionerFactory(JNIEnv*, jobject,
                                                       jlong jhandle,
                                                       jlong factory_handle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto factory = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::SstPartitionerFactory>*>(
      factory_handle);
  options->sst_partitioner_factory = *factory;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionThreadLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionThreadLimiter(
    JNIEnv*, jclass, jlong jhandle, jlong jlimiter_handle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* limiter = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter>*>(
      jlimiter_handle);
  options->compaction_thread_limiter = *limiter;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowMmapReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowMmapReads(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->allow_mmap_reads;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowMmapReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowMmapReads(
    JNIEnv*, jobject, jlong jhandle, jboolean allow_mmap_reads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->allow_mmap_reads =
      static_cast<bool>(allow_mmap_reads);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowMmapWrites
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowMmapWrites(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->allow_mmap_writes;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowMmapWrites
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowMmapWrites(
    JNIEnv*, jobject, jlong jhandle, jboolean allow_mmap_writes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->allow_mmap_writes =
      static_cast<bool>(allow_mmap_writes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useDirectReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useDirectReads(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->use_direct_reads;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseDirectReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseDirectReads(
    JNIEnv*, jobject, jlong jhandle, jboolean use_direct_reads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->use_direct_reads =
      static_cast<bool>(use_direct_reads);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useDirectIoForFlushAndCompaction
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useDirectIoForFlushAndCompaction(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->use_direct_io_for_flush_and_compaction;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseDirectIoForFlushAndCompaction
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseDirectIoForFlushAndCompaction(
    JNIEnv*, jobject, jlong jhandle,
    jboolean use_direct_io_for_flush_and_compaction) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->use_direct_io_for_flush_and_compaction =
      static_cast<bool>(use_direct_io_for_flush_and_compaction);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowFAllocate
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowFAllocate(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_fallocate) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->allow_fallocate =
      static_cast<bool>(jallow_fallocate);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowFAllocate
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowFAllocate(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->allow_fallocate);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    isFdCloseOnExec
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_isFdCloseOnExec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->is_fd_close_on_exec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setIsFdCloseOnExec
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setIsFdCloseOnExec(
    JNIEnv*, jobject, jlong jhandle, jboolean is_fd_close_on_exec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->is_fd_close_on_exec =
      static_cast<bool>(is_fd_close_on_exec);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    statsDumpPeriodSec
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_statsDumpPeriodSec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->stats_dump_period_sec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setStatsDumpPeriodSec
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setStatsDumpPeriodSec(
    JNIEnv*, jobject, jlong jhandle,
    jint jstats_dump_period_sec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->stats_dump_period_sec =
      static_cast<unsigned int>(jstats_dump_period_sec);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    statsPersistPeriodSec
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_statsPersistPeriodSec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->stats_persist_period_sec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setStatsPersistPeriodSec
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setStatsPersistPeriodSec(
    JNIEnv*, jobject, jlong jhandle, jint jstats_persist_period_sec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->stats_persist_period_sec =
      static_cast<unsigned int>(jstats_persist_period_sec);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    statsHistoryBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_statsHistoryBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->stats_history_buffer_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setStatsHistoryBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setStatsHistoryBufferSize(
    JNIEnv*, jobject, jlong jhandle, jlong jstats_history_buffer_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->stats_history_buffer_size =
      static_cast<size_t>(jstats_history_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    adviseRandomOnOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_adviseRandomOnOpen(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->advise_random_on_open;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAdviseRandomOnOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAdviseRandomOnOpen(
    JNIEnv*, jobject, jlong jhandle,
    jboolean advise_random_on_open) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->advise_random_on_open = static_cast<bool>(advise_random_on_open);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDbWriteBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setDbWriteBufferSize(
    JNIEnv*, jobject, jlong jhandle,
    jlong jdb_write_buffer_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->db_write_buffer_size = static_cast<size_t>(jdb_write_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dbWriteBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_dbWriteBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->db_write_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAccessHintOnCompactionStart
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setAccessHintOnCompactionStart(
    JNIEnv*, jobject, jlong jhandle,
    jbyte jaccess_hint_value) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->access_hint_on_compaction_start =
      ROCKSDB_NAMESPACE::AccessHintJni::toCppAccessHint(jaccess_hint_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    accessHintOnCompactionStart
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_accessHintOnCompactionStart(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return ROCKSDB_NAMESPACE::AccessHintJni::toJavaAccessHint(
      opt->access_hint_on_compaction_start);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setNewTableReaderForCompactionInputs
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setNewTableReaderForCompactionInputs(
    JNIEnv*, jobject, jlong jhandle,
    jboolean jnew_table_reader_for_compaction_inputs) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->new_table_reader_for_compaction_inputs =
      static_cast<bool>(jnew_table_reader_for_compaction_inputs);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    newTableReaderForCompactionInputs
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_newTableReaderForCompactionInputs(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<bool>(opt->new_table_reader_for_compaction_inputs);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionReadaheadSize(
    JNIEnv*, jobject, jlong jhandle,
    jlong jcompaction_readahead_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->compaction_readahead_size =
      static_cast<size_t>(jcompaction_readahead_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compactionReadaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_compactionReadaheadSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setRandomAccessMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setRandomAccessMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle, jlong jrandom_access_max_buffer_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->random_access_max_buffer_size =
      static_cast<size_t>(jrandom_access_max_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    randomAccessMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_randomAccessMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->random_access_max_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWritableFileMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWritableFileMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle,
    jlong jwritable_file_max_buffer_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->writable_file_max_buffer_size =
      static_cast<size_t>(jwritable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writableFileMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writableFileMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->writable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useAdaptiveMutex
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useAdaptiveMutex(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->use_adaptive_mutex;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseAdaptiveMutex
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseAdaptiveMutex(
    JNIEnv*, jobject, jlong jhandle, jboolean use_adaptive_mutex) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->use_adaptive_mutex =
      static_cast<bool>(use_adaptive_mutex);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_bytesPerSync(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->bytes_per_sync;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setBytesPerSync(
    JNIEnv*, jobject, jlong jhandle, jlong bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->bytes_per_sync =
      static_cast<int64_t>(bytes_per_sync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalBytesPerSync(
    JNIEnv*, jobject, jlong jhandle, jlong jwal_bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->wal_bytes_per_sync =
      static_cast<int64_t>(jwal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walBytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_walBytesPerSync(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->wal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setStrictBytesPerSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setStrictBytesPerSync(
    JNIEnv*, jobject, jlong jhandle, jboolean jstrict_bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->strict_bytes_per_sync = jstrict_bytes_per_sync == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    strictBytesPerSync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_strictBytesPerSync(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->strict_bytes_per_sync);
}

// Note: the RocksJava API currently only supports EventListeners implemented in
// Java. It could be extended in future to also support adding/removing
// EventListeners implemented in C++.
static void rocksdb_set_event_listeners_helper(
    JNIEnv* env, jlongArray jlistener_array,
    std::vector<std::shared_ptr<ROCKSDB_NAMESPACE::EventListener>>&
        listener_sptr_vec) {
  jlong* ptr_jlistener_array =
      env->GetLongArrayElements(jlistener_array, nullptr);
  if (ptr_jlistener_array == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  const jsize array_size = env->GetArrayLength(jlistener_array);
  listener_sptr_vec.clear();
  for (jsize i = 0; i < array_size; ++i) {
    const auto& listener_sptr =
        *reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::EventListener>*>(
            ptr_jlistener_array[i]);
    listener_sptr_vec.push_back(listener_sptr);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEventListeners
 * Signature: (J[J)V
 */
void Java_org_rocksdb_Options_setEventListeners(JNIEnv* env, jclass,
                                                jlong jhandle,
                                                jlongArray jlistener_array) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  rocksdb_set_event_listeners_helper(env, jlistener_array, opt->listeners);
}

// Note: the RocksJava API currently only supports EventListeners implemented in
// Java. It could be extended in future to also support adding/removing
// EventListeners implemented in C++.
static jobjectArray rocksdb_get_event_listeners_helper(
    JNIEnv* env,
    const std::vector<std::shared_ptr<ROCKSDB_NAMESPACE::EventListener>>&
        listener_sptr_vec) {
  jsize sz = static_cast<jsize>(listener_sptr_vec.size());
  jclass jlistener_clazz =
      ROCKSDB_NAMESPACE::AbstractEventListenerJni::getJClass(env);
  jobjectArray jlisteners = env->NewObjectArray(sz, jlistener_clazz, nullptr);
  if (jlisteners == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  for (jsize i = 0; i < sz; ++i) {
    const auto* jni_cb =
        static_cast<ROCKSDB_NAMESPACE::EventListenerJniCallback*>(
            listener_sptr_vec[i].get());
    env->SetObjectArrayElement(jlisteners, i, jni_cb->GetJavaObject());
  }
  return jlisteners;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    eventListeners
 * Signature: (J)[Lorg/rocksdb/AbstractEventListener;
 */
jobjectArray Java_org_rocksdb_Options_eventListeners(JNIEnv* env, jclass,
                                                     jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return rocksdb_get_event_listeners_helper(env, opt->listeners);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnableThreadTracking
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setEnableThreadTracking(
    JNIEnv*, jobject, jlong jhandle, jboolean jenable_thread_tracking) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->enable_thread_tracking = static_cast<bool>(jenable_thread_tracking);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    enableThreadTracking
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_enableThreadTracking(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->enable_thread_tracking);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDelayedWriteRate
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setDelayedWriteRate(
    JNIEnv*, jobject, jlong jhandle, jlong jdelayed_write_rate) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->delayed_write_rate = static_cast<uint64_t>(jdelayed_write_rate);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    delayedWriteRate
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_delayedWriteRate(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->delayed_write_rate);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnablePipelinedWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setEnablePipelinedWrite(
    JNIEnv*, jobject, jlong jhandle, jboolean jenable_pipelined_write) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->enable_pipelined_write = jenable_pipelined_write == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    enablePipelinedWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_enablePipelinedWrite(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->enable_pipelined_write);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUnorderedWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUnorderedWrite(
    JNIEnv*, jobject, jlong jhandle, jboolean unordered_write) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->unordered_write =
      static_cast<bool>(unordered_write);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    unorderedWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_unorderedWrite(
        JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->unordered_write;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowConcurrentMemtableWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowConcurrentMemtableWrite(
    JNIEnv*, jobject, jlong jhandle, jboolean allow) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->allow_concurrent_memtable_write = static_cast<bool>(allow);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowConcurrentMemtableWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowConcurrentMemtableWrite(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->allow_concurrent_memtable_write;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnableWriteThreadAdaptiveYield
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setEnableWriteThreadAdaptiveYield(
    JNIEnv*, jobject, jlong jhandle, jboolean yield) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->enable_write_thread_adaptive_yield = static_cast<bool>(yield);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    enableWriteThreadAdaptiveYield
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_enableWriteThreadAdaptiveYield(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->enable_write_thread_adaptive_yield;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteThreadMaxYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWriteThreadMaxYieldUsec(
    JNIEnv*, jobject, jlong jhandle, jlong max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->write_thread_max_yield_usec = static_cast<int64_t>(max);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writeThreadMaxYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writeThreadMaxYieldUsec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->write_thread_max_yield_usec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteThreadSlowYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWriteThreadSlowYieldUsec(
    JNIEnv*, jobject, jlong jhandle, jlong slow) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->write_thread_slow_yield_usec = static_cast<int64_t>(slow);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writeThreadSlowYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writeThreadSlowYieldUsec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->write_thread_slow_yield_usec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setSkipStatsUpdateOnDbOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setSkipStatsUpdateOnDbOpen(
    JNIEnv*, jobject, jlong jhandle,
    jboolean jskip_stats_update_on_db_open) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->skip_stats_update_on_db_open =
      static_cast<bool>(jskip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    skipStatsUpdateOnDbOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_skipStatsUpdateOnDbOpen(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->skip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setSkipCheckingSstFileSizesOnDbOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setSkipCheckingSstFileSizesOnDbOpen(
    JNIEnv*, jclass, jlong jhandle,
    jboolean jskip_checking_sst_file_sizes_on_db_open) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->skip_checking_sst_file_sizes_on_db_open =
      static_cast<bool>(jskip_checking_sst_file_sizes_on_db_open);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    skipCheckingSstFileSizesOnDbOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_skipCheckingSstFileSizesOnDbOpen(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->skip_checking_sst_file_sizes_on_db_open);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalRecoveryMode
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setWalRecoveryMode(
    JNIEnv*, jobject, jlong jhandle,
    jbyte jwal_recovery_mode_value) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->wal_recovery_mode =
      ROCKSDB_NAMESPACE::WALRecoveryModeJni::toCppWALRecoveryMode(
          jwal_recovery_mode_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walRecoveryMode
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_walRecoveryMode(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return ROCKSDB_NAMESPACE::WALRecoveryModeJni::toJavaWALRecoveryMode(
      opt->wal_recovery_mode);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllow2pc
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllow2pc(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_2pc) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->allow_2pc = static_cast<bool>(jallow_2pc);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allow2pc
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allow2pc(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->allow_2pc);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setRowCache
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setRowCache(
    JNIEnv*, jobject, jlong jhandle, jlong jrow_cache_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* row_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(
          jrow_cache_handle);
  opt->row_cache = *row_cache;
}


/*
 * Class:     org_rocksdb_Options
 * Method:    setWalFilter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalFilter(
    JNIEnv*, jobject, jlong jhandle, jlong jwal_filter_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* wal_filter = reinterpret_cast<ROCKSDB_NAMESPACE::WalFilterJniCallback*>(
      jwal_filter_handle);
  opt->wal_filter = wal_filter;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setFailIfOptionsFileError
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setFailIfOptionsFileError(
    JNIEnv*, jobject, jlong jhandle, jboolean jfail_if_options_file_error) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->fail_if_options_file_error =
      static_cast<bool>(jfail_if_options_file_error);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    failIfOptionsFileError
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_failIfOptionsFileError(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->fail_if_options_file_error);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDumpMallocStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setDumpMallocStats(
    JNIEnv*, jobject, jlong jhandle, jboolean jdump_malloc_stats) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->dump_malloc_stats = static_cast<bool>(jdump_malloc_stats);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dumpMallocStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_dumpMallocStats(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->dump_malloc_stats);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAvoidFlushDuringRecovery
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAvoidFlushDuringRecovery(
    JNIEnv*, jobject, jlong jhandle, jboolean javoid_flush_during_recovery) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->avoid_flush_during_recovery =
      static_cast<bool>(javoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    avoidFlushDuringRecovery
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_avoidFlushDuringRecovery(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->avoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAvoidUnnecessaryBlockingIO
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAvoidUnnecessaryBlockingIO(
    JNIEnv*, jclass, jlong jhandle, jboolean avoid_blocking_io) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->avoid_unnecessary_blocking_io = static_cast<bool>(avoid_blocking_io);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    avoidUnnecessaryBlockingIO
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_avoidUnnecessaryBlockingIO(JNIEnv*, jclass,
                                                             jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->avoid_unnecessary_blocking_io);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setPersistStatsToDisk
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setPersistStatsToDisk(
    JNIEnv*, jclass, jlong jhandle, jboolean persist_stats_to_disk) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->persist_stats_to_disk = static_cast<bool>(persist_stats_to_disk);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    persistStatsToDisk
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_persistStatsToDisk(JNIEnv*, jclass,
                                                     jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->persist_stats_to_disk);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteDbidToManifest
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setWriteDbidToManifest(
    JNIEnv*, jclass, jlong jhandle, jboolean jwrite_dbid_to_manifest) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->write_dbid_to_manifest = static_cast<bool>(jwrite_dbid_to_manifest);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writeDbidToManifest
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_writeDbidToManifest(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->write_dbid_to_manifest);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLogReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setLogReadaheadSize(JNIEnv*, jclass,
                                                  jlong jhandle,
                                                  jlong jlog_readahead_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->log_readahead_size = static_cast<size_t>(jlog_readahead_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    logReasaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_logReadaheadSize(JNIEnv*, jclass,
                                                jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->log_readahead_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBestEffortsRecovery
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setBestEffortsRecovery(
    JNIEnv*, jclass, jlong jhandle, jboolean jbest_efforts_recovery) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->best_efforts_recovery = static_cast<bool>(jbest_efforts_recovery);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bestEffortsRecovery
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_bestEffortsRecovery(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->best_efforts_recovery);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBgErrorResumeCount
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBgErrorResumeCount(
    JNIEnv*, jclass, jlong jhandle, jint jmax_bgerror_resume_count) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->max_bgerror_resume_count = static_cast<int>(jmax_bgerror_resume_count);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBgerrorResumeCount
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBgerrorResumeCount(JNIEnv*, jclass,
                                                    jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jint>(opt->max_bgerror_resume_count);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBgerrorResumeRetryInterval
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setBgerrorResumeRetryInterval(
    JNIEnv*, jclass, jlong jhandle, jlong jbgerror_resume_retry_interval) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->bgerror_resume_retry_interval =
      static_cast<uint64_t>(jbgerror_resume_retry_interval);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bgerrorResumeRetryInterval
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_bgerrorResumeRetryInterval(JNIEnv*, jclass,
                                                          jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opt->bgerror_resume_retry_interval);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAvoidFlushDuringShutdown
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAvoidFlushDuringShutdown(
    JNIEnv*, jobject, jlong jhandle, jboolean javoid_flush_during_shutdown) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->avoid_flush_during_shutdown =
      static_cast<bool>(javoid_flush_during_shutdown);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    avoidFlushDuringShutdown
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_avoidFlushDuringShutdown(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->avoid_flush_during_shutdown);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowIngestBehind
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowIngestBehind(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_ingest_behind) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->allow_ingest_behind = jallow_ingest_behind == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowIngestBehind
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowIngestBehind(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->allow_ingest_behind);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setPreserveDeletes
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setPreserveDeletes(
    JNIEnv*, jobject, jlong jhandle, jboolean jpreserve_deletes) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->preserve_deletes = jpreserve_deletes == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    preserveDeletes
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_preserveDeletes(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->preserve_deletes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTwoWriteQueues
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setTwoWriteQueues(
    JNIEnv*, jobject, jlong jhandle, jboolean jtwo_write_queues) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->two_write_queues = jtwo_write_queues == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    twoWriteQueues
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_twoWriteQueues(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->two_write_queues);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setManualWalFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setManualWalFlush(
    JNIEnv*, jobject, jlong jhandle, jboolean jmanual_wal_flush) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->manual_wal_flush = jmanual_wal_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    manualWalFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_manualWalFlush(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->manual_wal_flush);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAtomicFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAtomicFlush(
    JNIEnv*, jobject, jlong jhandle, jboolean jatomic_flush) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->atomic_flush = jatomic_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    atomicFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_atomicFlush(
    JNIEnv *, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jboolean>(opt->atomic_flush);
}

/*
 * Method:    tableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_tableFactoryName(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  ROCKSDB_NAMESPACE::TableFactory* tf = opt->table_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  return env->NewStringUTF(tf->Name());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    minWriteBufferNumberToMerge
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_minWriteBufferNumberToMerge(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->min_write_buffer_number_to_merge;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMinWriteBufferNumberToMerge
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMinWriteBufferNumberToMerge(
    JNIEnv*, jobject, jlong jhandle, jint jmin_write_buffer_number_to_merge) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->min_write_buffer_number_to_merge =
      static_cast<int>(jmin_write_buffer_number_to_merge);
}
/*
 * Class:     org_rocksdb_Options
 * Method:    maxWriteBufferNumberToMaintain
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxWriteBufferNumberToMaintain(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_write_buffer_number_to_maintain;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxWriteBufferNumberToMaintain
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxWriteBufferNumberToMaintain(
    JNIEnv*, jobject, jlong jhandle,
    jint jmax_write_buffer_number_to_maintain) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_write_buffer_number_to_maintain =
      static_cast<int>(jmax_write_buffer_number_to_maintain);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setCompressionType(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompression_type_value) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->compression =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_compressionType(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      opts->compression);
}

/**
 * Helper method to convert a Java byte array of compression levels
 * to a C++ vector of ROCKSDB_NAMESPACE::CompressionType
 *
 * @param env A pointer to the Java environment
 * @param jcompression_levels A reference to a java byte array
 *     where each byte indicates a compression level
 *
 * @return A std::unique_ptr to the vector, or std::unique_ptr(nullptr) if a JNI
 * exception occurs
 */
std::unique_ptr<std::vector<ROCKSDB_NAMESPACE::CompressionType>>
rocksdb_compression_vector_helper(JNIEnv* env, jbyteArray jcompression_levels) {
  jsize len = env->GetArrayLength(jcompression_levels);
  jbyte* jcompression_level =
      env->GetByteArrayElements(jcompression_levels, nullptr);
  if (jcompression_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return std::unique_ptr<std::vector<ROCKSDB_NAMESPACE::CompressionType>>();
  }

  auto* compression_levels =
      new std::vector<ROCKSDB_NAMESPACE::CompressionType>();
  std::unique_ptr<std::vector<ROCKSDB_NAMESPACE::CompressionType>>
      uptr_compression_levels(compression_levels);

  for (jsize i = 0; i < len; i++) {
    jbyte jcl = jcompression_level[i];
    compression_levels->push_back(
        static_cast<ROCKSDB_NAMESPACE::CompressionType>(jcl));
  }

  env->ReleaseByteArrayElements(jcompression_levels, jcompression_level,
                                JNI_ABORT);

  return uptr_compression_levels;
}

/**
 * Helper method to convert a C++ vector of ROCKSDB_NAMESPACE::CompressionType
 * to a Java byte array of compression levels
 *
 * @param env A pointer to the Java environment
 * @param jcompression_levels A reference to a java byte array
 *     where each byte indicates a compression level
 *
 * @return A jbytearray or nullptr if an exception occurs
 */
jbyteArray rocksdb_compression_list_helper(
    JNIEnv* env,
    std::vector<ROCKSDB_NAMESPACE::CompressionType> compression_levels) {
  const size_t len = compression_levels.size();
  jbyte* jbuf = new jbyte[len];

  for (size_t i = 0; i < len; i++) {
    jbuf[i] = compression_levels[i];
  }

  // insert in java array
  jbyteArray jcompression_levels = env->NewByteArray(static_cast<jsize>(len));
  if (jcompression_levels == nullptr) {
    // exception thrown: OutOfMemoryError
    delete[] jbuf;
    return nullptr;
  }
  env->SetByteArrayRegion(jcompression_levels, 0, static_cast<jsize>(len),
                          jbuf);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jcompression_levels);
    delete[] jbuf;
    return nullptr;
  }

  delete[] jbuf;

  return jcompression_levels;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompressionPerLevel
 * Signature: (J[B)V
 */
void Java_org_rocksdb_Options_setCompressionPerLevel(
    JNIEnv* env, jobject, jlong jhandle, jbyteArray jcompressionLevels) {
  auto uptr_compression_levels =
      rocksdb_compression_vector_helper(env, jcompressionLevels);
  if (!uptr_compression_levels) {
    // exception occurred
    return;
  }
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  options->compression_per_level = *(uptr_compression_levels.get());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compressionPerLevel
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_Options_compressionPerLevel(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return rocksdb_compression_list_helper(env, options->compression_per_level);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBottommostCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setBottommostCompressionType(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompression_type_value) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  options->bottommost_compression =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bottommostCompressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_bottommostCompressionType(
    JNIEnv*, jobject, jlong jhandle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      options->bottommost_compression);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBottommostCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setBottommostCompressionOptions(
    JNIEnv*, jobject, jlong jhandle,
    jlong jbottommost_compression_options_handle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* bottommost_compression_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompressionOptions*>(
          jbottommost_compression_options_handle);
  options->bottommost_compression_opts = *bottommost_compression_options;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompressionOptions(
    JNIEnv*, jobject, jlong jhandle, jlong jcompression_options_handle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* compression_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompressionOptions*>(
          jcompression_options_handle);
  options->compression_opts = *compression_options;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionStyle
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setCompactionStyle(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompaction_style) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  options->compaction_style =
      ROCKSDB_NAMESPACE::CompactionStyleJni::toCppCompactionStyle(
          jcompaction_style);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compactionStyle
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_compactionStyle(
    JNIEnv*, jobject, jlong jhandle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return ROCKSDB_NAMESPACE::CompactionStyleJni::toJavaCompactionStyle(
      options->compaction_style);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxTableFilesSizeFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxTableFilesSizeFIFO(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_table_files_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->compaction_options_fifo.max_table_files_size =
      static_cast<uint64_t>(jmax_table_files_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxTableFilesSizeFIFO
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxTableFilesSizeFIFO(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->compaction_options_fifo.max_table_files_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    numLevels
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_numLevels(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->num_levels;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setNumLevels
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setNumLevels(
    JNIEnv*, jobject, jlong jhandle, jint jnum_levels) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->num_levels =
      static_cast<int>(jnum_levels);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelZeroFileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_levelZeroFileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelZeroFileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevelZeroFileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle,
    jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelZeroSlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_levelZeroSlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelSlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevelZeroSlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_slowdown_writes_trigger =
      static_cast<int>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelZeroStopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_levelZeroStopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelStopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevelZeroStopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    targetFileSizeBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_targetFileSizeBase(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->target_file_size_base;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTargetFileSizeBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setTargetFileSizeBase(
    JNIEnv*, jobject, jlong jhandle, jlong jtarget_file_size_base) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->target_file_size_base = static_cast<uint64_t>(jtarget_file_size_base);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    targetFileSizeMultiplier
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_targetFileSizeMultiplier(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->target_file_size_multiplier;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTargetFileSizeMultiplier
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setTargetFileSizeMultiplier(
    JNIEnv*, jobject, jlong jhandle, jint jtarget_file_size_multiplier) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->target_file_size_multiplier =
      static_cast<int>(jtarget_file_size_multiplier);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBytesForLevelBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxBytesForLevelBase(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_bytes_for_level_base;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBytesForLevelBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxBytesForLevelBase(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_bytes_for_level_base) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_bytes_for_level_base =
      static_cast<int64_t>(jmax_bytes_for_level_base);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelCompactionDynamicLevelBytes
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_levelCompactionDynamicLevelBytes(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level_compaction_dynamic_level_bytes;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelCompactionDynamicLevelBytes
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setLevelCompactionDynamicLevelBytes(
    JNIEnv*, jobject, jlong jhandle, jboolean jenable_dynamic_level_bytes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level_compaction_dynamic_level_bytes = (jenable_dynamic_level_bytes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBytesForLevelMultiplier
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_Options_maxBytesForLevelMultiplier(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_bytes_for_level_multiplier;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBytesForLevelMultiplier
 * Signature: (JD)V
 */
void Java_org_rocksdb_Options_setMaxBytesForLevelMultiplier(
    JNIEnv*, jobject, jlong jhandle, jdouble jmax_bytes_for_level_multiplier) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_bytes_for_level_multiplier =
      static_cast<double>(jmax_bytes_for_level_multiplier);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxCompactionBytes
 * Signature: (J)I
 */
jlong Java_org_rocksdb_Options_maxCompactionBytes(
    JNIEnv*, jobject, jlong jhandle) {
  return static_cast<jlong>(
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
          ->max_compaction_bytes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxCompactionBytes
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxCompactionBytes(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_compaction_bytes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->max_compaction_bytes =
      static_cast<uint64_t>(jmax_compaction_bytes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    arenaBlockSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_arenaBlockSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->arena_block_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setArenaBlockSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setArenaBlockSize(
    JNIEnv* env, jobject, jlong jhandle, jlong jarena_block_size) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jarena_block_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->arena_block_size =
        jarena_block_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    disableAutoCompactions
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_disableAutoCompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->disable_auto_compactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDisableAutoCompactions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setDisableAutoCompactions(
    JNIEnv*, jobject, jlong jhandle, jboolean jdisable_auto_compactions) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->disable_auto_compactions = static_cast<bool>(jdisable_auto_compactions);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxSequentialSkipInIterations
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxSequentialSkipInIterations(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_sequential_skip_in_iterations;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxSequentialSkipInIterations
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxSequentialSkipInIterations(
    JNIEnv*, jobject, jlong jhandle,
    jlong jmax_sequential_skip_in_iterations) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_sequential_skip_in_iterations =
      static_cast<int64_t>(jmax_sequential_skip_in_iterations);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    inplaceUpdateSupport
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_inplaceUpdateSupport(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->inplace_update_support;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setInplaceUpdateSupport
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setInplaceUpdateSupport(
    JNIEnv*, jobject, jlong jhandle, jboolean jinplace_update_support) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->inplace_update_support = static_cast<bool>(jinplace_update_support);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    inplaceUpdateNumLocks
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_inplaceUpdateNumLocks(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->inplace_update_num_locks;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setInplaceUpdateNumLocks
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setInplaceUpdateNumLocks(
    JNIEnv* env, jobject, jlong jhandle, jlong jinplace_update_num_locks) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jinplace_update_num_locks);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
        ->inplace_update_num_locks = jinplace_update_num_locks;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    memtablePrefixBloomSizeRatio
 * Signature: (J)I
 */
jdouble Java_org_rocksdb_Options_memtablePrefixBloomSizeRatio(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->memtable_prefix_bloom_size_ratio;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMemtablePrefixBloomSizeRatio
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMemtablePrefixBloomSizeRatio(
    JNIEnv*, jobject, jlong jhandle,
    jdouble jmemtable_prefix_bloom_size_ratio) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->memtable_prefix_bloom_size_ratio =
      static_cast<double>(jmemtable_prefix_bloom_size_ratio);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bloomLocality
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_bloomLocality(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->bloom_locality;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBloomLocality
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setBloomLocality(
    JNIEnv*, jobject, jlong jhandle, jint jbloom_locality) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->bloom_locality =
      static_cast<int32_t>(jbloom_locality);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxSuccessiveMerges
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxSuccessiveMerges(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->max_successive_merges;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxSuccessiveMerges
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxSuccessiveMerges(
    JNIEnv* env, jobject, jlong jhandle, jlong jmax_successive_merges) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jmax_successive_merges);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
        ->max_successive_merges = jmax_successive_merges;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeFiltersForHits
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_optimizeFiltersForHits(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->optimize_filters_for_hits;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setOptimizeFiltersForHits
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setOptimizeFiltersForHits(
    JNIEnv*, jobject, jlong jhandle, jboolean joptimize_filters_for_hits) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->optimize_filters_for_hits =
      static_cast<bool>(joptimize_filters_for_hits);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    oldDefaults
 * Signature: (JII)V
 */
void Java_org_rocksdb_Options_oldDefaults(JNIEnv*, jclass, jlong jhandle,
                                          jint major_version,
                                          jint minor_version) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->OldDefaults(
      major_version, minor_version);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeForSmallDb
 * Signature: (J)V
 */
void Java_org_rocksdb_Options_optimizeForSmallDb__J(JNIEnv*, jobject,
                                                    jlong jhandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->OptimizeForSmallDb();
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeForSmallDb
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_optimizeForSmallDb__JJ(JNIEnv*, jclass,
                                                     jlong jhandle,
                                                     jlong cache_handle) {
  auto* cache_sptr_ptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(
          cache_handle);
  auto* options_ptr = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* cf_options_ptr =
      static_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(options_ptr);
  cf_options_ptr->OptimizeForSmallDb(cache_sptr_ptr);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeForPointLookup
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_optimizeForPointLookup(
    JNIEnv*, jobject, jlong jhandle, jlong block_cache_size_mb) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->OptimizeForPointLookup(block_cache_size_mb);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeLevelStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_optimizeLevelStyleCompaction(
    JNIEnv*, jobject, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->OptimizeLevelStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeUniversalStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_optimizeUniversalStyleCompaction(
    JNIEnv*, jobject, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->OptimizeUniversalStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    prepareForBulkLoad
 * Signature: (J)V
 */
void Java_org_rocksdb_Options_prepareForBulkLoad(
    JNIEnv*, jobject, jlong jhandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->PrepareForBulkLoad();
}

/*
 * Class:     org_rocksdb_Options
 * Method:    memtableHugePageSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_memtableHugePageSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->memtable_huge_page_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMemtableHugePageSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMemtableHugePageSize(
    JNIEnv* env, jobject, jlong jhandle, jlong jmemtable_huge_page_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jmemtable_huge_page_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
        ->memtable_huge_page_size = jmemtable_huge_page_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    softPendingCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_softPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->soft_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setSoftPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setSoftPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle,
    jlong jsoft_pending_compaction_bytes_limit) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->soft_pending_compaction_bytes_limit =
      static_cast<int64_t>(jsoft_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    softHardCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_hardPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->hard_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setHardPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setHardPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle,
    jlong jhard_pending_compaction_bytes_limit) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->hard_pending_compaction_bytes_limit =
      static_cast<int64_t>(jhard_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    level0FileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_level0FileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevel0FileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevel0FileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle,
    jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int32_t>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    level0SlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_level0SlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevel0SlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevel0SlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_slowdown_writes_trigger =
      static_cast<int32_t>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    level0StopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_level0StopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevel0StopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevel0StopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int32_t>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBytesForLevelMultiplierAdditional
 * Signature: (J)[I
 */
jintArray Java_org_rocksdb_Options_maxBytesForLevelMultiplierAdditional(
    JNIEnv* env, jobject, jlong jhandle) {
  auto mbflma = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
                    ->max_bytes_for_level_multiplier_additional;

  const size_t size = mbflma.size();

  jint* additionals = new jint[size];
  for (size_t i = 0; i < size; i++) {
    additionals[i] = static_cast<jint>(mbflma[i]);
  }

  jsize jlen = static_cast<jsize>(size);
  jintArray result = env->NewIntArray(jlen);
  if (result == nullptr) {
    // exception thrown: OutOfMemoryError
    delete[] additionals;
    return nullptr;
  }

  env->SetIntArrayRegion(result, 0, jlen, additionals);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(result);
    delete[] additionals;
    return nullptr;
  }

  delete[] additionals;

  return result;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBytesForLevelMultiplierAdditional
 * Signature: (J[I)V
 */
void Java_org_rocksdb_Options_setMaxBytesForLevelMultiplierAdditional(
    JNIEnv* env, jobject, jlong jhandle,
    jintArray jmax_bytes_for_level_multiplier_additional) {
  jsize len = env->GetArrayLength(jmax_bytes_for_level_multiplier_additional);
  jint* additionals = env->GetIntArrayElements(
      jmax_bytes_for_level_multiplier_additional, nullptr);
  if (additionals == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opt->max_bytes_for_level_multiplier_additional.clear();
  for (jsize i = 0; i < len; i++) {
    opt->max_bytes_for_level_multiplier_additional.push_back(
        static_cast<int32_t>(additionals[i]));
  }

  env->ReleaseIntArrayElements(jmax_bytes_for_level_multiplier_additional,
                               additionals, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    paranoidFileChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_paranoidFileChecks(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)
      ->paranoid_file_checks;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setParanoidFileChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setParanoidFileChecks(
    JNIEnv*, jobject, jlong jhandle, jboolean jparanoid_file_checks) {
  reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle)->paranoid_file_checks =
      static_cast<bool>(jparanoid_file_checks);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionPriority
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setCompactionPriority(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompaction_priority_value) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->compaction_pri =
      ROCKSDB_NAMESPACE::CompactionPriorityJni::toCppCompactionPriority(
          jcompaction_priority_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compactionPriority
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_compactionPriority(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return ROCKSDB_NAMESPACE::CompactionPriorityJni::toJavaCompactionPriority(
      opts->compaction_pri);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setReportBgIoStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setReportBgIoStats(
    JNIEnv*, jobject, jlong jhandle, jboolean jreport_bg_io_stats) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->report_bg_io_stats = static_cast<bool>(jreport_bg_io_stats);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    reportBgIoStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_reportBgIoStats(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<bool>(opts->report_bg_io_stats);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTtl
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setTtl(
    JNIEnv*, jobject, jlong jhandle, jlong jttl) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->ttl = static_cast<uint64_t>(jttl);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    ttl
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_ttl(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<jlong>(opts->ttl);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionOptionsUniversal
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionOptionsUniversal(
    JNIEnv*, jobject, jlong jhandle,
    jlong jcompaction_options_universal_handle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* opts_uni =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(
          jcompaction_options_universal_handle);
  opts->compaction_options_universal = *opts_uni;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionOptionsFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionOptionsFIFO(
    JNIEnv*, jobject, jlong jhandle, jlong jcompaction_options_fifo_handle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  auto* opts_fifo = reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsFIFO*>(
      jcompaction_options_fifo_handle);
  opts->compaction_options_fifo = *opts_fifo;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setForceConsistencyChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setForceConsistencyChecks(
    JNIEnv*, jobject, jlong jhandle, jboolean jforce_consistency_checks) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  opts->force_consistency_checks = static_cast<bool>(jforce_consistency_checks);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    forceConsistencyChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_forceConsistencyChecks(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opts = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jhandle);
  return static_cast<bool>(opts->force_consistency_checks);
}

//////////////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::ColumnFamilyOptions

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    newColumnFamilyOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_newColumnFamilyOptions(
    JNIEnv*, jclass) {
  auto* op = new ROCKSDB_NAMESPACE::ColumnFamilyOptions();
  return reinterpret_cast<jlong>(op);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    copyColumnFamilyOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_copyColumnFamilyOptions(
    JNIEnv*, jclass, jlong jhandle) {
  auto new_opt = new ROCKSDB_NAMESPACE::ColumnFamilyOptions(
      *(reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    newColumnFamilyOptionsFromOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_newColumnFamilyOptionsFromOptions(
    JNIEnv*, jclass, jlong joptions_handle) {
  auto new_opt = new ROCKSDB_NAMESPACE::ColumnFamilyOptions(
      *reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(joptions_handle));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    getColumnFamilyOptionsFromProps
 * Signature: (JLjava/lang/String;)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_getColumnFamilyOptionsFromProps__JLjava_lang_String_2(
    JNIEnv* env, jclass, jlong cfg_handle, jstring jopt_string) {
  const char* opt_string = env->GetStringUTFChars(jopt_string, nullptr);
  if (opt_string == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  auto* config_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ConfigOptions*>(cfg_handle);
  auto* cf_options = new ROCKSDB_NAMESPACE::ColumnFamilyOptions();
  ROCKSDB_NAMESPACE::Status status =
      ROCKSDB_NAMESPACE::GetColumnFamilyOptionsFromString(
          *config_options, ROCKSDB_NAMESPACE::ColumnFamilyOptions(), opt_string,
          cf_options);

  env->ReleaseStringUTFChars(jopt_string, opt_string);

  // Check if ColumnFamilyOptions creation was possible.
  jlong ret_value = 0;
  if (status.ok()) {
    ret_value = reinterpret_cast<jlong>(cf_options);
  } else {
    // if operation failed the ColumnFamilyOptions need to be deleted
    // again to prevent a memory leak.
    delete cf_options;
  }
  return ret_value;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    getColumnFamilyOptionsFromProps
 * Signature: (Ljava/util/String;)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_getColumnFamilyOptionsFromProps__Ljava_lang_String_2(
    JNIEnv* env, jclass, jstring jopt_string) {
  const char* opt_string = env->GetStringUTFChars(jopt_string, nullptr);
  if (opt_string == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* cf_options = new ROCKSDB_NAMESPACE::ColumnFamilyOptions();
  ROCKSDB_NAMESPACE::Status status =
      ROCKSDB_NAMESPACE::GetColumnFamilyOptionsFromString(
          ROCKSDB_NAMESPACE::ColumnFamilyOptions(), opt_string, cf_options);

  env->ReleaseStringUTFChars(jopt_string, opt_string);

  // Check if ColumnFamilyOptions creation was possible.
  jlong ret_value = 0;
  if (status.ok()) {
    ret_value = reinterpret_cast<jlong>(cf_options);
  } else {
    // if operation failed the ColumnFamilyOptions need to be deleted
    // again to prevent a memory leak.
    delete cf_options;
  }
  return ret_value;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_disposeInternal(
    JNIEnv*, jobject, jlong handle) {
  auto* cfo = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(handle);
  assert(cfo != nullptr);
  delete cfo;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    oldDefaults
 * Signature: (JII)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_oldDefaults(JNIEnv*, jclass,
                                                      jlong jhandle,
                                                      jint major_version,
                                                      jint minor_version) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->OldDefaults(major_version, minor_version);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeForSmallDb
 * Signature: (J)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeForSmallDb__J(JNIEnv*,
                                                                jobject,
                                                                jlong jhandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->OptimizeForSmallDb();
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeForSmallDb
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeForSmallDb__JJ(
    JNIEnv*, jclass, jlong jhandle, jlong cache_handle) {
  auto* cache_sptr_ptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(
          cache_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->OptimizeForSmallDb(cache_sptr_ptr);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeForPointLookup
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeForPointLookup(
    JNIEnv*, jobject, jlong jhandle, jlong block_cache_size_mb) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->OptimizeForPointLookup(block_cache_size_mb);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeLevelStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeLevelStyleCompaction(
    JNIEnv*, jobject, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->OptimizeLevelStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeUniversalStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeUniversalStyleCompaction(
    JNIEnv*, jobject, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->OptimizeUniversalStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setComparatorHandle
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setComparatorHandle__JI(
    JNIEnv*, jobject, jlong jhandle, jint builtinComparator) {
  switch (builtinComparator) {
    case 1:
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
          ->comparator = ROCKSDB_NAMESPACE::ReverseBytewiseComparator();
      break;
    default:
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
          ->comparator = ROCKSDB_NAMESPACE::BytewiseComparator();
      break;
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setComparatorHandle
 * Signature: (JJB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setComparatorHandle__JJB(
    JNIEnv*, jobject, jlong jopt_handle, jlong jcomparator_handle,
    jbyte jcomparator_type) {
  ROCKSDB_NAMESPACE::Comparator* comparator = nullptr;
  switch (jcomparator_type) {
    // JAVA_COMPARATOR
    case 0x0:
      comparator = reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallback*>(
          jcomparator_handle);
      break;

    // JAVA_NATIVE_COMPARATOR_WRAPPER
    case 0x1:
      comparator =
          reinterpret_cast<ROCKSDB_NAMESPACE::Comparator*>(jcomparator_handle);
      break;
  }
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jopt_handle);
  opt->comparator = comparator;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMergeOperatorName
 * Signature: (JJjava/lang/String)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMergeOperatorName(
    JNIEnv* env, jobject, jlong jhandle, jstring jop_name) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  const char* op_name = env->GetStringUTFChars(jop_name, nullptr);
  if (op_name == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  options->merge_operator =
      ROCKSDB_NAMESPACE::MergeOperators::CreateFromStringId(op_name);
  env->ReleaseStringUTFChars(jop_name, op_name);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMergeOperator
 * Signature: (JJjava/lang/String)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMergeOperator(
    JNIEnv*, jobject, jlong jhandle, jlong mergeOperatorHandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->merge_operator =
      *(reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::MergeOperator>*>(
          mergeOperatorHandle));
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionFilterHandle
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionFilterHandle(
    JNIEnv*, jobject, jlong jopt_handle, jlong jcompactionfilter_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jopt_handle)
      ->compaction_filter =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionFilter*>(
          jcompactionfilter_handle);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionFilterFactoryHandle
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionFilterFactoryHandle(
    JNIEnv*, jobject, jlong jopt_handle,
    jlong jcompactionfilterfactory_handle) {
  auto* cff_factory = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::CompactionFilterFactoryJniCallback>*>(
      jcompactionfilterfactory_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jopt_handle)
      ->compaction_filter_factory = *cff_factory;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setWriteBufferSize
 * Signature: (JJ)I
 */
void Java_org_rocksdb_ColumnFamilyOptions_setWriteBufferSize(
    JNIEnv* env, jobject, jlong jhandle, jlong jwrite_buffer_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jwrite_buffer_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
        ->write_buffer_size = jwrite_buffer_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    writeBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_writeBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->write_buffer_size;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxWriteBufferNumber
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxWriteBufferNumber(
    JNIEnv*, jobject, jlong jhandle, jint jmax_write_buffer_number) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_write_buffer_number = jmax_write_buffer_number;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxWriteBufferNumber
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_maxWriteBufferNumber(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_write_buffer_number;
}

/*
 * Method:    setMemTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMemTableFactory(
    JNIEnv*, jobject, jlong jhandle, jlong jfactory_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->memtable_factory.reset(
          reinterpret_cast<ROCKSDB_NAMESPACE::MemTableRepFactory*>(
              jfactory_handle));
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    memTableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_ColumnFamilyOptions_memTableFactoryName(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  ROCKSDB_NAMESPACE::MemTableRepFactory* tf = opt->memtable_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  // temporarly fix for the historical typo
  if (strcmp(tf->Name(), "HashLinkListRepFactory") == 0) {
    return env->NewStringUTF("HashLinkedListRepFactory");
  }

  return env->NewStringUTF(tf->Name());
}

/*
 * Method:    useFixedLengthPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_useFixedLengthPrefixExtractor(
    JNIEnv*, jobject, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->prefix_extractor.reset(ROCKSDB_NAMESPACE::NewFixedPrefixTransform(
          static_cast<int>(jprefix_length)));
}

/*
 * Method:    useCappedPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_useCappedPrefixExtractor(
    JNIEnv*, jobject, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->prefix_extractor.reset(ROCKSDB_NAMESPACE::NewCappedPrefixTransform(
          static_cast<int>(jprefix_length)));
}

/*
 * Method:    setTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setTableFactory(
    JNIEnv*, jobject, jlong jhandle, jlong jfactory_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->table_factory.reset(
          reinterpret_cast<ROCKSDB_NAMESPACE::TableFactory*>(jfactory_handle));
}

/*
 * Method:    setSstPartitionerFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setSstPartitionerFactory(
    JNIEnv*, jobject, jlong jhandle, jlong factory_handle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto* factory = reinterpret_cast<ROCKSDB_NAMESPACE::SstPartitionerFactory*>(
      factory_handle);
  options->sst_partitioner_factory.reset(factory);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionThreadLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionThreadLimiter(
    JNIEnv*, jclass, jlong jhandle, jlong jlimiter_handle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto* limiter = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter>*>(
      jlimiter_handle);
  options->compaction_thread_limiter = *limiter;
}

/*
 * Method:    tableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_ColumnFamilyOptions_tableFactoryName(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  ROCKSDB_NAMESPACE::TableFactory* tf = opt->table_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  return env->NewStringUTF(tf->Name());
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCfPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCfPaths(JNIEnv* env, jclass,
                                                     jlong jhandle,
                                                     jobjectArray path_array,
                                                     jlongArray size_array) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  jboolean has_exception = JNI_FALSE;
  std::vector<ROCKSDB_NAMESPACE::DbPath> cf_paths =
      rocksdb_convert_cf_paths_from_java_helper(env, path_array, size_array,
                                                &has_exception);
  if (JNI_FALSE == has_exception) {
    options->cf_paths = std::move(cf_paths);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    cfPathsLen
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_cfPathsLen(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jlong>(opt->cf_paths.size());
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    cfPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_cfPaths(JNIEnv* env, jclass,
                                                  jlong jhandle,
                                                  jobjectArray jpaths,
                                                  jlongArray jtarget_sizes) {
  rocksdb_convert_cf_paths_to_java_helper<
      ROCKSDB_NAMESPACE::ColumnFamilyOptions>(env, jhandle, jpaths,
                                              jtarget_sizes);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    minWriteBufferNumberToMerge
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_minWriteBufferNumberToMerge(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->min_write_buffer_number_to_merge;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMinWriteBufferNumberToMerge
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMinWriteBufferNumberToMerge(
    JNIEnv*, jobject, jlong jhandle, jint jmin_write_buffer_number_to_merge) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->min_write_buffer_number_to_merge =
      static_cast<int>(jmin_write_buffer_number_to_merge);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxWriteBufferNumberToMaintain
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_maxWriteBufferNumberToMaintain(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_write_buffer_number_to_maintain;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxWriteBufferNumberToMaintain
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxWriteBufferNumberToMaintain(
    JNIEnv*, jobject, jlong jhandle,
    jint jmax_write_buffer_number_to_maintain) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_write_buffer_number_to_maintain =
      static_cast<int>(jmax_write_buffer_number_to_maintain);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompressionType(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompression_type_value) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_opts->compression =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    compressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_compressionType(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      cf_opts->compression);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompressionPerLevel
 * Signature: (J[B)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompressionPerLevel(
    JNIEnv* env, jobject, jlong jhandle, jbyteArray jcompressionLevels) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto uptr_compression_levels =
      rocksdb_compression_vector_helper(env, jcompressionLevels);
  if (!uptr_compression_levels) {
    // exception occurred
    return;
  }
  options->compression_per_level = *(uptr_compression_levels.get());
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    compressionPerLevel
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_ColumnFamilyOptions_compressionPerLevel(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return rocksdb_compression_list_helper(env,
                                         cf_options->compression_per_level);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBottommostCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBottommostCompressionType(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompression_type_value) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_options->bottommost_compression =
      ROCKSDB_NAMESPACE::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    bottommostCompressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_bottommostCompressionType(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      cf_options->bottommost_compression);
}
/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBottommostCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBottommostCompressionOptions(
    JNIEnv*, jobject, jlong jhandle,
    jlong jbottommost_compression_options_handle) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto* bottommost_compression_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompressionOptions*>(
          jbottommost_compression_options_handle);
  cf_options->bottommost_compression_opts = *bottommost_compression_options;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompressionOptions(
    JNIEnv*, jobject, jlong jhandle, jlong jcompression_options_handle) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto* compression_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompressionOptions*>(
          jcompression_options_handle);
  cf_options->compression_opts = *compression_options;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionStyle
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionStyle(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompaction_style) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_options->compaction_style =
      ROCKSDB_NAMESPACE::CompactionStyleJni::toCppCompactionStyle(
          jcompaction_style);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    compactionStyle
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_compactionStyle(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::CompactionStyleJni::toJavaCompactionStyle(
      cf_options->compaction_style);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxTableFilesSizeFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxTableFilesSizeFIFO(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_table_files_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->compaction_options_fifo.max_table_files_size =
      static_cast<uint64_t>(jmax_table_files_size);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxTableFilesSizeFIFO
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxTableFilesSizeFIFO(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->compaction_options_fifo.max_table_files_size;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    numLevels
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_numLevels(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->num_levels;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setNumLevels
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setNumLevels(
    JNIEnv*, jobject, jlong jhandle, jint jnum_levels) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->num_levels = static_cast<int>(jnum_levels);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    levelZeroFileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_levelZeroFileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevelZeroFileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevelZeroFileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle,
    jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    levelZeroSlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_levelZeroSlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevelSlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevelZeroSlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_slowdown_writes_trigger =
      static_cast<int>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    levelZeroStopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_levelZeroStopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevelStopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevelZeroStopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    targetFileSizeBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_targetFileSizeBase(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->target_file_size_base;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setTargetFileSizeBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setTargetFileSizeBase(
    JNIEnv*, jobject, jlong jhandle, jlong jtarget_file_size_base) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->target_file_size_base = static_cast<uint64_t>(jtarget_file_size_base);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    targetFileSizeMultiplier
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_targetFileSizeMultiplier(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->target_file_size_multiplier;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setTargetFileSizeMultiplier
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setTargetFileSizeMultiplier(
    JNIEnv*, jobject, jlong jhandle, jint jtarget_file_size_multiplier) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->target_file_size_multiplier =
      static_cast<int>(jtarget_file_size_multiplier);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxBytesForLevelBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxBytesForLevelBase(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_bytes_for_level_base;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxBytesForLevelBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxBytesForLevelBase(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_bytes_for_level_base) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_bytes_for_level_base =
      static_cast<int64_t>(jmax_bytes_for_level_base);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    levelCompactionDynamicLevelBytes
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_levelCompactionDynamicLevelBytes(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level_compaction_dynamic_level_bytes;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevelCompactionDynamicLevelBytes
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevelCompactionDynamicLevelBytes(
    JNIEnv*, jobject, jlong jhandle, jboolean jenable_dynamic_level_bytes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level_compaction_dynamic_level_bytes = (jenable_dynamic_level_bytes);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxBytesForLevelMultiplier
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_ColumnFamilyOptions_maxBytesForLevelMultiplier(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_bytes_for_level_multiplier;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxBytesForLevelMultiplier
 * Signature: (JD)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxBytesForLevelMultiplier(
    JNIEnv*, jobject, jlong jhandle, jdouble jmax_bytes_for_level_multiplier) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_bytes_for_level_multiplier =
      static_cast<double>(jmax_bytes_for_level_multiplier);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxCompactionBytes
 * Signature: (J)I
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxCompactionBytes(
    JNIEnv*, jobject, jlong jhandle) {
  return static_cast<jlong>(
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
          ->max_compaction_bytes);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxCompactionBytes
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxCompactionBytes(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_compaction_bytes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_compaction_bytes = static_cast<uint64_t>(jmax_compaction_bytes);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    arenaBlockSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_arenaBlockSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->arena_block_size;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setArenaBlockSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setArenaBlockSize(
    JNIEnv* env, jobject, jlong jhandle, jlong jarena_block_size) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jarena_block_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
        ->arena_block_size = jarena_block_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    disableAutoCompactions
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_disableAutoCompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->disable_auto_compactions;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setDisableAutoCompactions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setDisableAutoCompactions(
    JNIEnv*, jobject, jlong jhandle, jboolean jdisable_auto_compactions) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->disable_auto_compactions = static_cast<bool>(jdisable_auto_compactions);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxSequentialSkipInIterations
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxSequentialSkipInIterations(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_sequential_skip_in_iterations;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxSequentialSkipInIterations
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxSequentialSkipInIterations(
    JNIEnv*, jobject, jlong jhandle,
    jlong jmax_sequential_skip_in_iterations) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_sequential_skip_in_iterations =
      static_cast<int64_t>(jmax_sequential_skip_in_iterations);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    inplaceUpdateSupport
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_inplaceUpdateSupport(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->inplace_update_support;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setInplaceUpdateSupport
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setInplaceUpdateSupport(
    JNIEnv*, jobject, jlong jhandle, jboolean jinplace_update_support) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->inplace_update_support = static_cast<bool>(jinplace_update_support);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    inplaceUpdateNumLocks
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_inplaceUpdateNumLocks(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->inplace_update_num_locks;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setInplaceUpdateNumLocks
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setInplaceUpdateNumLocks(
    JNIEnv* env, jobject, jlong jhandle, jlong jinplace_update_num_locks) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jinplace_update_num_locks);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
        ->inplace_update_num_locks = jinplace_update_num_locks;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    memtablePrefixBloomSizeRatio
 * Signature: (J)I
 */
jdouble Java_org_rocksdb_ColumnFamilyOptions_memtablePrefixBloomSizeRatio(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->memtable_prefix_bloom_size_ratio;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMemtablePrefixBloomSizeRatio
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMemtablePrefixBloomSizeRatio(
    JNIEnv*, jobject, jlong jhandle,
    jdouble jmemtable_prefix_bloom_size_ratio) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->memtable_prefix_bloom_size_ratio =
      static_cast<double>(jmemtable_prefix_bloom_size_ratio);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    bloomLocality
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_bloomLocality(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->bloom_locality;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBloomLocality
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBloomLocality(
    JNIEnv*, jobject, jlong jhandle, jint jbloom_locality) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->bloom_locality = static_cast<int32_t>(jbloom_locality);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxSuccessiveMerges
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxSuccessiveMerges(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->max_successive_merges;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxSuccessiveMerges
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxSuccessiveMerges(
    JNIEnv* env, jobject, jlong jhandle, jlong jmax_successive_merges) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jmax_successive_merges);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
        ->max_successive_merges = jmax_successive_merges;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeFiltersForHits
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_optimizeFiltersForHits(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->optimize_filters_for_hits;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setOptimizeFiltersForHits
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setOptimizeFiltersForHits(
    JNIEnv*, jobject, jlong jhandle, jboolean joptimize_filters_for_hits) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->optimize_filters_for_hits =
      static_cast<bool>(joptimize_filters_for_hits);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    memtableHugePageSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_memtableHugePageSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->memtable_huge_page_size;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMemtableHugePageSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMemtableHugePageSize(
    JNIEnv* env, jobject, jlong jhandle, jlong jmemtable_huge_page_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      jmemtable_huge_page_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
        ->memtable_huge_page_size = jmemtable_huge_page_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    softPendingCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_softPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->soft_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setSoftPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setSoftPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle,
    jlong jsoft_pending_compaction_bytes_limit) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->soft_pending_compaction_bytes_limit =
      static_cast<int64_t>(jsoft_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    softHardCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_hardPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->hard_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setHardPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setHardPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle,
    jlong jhard_pending_compaction_bytes_limit) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->hard_pending_compaction_bytes_limit =
      static_cast<int64_t>(jhard_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    level0FileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_level0FileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevel0FileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevel0FileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle,
    jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int32_t>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    level0SlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_level0SlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevel0SlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevel0SlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_slowdown_writes_trigger =
      static_cast<int32_t>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    level0StopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_level0StopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevel0StopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevel0StopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int32_t>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxBytesForLevelMultiplierAdditional
 * Signature: (J)[I
 */
jintArray Java_org_rocksdb_ColumnFamilyOptions_maxBytesForLevelMultiplierAdditional(
    JNIEnv* env, jobject, jlong jhandle) {
  auto mbflma =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
          ->max_bytes_for_level_multiplier_additional;

  const size_t size = mbflma.size();

  jint* additionals = new jint[size];
  for (size_t i = 0; i < size; i++) {
    additionals[i] = static_cast<jint>(mbflma[i]);
  }

  jsize jlen = static_cast<jsize>(size);
  jintArray result = env->NewIntArray(jlen);
  if (result == nullptr) {
    // exception thrown: OutOfMemoryError
    delete[] additionals;
    return nullptr;
  }
  env->SetIntArrayRegion(result, 0, jlen, additionals);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(result);
    delete[] additionals;
    return nullptr;
  }

  delete[] additionals;

  return result;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxBytesForLevelMultiplierAdditional
 * Signature: (J[I)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxBytesForLevelMultiplierAdditional(
    JNIEnv* env, jobject, jlong jhandle,
    jintArray jmax_bytes_for_level_multiplier_additional) {
  jsize len = env->GetArrayLength(jmax_bytes_for_level_multiplier_additional);
  jint* additionals = env->GetIntArrayElements(
      jmax_bytes_for_level_multiplier_additional, nullptr);
  if (additionals == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* cf_opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_opt->max_bytes_for_level_multiplier_additional.clear();
  for (jsize i = 0; i < len; i++) {
    cf_opt->max_bytes_for_level_multiplier_additional.push_back(
        static_cast<int32_t>(additionals[i]));
  }

  env->ReleaseIntArrayElements(jmax_bytes_for_level_multiplier_additional,
                               additionals, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    paranoidFileChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_paranoidFileChecks(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->paranoid_file_checks;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setParanoidFileChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setParanoidFileChecks(
    JNIEnv*, jobject, jlong jhandle, jboolean jparanoid_file_checks) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle)
      ->paranoid_file_checks = static_cast<bool>(jparanoid_file_checks);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionPriority
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionPriority(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompaction_priority_value) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_opts->compaction_pri =
      ROCKSDB_NAMESPACE::CompactionPriorityJni::toCppCompactionPriority(
          jcompaction_priority_value);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    compactionPriority
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_compactionPriority(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::CompactionPriorityJni::toJavaCompactionPriority(
      cf_opts->compaction_pri);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setReportBgIoStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setReportBgIoStats(
    JNIEnv*, jobject, jlong jhandle, jboolean jreport_bg_io_stats) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_opts->report_bg_io_stats = static_cast<bool>(jreport_bg_io_stats);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    reportBgIoStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_reportBgIoStats(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<bool>(cf_opts->report_bg_io_stats);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setTtl
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setTtl(
    JNIEnv*, jobject, jlong jhandle, jlong jttl) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_opts->ttl = static_cast<uint64_t>(jttl);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    ttl
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_ColumnFamilyOptions_ttl(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<jlong>(cf_opts->ttl);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionOptionsUniversal
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionOptionsUniversal(
    JNIEnv*, jobject, jlong jhandle,
    jlong jcompaction_options_universal_handle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto* opts_uni =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(
          jcompaction_options_universal_handle);
  cf_opts->compaction_options_universal = *opts_uni;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionOptionsFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionOptionsFIFO(
    JNIEnv*, jobject, jlong jhandle, jlong jcompaction_options_fifo_handle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  auto* opts_fifo = reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsFIFO*>(
      jcompaction_options_fifo_handle);
  cf_opts->compaction_options_fifo = *opts_fifo;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setForceConsistencyChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setForceConsistencyChecks(
    JNIEnv*, jobject, jlong jhandle, jboolean jforce_consistency_checks) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  cf_opts->force_consistency_checks =
      static_cast<bool>(jforce_consistency_checks);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    forceConsistencyChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_forceConsistencyChecks(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jhandle);
  return static_cast<bool>(cf_opts->force_consistency_checks);
}

/////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::DBOptions

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    newDBOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_DBOptions_newDBOptions(
    JNIEnv*, jclass) {
  auto* dbop = new ROCKSDB_NAMESPACE::DBOptions();
  return reinterpret_cast<jlong>(dbop);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    copyDBOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_copyDBOptions(
    JNIEnv*, jclass, jlong jhandle) {
  auto new_opt = new ROCKSDB_NAMESPACE::DBOptions(
      *(reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    newDBOptionsFromOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_newDBOptionsFromOptions(
    JNIEnv*, jclass, jlong joptions_handle) {
  auto new_opt = new ROCKSDB_NAMESPACE::DBOptions(
      *reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(joptions_handle));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    getDBOptionsFromProps
 * Signature: (JLjava/lang/String;)J
 */
jlong Java_org_rocksdb_DBOptions_getDBOptionsFromProps__JLjava_lang_String_2(
    JNIEnv* env, jclass, jlong config_handle, jstring jopt_string) {
  const char* opt_string = env->GetStringUTFChars(jopt_string, nullptr);
  if (opt_string == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* config_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ConfigOptions*>(config_handle);
  auto* db_options = new ROCKSDB_NAMESPACE::DBOptions();
  ROCKSDB_NAMESPACE::Status status = ROCKSDB_NAMESPACE::GetDBOptionsFromString(
      *config_options, ROCKSDB_NAMESPACE::DBOptions(), opt_string, db_options);

  env->ReleaseStringUTFChars(jopt_string, opt_string);

  // Check if DBOptions creation was possible.
  jlong ret_value = 0;
  if (status.ok()) {
    ret_value = reinterpret_cast<jlong>(db_options);
  } else {
    // if operation failed the DBOptions need to be deleted
    // again to prevent a memory leak.
    delete db_options;
  }
  return ret_value;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    getDBOptionsFromProps
 * Signature: (Ljava/util/String;)J
 */
jlong Java_org_rocksdb_DBOptions_getDBOptionsFromProps__Ljava_lang_String_2(
    JNIEnv* env, jclass, jstring jopt_string) {
  const char* opt_string = env->GetStringUTFChars(jopt_string, nullptr);
  if (opt_string == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* db_options = new ROCKSDB_NAMESPACE::DBOptions();
  ROCKSDB_NAMESPACE::Status status = ROCKSDB_NAMESPACE::GetDBOptionsFromString(
      ROCKSDB_NAMESPACE::DBOptions(), opt_string, db_options);

  env->ReleaseStringUTFChars(jopt_string, opt_string);

  // Check if DBOptions creation was possible.
  jlong ret_value = 0;
  if (status.ok()) {
    ret_value = reinterpret_cast<jlong>(db_options);
  } else {
    // if operation failed the DBOptions need to be deleted
    // again to prevent a memory leak.
    delete db_options;
  }
  return ret_value;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_DBOptions_disposeInternal(
    JNIEnv*, jobject, jlong handle) {
  auto* dbo = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(handle);
  assert(dbo != nullptr);
  delete dbo;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    optimizeForSmallDb
 * Signature: (J)V
 */
void Java_org_rocksdb_DBOptions_optimizeForSmallDb(
    JNIEnv*, jobject, jlong jhandle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->OptimizeForSmallDb();
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEnv
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setEnv(
    JNIEnv*, jobject, jlong jhandle, jlong jenv_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->env =
      reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jenv_handle);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setIncreaseParallelism
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setIncreaseParallelism(
    JNIEnv*, jobject, jlong jhandle, jint totalThreads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->IncreaseParallelism(
      static_cast<int>(totalThreads));
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setCreateIfMissing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setCreateIfMissing(
    JNIEnv*, jobject, jlong jhandle, jboolean flag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->create_if_missing =
      flag;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    createIfMissing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_createIfMissing(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->create_if_missing;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setCreateMissingColumnFamilies
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setCreateMissingColumnFamilies(
    JNIEnv*, jobject, jlong jhandle, jboolean flag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->create_missing_column_families = flag;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    createMissingColumnFamilies
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_createMissingColumnFamilies(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->create_missing_column_families;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setErrorIfExists
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setErrorIfExists(
    JNIEnv*, jobject, jlong jhandle, jboolean error_if_exists) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->error_if_exists =
      static_cast<bool>(error_if_exists);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    errorIfExists
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_errorIfExists(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->error_if_exists;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setParanoidChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setParanoidChecks(
    JNIEnv*, jobject, jlong jhandle, jboolean paranoid_checks) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->paranoid_checks =
      static_cast<bool>(paranoid_checks);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    paranoidChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_paranoidChecks(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->paranoid_checks;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setRateLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setRateLimiter(
    JNIEnv*, jobject, jlong jhandle, jlong jrate_limiter_handle) {
  std::shared_ptr<ROCKSDB_NAMESPACE::RateLimiter>* pRateLimiter =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::RateLimiter>*>(
          jrate_limiter_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->rate_limiter =
      *pRateLimiter;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setSstFileManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setSstFileManager(
    JNIEnv*, jobject, jlong jhandle, jlong jsst_file_manager_handle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jsst_file_manager_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->sst_file_manager =
      *sptr_sst_file_manager;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setLogger
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setLogger(
    JNIEnv*, jobject, jlong jhandle, jlong jlogger_handle) {
  std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>* pLogger =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>*>(
          jlogger_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->info_log = *pLogger;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setInfoLogLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_DBOptions_setInfoLogLevel(
    JNIEnv*, jobject, jlong jhandle, jbyte jlog_level) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->info_log_level =
      static_cast<ROCKSDB_NAMESPACE::InfoLogLevel>(jlog_level);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    infoLogLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_DBOptions_infoLogLevel(
    JNIEnv*, jobject, jlong jhandle) {
  return static_cast<jbyte>(
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->info_log_level);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxTotalWalSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setMaxTotalWalSize(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_total_wal_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->max_total_wal_size =
      static_cast<jlong>(jmax_total_wal_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxTotalWalSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_maxTotalWalSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_total_wal_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxOpenFiles
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxOpenFiles(
    JNIEnv*, jobject, jlong jhandle, jint max_open_files) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->max_open_files =
      static_cast<int>(max_open_files);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxOpenFiles
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxOpenFiles(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_open_files;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxFileOpeningThreads
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxFileOpeningThreads(
    JNIEnv*, jobject, jlong jhandle, jint jmax_file_opening_threads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_file_opening_threads = static_cast<int>(jmax_file_opening_threads);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxFileOpeningThreads
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxFileOpeningThreads(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<int>(opt->max_file_opening_threads);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setStatistics
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setStatistics(
    JNIEnv*, jobject, jlong jhandle, jlong jstatistics_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  auto* pSptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::StatisticsJni>*>(
          jstatistics_handle);
  opt->statistics = *pSptr;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    statistics
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_statistics(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  std::shared_ptr<ROCKSDB_NAMESPACE::Statistics> sptr = opt->statistics;
  if (sptr == nullptr) {
    return 0;
  } else {
    std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>* pSptr =
        new std::shared_ptr<ROCKSDB_NAMESPACE::Statistics>(sptr);
    return reinterpret_cast<jlong>(pSptr);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUseFsync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUseFsync(
    JNIEnv*, jobject, jlong jhandle, jboolean use_fsync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->use_fsync =
      static_cast<bool>(use_fsync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    useFsync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_useFsync(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->use_fsync;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_DBOptions_setDbPaths(
    JNIEnv* env, jobject, jlong jhandle, jobjectArray jpaths,
    jlongArray jtarget_sizes) {
  std::vector<ROCKSDB_NAMESPACE::DbPath> db_paths;
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, nullptr);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jboolean has_exception = JNI_FALSE;
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    jobject jpath =
        reinterpret_cast<jstring>(env->GetObjectArrayElement(jpaths, i));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    std::string path = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
        env, static_cast<jstring>(jpath), &has_exception);
    env->DeleteLocalRef(jpath);

    if (has_exception == JNI_TRUE) {
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    jlong jtarget_size = ptr_jtarget_size[i];

    db_paths.push_back(
        ROCKSDB_NAMESPACE::DbPath(path, static_cast<uint64_t>(jtarget_size)));
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->db_paths = db_paths;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dbPathsLen
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_dbPathsLen(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->db_paths.size());
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_DBOptions_dbPaths(
    JNIEnv* env, jobject, jlong jhandle, jobjectArray jpaths,
    jlongArray jtarget_sizes) {
  jboolean is_copy;
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, &is_copy);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    ROCKSDB_NAMESPACE::DbPath db_path = opt->db_paths[i];

    jstring jpath = env->NewStringUTF(db_path.path.c_str());
    if (jpath == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    env->SetObjectArrayElement(jpaths, i, jpath);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jpath);
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    ptr_jtarget_size[i] = static_cast<jint>(db_path.target_size);
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size,
                                is_copy == JNI_TRUE ? 0 : JNI_ABORT);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDbLogDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_DBOptions_setDbLogDir(
    JNIEnv* env, jobject, jlong jhandle, jstring jdb_log_dir) {
  const char* log_dir = env->GetStringUTFChars(jdb_log_dir, nullptr);
  if (log_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->db_log_dir.assign(
      log_dir);
  env->ReleaseStringUTFChars(jdb_log_dir, log_dir);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dbLogDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_DBOptions_dbLogDir(
    JNIEnv* env, jobject, jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
          ->db_log_dir.c_str());
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_DBOptions_setWalDir(
    JNIEnv* env, jobject, jlong jhandle, jstring jwal_dir) {
  const char* wal_dir = env->GetStringUTFChars(jwal_dir, 0);
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->wal_dir.assign(
      wal_dir);
  env->ReleaseStringUTFChars(jwal_dir, wal_dir);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_DBOptions_walDir(
    JNIEnv* env, jobject, jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
          ->wal_dir.c_str());
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDeleteObsoleteFilesPeriodMicros
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setDeleteObsoleteFilesPeriodMicros(
    JNIEnv*, jobject, jlong jhandle, jlong micros) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->delete_obsolete_files_period_micros = static_cast<int64_t>(micros);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    deleteObsoleteFilesPeriodMicros
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_deleteObsoleteFilesPeriodMicros(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->delete_obsolete_files_period_micros;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setBaseBackgroundCompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setBaseBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle, jint max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->base_background_compactions = static_cast<int>(max);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    baseBackgroundCompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_baseBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->base_background_compactions;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxBackgroundCompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle, jint max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_background_compactions = static_cast<int>(max);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxBackgroundCompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_background_compactions;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxSubcompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxSubcompactions(
    JNIEnv*, jobject, jlong jhandle, jint max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->max_subcompactions =
      static_cast<int32_t>(max);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxSubcompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxSubcompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_subcompactions;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxBackgroundFlushes
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxBackgroundFlushes(
    JNIEnv*, jobject, jlong jhandle, jint max_background_flushes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_background_flushes = static_cast<int>(max_background_flushes);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxBackgroundFlushes
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxBackgroundFlushes(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_background_flushes;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxBackgroundJobs
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxBackgroundJobs(
    JNIEnv*, jobject, jlong jhandle, jint max_background_jobs) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_background_jobs = static_cast<int>(max_background_jobs);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxBackgroundJobs
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxBackgroundJobs(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_background_jobs;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxLogFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setMaxLogFileSize(
    JNIEnv* env, jobject, jlong jhandle, jlong max_log_file_size) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(max_log_file_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
        ->max_log_file_size = max_log_file_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxLogFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_maxLogFileSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_log_file_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setLogFileTimeToRoll
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setLogFileTimeToRoll(
    JNIEnv* env, jobject, jlong jhandle, jlong log_file_time_to_roll) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      log_file_time_to_roll);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
        ->log_file_time_to_roll = log_file_time_to_roll;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    logFileTimeToRoll
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_logFileTimeToRoll(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->log_file_time_to_roll;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setKeepLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setKeepLogFileNum(
    JNIEnv* env, jobject, jlong jhandle, jlong keep_log_file_num) {
  auto s =
      ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(keep_log_file_num);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
        ->keep_log_file_num = keep_log_file_num;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    keepLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_keepLogFileNum(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->keep_log_file_num;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setRecycleLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setRecycleLogFileNum(
    JNIEnv* env, jobject, jlong jhandle, jlong recycle_log_file_num) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      recycle_log_file_num);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
        ->recycle_log_file_num = recycle_log_file_num;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    recycleLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_recycleLogFileNum(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->recycle_log_file_num;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxManifestFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setMaxManifestFileSize(
    JNIEnv*, jobject, jlong jhandle, jlong max_manifest_file_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_manifest_file_size = static_cast<int64_t>(max_manifest_file_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxManifestFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_maxManifestFileSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->max_manifest_file_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setTableCacheNumshardbits
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setTableCacheNumshardbits(
    JNIEnv*, jobject, jlong jhandle, jint table_cache_numshardbits) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->table_cache_numshardbits = static_cast<int>(table_cache_numshardbits);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    tableCacheNumshardbits
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_tableCacheNumshardbits(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->table_cache_numshardbits;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalTtlSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWalTtlSeconds(
    JNIEnv*, jobject, jlong jhandle, jlong WAL_ttl_seconds) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->WAL_ttl_seconds =
      static_cast<int64_t>(WAL_ttl_seconds);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_walTtlSeconds(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->WAL_ttl_seconds;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalSizeLimitMB
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWalSizeLimitMB(
    JNIEnv*, jobject, jlong jhandle, jlong WAL_size_limit_MB) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->WAL_size_limit_MB =
      static_cast<int64_t>(WAL_size_limit_MB);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_walSizeLimitMB(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->WAL_size_limit_MB;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxWriteBatchGroupSizeBytes
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setMaxWriteBatchGroupSizeBytes(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_write_batch_group_size_bytes) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->max_write_batch_group_size_bytes =
      static_cast<uint64_t>(jmax_write_batch_group_size_bytes);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxWriteBatchGroupSizeBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_maxWriteBatchGroupSizeBytes(JNIEnv*, jclass,
                                                             jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->max_write_batch_group_size_bytes);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setManifestPreallocationSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setManifestPreallocationSize(
    JNIEnv* env, jobject, jlong jhandle, jlong preallocation_size) {
  auto s = ROCKSDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
      preallocation_size);
  if (s.ok()) {
    reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
        ->manifest_preallocation_size = preallocation_size;
  } else {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    manifestPreallocationSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_manifestPreallocationSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->manifest_preallocation_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    useDirectReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_useDirectReads(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->use_direct_reads;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUseDirectReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUseDirectReads(
    JNIEnv*, jobject, jlong jhandle, jboolean use_direct_reads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->use_direct_reads =
      static_cast<bool>(use_direct_reads);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    useDirectIoForFlushAndCompaction
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_useDirectIoForFlushAndCompaction(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->use_direct_io_for_flush_and_compaction;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUseDirectReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUseDirectIoForFlushAndCompaction(
    JNIEnv*, jobject, jlong jhandle,
    jboolean use_direct_io_for_flush_and_compaction) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->use_direct_io_for_flush_and_compaction =
      static_cast<bool>(use_direct_io_for_flush_and_compaction);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowFAllocate
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowFAllocate(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_fallocate) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->allow_fallocate =
      static_cast<bool>(jallow_fallocate);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowFAllocate
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowFAllocate(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->allow_fallocate);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowMmapReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowMmapReads(
    JNIEnv*, jobject, jlong jhandle, jboolean allow_mmap_reads) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->allow_mmap_reads =
      static_cast<bool>(allow_mmap_reads);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowMmapReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowMmapReads(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->allow_mmap_reads;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowMmapWrites
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowMmapWrites(
    JNIEnv*, jobject, jlong jhandle, jboolean allow_mmap_writes) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->allow_mmap_writes =
      static_cast<bool>(allow_mmap_writes);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowMmapWrites
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowMmapWrites(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->allow_mmap_writes;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setIsFdCloseOnExec
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setIsFdCloseOnExec(
    JNIEnv*, jobject, jlong jhandle, jboolean is_fd_close_on_exec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->is_fd_close_on_exec = static_cast<bool>(is_fd_close_on_exec);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    isFdCloseOnExec
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_isFdCloseOnExec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->is_fd_close_on_exec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setStatsDumpPeriodSec
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setStatsDumpPeriodSec(
    JNIEnv*, jobject, jlong jhandle, jint jstats_dump_period_sec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->stats_dump_period_sec =
      static_cast<unsigned int>(jstats_dump_period_sec);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    statsDumpPeriodSec
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_statsDumpPeriodSec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->stats_dump_period_sec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setStatsPersistPeriodSec
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setStatsPersistPeriodSec(
    JNIEnv*, jobject, jlong jhandle, jint jstats_persist_period_sec) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->stats_persist_period_sec =
      static_cast<unsigned int>(jstats_persist_period_sec);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    statsPersistPeriodSec
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_statsPersistPeriodSec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->stats_persist_period_sec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setStatsHistoryBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setStatsHistoryBufferSize(
    JNIEnv*, jobject, jlong jhandle, jlong jstats_history_buffer_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->stats_history_buffer_size =
      static_cast<size_t>(jstats_history_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    statsHistoryBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_statsHistoryBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->stats_history_buffer_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAdviseRandomOnOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAdviseRandomOnOpen(
    JNIEnv*, jobject, jlong jhandle, jboolean advise_random_on_open) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->advise_random_on_open = static_cast<bool>(advise_random_on_open);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    adviseRandomOnOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_adviseRandomOnOpen(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->advise_random_on_open;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDbWriteBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setDbWriteBufferSize(
    JNIEnv*, jobject, jlong jhandle, jlong jdb_write_buffer_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->db_write_buffer_size = static_cast<size_t>(jdb_write_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWriteBufferManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWriteBufferManager(
    JNIEnv*, jobject, jlong jdb_options_handle,
    jlong jwrite_buffer_manager_handle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jwrite_buffer_manager_handle);
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jdb_options_handle)
      ->write_buffer_manager = *write_buffer_manager;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dbWriteBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_dbWriteBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->db_write_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAccessHintOnCompactionStart
 * Signature: (JB)V
 */
void Java_org_rocksdb_DBOptions_setAccessHintOnCompactionStart(
    JNIEnv*, jobject, jlong jhandle, jbyte jaccess_hint_value) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->access_hint_on_compaction_start =
      ROCKSDB_NAMESPACE::AccessHintJni::toCppAccessHint(jaccess_hint_value);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    accessHintOnCompactionStart
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_DBOptions_accessHintOnCompactionStart(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::AccessHintJni::toJavaAccessHint(
      opt->access_hint_on_compaction_start);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setNewTableReaderForCompactionInputs
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setNewTableReaderForCompactionInputs(
    JNIEnv*, jobject, jlong jhandle,
    jboolean jnew_table_reader_for_compaction_inputs) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->new_table_reader_for_compaction_inputs =
      static_cast<bool>(jnew_table_reader_for_compaction_inputs);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    newTableReaderForCompactionInputs
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_newTableReaderForCompactionInputs(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<bool>(opt->new_table_reader_for_compaction_inputs);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setCompactionReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setCompactionReadaheadSize(
    JNIEnv*, jobject, jlong jhandle, jlong jcompaction_readahead_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->compaction_readahead_size =
      static_cast<size_t>(jcompaction_readahead_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    compactionReadaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_compactionReadaheadSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setRandomAccessMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setRandomAccessMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle, jlong jrandom_access_max_buffer_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->random_access_max_buffer_size =
      static_cast<size_t>(jrandom_access_max_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    randomAccessMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_randomAccessMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->random_access_max_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWritableFileMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWritableFileMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle, jlong jwritable_file_max_buffer_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->writable_file_max_buffer_size =
      static_cast<size_t>(jwritable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    writableFileMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_writableFileMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->writable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUseAdaptiveMutex
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUseAdaptiveMutex(
    JNIEnv*, jobject, jlong jhandle, jboolean use_adaptive_mutex) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->use_adaptive_mutex =
      static_cast<bool>(use_adaptive_mutex);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    useAdaptiveMutex
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_useAdaptiveMutex(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->use_adaptive_mutex;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setBytesPerSync(
    JNIEnv*, jobject, jlong jhandle, jlong bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->bytes_per_sync =
      static_cast<int64_t>(bytes_per_sync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    bytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_bytesPerSync(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->bytes_per_sync;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWalBytesPerSync(
    JNIEnv*, jobject, jlong jhandle, jlong jwal_bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)->wal_bytes_per_sync =
      static_cast<int64_t>(jwal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walBytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_walBytesPerSync(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->wal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setStrictBytesPerSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setStrictBytesPerSync(
    JNIEnv*, jobject, jlong jhandle, jboolean jstrict_bytes_per_sync) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->strict_bytes_per_sync = jstrict_bytes_per_sync == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    strictBytesPerSync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_strictBytesPerSync(
    JNIEnv*, jobject, jlong jhandle) {
  return static_cast<jboolean>(
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
          ->strict_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEventListeners
 * Signature: (J[J)V
 */
void Java_org_rocksdb_DBOptions_setEventListeners(JNIEnv* env, jclass,
                                                  jlong jhandle,
                                                  jlongArray jlistener_array) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  rocksdb_set_event_listeners_helper(env, jlistener_array, opt->listeners);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    eventListeners
 * Signature: (J)[Lorg/rocksdb/AbstractEventListener;
 */
jobjectArray Java_org_rocksdb_DBOptions_eventListeners(JNIEnv* env, jclass,
                                                       jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return rocksdb_get_event_listeners_helper(env, opt->listeners);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDelayedWriteRate
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setDelayedWriteRate(
    JNIEnv*, jobject, jlong jhandle, jlong jdelayed_write_rate) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->delayed_write_rate = static_cast<uint64_t>(jdelayed_write_rate);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    delayedWriteRate
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_delayedWriteRate(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->delayed_write_rate);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEnablePipelinedWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setEnablePipelinedWrite(
    JNIEnv*, jobject, jlong jhandle, jboolean jenable_pipelined_write) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->enable_pipelined_write = jenable_pipelined_write == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    enablePipelinedWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_enablePipelinedWrite(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->enable_pipelined_write);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUnorderedWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUnorderedWrite(
        JNIEnv*, jobject, jlong jhandle, jboolean junordered_write) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->unordered_write = junordered_write == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    unorderedWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_unorderedWrite(
        JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->unordered_write);
}


/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEnableThreadTracking
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setEnableThreadTracking(
    JNIEnv*, jobject, jlong jhandle, jboolean jenable_thread_tracking) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->enable_thread_tracking = jenable_thread_tracking == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    enableThreadTracking
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_enableThreadTracking(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->enable_thread_tracking);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowConcurrentMemtableWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowConcurrentMemtableWrite(
    JNIEnv*, jobject, jlong jhandle, jboolean allow) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->allow_concurrent_memtable_write = static_cast<bool>(allow);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowConcurrentMemtableWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowConcurrentMemtableWrite(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->allow_concurrent_memtable_write;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEnableWriteThreadAdaptiveYield
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setEnableWriteThreadAdaptiveYield(
    JNIEnv*, jobject, jlong jhandle, jboolean yield) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->enable_write_thread_adaptive_yield = static_cast<bool>(yield);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    enableWriteThreadAdaptiveYield
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_enableWriteThreadAdaptiveYield(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->enable_write_thread_adaptive_yield;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWriteThreadMaxYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWriteThreadMaxYieldUsec(
    JNIEnv*, jobject, jlong jhandle, jlong max) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->write_thread_max_yield_usec = static_cast<int64_t>(max);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    writeThreadMaxYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_writeThreadMaxYieldUsec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->write_thread_max_yield_usec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWriteThreadSlowYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWriteThreadSlowYieldUsec(
    JNIEnv*, jobject, jlong jhandle, jlong slow) {
  reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->write_thread_slow_yield_usec = static_cast<int64_t>(slow);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    writeThreadSlowYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_writeThreadSlowYieldUsec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle)
      ->write_thread_slow_yield_usec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setSkipStatsUpdateOnDbOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setSkipStatsUpdateOnDbOpen(
    JNIEnv*, jobject, jlong jhandle, jboolean jskip_stats_update_on_db_open) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->skip_stats_update_on_db_open =
      static_cast<bool>(jskip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    skipStatsUpdateOnDbOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_skipStatsUpdateOnDbOpen(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->skip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setSkipCheckingSstFileSizesOnDbOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setSkipCheckingSstFileSizesOnDbOpen(
    JNIEnv*, jclass, jlong jhandle,
    jboolean jskip_checking_sst_file_sizes_on_db_open) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->skip_checking_sst_file_sizes_on_db_open =
      static_cast<bool>(jskip_checking_sst_file_sizes_on_db_open);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    skipCheckingSstFileSizesOnDbOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_skipCheckingSstFileSizesOnDbOpen(
    JNIEnv*, jclass, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->skip_checking_sst_file_sizes_on_db_open);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalRecoveryMode
 * Signature: (JB)V
 */
void Java_org_rocksdb_DBOptions_setWalRecoveryMode(
    JNIEnv*, jobject, jlong jhandle, jbyte jwal_recovery_mode_value) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->wal_recovery_mode =
      ROCKSDB_NAMESPACE::WALRecoveryModeJni::toCppWALRecoveryMode(
          jwal_recovery_mode_value);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walRecoveryMode
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_DBOptions_walRecoveryMode(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::WALRecoveryModeJni::toJavaWALRecoveryMode(
      opt->wal_recovery_mode);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllow2pc
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllow2pc(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_2pc) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->allow_2pc = static_cast<bool>(jallow_2pc);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allow2pc
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allow2pc(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->allow_2pc);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setRowCache
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setRowCache(
    JNIEnv*, jobject, jlong jhandle, jlong jrow_cache_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  auto* row_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(
          jrow_cache_handle);
  opt->row_cache = *row_cache;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalFilter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWalFilter(
    JNIEnv*, jobject, jlong jhandle, jlong jwal_filter_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  auto* wal_filter = reinterpret_cast<ROCKSDB_NAMESPACE::WalFilterJniCallback*>(
      jwal_filter_handle);
  opt->wal_filter = wal_filter;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setFailIfOptionsFileError
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setFailIfOptionsFileError(
    JNIEnv*, jobject, jlong jhandle, jboolean jfail_if_options_file_error) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->fail_if_options_file_error =
      static_cast<bool>(jfail_if_options_file_error);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    failIfOptionsFileError
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_failIfOptionsFileError(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->fail_if_options_file_error);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDumpMallocStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setDumpMallocStats(
    JNIEnv*, jobject, jlong jhandle, jboolean jdump_malloc_stats) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->dump_malloc_stats = static_cast<bool>(jdump_malloc_stats);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dumpMallocStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_dumpMallocStats(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->dump_malloc_stats);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAvoidFlushDuringRecovery
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAvoidFlushDuringRecovery(
    JNIEnv*, jobject, jlong jhandle, jboolean javoid_flush_during_recovery) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->avoid_flush_during_recovery =
      static_cast<bool>(javoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    avoidFlushDuringRecovery
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_avoidFlushDuringRecovery(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->avoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowIngestBehind
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowIngestBehind(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_ingest_behind) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->allow_ingest_behind = jallow_ingest_behind == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowIngestBehind
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowIngestBehind(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->allow_ingest_behind);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setPreserveDeletes
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setPreserveDeletes(
    JNIEnv*, jobject, jlong jhandle, jboolean jpreserve_deletes) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->preserve_deletes = jpreserve_deletes == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    preserveDeletes
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_preserveDeletes(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->preserve_deletes);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setTwoWriteQueues
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setTwoWriteQueues(
    JNIEnv*, jobject, jlong jhandle, jboolean jtwo_write_queues) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->two_write_queues = jtwo_write_queues == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    twoWriteQueues
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_twoWriteQueues(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->two_write_queues);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setManualWalFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setManualWalFlush(
    JNIEnv*, jobject, jlong jhandle, jboolean jmanual_wal_flush) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->manual_wal_flush = jmanual_wal_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    manualWalFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_manualWalFlush(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->manual_wal_flush);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAtomicFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAtomicFlush(
    JNIEnv*, jobject, jlong jhandle, jboolean jatomic_flush) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->atomic_flush = jatomic_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    atomicFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_atomicFlush(
    JNIEnv *, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->atomic_flush);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAvoidFlushDuringShutdown
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAvoidFlushDuringShutdown(
    JNIEnv*, jobject, jlong jhandle, jboolean javoid_flush_during_shutdown) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->avoid_flush_during_shutdown =
      static_cast<bool>(javoid_flush_during_shutdown);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    avoidFlushDuringShutdown
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_avoidFlushDuringShutdown(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->avoid_flush_during_shutdown);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAvoidUnnecessaryBlockingIO
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAvoidUnnecessaryBlockingIO(
    JNIEnv*, jclass, jlong jhandle, jboolean avoid_blocking_io) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->avoid_unnecessary_blocking_io = static_cast<bool>(avoid_blocking_io);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    avoidUnnecessaryBlockingIO
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_avoidUnnecessaryBlockingIO(JNIEnv*, jclass,
                                                               jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->avoid_unnecessary_blocking_io);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setPersistStatsToDisk
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setPersistStatsToDisk(
    JNIEnv*, jclass, jlong jhandle, jboolean persist_stats_to_disk) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->persist_stats_to_disk = static_cast<bool>(persist_stats_to_disk);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    persistStatsToDisk
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_persistStatsToDisk(JNIEnv*, jclass,
                                                       jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->persist_stats_to_disk);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWriteDbidToManifest
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setWriteDbidToManifest(
    JNIEnv*, jclass, jlong jhandle, jboolean jwrite_dbid_to_manifest) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->write_dbid_to_manifest = static_cast<bool>(jwrite_dbid_to_manifest);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    writeDbidToManifest
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_writeDbidToManifest(JNIEnv*, jclass,
                                                        jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->write_dbid_to_manifest);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setLogReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setLogReadaheadSize(JNIEnv*, jclass,
                                                    jlong jhandle,
                                                    jlong jlog_readahead_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->log_readahead_size = static_cast<size_t>(jlog_readahead_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    logReasaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_logReadaheadSize(JNIEnv*, jclass,
                                                  jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->log_readahead_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setBestEffortsRecovery
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setBestEffortsRecovery(
    JNIEnv*, jclass, jlong jhandle, jboolean jbest_efforts_recovery) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->best_efforts_recovery = static_cast<bool>(jbest_efforts_recovery);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    bestEffortsRecovery
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_bestEffortsRecovery(JNIEnv*, jclass,
                                                        jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->best_efforts_recovery);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxBgErrorResumeCount
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxBgErrorResumeCount(
    JNIEnv*, jclass, jlong jhandle, jint jmax_bgerror_resume_count) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->max_bgerror_resume_count = static_cast<int>(jmax_bgerror_resume_count);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxBgerrorResumeCount
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxBgerrorResumeCount(JNIEnv*, jclass,
                                                      jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jint>(opt->max_bgerror_resume_count);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setBgerrorResumeRetryInterval
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setBgerrorResumeRetryInterval(
    JNIEnv*, jclass, jlong jhandle, jlong jbgerror_resume_retry_interval) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  opt->bgerror_resume_retry_interval =
      static_cast<uint64_t>(jbgerror_resume_retry_interval);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    bgerrorResumeRetryInterval
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_bgerrorResumeRetryInterval(JNIEnv*, jclass,
                                                            jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->bgerror_resume_retry_interval);
}

//////////////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::WriteOptions

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    newWriteOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_WriteOptions_newWriteOptions(
    JNIEnv*, jclass) {
  auto* op = new ROCKSDB_NAMESPACE::WriteOptions();
  return reinterpret_cast<jlong>(op);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    copyWriteOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteOptions_copyWriteOptions(
    JNIEnv*, jclass, jlong jhandle) {
  auto new_opt = new ROCKSDB_NAMESPACE::WriteOptions(
      *(reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    disposeInternal
 * Signature: ()V
 */
void Java_org_rocksdb_WriteOptions_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle);
  assert(write_options != nullptr);
  delete write_options;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setSync(
    JNIEnv*, jobject, jlong jhandle, jboolean jflag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)->sync = jflag;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    sync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_sync(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)->sync;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setDisableWAL
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setDisableWAL(
    JNIEnv*, jobject, jlong jhandle, jboolean jflag) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)->disableWAL =
      jflag;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    disableWAL
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_disableWAL(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)
      ->disableWAL;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setIgnoreMissingColumnFamilies
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setIgnoreMissingColumnFamilies(
    JNIEnv*, jobject, jlong jhandle,
    jboolean jignore_missing_column_families) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)
      ->ignore_missing_column_families =
      static_cast<bool>(jignore_missing_column_families);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    ignoreMissingColumnFamilies
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_ignoreMissingColumnFamilies(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)
      ->ignore_missing_column_families;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setNoSlowdown
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setNoSlowdown(
    JNIEnv*, jobject, jlong jhandle, jboolean jno_slowdown) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)->no_slowdown =
      static_cast<bool>(jno_slowdown);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    noSlowdown
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_noSlowdown(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)
      ->no_slowdown;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setLowPri
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setLowPri(
    JNIEnv*, jobject, jlong jhandle, jboolean jlow_pri) {
  reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)->low_pri =
      static_cast<bool>(jlow_pri);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    lowPri
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_lowPri(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle)->low_pri;
}

/////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::ReadOptions

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    newReadOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_ReadOptions_newReadOptions__(
    JNIEnv*, jclass) {
  auto* read_options = new ROCKSDB_NAMESPACE::ReadOptions();
  return reinterpret_cast<jlong>(read_options);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    newReadOptions
 * Signature: (ZZ)J
 */
jlong Java_org_rocksdb_ReadOptions_newReadOptions__ZZ(
    JNIEnv*, jclass, jboolean jverify_checksums, jboolean jfill_cache) {
  auto* read_options = new ROCKSDB_NAMESPACE::ReadOptions(
      static_cast<bool>(jverify_checksums), static_cast<bool>(jfill_cache));
  return reinterpret_cast<jlong>(read_options);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    copyReadOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_copyReadOptions(
    JNIEnv*, jclass, jlong jhandle) {
  auto new_opt = new ROCKSDB_NAMESPACE::ReadOptions(
      *(reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ReadOptions_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* read_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  assert(read_options != nullptr);
  delete read_options;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setVerifyChecksums
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setVerifyChecksums(
    JNIEnv*, jobject, jlong jhandle, jboolean jverify_checksums) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->verify_checksums =
      static_cast<bool>(jverify_checksums);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    verifyChecksums
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_verifyChecksums(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
      ->verify_checksums;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setFillCache
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setFillCache(
    JNIEnv*, jobject, jlong jhandle, jboolean jfill_cache) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->fill_cache =
      static_cast<bool>(jfill_cache);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    fillCache
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_fillCache(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->fill_cache;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setTailing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setTailing(
    JNIEnv*, jobject, jlong jhandle, jboolean jtailing) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->tailing =
      static_cast<bool>(jtailing);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    tailing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_tailing(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->tailing;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    managed
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_managed(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->managed;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setManaged
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setManaged(
    JNIEnv*, jobject, jlong jhandle, jboolean jmanaged) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->managed =
      static_cast<bool>(jmanaged);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    totalOrderSeek
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_totalOrderSeek(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
      ->total_order_seek;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setTotalOrderSeek
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setTotalOrderSeek(
    JNIEnv*, jobject, jlong jhandle, jboolean jtotal_order_seek) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->total_order_seek =
      static_cast<bool>(jtotal_order_seek);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    prefixSameAsStart
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_prefixSameAsStart(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
      ->prefix_same_as_start;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setPrefixSameAsStart
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setPrefixSameAsStart(
    JNIEnv*, jobject, jlong jhandle, jboolean jprefix_same_as_start) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
      ->prefix_same_as_start = static_cast<bool>(jprefix_same_as_start);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    pinData
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_pinData(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->pin_data;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setPinData
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setPinData(
    JNIEnv*, jobject, jlong jhandle, jboolean jpin_data) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->pin_data =
      static_cast<bool>(jpin_data);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    backgroundPurgeOnIteratorCleanup
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_backgroundPurgeOnIteratorCleanup(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jboolean>(opt->background_purge_on_iterator_cleanup);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setBackgroundPurgeOnIteratorCleanup
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setBackgroundPurgeOnIteratorCleanup(
    JNIEnv*, jobject, jlong jhandle,
    jboolean jbackground_purge_on_iterator_cleanup) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->background_purge_on_iterator_cleanup =
      static_cast<bool>(jbackground_purge_on_iterator_cleanup);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    readaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_readaheadSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jlong>(opt->readahead_size);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setReadaheadSize(
    JNIEnv*, jobject, jlong jhandle, jlong jreadahead_size) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->readahead_size = static_cast<size_t>(jreadahead_size);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    maxSkippableInternalKeys
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_maxSkippableInternalKeys(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jlong>(opt->max_skippable_internal_keys);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setMaxSkippableInternalKeys
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setMaxSkippableInternalKeys(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_skippable_internal_keys) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->max_skippable_internal_keys =
      static_cast<uint64_t>(jmax_skippable_internal_keys);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    ignoreRangeDeletions
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_ignoreRangeDeletions(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jboolean>(opt->ignore_range_deletions);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setIgnoreRangeDeletions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setIgnoreRangeDeletions(
    JNIEnv*, jobject, jlong jhandle, jboolean jignore_range_deletions) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->ignore_range_deletions = static_cast<bool>(jignore_range_deletions);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setSnapshot
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setSnapshot(
    JNIEnv*, jobject, jlong jhandle, jlong jsnapshot) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->snapshot =
      reinterpret_cast<ROCKSDB_NAMESPACE::Snapshot*>(jsnapshot);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    snapshot
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_snapshot(
    JNIEnv*, jobject, jlong jhandle) {
  auto& snapshot =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->snapshot;
  return reinterpret_cast<jlong>(snapshot);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    readTier
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ReadOptions_readTier(
    JNIEnv*, jobject, jlong jhandle) {
  return static_cast<jbyte>(
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->read_tier);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setReadTier
 * Signature: (JB)V
 */
void Java_org_rocksdb_ReadOptions_setReadTier(
    JNIEnv*, jobject, jlong jhandle, jbyte jread_tier) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)->read_tier =
      static_cast<ROCKSDB_NAMESPACE::ReadTier>(jread_tier);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setIterateUpperBound
 * Signature: (JJ)I
 */
void Java_org_rocksdb_ReadOptions_setIterateUpperBound(
    JNIEnv*, jobject, jlong jhandle, jlong jupper_bound_slice_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
      ->iterate_upper_bound =
      reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(jupper_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    iterateUpperBound
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_iterateUpperBound(
    JNIEnv*, jobject, jlong jhandle) {
  auto& upper_bound_slice_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
          ->iterate_upper_bound;
  return reinterpret_cast<jlong>(upper_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setIterateLowerBound
 * Signature: (JJ)I
 */
void Java_org_rocksdb_ReadOptions_setIterateLowerBound(
    JNIEnv*, jobject, jlong jhandle, jlong jlower_bound_slice_handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
      ->iterate_lower_bound =
      reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(jlower_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    iterateLowerBound
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_iterateLowerBound(
    JNIEnv*, jobject, jlong jhandle) {
  auto& lower_bound_slice_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle)
          ->iterate_lower_bound;
  return reinterpret_cast<jlong>(lower_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setTableFilter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setTableFilter(
    JNIEnv*, jobject, jlong jhandle, jlong jjni_table_filter_handle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  auto* jni_table_filter =
      reinterpret_cast<ROCKSDB_NAMESPACE::TableFilterJniCallback*>(
          jjni_table_filter_handle);
  opt->table_filter = jni_table_filter->GetTableFilterFunction();
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setIterStartSeqnum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setIterStartSeqnum(
    JNIEnv*, jobject, jlong jhandle, jlong jiter_start_seqnum) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  opt->iter_start_seqnum = static_cast<uint64_t>(jiter_start_seqnum);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    iterStartSeqnum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_iterStartSeqnum(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jhandle);
  return static_cast<jlong>(opt->iter_start_seqnum);
}

/////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::ComparatorOptions

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    newComparatorOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_ComparatorOptions_newComparatorOptions(
    JNIEnv*, jclass) {
  auto* comparator_opt = new ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions();
  return reinterpret_cast<jlong>(comparator_opt);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    reusedSynchronisationType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ComparatorOptions_reusedSynchronisationType(
    JNIEnv *, jobject, jlong jhandle) {
  auto* comparator_opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(
          jhandle);
  return ROCKSDB_NAMESPACE::ReusedSynchronisationTypeJni::
      toJavaReusedSynchronisationType(
          comparator_opt->reused_synchronisation_type);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    setReusedSynchronisationType
 * Signature: (JB)V
 */
void Java_org_rocksdb_ComparatorOptions_setReusedSynchronisationType(
    JNIEnv*, jobject, jlong jhandle, jbyte jreused_synhcronisation_type) {
  auto* comparator_opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(
          jhandle);
  comparator_opt->reused_synchronisation_type =
      ROCKSDB_NAMESPACE::ReusedSynchronisationTypeJni::
          toCppReusedSynchronisationType(jreused_synhcronisation_type);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    useDirectBuffer
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ComparatorOptions_useDirectBuffer(
    JNIEnv*, jobject, jlong jhandle) {
  return static_cast<jboolean>(
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(
          jhandle)
          ->direct_buffer);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    setUseDirectBuffer
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ComparatorOptions_setUseDirectBuffer(
    JNIEnv*, jobject, jlong jhandle, jboolean jdirect_buffer) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(jhandle)
      ->direct_buffer = jdirect_buffer == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    maxReusedBufferSize
 * Signature: (J)I
 */
jint Java_org_rocksdb_ComparatorOptions_maxReusedBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  return static_cast<jint>(
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(
          jhandle)
          ->max_reused_buffer_size);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    setMaxReusedBufferSize
 * Signature: (JI)V
 */
void Java_org_rocksdb_ComparatorOptions_setMaxReusedBufferSize(
    JNIEnv*, jobject, jlong jhandle, jint jmax_reused_buffer_size) {
  reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(jhandle)
      ->max_reused_buffer_size = static_cast<int32_t>(jmax_reused_buffer_size);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ComparatorOptions_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* comparator_opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(
          jhandle);
  assert(comparator_opt != nullptr);
  delete comparator_opt;
}

/////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::FlushOptions

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    newFlushOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_FlushOptions_newFlushOptions(
    JNIEnv*, jclass) {
  auto* flush_opt = new ROCKSDB_NAMESPACE::FlushOptions();
  return reinterpret_cast<jlong>(flush_opt);
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    setWaitForFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_FlushOptions_setWaitForFlush(
    JNIEnv*, jobject, jlong jhandle, jboolean jwait) {
  reinterpret_cast<ROCKSDB_NAMESPACE::FlushOptions*>(jhandle)->wait =
      static_cast<bool>(jwait);
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    waitForFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_FlushOptions_waitForFlush(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::FlushOptions*>(jhandle)->wait;
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    setAllowWriteStall
 * Signature: (JZ)V
 */
void Java_org_rocksdb_FlushOptions_setAllowWriteStall(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_write_stall) {
  auto* flush_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::FlushOptions*>(jhandle);
  flush_options->allow_write_stall = jallow_write_stall == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    allowWriteStall
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_FlushOptions_allowWriteStall(
    JNIEnv*, jobject, jlong jhandle) {
  auto* flush_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::FlushOptions*>(jhandle);
  return static_cast<jboolean>(flush_options->allow_write_stall);
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_FlushOptions_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* flush_opt = reinterpret_cast<ROCKSDB_NAMESPACE::FlushOptions*>(jhandle);
  assert(flush_opt != nullptr);
  delete flush_opt;
}
