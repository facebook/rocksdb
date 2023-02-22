// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ ROCKSDB_NAMESPACE::SstFileWriter methods
// from Java side.
#ifndef ROCKSDB_LITE

#include <jni.h>
#include "include/org_rocksdb_SSTDumpTool.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_dump_tool.h"
#include "string"

void Java_org_rocksdb_SSTDumpTool_runInternal(JNIEnv *env, jobject /*obj*/, jobjectArray argsArray,
                                              jlong options_native_handle) {
  auto* options = reinterpret_cast<const ROCKSDB_NAMESPACE::Options*>(options_native_handle);
  ROCKSDB_NAMESPACE::SSTDumpTool dumpTool;
  int length = env->GetArrayLength(argsArray);
  const char* args[length + 1];
  args[0] = strdup("./sst_dump");
  for(int i = 0; i < env->GetArrayLength(argsArray); i++) {

    args[i+1] = (char*)env->GetStringUTFChars((jstring)env->
                GetObjectArrayElement(argsArray, (jsize)i), JNI_FALSE);
  }
  dumpTool.Run(length + 1, args, *options);
}

#endif  // ROCKSDB_LITE