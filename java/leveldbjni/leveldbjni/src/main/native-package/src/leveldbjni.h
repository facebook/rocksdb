/*******************************************************************************
 * Copyright (C) 2011, FuseSource Corp.  All rights reserved.
 *
 *     http://fusesource.com
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *    * Neither the name of FuseSource Corp. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *******************************************************************************/
#ifndef LEVELDBJNI_H
#define LEVELDBJNI_H

#ifdef HAVE_CONFIG_H
  /* configure based build.. we will use what it discovered about the platform */
  #include "config.h"
#else
  #if defined(_WIN32) || defined(_WIN64)
    /* Windows based build */
    #define HAVE_STDLIB_H 1
    #define HAVE_STRINGS_H 1
    #include <windows.h>
  #endif
#endif

#ifdef HAVE_UNISTD_H
  #include <unistd.h>
#endif

#ifdef HAVE_STDLIB_H
  #include <stdlib.h>
#endif

#ifdef HAVE_STRINGS_H
  #include <string.h>
#endif

#ifdef HAVE_SYS_ERRNO_H
  #include <sys/errno.h>
#endif

#include "hawtjni.h"
#include <stdint.h>
#include <stdarg.h>

#ifdef __cplusplus

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/write_batch.h"
#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/slice.h"

struct JNIComparator : public rocksdb::Comparator {
  jobject target;
  jmethodID compare_method;
  const char *name;

  int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
    JNIEnv *env;
    if ( hawtjni_attach_thread(&env, "leveldb") ) {
      return 0;
    }
    int rc = env->CallIntMethod(target, compare_method, (jlong)(intptr_t)&a, (jlong)(intptr_t)&b);
    hawtjni_detach_thread();
    return rc;
  }

  const char* Name() const {
     return name;
  }

  void FindShortestSeparator(std::string*, const rocksdb::Slice&) const { }
  void FindShortSuccessor(std::string*) const { }
};

struct JNILogger : public rocksdb::Logger {
  jobject target;
  jmethodID log_method;

  void Logv(const char* format, va_list ap) {
    JNIEnv *env;
    if ( hawtjni_attach_thread(&env, "leveldb") ) {
      return;
    }

    char buffer[1024];
    vsnprintf(buffer, sizeof(buffer), format, ap);

    jstring message = env->NewStringUTF(buffer);
    if( message ) {
      env->CallVoidMethod(target, log_method, message);
      env->DeleteLocalRef(message);
    }

    if (env->ExceptionOccurred() ) {
      env->ExceptionDescribe();
      env->ExceptionClear();
    }
    hawtjni_detach_thread();
  }

};

#endif


#ifdef __cplusplus
extern "C" {
#endif

void buffer_copy(const void *source, size_t source_pos, void *dest, size_t dest_pos, size_t length);

#ifdef __cplusplus
} /* extern "C" */
#endif


#endif /* LEVELDBJNI_H */
