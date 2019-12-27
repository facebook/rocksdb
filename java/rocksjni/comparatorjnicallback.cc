// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Comparator.

#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
ComparatorJniCallback::ComparatorJniCallback(
    JNIEnv* env, jobject jcomparator,
    const ComparatorJniCallbackOptions* options)
    : JniCallback(env, jcomparator),
    m_options(options),
    mtx_compare(new port::Mutex(options->use_adaptive_mutex)),
    mtx_shortest(new port::Mutex(options->use_adaptive_mutex)),
    mtx_short(new port::Mutex(options->use_adaptive_mutex)) {

  // Note: The name of a Comparator will not change during it's lifetime,
  // so we cache it in a global var
  jmethodID jname_mid = AbstractComparatorJni::getNameMethodId(env);
  if (jname_mid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }
  jstring js_name = (jstring)env->CallObjectMethod(m_jcallback_obj, jname_mid);
  if (env->ExceptionCheck()) {
    // exception thrown
    return;
  }
  jboolean has_exception = JNI_FALSE;
  m_name = JniUtil::copyString(env, js_name,
      &has_exception);  // also releases jsName
  if (has_exception == JNI_TRUE) {
    // exception thrown
    return;
  }

  m_jcompare_mid = AbstractComparatorJni::getCompareInternalMethodId(env);
  if (m_jcompare_mid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  m_jshortest_mid =
    AbstractComparatorJni::getFindShortestSeparatorInternalMethodId(env);
  if (m_jshortest_mid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  m_jshort_mid =
    AbstractComparatorJni::getFindShortSuccessorInternalMethodId(env);
  if (m_jshort_mid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  // do we need reusable buffers?
  if (m_options->max_reused_buffer_size > -1) {
    m_jcompare_buf_a = env->NewGlobalRef(ByteBufferJni::construct(
        env, m_options->direct_buffer, m_options->max_reused_buffer_size));
    if (m_jcompare_buf_a == nullptr) {
      // exception thrown: OutOfMemoryError
      return;
    }

    m_jcompare_buf_b = env->NewGlobalRef(ByteBufferJni::construct(
        env, m_options->direct_buffer, m_options->max_reused_buffer_size));
    if (m_jcompare_buf_b == nullptr) {
      // exception thrown: OutOfMemoryError
      return;
    }

    m_jshortest_buf_start = env->NewGlobalRef(ByteBufferJni::construct(
        env, m_options->direct_buffer, m_options->max_reused_buffer_size));
    if (m_jshortest_buf_start == nullptr) {
      // exception thrown: OutOfMemoryError
      return;
    }

    m_jshortest_buf_limit = env->NewGlobalRef(ByteBufferJni::construct(
        env, m_options->direct_buffer, m_options->max_reused_buffer_size));
    if (m_jshortest_buf_limit == nullptr) {
      // exception thrown: OutOfMemoryError
      return;
    }

    m_jshort_buf_key = env->NewGlobalRef(ByteBufferJni::construct(
        env, m_options->direct_buffer, m_options->max_reused_buffer_size));
    if (m_jshort_buf_key == nullptr) {
      // exception thrown: OutOfMemoryError
      return;
    }
  } else {
    m_jcompare_buf_a = nullptr;
    m_jcompare_buf_b = nullptr;
    m_jshortest_buf_start = nullptr;
    m_jshortest_buf_limit = nullptr;
    m_jshort_buf_key = nullptr;
  }
}

ComparatorJniCallback::~ComparatorJniCallback() {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  if (m_jcompare_buf_a != nullptr) {
    if (m_options->direct_buffer) {
      void* buf = env->GetDirectBufferAddress(m_jcompare_buf_a);
      delete[] static_cast<char*>(buf);
    }
    env->DeleteGlobalRef(m_jcompare_buf_a);
  }

  if (m_jcompare_buf_b != nullptr) {
    if (m_options->direct_buffer) {
      void* buf = env->GetDirectBufferAddress(m_jcompare_buf_b);
      delete[] static_cast<char*>(buf);
    }
    env->DeleteGlobalRef(m_jcompare_buf_b);
  }

  if (m_jshortest_buf_start != nullptr) {
    if (m_options->direct_buffer) {
      void* buf = env->GetDirectBufferAddress(m_jshortest_buf_start);
      delete[] static_cast<char*>(buf);
    }
    env->DeleteGlobalRef(m_jshortest_buf_start);
  }

  if (m_jshortest_buf_limit != nullptr) {
    if (m_options->direct_buffer) {
      void* buf = env->GetDirectBufferAddress(m_jshortest_buf_limit);
      delete[] static_cast<char*>(buf);
    }
    env->DeleteGlobalRef(m_jshortest_buf_limit);
  }

  if (m_jshort_buf_key != nullptr) {
    if (m_options->direct_buffer) {
      void* buf = env->GetDirectBufferAddress(m_jshort_buf_key);
      delete[] static_cast<char*>(buf);
    }
    env->DeleteGlobalRef(m_jshort_buf_key);
  }

  releaseJniEnv(attached_thread);
}

const char* ComparatorJniCallback::Name() const {
  return m_name.get();
}

int ComparatorJniCallback::Compare(const Slice& a, const Slice& b) const {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  const bool reuse_jbuf_a =
      static_cast<int64_t>(a.size()) <= m_options->max_reused_buffer_size;
  const bool reuse_jbuf_b =
      static_cast<int64_t>(b.size()) <= m_options->max_reused_buffer_size;

  // TODO(adamretter): ByteBuffer objects can potentially be cached using thread
  // local variables to avoid locking. Could make this configurable depending on
  // performance.
  if (reuse_jbuf_a || reuse_jbuf_b) {
    mtx_compare.get()->Lock();
  }

  jobject jcompare_buf_a = reuse_jbuf_a ? ReuseBuffer(env, a, m_jcompare_buf_a) : NewBuffer(env, a);
  if (jcompare_buf_a == nullptr) {
    // exception occurred
    env->ExceptionDescribe(); // print out exception to stderr
    releaseJniEnv(attached_thread);
    return 0;
  }

  jobject jcompare_buf_b = reuse_jbuf_b ? ReuseBuffer(env, b, m_jcompare_buf_b) : NewBuffer(env, b);
  if (jcompare_buf_b == nullptr) {
    // exception occurred
    env->ExceptionDescribe(); // print out exception to stderr
    if (!reuse_jbuf_a) {
      DeleteBuffer(env, jcompare_buf_a);
    }
    releaseJniEnv(attached_thread);
    return 0;
  }

  jint result =
    env->CallIntMethod(m_jcallback_obj, m_jcompare_mid,
      jcompare_buf_a, reuse_jbuf_a ? a.size() : -1,
      jcompare_buf_b, reuse_jbuf_b ? b.size() : -1);

  if (!reuse_jbuf_a) {
    DeleteBuffer(env, jcompare_buf_a);
  }
  if (!reuse_jbuf_b) {
    DeleteBuffer(env, jcompare_buf_b);
  }

  if (reuse_jbuf_a || reuse_jbuf_b) {
    mtx_compare.get()->Unlock();
  }

  if(env->ExceptionCheck()) {
    // exception thrown from CallIntMethod
    env->ExceptionDescribe(); // print out exception to stderr
    result = 0; // we could not get a result from java callback so use 0
  }

  releaseJniEnv(attached_thread);

  return result;
}

void ComparatorJniCallback::FindShortestSeparator(
    std::string* start, const Slice& limit) const {
  if (start == nullptr) {
    return;
  }

  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  const bool reuse_jbuf_start =
      static_cast<int64_t>(start->length()) <= m_options->max_reused_buffer_size;
  const bool reuse_jbuf_limit =
      static_cast<int64_t>(limit.size()) <= m_options->max_reused_buffer_size;

  // TODO(adamretter): slice object can potentially be cached using thread local
  // variable to avoid locking. Could make this configurable depending on
  // performance.
  if (reuse_jbuf_start || reuse_jbuf_limit) {
    mtx_shortest.get()->Lock();
  }

  Slice sstart(start->data(), start->length());
  jobject j_start_buf = reuse_jbuf_start ? ReuseBuffer(env, sstart, m_jshortest_buf_start) : NewBuffer(env, sstart);
  if (j_start_buf == nullptr) {
    // exception occurred
    env->ExceptionDescribe(); // print out exception to stderr
    releaseJniEnv(attached_thread);
    return;
  }

  jobject j_limit_buf = reuse_jbuf_limit ? ReuseBuffer(env, limit, m_jshortest_buf_limit) : NewBuffer(env, limit);
  if (j_limit_buf == nullptr) {
    // exception occurred
    env->ExceptionDescribe(); // print out exception to stderr
    if (!reuse_jbuf_start) {
      DeleteBuffer(env, j_start_buf);
    }
    releaseJniEnv(attached_thread);
    return;
  }

  jint jstart_len = env->CallIntMethod(m_jcallback_obj, m_jshortest_mid,
      j_start_buf, reuse_jbuf_start ? start->length() : -1,
      j_limit_buf, reuse_jbuf_limit ? limit.size() : -1);

  // if start buffer has changed in Java, update `start` with the result
  if (static_cast<size_t>(jstart_len) != start->length()) {
    bool copy_from_non_direct = false;
    if (reuse_jbuf_start) {
        // reused a buffer
        if (m_options->direct_buffer) {
          // reused direct buffer
          void* start_buf = env->GetDirectBufferAddress(j_start_buf);
          if (start_buf == nullptr) {
            rocksdb::RocksDBExceptionJni::ThrowNew(env, "Unable to get Direct Buffer Address");
            env->ExceptionDescribe();  // print out exception to stderr
            if (!reuse_jbuf_start) {
              DeleteBuffer(env, j_start_buf);
            }
            if (!reuse_jbuf_limit) {
              DeleteBuffer(env, j_limit_buf);
            }
            releaseJniEnv(attached_thread);
            return;
          }
          start->assign(static_cast<const char*>(start_buf), jstart_len);
        } else {
          // reused non-direct buffer
          copy_from_non_direct = true;
        }
    } else {
        // there was a new buffer
        copy_from_non_direct = !m_options->direct_buffer;
    }

    if (copy_from_non_direct) {
      jbyteArray jarray = ByteBufferJni::array(env, j_start_buf);
      if (jarray == nullptr) {
        env->ExceptionDescribe();  // print out exception to stderr
        if (!reuse_jbuf_start) {
          DeleteBuffer(env, j_start_buf);
        }
        if (!reuse_jbuf_limit) {
          DeleteBuffer(env, j_limit_buf);
        }
        releaseJniEnv(attached_thread);
        return;
      }
      jboolean has_exception = JNI_FALSE;
      JniUtil::byteString<std::string>(env, jarray, [start, jstart_len](const char* data, const size_t) {
        return start->assign(data, static_cast<size_t>(jstart_len));
      }, &has_exception);
      env->DeleteLocalRef(jarray);
      if (has_exception == JNI_TRUE) {
        env->ExceptionDescribe();  // print out exception to stderr
        if (!reuse_jbuf_start) {
          DeleteBuffer(env, j_start_buf);
        }
        if (!reuse_jbuf_limit) {
          DeleteBuffer(env, j_limit_buf);
        } 
        releaseJniEnv(attached_thread);
        return;
      }
    }
  }

  if (!reuse_jbuf_start) {
    DeleteBuffer(env, j_start_buf);
  }
  if (!reuse_jbuf_limit) {
    DeleteBuffer(env, j_limit_buf);
  } 

  if (reuse_jbuf_start || reuse_jbuf_limit) {
    mtx_shortest.get()->Unlock();
  }

  if(env->ExceptionCheck()) {
    // exception thrown from CallObjectMethod
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return;
  }

  releaseJniEnv(attached_thread);
}

void ComparatorJniCallback::FindShortSuccessor(
    std::string* key) const {
  if (key == nullptr) {
    return;
  }

  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  const bool reuse_jbuf_key =
      static_cast<int64_t>(key->length()) <= m_options->max_reused_buffer_size;

  // TODO(adamretter): slice object can potentially be cached using thread local
  // variable to avoid locking. Could make this configurable depending on
  // performance.
  if (reuse_jbuf_key) {
    mtx_short.get()->Lock();
  }

  Slice skey(key->data(), key->length());
  jobject j_key_buf = reuse_jbuf_key ? ReuseBuffer(env, skey, m_jshort_buf_key) : NewBuffer(env, skey);
  if (j_key_buf == nullptr) {
    // exception occurred
    env->ExceptionDescribe(); // print out exception to stderr
    releaseJniEnv(attached_thread);
    return;
  }

  jint jkey_len = env->CallIntMethod(m_jcallback_obj, m_jshort_mid,
      j_key_buf, reuse_jbuf_key ? key->length() : -1);

  // if key buffer has changed in Java, update `key` with the result
  if (static_cast<size_t>(jkey_len) != key->length()) {
    bool copy_from_non_direct = false;
    if (reuse_jbuf_key) {
        // reused a buffer
        if (m_options->direct_buffer) {
          // reused direct buffer
          void* key_buf = env->GetDirectBufferAddress(j_key_buf);
          if (key_buf == nullptr) {
            rocksdb::RocksDBExceptionJni::ThrowNew(env, "Unable to get Direct Buffer Address");
            env->ExceptionDescribe();  // print out exception to stderr
            if (!reuse_jbuf_key) {
              DeleteBuffer(env, j_key_buf);
            }
            releaseJniEnv(attached_thread);
            return;
          }
          key->assign(static_cast<const char*>(key_buf), jkey_len);
        } else {
          // reused non-direct buffer
          copy_from_non_direct = true;
        }
    } else {
        // there was a new buffer
        copy_from_non_direct = !m_options->direct_buffer;
    }

    if (copy_from_non_direct) {
      jbyteArray jarray = ByteBufferJni::array(env, j_key_buf);
      if (jarray == nullptr) {
        env->ExceptionDescribe();  // print out exception to stderr
        if (!reuse_jbuf_key) {
          DeleteBuffer(env, j_key_buf);
        }
        releaseJniEnv(attached_thread);
        return;
      }
      jboolean has_exception = JNI_FALSE;
      JniUtil::byteString<std::string>(env, jarray, [key, jkey_len](const char* data, const size_t) {
        return key->assign(data, static_cast<size_t>(jkey_len));
      }, &has_exception);
      env->DeleteLocalRef(jarray);
      if (has_exception == JNI_TRUE) {
        env->ExceptionDescribe();  // print out exception to stderr
        if (!reuse_jbuf_key) {
          DeleteBuffer(env, j_key_buf);
        }
        releaseJniEnv(attached_thread);
        return;
      }
    }
  }

  if (!reuse_jbuf_key) {
    DeleteBuffer(env, j_key_buf);
  }

  if (reuse_jbuf_key) {
    mtx_short.get()->Unlock();
  }

  if(env->ExceptionCheck()) {
    // exception thrown from CallObjectMethod
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return;
  }

  releaseJniEnv(attached_thread);
}

jobject ComparatorJniCallback::ReuseBuffer(
    JNIEnv* env, const Slice& src, jobject jreuse_buffer) const {
  // we can reuse the buffer
  if (m_options->direct_buffer) {
    // copy into direct buffer
    void* buf = env->GetDirectBufferAddress(jreuse_buffer);
    if (buf == nullptr) {
      // either memory region is undefined, given object is not a direct java.nio.Buffer, or JNI access to direct buffers is not supported by this virtual machine.
      rocksdb::RocksDBExceptionJni::ThrowNew(env, "Unable to get Direct Buffer Address");
      return nullptr;
    }
    memcpy(buf, src.data(), src.size());
  } else {
    // copy into non-direct buffer
    const jbyteArray jarray = ByteBufferJni::array(env, jreuse_buffer);
    if (jarray == nullptr) {
      // exception occurred
      return nullptr;
    }
    env->SetByteArrayRegion(jarray, 0, static_cast<jsize>(src.size()),
        const_cast<jbyte*>(reinterpret_cast<const jbyte*>(src.data())));
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(jarray);
      return nullptr;
    }
    env->DeleteLocalRef(jarray);
  }
  return jreuse_buffer;
}

jobject ComparatorJniCallback::NewBuffer(JNIEnv* env, const Slice& src) const {
  // we need a new buffer
  jobject jbuf = ByteBufferJni::constructWith(env, m_options->direct_buffer, src.data(), src.size());
  if (jbuf == nullptr) {
    // exception occurred
    return nullptr;
  }
  return jbuf;
}

void ComparatorJniCallback::DeleteBuffer(JNIEnv* env, jobject jbuffer) const {
  // if direct then free delete the underlying array too
  // if (m_options->direct_buffer) {
  //   void* buf = env->GetDirectBufferAddress(jbuffer);
  //   if (buf != nullptr) {
  //     delete[] static_cast<const char*>(buf);
  //   }
  // }
  env->DeleteLocalRef(jbuffer);
}

}  // namespace rocksdb
