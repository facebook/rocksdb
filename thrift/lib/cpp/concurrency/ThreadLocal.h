/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef THRIFT_CONCURRENCY_THREADLOCAL_H_
#define THRIFT_CONCURRENCY_THREADLOCAL_H_ 1

#include "thrift/lib/cpp/Thrift.h"
#include <pthread.h>

namespace apache { namespace thrift { namespace concurrency {

template <typename T>
class DefaultThreadLocalManager;

/**
 * ThreadLocal manages thread-local storage for a particular object type.
 *
 * Each ThreadLocal object contains a separate instance of an object for each
 * thread that accesses the ThreadLocal object.
 *
 * Note that you should avoid creating too many ThreadLocal objects (e.g., such
 * as keeping a ThreadLocal member variable in commonly allocated objects).
 * The number of ThreadLocal objects cannot be larger than the value of
 * PTHREAD_KEYS_MAX, which is 1024 on many systems.
 *
 * The ManagerT template parameter controls how object allocation and
 * deallocation should be performed.  When get() is called from a thread that
 * does not already have an instance of the object, Manager::allocate() is
 * called.  When a thread exits, Manager::destroy() is called if the thread has
 * an instance of this object.
 */
template <typename T, typename ManagerT = DefaultThreadLocalManager<T> >
class ThreadLocal {
 public:
  typedef T DataType;
  typedef ManagerT Manager;

  /**
   * Create a new ThreadLocal object.
   */
  ThreadLocal() {
    int ret = pthread_key_create(&key_, &ThreadLocal::onThreadExit);
    if (ret != 0) {
      throw TLibraryException("failed to allocate new thread-local key", ret);
    }
  }

  /**
   * Access this thread's local instance of the object.
   *
   * If there is no instance of the object in this thread, Manager::allocate()
   * will be called to allocate a new instance.  (Though some Manager
   * implementations may return NULL, if each thread's instance must be
   * expilcitly initialized.)
   */
  T *get() const {
    T *obj = getNoAlloc();
    if (obj == NULL) {
      Manager m;
      obj = m.allocate();
      if (obj != NULL) {
        setImpl(obj);
      }
    }
    return obj;
  }

  /**
   * Access this thread's local instance of the object.
   *
   * If there is no instance of the object in this thread, NULL will be
   * returned.  Manager::allocate() will never be called.
   */
  T *getNoAlloc() const {
    return static_cast<T*>(pthread_getspecific(key_));
  }

  /**
   * Operator overload to perform get()
   */
  T *operator->() const {
    return get();
  }

  /**
   * Operator overload to perform get()
   */
  T &operator*() const {
    return *get();
  }

  /**
   * Set the instance of the object to be used by this thread.
   */
  void set(T* obj) {
    T *old = getNoAlloc();
    Manager m;
    m.replace(old, obj);
    setImpl(obj);
  }

  /**
   * Clear the instance of the object used by this thread.
   *
   * If this thread had a non-NULL object, Manager::destroy() will be called.
   */
  void clear() {
    T *obj = getNoAlloc();
    if (obj != NULL) {
      Manager m;
      m.destroy(obj);
      setImpl(NULL);
    }
  }

 private:
  void setImpl(T* obj) const {
    int ret = pthread_setspecific(key_, obj);
    if (ret != 0) {
      throw TLibraryException("failed to update thread-local key", ret);
    }
  }

  static void onThreadExit(void* arg) {
    T *obj = static_cast<T*>(arg);
    if (obj != NULL) {
      Manager m;
      m.destroy(obj);
    }
  }

  pthread_key_t key_;
};

template <typename T>
class DefaultThreadLocalManager {
 public:
  T* allocate() {
    return new T;
  }

  void destroy(T* t) {
    delete t;
  }

  void replace(T* oldObj, T* newObj) {
    if (oldObj != newObj) {
      delete oldObj;
    }
  }
};

template <typename T>
class DestroyOnlyThreadLocalManager {
 public:
  T* allocate() {
    return NULL;
  }

  void destroy(T* t) {
    delete t;
  }

  void replace(T* oldObj, T* newObj) {
    if (oldObj != newObj) {
      delete oldObj;
    }
  }
};

template <typename T>
class NoopThreadLocalManager {
 public:
  T* allocate() {
    return NULL;
  }

  void destroy(T*) {
  }

  void replace(T*, T*) {
  }
};

}}} // apache::thrift::concurrency

#endif // THRIFT_CONCURRENCY_THREADLOCAL_H_
