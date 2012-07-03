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

#ifndef THRIFT_CONCURRENCY_MUTEX_H_
#define THRIFT_CONCURRENCY_MUTEX_H_ 1

#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>

namespace apache { namespace thrift { namespace concurrency {

#ifndef THRIFT_NO_CONTENTION_PROFILING

/**
 * Determines if the Thrift Mutex and ReadWriteMutex classes will attempt to
 * profile their blocking acquire methods. If this value is set to non-zero,
 * Thrift will attempt to invoke the callback once every profilingSampleRate
 * times.  However, as the sampling is not synchronized the rate is not
 * guaranteed, and could be subject to big bursts and swings.  Please ensure
 * your sampling callback is as performant as your application requires.
 *
 * The callback will get called with the wait time taken to lock the mutex in
 * usec and a (void*) that uniquely identifies the Mutex (or ReadWriteMutex)
 * being locked.
 *
 * The enableMutexProfiling() function is unsynchronized; calling this function
 * while profiling is already enabled may result in race conditions.  On
 * architectures where a pointer assignment is atomic, this is safe but there
 * is no guarantee threads will agree on a single callback within any
 * particular time period.
 */
typedef void (*MutexWaitCallback)(const void* id, int64_t waitTimeMicros);
void enableMutexProfiling(int32_t profilingSampleRate,
                          MutexWaitCallback callback);

#endif

/**
 * A simple mutex class
 *
 * @version $Id:$
 */
class Mutex {
 public:
  typedef void (*Initializer)(void*);

  // Specifying the type of the mutex with one of the static Initializer
  // methods defined in this class.
  explicit Mutex(Initializer init = DEFAULT_INITIALIZER);

  // Specifying the type of the mutex with an integer. The value has
  // to be supported by the underlying implementation, currently
  // pthread_mutex. So the possible values are PTHREAD_MUTEX_NORMAL,
  // PTHREAD_MUTEX_ERRORCHECK, PTHREAD_MUTEX_RECURSIVE and
  // PTHREAD_MUTEX_DEFAULT.
  explicit Mutex(int type);

  virtual ~Mutex() {}
  virtual void lock() const;
  virtual bool trylock() const;
  virtual bool timedlock(int64_t milliseconds) const;
  virtual void unlock() const;

  /**
   * Determine if the mutex is locked.
   *
   * This is intended to be used primarily as a debugging aid, and is not
   * guaranteed to be a fast operation.  For example, a common use case is to
   * assert(mutex.isLocked()) in functions that may only be called with a
   * particular mutex already locked.
   *
   * TODO: This method currently always returns false for recursive mutexes.
   * Avoid calling this method on recursive mutexes.
   */
  virtual bool isLocked() const;

  void* getUnderlyingImpl() const;

  static void DEFAULT_INITIALIZER(void*);
  static void ADAPTIVE_INITIALIZER(void*);
  static void RECURSIVE_INITIALIZER(void*);

 private:

  class impl;
  boost::shared_ptr<impl> impl_;
};

class ReadWriteMutex {
public:
  ReadWriteMutex();
  virtual ~ReadWriteMutex() {}

  // these get the lock and block until it is done successfully
  virtual void acquireRead() const;
  virtual void acquireWrite() const;

  // these get the lock and block until it is done successfully
  // or run out of time
  virtual bool timedRead(int64_t milliseconds) const;
  virtual bool timedWrite(int64_t milliseconds) const;

  // these attempt to get the lock, returning false immediately if they fail
  virtual bool attemptRead() const;
  virtual bool attemptWrite() const;

  // this releases both read and write locks
  virtual void release() const;

private:

  class impl;
  boost::shared_ptr<impl> impl_;
};

/**
 * A ReadWriteMutex that guarantees writers will not be starved by readers:
 * When a writer attempts to acquire the mutex, all new readers will be
 * blocked from acquiring the mutex until the writer has acquired and
 * released it. In some operating systems, this may already be guaranteed
 * by a regular ReadWriteMutex.
 */
class NoStarveReadWriteMutex : public ReadWriteMutex {
public:
  NoStarveReadWriteMutex();

  virtual void acquireRead() const;
  virtual void acquireWrite() const;

  // these get the lock and block until it is done successfully
  // or run out of time
  virtual bool timedRead(int64_t milliseconds) const;
  virtual bool timedWrite(int64_t milliseconds) const;

private:
  Mutex mutex_;
  mutable volatile bool writerWaiting_;
};

class Guard : boost::noncopyable {
 public:
  explicit Guard(const Mutex& value, int64_t timeout = 0) : mutex_(&value) {
    if (timeout == 0) {
      value.lock();
    } else if (timeout < 0) {
      if (!value.trylock()) {
        mutex_ = NULL;
      }
    } else {
      if (!value.timedlock(timeout)) {
        mutex_ = NULL;
      }
    }
  }
  ~Guard() {
    if (mutex_) {
      mutex_->unlock();
    }
  }

  /*
   * This is really operator bool. However, implementing it to return
   * bool is actually harmful. See
   * www.artima.com/cppsource/safebool.html for the details; in brief,
   * converting to bool allows a lot of nonsensical operations in
   * addition to simple testing. To avoid that, we return a pointer to
   * member which can only be used for testing.
   */
  typedef const Mutex*const Guard::*const pBoolMember;
  inline operator pBoolMember() const {
    return mutex_ != NULL ? &Guard::mutex_ : NULL;
  }

 private:
  const Mutex* mutex_;
};

// Can be used as second argument to RWGuard to make code more readable
// as to whether we're doing acquireRead() or acquireWrite().
enum RWGuardType {
  RW_READ = 0,
  RW_WRITE = 1,
};


class RWGuard : boost::noncopyable {
  public:
  explicit RWGuard(const ReadWriteMutex& value, bool write = false,
                   int64_t timeout=0)
         : rw_mutex_(value), locked_(true) {
      if (write) {
        if (timeout) {
          locked_ = rw_mutex_.timedWrite(timeout);
        } else {
          rw_mutex_.acquireWrite();
        }
      } else {
        if (timeout) {
          locked_ = rw_mutex_.timedRead(timeout);
        } else {
          rw_mutex_.acquireRead();
        }
      }
    }

    RWGuard(const ReadWriteMutex& value, RWGuardType type, int64_t timeout = 0)
         : rw_mutex_(value), locked_(true) {
      if (type == RW_WRITE) {
        if (timeout) {
          locked_ = rw_mutex_.timedWrite(timeout);
        } else {
          rw_mutex_.acquireWrite();
        }
      } else {
        if (timeout) {
          locked_ = rw_mutex_.timedRead(timeout);
        } else {
          rw_mutex_.acquireRead();
        }
      }
    }
    ~RWGuard() {
      if (locked_) {
        rw_mutex_.release();
      }
    }

  typedef const bool RWGuard::*const pBoolMember;
  operator pBoolMember() const {
    return locked_ ? &RWGuard::locked_ : NULL;
    }

    bool operator!() const {
      return !locked_;
    }

    bool release() {
      if (!locked_) return false;
      rw_mutex_.release();
      locked_ = false;
      return true;
    }

  private:
    const ReadWriteMutex& rw_mutex_;
    mutable bool locked_;
};


// A little hack to prevent someone from trying to do "Guard(m);"
// Such a use is invalid because the temporary Guard object is
// destroyed at the end of the line, releasing the lock.
// Sorry for polluting the global namespace, but I think it's worth it.
#define Guard(m) incorrect_use_of_Guard(m)
#define RWGuard(m) incorrect_use_of_RWGuard(m)


}}} // apache::thrift::concurrency

#endif // #ifndef THRIFT_CONCURRENCY_MUTEX_H_
