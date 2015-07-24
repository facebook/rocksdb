//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// See port_example.h for documentation for the following types/functions.

#ifndef STORAGE_LEVELDB_PORT_PORT_WIN_H_
#define STORAGE_LEVELDB_PORT_PORT_WIN_H_

// Always want minimum headers
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

// Assume that for everywhere
#undef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN true

#include <windows.h>
#include <string>
#include <string.h>
#include <mutex>
#include <condition_variable>

#include <stdint.h>

#include "rocksdb/options.h"

#undef min
#undef max
#undef DeleteFile
#undef GetCurrentTime


#ifndef strcasecmp
#define strcasecmp _stricmp
#endif

// defined in stdio.h
#ifndef snprintf
#define snprintf _snprintf
#endif

#undef GetCurrentTime
#undef DeleteFile

typedef SSIZE_T ssize_t;

// size_t printf formatting named in the manner of C99 standard formatting
// strings such as PRIu64
// in fact, we could use that one
#ifndef ROCKSDB_PRIszt
#define ROCKSDB_PRIszt "Iu"
#endif

#define __attribute__(A)

#ifdef ZLIB
#include <zlib.h>
#endif

#ifdef BZIP2
#include <bzlib.h>
#endif

#if defined(LZ4)
#include <lz4.h>
#include <lz4hc.h>
#endif

#ifdef SNAPPY
#include <snappy.h>
#endif

// Thread local storage on Linux
// There is thread_local in C++11
#ifndef __thread
#define __thread __declspec(thread)
#endif

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

namespace rocksdb {

#define PREFETCH(addr, rw, locality)

namespace port {

// For use at db/file_indexer.h kLevelMaxIndex
const int kMaxInt32 = INT32_MAX;
const uint64_t kMaxUint64 = UINT64_MAX;

const bool kLittleEndian = true;

class CondVar;

class Mutex {
 public:
  /* implicit */ Mutex(bool adaptive = false);
  ~Mutex();

  void Lock();
  void Unlock();

  // this will assert if the mutex is not locked
  // it does NOT verify that mutex is held by a calling thread
  void AssertHeld();

  std::unique_lock<std::mutex>& getLock() { return lock; }

 private:
  friend class CondVar;
  std::mutex m_mutex;
  std::unique_lock<std::mutex> lock;
#ifndef NDEBUG
  bool locked_;
#endif

  // No copying
  Mutex(const Mutex&);
  void operator=(const Mutex&);
};

class RWMutex {
 public:
  RWMutex() { InitializeSRWLock(&srwLock_); }

  void ReadLock() { AcquireSRWLockShared(&srwLock_); }

  void WriteLock() { AcquireSRWLockExclusive(&srwLock_); }

  void ReadUnlock() { ReleaseSRWLockShared(&srwLock_); }

  void WriteUnlock() { ReleaseSRWLockExclusive(&srwLock_); }

  // Empty as in POSIX
  void AssertHeld() {}

 private:
  SRWLOCK srwLock_;
  // No copying allowed
  RWMutex(const RWMutex&);
  void operator=(const RWMutex&);
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu);
  ~CondVar();
  void Wait();
  bool TimedWait(uint64_t expiration_time);
  void Signal();
  void SignalAll();

 private:
  std::condition_variable cv_;
  Mutex* mu_;
};

typedef std::once_flag OnceType;
#define LEVELDB_ONCE_INIT std::once_flag::once_flag();
extern void InitOnce(OnceType* once, void (*initializer)());

inline bool Snappy_Compress(const CompressionOptions& opts, const char* input,
                            size_t length, ::std::string* output) {
#ifdef SNAPPY
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
#endif
  return false;
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#ifdef SNAPPY
  return snappy::GetUncompressedLength(input, length, result);
#else
  return false;
#endif
}

inline bool Snappy_Uncompress(const char* input, size_t length, char* output) {
#ifdef SNAPPY
  return snappy::RawUncompress(input, length, output);
#else
  return false;
#endif
}

inline bool Zlib_Compress(const CompressionOptions& opts, const char* input,
                          size_t length, ::std::string* output) {
#ifdef ZLIB
  // The memLevel parameter specifies how much memory should be allocated for
  // the internal compression state.
  // memLevel=1 uses minimum memory but is slow and reduces compression ratio.
  // memLevel=9 uses maximum memory for optimal speed.
  // The default value is 8. See zconf.h for more details.
  static const int memLevel = 8;
  z_stream _stream;
  memset(&_stream, 0, sizeof(z_stream));
  int st = deflateInit2(&_stream, opts.level, Z_DEFLATED, opts.window_bits,
                        memLevel, opts.strategy);
  if (st != Z_OK) {
    return false;
  }

  // Resize output to be the plain data length.
  // This may not be big enough if the compression actually expands data.
  output->resize(length);

  // Compress the input, and put compressed data in output.
  _stream.next_in = (Bytef*)input;
  _stream.avail_in = length;

  // Initialize the output size.
  _stream.avail_out = length;
  _stream.next_out = (Bytef*)&(*output)[0];

  int old_sz = 0, new_sz = 0, new_sz_delta = 0;
  bool done = false;
  while (!done) {
    int st = deflate(&_stream, Z_FINISH);
    switch (st) {
      case Z_STREAM_END:
        done = true;
        break;
      case Z_OK:
        // No output space. Increase the output space by 20%.
        // (Should we fail the compression since it expands the size?)
        old_sz = output->size();
        new_sz_delta = (int)(output->size() * 0.2);
        new_sz = output->size() + (new_sz_delta < 10 ? 10 : new_sz_delta);
        output->resize(new_sz);
        // Set more output.
        _stream.next_out = (Bytef*)&(*output)[old_sz];
        _stream.avail_out = new_sz - old_sz;
        break;
      case Z_BUF_ERROR:
      default:
        deflateEnd(&_stream);
        return false;
    }
  }

  output->resize(output->size() - _stream.avail_out);
  deflateEnd(&_stream);
  return true;
#endif
  return false;
}

inline char* Zlib_Uncompress(const char* input_data, size_t input_length,
                             int* decompress_size, int windowBits = -14) {
#ifdef ZLIB
  z_stream _stream;
  memset(&_stream, 0, sizeof(z_stream));

  // For raw inflate, the windowBits should be -8..-15.
  // If windowBits is bigger than zero, it will use either zlib
  // header or gzip header. Adding 32 to it will do automatic detection.
  int st =
      inflateInit2(&_stream, windowBits > 0 ? windowBits + 32 : windowBits);
  if (st != Z_OK) {
    return nullptr;
  }

  _stream.next_in = (Bytef*)input_data;
  _stream.avail_in = input_length;

  // Assume the decompressed data size will 5x of compressed size.
  int output_len = input_length * 5;
  char* output = new char[output_len];
  int old_sz = output_len;

  _stream.next_out = (Bytef*)output;
  _stream.avail_out = output_len;

  char* tmp = nullptr;
  int output_len_delta;
  bool done = false;

  // while(_stream.next_in != nullptr && _stream.avail_in != 0) {
  while (!done) {
    int st = inflate(&_stream, Z_SYNC_FLUSH);
    switch (st) {
      case Z_STREAM_END:
        done = true;
        break;
      case Z_OK:
        // No output space. Increase the output space by 20%.
        old_sz = output_len;
        output_len_delta = (int)(output_len * 0.2);
        output_len += output_len_delta < 10 ? 10 : output_len_delta;
        tmp = new char[output_len];
        memcpy(tmp, output, old_sz);
        delete[] output;
        output = tmp;

        // Set more output.
        _stream.next_out = (Bytef*)(output + old_sz);
        _stream.avail_out = output_len - old_sz;
        break;
      case Z_BUF_ERROR:
      default:
        delete[] output;
        inflateEnd(&_stream);
        return nullptr;
    }
  }

  *decompress_size = output_len - _stream.avail_out;
  inflateEnd(&_stream);
  return output;
#endif

  return nullptr;
}

inline bool BZip2_Compress(const CompressionOptions& opts, const char* input,
                           size_t length, ::std::string* output) {
#ifdef BZIP2
  bz_stream _stream;
  memset(&_stream, 0, sizeof(bz_stream));

  // Block size 1 is 100K.
  // 0 is for silent.
  // 30 is the default workFactor
  int st = BZ2_bzCompressInit(&_stream, 1, 0, 30);
  if (st != BZ_OK) {
    return false;
  }

  // Resize output to be the plain data length.
  // This may not be big enough if the compression actually expands data.
  output->resize(length);

  // Compress the input, and put compressed data in output.
  _stream.next_in = (char*)input;
  _stream.avail_in = length;

  // Initialize the output size.
  _stream.next_out = (char*)&(*output)[0];
  _stream.avail_out = length;

  int old_sz = 0, new_sz = 0;
  while (_stream.next_in != nullptr && _stream.avail_in != 0) {
    int st = BZ2_bzCompress(&_stream, BZ_FINISH);
    switch (st) {
      case BZ_STREAM_END:
        break;
      case BZ_FINISH_OK:
        // No output space. Increase the output space by 20%.
        // (Should we fail the compression since it expands the size?)
        old_sz = output->size();
        new_sz = (int)(output->size() * 1.2);
        output->resize(new_sz);
        // Set more output.
        _stream.next_out = (char*)&(*output)[old_sz];
        _stream.avail_out = new_sz - old_sz;
        break;
      case BZ_SEQUENCE_ERROR:
      default:
        BZ2_bzCompressEnd(&_stream);
        return false;
    }
  }

  output->resize(output->size() - _stream.avail_out);
  BZ2_bzCompressEnd(&_stream);
  return true;
#endif
  return false;
}

inline char* BZip2_Uncompress(const char* input_data, size_t input_length,
                              int* decompress_size) {
#ifdef BZIP2
  bz_stream _stream;
  memset(&_stream, 0, sizeof(bz_stream));

  int st = BZ2_bzDecompressInit(&_stream, 0, 0);
  if (st != BZ_OK) {
    return nullptr;
  }

  _stream.next_in = (char*)input_data;
  _stream.avail_in = input_length;

  // Assume the decompressed data size will be 5x of compressed size.
  int output_len = input_length * 5;
  char* output = new char[output_len];
  int old_sz = output_len;

  _stream.next_out = (char*)output;
  _stream.avail_out = output_len;

  char* tmp = nullptr;

  while (_stream.next_in != nullptr && _stream.avail_in != 0) {
    int st = BZ2_bzDecompress(&_stream);
    switch (st) {
      case BZ_STREAM_END:
        break;
      case BZ_OK:
        // No output space. Increase the output space by 20%.
        old_sz = output_len;
        output_len = (int)(output_len * 1.2);
        tmp = new char[output_len];
        memcpy(tmp, output, old_sz);
        delete[] output;
        output = tmp;

        // Set more output.
        _stream.next_out = (char*)(output + old_sz);
        _stream.avail_out = output_len - old_sz;
        break;
      default:
        delete[] output;
        BZ2_bzDecompressEnd(&_stream);
        return nullptr;
    }
  }

  *decompress_size = output_len - _stream.avail_out;
  BZ2_bzDecompressEnd(&_stream);
  return output;
#endif
  return nullptr;
}

inline bool LZ4_Compress(const CompressionOptions& opts, const char* input,
                         size_t length, ::std::string* output) {
#ifdef LZ4
  int compressBound = LZ4_compressBound(length);
  output->resize(8 + compressBound);
  char* p = const_cast<char*>(output->c_str());
  memcpy(p, &length, sizeof(length));
  size_t outlen;
  outlen = LZ4_compress_limitedOutput(input, p + 8, length, compressBound);
  if (outlen == 0) {
    return false;
  }
  output->resize(8 + outlen);
  return true;
#endif
  return false;
}

inline char* LZ4_Uncompress(const char* input_data, size_t input_length,
                            int* decompress_size) {
#ifdef LZ4
  if (input_length < 8) {
    return nullptr;
  }
  int output_len;
  memcpy(&output_len, input_data, sizeof(output_len));
  char* output = new char[output_len];
  *decompress_size = LZ4_decompress_safe_partial(
      input_data + 8, output, input_length - 8, output_len, output_len);
  if (*decompress_size < 0) {
    delete[] output;
    return nullptr;
  }
  return output;
#endif
  return nullptr;
}

inline bool LZ4HC_Compress(const CompressionOptions& opts, const char* input,
                           size_t length, ::std::string* output) {
#ifdef LZ4
  int compressBound = LZ4_compressBound(length);
  output->resize(8 + compressBound);
  char* p = const_cast<char*>(output->c_str());
  memcpy(p, &length, sizeof(length));
  size_t outlen;
#ifdef LZ4_VERSION_MAJOR  // they only started defining this since r113
  outlen = LZ4_compressHC2_limitedOutput(input, p + 8, length, compressBound,
                                         opts.level);
#else
  outlen = LZ4_compressHC_limitedOutput(input, p + 8, length, compressBound);
#endif
  if (outlen == 0) {
    return false;
  }
  output->resize(8 + outlen);
  return true;
#endif
  return false;
}

#define CACHE_LINE_SIZE 64U

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

// For Thread Local Storage abstraction
typedef DWORD pthread_key_t;

inline int pthread_key_create(pthread_key_t* key, void (*destructor)(void*)) {
  // Not used
  (void)destructor;

  pthread_key_t k = TlsAlloc();
  if (TLS_OUT_OF_INDEXES == k) {
    return ENOMEM;
  }

  *key = k;
  return 0;
}

inline int pthread_key_delete(pthread_key_t key) {
  if (!TlsFree(key)) {
    return EINVAL;
  }
  return 0;
}

inline int pthread_setspecific(pthread_key_t key, const void* value) {
  if (!TlsSetValue(key, const_cast<void*>(value))) {
    return ENOMEM;
  }
  return 0;
}

inline void* pthread_getspecific(pthread_key_t key) {
  void* result = TlsGetValue(key);
  if (!result) {
    if (GetLastError() != ERROR_SUCCESS) {
      errno = EINVAL;
    } else {
      errno = NOERROR;
    }
  }
  return result;
}

// UNIX equiv although errno numbers will be off
// using C-runtime to implement. Note, this does not
// feel space with zeros in case the file is extended.
int truncate(const char* path, int64_t length);
void Crash(const std::string& srcfile, int srcline);

}  // namespace port

using port::pthread_key_t;
using port::pthread_key_create;
using port::pthread_key_delete;
using port::pthread_setspecific;
using port::pthread_getspecific;
using port::truncate;

}  // namespace rocksdb

#endif  // STORAGE_LEVELDB_PORT_PORT_POSIX_H_
