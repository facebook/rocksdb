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

#ifndef STORAGE_LEVELDB_PORT_PORT_POSIX_H_
#define STORAGE_LEVELDB_PORT_PORT_POSIX_H_

#undef PLATFORM_IS_LITTLE_ENDIAN
#if defined(OS_MACOSX)
  #include <machine/endian.h>
  #if defined(__DARWIN_LITTLE_ENDIAN) && defined(__DARWIN_BYTE_ORDER)
    #define PLATFORM_IS_LITTLE_ENDIAN \
        (__DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN)
  #endif
#elif defined(OS_SOLARIS)
  #include <sys/isa_defs.h>
  #ifdef _LITTLE_ENDIAN
    #define PLATFORM_IS_LITTLE_ENDIAN true
  #else
    #define PLATFORM_IS_LITTLE_ENDIAN false
  #endif
#elif defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) ||\
      defined(OS_DRAGONFLYBSD) || defined(OS_ANDROID)
  #include <sys/types.h>
  #include <sys/endian.h>
#else
  #include <endian.h>
#endif
#include <pthread.h>
#ifdef SNAPPY
#include <snappy.h>
#endif

#ifdef ZLIB
#include <zlib.h>
#endif

#ifdef BZIP2
#include <bzlib.h>
#endif

#include <stdint.h>
#include <string>
#include <string.h>
#include "rocksdb/options.h"
#include "port/atomic_pointer.h"

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

#if defined(OS_MACOSX) || defined(OS_SOLARIS) || defined(OS_FREEBSD) ||\
    defined(OS_NETBSD) || defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD) ||\
    defined(OS_ANDROID)
// Use fread/fwrite/fflush on platforms without _unlocked variants
#define fread_unlocked fread
#define fwrite_unlocked fwrite
#define fflush_unlocked fflush
#endif

#if defined(OS_MACOSX) || defined(OS_FREEBSD) ||\
    defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD)
// Use fsync() on platforms without fdatasync()
#define fdatasync fsync
#endif

#if defined(OS_ANDROID) && __ANDROID_API__ < 9
// fdatasync() was only introduced in API level 9 on Android. Use fsync()
// when targetting older platforms.
#define fdatasync fsync
#endif

namespace rocksdb {
namespace port {

static const bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;
#undef PLATFORM_IS_LITTLE_ENDIAN

class CondVar;

class Mutex {
 public:
  /* implicit */ Mutex(bool adaptive = false);
  ~Mutex();

  void Lock();
  void Unlock();
  void AssertHeld() { }

 private:
  friend class CondVar;
  pthread_mutex_t mu_;

  // No copying
  Mutex(const Mutex&);
  void operator=(const Mutex&);
};

class RWMutex {
 public:
  RWMutex();
  ~RWMutex();

  void ReadLock();
  void WriteLock();
  void Unlock();
  void AssertHeld() { }

 private:
  pthread_rwlock_t mu_; // the underlying platform mutex

  // No copying allowed
  RWMutex(const RWMutex&);
  void operator=(const RWMutex&);
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu);
  ~CondVar();
  void Wait();
  void Signal();
  void SignalAll();
 private:
  pthread_cond_t cv_;
  Mutex* mu_;
};

typedef pthread_once_t OnceType;
#define LEVELDB_ONCE_INIT PTHREAD_ONCE_INIT
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

inline bool Snappy_Uncompress(const char* input, size_t length,
                              char* output) {
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
  _stream.next_in = (Bytef *)input;
  _stream.avail_in = length;

  // Initialize the output size.
  _stream.avail_out = length;
  _stream.next_out = (Bytef *)&(*output)[0];

  int old_sz =0, new_sz =0, new_sz_delta =0;
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
        _stream.next_out = (Bytef *)&(*output)[old_sz];
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
  int st = inflateInit2(&_stream,
      windowBits > 0 ? windowBits + 32 : windowBits);
  if (st != Z_OK) {
    return nullptr;
  }

  _stream.next_in = (Bytef *)input_data;
  _stream.avail_in = input_length;

  // Assume the decompressed data size will 5x of compressed size.
  int output_len = input_length * 5;
  char* output = new char[output_len];
  int old_sz = output_len;

  _stream.next_out = (Bytef *)output;
  _stream.avail_out = output_len;

  char* tmp = nullptr;
  int output_len_delta;
  bool done = false;

  //while(_stream.next_in != nullptr && _stream.avail_in != 0) {
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
        _stream.next_out = (Bytef *)(output + old_sz);
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
  _stream.next_in = (char *)input;
  _stream.avail_in = length;

  // Initialize the output size.
  _stream.next_out = (char *)&(*output)[0];
  _stream.avail_out = length;

  int old_sz =0, new_sz =0;
  while(_stream.next_in != nullptr && _stream.avail_in != 0) {
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
        _stream.next_out = (char *)&(*output)[old_sz];
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
  return output;
#endif
  return false;
}

inline char*  BZip2_Uncompress(const char* input_data, size_t input_length,
    int* decompress_size) {
#ifdef BZIP2
  bz_stream _stream;
  memset(&_stream, 0, sizeof(bz_stream));

  int st = BZ2_bzDecompressInit(&_stream, 0, 0);
  if (st != BZ_OK) {
    return nullptr;
  }

  _stream.next_in = (char *)input_data;
  _stream.avail_in = input_length;

  // Assume the decompressed data size will be 5x of compressed size.
  int output_len = input_length * 5;
  char* output = new char[output_len];
  int old_sz = output_len;

  _stream.next_out = (char *)output;
  _stream.avail_out = output_len;

  char* tmp = nullptr;

  while(_stream.next_in != nullptr && _stream.avail_in != 0) {
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
        _stream.next_out = (char *)(output + old_sz);
        _stream.avail_out = output_len - old_sz;
        break;
      case Z_BUF_ERROR:
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

inline bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg) {
  return false;
}

} // namespace port
} // namespace rocksdb

#endif  // STORAGE_LEVELDB_PORT_PORT_POSIX_H_
