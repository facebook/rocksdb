//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#if defined(OS_WIN)

#include "port/win/xpress_win.h"

#include <windows.h>

#include <cassert>
#include <iostream>
#include <limits>
#include <memory>

#ifdef XPRESS

// Put this under ifdef so windows systems w/o this
// can still build
#include <compressapi.h>

namespace ROCKSDB_NAMESPACE {
namespace port {
namespace xpress {

// Helpers
namespace {

auto CloseCompressorFun = [](void* h) {
  if (NULL != h) {
    ::CloseCompressor(reinterpret_cast<COMPRESSOR_HANDLE>(h));
  }
};

auto CloseDecompressorFun = [](void* h) {
  if (NULL != h) {
    ::CloseDecompressor(reinterpret_cast<DECOMPRESSOR_HANDLE>(h));
  }
};
}  // namespace

bool Compress(const char* input, size_t length, std::string* output) {
  assert(input != nullptr);
  assert(output != nullptr);

  if (length == 0) {
    output->clear();
    return true;
  }

  COMPRESS_ALLOCATION_ROUTINES* allocRoutinesPtr = nullptr;

  COMPRESSOR_HANDLE compressor = NULL;

  BOOL success =
      CreateCompressor(COMPRESS_ALGORITHM_XPRESS,  //  Compression Algorithm
                       allocRoutinesPtr,  //  Optional allocation routine
                       &compressor);      //  Handle

  if (!success) {
#ifdef _DEBUG
    std::cerr << "XPRESS: Failed to create Compressor LastError: "
              << GetLastError() << std::endl;
#endif
    return false;
  }

  std::unique_ptr<void, decltype(CloseCompressorFun)> compressorGuard(
      compressor, CloseCompressorFun);

  SIZE_T compressedBufferSize = 0;

  //  Query compressed buffer size.
  success = ::Compress(compressor,                //  Compressor Handle
                       const_cast<char*>(input),  //  Input buffer
                       length,                    //  Uncompressed data size
                       NULL,                      //  Compressed Buffer
                       0,                         //  Compressed Buffer size
                       &compressedBufferSize);    //  Compressed Data size

  if (!success) {
    auto lastError = GetLastError();

    if (lastError != ERROR_INSUFFICIENT_BUFFER) {
#ifdef _DEBUG
      std::cerr
          << "XPRESS: Failed to estimate compressed buffer size LastError "
          << lastError << std::endl;
#endif
      return false;
    }
  }

  assert(compressedBufferSize > 0);

  std::string result;
  result.resize(compressedBufferSize);

  SIZE_T compressedDataSize = 0;

  //  Compress
  success = ::Compress(compressor,                //  Compressor Handle
                       const_cast<char*>(input),  //  Input buffer
                       length,                    //  Uncompressed data size
                       &result[0],                //  Compressed Buffer
                       compressedBufferSize,      //  Compressed Buffer size
                       &compressedDataSize);      //  Compressed Data size

  if (!success) {
#ifdef _DEBUG
    std::cerr << "XPRESS: Failed to compress LastError " << GetLastError()
              << std::endl;
#endif
    return false;
  }

  result.resize(compressedDataSize);
  output->swap(result);

  return true;
}

char* Decompress(const char* input_data, size_t input_length,
                 size_t* uncompressed_size) {
  assert(input_data != nullptr);
  assert(uncompressed_size != nullptr);

  if (input_length == 0) {
    return nullptr;
  }

  COMPRESS_ALLOCATION_ROUTINES* allocRoutinesPtr = nullptr;

  DECOMPRESSOR_HANDLE decompressor = NULL;

  BOOL success =
      CreateDecompressor(COMPRESS_ALGORITHM_XPRESS,  //  Compression Algorithm
                         allocRoutinesPtr,  //  Optional allocation routine
                         &decompressor);    //  Handle

  if (!success) {
#ifdef _DEBUG
    std::cerr << "XPRESS: Failed to create Decompressor LastError "
              << GetLastError() << std::endl;
#endif
    return nullptr;
  }

  std::unique_ptr<void, decltype(CloseDecompressorFun)> decompressorGuard(
      decompressor, CloseDecompressorFun);

  SIZE_T decompressedBufferSize = 0;

  success = ::Decompress(decompressor,                   //  Compressor Handle
                         const_cast<char*>(input_data),  //  Compressed data
                         input_length,              //  Compressed data size
                         NULL,                      //  Buffer set to NULL
                         0,                         //  Buffer size set to 0
                         &decompressedBufferSize);  //  Decompressed Data size

  if (!success) {
    auto lastError = GetLastError();

    if (lastError != ERROR_INSUFFICIENT_BUFFER) {
#ifdef _DEBUG
      std::cerr
          << "XPRESS: Failed to estimate decompressed buffer size LastError "
          << lastError << std::endl;
#endif
      return nullptr;
    }
  }

  assert(decompressedBufferSize > 0);

  // The callers are deallocating using delete[]
  // thus we must allocate with new[]
  std::unique_ptr<char[]> outputBuffer(new char[decompressedBufferSize]);

  SIZE_T decompressedDataSize = 0;

  success = ::Decompress(decompressor, const_cast<char*>(input_data),
                         input_length, outputBuffer.get(),
                         decompressedBufferSize, &decompressedDataSize);

  if (!success) {
#ifdef _DEBUG
    std::cerr << "XPRESS: Failed to decompress LastError " << GetLastError()
              << std::endl;
#endif
    return nullptr;
  }

  *uncompressed_size = decompressedDataSize;

  // Return the raw buffer to the caller supporting the tradition
  return outputBuffer.release();
}

int64_t DecompressToBuffer(const char* input, size_t input_length, char* output,
                           size_t output_length) {
  assert(input != nullptr);
  assert(output != nullptr);

  DECOMPRESSOR_HANDLE decompressor = NULL;

  BOOL success =
      CreateDecompressor(COMPRESS_ALGORITHM_XPRESS,  //  Compression Algorithm
                         allocRoutinesPtr,  //  Optional allocation routine
                         &decompressor);    //  Handle

  if (!success) {
#ifdef _DEBUG
    std::cerr << "XPRESS: Failed to create Decompressor LastError "
              << GetLastError() << std::endl;
#endif
    return -1;
  }

  std::unique_ptr<void, decltype(CloseDecompressorFun)> decompressorGuard(
      decompressor, CloseDecompressorFun);

  SIZE_T decompressedDataSize = 0;

  success = ::Decompress(decompressor, const_cast<char*>(input), input_length,
                         output, output_length, &decompressedDataSize);

  if (!success) {
#ifdef _DEBUG
    std::cerr << "XPRESS: Failed to decompress LastError " << GetLastError()
              << std::endl;
#endif
    return -1;
  }

  return static_cast<int64_t>(decompressedDataSize);
}

}  // namespace xpress
}  // namespace port
}  // namespace ROCKSDB_NAMESPACE

#endif

#endif
