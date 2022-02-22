// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

StreamingCompress* StreamingCompress::create(CompressionType compression_type,
                                             const CompressionOptions& opts,
                                             uint32_t compress_format_version,
                                             size_t max_output_len) {
  switch (compression_type) {
    case kZSTD: {
      if (!ZSTD_Streaming_Supported()) {
        return nullptr;
      }
      return new ZSTDStreamingCompress(opts, compress_format_version,
                                       max_output_len);
    }
    default:
      return nullptr;
  }
}

StreamingUncompress* StreamingUncompress::create(
    CompressionType compression_type, uint32_t compress_format_version,
    size_t max_output_len) {
  switch (compression_type) {
    case kZSTD: {
      if (!ZSTD_Streaming_Supported()) {
        return nullptr;
      }
      return new ZSTDStreamingUncompress(compress_format_version,
                                         max_output_len);
    }
    default:
      return nullptr;
  }
}

int ZSTDStreamingCompress::compress(const char* input, size_t input_size,
                                    char* output, size_t* output_size) {
  assert(input != nullptr && output != nullptr && input_size > 0 &&
         output_size != nullptr);
  *output_size = 0;
#ifndef ZSTD_STREAMING
  return -1;
#else
  if (input_buffer_.src == nullptr || input_buffer_.src != input) {
    // New input
    // Catch errors where the previous input was not fully decompressed.
    assert(input_buffer_.pos == input_buffer_.size);
    input_buffer_ = {input, input_size, 0};
  } else if (input_buffer_.src == input) {
    // Same input, not fully compressed.
  }
  ZSTD_outBuffer output_buffer = {output, max_output_len_, 0};
  const size_t remaining =
      ZSTD_compressStream2(cctx_, &output_buffer, &input_buffer_, ZSTD_e_flush);
  if (ZSTD_isError(remaining)) {
    // Failure
    reset();
    return -1;
  }
  // Success
  *output_size = output_buffer.pos;
  return (int)(input_buffer_.size - input_buffer_.pos);
#endif
}

void ZSTDStreamingCompress::reset() {
#ifdef ZSTD_STREAMING
  ZSTD_CCtx_reset(cctx_, ZSTD_ResetDirective::ZSTD_reset_session_only);
  input_buffer_ = {nullptr, 0, 0};
#endif
}

int ZSTDStreamingUncompress::uncompress(const char* input, size_t input_size,
                                        char* output, size_t* output_size) {
  assert(input != nullptr && output != nullptr && input_size > 0 &&
         output_size != nullptr);
  *output_size = 0;
#ifdef ZSTD_STREAMING
  if (input_buffer_.src != input) {
    // New input
    input_buffer_ = {input, input_size, 0};
  }
  ZSTD_outBuffer output_buffer = {output, max_output_len_, 0};
  size_t ret = ZSTD_decompressStream(dctx_, &output_buffer, &input_buffer_);
  if (ZSTD_isError(ret)) {
    reset();
    return -1;
  }
  *output_size = output_buffer.pos;
  return input_buffer_.size - input_buffer_.pos;
#else
  return -1;
#endif
}

void ZSTDStreamingUncompress::reset() {
#ifdef ZSTD_STREAMING
  ZSTD_DCtx_reset(dctx_, ZSTD_ResetDirective::ZSTD_reset_session_only);
  input_buffer_ = {nullptr, 0, 0};
#endif
}

}  // namespace ROCKSDB_NAMESPACE
