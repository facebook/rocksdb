// Copyright (c) 2022-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

StreamingCompress* StreamingCompress::Create(CompressionType compression_type,
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

StreamingUncompress* StreamingUncompress::Create(
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

int ZSTDStreamingCompress::Compress(const char* input, size_t input_size,
                                    char* output, size_t* output_pos) {
  assert(input != nullptr && output != nullptr && output_pos != nullptr);
  *output_pos = 0;
  // Don't need to compress an empty input
  if (input_size == 0) {
    return 0;
  }
#ifndef ZSTD_STREAMING
  (void)input;
  (void)input_size;
  (void)output;
  return -1;
#else
  if (input_buffer_.src == nullptr || input_buffer_.src != input) {
    // New input
    // Catch errors where the previous input was not fully decompressed.
    assert(input_buffer_.pos == input_buffer_.size);
    input_buffer_ = {input, input_size, /*pos=*/0};
  } else if (input_buffer_.src == input) {
    // Same input, not fully compressed.
  }
  ZSTD_outBuffer output_buffer = {output, max_output_len_, /*pos=*/0};
  const size_t remaining =
      ZSTD_compressStream2(cctx_, &output_buffer, &input_buffer_, ZSTD_e_end);
  if (ZSTD_isError(remaining)) {
    // Failure
    Reset();
    return -1;
  }
  // Success
  *output_pos = output_buffer.pos;
  return (int)remaining;
#endif
}

void ZSTDStreamingCompress::Reset() {
#ifdef ZSTD_STREAMING
  ZSTD_CCtx_reset(cctx_, ZSTD_ResetDirective::ZSTD_reset_session_only);
  input_buffer_ = {/*src=*/nullptr, /*size=*/0, /*pos=*/0};
#endif
}

int ZSTDStreamingUncompress::Uncompress(const char* input, size_t input_size,
                                        char* output, size_t* output_pos) {
  assert(output != nullptr && output_pos != nullptr);
  *output_pos = 0;
  // Don't need to uncompress an empty input
  if (input_size == 0) {
    return 0;
  }
#ifdef ZSTD_STREAMING
  if (input) {
    // New input
    input_buffer_ = {input, input_size, /*pos=*/0};
  }
  ZSTD_outBuffer output_buffer = {output, max_output_len_, /*pos=*/0};
  size_t ret = ZSTD_decompressStream(dctx_, &output_buffer, &input_buffer_);
  if (ZSTD_isError(ret)) {
    Reset();
    return -1;
  }
  *output_pos = output_buffer.pos;
  return (int)(input_buffer_.size - input_buffer_.pos);
#else
  (void)input;
  (void)input_size;
  (void)output;
  return -1;
#endif
}

void ZSTDStreamingUncompress::Reset() {
#ifdef ZSTD_STREAMING
  ZSTD_DCtx_reset(dctx_, ZSTD_ResetDirective::ZSTD_reset_session_only);
  input_buffer_ = {/*src=*/nullptr, /*size=*/0, /*pos=*/0};
#endif
}

}  // namespace ROCKSDB_NAMESPACE
