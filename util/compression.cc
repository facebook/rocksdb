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
#ifndef ZSTD
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
#ifdef ZSTD
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
#ifdef ZSTD
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
#ifdef ZSTD
  ZSTD_DCtx_reset(dctx_, ZSTD_ResetDirective::ZSTD_reset_session_only);
  input_buffer_ = {/*src=*/nullptr, /*size=*/0, /*pos=*/0};
#endif
}

// ***********************************************************************
// BEGIN built-in implementation of customization interface
// ***********************************************************************
namespace {

class BuiltinCompressorV1 : public Compressor {
 public:
  explicit BuiltinCompressorV1(const CompressionOptions& opts,
                               CompressionType type)
      : opts_(opts), type_(type) {
    assert(type != kNoCompression);
  }

  Status CompressBlock(Slice uncompressed_data, std::string* compressed_output,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override {
    std::optional<CompressionContext> tmp_ctx;
    CompressionContext* ctx = nullptr;
    if (wa != nullptr && wa->owner() == this) {
      ctx = static_cast<CompressionContext*>(wa->get());
    }
    if (ctx == nullptr) {
      tmp_ctx.emplace(type_, opts_);
      ctx = &*tmp_ctx;
    }
    CompressionInfo info(opts_, *ctx, CompressionDict::GetEmptyDict(), type_);
    if (!CompressData(uncompressed_data, info, 1 /*compress_format_version*/,
                      compressed_output)) {
      *out_compression_type = kNoCompression;
      return Status::OK();
    }
    // TODO: verify compression, ratio, etc.
    *out_compression_type = type_;
    return Status::OK();
  }

 protected:
  const CompressionOptions opts_;
  const CompressionType type_;
};

class BuiltinCompressorV2 : public Compressor {
 public:
  explicit BuiltinCompressorV2(const CompressionOptions& opts,
                               CompressionType type,
                               CompressionDict&& dict = {})
      : opts_(opts), type_(type), dict_(std::move(dict)) {
    assert(type != kNoCompression);
  }

  size_t GetMaxSampleSizeIfWantDict(
      CacheEntryRole /*block_type*/) const override {
    if (opts_.max_dict_bytes == 0) {
      // Dictionary compression disabled
      return 0;
    }
    return opts_.zstd_max_train_bytes > 0 ? opts_.zstd_max_train_bytes
                                          : opts_.max_dict_bytes;
  }

  // NOTE: empty dict is equivalent to no dict
  Slice GetSerializedDict() const override { return dict_.GetRawDict(); }

  std::unique_ptr<Compressor> MaybeCloneSpecialized(
      CacheEntryRole /*block_type*/, DictSampleArgs&& dict_samples) override {
    assert(dict_samples.Verify());
    if (dict_samples.empty()) {
      // Nothing to specialize on
      return nullptr;
    }
    std::string dict_data;
    if (type_ == kZSTD && opts_.zstd_max_train_bytes > 0) {
      assert(dict_samples.sample_data.size() <= opts_.zstd_max_train_bytes);
      if (opts_.use_zstd_dict_trainer) {
        dict_data = ZSTD_TrainDictionary(dict_samples.sample_data,
                                         dict_samples.sample_lens,
                                         opts_.max_dict_bytes);
      } else {
        dict_data = ZSTD_FinalizeDictionary(dict_samples.sample_data,
                                            dict_samples.sample_lens,
                                            opts_.max_dict_bytes, opts_.level);
      }
    } else {
      assert(dict_samples.sample_data.size() <= opts_.max_dict_bytes);
      // ZSTD "raw content dictionary" - "Any buffer is a valid raw content
      // dictionary." Or similar for other compressions.
      dict_data = std::move(dict_samples.sample_data);
    }
    CompressionDict dict{std::move(dict_data), type_, opts_.level};
    return std::make_unique<BuiltinCompressorV2>(opts_, type_, std::move(dict));
  }

  // TODO: use ZSTD_CCtx directly
  WorkingArea* GetWorkingAreaImpl() override {
    return static_cast<WorkingArea*>(new CompressionContext(type_, opts_));
  }
  void ReleaseWorkingArea(WorkingArea* wa) override {
    delete static_cast<CompressionContext*>(wa);
  }

  Status CompressBlock(Slice uncompressed_data, std::string* compressed_output,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override {
    std::optional<CompressionContext> tmp_ctx;
    CompressionContext* ctx = nullptr;
    if (wa != nullptr && wa->owner() == this) {
      ctx = static_cast<CompressionContext*>(wa->get());
    }
    if (ctx == nullptr) {
      tmp_ctx.emplace(type_, opts_);
      ctx = &*tmp_ctx;
    }
    CompressionInfo info(opts_, *ctx, dict_, type_);
    if (!CompressData(uncompressed_data, info, 2 /*compress_format_version*/,
                      compressed_output)) {
      *out_compression_type = kNoCompression;
      return Status::OK();
    }
    // TODO: verify compression, ratio, etc.
    *out_compression_type = type_;
    return Status::OK();
  }

 protected:
  const CompressionOptions opts_;
  const CompressionType type_;
  const CompressionDict dict_;
};

// NOTE: this implementation is intentionally SIMPLE based on existing code
// and NOT EFFICIENT because this is an old/deprecated format.
class BuiltinDecompressorV1 : public Decompressor {
 public:
  Status ExtractUncompressedSize(Args& args) override {
    CacheAllocationPtr throw_away_output;
    return DoUncompress(args, &throw_away_output, &args.uncompressed_size);
  }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    uint64_t same_uncompressed_size = 0;
    CacheAllocationPtr output;
    Status s = DoUncompress(args, &output, &same_uncompressed_size);
    if (same_uncompressed_size != args.uncompressed_size) {
      s = Status::Corruption("Compressed block size mismatch");
    }
    if (s.ok()) {
      // NOTE: simple but inefficient
      memcpy(uncompressed_output, output.get(), args.uncompressed_size);
    }
    return s;
  }

 protected:
  Status DoUncompress(const Args& args, CacheAllocationPtr* out_data,
                      uint64_t* out_uncompressed_size) {
    assert(args.dict == nullptr);
    assert(args.working_area == nullptr);
    assert(args.uncompressed_size == 0);

    // NOTE: simple but inefficient
    UncompressionContext dummy_ctx{kNoCompression};
    UncompressionInfo info{dummy_ctx, UncompressionDict::GetEmptyDict(),
                           args.compression_type};
    const char* error_message = nullptr;
    *out_data = UncompressData(
        info, args.compressed_data.data(), args.compressed_data.size(),
        out_uncompressed_size, 1 /*compress_format_version*/,
        nullptr /*allocator*/, &error_message);
    if (*out_data == nullptr) {
      if (error_message != nullptr) {
        return Status::Corruption(error_message);
      } else {
        return Status::Corruption("Corrupted compressed block contents");
      }
    }
    assert(*out_uncompressed_size > 0);
    return Status::OK();
  }
};

// Subroutines for BuiltinDecompressorV2

Status Snappy_DecompressBlock(const Decompressor::Args& args,
                              char* uncompressed_output) {
#ifdef SNAPPY
  if (!snappy::RawUncompress(args.compressed_data.data(),
                             args.compressed_data.size(),
                             uncompressed_output)) {
    return Status::Corruption("Error decompressing snappy data");
  }
  return Status::OK();
#else
  (void)args;
  (void)uncompressed_output;
  return Status::NotSupported("Snappy not supported in this build");
#endif
}

Status Zlib_DecompressBlock(const Decompressor::Args& args,
                            char* uncompressed_output) {
#ifdef ZLIB
  uLongf uncompressed_size = args.uncompressed_size;
  if (Z_OK !=
      uncompress(reinterpret_cast<Bytef*>(uncompressed_output),
                 &uncompressed_size,
                 reinterpret_cast<const Bytef*>(args.compressed_data.data()),
                 args.compressed_data.size())) {
    return Status::Corruption("Error decompressing zlib data");
  }
  if (uncompressed_size != args.uncompressed_size) {
    return Status::Corruption("Size mismatch decompressing zlib data");
  }
  return Status::OK();
#else
  (void)args;
  (void)uncompressed_output;
  return Status::NotSupported("Zlib not supported in this build");
#endif
}

Status BZip2_DecompressBlock(const Decompressor::Args& args,
                             char* uncompressed_output) {
#ifdef BZIP2
  auto uncompressed_size = static_cast<unsigned int>(args.uncompressed_size);
  if (BZ_OK != BZ2_bzBuffToBuffDecompress(
                   uncompressed_output, &uncompressed_size,
                   const_cast<char*>(args.compressed_data.data()),
                   static_cast<unsigned int>(args.compressed_data.size()),
                   0 /*small mem*/, 0 /*verbosity*/)) {
    return Status::Corruption("Error decompressing bzip2 data");
  }
  if (uncompressed_size != args.uncompressed_size) {
    return Status::Corruption("Size mismatch decompressing bzip2 data");
  }
  return Status::OK();
#else
  (void)args;
  (void)uncompressed_output;
  return Status::NotSupported("BZip2 not supported in this build");
#endif
}

Status LZ4_DecompressBlock(const Decompressor::Args& args,
                           char* uncompressed_output) {
#ifdef LZ4
  int expected_uncompressed_size = static_cast<int>(args.uncompressed_size);
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  LZ4_streamDecode_t* stream = LZ4_createStreamDecode();
  if (args.dict != nullptr) {
    LZ4_setStreamDecode(stream, args.dict->serialized_dict.data(),
                        static_cast<int>(args.dict->serialized_dict.size()));
  }
  int uncompressed_size = LZ4_decompress_safe_continue(
      stream, args.compressed_data.data(), uncompressed_output,
      static_cast<int>(args.compressed_data.size()),
      expected_uncompressed_size);
  LZ4_freeStreamDecode(stream);
#else   // up to r123
  int uncompressed_size =
      LZ4_decompress_safe(args.compressed_data.data(), uncompressed_output,
                          static_cast<int>(args.compressed_data.size()),
                          expected_uncompressed_size);
#endif  // LZ4_VERSION_NUMBER >= 10400

  if (uncompressed_size != expected_uncompressed_size) {
    if (uncompressed_size < 0) {
      return Status::Corruption("Error decompressing LZ4 data");
    } else {
      return Status::Corruption("Size mismatch decompressing LZ4 data");
    }
  }
  return Status::OK();
#else
  (void)args;
  (void)uncompressed_output;
  return Status::NotSupported("LZ4 not supported in this build");
#endif
}

Status XPRESS_DecompressBlock(const Decompressor::Args& args,
                              char* uncompressed_output) {
#ifdef XPRESS
  FIXME();
#else
  (void)args;
  (void)uncompressed_output;
  return Status::NotSupported("XPRESS not supported in this build");
#endif
}

Status ZSTD_DecompressBlockWithContext(
    const Decompressor::Args& args,
    ZSTDUncompressCachedData::ZSTDNativeContext zstd_context,
    char* uncompressed_output) {
#ifdef ZSTD
  size_t uncompressed_size;
  assert(zstd_context != nullptr);
  if (args.dict == nullptr) {
    uncompressed_size = ZSTD_decompressDCtx(
        zstd_context, uncompressed_output, args.uncompressed_size,
        args.compressed_data.data(), args.compressed_data.size());
#ifdef ROCKSDB_ZSTD_DDICT
  } else if (args.dict->processed.get() != nullptr) {
    uncompressed_size = ZSTD_decompress_usingDDict(
        zstd_context, uncompressed_output, args.uncompressed_size,
        args.compressed_data.data(), args.compressed_data.size(),
        reinterpret_cast<ZSTD_DDict*>(args.dict->processed.get()));
#endif  // ROCKSDB_ZSTD_DDICT
  } else {
    uncompressed_size = ZSTD_decompress_usingDict(
        zstd_context, uncompressed_output, args.uncompressed_size,
        args.compressed_data.data(), args.compressed_data.size(),
        args.dict->serialized_dict.data(), args.dict->serialized_dict.size());
  }
  if (ZSTD_isError(uncompressed_size)) {
    return Status::Corruption(ZSTD_getErrorName(uncompressed_size));
  } else if (uncompressed_size != args.uncompressed_size) {
    return Status::Corruption("ZSTD decompression size mismatch");
  } else {
    return Status::OK();
  }
#else
  (void)args;
  (void)uncompressed_output;
  return Status::NotSupported("ZSTD not supported in this build");
#endif
}

Status ZSTD_DecompressBlock(const Decompressor::Args& args,
                            const Decompressor* decompressor,
                            char* uncompressed_output) {
  if (args.working_area && args.working_area->owner() == decompressor) {
    auto ctx = static_cast<UncompressionContext*>(args.working_area->get());
    if (ctx != nullptr && ctx->GetZSTDContext() != nullptr) {
      return ZSTD_DecompressBlockWithContext(args, ctx->GetZSTDContext(),
                                             uncompressed_output);
    }
  }
  UncompressionContext tmp_ctx{kZSTD};
  return ZSTD_DecompressBlockWithContext(args, tmp_ctx.GetZSTDContext(),
                                         uncompressed_output);
}

class BuiltinDecompressorV2 : public Decompressor {
 public:
  Status ExtractUncompressedSize(Args& args) override {
    assert(args.compression_type != kNoCompression);
    if (args.compression_type == kSnappyCompression) {
      // Exception to encoding of uncompressed size
#ifdef SNAPPY
      size_t uncompressed_length = 0;
      if (!snappy::GetUncompressedLength(args.compressed_data.data(),
                                         args.compressed_data.size(),
                                         &uncompressed_length)) {
        return Status::Corruption("Error reading snappy compressed length");
      }
      args.uncompressed_size = uncompressed_length;
      return Status::OK();
#else
      return Status::NotSupported("Snappy not supported in this build");
#endif
    } else {
      // Extract encoded uncompressed size
      return Decompressor::ExtractUncompressedSize(args);
    }
  }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    switch (args.compression_type) {
      case kSnappyCompression:
        return Snappy_DecompressBlock(args, uncompressed_output);
      case kZlibCompression:
        return Zlib_DecompressBlock(args, uncompressed_output);
      case kBZip2Compression:
        return BZip2_DecompressBlock(args, uncompressed_output);
      case kLZ4Compression:
      case kLZ4HCCompression:
        return LZ4_DecompressBlock(args, uncompressed_output);
      case kXpressCompression:
        return XPRESS_DecompressBlock(args, uncompressed_output);
      case kZSTD:
        return ZSTD_DecompressBlock(args, this, uncompressed_output);
      default:
        return Status::NotSupported(
            "Compression type not supported or not built-in: " +
            CompressionTypeToString(args.compression_type));
    }
  }
};

class BuiltinDecompressorV2OptimizeZSTD : public BuiltinDecompressorV2 {
 protected:
  WorkingArea* GetWorkingAreaImpl(CompressionType preferred) override {
    if (preferred == kZSTD) {
      // TODO: evaluate whether it makes sense to use core local cache here
      return static_cast<WorkingArea*>(new UncompressionContext(kZSTD));
    } else {
      return nullptr;
    }
  }

  void ReleaseWorkingArea(WorkingArea* wa) override {
    delete static_cast<UncompressionContext*>(wa);
  }

  ProcessedDict* ProcessDictImpl(Slice dict) override {
#ifdef ROCKSDB_ZSTD_DDICT
    if (!dict.empty()) {
      auto ddict = ZSTD_createDDict_byReference(dict.data(), dict.size());
      assert(ddict != nullptr);
      return reinterpret_cast<ProcessedDict*>(ddict);
    }
#else
    (void)dict;
#endif  // ROCKSDB_ZSTD_DDICT
    return nullptr;
  }

  void ReleaseProcessedDict(ProcessedDict* pdict) override {
#ifdef ROCKSDB_ZSTD_DDICT
    size_t res = 0;
    if (pdict != nullptr) {
      res = ZSTD_freeDDict(reinterpret_cast<ZSTD_DDict*>(pdict));
    }
    assert(res == 0);  // Last I checked they can't fail
    (void)res;         // prevent unused var warning
#else
    assert(pdict == nullptr);
    (void)pdict;
#endif  // ROCKSDB_ZSTD_DDICT
  }

 public:
  size_t ApproximateMemoryUsage(
      const ManagedProcessedDict& pdict) const override {
#ifdef ROCKSDB_ZSTD_DDICT
    if (pdict.get() != nullptr) {
      return ZSTD_sizeof_DDict(
          reinterpret_cast<const ZSTD_DDict*>(pdict.get()));
    }
#else
    (void)pdict;
#endif  // ROCKSDB_ZSTD_DDICT
    return 0;
  }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    if (LIKELY(args.compression_type == kZSTD)) {
      return ZSTD_DecompressBlock(args, this, uncompressed_output);
    } else {
      return BuiltinDecompressorV2::DecompressBlock(args, uncompressed_output);
    }
  }
};

class BuiltinCompressionManagerV2 : public CompressionManager {
 public:
  BuiltinCompressionManagerV2() = default;
  ~BuiltinCompressionManagerV2() override = default;

  std::unique_ptr<Compressor> GetCompressor(const CompressionOptions& opts,
                                            CompressionType type) override {
    return std::make_unique<BuiltinCompressorV2>(opts, type);
  }

  std::shared_ptr<Decompressor> GetDecompressor() override {
    return std::shared_ptr<Decompressor>(shared_from_this(), &decompressor_);
  }

  Status FindCompressionManager(Slice compatibility_name) override {
    return FindBuiltinCompressionManager(compatibility_name);
  }

 protected:
  BuiltinDecompressorV2 decompressor_;
};

}  // namespace

// ***********************************************************************
// END built-in implementation of customization interface
// ***********************************************************************

}  // namespace ROCKSDB_NAMESPACE
