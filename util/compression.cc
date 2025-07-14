// Copyright (c) 2022-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/compression.h"

#include "options/options_helper.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/object_registry.h"

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
Status Decompressor::ExtractUncompressedSize(Args& args) {
  // Default implementation:
  //
  // Standard format for prepending uncompressed size to the compressed
  // payload. (RocksDB compress_format_version=2 except Snappy)
  //
  // This is historically a varint32, but it is preliminarily generalized
  // to varint64, in case that is supported on the write side for some
  // algorithms.
  if (LIKELY(GetVarint64(&args.compressed_data, &args.uncompressed_size))) {
    if (LIKELY(args.uncompressed_size <= SIZE_MAX)) {
      return Status::OK();
    } else {
      return Status::MemoryLimit("Uncompressed size too large for platform");
    }
  } else {
    return Status::Corruption("Unable to extract uncompressed size");
  }
}

const Slice& Decompressor::GetSerializedDict() const {
  // Default: empty slice => no dictionary
  static Slice kEmptySlice;
  return kEmptySlice;
}

namespace {

class BuiltinCompressorV1 : public Compressor {
 public:
  const char* Name() const override { return "BuiltinCompressorV1"; }

  explicit BuiltinCompressorV1(const CompressionOptions& opts,
                               CompressionType type)
      : opts_(opts), type_(type) {
    assert(type != kNoCompression);
  }

  CompressionType GetPreferredCompressionType() const override { return type_; }

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
    if (!OLD_CompressData(uncompressed_data, info,
                          1 /*compress_format_version*/, compressed_output)) {
      *out_compression_type = kNoCompression;
      return Status::OK();
    }
    *out_compression_type = type_;
    return Status::OK();
  }

 protected:
  const CompressionOptions opts_;
  const CompressionType type_;
};

class BuiltinCompressorV2 : public Compressor {
 public:
  const char* Name() const override { return "BuiltinCompressorV2"; }

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
    } else {
      return type_ == kZSTD && opts_.zstd_max_train_bytes > 0
                 ? opts_.zstd_max_train_bytes
                 : opts_.max_dict_bytes;
    }
  }

  // NOTE: empty dict is equivalent to no dict
  Slice GetSerializedDict() const override { return dict_.GetRawDict(); }

  CompressionType GetPreferredCompressionType() const override { return type_; }

  std::unique_ptr<Compressor> MaybeCloneSpecialized(
      CacheEntryRole /*block_type*/, DictSampleArgs&& dict_samples) override {
    assert(dict_samples.Verify());
    if (dict_samples.empty()) {
      // Nothing to specialize on
      return nullptr;
    }
    std::string dict_data;
    // Migrated from BlockBasedTableBuilder::EnterUnbuffered()
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
  ManagedWorkingArea ObtainWorkingArea() override {
    return ManagedWorkingArea(new CompressionContext(type_, opts_), this);
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
    CompressionType type = type_;
    if (ctx == nullptr) {
      tmp_ctx.emplace(type, opts_);
      ctx = &*tmp_ctx;
    }
    CompressionInfo info(opts_, *ctx, dict_, type);
    if (!OLD_CompressData(uncompressed_data, info,
                          2 /*compress_format_version*/, compressed_output)) {
      *out_compression_type = kNoCompression;
      return Status::OK();
    }
    *out_compression_type = type;
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
  const char* Name() const override { return "BuiltinDecompressorV1"; }

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
    assert(args.working_area == nullptr);
    assert(*out_uncompressed_size == 0);

    // NOTE: simple but inefficient
    UncompressionContext dummy_ctx{args.compression_type};
    UncompressionInfo info{dummy_ctx, UncompressionDict::GetEmptyDict(),
                           args.compression_type};
    const char* error_message = nullptr;
    size_t size_t_uncompressed_size = 0;
    *out_data = OLD_UncompressData(
        info, args.compressed_data.data(), args.compressed_data.size(),
        &size_t_uncompressed_size, 1 /*compress_format_version*/,
        nullptr /*allocator*/, &error_message);
    if (*out_data == nullptr) {
      if (error_message != nullptr) {
        return Status::Corruption(error_message);
      } else {
        return Status::Corruption("Corrupted compressed block contents");
      }
    }
    *out_uncompressed_size = size_t_uncompressed_size;
    assert(*out_uncompressed_size > 0);
    return Status::OK();
  }
};

class BuiltinCompressionManagerV1 : public CompressionManager {
 public:
  BuiltinCompressionManagerV1() = default;
  ~BuiltinCompressionManagerV1() override = default;

  const char* Name() const override { return "BuiltinCompressionManagerV1"; }

  const char* CompatibilityName() const override { return "BuiltinV1"; }

  std::unique_ptr<Compressor> GetCompressor(const CompressionOptions& opts,
                                            CompressionType type) override {
    // At the time of deprecating the writing of new format_version=1 files,
    // ZSTD was the last supported built-in compression type.
    if (type > kZSTD) {
      // Unrecognized; fall back on default compression
      type = ColumnFamilyOptions{}.compression;
    }
    if (type == kNoCompression) {
      return nullptr;
    } else {
      return std::make_unique<BuiltinCompressorV1>(opts, type);
    }
  }

  std::shared_ptr<Decompressor> GetDecompressor() override {
    return std::shared_ptr<Decompressor>(shared_from_this(), &decompressor_);
  }

  bool SupportsCompressionType(CompressionType type) const override {
    return CompressionTypeSupported(type);
  }

 protected:
  BuiltinDecompressorV1 decompressor_;
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

Status Zlib_DecompressBlock(const Decompressor::Args& args, Slice dict,
                            char* uncompressed_output) {
#ifdef ZLIB
  // NOTE: uses "raw" format
  constexpr int kWindowBits = -14;

  z_stream _stream;
  memset(&_stream, 0, sizeof(z_stream));

  // For raw inflate, the windowBits should be -8..-15.
  // If windowBits is bigger than zero, it will use either zlib
  // header or gzip header. Adding 32 to it will do automatic detection.
  int st = inflateInit2(&_stream, kWindowBits);
  if (UNLIKELY(st != Z_OK)) {
    return Status::Corruption("Failed to initialize zlib inflate: " +
                              std::to_string(st));
  }

  if (!dict.empty()) {
    // Initialize the compression library's dictionary
    st = inflateSetDictionary(&_stream,
                              reinterpret_cast<const Bytef*>(dict.data()),
                              static_cast<unsigned int>(dict.size()));
    if (UNLIKELY(st != Z_OK)) {
      return Status::Corruption("Failed to initialize zlib dictionary: " +
                                std::to_string(st));
    }
  }

  _stream.next_in = const_cast<Bytef*>(
      reinterpret_cast<const Bytef*>(args.compressed_data.data()));
  _stream.avail_in = static_cast<unsigned int>(args.compressed_data.size());

  _stream.next_out = reinterpret_cast<Bytef*>(uncompressed_output);
  _stream.avail_out = static_cast<unsigned int>(args.uncompressed_size);

  st = inflate(&_stream, Z_SYNC_FLUSH);
  if (UNLIKELY(st != Z_STREAM_END)) {
    inflateEnd(&_stream);
    // NOTE: Z_OK is still corruption because it means we got the size wrong
    return Status::Corruption("Failed zlib inflate: " + std::to_string(st));
  }

  // We should have no bytes left
  if (_stream.avail_out != 0) {
    inflateEnd(&_stream);
    return Status::Corruption("Size mismatch decompressing zlib data");
  }

  inflateEnd(&_stream);
  return Status::OK();
#else
  (void)args;
  (void)dict;
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

Status LZ4_DecompressBlock(const Decompressor::Args& args, Slice dict,
                           char* uncompressed_output) {
#ifdef LZ4
  int expected_uncompressed_size = static_cast<int>(args.uncompressed_size);
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  LZ4_streamDecode_t* stream = LZ4_createStreamDecode();
  if (!dict.empty()) {
    LZ4_setStreamDecode(stream, dict.data(), static_cast<int>(dict.size()));
  }
  int uncompressed_size = LZ4_decompress_safe_continue(
      stream, args.compressed_data.data(), uncompressed_output,
      static_cast<int>(args.compressed_data.size()),
      expected_uncompressed_size);
  LZ4_freeStreamDecode(stream);
#else   // up to r123
  if (!dict.empty()) {
    return Status::NotSupported(
        "This build doesn't support dictionary compression with LZ4");
  }
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
  (void)dict;
  (void)uncompressed_output;
  return Status::NotSupported("LZ4 not supported in this build");
#endif
}

Status XPRESS_DecompressBlock(const Decompressor::Args& args,
                              char* uncompressed_output) {
#ifdef XPRESS
  int64_t actual_uncompressed_size = port::xpress::DecompressToBuffer(
      args.compressed_data.data(), args.compressed_data.size(),
      uncompressed_output, args.uncompressed_size);
  if (actual_uncompressed_size !=
      static_cast<int64_t>(args.uncompressed_size)) {
    if (actual_uncompressed_size < 0) {
      return Status::Corruption("Error decompressing XPRESS data");
    } else {
      return Status::Corruption("Size mismatch decompressing XPRESS data");
    }
  }
  return Status::OK();
#else
  (void)args;
  (void)uncompressed_output;
  return Status::NotSupported("XPRESS not supported in this build");
#endif
}

template <bool kIsDigestedDict = false>
Status ZSTD_DecompressBlockWithContext(
    const Decompressor::Args& args,
    std::conditional_t<kIsDigestedDict, void*, Slice> dict,
    ZSTDUncompressCachedData::ZSTDNativeContext zstd_context,
    char* uncompressed_output) {
#ifdef ZSTD
  size_t uncompressed_size;
  assert(zstd_context != nullptr);
  if constexpr (kIsDigestedDict) {
#ifdef ROCKSDB_ZSTD_DDICT
    uncompressed_size = ZSTD_decompress_usingDDict(
        zstd_context, uncompressed_output, args.uncompressed_size,
        args.compressed_data.data(), args.compressed_data.size(),
        static_cast<ZSTD_DDict*>(dict));
#else
    static_assert(!kIsDigestedDict,
                  "Inconsistent expectation of ZSTD digested dict support");
#endif  // ROCKSDB_ZSTD_DDICT
  } else if (dict.empty()) {
    uncompressed_size = ZSTD_decompressDCtx(
        zstd_context, uncompressed_output, args.uncompressed_size,
        args.compressed_data.data(), args.compressed_data.size());
  } else {
    uncompressed_size = ZSTD_decompress_usingDict(
        zstd_context, uncompressed_output, args.uncompressed_size,
        args.compressed_data.data(), args.compressed_data.size(), dict.data(),
        dict.size());
  }
  if (ZSTD_isError(uncompressed_size)) {
    return Status::Corruption(std::string("ZSTD ") +
                              ZSTD_getErrorName(uncompressed_size));
  } else if (uncompressed_size != args.uncompressed_size) {
    return Status::Corruption("ZSTD decompression size mismatch");
  } else {
    return Status::OK();
  }
#else
  (void)args;
  (void)dict;
  (void)zstd_context;
  (void)uncompressed_output;
  return Status::NotSupported("ZSTD not supported in this build");
#endif
}

template <bool kIsDigestedDict = false>
Status ZSTD_DecompressBlock(
    const Decompressor::Args& args,
    std::conditional_t<kIsDigestedDict, void*, Slice> dict,
    const Decompressor* decompressor, char* uncompressed_output) {
  if (args.working_area && args.working_area->owner() == decompressor) {
    auto ctx = static_cast<UncompressionContext*>(args.working_area->get());
    assert(ctx != nullptr);
    if (ctx->GetZSTDContext() != nullptr) {
      return ZSTD_DecompressBlockWithContext<kIsDigestedDict>(
          args, dict, ctx->GetZSTDContext(), uncompressed_output);
    }
  }
  UncompressionContext tmp_ctx{kZSTD};
  return ZSTD_DecompressBlockWithContext<kIsDigestedDict>(
      args, dict, tmp_ctx.GetZSTDContext(), uncompressed_output);
}

class BuiltinDecompressorV2 : public Decompressor {
 public:
  const char* Name() const override { return "BuiltinDecompressorV2"; }

  Status ExtractUncompressedSize(Args& args) override {
    assert(args.compression_type != kNoCompression);
    if (args.compression_type == kSnappyCompression) {
      // 1st exception to encoding of uncompressed size
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
    } else if (args.compression_type == kXpressCompression) {
      // 2nd exception to encoding of uncompressed size
#ifdef XPRESS
      int64_t result = port::xpress::GetDecompressedSize(
          args.compressed_data.data(), args.compressed_data.size());
      if (result < 0) {
        return Status::Corruption("Error reading XPRESS compressed length");
      }
      args.uncompressed_size = static_cast<size_t>(result);
      return Status::OK();
#else
      return Status::NotSupported("XPRESS not supported in this build");
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
        return Zlib_DecompressBlock(args, /*dict=*/Slice{},
                                    uncompressed_output);
      case kBZip2Compression:
        return BZip2_DecompressBlock(args, uncompressed_output);
      case kLZ4Compression:
      case kLZ4HCCompression:
        return LZ4_DecompressBlock(args, /*dict=*/Slice{}, uncompressed_output);
      case kXpressCompression:
        return XPRESS_DecompressBlock(args, uncompressed_output);
      case kZSTD:
        return ZSTD_DecompressBlock(args, /*dict=*/Slice{}, this,
                                    uncompressed_output);
      default:
        return Status::NotSupported(
            "Compression type not supported or not built-in: " +
            CompressionTypeToString(args.compression_type));
    }
  }

  Status MaybeCloneForDict(const Slice&,
                           std::unique_ptr<Decompressor>*) override;

  size_t ApproximateOwnedMemoryUsage() const override {
    return sizeof(BuiltinDecompressorV2);
  }
};

class BuiltinDecompressorV2SnappyOnly : public BuiltinDecompressorV2 {
 public:
  const char* Name() const override {
    return "BuiltinDecompressorV2SnappyOnly";
  }

  Status ExtractUncompressedSize(Args& args) override {
    assert(args.compression_type == kSnappyCompression);
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
  }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    assert(args.compression_type == kSnappyCompression);
    return Snappy_DecompressBlock(args, uncompressed_output);
  }
};

class BuiltinDecompressorV2WithDict : public BuiltinDecompressorV2 {
 public:
  explicit BuiltinDecompressorV2WithDict(const Slice& dict) : dict_(dict) {}

  const char* Name() const override { return "BuiltinDecompressorV2WithDict"; }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    switch (args.compression_type) {
      case kSnappyCompression:
        // NOTE: quietly ignores the dictionary (for compatibility)
        return Snappy_DecompressBlock(args, uncompressed_output);
      case kZlibCompression:
        return Zlib_DecompressBlock(args, dict_, uncompressed_output);
      case kBZip2Compression:
        // NOTE: quietly ignores the dictionary (for compatibility)
        return BZip2_DecompressBlock(args, uncompressed_output);
      case kLZ4Compression:
      case kLZ4HCCompression:
        return LZ4_DecompressBlock(args, dict_, uncompressed_output);
      case kXpressCompression:
        // NOTE: quietly ignores the dictionary (for compatibility)
        return XPRESS_DecompressBlock(args, uncompressed_output);
      case kZSTD:
        return ZSTD_DecompressBlock(args, dict_, this, uncompressed_output);
      default:
        return Status::NotSupported(
            "Compression type not supported or not built-in: " +
            CompressionTypeToString(args.compression_type));
    }
  }

  const Slice& GetSerializedDict() const override { return dict_; }

  size_t ApproximateOwnedMemoryUsage() const override {
    return sizeof(BuiltinDecompressorV2WithDict);
  }

 protected:
  const Slice dict_;
};

Status BuiltinDecompressorV2::MaybeCloneForDict(
    const Slice& dict, std::unique_ptr<Decompressor>* out) {
  // Check RocksDB-promised precondition
  assert(dict.size() > 0);
  // Because of unfortunate decisions in handling built-in compression types,
  // all the compression types before ZSTD that do not actually support
  // dictionary compression pretend to support it. Specifically, we have to be
  // able to read files with a compression dictionary block using those
  // compression types even though the compression dictionary is ignored by
  // the compression algorithm. And the Decompressor has to return the
  // configured dictionary from GetSerializedDict() even if it is ignored. This
  // unfortunately means that a new schema version (BuiltinV3?) would be needed
  // toactually support dictionary compression in the future for these
  // algorithms (if the libraries add support).
  // TODO: can we make this a better/cleaner experience?
  *out = std::make_unique<BuiltinDecompressorV2WithDict>(dict);
  return Status::OK();
}

class BuiltinDecompressorV2OptimizeZstd : public BuiltinDecompressorV2 {
 public:
  const char* Name() const override {
    return "BuiltinDecompressorV2OptimizeZstd";
  }

  ManagedWorkingArea ObtainWorkingArea(CompressionType preferred) override {
    if (preferred == kZSTD) {
      // TODO: evaluate whether it makes sense to use core local cache here.
      // (Perhaps not, because explicit WorkingArea could be long-running.)
      return ManagedWorkingArea(new UncompressionContext(kZSTD), this);
    } else {
      return {};
    }
  }

  void ReleaseWorkingArea(WorkingArea* wa) override {
    delete static_cast<UncompressionContext*>(wa);
  }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    if (LIKELY(args.compression_type == kZSTD)) {
      return ZSTD_DecompressBlock(args, /*dict=*/Slice{}, this,
                                  uncompressed_output);
    } else {
      return BuiltinDecompressorV2::DecompressBlock(args, uncompressed_output);
    }
  }

  Status MaybeCloneForDict(const Slice& /*serialized_dict*/,
                           std::unique_ptr<Decompressor>* /*out*/) override;
};

class BuiltinDecompressorV2OptimizeZstdWithDict
    : public BuiltinDecompressorV2OptimizeZstd {
 public:
  BuiltinDecompressorV2OptimizeZstdWithDict(const Slice& dict)
      :
#ifdef ROCKSDB_ZSTD_DDICT
        dict_(dict),
        ddict_(ZSTD_createDDict_byReference(dict.data(), dict.size())) {
    assert(ddict_ != nullptr);
  }
#else
        dict_(dict) {
  }
#endif  // ROCKSDB_ZSTD_DDICT

  const char* Name() const override {
    return "BuiltinDecompressorV2OptimizeZstdWithDict";
  }

  ~BuiltinDecompressorV2OptimizeZstdWithDict() override {
#ifdef ROCKSDB_ZSTD_DDICT
    size_t res = ZSTD_freeDDict(ddict_);
    assert(res == 0);  // Last I checked they can't fail
    (void)res;         // prevent unused var warning
#endif                 // ROCKSDB_ZSTD_DDICT
  }

  const Slice& GetSerializedDict() const override { return dict_; }

  size_t ApproximateOwnedMemoryUsage() const override {
    size_t sz = sizeof(BuiltinDecompressorV2WithDict);
#ifdef ROCKSDB_ZSTD_DDICT
    sz += ZSTD_sizeof_DDict(ddict_);
#endif  // ROCKSDB_ZSTD_DDICT
    return sz;
  }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    if (LIKELY(args.compression_type == kZSTD)) {
#ifdef ROCKSDB_ZSTD_DDICT
      return ZSTD_DecompressBlock</*kIsDigestedDict=*/true>(
          args, ddict_, this, uncompressed_output);
#else
      return ZSTD_DecompressBlock(args, dict_, this, uncompressed_output);
#endif  // ROCKSDB_ZSTD_DDICT
    } else {
      return BuiltinDecompressorV2WithDict(dict_).DecompressBlock(
          args, uncompressed_output);
    }
  }

 protected:
  const Slice dict_;
#ifdef ROCKSDB_ZSTD_DDICT
  ZSTD_DDict* const ddict_;
#endif  // ROCKSDB_ZSTD_DDICT
};

Status BuiltinDecompressorV2OptimizeZstd::MaybeCloneForDict(
    const Slice& serialized_dict, std::unique_ptr<Decompressor>* out) {
  *out = std::make_unique<BuiltinDecompressorV2OptimizeZstdWithDict>(
      serialized_dict);
  return Status::OK();
}
class BuiltinCompressionManagerV2 : public CompressionManager {
 public:
  BuiltinCompressionManagerV2() = default;
  ~BuiltinCompressionManagerV2() override = default;

  const char* Name() const override { return "BuiltinCompressionManagerV2"; }

  const char* CompatibilityName() const override { return "BuiltinV2"; }

  std::unique_ptr<Compressor> GetCompressor(const CompressionOptions& opts,
                                            CompressionType type) override {
    if (opts.max_compressed_bytes_per_kb <= 0) {
      // No acceptable compression ratio => no compression
      return nullptr;
    }
    if (type > kLastBuiltinCompression) {
      // Unrecognized; fall back on default compression
      type = ColumnFamilyOptions{}.compression;
    }
    if (type == kNoCompression) {
      return nullptr;
    } else {
      return std::make_unique<BuiltinCompressorV2>(opts, type);
    }
  }

  std::shared_ptr<Decompressor> GetDecompressor() override {
    return GetGeneralDecompressor();
  }

  std::shared_ptr<Decompressor> GetDecompressorOptimizeFor(
      CompressionType optimize_for_type) override {
    if (optimize_for_type == kZSTD) {
      return GetZstdDecompressor();
    } else {
      return GetGeneralDecompressor();
    }
  }

  std::shared_ptr<Decompressor> GetDecompressorForTypes(
      const CompressionType* types_begin,
      const CompressionType* types_end) override {
    if (types_begin == types_end) {
      return nullptr;
    } else if (types_begin + 1 == types_end &&
               *types_begin == kSnappyCompression) {
      return GetSnappyDecompressor();
    } else if (std::find(types_begin, types_end, kZSTD)) {
      return GetZstdDecompressor();
    } else {
      return GetGeneralDecompressor();
    }
  }
  std::shared_ptr<Decompressor> GetDecompressorForCompressor(
      const Compressor& compressor) override {
#ifdef ROCKSDB_USE_RTTI
    // To be extra safe, only optimize here if we are certain we are not
    // looking at a wrapped compressor, so that we are sure it only uses that
    // one compression type.
    if (dynamic_cast<const BuiltinCompressorV2*>(&compressor)) {
      CompressionType type = compressor.GetPreferredCompressionType();
      return GetDecompressorForTypes(&type, &type + 1);
    }
#endif
    // Fallback
    return CompressionManager::GetDecompressorForCompressor(compressor);
  }

  bool SupportsCompressionType(CompressionType type) const override {
    return CompressionTypeSupported(type);
  }

 protected:
  BuiltinDecompressorV2 decompressor_;
  BuiltinDecompressorV2OptimizeZstd zstd_decompressor_;
  BuiltinDecompressorV2SnappyOnly snappy_decompressor_;

  inline std::shared_ptr<Decompressor> GetGeneralDecompressor() {
    return std::shared_ptr<Decompressor>(shared_from_this(), &decompressor_);
  }

  inline std::shared_ptr<Decompressor> GetZstdDecompressor() {
    return std::shared_ptr<Decompressor>(shared_from_this(),
                                         &zstd_decompressor_);
  }

  inline std::shared_ptr<Decompressor> GetSnappyDecompressor() {
    return std::shared_ptr<Decompressor>(shared_from_this(),
                                         &snappy_decompressor_);
  }
};

const std::shared_ptr<BuiltinCompressionManagerV1>
    kBuiltinCompressionManagerV1 =
        std::make_shared<BuiltinCompressionManagerV1>();
const std::shared_ptr<BuiltinCompressionManagerV2>
    kBuiltinCompressionManagerV2 =
        std::make_shared<BuiltinCompressionManagerV2>();

}  // namespace

Status CompressionManager::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<CompressionManager>* result) {
  if (value == kNullptrString || value.empty()) {
    result->reset();
    return Status::OK();
  }

  static std::once_flag loaded;
  std::call_once(loaded, [&]() {
    auto& library = *ObjectLibrary::Default();
    // TODO: try to enhance ObjectLibrary to support singletons
    library.AddFactory<CompressionManager>(
        kBuiltinCompressionManagerV1->CompatibilityName(),
        [](const std::string& /*uri*/,
           std::unique_ptr<CompressionManager>* guard,
           std::string* /*errmsg*/) {
          *guard = std::make_unique<BuiltinCompressionManagerV1>();
          return guard->get();
        });
    library.AddFactory<CompressionManager>(
        kBuiltinCompressionManagerV2->CompatibilityName(),
        [](const std::string& /*uri*/,
           std::unique_ptr<CompressionManager>* guard,
           std::string* /*errmsg*/) {
          *guard = std::make_unique<BuiltinCompressionManagerV2>();
          return guard->get();
        });
  });

  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(config_options, result->get(),
                                              value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  } else if (id.empty()) {  // We have no Id but have options.  Not good
    return Status::NotSupported("Cannot reset object ", id);
  } else {
    status = config_options.registry->NewSharedObject(id, result);
  }
  if (config_options.ignore_unsupported_options && status.IsNotSupported()) {
    return Status::OK();
  } else if (status.ok()) {
    status = Customizable::ConfigureNewObject(config_options, result->get(),
                                              opt_map);
  }
  return status;
}

std::shared_ptr<CompressionManager>
CompressionManager::FindCompatibleCompressionManager(Slice compatibility_name) {
  if (compatibility_name.compare(CompatibilityName()) == 0) {
    return shared_from_this();
  } else {
    std::shared_ptr<CompressionManager> out;
    Status s =
        CreateFromString(ConfigOptions(), compatibility_name.ToString(), &out);
    if (s.ok()) {
      return out;
    } else {
      return nullptr;
    }
  }
}

const std::shared_ptr<CompressionManager>& GetBuiltinCompressionManager(
    int compression_format_version) {
  static const std::shared_ptr<CompressionManager> v1_as_base =
      kBuiltinCompressionManagerV1;
  static const std::shared_ptr<CompressionManager> v2_as_base =
      kBuiltinCompressionManagerV2;
  static const std::shared_ptr<CompressionManager> none;
  if (compression_format_version == 1) {
    return v1_as_base;
  } else if (compression_format_version == 2) {
    return v2_as_base;
  } else {
    // Unrecognized. In some cases this is unexpected and the caller can
    // rightfully crash.
    return none;
  }
}

const std::shared_ptr<CompressionManager>& GetBuiltinV2CompressionManager() {
  return GetBuiltinCompressionManager(2);
}

// ***********************************************************************
// END built-in implementation of customization interface
// ***********************************************************************

}  // namespace ROCKSDB_NAMESPACE
