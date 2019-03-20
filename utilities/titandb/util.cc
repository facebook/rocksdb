#include "utilities/titandb/util.h"

namespace rocksdb {
namespace titandb {

// See util/compression.h.
const uint32_t kCompressionFormat = 2;

bool GoodCompressionRatio(size_t compressed_size, size_t raw_size) {
  // Check to see if compressed less than 12.5%
  return compressed_size < raw_size - (raw_size / 8u);
}

Slice Compress(const CompressionContext& ctx, const Slice& input,
               std::string* output, CompressionType* type) {
  *type = ctx.type();
  if (ctx.type() == kNoCompression) {
    return input;
  }

  // Returns compressed block contents if:
  // (1) the compression method is supported in this platform and
  // (2) the compression rate is "good enough".
  switch (ctx.type()) {
    case kSnappyCompression:
      if (Snappy_Compress(ctx, input.data(), input.size(), output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    case kZlibCompression:
      if (Zlib_Compress(ctx, kCompressionFormat, input.data(), input.size(),
                        output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    case kBZip2Compression:
      if (BZip2_Compress(ctx, kCompressionFormat, input.data(), input.size(),
                         output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    case kLZ4Compression:
      if (LZ4_Compress(ctx, kCompressionFormat, input.data(), input.size(),
                       output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    case kLZ4HCCompression:
      if (LZ4HC_Compress(ctx, kCompressionFormat, input.data(), input.size(),
                         output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    case kXpressCompression:
      if (XPRESS_Compress(input.data(), input.size(), output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    case kZSTD:
    case kZSTDNotFinalCompression:
      if (ZSTD_Compress(ctx, input.data(), input.size(), output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    default: {}  // Do not recognize this compression type
  }

  // Compression method is not supported, or not good compression
  // ratio, so just fall back to uncompressed form.
  *type = kNoCompression;
  return input;
}

Status Uncompress(const UncompressionContext& ctx, const Slice& input,
                  OwnedSlice* output) {
  int size = 0;
  std::unique_ptr<char[]> ubuf;
  assert(ctx.type() != kNoCompression);

  switch (ctx.type()) {
    case kSnappyCompression: {
      size_t usize = 0;
      if (!Snappy_GetUncompressedLength(input.data(), input.size(), &usize)) {
        return Status::Corruption("Corrupted compressed blob", "Snappy");
      }
      ubuf.reset(new char[usize]);
      if (!Snappy_Uncompress(input.data(), input.size(), ubuf.get())) {
        return Status::Corruption("Corrupted compressed blob", "Snappy");
      }
      output->reset(std::move(ubuf), usize);
      break;
    }
    case kZlibCompression:
      ubuf.reset(Zlib_Uncompress(ctx, input.data(), input.size(), &size,
                                 kCompressionFormat));
      if (!ubuf.get()) {
        return Status::Corruption("Corrupted compressed blob", "Zlib");
      }
      output->reset(std::move(ubuf), size);
      break;
    case kBZip2Compression:
      ubuf.reset(BZip2_Uncompress(input.data(), input.size(), &size,
                                  kCompressionFormat));
      if (!ubuf.get()) {
        return Status::Corruption("Corrupted compressed blob", "Bzip2");
      }
      output->reset(std::move(ubuf), size);
      break;
    case kLZ4Compression:
      ubuf.reset(LZ4_Uncompress(ctx, input.data(), input.size(), &size,
                                kCompressionFormat));
      if (!ubuf.get()) {
        return Status::Corruption("Corrupted compressed blob", "LZ4");
      }
      output->reset(std::move(ubuf), size);
      break;
    case kLZ4HCCompression:
      ubuf.reset(LZ4_Uncompress(ctx, input.data(), input.size(), &size,
                                kCompressionFormat));
      if (!ubuf.get()) {
        return Status::Corruption("Corrupted compressed blob", "LZ4HC");
      }
      output->reset(std::move(ubuf), size);
      break;
    case kXpressCompression:
      ubuf.reset(XPRESS_Uncompress(input.data(), input.size(), &size));
      if (!ubuf.get()) {
        return Status::Corruption("Corrupted compressed blob", "Xpress");
      }
      output->reset(std::move(ubuf), size);
      break;
    case kZSTD:
    case kZSTDNotFinalCompression:
      ubuf.reset(ZSTD_Uncompress(ctx, input.data(), input.size(), &size));
      if (!ubuf.get()) {
        return Status::Corruption("Corrupted compressed blob", "ZSTD");
      }
      output->reset(std::move(ubuf), size);
      break;
    default:
      return Status::Corruption("bad compression type");
  }

  return Status::OK();
}

void UnrefCacheHandle(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

}  // namespace titandb
}  // namespace rocksdb
