#include "utilities/titandb/blob_file_reader.h"

#include "util/crc32c.h"
#include "util/filename.h"

namespace rocksdb {
namespace titandb {

Status NewBlobFileReader(uint64_t file_number, uint64_t readahead_size,
                         const TitanDBOptions& db_options,
                         const EnvOptions& env_options, Env* env,
                         std::unique_ptr<RandomAccessFileReader>* result) {
  std::unique_ptr<RandomAccessFile> file;
  auto file_name = BlobFileName(db_options.dirname, file_number);
  Status s = env->NewRandomAccessFile(file_name, &file, env_options);
  if (!s.ok()) return s;

  if (readahead_size > 0) {
    file = NewReadaheadRandomAccessFile(std::move(file), readahead_size);
  }
  result->reset(new RandomAccessFileReader(std::move(file), file_name));
  return s;
}

const uint64_t kMaxReadaheadSize = 256 << 10;

namespace {

void GenerateCachePrefix(std::string* dst, Cache* cc, RandomAccessFile* file) {
  char buffer[kMaxVarint64Length * 3 + 1];
  auto size = file->GetUniqueId(buffer, sizeof(buffer));
  if (size == 0) {
    auto end = EncodeVarint64(buffer, cc->NewId());
    size = end - buffer;
  }
  dst->assign(buffer, size);
}

void EncodeBlobCache(std::string* dst, const Slice& prefix, uint64_t offset) {
  dst->assign(prefix.data(), prefix.size());
  PutVarint64(dst, offset);
}

}  // namespace

Status BlobFileReader::Open(const TitanCFOptions& options,
                            std::unique_ptr<RandomAccessFileReader> file,
                            uint64_t file_size,
                            std::unique_ptr<BlobFileReader>* result) {
  if (file_size < BlobFileFooter::kEncodedLength) {
    return Status::Corruption("file is too short to be a blob file");
  }

  FixedSlice<BlobFileFooter::kEncodedLength> buffer;
  TRY(file->Read(file_size - BlobFileFooter::kEncodedLength,
                 BlobFileFooter::kEncodedLength, &buffer, buffer.get()));

  BlobFileFooter footer;
  TRY(DecodeInto(buffer, &footer));

  auto reader = new BlobFileReader(options, std::move(file));
  reader->footer_ = footer;
  result->reset(reader);
  return Status::OK();
}

BlobFileReader::BlobFileReader(const TitanCFOptions& options,
                               std::unique_ptr<RandomAccessFileReader> file)
    : options_(options), file_(std::move(file)), cache_(options.blob_cache) {
  if (cache_) {
    GenerateCachePrefix(&cache_prefix_, cache_.get(), file_->file());
  }
}

Status BlobFileReader::Get(const ReadOptions& /*options*/,
                           const BlobHandle& handle, BlobRecord* record,
                           PinnableSlice* buffer) {
  std::string cache_key;
  Cache::Handle* cache_handle = nullptr;
  if (cache_) {
    EncodeBlobCache(&cache_key, cache_prefix_, handle.offset);
    cache_handle = cache_->Lookup(cache_key);
    if (cache_handle) {
      auto blob = reinterpret_cast<OwnedSlice*>(cache_->Value(cache_handle));
      buffer->PinSlice(*blob, UnrefCacheHandle, cache_.get(), cache_handle);
      return DecodeInto(*blob, record);
    }
  }

  OwnedSlice blob;
  TRY(ReadRecord(handle, record, &blob));

  if (cache_) {
    auto cache_value = new OwnedSlice(std::move(blob));
    auto cache_size = cache_value->size() + sizeof(*cache_value);
    cache_->Insert(cache_key, cache_value, cache_size,
                   &DeleteCacheValue<OwnedSlice>, &cache_handle);
    buffer->PinSlice(*cache_value, UnrefCacheHandle, cache_.get(),
                     cache_handle);
  } else {
    buffer->PinSlice(blob, OwnedSlice::CleanupFunc, blob.release(), nullptr);
  }

  return Status::OK();
}

Status BlobFileReader::ReadRecord(const BlobHandle& handle, BlobRecord* record,
                                  OwnedSlice* buffer) {
  Slice blob;
  std::unique_ptr<char[]> ubuf(new char[handle.size]);
  TRY(file_->Read(handle.offset, handle.size, &blob, ubuf.get()));
  // something must be wrong
  if (handle.size != blob.size()) {
    fprintf(stderr, "ReadRecord actual size:%lu != blob size:%lu\n",
            blob.size(), static_cast<std::size_t>(handle.size));
    abort();
  }

  BlobDecoder decoder;
  TRY(decoder.DecodeHeader(&blob));
  buffer->reset(std::move(ubuf), blob);
  TRY(decoder.DecodeRecord(&blob, record, buffer));
  return Status::OK();
}

Status BlobFilePrefetcher::Get(const ReadOptions& options,
                               const BlobHandle& handle, BlobRecord* record,
                               PinnableSlice* buffer) {
  if (handle.offset == last_offset_) {
    last_offset_ = handle.offset + handle.size;
    if (handle.offset + handle.size > readahead_limit_) {
      readahead_size_ = std::max(handle.size, readahead_size_);
      reader_->file_->Prefetch(handle.offset, readahead_size_);
      readahead_limit_ = handle.offset + readahead_size_;
      readahead_size_ = std::min(kMaxReadaheadSize, readahead_size_ * 2);
    }
  } else {
    last_offset_ = handle.offset + handle.size;
    readahead_size_ = 0;
    readahead_limit_ = 0;
  }

  return reader_->Get(options, handle, record, buffer);
}

}  // namespace titandb
}  // namespace rocksdb
