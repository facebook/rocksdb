#include "utilities/titandb/blob_file_cache.h"

#include "util/filename.h"
#include "utilities/titandb/util.h"

namespace rocksdb {
namespace titandb {

namespace {

Slice EncodeFileNumber(const uint64_t* number) {
  return Slice(reinterpret_cast<const char*>(number), sizeof(*number));
}

}  // namespace

BlobFileCache::BlobFileCache(const TitanDBOptions& db_options,
                             const TitanCFOptions& cf_options,
                             std::shared_ptr<Cache> cache)
    : env_(db_options.env),
      env_options_(db_options),
      db_options_(db_options),
      cf_options_(cf_options),
      cache_(cache) {}

Status BlobFileCache::Get(const ReadOptions& options, uint64_t file_number,
                          uint64_t file_size, const BlobHandle& handle,
                          BlobRecord* record, PinnableSlice* buffer) {
  Cache::Handle* cache_handle = nullptr;
  Status s = FindFile(file_number, file_size, &cache_handle);
  if (!s.ok()) return s;

  auto reader = reinterpret_cast<BlobFileReader*>(cache_->Value(cache_handle));
  s = reader->Get(options, handle, record, buffer);
  cache_->Release(cache_handle);
  return s;
}

Status BlobFileCache::NewPrefetcher(
    uint64_t file_number, uint64_t file_size,
    std::unique_ptr<BlobFilePrefetcher>* result) {
  Cache::Handle* cache_handle = nullptr;
  Status s = FindFile(file_number, file_size, &cache_handle);
  if (!s.ok()) return s;

  auto reader = reinterpret_cast<BlobFileReader*>(cache_->Value(cache_handle));
  auto prefetcher = new BlobFilePrefetcher(reader);
  prefetcher->RegisterCleanup(&UnrefCacheHandle, cache_.get(), cache_handle);
  result->reset(prefetcher);
  return s;
}

void BlobFileCache::Evict(uint64_t file_number) {
  cache_->Erase(EncodeFileNumber(&file_number));
}

Status BlobFileCache::FindFile(uint64_t file_number, uint64_t file_size,
                               Cache::Handle** handle) {
  Status s;
  Slice cache_key = EncodeFileNumber(&file_number);
  *handle = cache_->Lookup(cache_key);
  if (*handle) return s;

  std::unique_ptr<RandomAccessFileReader> file;
  {
    std::unique_ptr<RandomAccessFile> f;
    auto file_name = BlobFileName(db_options_.dirname, file_number);
    s = env_->NewRandomAccessFile(file_name, &f, env_options_);
    if (!s.ok()) return s;
    if (db_options_.advise_random_on_open) {
      f->Hint(RandomAccessFile::RANDOM);
    }
    file.reset(new RandomAccessFileReader(std::move(f), file_name));
  }

  std::unique_ptr<BlobFileReader> reader;
  s = BlobFileReader::Open(cf_options_, std::move(file), file_size, &reader);
  if (!s.ok()) return s;

  cache_->Insert(cache_key, reader.release(), 1,
                 &DeleteCacheValue<BlobFileReader>, handle);
  return s;
}

}  // namespace titandb
}  // namespace rocksdb
