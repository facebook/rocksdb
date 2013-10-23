// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "util/coding.h"

#include <list>
#include <deque>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <cstdio>

namespace rocksdb {

struct BlobChunk {
  uint32_t bucket_id;
  uint32_t offset; // in blocks
  uint32_t size; // in blocks
  BlobChunk() {}
  BlobChunk(uint32_t bucket_id, uint32_t offset, uint32_t size) :
    bucket_id(bucket_id), offset(offset), size(size) {}

  // returns true if it's immediately before chunk
  bool ImmediatelyBefore(const BlobChunk& chunk) const;
  // returns true if chunks overlap
  bool Overlap(const BlobChunk &chunk) const;
};

// We represent each Blob as a string in format:
// bucket_id offset size|bucket_id offset size...
// The string can be used to reference the Blob stored on external
// device/file
// Not thread-safe!
struct Blob {
  // Generates the string
  std::string ToString() const;
  // Parses the previously generated string
  explicit Blob(const std::string& blob);
  // Creates unfragmented Blob
  Blob(uint32_t bucket_id, uint32_t offset, uint32_t size) {
    SetOneChunk(bucket_id, offset, size);
  }
  Blob() {}

  void SetOneChunk(uint32_t bucket_id, uint32_t offset, uint32_t size) {
    chunks.clear();
    chunks.push_back(BlobChunk(bucket_id, offset, size));
  }

  uint32_t Size() const { // in blocks
    uint32_t ret = 0;
    for (auto chunk : chunks) {
      ret += chunk.size;
    }
    assert(ret > 0);
    return ret;
  }

  // bucket_id, offset, size
  std::vector<BlobChunk> chunks;
};

// Keeps a list of free chunks
// NOT thread-safe. Externally synchronized
class FreeList {
 public:
  FreeList() :
    free_blocks_(0) {}
  ~FreeList() {}

  // Allocates a a blob. Stores the allocated blob in
  // 'blob'. Returns non-OK status if it failed to allocate.
  // Thread-safe
  Status Allocate(uint32_t blocks, Blob* blob);
  // Frees the blob for reuse. Thread-safe
  Status Free(const Blob& blob);

  // returns true if blob is overlapping with any of the
  // chunks stored in free list
  bool Overlap(const Blob &blob) const;

 private:
  std::deque<BlobChunk> fifo_free_chunks_;
  uint32_t free_blocks_;
  mutable port::Mutex mutex_;
};

// thread-safe
class BlobStore {
 public:
   // directory - wherever the blobs should be stored. It will be created
   //   if missing
   // block_size - self explanatory
   // blocks_per_bucket - how many blocks we want to keep in one bucket.
   //   Bucket is a device or a file that we use to store the blobs.
   //   If we don't have enough blocks to allocate a new blob, we will
   //   try to create a new file or device.
   // max_buckets - maximum number of buckets BlobStore will create
   //   BlobStore max size in bytes is
   //     max_buckets * blocks_per_bucket * block_size
   // env - env for creating new files
  BlobStore(const std::string& directory,
            uint64_t block_size,
            uint32_t blocks_per_bucket,
            uint32_t max_buckets,
            Env* env);
  ~BlobStore();

  // Allocates space for value.size bytes (rounded up to be multiple of
  // block size) and writes value.size bytes from value.data to a backing store.
  // Sets Blob blob that can than be used for addressing the
  // stored value. Returns non-OK status on error.
  Status Put(const Slice& value, Blob* blob);
  // Value needs to have enough space to store all the loaded stuff.
  // This function is thread safe!
  Status Get(const Blob& blob, std::string* value) const;
  // Frees the blob for reuse, but does not delete the data
  // on the backing store.
  Status Delete(const Blob& blob);
  // Sync all opened files that are modified
  Status Sync();

 private:
  const std::string directory_;
  // block_size_ is uint64_t because when we multiply with
  // blocks_size_ we want the result to be uint64_t or
  // we risk overflowing
  const uint64_t block_size_;
  const uint32_t blocks_per_bucket_;
  Env* env_;
  EnvOptions storage_options_;
  // protected by free_list_mutex_
  FreeList free_list_;
  // free_list_mutex_ is locked BEFORE buckets_mutex_
  mutable port::Mutex free_list_mutex_;
  // protected by buckets_mutex_
  // array of buckets
  unique_ptr<RandomRWFile>* buckets_;
  // number of buckets in the array
  uint32_t buckets_size_;
  uint32_t max_buckets_;
  mutable port::Mutex buckets_mutex_;

  // Calls FreeList allocate. If free list can't allocate
  // new blob, creates new bucket and tries again
  // Thread-safe
  Status Allocate(uint32_t blocks, Blob* blob);

  // Creates a new backing store and adds all the blocks
  // from the new backing store to the free list
  Status CreateNewBucket();
};

} // namespace rocksdb
