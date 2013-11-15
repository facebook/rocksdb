// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/blob_store.h"

namespace rocksdb {

using namespace std;

// BlobChunk
bool BlobChunk::ImmediatelyBefore(const BlobChunk& chunk) const {
  // overlapping!?
  assert(!Overlap(chunk));
  // size == 0 is a marker, not a block
  return size != 0 &&
    bucket_id == chunk.bucket_id &&
    offset + size == chunk.offset;
}

bool BlobChunk::Overlap(const BlobChunk &chunk) const {
  return size != 0 && chunk.size != 0 && bucket_id == chunk.bucket_id &&
    ((offset >= chunk.offset && offset < chunk.offset + chunk.size) ||
     (chunk.offset >= offset && chunk.offset < offset + size));
}

// Blob
string Blob::ToString() const {
  string ret;
  for (auto chunk : chunks) {
    PutFixed32(&ret, chunk.bucket_id);
    PutFixed32(&ret, chunk.offset);
    PutFixed32(&ret, chunk.size);
  }
  return ret;
}

Blob::Blob(const std::string& blob) {
  for (uint32_t i = 0; i < blob.size(); ) {
    uint32_t t[3] = {0};
    for (int j = 0; j < 3 && i + sizeof(uint32_t) - 1 < blob.size();
                    ++j, i += sizeof(uint32_t)) {
      t[j] = DecodeFixed32(blob.data() + i);
    }
    chunks.push_back(BlobChunk(t[0], t[1], t[2]));
  }
}

// FreeList
Status FreeList::Free(const Blob& blob) {
  // add it back to the free list
  for (auto chunk : blob.chunks) {
    free_blocks_ += chunk.size;
    if (fifo_free_chunks_.size() &&
        fifo_free_chunks_.back().ImmediatelyBefore(chunk)) {
      fifo_free_chunks_.back().size += chunk.size;
    } else {
      fifo_free_chunks_.push_back(chunk);
    }
  }

  return Status::OK();
}

Status FreeList::Allocate(uint32_t blocks, Blob* blob) {
  if (free_blocks_ < blocks) {
    return Status::Incomplete("");
  }

  blob->chunks.clear();
  free_blocks_ -= blocks;

  while (blocks > 0) {
    assert(fifo_free_chunks_.size() > 0);
    auto& front = fifo_free_chunks_.front();
    if (front.size > blocks) {
      blob->chunks.push_back(BlobChunk(front.bucket_id, front.offset, blocks));
      front.offset += blocks;
      front.size -= blocks;
      blocks = 0;
    } else {
      blob->chunks.push_back(front);
      blocks -= front.size;
      fifo_free_chunks_.pop_front();
    }
  }
  assert(blocks == 0);

  return Status::OK();
}

bool FreeList::Overlap(const Blob &blob) const {
  for (auto chunk : blob.chunks) {
    for (auto itr = fifo_free_chunks_.begin();
         itr != fifo_free_chunks_.end();
         ++itr) {
      if (itr->Overlap(chunk)) {
        return true;
      }
    }
  }
  return false;
}

// BlobStore
BlobStore::BlobStore(const string& directory,
                     uint64_t block_size,
                     uint32_t blocks_per_bucket,
                     uint32_t max_buckets,
                     Env* env) :
    directory_(directory),
    block_size_(block_size),
    blocks_per_bucket_(blocks_per_bucket),
    env_(env),
    max_buckets_(max_buckets) {
  env_->CreateDirIfMissing(directory_);

  storage_options_.use_mmap_writes = false;
  storage_options_.use_mmap_reads = false;

  buckets_size_ = 0;
  buckets_ = new unique_ptr<RandomRWFile>[max_buckets_];

  CreateNewBucket();
}

BlobStore::~BlobStore() {
  // TODO we don't care about recovery for now
  delete [] buckets_;
}

Status BlobStore::Put(const Slice& value, Blob* blob) {
  // convert size to number of blocks
  Status s = Allocate((value.size() + block_size_ - 1) / block_size_, blob);
  if (!s.ok()) {
    return s;
  }
  auto size_left = (uint64_t) value.size();

  uint64_t offset = 0; // in bytes, not blocks
  for (auto chunk : blob->chunks) {
    uint64_t write_size = min(chunk.size * block_size_, size_left);
    assert(chunk.bucket_id < buckets_size_);
    s = buckets_[chunk.bucket_id].get()->Write(chunk.offset * block_size_,
                                               Slice(value.data() + offset,
                                                     write_size));
    if (!s.ok()) {
      Delete(*blob);
      return s;
    }
    offset += write_size;
    size_left -= write_size;
    if (write_size < chunk.size * block_size_) {
      // if we have any space left in the block, fill it up with zeros
      string zero_string(chunk.size * block_size_ - write_size, 0);
      s = buckets_[chunk.bucket_id].get()->Write(chunk.offset * block_size_ +
                                                    write_size,
                                                 Slice(zero_string));
    }
  }

  if (size_left > 0) {
    Delete(*blob);
    return Status::IOError("Tried to write more data than fits in the blob");
  }

  return Status::OK();
}

Status BlobStore::Get(const Blob& blob,
                      string* value) const {
  {
    // assert that it doesn't overlap with free list
    // it will get compiled out for release
    MutexLock l(&free_list_mutex_);
    assert(!free_list_.Overlap(blob));
  }

  value->resize(blob.Size() * block_size_);

  uint64_t offset = 0; // in bytes, not blocks
  for (auto chunk : blob.chunks) {
    Slice result;
    assert(chunk.bucket_id < buckets_size_);
    Status s;
    s = buckets_[chunk.bucket_id].get()->Read(chunk.offset * block_size_,
                                              chunk.size * block_size_,
                                              &result,
                                              &value->at(offset));
    if (!s.ok() || result.size() < chunk.size * block_size_) {
      value->clear();
      return Status::IOError("Could not read in from file");
    }
    offset += chunk.size * block_size_;
  }

  // remove the '\0's at the end of the string
  value->erase(find(value->begin(), value->end(), '\0'), value->end());

  return Status::OK();
}

Status BlobStore::Delete(const Blob& blob) {
  MutexLock l(&free_list_mutex_);
  return free_list_.Free(blob);
}

Status BlobStore::Sync() {
  for (size_t i = 0; i < buckets_size_; ++i) {
    Status s = buckets_[i].get()->Sync();
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status BlobStore::Allocate(uint32_t blocks, Blob* blob) {
  MutexLock l(&free_list_mutex_);
  Status s;

  s = free_list_.Allocate(blocks, blob);
  if (!s.ok()) {
    s = CreateNewBucket();
    if (!s.ok()) {
      return s;
    }
    s = free_list_.Allocate(blocks, blob);
  }

  return s;
}

// called with free_list_mutex_ held
Status BlobStore::CreateNewBucket() {
  MutexLock l(&buckets_mutex_);

  if (buckets_size_ >= max_buckets_) {
    return Status::IOError("Max size exceeded\n");
  }

  int new_bucket_id = buckets_size_;

  char fname[200];
  sprintf(fname, "%s/%d.bs", directory_.c_str(), new_bucket_id);

  Status s = env_->NewRandomRWFile(string(fname),
                                   &buckets_[new_bucket_id],
                                   storage_options_);
  if (!s.ok()) {
    return s;
  }

  // whether Allocate succeeds or not, does not affect the overall correctness
  // of this function - calling Allocate is really optional
  // (also, tmpfs does not support allocate)
  buckets_[new_bucket_id].get()->Allocate(0, block_size_ * blocks_per_bucket_);

  buckets_size_ = new_bucket_id + 1;

  return free_list_.Free(Blob(new_bucket_id, 0, blocks_per_bucket_));
}

} // namespace rocksdb
