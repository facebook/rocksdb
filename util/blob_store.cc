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
FreeList::FreeList() {
  // We add (0, 0, 0) blob because it makes our life easier and
  // code cleaner. (0, 0, 0) is always in the list so we can
  // guarantee that free_chunks_list_ != nullptr, which avoids
  // lots of unnecessary ifs
  free_chunks_list_ = (FreeChunk *)malloc(sizeof(FreeChunk));
  free_chunks_list_->chunk = BlobChunk(0, 0, 0);
  free_chunks_list_->next = nullptr;
}

FreeList::~FreeList() {
  while (free_chunks_list_ != nullptr) {
    FreeChunk* t = free_chunks_list_;
    free_chunks_list_ = free_chunks_list_->next;
    free(t);
  }
}

Status FreeList::Free(const Blob& blob) {
  MutexLock l(&mutex_);

  // add it back to the free list
  for (auto chunk : blob.chunks) {
    FreeChunk* itr = free_chunks_list_;

    // find a node AFTER which we'll add the block
    for ( ; itr->next != nullptr && itr->next->chunk <= chunk;
            itr = itr->next) {
    }

    // try to merge with previous block
    if (itr->chunk.ImmediatelyBefore(chunk)) {
      // merge
      itr->chunk.size += chunk.size;
    } else {
      // Insert the block after itr
      FreeChunk* t = (FreeChunk*)malloc(sizeof(FreeChunk));
      if (t == nullptr) {
        throw runtime_error("Malloc failed");
      }
      t->chunk = chunk;
      t->next = itr->next;
      itr->next = t;
      itr = t;
    }

    // try to merge with the next block
    if (itr->next != nullptr &&
        itr->chunk.ImmediatelyBefore(itr->next->chunk)) {
      FreeChunk *tobedeleted = itr->next;
      itr->chunk.size += itr->next->chunk.size;
      itr->next = itr->next->next;
      free(tobedeleted);
    }
  }

  return Status::OK();
}

Status FreeList::Allocate(uint32_t blocks, Blob* blob) {
  MutexLock l(&mutex_);
  FreeChunk** best_fit_node = nullptr;

  // Find the smallest free chunk whose size is greater or equal to blocks
  for (FreeChunk** itr = &free_chunks_list_; (*itr) != nullptr;
       itr = &((*itr)->next)) {
    if ((*itr)->chunk.size >= blocks &&
        (best_fit_node == nullptr ||
         (*best_fit_node)->chunk.size > (*itr)->chunk.size)) {
      best_fit_node = itr;
    }
  }

  if (best_fit_node == nullptr || *best_fit_node == nullptr) {
    // Not enough memory
    return Status::Incomplete("");
  }

  blob->SetOneChunk((*best_fit_node)->chunk.bucket_id,
                    (*best_fit_node)->chunk.offset,
                    blocks);

  if ((*best_fit_node)->chunk.size > blocks) {
    // just shorten best_fit_node
    (*best_fit_node)->chunk.offset += blocks;
    (*best_fit_node)->chunk.size -= blocks;
  } else {
    assert(blocks == (*best_fit_node)->chunk.size);
    // delete best_fit_node
    FreeChunk* t = *best_fit_node;
    (*best_fit_node) = (*best_fit_node)->next;
    free(t);
  }

  return Status::OK();
}

bool FreeList::Overlap(const Blob &blob) const {
  MutexLock l(&mutex_);
  for (auto chunk : blob.chunks) {
    for (auto itr = free_chunks_list_; itr != nullptr; itr = itr->next) {
      if (itr->chunk.Overlap(chunk)) {
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
                     Env* env) :
    directory_(directory),
    block_size_(block_size),
    blocks_per_bucket_(blocks_per_bucket),
    env_(env) {
  env_->CreateDirIfMissing(directory_);

  storage_options_.use_mmap_writes = false;
  storage_options_.use_mmap_reads = false;

  CreateNewBucket();
}

BlobStore::~BlobStore() {
  // TODO we don't care about recovery for now
}

Status BlobStore::Put(const char* value, uint64_t size, Blob* blob) {
  // convert size to number of blocks
  Status s = Allocate((size + block_size_ - 1) / block_size_, blob);
  if (!s.ok()) {
    return s;
  }

  uint64_t offset = 0; // in bytes, not blocks
  for (auto chunk : blob->chunks) {
    uint64_t write_size = min(chunk.size * block_size_, size);
    assert(chunk.bucket_id < buckets_.size());
    s = buckets_[chunk.bucket_id].get()->Write(chunk.offset * block_size_,
                                               Slice(value + offset,
                                                     write_size));
    if (!s.ok()) {
      Delete(*blob);
      return s;
    }
    offset += write_size;
    size -= write_size;
    if (write_size < chunk.size * block_size_) {
      // if we have any space left in the block, fill it up with zeros
      string zero_string(chunk.size * block_size_ - write_size, 0);
      s = buckets_[chunk.bucket_id].get()->Write(chunk.offset * block_size_ +
                                                    write_size,
                                                 Slice(zero_string));
    }
  }

  if (size > 0) {
    Delete(*blob);
    return Status::IOError("Tried to write more data than fits in the blob");
  }

  return Status::OK();
}

Status BlobStore::Get(const Blob& blob,
                      string* value) const {
  // assert that it doesn't overlap with free list
  // it will get compiled out for release
  assert(!free_list_.Overlap(blob));

  uint32_t total_size = 0; // in blocks
  for (auto chunk : blob.chunks) {
    total_size += chunk.size;
  }
  assert(total_size > 0);
  value->resize(total_size * block_size_);

  uint64_t offset = 0; // in bytes, not blocks
  for (auto chunk : blob.chunks) {
    Slice result;
    assert(chunk.bucket_id < buckets_.size());
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
  return free_list_.Free(blob);
}

Status BlobStore::Allocate(uint32_t blocks, Blob* blob) {
  // TODO we don't currently support fragmented blobs
  MutexLock l(&allocate_mutex_);
  assert(blocks <= blocks_per_bucket_);
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

Status BlobStore::CreateNewBucket() {
  int new_bucket_id;
  new_bucket_id = buckets_.size();
  buckets_.push_back(unique_ptr<RandomRWFile>());

  char fname[200];
  sprintf(fname, "%s/%d.bs", directory_.c_str(), new_bucket_id);

  Status s = env_->NewRandomRWFile(string(fname),
                                   &buckets_[new_bucket_id],
                                   storage_options_);
  if (!s.ok()) {
    buckets_.erase(buckets_.begin() + new_bucket_id);
    return s;
  }

  // tmpfs does not support allocate
  buckets_[new_bucket_id].get()->Allocate(0, block_size_ * blocks_per_bucket_);

  return free_list_.Free(Blob(new_bucket_id, 0, blocks_per_bucket_));
}

} // namespace rocksdb
