// Copyright (c) 2021, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <stdint.h>

#include <memory>
#include <string>

#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "table/block_based/block.h"
#include "table/block_based/parsed_full_filter_block.h"
#include "table/format.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

void CacheBlockContentsHelperCB(Cache::SizeCallback* size_cb,
                                Cache::SaveToCallback* saveto_cb,
                                Cache::DeletionCallback* del_cb) {
  if (size_cb) {
    *size_cb = [](void* obj) -> size_t {
      BlockContents* ptr = reinterpret_cast<BlockContents*>(obj);
      return ptr->data.size();
    };
  }

  if (saveto_cb) {
    *saveto_cb = [](void* obj, size_t offset, size_t size,
                    void* out) -> Status {
      BlockContents* ptr = reinterpret_cast<BlockContents*>(obj);
      const char* buf = ptr->data.data();
      assert(size == ptr->data.size());
      assert(offset == 0);
      memcpy(out, buf, size);
      return Status::OK();
    };
  }

  if (del_cb) {
    *del_cb = [](const Slice& /*key*/, void* obj) -> void {
      delete reinterpret_cast<BlockContents*>(obj);
    };
  }
}

void CacheBlockHelperCB(Cache::SizeCallback* size_cb,
                        Cache::SaveToCallback* saveto_cb,
                        Cache::DeletionCallback* del_cb) {
  if (size_cb) {
    *size_cb = [](void* obj) -> size_t {
      Block* ptr = reinterpret_cast<Block*>(obj);
      return ptr->size();
    };
  }

  if (saveto_cb) {
    *saveto_cb = [](void* obj, size_t offset, size_t size,
                    void* out) -> Status {
      Block* ptr = reinterpret_cast<Block*>(obj);
      const char* buf = ptr->data();
      assert(size == ptr->size());
      assert(offset == 0);
      memcpy(out, buf, size);
      return Status::OK();
    };
  }

  if (del_cb) {
    *del_cb = [](const Slice& /*key*/, void* obj) -> void {
      delete reinterpret_cast<Block*>(obj);
    };
  }
}

void CacheUncompressionDictHelperCB(Cache::SizeCallback* size_cb,
                                    Cache::SaveToCallback* saveto_cb,
                                    Cache::DeletionCallback* del_cb) {
  if (size_cb) {
    *size_cb = [](void* obj) -> size_t {
      UncompressionDict* ptr = reinterpret_cast<UncompressionDict*>(obj);
      return ptr->slice_.size();
    };
  }

  if (saveto_cb) {
    *saveto_cb = [](void* obj, size_t offset, size_t size,
                    void* out) -> Status {
      UncompressionDict* ptr = reinterpret_cast<UncompressionDict*>(obj);
      const char* buf = ptr->slice_.data();
      assert(size == ptr->slice_.size());
      assert(offset == 0);
      memcpy(out, buf, size);
      return Status::OK();
    };
  }

  if (del_cb) {
    *del_cb = [](const Slice& /*key*/, void* obj) -> void {
      delete reinterpret_cast<UncompressionDict*>(obj);
    };
  }
}
void CacheParsedFullFilterBlockHelperCB(Cache::SizeCallback* size_cb,
                                        Cache::SaveToCallback* saveto_cb,
                                        Cache::DeletionCallback* del_cb) {
  if (size_cb) {
    *size_cb = [](void* obj) -> size_t {
      ParsedFullFilterBlock* ptr =
          reinterpret_cast<ParsedFullFilterBlock*>(obj);
      return ptr->GetBlockContentsData().size();
    };
  }

  if (saveto_cb) {
    *saveto_cb = [](void* obj, size_t offset, size_t size,
                    void* out) -> Status {
      ParsedFullFilterBlock* ptr =
          reinterpret_cast<ParsedFullFilterBlock*>(obj);
      const char* buf = ptr->GetBlockContentsData().data();
      assert(size == ptr->GetBlockContentsData().size());
      assert(offset == 0);
      memcpy(out, buf, size);
      return Status::OK();
    };
  }

  if (del_cb) {
    *del_cb = [](const Slice& /*key*/, void* obj) -> void {
      delete reinterpret_cast<ParsedFullFilterBlock*>(obj);
    };
  }
}

Status CreateBlockContentsBuffCB(void* buf, size_t size, void** out_obj,
                                 size_t* charge) {
  assert(buf != nullptr);
  std::unique_ptr<char[]> buf_data(new char[size]());
  memcpy(buf_data.get(), buf, size);
  BlockContents* bc = new BlockContents(std::move(buf_data), size);
  *out_obj = reinterpret_cast<void*>(bc);
  *charge = size;
  return Status::OK();
}

Status CreateBlockBuffCB(void* buf, size_t size, void** out_obj,
                         size_t* charge) {
  assert(buf != nullptr);
  std::unique_ptr<char[]> buf_data(new char[size]());
  memcpy(buf_data.get(), buf, size);
  BlockContents bc = BlockContents(std::move(buf_data), size);
  Block* b_ptr = new Block(std::move(bc));
  *out_obj = reinterpret_cast<void*>(b_ptr);
  *charge = size;
  return Status::OK();
}

Status CreateUncompressionDictBuffCB(void* buf, size_t size, void** out_obj,
                                     size_t* charge, bool using_zstd) {
  assert(buf != nullptr);
  std::unique_ptr<char[]> buf_data(new char[size]());
  memcpy(buf_data.get(), buf, size);
  BlockContents bc = BlockContents(std::move(buf_data), size);
  UncompressionDict* ucd_ptr =
      new UncompressionDict(bc.data, std::move(bc.allocation), using_zstd);
  *out_obj = reinterpret_cast<void*>(ucd_ptr);
  *charge = size;
  return Status::OK();
}

Status CreateParsedFullFilterBlockBuffCB(void* buf, size_t size, void** out_obj,
                                         size_t* charge,
                                         const FilterPolicy* filter_policy) {
  assert(buf != nullptr);
  std::unique_ptr<char[]> buf_data(new char[size]());
  memcpy(buf_data.get(), buf, size);
  BlockContents bc = BlockContents(std::move(buf_data), size);
  ParsedFullFilterBlock* b_ptr =
      new ParsedFullFilterBlock(filter_policy, std::move(bc));
  *out_obj = reinterpret_cast<void*>(b_ptr);
  *charge = size;
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
