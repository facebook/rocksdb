//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "table/block.h"
#include "table/format.h"

namespace rocksdb {
class BlockFetcher {
 public:
  // Read the block identified by "handle" from "file".
  // The only relevant option is options.verify_checksums for now.
  // On failure return non-OK.
  // On success fill *result and return OK - caller owns *result
  // @param compression_dict Data for presetting the compression library's
  //    dictionary.
  BlockFetcher(RandomAccessFileReader* file,
               FilePrefetchBuffer* prefetch_buffer, const Footer& footer,
               const ReadOptions& read_options, const BlockHandle& handle,
               BlockContents* contents,
               const ImmutableCFOptions& ioptions,
               bool do_uncompress, const Slice& compression_dict,
               const PersistentCacheOptions& cache_options)
      : file_(file),
        prefetch_buffer_(prefetch_buffer),
        footer_(footer),
        read_options_(read_options),
        handle_(handle),
        contents_(contents),
        ioptions_(ioptions),
        do_uncompress_(do_uncompress),
        compression_dict_(compression_dict),
        cache_options_(cache_options) {}
  Status ReadBlockContents();

 private:
  static const uint32_t kDefaultStackBufferSize = 5000;

  RandomAccessFileReader* file_;
  FilePrefetchBuffer* prefetch_buffer_;
  const Footer& footer_;
  const ReadOptions read_options_;
  const BlockHandle& handle_;
  BlockContents* contents_;
  const ImmutableCFOptions& ioptions_;
  bool do_uncompress_;
  const Slice& compression_dict_;
  const PersistentCacheOptions& cache_options_;
  Status status_;
  Slice slice_;
  char* used_buf_ = nullptr;
  size_t block_size_;
  std::unique_ptr<char[]> heap_buf_;
  char stack_buf_[kDefaultStackBufferSize];
  bool got_from_prefetch_buffer_ = false;
  rocksdb::CompressionType compression_type;

  // return true if found
  bool TryGetUncompressBlockFromPersistentCache();
  // return true if found
  bool TryGetFromPrefetchBuffer();
  bool TryGetCompressedBlockFromPersistentCache();
  void PrepareBufferForBlockFromFile();
  void GetBlockContents();
  void InsertCompressedBlockToPersistentCacheIfNeeded();
  void InsertUncompressedBlockToPersistentCacheIfNeeded();
  void CheckBlockChecksum();
};
}  // namespace rocksdb
