//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <array>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "db/version_edit.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "util/atomic.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

class BlockBuilder;
class BlockHandle;
class WritableFile;
struct BlockBasedTableOptions;

extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;

class BlockBasedTableBuilder : public TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  BlockBasedTableBuilder(const BlockBasedTableOptions& table_options,
                         const TableBuilderOptions& table_builder_options,
                         WritableFileWriter* file);

  // No copying allowed
  BlockBasedTableBuilder(const BlockBasedTableBuilder&) = delete;
  BlockBasedTableBuilder& operator=(const BlockBasedTableBuilder&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~BlockBasedTableBuilder();

  // Add key,value to the table being constructed.
  // REQUIRES: Unless key has type kTypeRangeDeletion, key is after any
  //           previously added non-kTypeRangeDeletion key according to
  //           comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  // Return non-ok iff some error has been detected.
  Status status() const override;

  // Return non-ok iff some error happens during IO.
  IOStatus io_status() const override;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  bool IsEmpty() const override;

  uint64_t PreCompressionSize() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

  // Estimated size of the file generated so far. This is used when
  // FileSize() cannot estimate final SST size, e.g. parallel compression
  // is enabled.
  uint64_t EstimatedFileSize() const override;

  // Get the size of the "tail" part of a SST file. "Tail" refers to
  // all blocks after data blocks till the end of the SST file.
  uint64_t GetTailSize() const override;

  bool NeedCompact() const override;

  // Get table properties
  TableProperties GetTableProperties() const override;

  // Get file checksum
  std::string GetFileChecksum() const override;

  // Get file checksum function name
  const char* GetFileChecksumFuncName() const override;

  void SetSeqnoTimeTableProperties(const SeqnoToTimeMapping& relevant_mapping,
                                   uint64_t oldest_ancestor_time) override;

 private:
  bool ok() const;

  // Transition state from buffered to unbuffered if the conditions are met. See
  // `Rep::State` API comment for details of the states.
  // REQUIRES: `rep_->state == kBuffered`
  void MaybeEnterUnbuffered(const Slice* first_key_in_next_block);

  // Try to keep some parallel-specific code separate to improve hot code
  // locality for non-parallel case
  void EmitBlock(std::string& uncompressed,
                 const Slice& last_key_in_current_block,
                 const Slice* first_key_in_next_block);
  void EmitBlockForParallel(std::string& uncompressed,
                            const Slice& last_key_in_current_block,
                            const Slice* first_key_in_next_block);

  // Compress and write block content to the file, from a single-threaded
  // context
  void WriteBlock(const Slice& block_contents, BlockHandle* handle,
                  BlockType block_type);
  // Directly write data to the file.
  void WriteMaybeCompressedBlock(
      const Slice& block_contents, CompressionType, BlockHandle* handle,
      BlockType block_type, const Slice* uncompressed_block_data = nullptr);
  IOStatus WriteMaybeCompressedBlockImpl(
      const Slice& block_contents, CompressionType, BlockHandle* handle,
      BlockType block_type, const Slice* uncompressed_block_data = nullptr);

  void SetupCacheKeyPrefix(const TableBuilderOptions& tbo);

  template <typename TBlocklike>
  Status InsertBlockInCache(const Slice& block_contents,
                            const BlockHandle* handle, BlockType block_type);

  Status InsertBlockInCacheHelper(const Slice& block_contents,
                                  const BlockHandle* handle,
                                  BlockType block_type);

  Status InsertBlockInCompressedCache(const Slice& block_contents,
                                      const CompressionType type,
                                      const BlockHandle* handle);

  void WriteFilterBlock(MetaIndexBuilder* meta_index_builder);
  void WriteIndexBlock(MetaIndexBuilder* meta_index_builder,
                       BlockHandle* index_block_handle);
  void WritePropertiesBlock(MetaIndexBuilder* meta_index_builder);
  void WriteCompressionDictBlock(MetaIndexBuilder* meta_index_builder);
  void WriteRangeDelBlock(MetaIndexBuilder* meta_index_builder);
  void WriteFooter(BlockHandle& metaindex_block_handle,
                   BlockHandle& index_block_handle);

  struct Rep;
  class BlockBasedTablePropertiesCollectorFactory;
  class BlockBasedTablePropertiesCollector;
  std::unique_ptr<Rep> rep_;
  struct WorkingAreaPair;
  struct ParallelCompressionRep;

  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  void Flush(const Slice* first_key_in_next_block);

  // Some compression libraries fail when the uncompressed size is bigger than
  // int. If uncompressed size is bigger than kCompressionSizeLimit, don't
  // compress it
  const uint64_t kCompressionSizeLimit = std::numeric_limits<int>::max();

  // Code for a "parallel compression" worker thread, which can really do SST
  // writes and block compressions alternately.
  void BGWorker(WorkingAreaPair& working_area);

  // Given uncompressed block content, try to compress it and return result and
  // compression type
  Status CompressAndVerifyBlock(const Slice& uncompressed_block_data,
                                bool is_data_block,
                                WorkingAreaPair& working_area,
                                GrowableBuffer* compressed_output,
                                CompressionType* result_compression_type);

  // If configured, start worker threads for parallel compression
  void MaybeStartParallelCompression();

  // Stop worker threads for parallel compression
  void StopParallelCompression(bool abort);
};

}  // namespace ROCKSDB_NAMESPACE
