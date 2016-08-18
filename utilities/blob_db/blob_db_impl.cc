// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "utilities/blob_db/blob_db_impl.h"

#include "db/filename.h"
#include "db/write_batch_internal.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/stackable_db.h"
#include "table/block.h"
#include "table/block_based_table_builder.h"
#include "table/block_builder.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/instrumented_mutex.h"

namespace rocksdb {

namespace {
int kBlockBasedTableVersionFormat = 2;
}  // namespace


const std::string BlobDBImpl::kFileName = "blob_log";
const size_t BlobDBImpl::kBlockHeaderSize = 8;
const size_t BlobDBImpl::kBytesPerSync = 1024 * 1024 * 128;

BlobDBImpl::BlobDBImpl(DB* db, const BlobDBOptions& blob_db_options)
    : BlobDB(db),
      bdb_options_(blob_db_options),
      ioptions_(db->GetOptions()),
      writer_offset_(0),
      next_sync_offset_(kBytesPerSync) {}

Status BlobDBImpl::Open() {

  unique_ptr<WritableFile> wfile;
  EnvOptions env_options(db_->GetOptions());
  Status s = ioptions_.env->NewWritableFile(db_->GetName() + "/" + kFileName,
                                            &wfile, env_options);
  if (!s.ok()) {
    return s;
  }
  file_writer_.reset(new WritableFileWriter(std::move(wfile), env_options));

  // Write version
  std::string version;
  PutFixed64(&version, 0);
  s = file_writer_->Append(Slice(version));
  if (!s.ok()) {
    return s;
  }
  writer_offset_ += version.size();

  std::unique_ptr<RandomAccessFile> rfile;
  s = ioptions_.env->NewRandomAccessFile(db_->GetName() + "/" + kFileName,
                                         &rfile, env_options);
  if (!s.ok()) {
    return s;
  }
  file_reader_.reset(new RandomAccessFileReader(std::move(rfile)));
  return s;
}

Status BlobDBImpl::Put(const WriteOptions& options, const Slice& key,
                   const Slice& value) {
  BlockBuilder block_builder(1, false);
  block_builder.Add(key, value);

  CompressionType compression = CompressionType::kLZ4Compression;
  CompressionOptions compression_opts;

  Slice block_contents;
  std::string compression_output;

  block_contents = CompressBlock(block_builder.Finish(), compression_opts,
                                 &compression, kBlockBasedTableVersionFormat,
                                 Slice() /* dictionary */, &compression_output);

  char header[kBlockHeaderSize];
  char trailer[kBlockTrailerSize];
  trailer[0] = compression;
  auto crc = crc32c::Value(block_contents.data(), block_contents.size());
  crc = crc32c::Extend(crc, trailer, 1);  // Extend to cover block type
  EncodeFixed32(trailer + 1, crc32c::Mask(crc));

  BlockHandle handle;
  std::string index_entry;
  Status s;
  {
    InstrumentedMutexLock l(&mutex_);
    auto raw_block_size = block_contents.size();
    EncodeFixed64(header, raw_block_size);
    s = file_writer_->Append(Slice(header, kBlockHeaderSize));
    writer_offset_ += kBlockHeaderSize;
    if (s.ok()) {
      handle.set_offset(writer_offset_);
      handle.set_size(raw_block_size);
      s = file_writer_->Append(block_contents);
    }
    if (s.ok()) {
      s = file_writer_->Append(Slice(trailer, kBlockTrailerSize));
    }
    if (s.ok()) {
      s = file_writer_->Flush();
    }
    if (s.ok() && writer_offset_ > next_sync_offset_) {
      // Sync every kBytesPerSync. This is a hacky way to limit unsynced data.
      next_sync_offset_ += kBytesPerSync;
      s = file_writer_->Sync(db_->GetOptions().use_fsync);
    }
    if (s.ok()) {
      writer_offset_ += block_contents.size() + kBlockTrailerSize;
      // Put file number
      PutVarint64(&index_entry, 0);
      handle.EncodeTo(&index_entry);
      s = db_->Put(options, key, index_entry);
    }
  }
  return s;
}

Status BlobDBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  std::string index_entry;
  s = db_->Get(options, key, &index_entry);
  if (!s.ok()) {
    return s;
  }
  BlockHandle handle;
  Slice index_entry_slice(index_entry);
  uint64_t file_number;
  if (!GetVarint64(&index_entry_slice, &file_number)) {
    return Status::Corruption();
  }
  assert(file_number == 0);
  s = handle.DecodeFrom(&index_entry_slice);
  if (!s.ok()) {
    return s;
  }
  Footer footer(0, kBlockBasedTableVersionFormat);
  BlockContents contents;
  s = ReadBlockContents(file_reader_.get(), footer, options, handle, &contents,
                        ioptions_);
  if (!s.ok()) {
    return s;
  }
  Block block(std::move(contents));
  BlockIter bit;
  InternalIterator* it = block.NewIterator(nullptr, &bit);
  it->SeekToFirst();
  if (!it->status().ok()) {
    return it->status();
  }
  *value = it->value().ToString();
  return s;
}
}  // namespace rocksdb
