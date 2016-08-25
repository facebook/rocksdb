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
#include "table/meta_blocks.h"
#include <chrono>


// create multiple writers.
// have a mutex to select the writer

// write down the footer with # blobs.
// create an option which is just based on size of disk
// create GC thread which evicts based on et-lt
// create GC thread which evicts based on size of disk like FIFO
// close the file after size has been reached and create a new file.
// on startup have a recovery function which reads all files.
// instead of TTL, use timestamp of the data.

namespace rocksdb {

namespace {
int kBlockBasedTableVersionFormat = 2;
}  // namespace

const std::string BlobDBImpl::kFileName = "blob_log";
const size_t BlobDBImpl::kBlockHeaderSize = 8;
const size_t BlobDBImpl::kBytesPerSync = 1024 * 1024 * 128;

extern const uint64_t kBlockBasedTableMagicNumber;


namespace 
{

void createHeader(const BlobDBOptions& bdb_options, Slice& header_str,
  std::pair<uint64_t, uint64_t> *ttl_range) {

  PropertyBlockBuilder property_block_builder;
  property_block_builder.Add("magic", kBlockBasedTableMagicNumber);
  property_block_builder.Add("version", 0);
  property_block_builder.Add("has_ttl", bdb_options.has_ttl ? 1 : 0);
  property_block_builder.Add("compression", (uint64_t)CompressionType::kLZ4Compression);

  if (bdb_options.has_ttl) {
    property_block_builder.Add("earliest", (ttl_range) ? ttl_range->first : 0);
    property_block_builder.Add("latest", (ttl_range) ? ttl_range->second : 0);
  }

  header_str = property_block_builder.Finish();
}

}

Status BlobDBImpl::openNewFile()
{
  InstrumentedMutexLock l(&mutex_);

  next_file_number_++;

  unique_ptr<WritableFile> wfile;
  EnvOptions env_options(db_->GetOptions());
  Status s = ioptions_.env->NewWritableFile(BlobFileName(blob_dir_, next_file_number_.load()),
                                            &wfile, env_options);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<BlobFile> bfile(new BlobFile(blob_dir_, next_file_number_.load()));
  blob_files_.insert(std::make_pair(next_file_number_.load(), std::move(bfile)));

  file_writer_.reset(new WritableFileWriter(std::move(wfile), env_options));

  // Write header
  Slice header_slice;
  createHeader(bdb_options_, header_slice, nullptr);
  s = file_writer_->Append(header_slice);

  if (!s.ok()) {
    return s;
  }
  writer_offset_ += header_slice.size();
  return s;
}


BlobDBImpl::BlobDBImpl(DB* db, const BlobDBOptions& blob_db_options)
    : BlobDB(db),
  bdb_options_(blob_db_options),
  ioptions_(db->GetOptions()),
  writer_offset_(0),
  next_sync_offset_(kBytesPerSync),
  next_file_number_(0)
{
  if (!bdb_options_.blob_dir.empty())
    blob_dir_ = ( bdb_options_.path_relative )  ? db_->GetName() + "/" + bdb_options_.blob_dir : bdb_options_.blob_dir;
}

// this is opening of the entire BlobDB.
// go through the directory and do an fstat on all the files.
Status BlobDBImpl::Open() {
 
  if (blob_dir_.empty()) {
    return Status::NotSupported("No blob directory in options");
  }

  Status s = db_->GetEnv()->CreateDirIfMissing(blob_dir_);
  if (!s.ok()) {
    return s;
  }

  s = openAllFiles();
  return s;
}

Status BlobDBImpl::GetSortedBlobLogs(const std::string& path)
{
  std::vector<std::string> all_files;
  Status status = db_->GetEnv()->GetChildren(path, &all_files);
  if (!status.ok()) {
    return status;
  }

  std::vector<std::pair<uint64_t, std::string> > file_nums;
  file_nums.reserve(all_files.size());

  for (const auto& f : all_files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kBlobFile) {
      file_nums.push_back(std::make_pair(number, f));

      //SequenceNumber sequence;
      //Status s = ReadFirstRecord(log_type, number, &sequence);
      //if (!s.ok()) {
        //return s;
      //}
      //if (sequence == 0) {
        //// empty file
        //continue;
      //}

      uint64_t size_bytes;
      status = db_->GetEnv()->GetFileSize(BlobFileName(path, number), &size_bytes);
      if (!status.ok()) {
        return status;
      }

#if 0
      log_files.push_back(std::unique_ptr<LogFile>(
          new LogFileImpl(number, log_type, sequence, size_bytes)));
#endif
    }
  }

  std::sort(file_nums.begin(), file_nums.end());
  return status;
}

Status BlobDBImpl::openAllFiles() {

  Status s;
  return s;
  // go through the blob db directory open all files.
  // read header and read footer if present.
  // if there are any files that are not closed, keep them open
  // provided they are not ttl based, and the ttl has expired.
  // if ttl has exp
  // assuming that they are 
}

Status BlobDBImpl::addNewFile()
{
  Status s = openNewFile();
  if (!s.ok()) {
    return s;
  }

  EnvOptions env_options(db_->GetOptions());
  std::unique_ptr<RandomAccessFile> rfile;
  s = ioptions_.env->NewRandomAccessFile(BlobFileName(blob_dir_, next_file_number_.load()),
                                         &rfile, env_options);
  if (!s.ok()) {
    return s;
  }
  file_reader_.reset(new RandomAccessFileReader(std::move(rfile)));
  return s;
}

Status BlobDBImpl::PutUntil(const WriteOptions& options, const Slice& key,
             const Slice& value, uint32_t expiration)
{
  Status s;
  return s;
}

Status BlobDBImpl::PutWithTTL(const WriteOptions& options, const Slice& key,
             const Slice& value, uint32_t ttl)
{
  using std::chrono::system_clock;
  std::time_t epoch_now = system_clock::to_time_t(system_clock::now());
  return PutUntil(options, key, value, epoch_now + ttl);
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

BlobFile::BlobFile(const std::string& bdir, uint64_t fn)
  : path_to_dir_(bdir), blob_count_(0),
    file_number_(fn), file_size_(0), 
    has_ttl_(false), has_timestamps_(false) 
{
}
  
}  // namespace rocksdb
