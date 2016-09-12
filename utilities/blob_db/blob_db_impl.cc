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

Status BlobDBImpl::Put(const WriteOptions& options, const Slice& key,
             const Slice& value)
{
   Status s;
   return s;
}

#if 0

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
#endif

namespace {

   void extendTTL(std::pair<uint32_t, uint32_t>& ttl_range, uint32_t ttl) {
     ttl_range.first = std::min(ttl_range.first, ttl);
     ttl_range.second = std::max(ttl_range.second, ttl);
   }

   void extendTimestamps(std::pair<uint64_t, uint64_t>& ts_range, uint64_t ts) {
     ts_range.first = std::min(ts_range.first, ts);
     ts_range.second = std::max(ts_range.second, ts);
   }

   void extendSN(std::pair<uint64_t, uint64_t>& sn_range, uint64_t sn) {
     sn_range.first = std::min(sn_range.first, sn);
     sn_range.second = std::max(sn_range.second, sn);
   }
}

BlobDBImpl::BlobDBImpl(DB* db, const BlobDBOptions& blob_db_options)
    : BlobDB(db),
  bdb_options_(blob_db_options),
  ioptions_(db->GetOptions()),
  db_options_(db->GetOptions()),
  next_file_number_(0)
{
  if (!bdb_options_.blob_dir.empty())
    blob_dir_ = ( bdb_options_.path_relative )  ? db_->GetName() + "/" + bdb_options_.blob_dir : bdb_options_.blob_dir;
}

Status BlobDBImpl::openNewFile_P1_lock(std::unique_ptr<BlobFile>& bfile)
{
  uint64_t file_num = ++next_file_number_;
  EnvOptions env_options(db_->GetOptions());

  bfile.reset(new BlobFile(blob_dir_, file_num));
  Status s = createWriter(bfile.get(), ioptions_.env, env_options);
  return s;
}

// this opens a standard new file.
Status BlobDBImpl::openNewFile(BlobFile *& bfile_ret)
{
  bfile_ret = nullptr;
  InstrumentedMutexLock l(&mutex_);

  std::unique_ptr<BlobFile> bfile;
  Status s = openNewFile_P1_lock(bfile);
  if (!s.ok())
    return s;

  blob_log::Writer *writer = bfile->GetWriter();
  blob_log::BlobLogHeader& header(bfile->Header());

  s = writer->WriteHeader(header);
  if (!s.ok())
    return s;

  bfile_ret = bfile.get();
  blob_files_.insert(std::make_pair(bfile->BlobFileNumber(), std::move(bfile)));
  return s;
}

// this opens a new file with TTL support
Status BlobDBImpl::openNewFileWithTTL(std::pair<uint32_t, uint32_t>& ttl_guess, BlobFile*& bfile_ret)
{
  bfile_ret = nullptr;
  InstrumentedMutexLock l(&mutex_);

  std::unique_ptr<BlobFile> bfile;
  Status s = openNewFile_P1_lock(bfile);
  if (!s.ok())
    return s;

  blob_log::Writer *writer = bfile->GetWriter();
  blob_log::BlobLogHeader& header(bfile->Header());
  header.setTTLGuess(ttl_guess);

  s = writer->WriteHeader(header);
  if (!s.ok())
    return s;

  bfile_ret = bfile.get();
  blob_files_.insert(std::make_pair(bfile->BlobFileNumber(), std::move(bfile)));
  open_blob_files_.insert(bfile_ret);

  return s;
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

Status BlobDBImpl::openAllFiles()
{
  InstrumentedMutexLock l(&mutex_);

  std::vector<std::string> all_files;
  Status status = db_->GetEnv()->GetChildren(blob_dir_, &all_files);
  if (!status.ok()) {
    return status;
  }

  std::set<std::pair<uint64_t, std::string> > file_nums;

  EnvOptions env_options(db_->GetOptions());

  for (const auto& f : all_files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kBlobFile) {
      file_nums.insert(std::make_pair(number, f));
    }
  }

  for (auto f_iter: file_nums) {
    uint64_t size_bytes;
    Status s1 = db_->GetEnv()->GetFileSize(f_iter.second, &size_bytes);
    if (!s1.ok()) {
      // report something here.
      continue;
    }

    std::unique_ptr<BlobFile> bfptr(new BlobFile(blob_dir_, f_iter.first));
    bfptr->setFileSize(size_bytes);

    blob_log::BlobLogFooter bf;
    s1 = ReadFooter(bfptr.get(), bf);

    if (s1.ok()) {
      bfptr->setFromFooter(bf);
    } else {

      // sequentially iterate over the file and read all the records
      s1 = bfptr->createSequentialReader(db_->GetEnv(), db_options_, env_options);
      if (!s1.ok()) {
        // report something here.
        continue;
      }

      s1 = bfptr->ReadHeader();
      if (!s1.ok()) {
        // report something here.
        // close the file
        continue;
      }

      blob_log::Reader *reader = bfptr->GetReader();

      uint64_t blob_count = 0;
      std::pair<uint32_t, uint32_t> ttl_range(std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::min());
      std::pair<uint64_t, uint64_t> ts_range(std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::min());
      std::pair<uint64_t, uint64_t> sn_range(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::min());

      blob_log::Record record;
      bool shallow = true;
      std::string scratch;

      while (reader->ReadRecord(record, &scratch, shallow)) {
        ++blob_count;
        if (bfptr->HasTTL()) {
          extendTTL(ttl_range, record.GetTTL());
        }
        if (bfptr->HasTimestamps()) {
          extendTimestamps(ts_range, record.GetTimeVal());
        }
        extendSN(sn_range, record.GetSN());
      }

      if (blob_count) {
        bfptr->setSNRange(sn_range);

        if (bfptr->HasTimestamps()) {
          bfptr->setTimeRange(ts_range);
        } 
   
        if (bfptr->HasTTL()) {
          bfptr->setTTLRange(ttl_range);
        }
      }
    }

    blob_files_.insert(std::make_pair(f_iter.first, std::move(bfptr)));
  }

  return status;
}

BlobFile::BlobFile(const std::string& bdir, uint64_t fn)
  : path_to_dir_(bdir), blob_count_(0),
    file_number_(fn), file_size_(0), 
    closed_(false), header_read_(false)
{
}

Status BlobFile::createSequentialReader(Env *env, const DBOptions& db_options, const EnvOptions& env_options)
{
  Status s = env->NewSequentialFile(BlobFileName(path_to_dir_, file_number_), &sfile_, env_options);
  if (!s.ok()) {
    // report something here.
    return s;
  }

  sfile_reader_.reset(new SequentialFileReader(std::move(sfile_)));
  log_reader_.reset(new blob_log::Reader(db_options.info_log, std::move(sfile_reader_), nullptr,
      true, 0, file_number_));

  return s;
}

Status BlobFile::WriteFooterAndClose()
{
  blob_log::BlobLogFooter footer;
  footer.blob_count_ = blob_count_;
  footer.ttl_range_ = ttl_range_;
  footer.sn_range_ = sn_range_;
  footer.ts_range_ = time_range_;

  footer.has_ttl_ = HasTTL();
  footer.has_ts_ = HasTimestamps();

  // this will close the file and reset the Writable File Pointer.
  Status s = log_writer_->AppendFooter(footer);
  if (s.ok()) {
    closed_ = true;
  }
  return s;
}

void BlobFile::setFromFooter(const blob_log::BlobLogFooter& footer)
{
  // if header has already been read, assert that it is the same.
  if (header_read_) {
    assert(footer.HasTTL() == header_.HasTTL());
    assert(footer.HasTimestamps() == header_.HasTimestamps());
  } else {

    header_.setTTL(footer.HasTTL());
    header_.setTimestamps(footer.HasTimestamps());
  }

  blob_count_ = footer.GetBlobCount();
  ttl_range_ = footer.GetTTLRange();
  time_range_ = footer.GetTimeRange();
  sn_range_ = footer.GetSNRange();
  closed_ = true;
}

Status BlobDBImpl::ReadFooter(BlobFile *bfile, blob_log::BlobLogFooter& bf) {

  EnvOptions env_options(db_->GetOptions());
  RandomAccessFileReader *reader = bfile->openRandomAccess(ioptions_.env, env_options);

  Slice result;
  char scratch[blob_log::BlobLogFooter::kFooterSize+10];
  Status s = reader->Read(bfile->GetFileSize() - blob_log::BlobLogFooter::kFooterSize,
    blob_log::BlobLogFooter::kFooterSize, &result, scratch);

  if (!s.ok()) {
    return s;
  }

  s = bf.DecodeFrom(&result);
  return s;
}

Status BlobFile::ReadHeader() {

  Status s = log_reader_->ReadHeader(header_);
  if (s.ok()) {
    header_read_ = true;
  }
  return s;
}

Status BlobDBImpl::createWriter(BlobFile *bfile, Env *env, const EnvOptions& env_options) {

  Status s = env->NewWritableFile(BlobFileName(bfile->path_to_dir_, bfile->file_number_), &bfile->wfile_, env_options);
  if (!s.ok())
    return s;

  bfile->file_writer_.reset(new WritableFileWriter(std::move(bfile->wfile_), env_options));
  bfile->log_writer_.reset(new blob_log::Writer(std::move(bfile->file_writer_), bfile->file_number_, bdb_options_.bytes_per_sync, db_->GetOptions().use_fsync));

  return s;
}

Status BlobDBImpl::PutWithTTL(const WriteOptions& options, const Slice& key,
             const Slice& value, uint32_t ttl)
{
  using std::chrono::system_clock;
  std::time_t epoch_now = system_clock::to_time_t(system_clock::now());
  return PutUntil(options, key, value, epoch_now + ttl);
}

BlobFile* BlobDBImpl::findBlobFile(uint32_t expiration) const
{
   BlobFile tmp;
   tmp.ttl_range_ = std::make_pair(expiration, 0);

   auto citr = open_blob_files_.equal_range(&tmp);
   if (citr.first == open_blob_files_.end()) {
     return nullptr;
   }

   auto finditr = citr.second;
   --finditr;

   return ((*finditr)->ttl_range_.second < expiration) ? nullptr : *finditr;
}

Status BlobDBImpl::PutUntil(const WriteOptions& options, const Slice& key,
                   const Slice& value, uint32_t expiration) {
  BlobFile *bfile = findBlobFile(expiration);
  if (!bfile)  {
    std::pair<uint32_t, uint32_t> ttl_guess;
    ttl_guess.first = expiration;
    ttl_guess.second += 3600;
    // this opens a new file with TTL support
    Status s =openNewFileWithTTL(ttl_guess, bfile);
    if (!s.ok() || !bfile) {
      // show some error
      return s;
    }
  }

  // we need to lock, so that some other thread cannot close the writer.
  blob_log::Writer *writer = bfile->GetWriter();

  uint64_t blob_offset = 0;
  uint64_t key_offset = 0;

  // write the key to the blob log.
  Status s = writer->AddRecord(key, value, key_offset, blob_offset, expiration);
  if (!s.ok()) 
  {
    return s;
  }

  BlockHandle handle;
  std::string index_entry;
  PutVarint64(&index_entry, bfile->BlobFileNumber());

  InstrumentedMutexLock l(&mutex_);

  auto raw_block_size = value.size();
  handle.set_offset(blob_offset);
  handle.set_size(raw_block_size);

  handle.EncodeTo(&index_entry);
  s = db_->Put(options, key, index_entry);
  return s;
}

RandomAccessFileReader* BlobFile::openRandomAccess(Env *env, const EnvOptions& env_options)
{
  if (ra_file_reader_.get()) {
    return ra_file_reader_.get();
  }

  std::unique_ptr<RandomAccessFile> rfile;
  Status s = env->NewRandomAccessFile(BlobFileName(path_to_dir_, file_number_),
                                         &rfile, env_options);
  if (!s.ok()) {
    return nullptr;
  }
  ra_file_reader_.reset(new RandomAccessFileReader(std::move(rfile)));
  return ra_file_reader_.get();
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
  s = handle.DecodeFrom(&index_entry_slice);
  if (!s.ok()) {
    return s;
  }

  auto hitr = blob_files_.find(file_number);
  assert (hitr != blob_files_.end());

  EnvOptions env_options(db_->GetOptions());
  BlobFile *bfile = hitr->second.get();
  RandomAccessFileReader *reader = bfile->openRandomAccess(ioptions_.env, env_options);

  Slice blob_value;
  char buffer[16384];
  s = reader->Read(handle.offset(), handle.size(), &blob_value, buffer);

  std::string ret(blob_value.ToString());
  value->swap(ret);
  return s;

#if 0
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
#endif
}

bool blobf_compare_ttl::operator() (const BlobFile* lhs, const BlobFile* rhs) const {
  return lhs->ttl_range_.first < rhs->ttl_range_.first;
}

}  // namespace rocksdb
