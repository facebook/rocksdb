// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <chrono>
#include <iostream>
#include <iomanip>
#include <ctime>
#include <memory>
#include <cinttypes>

#include "utilities/blob_db/blob_db_impl.h"
#include "db/filename.h"
#include "db/write_batch_internal.h"
#include "db/db_impl.h"
#include "db/write_batch_internal.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/stackable_db.h"
#include "rocksdb/utilities/transaction.h"
#include "table/block.h"
#include "table/block_based_table_builder.h"
#include "table/block_builder.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/instrumented_mutex.h"
#include "table/meta_blocks.h"
#include "utilities/transactions/optimistic_transaction_db_impl.h"
#include "utilities/transactions/optimistic_transaction_impl.h"


// create multiple writers.
// have a mutex to select the writer
// write down the footer with # blobs.
// create an option which is just based on size of disk
// create GC thread which evicts based on et-lt
// create GC thread which evicts based on size of disk like FIFO
// on startup have a recovery function which reads all files.
// instead of TTL, use timestamp of the data.
using std::chrono::system_clock;
const int delta_time1 = 1;
const int delta_time2 = 60;

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
namespace {

   void extendTTL(rocksdb::ttlrange_t& ttl_range, uint32_t ttl) {
     ttl_range.first = std::min(ttl_range.first, ttl);
     ttl_range.second = std::max(ttl_range.second, ttl);
   }

   void extendTimestamps(rocksdb::tsrange_t& ts_range, uint64_t ts) {
     ts_range.first = std::min(ts_range.first, ts);
     ts_range.second = std::max(ts_range.second, ts);
   }

   void extendSN(rocksdb::snrange_t& sn_range, rocksdb::SequenceNumber sn) {
     sn_range.first = std::min(sn_range.first, sn);
     sn_range.second = std::max(sn_range.second, sn);
   }
}

namespace rocksdb {

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
bool blobf_compare_ttl::operator() (const std::shared_ptr<BlobFile>& lhs,
  const std::shared_ptr<BlobFile>& rhs) const {
  return lhs->ttl_range_.first < rhs->ttl_range_.first;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::Put(const WriteOptions& options,
  ColumnFamilyHandle* column_family, const Slice& key, const Slice& value)
{
  updateWriteOptions(options);

  std::shared_ptr<BlobFile> bfile = selectBlobFile();
  if (!bfile)
    return Status::NotFound("blob file not found");

  char headerbuf[blob_log::BlobLogRecord::kHeaderSize];
  blob_log::Writer::ConstructBlobHeader(headerbuf, key, value, -1, -1);

  Status s = PutCommon(bfile, options, column_family, headerbuf, key, value);

  closeIf(bfile);

  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::Delete(const WriteOptions& options,
  ColumnFamilyHandle* column_family, const Slice& key)
{
  delete_keys_q_.enqueue({column_family, key.ToString(), db_impl_->GetLatestSequenceNumber()});

  return db_->Delete(options, column_family, key);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
BlobDBImpl::BlobDBImpl(DB* db, const BlobDBOptions& blob_db_options)
  : BlobDB(db), db_impl_(dynamic_cast<DBImpl*>(db)),
  opt_db_(new OptimisticTransactionDBImpl(db, false)),
  wo_set_(false), bdb_options_(blob_db_options), ioptions_(db->GetOptions()),
  db_options_(db->GetOptions()), env_options_(db_->GetOptions()),
  next_file_number_(1), shutdown_(false)
{
  assert (db_impl_ != nullptr);
  if (!bdb_options_.blob_dir.empty())
    blob_dir_ = (bdb_options_.path_relative)  ? db_->GetName() +
      "/" + bdb_options_.blob_dir : bdb_options_.blob_dir;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobDBImpl::sanityCheck() {
  Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
    "Starting Sanity Check");

  Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
    "Number of files %" PRIu64, blob_files_.size());

  Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
    "Number of open files %" PRIu64, open_blob_files_.size());

  for (auto bfile: open_blob_files_) {
    assert(bfile->ActiveForAppend());
  }

  std::time_t epoch_now = system_clock::to_time_t(system_clock::now());

  for (auto bfile_pair: blob_files_) {
    auto bfile = bfile_pair.second;
    Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
      "Blob File %s %" PRIu64 " %" PRIu64  " %" PRIu64
      " %" PRIu64 " %d", bfile->PathName().c_str(),
      bfile->GetFileSize(), bfile->BlobCount(), bfile->deleted_count_,
      bfile->deleted_size_, (bfile->ttl_range_.second - epoch_now));
  }
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
BlobDBImpl::~BlobDBImpl()
{
  CancelAllBackgroundWork(db_, true);

  shutdown();

  // Wait for all other threads (if there are any) to finish execution
  for (auto& gc_thd : gc_threads_) {
    gc_thd.join();
  }
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<BlobFile> BlobDBImpl::openNewFile_P1()
{
  uint64_t file_num = next_file_number_++;
  return std::make_shared<BlobFile>(blob_dir_, file_num);
}

////////////////////////////////////////////////////////////////////////////////
// this is opening of the entire BlobDB.
// go through the directory and do an fstat on all the files.
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::Open() {

  if (blob_dir_.empty()) {
    return Status::NotSupported("No blob directory in options");
  }

  Status s = db_->GetEnv()->CreateDirIfMissing(blob_dir_);
  if (!s.ok()) {
    return s;
  }

  s = openAllFiles();
  if (!s.ok()) {
    return s;
  }

  s = startGCThreads();
  return s;
}

////////////////////////////////////////////////////////////////////////////////
// Maybe Parallelize later
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::openAllFiles()
{
  WriteLock wl(&mutex_);

  std::vector<std::string> all_files;
  Status status = db_->GetEnv()->GetChildren(blob_dir_, &all_files);
  if (!status.ok()) {
    return status;
  }

  std::set<std::pair<uint64_t, std::string> > file_nums;
  for (const auto& f : all_files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kBlobFile) {
      file_nums.insert(std::make_pair(number, f));
    } else {
      Log(InfoLogLevel::WARN_LEVEL, db_options_.info_log,
        "Skipping file in blob directory %s", f.c_str());
    }
  }

  if (!file_nums.empty()) {
    next_file_number_.store((file_nums.rbegin())->first + 1);
  }

  for (auto f_iter: file_nums) {
    std::string bfpath = BlobFileName(blob_dir_, f_iter.first);
    uint64_t size_bytes;
    Status s1 = db_->GetEnv()->GetFileSize(bfpath, &size_bytes);
    if (!s1.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, db_options_.info_log,
        "Not able to get size of file %s", bfpath.c_str());
      continue;
    }

    std::shared_ptr<BlobFile> bfptr
      = std::make_shared<BlobFile>(blob_dir_, f_iter.first);

    // no-locks since bfptr is not visible yet
    bfptr->setFileSize(size_bytes);
    std::shared_ptr<RandomAccessFileReader> ra_reader
      = bfptr->openRandomAccess_locked(ioptions_.env, env_options_);

    blob_log::BlobLogFooter bf;
    s1 = bfptr->ReadFooter(bf, true);
    if (s1.ok()) {
      bfptr->setFromFooter(bf);
    } else {
      Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
        "File found incomplete (w/o footer) %s", bfpath.c_str());

      // sequentially iterate over the file and read all the records
      std::shared_ptr<blob_log::Reader> reader;
      reader = bfptr->openSequentialReader_locked(db_->GetEnv(),
        db_options_, env_options_);

      s1 = bfptr->ReadHeader();
      if (!s1.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, db_options_.info_log,
          "Failure to read Header for blob-file %s", bfpath.c_str());
        continue;
      }

      ttlrange_t ttl_range(std::numeric_limits<uint32_t>::max(),
        std::numeric_limits<uint32_t>::min());
      tsrange_t ts_range(std::numeric_limits<uint32_t>::max(),
        std::numeric_limits<uint32_t>::min());
      snrange_t sn_range(std::numeric_limits<SequenceNumber>::max(),
        std::numeric_limits<SequenceNumber>::min());

      uint64_t blob_count = 0;
      blob_log::BlobLogRecord record;
      int shallow = 0;
      while (reader->ReadRecord(record, shallow).ok()) {
        ++blob_count;
        if (bfptr->HasTTL()) {
          extendTTL(ttl_range, record.GetTTL());
        }
        if (bfptr->HasTimestamps()) {
          extendTimestamps(ts_range, record.GetTimeVal());
        }
        extendSN(sn_range, record.GetSN());
      }

      if (!blob_count) {
        Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
          "0 blobs found in file %s", bfpath.c_str());
        continue;
      } else
        Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
          "%" PRIu64 " blobs found in file %s", blob_count,
          bfpath.c_str());

      bfptr->setBlobCount(blob_count);
      bfptr->setSNRange(sn_range);

      if (bfptr->HasTimestamps()) {
        bfptr->setTimeRange(ts_range);
      }

      if (bfptr->HasTTL()) {
        ttl_range.second  = std::max(ttl_range.second, ttl_range.first + (uint32_t)bdb_options_.ttl_range);
        bfptr->setTTLRange(ttl_range);
        std::time_t epoch_now = system_clock::to_time_t(system_clock::now());
        if (ttl_range.second < epoch_now) {
          Status fstatus = createWriter_locked(bfptr.get(), true);
          if (fstatus.ok())
            fstatus = bfptr->WriteFooterAndClose_locked();
          if (!fstatus.ok()) {
            // report error here
            continue;
          }
        } else
          open_blob_files_.insert(bfptr);
      }
    }

    blob_files_.insert(std::make_pair(f_iter.first, bfptr));
  }

  return status;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobFile::ReadFooter(blob_log::BlobLogFooter& bf, bool close_reader) {

  if (file_size_ < (blob_log::BlobLogHeader::kHeaderSize +
    blob_log::BlobLogFooter::kFooterSize))
    return Status::IOError("File does not have footer", PathName());

  uint64_t footer_offset = file_size_ - blob_log::BlobLogFooter::kFooterSize;
  // assume that ra_file_reader_ is valid before we enter this
  assert(ra_file_reader_);

  Slice result;
  char scratch[blob_log::BlobLogFooter::kFooterSize+10];
  Status s = ra_file_reader_->Read(footer_offset,
        blob_log::BlobLogFooter::kFooterSize, &result, scratch);

  if (!s.ok())
    return s;

  // if we don't close the reader, we don't need to take any locks.
  if (close_reader) {
    ra_file_reader_.reset();
  }
  s = bf.DecodeFrom(&result);
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::createWriter_locked(BlobFile *bfile, bool reopen) {
  std::string fpath(bfile->PathName());

  std::unique_ptr<WritableFile> wfile;
  Status s = (reopen) ?
   db_->GetEnv()->ReopenWritableFile(fpath, &wfile, env_options_) :
   db_->GetEnv()->NewWritableFile(fpath, &wfile, env_options_);

  if (!s.ok())
    return s;

  std::unique_ptr<WritableFileWriter> fwriter;
  fwriter.reset(new WritableFileWriter(std::move(wfile), env_options_));

  uint64_t boffset = bfile->GetFileSize();
  bfile->log_writer_ = std::make_shared<blob_log::Writer>(std::move(fwriter),
    bfile->file_number_, bdb_options_.bytes_per_sync,
    db_->GetOptions().use_fsync, boffset);

  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::PutWithTTL(const WriteOptions& options,
  ColumnFamilyHandle* column_family, const Slice& key,
  const Slice& value, uint32_t ttl)
{
  std::time_t epoch_now = system_clock::to_time_t(system_clock::now());
  return PutUntil(options, column_family, key, value, epoch_now + ttl);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<BlobFile> BlobDBImpl::findBlobFile_locked(uint32_t expiration) const
{
   if (open_blob_files_.empty())
     return nullptr;

   std::shared_ptr<BlobFile> tmp = std::make_shared<BlobFile>();
   tmp->ttl_range_ = std::make_pair(expiration, 0);

   auto citr = open_blob_files_.equal_range(tmp);
   if (citr.first == open_blob_files_.end()) {
     assert(citr.second == open_blob_files_.end());

     std::shared_ptr<BlobFile> check = *(open_blob_files_.rbegin());
     return (check->ttl_range_.second  < expiration) ? nullptr : check;
   }

   if (citr.first != citr.second) {
     return *(citr.first);
   }

   auto finditr = citr.second;
   if (finditr != open_blob_files_.begin())
     --finditr;

   bool b2 = (*finditr)->ttl_range_.second < expiration;
   bool b1 = (*finditr)->ttl_range_.first > expiration;

   return (b1 || b2) ?  nullptr : (*finditr);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<blob_log::Writer> BlobDBImpl::checkOrCreateWriter_locked(BlobFile *bfile) {
  std::shared_ptr<blob_log::Writer> writer = bfile->GetWriter();
  if (writer)
    return writer;

  Status s = createWriter_locked(bfile, true);
  if (!s.ok())
    return writer;

  writer = bfile->GetWriter();
  return writer;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobDBImpl::updateWriteOptions(const WriteOptions& options) {
  if (!wo_set_.load(std::memory_order_relaxed)) {
    // DCLP
    WriteLock wl(&mutex_);
    if (!wo_set_.load(std::memory_order_acquire)) {
      wo_set_.store(true, std::memory_order_release);
      write_options_ = options;
    }
  }
}

Random blob_rgen(time(NULL));

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<BlobFile> BlobDBImpl::selectBlobFile() {

  {
    ReadLock rl(&mutex_);
    uint32_t val = blob_rgen.Next();
    if (!open_simple_files_.empty() && open_simple_files_.size() <
      bdb_options_.num_simple_blobs)
      return open_simple_files_[val % bdb_options_.num_simple_blobs];
  }

  std::shared_ptr<BlobFile> bfile = openNewFile_P1();
  if (!bfile)
    return bfile;

  std::shared_ptr<blob_log::Writer> writer =
    checkOrCreateWriter_locked(bfile.get());

  blob_log::BlobLogHeader& header(bfile->Header());

  bfile->file_size_ = blob_log::BlobLogHeader::kHeaderSize;

  WriteLock wl(&mutex_);
  // CHECK again
  if (open_simple_files_.size() == bdb_options_.num_simple_blobs) {
    uint32_t val = blob_rgen.Next();
    return open_simple_files_[val % bdb_options_.num_simple_blobs];
  }

  Status s = writer->WriteHeader(header);
  if (!s.ok())
    return nullptr;

  open_simple_files_.push_back(bfile);
  return bfile;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<BlobFile> BlobDBImpl::selectBlobFileTTL(uint32_t expiration)
{
  std::shared_ptr<BlobFile> bfile;
  {
    ReadLock rl(&mutex_);
    bfile = findBlobFile_locked(expiration);
  }

  if (bfile)
    return bfile;

  uint32_t exp_low = (expiration / bdb_options_.ttl_range) * bdb_options_.ttl_range;
  ttlrange_t ttl_guess = std::make_pair(exp_low, exp_low + bdb_options_.ttl_range);

  bfile = openNewFile_P1();
  if (!bfile)
    return bfile;

  // we don't need to take blob file lock as no other thread is seeing bfile yet
  std::shared_ptr<blob_log::Writer> writer =
    checkOrCreateWriter_locked(bfile.get());

  blob_log::BlobLogHeader& header(bfile->Header());
  header.setTTLGuess(ttl_guess);

  bfile->file_size_ = blob_log::BlobLogHeader::kHeaderSize;

  // set the first value of the range, since that is concrete at this time.
  // also necessary to add to open_blob_files_
  bfile->ttl_range_ = ttl_guess;

  WriteLock wl(&mutex_);
  // check condition again, because of missing upgrade lock
  auto bfile2 = findBlobFile_locked(expiration);
  if (bfile2)
    return bfile2;

  Status s = writer->WriteHeader(header);
  if (!s.ok())
    return nullptr;

  blob_files_.insert(std::make_pair(bfile->BlobFileNumber(), bfile));
  open_blob_files_.insert(bfile);

  return bfile;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::PutUntil(const WriteOptions& options,
  ColumnFamilyHandle* column_family, const Slice& key,
  const Slice& value, uint32_t expiration) {

  updateWriteOptions(options);

  std::shared_ptr<BlobFile> bfile = selectBlobFileTTL(expiration);
  if (!bfile)
    return Status::NotFound("blob file not found");

  char headerbuf[blob_log::BlobLogRecord::kHeaderSize];
  blob_log::Writer::ConstructBlobHeader(headerbuf, key, value, expiration, -1);

  Status s = PutCommon(bfile, options, column_family, headerbuf, key, value);

  extendTTL(bfile->ttl_range_, expiration);

  closeIf(bfile);

  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobDBImpl::closeIf(std::shared_ptr<BlobFile>& bfile) {

  bool close = bfile->GetFileSize() > bdb_options_.blob_file_size;
  if (!close)
    return;

  {
    WriteLock wl(&mutex_);

    // this prevents others from picking up this file
    open_blob_files_.erase(bfile);

    auto findit = std::find(open_simple_files_.begin(),
      open_simple_files_.end(), bfile);
    if (findit != open_simple_files_.end())
      open_simple_files_.erase(findit);
  }

  WriteLock wl_f(&bfile->mutex_);
  bfile->WriteFooterAndClose_locked();
}

////////////////////////////////////////////////////////////////////////////////
// bfile wont be deleted because of shared_ptr
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::PutCommon(std::shared_ptr<BlobFile>& bfile,
  const WriteOptions& options, ColumnFamilyHandle *column_family,
  const char *headerbuf, const Slice& key, const Slice& value) {

  BlockHandle handle;
  std::string index_entry;
  PutVarint64(&index_entry, bfile->BlobFileNumber());

  auto raw_block_size = value.size();
  handle.set_size(raw_block_size);

  auto key_size = key.size();
  Status s;
  uint64_t blob_offset = 0;
  uint64_t key_offset = 0;
  std::shared_ptr<blob_log::Writer> writer;
  {
    WriteLock lockbfile_w(&bfile->mutex_);
    writer = checkOrCreateWriter_locked(bfile.get());

    // write the blob to the blob log.
    s = writer->EmitPhysicalRecord(headerbuf, key, value,
      key_offset, blob_offset);
  }

  if (!s.ok())
    return s;

  // increment blob count
  bfile->blob_count_++;
  bfile->file_size_ += blob_log::BlobLogRecord::kHeaderSize
    + key_size + raw_block_size;

  handle.set_offset(blob_offset);
  handle.EncodeTo(&index_entry);

  WriteBatch batch;
  batch.Put(column_family, key, index_entry);

  // this goes to the base db and can be expensive
  s = db_->Write(options, &batch);

  // this is the sequence number of the write.
  SequenceNumber sn = WriteBatchInternal::Sequence(&batch);

  {
    WriteLock lockbfile(&bfile->mutex_);
    Status s1 = writer->AddRecordFooter(sn);
    if (!s1.ok()) {
      // do something here.
    }
    extendSN(bfile->sn_range_, sn);
  }

  bfile->file_size_ += blob_log::BlobLogRecord::kFooterSize;
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::Get(const ReadOptions& options,
  ColumnFamilyHandle* column_family, const Slice& key, std::string* value) {

  Status s;
  std::string index_entry;
  s = db_->Get(options, column_family, key, &index_entry);
  if (!s.ok())
    return s;

  BlockHandle handle;
  Slice index_entry_slice(index_entry);
  uint64_t file_number;
  if (!GetVarint64(&index_entry_slice, &file_number))
    return Status::Corruption();

  s = handle.DecodeFrom(&index_entry_slice);
  if (!s.ok())
    return s;

  std::shared_ptr<BlobFile> bfile;
  {
    ReadLock l(&mutex_);
    auto hitr = blob_files_.find(file_number);

    // file was deleted
    if (hitr == blob_files_.end()) {
      return Status::NotFound("Blob Not Found");
    }

    bfile = hitr->second;
  }

  std::shared_ptr<RandomAccessFileReader> reader;
  {
    WriteLock lockbfile_w(&bfile->mutex_);
    reader = bfile->openRandomAccess_locked(ioptions_.env, env_options_);
  }

  // allocate the buffer
  value->resize(handle.size());
  char *buffer = &(*value)[0];

  Slice blob_value;
  s = reader->Read(handle.offset(), handle.size(), &blob_value, buffer);

  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobDBImpl::shutdown()
{
   shutdown_.store(true);

   // fire the conditional variable
   gc_cv_.notify_all();
}

////////////////////////////////////////////////////////////////////////////////
// iterate over the blobs sequentially and check if the blob sequence number
// is the latest. If it is the latest, preserve it, otherwise delete it
// if it is TTL based, and the TTL has expired, then
// we can blow the entity if the key is still the latest or the Key is not
// found
// WHAT HAPPENS IF THE KEY HAS BEEN OVERRIDEN. Then we can drop the blob
// without doing anything if the earliest snapshot is not
// refering to that sequence number, i.e. it is later than the sequence number
// of the new key
//
// if it is not TTL based, then we can blow the key if the key has been
// DELETED in the LSM
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::writeBatchOfDeleteKeys(BlobFile *bfptr)
{
  std::shared_ptr<blob_log::Reader> reader;
  {
    WriteLock lockbfile_w(&(bfptr->mutex_));
    // sequentially iterate over the file and read all the records
    reader = bfptr->openSequentialReader_locked(db_->GetEnv(), db_options_,
      env_options_, true);
    if (!reader) {
      // report something here.
      return Status::IOError("failed to create sequential reader");
    }
  }

  Status s = bfptr->ReadHeader();
  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, db_options_.info_log,
      "Failure to read Header for blob-file %s", bfptr->PathName().c_str());
    return s;
  }

  ColumnFamilyHandle *cfh = bfptr->GetColumnFamily(db_);
  auto cfhi = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh);
  auto cfd = cfhi->cfd();

  uint64_t blob_count = 0;
  blob_log::BlobLogRecord record;

  // this reads the key but skips the blob
  int shallow = 1;

  SuperVersion* sv = db_impl_->GetAndRefSuperVersion(cfd);
  if (sv == nullptr) {
    Status result = Status::InvalidArgument("Could not access column family 0");
    return result;
  }

  assert (opt_db_);
  Transaction* txn = static_cast<OptimisticTransactionDB*>(opt_db_.get())->BeginTransaction(write_options_);
  OptimisticTransactionImpl *otxn = dynamic_cast<OptimisticTransactionImpl*>(txn);
  assert (otxn != nullptr);

  while (reader->ReadRecord(record, shallow).ok()) {
    ++blob_count;
    SequenceNumber seq = kMaxSequenceNumber;
    bool found_record_for_key = false;

    s = db_impl_->GetLatestSequenceForKey(sv, record.Key(), false,
      &seq, &found_record_for_key);

    if (!s.ok())
      continue;

    if (!found_record_for_key || seq == record.GetSN()) {
      // stil could have a TOCTOU
      txn->Delete(cfh, record.Key());
    }
  }

  s = txn->Commit();

  // Now write the
  db_impl_->ReturnAndCleanupSuperVersion(cfd, sv);

  // if this fails, we should try to preserve the write batch.
  if (s.IsBusy()) {
    return s;
  }

  if (s.ok()) {
    // we are done.
  }

  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
bool BlobDBImpl::TryDeleteFile(std::shared_ptr<BlobFile>& bfile) {
   assert (bfile->Obsolete());

   SequenceNumber esn = bfile->GetSNRange().first;

   // this is not correct.
   // you want to check that there are no snapshots in the
   bool notok = db_impl_->HasActiveSnapshotLaterThanSN(esn);
   if (notok) {
     Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
       "Could not delete file due to snapshot failure %s",
       bfile->PathName().c_str());
     return false;
   } else {
     Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
       "Will delete file due to snapshot success %s",
       bfile->PathName().c_str());
   }

   // race here
   open_blob_files_.erase(bfile);
   blob_files_.erase(bfile->BlobFileNumber());

   Status s = db_->GetEnv()->DeleteFile(bfile->PathName());
   if (!s.ok()) {
     // return
     return false;
   }

   return true;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobDBImpl::evictDeletions() {
  ColumnFamilyHandle *last_cfh = nullptr;
  Options last_op;

  Arena arena;
  ScopedArenaIterator iter;
  RangeDelAggregator range_del_agg(InternalKeyComparator(ioptions_.user_comparator),
    {} /* snapshots */);                   

  delete_packet_t dpacket;
  while (delete_keys_q_.dequeue(dpacket)) {
    if (last_cfh != dpacket.cfh_) {
      // this can be expensive
      last_cfh = dpacket.cfh_;
      last_op = db_impl_->GetOptions(last_cfh);
      iter.set(db_impl_->NewInternalIterator(&arena, &range_del_agg, dpacket.cfh_));
      // this will not work for multiple CF's.
    }

    Slice user_key(dpacket.key_);
    InternalKey target(user_key, dpacket.dsn_, kTypeValue);

    Slice eslice = target.Encode();
    iter->Seek(eslice);

    if (!iter->status().ok()) {
      Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
        "Invalid iterator seek %s", dpacket.key_.c_str());
      continue;
    }

    const Comparator *bwc = BytewiseComparator();
    while (iter->Valid()) {
      if (!bwc->Equal(ExtractUserKey(iter->key()), ExtractUserKey(eslice)))
        break;

      ParsedInternalKey ikey(Slice(), 0, kTypeValue);
      if (!ParseInternalKey(iter->key(), &ikey)) {
        continue;
      }

      // once you hit a DELETE, assume the keys below have been
      // processed previously
      if (ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion)
        break;

      Slice val = iter->value();
      iter->Next();

      BlockHandle handle;
      uint64_t file_number;
      Status s;
      if (!GetVarint64(&val, &file_number) || !(s = handle.DecodeFrom(&val)).ok()) {
        Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
            "Invalid parse of key-value for file_number %s", val.ToString().c_str());
        continue;
      }

      std::shared_ptr<BlobFile> bfile;
      {
        ReadLock l(&mutex_);
        auto hitr = blob_files_.find(file_number);

        // file was deleted
        if (hitr == blob_files_.end()) {
          Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
              "Could not find file_number %" PRIu64, file_number);
          continue;
        }

        bfile = hitr->second;
        bfile->deleted_count_++;
        bfile->deleted_size_ += handle.size() + blob_log::BlobLogRecord::kHeaderSize
          + blob_log::BlobLogRecord::kFooterSize;
      }
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobDBImpl::runGC() {

  std::time_t last_time_run = system_clock::to_time_t(system_clock::now());
  while (!shutdown_.load()) {

    std::unique_lock<std::mutex> lock(gc_mutex_);
    gc_cv_.wait_for(lock, std::chrono::milliseconds(delta_time1*1000));

    evictDeletions();

    system_clock::time_point now = system_clock::now();
    std::time_t tt = system_clock::to_time_t(now);

    // protect against spurious wakeups
    if ((tt - last_time_run) < delta_time2)
      continue;

    last_time_run = tt;
    sanityCheck();
    std::vector<std::shared_ptr<BlobFile> > blob_files;
    {
      // take a copy
      ReadLock l(&mutex_);
      blob_files.reserve(blob_files_.size());
      for (auto const& ent : blob_files_) {
        blob_files.push_back(ent.second);
      }
    }

    for (auto bfile: blob_files) {
      // File can be obsolete
      // File can be Open for Writes
      // File can be closed
      std::string pn = bfile->PathName();

      // in a previous pass, this file was marked obsolete
      if (bfile->Obsolete()) {
        Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
          "File will be deleted as obsolete %s", pn.c_str());
        TryDeleteFile(bfile);
        continue;
      }

      if (bfile->HasTTL()) {
        ttlrange_t ttl_range = bfile->GetTTLRange();
        if (tt > ttl_range.second) {
          Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
            "File has expired ttl %s %d %d", pn.c_str(), tt,
            ttl_range.second);
          // all the elements can be deleted.
          // Go through all the keys and do a WriteBatch

          Status s = writeBatchOfDeleteKeys(bfile.get());
          if (s.ok()) {
            //
            bfile->canBeDeleted();
            bool ret = TryDeleteFile(bfile);
            if (ret) {
              bfile = nullptr;
            }
          }
        }
      }
    }

    //std::cout << "current time: " << std::asctime(std::localtime(&tt)) << std::endl;
  }
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::startGCThreads() {
  Status s;
  // we can use more threads in the future, but 1 should be sufficient now.
  gc_threads_.reserve(1);
  gc_threads_.emplace_back(&BlobDBImpl::runGC, this);
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
BlobFile::BlobFile()
  : blob_count_(0), file_number_(0), file_size_(0),
    deleted_count_(0), deleted_size_(0), closed_(false),
    header_read_(false), can_be_deleted_(false),
    ttl_range_(std::make_pair(0,0)), time_range_(std::make_pair(0,0)),
    sn_range_(std::make_pair(0, 0))
{
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
BlobFile::BlobFile(const std::string& bdir, uint64_t fn)
  : path_to_dir_(bdir), blob_count_(0), file_number_(fn), file_size_(0),
    deleted_count_(0), deleted_size_(0),
    closed_(false), header_read_(false), can_be_deleted_(false),
    ttl_range_(std::make_pair(0,0)), time_range_(std::make_pair(0,0)),
    sn_range_(std::make_pair(0, 0))
{
}

BlobFile::~BlobFile()
{
  if (can_be_deleted_) {
    std::string pn(PathName());
    Status s = Env::Default()->DeleteFile(PathName());
     if (!s.ok()) {
       //Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
         //"File could not be deleted %s", pn.c_str());
     }
  }
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::string BlobFile::PathName() const {
  return BlobFileName(path_to_dir_, file_number_);
}

////////////////////////////////////////////////////////////////////////////////
//  Should take a WriteLock on the mutex_
//
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<blob_log::Reader> BlobFile::openSequentialReader_locked(
  Env *env, const DBOptions& db_options, const EnvOptions& env_options,
  bool rewind)
{
  if (log_reader_) {
    if (rewind) {
      assert(log_reader_->file() != nullptr);
      log_reader_->file()->Rewind();
    }
    return log_reader_;
  }

  std::unique_ptr<SequentialFile> sfile;
  Status s = env->NewSequentialFile(PathName(), &sfile, env_options);
  if (!s.ok()) {
    // report something here.
    return log_reader_;
  }

  std::unique_ptr<SequentialFileReader> sfile_reader;
  sfile_reader.reset(new SequentialFileReader(std::move(sfile)));

  log_reader_ = std::make_shared<blob_log::Reader>(db_options.info_log,
    std::move(sfile_reader), nullptr, true, 0, file_number_);

  return log_reader_;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobFile::WriteFooterAndClose_locked()
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
    file_size_ += blob_log::BlobLogFooter::kFooterSize;
  }
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
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

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobFile::Fsync_member() {
  if (log_writer_.get()) {
    log_writer_->Sync();
  }
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobFile::Fsync(void *arg) {

  return;
  BlobFile *bfile = static_cast<BlobFile*>(arg);
  assert(bfile != nullptr);
  bfile->Fsync_member();
}

////////////////////////////////////////////////////////////////////////////////
// Should take a WriteLock on the mutex_
//
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<RandomAccessFileReader> BlobFile::openRandomAccess_locked(
  Env *env, const EnvOptions& env_options)
{
  if (ra_file_reader_)
    return ra_file_reader_;

  std::unique_ptr<RandomAccessFile> rfile;
  Status s = env->NewRandomAccessFile(PathName(), &rfile, env_options);
  if (!s.ok()) {
    return nullptr;
  }

  ra_file_reader_ = std::make_shared<RandomAccessFileReader>(std::move(rfile));
  return ra_file_reader_;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobFile::ReadHeader() {

  Status s = log_reader_->ReadHeader(header_);
  if (s.ok()) {
    header_read_ = true;
  }
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
ColumnFamilyHandle* BlobFile::GetColumnFamily(DB *db) {
  return db->DefaultColumnFamily();
}

}  // namespace rocksdb
