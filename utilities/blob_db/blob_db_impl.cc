// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "utilities/blob_db/blob_db_impl.h"
#include <chrono>
#include <cinttypes>
#include <ctime>
#include <iomanip>
#include <limits>
#include <memory>

#include "db/db_impl.h"
#include "db/filename.h"
#include "db/write_batch_internal.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/stackable_db.h"
#include "rocksdb/utilities/transaction.h"
#include "table/block.h"
#include "table/block_based_table_builder.h"
#include "table/block_builder.h"
#include "table/meta_blocks.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/instrumented_mutex.h"
#include "util/timer_queue.h"
#include "utilities/transactions/optimistic_transaction_db_impl.h"
#include "utilities/transactions/optimistic_transaction_impl.h"

using std::chrono::system_clock;

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

Random blob_rgen(time(NULL));

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobDBFlushBeginListener::OnFlushBegin(DB *db,
  const FlushJobInfo& info) {
  if (impl_)
    impl_->OnFlushBeginHandler(db, info);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
WalFilter::WalProcessingOption BlobReconcileWalFilter::LogRecordFound(
  unsigned long long log_number, const std::string& log_file_name,
  const WriteBatch& batch, WriteBatch* new_batch, bool* batch_changed)
{
  return WalFilter::WalProcessingOption::kContinueProcessing;
}

////////////////////////////////////////////////////////////////////////////////
// Sort the blob files based on start of the TTL range.
//
////////////////////////////////////////////////////////////////////////////////
bool blobf_compare_ttl::operator() (const std::shared_ptr<BlobFile>& lhs,
  const std::shared_ptr<BlobFile>& rhs) const {
  return lhs->ttl_range_.first < rhs->ttl_range_.first;
}

////////////////////////////////////////////////////////////////////////////////
// Destroy the BlobDB completely
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDB::DestroyBlobDB(const std::string& dbname, const Options& options,
  const BlobDBOptions& bdb_options) {
  const ImmutableDBOptions soptions(SanitizeOptions(dbname, options));
  Env* env = soptions.env;

  Status result;
  std::string blobdir;
  blobdir = (bdb_options.path_relative)  ? dbname +
    "/" + bdb_options.blob_dir : bdb_options.blob_dir;

  std::vector<std::string> filenames;
  Status status = env->GetChildren(blobdir, &filenames);

  for (const auto& f : filenames) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kBlobFile) {
      Status del = env->DeleteFile(blobdir + "/" + f);
      if (result.ok() && !del.ok()) {
        result = del;
      }
    }
  }

  env->DeleteDir(blobdir);
  return result;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
BlobDBImpl::BlobDBImpl(const std::string& dbname,
  const BlobDBOptions& blob_db_options, const Options& options,
  const DBOptions &db_options, const EnvOptions& env_options)
  : BlobDB(nullptr),
    db_impl_(nullptr),
    myenv_(options.env),
    wo_set_(false),
    bdb_options_(blob_db_options),
    ioptions_(options),
    db_options_(db_options),
    env_options_(env_options),
    next_file_number_(1),
    epoch_of_(0),
    shutdown_(false),
    current_epoch_(0),
    open_file_count_(0),
    last_period_write_(0),
    last_period_ampl_(0),
    total_periods_write_(0),
    total_periods_ampl_(0),
    total_blob_space_(0),
    open_p1_done_(false) {
  blob_dir_ = (bdb_options_.path_relative)  ? dbname +
    "/" + bdb_options_.blob_dir : bdb_options_.blob_dir;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::LinkToBaseDB(DB *db) {
  assert(db_ == nullptr);
  assert(open_p1_done_);

  db_ = db;
  db_impl_ = dynamic_cast<DBImpl*>(db);

  myenv_ = db_->GetEnv();
  ioptions_ = ImmutableCFOptions(db->GetOptions());
  db_options_ = db->GetOptions();
  env_options_ = EnvOptions(db->GetOptions());

  opt_db_ = std::make_shared<OptimisticTransactionDBImpl>(db, false);

  Status s = myenv_->CreateDirIfMissing(blob_dir_);

  startBackgroundTasks();
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
BlobDBImpl::BlobDBImpl(DB* db, const BlobDBOptions& blob_db_options)
    : BlobDB(db),
      db_impl_(dynamic_cast<DBImpl*>(db)),
      opt_db_(new OptimisticTransactionDBImpl(db, false)),
      wo_set_(false),
      bdb_options_(blob_db_options),
      ioptions_(db->GetOptions()),
      db_options_(db->GetOptions()),
      env_options_(db_->GetOptions()),
      next_file_number_(1),
      epoch_of_(0),
      shutdown_(false),
      current_epoch_(0),
      open_file_count_(0),
      last_period_write_(0),
      last_period_ampl_(0),
      total_periods_write_(0),
      total_periods_ampl_(0),
      total_blob_space_(0) {
  assert (db_impl_ != nullptr);
  if (!bdb_options_.blob_dir.empty())
    blob_dir_ = (bdb_options_.path_relative)  ? db_->GetName() +
      "/" + bdb_options_.blob_dir : bdb_options_.blob_dir;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
BlobDBImpl::~BlobDBImpl()
{
  CancelAllBackgroundWork(db_, true);

  shutdown();
}

////////////////////////////////////////////////////////////////////////////////
// this is opening of the entire BlobDB.
// go through the directory and do an fstat on all the files.
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::openPhase1() {
  assert(db_ == nullptr);
  if (blob_dir_.empty())
    return Status::NotSupported("No blob directory in options");

  std::unique_ptr<Directory> dir_ent;
  Status s = myenv_->NewDirectory(blob_dir_, &dir_ent);
  if (!s.ok()) {
    open_p1_done_ = true;
    return Status::OK();
  }

  s = openAllFiles();
  open_p1_done_ = true;
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobDBImpl::startBackgroundTasks() {
  // store a call to a member function and object
  using std::placeholders::_1;
  tqueue_.add(bdb_options_.reclaim_of_period,
    std::bind(&BlobDBImpl::reclaimOpenFiles, this, _1));
  tqueue_.add(bdb_options_.gc_check_period,
    std::bind(&BlobDBImpl::runGC, this, _1));
  tqueue_.add(bdb_options_.deletion_check_period,
    std::bind(&BlobDBImpl::evictDeletions, this, _1));
  tqueue_.add(bdb_options_.delete_obsf_period,
    std::bind(&BlobDBImpl::deleteObsFiles, this, _1));
  tqueue_.add(bdb_options_.sanity_check_period,
    std::bind(&BlobDBImpl::sanityCheck, this, _1));
  tqueue_.add(bdb_options_.wa_stats_period,
    std::bind(&BlobDBImpl::waStats, this, _1));
  tqueue_.add(bdb_options_.fsync_files_period,
    std::bind(&BlobDBImpl::fsyncFiles, this, _1));
  tqueue_.add(bdb_options_.check_seqf_period,
    std::bind(&BlobDBImpl::checkSeqFiles, this, _1));
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobDBImpl::shutdown()
{
  shutdown_.store(true);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobDBImpl::OnFlushBeginHandler(DB *db, const FlushJobInfo& info) {

  if (shutdown_.load())
    return;

  // a callback that happens too soon needs to be ignored
  if (!db_)
    return;

  fsyncFiles(false);
}

////////////////////////////////////////////////////////////////////////////////
// Maybe Parallelize later
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::getAllLogFiles(
    std::set<std::pair<uint64_t, std::string> >* file_nums) {
  std::vector<std::string> all_files;
  Status status = myenv_->GetChildren(blob_dir_, &all_files);
  if (!status.ok()) {
    return status;
  }

  for (const auto& f : all_files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kBlobFile) {
      file_nums->insert(std::make_pair(number, f));
    } else {
      Log(InfoLogLevel::WARN_LEVEL, db_options_.info_log,
        "Skipping file in blob directory %s", f.c_str());
    }
  }

  return status;
}

////////////////////////////////////////////////////////////////////////////////
// Maybe Parallelize later
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::openAllFiles() {
  WriteLock wl(&mutex_);

  std::set<std::pair<uint64_t, std::string> > file_nums;
  Status status = getAllLogFiles(&file_nums);
  if (!status.ok()) return status;

  if (!file_nums.empty()) {
    next_file_number_.store((file_nums.rbegin())->first + 1);
  }

  for (auto f_iter: file_nums) {
    std::string bfpath = BlobFileName(blob_dir_, f_iter.first);
    uint64_t size_bytes;
    Status s1 = myenv_->GetFileSize(bfpath, &size_bytes);
    if (!s1.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, db_options_.info_log,
        "Not able to get size of file %s", bfpath.c_str());
      continue;
    }

    std::shared_ptr<BlobFile> bfptr
      = std::make_shared<BlobFile>(blob_dir_, f_iter.first);

    bfptr->setFileSize(size_bytes);

    WriteLock wl_f(&bfptr->mutex_);
    std::shared_ptr<RandomAccessFileReader> ra_reader =
      openRandomAccess_locked(bfptr, myenv_, env_options_);

    blob_log::BlobLogFooter bf;
    s1 = bfptr->readFooter_locked(bf);
    bfptr->closeRandomAccess_locked();
    if (s1.ok()) {
      bfptr->setFromFooter_locked(bf);
    } else {
      Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
        "File found incomplete (w/o footer) %s", bfpath.c_str());

      // sequentially iterate over the file and read all the records
      std::shared_ptr<blob_log::Reader> reader;
      reader = bfptr->openSequentialReader(myenv_,
        db_options_, env_options_);

      blob_log::BlobLogHeader hdr;
      s1 = reader->ReadHeader(hdr);
      if (!s1.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, db_options_.info_log,
          "Failure to read Header for blob-file %s", bfpath.c_str());
        continue;
      }

      bfptr->setHeader_locked(hdr);

      ttlrange_t ttl_range(std::numeric_limits<uint32_t>::max(),
        std::numeric_limits<uint32_t>::min());
      tsrange_t ts_range(std::numeric_limits<uint32_t>::max(),
        std::numeric_limits<uint32_t>::min());
      snrange_t sn_range(std::numeric_limits<SequenceNumber>::max(),
        std::numeric_limits<SequenceNumber>::min());

      uint64_t blob_count = 0;
      blob_log::BlobLogRecord record;
      blob_log::Reader::READ_LEVEL shallow =
          blob_log::Reader::READ_LEVEL_HDR_FOOTER_KEY;

      // TBD - when we detect corruption, we should truncate
      while (reader->ReadRecord(&record, shallow).ok()) {
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
        ttl_range.second  = std::max(ttl_range.second,
          ttl_range.first + (uint32_t)bdb_options_.ttl_range);
        bfptr->setTTLRange(ttl_range);
        std::time_t epoch_now = system_clock::to_time_t(system_clock::now());
        if (ttl_range.second < epoch_now) {
          Status fstatus = createWriter_locked(bfptr, true);
          if (fstatus.ok())
            fstatus = bfptr->writeFooterAndClose_locked();
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
Status BlobFile::readFooter_locked(blob_log::BlobLogFooter& bf) {

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

  s = bf.DecodeFrom(&result);
  return s;
}

////////////////////////////////////////////////////////////////////////////////
// Should take a WriteLock on the mutex_
//
////////////////////////////////////////////////////////////////////////////////
void BlobDBImpl::closeRandomAccess_locked(std::shared_ptr<BlobFile>& bfile) {
  bfile->closeRandomAccess_locked();
  open_file_count_--;
}

////////////////////////////////////////////////////////////////////////////////
// Write mutex on file needs to be held
//
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<RandomAccessFileReader> BlobDBImpl::openRandomAccess_locked(
  std::shared_ptr<BlobFile> &bfile, Env *env, const EnvOptions& env_options) {
  bool fresh_open = false;
  auto rar = bfile->openRandomAccess_locked(env, env_options, &fresh_open);
  if (fresh_open)
    open_file_count_++;
  return rar;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<BlobFile> BlobDBImpl::newBlobFile()
{
  uint64_t file_num = next_file_number_++;
  return std::make_shared<BlobFile>(blob_dir_, file_num);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::createWriter_locked(std::shared_ptr<BlobFile>& bfile,
  bool reopen) {
  std::string fpath(bfile->PathName());
  std::unique_ptr<WritableFile> wfile;

  Status s = (reopen) ?
   myenv_->ReopenWritableFile(fpath, &wfile, env_options_) :
   myenv_->NewWritableFile(fpath, &wfile, env_options_);

  if (!s.ok())
    return s;

  std::unique_ptr<WritableFileWriter> fwriter;
  fwriter.reset(new WritableFileWriter(std::move(wfile), env_options_));

  uint64_t boffset = bfile->GetFileSize();
  bfile->log_writer_ = std::make_shared<blob_log::Writer>(std::move(fwriter),
    bfile->file_number_, bdb_options_.bytes_per_sync,
    db_options_.use_fsync, boffset);

  if (reopen) {
    blob_log::Writer::ELEM_TYPE et = blob_log::Writer::ET_NONE;
    if (bfile->file_size_ == blob_log::BlobLogHeader::kHeaderSize)
      et = blob_log::Writer::ET_FILE_HDR;
    else if (bfile->file_size_ >  blob_log::BlobLogHeader::kHeaderSize) {
      et = blob_log::Writer::ET_FOOTER;
    }
    bfile->log_writer_->last_elem_type_ = et;
  }

  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<BlobFile> BlobDBImpl::findBlobFile_locked(uint32_t expiration) const
{
  if (open_blob_files_.empty()) return nullptr;

  std::shared_ptr<BlobFile> tmp = std::make_shared<BlobFile>();
  tmp->ttl_range_ = std::make_pair(expiration, 0);

  auto citr = open_blob_files_.equal_range(tmp);
  if (citr.first == open_blob_files_.end()) {
    assert(citr.second == open_blob_files_.end());

    std::shared_ptr<BlobFile> check = *(open_blob_files_.rbegin());
    return (check->ttl_range_.second < expiration) ? nullptr : check;
  }

  if (citr.first != citr.second) return *(citr.first);

  auto finditr = citr.second;
  if (finditr != open_blob_files_.begin()) --finditr;

  bool b2 = (*finditr)->ttl_range_.second < expiration;
  bool b1 = (*finditr)->ttl_range_.first > expiration;

  return (b1 || b2) ? nullptr : (*finditr);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<blob_log::Writer>
  BlobDBImpl::checkOrCreateWriter_locked(std::shared_ptr<BlobFile> &bfile) {
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

////////////////////////////////////////////////////////////////////////////////
// Aims to keep the # of open simple (no-TTL) blob files at the requested
// Opens a new blob file in case not
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<BlobFile> BlobDBImpl::selectBlobFile() {

  uint32_t val = blob_rgen.Next();
  {
    ReadLock rl(&mutex_);
    if (open_simple_files_.size() == bdb_options_.num_concurrent_simple_blobs)
      return open_simple_files_[val % bdb_options_.num_concurrent_simple_blobs];
  }

  std::shared_ptr<BlobFile> bfile = newBlobFile();
  assert(bfile);

  // file not visible, hence no lock
  std::shared_ptr<blob_log::Writer> writer = checkOrCreateWriter_locked(bfile);

  blob_log::BlobLogHeader header;
  bfile->setHeader_locked(header);

  bfile->file_size_ = blob_log::BlobLogHeader::kHeaderSize;

  // CHECK again
  WriteLock wl(&mutex_);
  if (open_simple_files_.size() == bdb_options_.num_concurrent_simple_blobs) {
    return open_simple_files_[val % bdb_options_.num_concurrent_simple_blobs];
  }

  Status s = writer->WriteHeader(header);
  if (!s.ok())
    return nullptr;

  blob_files_.insert(std::make_pair(bfile->BlobFileNumber(), bfile));
  open_simple_files_.push_back(bfile);
  return bfile;
}

////////////////////////////////////////////////////////////////////////////////
// Finds based on the expiration time, an existing open blob file
// If it doesn't find, opens a new one
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<BlobFile> BlobDBImpl::selectBlobFileTTL(uint32_t expiration)
{
  uint64_t epoch_read = 0;
  std::shared_ptr<BlobFile> bfile;
  {
    ReadLock rl(&mutex_);
    bfile = findBlobFile_locked(expiration);
    epoch_read = epoch_of_.load();
  }

  if (bfile)
    return bfile;

  uint32_t exp_low =
      (expiration / bdb_options_.ttl_range) * bdb_options_.ttl_range;
  ttlrange_t ttl_guess =
      std::make_pair(exp_low, exp_low + bdb_options_.ttl_range);

  bfile = newBlobFile();
  assert(bfile);

  // we don't need to take lock as no other thread is seeing bfile yet
  std::shared_ptr<blob_log::Writer> writer = checkOrCreateWriter_locked(bfile);

  blob_log::BlobLogHeader header;
  header.setTTLGuess(ttl_guess);
  bfile->setHeader_locked(header);

  bfile->file_size_ = blob_log::BlobLogHeader::kHeaderSize;

  // set the first value of the range, since that is concrete at this time.
  // also necessary to add to open_blob_files_
  bfile->ttl_range_ = ttl_guess;

  WriteLock wl(&mutex_);
  // in case the epoch has shifted in the interim, then check
  // check condition again - should be rare.
  if (epoch_of_.load() != epoch_read) {
    auto bfile2 = findBlobFile_locked(expiration);
    if (bfile2) return bfile2;
  }

  Status s = writer->WriteHeader(header);
  if (!s.ok())
    return nullptr;

  blob_files_.insert(std::make_pair(bfile->BlobFileNumber(), bfile));
  open_blob_files_.insert(bfile);
  epoch_of_++;

  return bfile;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
bool BlobDBImpl::extractTTLFromBlob(const Slice& value, Slice *newval,
  int32_t *ttl_val) {
  *newval = value;
  *ttl_val = -1;
  if (value.size() <= BlobDB::kTTLSuffixLength)
    return false;

  int32_t ttl_tmp = DecodeFixed32(value.data() + value.size() - sizeof(int32_t));
  std::string ttl_exp(value.data() + value.size() - BlobDB::kTTLSuffixLength, 4);
  if (ttl_exp != "ttl:")
    return false;

  newval->remove_suffix(BlobDB::kTTLSuffixLength);
  *ttl_val = ttl_tmp;
  return true;
}

////////////////////////////////////////////////////////////////////////////////
// A specific pattern is looked up at the end of the value part.
// ttl:TTLVAL . if this pattern is found, PutWithTTL is called, otherwise
// regular Put is called.
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::Put(const WriteOptions& options,
  ColumnFamilyHandle* column_family, const Slice& key, const Slice& value)
{
  Slice newval;
  int32_t ttl_val;
  extractTTLFromBlob(value, &newval, &ttl_val);

  return PutWithTTL(options, column_family, key, newval, ttl_val);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::Delete(const WriteOptions& options,
  ColumnFamilyHandle* column_family, const Slice& key)
{
  SequenceNumber lsn = db_impl_->GetLatestSequenceNumber();
  Status s = db_->Delete(options, column_family, key);

  // add deleted key to list of keys that have been deleted for book-keeping
  delete_keys_q_.enqueue({column_family, key.ToString(), lsn});
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::SingleDelete(const WriteOptions& wopts,
  ColumnFamilyHandle* column_family, const Slice& key)
{
  SequenceNumber lsn = db_impl_->GetLatestSequenceNumber();
  Status s = db_->SingleDelete(wopts, column_family, key);

  delete_keys_q_.enqueue({column_family, key.ToString(), lsn});
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::Write(const WriteOptions& opts, WriteBatch* updates)
{
  class Handler1 : public WriteBatch::Handler {
   public:
    explicit Handler1(BlobDBImpl *i) : impl(i), previous_put(false) {}

    BlobDBImpl *impl;
    WriteBatch updates_blob;
    Status batch_rewrite_status;
    std::shared_ptr<BlobFile> last_file;
    bool previous_put;

    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
      Slice newval;
      int32_t ttl_val;
      extractTTLFromBlob(value, &newval, &ttl_val);

      int32_t expiration = -1;
      if (ttl_val != -1)
        expiration = ttl_val + system_clock::to_time_t(system_clock::now());
      std::shared_ptr<BlobFile> bfile = (ttl_val != -1) ?
        impl->selectBlobFileTTL(expiration) : impl->selectBlobFile();
      if (last_file && last_file !=  bfile) {
        batch_rewrite_status = Status::NotFound("too many blob files");
        return batch_rewrite_status;
      }

      if (!bfile) {
        batch_rewrite_status = Status::NotFound("blob file not found");
        return batch_rewrite_status;
      }

      char headerbuf[blob_log::BlobLogRecord::kHeaderSize];
      blob_log::Writer::ConstructBlobHeader(headerbuf, key, value, expiration, -1);

      if (previous_put) {
        impl->appendSN(last_file, -1);
        previous_put = false;
      }

      last_file = bfile;

      std::string index_entry;
      Status st = impl->appendBlob(bfile, headerbuf, key, value, &index_entry);

      if (expiration != -1)
        extendTTL(bfile->ttl_range_, (uint32_t)expiration);

      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        previous_put = true;
        WriteBatchInternal::Put(&updates_blob, column_family_id, key,
                                index_entry);
      }
      return Status::OK();
    }

    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) override {
      batch_rewrite_status =
        Status::NotSupported("Not supported operation in blob db.");
      return batch_rewrite_status;
    }

    virtual Status DeleteCF(uint32_t column_family_id,
                            const Slice& key) override {
      WriteBatchInternal::Delete(&updates_blob, column_family_id, key);
      return Status::OK();
    }

    virtual void LogData(const Slice& blob) override {
      updates_blob.PutLogData(blob);
    }

   private:
  };

  Handler1 handler1(this);
  updates->Iterate(&handler1);

  Status s;
  SequenceNumber lsn = db_impl_->GetLatestSequenceNumber();

  if (!handler1.batch_rewrite_status.ok()) {
    return handler1.batch_rewrite_status;
  } else {
    s = db_->Write(opts, &(handler1.updates_blob));
  }

  if (!s.ok())
    return s;

  if (handler1.previous_put) {
    // this is the sequence number of the write.
    SequenceNumber sn = WriteBatchInternal::Sequence(&handler1.updates_blob);
    appendSN(handler1.last_file, sn);

    closeIf(handler1.last_file);
  }

  // add deleted key to list of keys that have been deleted for book-keeping
  class Handler2 : public WriteBatch::Handler {
   public:
    explicit Handler2(BlobDBImpl *i, const SequenceNumber& sn)
      : impl(i), lsn(sn) {}

    virtual Status DeleteCF(uint32_t column_family_id,
                            const Slice& key) override {

      ColumnFamilyHandle* cfh
        = impl->db_impl_->GetColumnFamilyHandleUnlocked(column_family_id);

      impl->delete_keys_q_.enqueue({cfh, key.ToString(), lsn});
      return Status::OK();
    }
   private:
     BlobDBImpl *impl;
     SequenceNumber lsn;
  };

  // add deleted key to list of keys that have been deleted for book-keeping
  Handler2 handler2(this, lsn);
  updates->Iterate(&handler2);

  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::PutWithTTL(const WriteOptions& options,
  ColumnFamilyHandle* column_family, const Slice& key,
  const Slice& value, int32_t ttl)
{
  return PutUntil(options, column_family, key, value,
    (ttl != -1) ? ttl + system_clock::to_time_t(system_clock::now()) : -1);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::PutUntil(const WriteOptions& options,
  ColumnFamilyHandle* column_family, const Slice& key,
  const Slice& value, int32_t expiration) {

  updateWriteOptions(options);

  std::shared_ptr<BlobFile> bfile =
    (expiration != -1) ? selectBlobFileTTL(expiration) :
                         selectBlobFile();

  if (!bfile)
    return Status::NotFound("blob file not found");

  char headerbuf[blob_log::BlobLogRecord::kHeaderSize];
  blob_log::Writer::ConstructBlobHeader(headerbuf, key, value, expiration, -1);

  std::string index_entry;
  Status s = appendBlob(bfile, headerbuf, key, value, &index_entry);

  WriteBatch batch;
  batch.Put(column_family, key, index_entry);

  // this goes to the base db and can be expensive
  s = db_->Write(options, &batch);

  // this is the sequence number of the write.
  SequenceNumber sn = WriteBatchInternal::Sequence(&batch);

  s = appendSN(bfile, sn);

  if (expiration != -1)
    extendTTL(bfile->ttl_range_, (uint32_t)expiration);

  closeIf(bfile);

  return s;
}

////////////////////////////////////////////////////////////////////////////////
// bfile wont be deleted because of shared_ptr
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::appendBlob(std::shared_ptr<BlobFile>& bfile,
  const char *headerbuf, const Slice& key, const Slice& value,
  std::string *index_entry) {
  Status s;

  uint64_t blob_offset = 0;
  uint64_t key_offset = 0;
  {
    WriteLock lockbfile_w(&bfile->mutex_);
    std::shared_ptr<blob_log::Writer> writer = checkOrCreateWriter_locked(bfile);

    // write the blob to the blob log.
    s = writer->EmitPhysicalRecord(headerbuf, key, value,
      &key_offset, &blob_offset);
  }

  if (!s.ok())
    return s;

  // increment blob count
  bfile->blob_count_++;
  auto size_put =
    blob_log::BlobLogRecord::kHeaderSize
    + key.size() + value.size();

  bfile->file_size_ += size_put;
  last_period_write_ += size_put;
  total_blob_space_ += size_put;

  PutVarint64(index_entry, bfile->BlobFileNumber());
  BlockHandle handle;
  handle.set_size(value.size());
  handle.set_offset(blob_offset);
  handle.EncodeTo(index_entry);

  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobDBImpl::appendSN(std::shared_ptr<BlobFile>& bfile,
  const SequenceNumber& sn) {
  Status s;
  {
    WriteLock lockbfile(&bfile->mutex_);
    std::shared_ptr<blob_log::Writer> writer = checkOrCreateWriter_locked(bfile);
    s = writer->AddRecordFooter(sn);
    if (!s.ok())
      return s;

    if (sn != std::numeric_limits<SequenceNumber>::max())
      extendSN(bfile->sn_range_, sn);
  }

  bfile->file_size_ += blob_log::BlobLogRecord::kFooterSize;
  last_period_write_ += blob_log::BlobLogRecord::kFooterSize;
  total_blob_space_ += blob_log::BlobLogRecord::kFooterSize;
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::vector<Status> BlobDBImpl::MultiGet( const ReadOptions& options,
  const std::vector<ColumnFamilyHandle*>& column_family,
  const std::vector<Slice>& keys, std::vector<std::string>* values) {

  auto statuses = db_->MultiGet(options, column_family, keys, values);

  for (size_t i = 0; i < keys.size(); ++i) {
    if (!statuses[i].ok())
      continue;

    BlockHandle handle;
    Slice index_entry_slice((*values)[i]);
    uint64_t file_number;
    if (!GetVarint64(&index_entry_slice, &file_number)) {
      statuses[i] = Status::Corruption();
      continue;
    }

    statuses[i] = handle.DecodeFrom(&index_entry_slice);
    if (!statuses[i].ok())
      continue;

    std::shared_ptr<BlobFile> bfile;
    {
      ReadLock l(&mutex_);
      auto hitr = blob_files_.find(file_number);

      // file was deleted
      if (hitr == blob_files_.end()) {
        statuses[i] = Status::NotFound("Blob Not Found");
        continue;
      }

      bfile = hitr->second;
    }

    std::shared_ptr<RandomAccessFileReader> reader;
    {
      WriteLock lockbfile_w(&bfile->mutex_);
      reader = openRandomAccess_locked(bfile, myenv_, env_options_);
    }

    // allocate the buffer
    (*values)[i].resize(handle.size());
    char *buffer = &((*values)[i])[0];

    Slice blob_value;
    statuses[i] = reader->Read(handle.offset(), handle.size(), &blob_value, buffer);
  }
  return statuses;
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
    reader = openRandomAccess_locked(bfile, myenv_, env_options_);
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
std::pair<bool, int64_t> BlobDBImpl::sanityCheck(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
    "Starting Sanity Check");

  Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
    "Number of files %" PRIu64, blob_files_.size());

  Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
    "Number of open files %" PRIu64, open_blob_files_.size());

  for (auto bfile: open_blob_files_) {
    assert(!bfile->Immutable());
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

  // reschedule
  return std::make_pair(true, -1);
}

////////////////////////////////////////////////////////////////////////////////
// First remove the file from open files list, to prevent any more selection
// Then append footer and close the file
////////////////////////////////////////////////////////////////////////////////
std::pair<bool, int64_t> BlobDBImpl::closeSeqWrite(
    std::shared_ptr<BlobFile> bfile, bool aborted) {
  {
    WriteLock wl(&mutex_);

    // this prevents others from picking up this file
    open_blob_files_.erase(bfile);
    epoch_of_++;

    auto findit = std::find(open_simple_files_.begin(),
      open_simple_files_.end(), bfile);
    if (findit != open_simple_files_.end())
      open_simple_files_.erase(findit);
  }

  if (!bfile->closed_.load()) {
    WriteLock wl_f(&bfile->mutex_);
    bfile->writeFooterAndClose_locked();
  }

  return std::make_pair(false, -1);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobDBImpl::closeIf(const std::shared_ptr<BlobFile>& bfile) {
  // atomic read
  bool close = bfile->GetFileSize() > bdb_options_.blob_file_size;
  if (!close) return;

  using std::placeholders::_1;
  tqueue_.add(0, std::bind(&BlobDBImpl::closeSeqWrite, this, bfile, _1));
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
bool BlobDBImpl::FileDeleteOk_SnapshotCheck_locked(const std::shared_ptr<BlobFile>& bfile) {
  assert(bfile->Obsolete());

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
    return true;
  }
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::pair<bool, int64_t> BlobDBImpl::evictDeletions(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  ColumnFamilyHandle *last_cfh = nullptr;
  Options last_op;

  Arena arena;
  ScopedArenaIterator iter;
  RangeDelAggregator range_del_agg(
      InternalKeyComparator(ioptions_.user_comparator), {} /* snapshots */);

  delete_packet_t dpacket;
  while (delete_keys_q_.dequeue(dpacket)) {
    if (last_cfh != dpacket.cfh_) {
      // this can be expensive
      last_cfh = dpacket.cfh_;
      last_op = db_impl_->GetOptions(last_cfh);
      iter.set(
          db_impl_->NewInternalIterator(&arena, &range_del_agg, dpacket.cfh_));
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
  return std::make_pair(true, -1);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::pair<bool, int64_t> BlobDBImpl::checkSeqFiles(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  std::vector<std::shared_ptr<BlobFile>> process_files;
  {
    ReadLock l(&mutex_);
    for (auto bfile: open_blob_files_) {
      {
        ReadLock rl_f(&bfile->mutex_);

        std::time_t epoch_now = system_clock::to_time_t(system_clock::now());
        if (bfile->ttl_range_.second > epoch_now)
          continue;
        process_files.push_back(bfile);
      }
    }
  }

  for (auto bfile: process_files)
    closeSeqWrite(bfile, false);

  return std::make_pair(true, -1);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::pair<bool, int64_t> BlobDBImpl::fsyncFiles(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  std::vector<std::shared_ptr<BlobFile>> process_files;
  {
    ReadLock l(&mutex_);
    for (auto fitr: open_blob_files_) {
      if (fitr->NeedsFsync(true, bdb_options_.bytes_per_sync))
        process_files.push_back(fitr);
    }

    for (auto fitr: open_simple_files_) {
      if (fitr->NeedsFsync(true, bdb_options_.bytes_per_sync))
        process_files.push_back(fitr);
    }
  }

  for (auto fitr : process_files) {
    if (fitr->NeedsFsync(true, bdb_options_.bytes_per_sync))
      fitr->Fsync();
  }

  return std::make_pair(true, -1);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::pair<bool, int64_t> BlobDBImpl::reclaimOpenFiles(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  if (open_file_count_.load() < bdb_options_.open_files_trigger)
    return std::make_pair(true, -1);

  // in the future, we should sort by last_access_
  // instead of closing every file
  ReadLock l(&mutex_);
  for (auto const& ent : blob_files_) {
    auto bfile = ent.second;
    if (bfile->last_access_.load() == -1) continue;

    WriteLock lockbfile_w(&bfile->mutex_);
    closeRandomAccess_locked(bfile);
  }

  return std::make_pair(true, -1);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::pair<bool, int64_t> BlobDBImpl::waStats(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  WriteLock wl(&mutex_);

  if (all_periods_write_.size() < bdb_options_.wa_num_stats_periods) {
    total_periods_write_ -= (*all_periods_write_.begin());
    total_periods_ampl_ = (*all_periods_ampl_.begin());

    all_periods_write_.pop_front();
    all_periods_ampl_.pop_front();
  }

  uint64_t val1 = last_period_write_.load();
  uint64_t val2 = last_period_ampl_.load();

  all_periods_write_.push_back(val1);
  all_periods_ampl_.push_back(val2);

  last_period_write_ = 0;
  last_period_ampl_ = 0;

  total_periods_write_ += val1;
  total_periods_ampl_ += val2;

  return std::make_pair(true, -1);
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
Status BlobDBImpl::writeBatchOfDeleteKeys(std::shared_ptr<BlobFile>& bfptr,
  std::time_t tt)
{
  // ensure that a sequential reader is available
  std::shared_ptr<blob_log::Reader> reader;
  {
    WriteLock lockbfile_w(&(bfptr->mutex_));
    // sequentially iterate over the file and read all the records
    reader = bfptr->openSequentialReader(myenv_, db_options_,
      env_options_);
    if (!reader) {
      // report something here.
      Log(InfoLogLevel::ERROR_LEVEL, db_options_.info_log,
         "File sequential reader could not be opened", bfptr->PathName().c_str());
      return Status::IOError("failed to create sequential reader");
    }
  }

  ReadLock rlock(&bfptr->mutex_);

  blob_log::BlobLogHeader header;
  Status s = reader->ReadHeader(header);
  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, db_options_.info_log,
      "Failure to read Header for blob-file %s", bfptr->PathName().c_str());
    return s;
  }

  ColumnFamilyHandle *cfh = bfptr->GetColumnFamily(db_);
  auto cfhi = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh);
  auto cfd = cfhi->cfd();
  bool has_ttl = header.HasTTL();

  uint64_t blob_count = 0;
  blob_log::BlobLogRecord record;

  // this reads the key but skips the blob
  blob_log::Reader::READ_LEVEL shallow =
      blob_log::Reader::READ_LEVEL_HDR_FOOTER_KEY;

  SuperVersion* sv = db_impl_->GetAndRefSuperVersion(cfd);
  if (sv == nullptr) {
    Status result = Status::InvalidArgument("Could not access column family 0");
    return result;
  }

  assert (opt_db_);
  Transaction* txn = static_cast<OptimisticTransactionDB*>(opt_db_.get())->BeginTransaction(write_options_);
  OptimisticTransactionImpl *otxn = dynamic_cast<OptimisticTransactionImpl*>(txn);
  assert (otxn != nullptr);

  bool no_relocation_ttl = (has_ttl && tt > bfptr->GetTTLRange().second);
  bool no_relocation_lsmdel = (bfptr->GetFileSize() ==
    (bfptr->deleted_size_ + blob_log::BlobLogFooter::kFooterSize));

  bool no_relocation = no_relocation_ttl || no_relocation_lsmdel;

  std::shared_ptr<BlobFile> newfile;
  std::shared_ptr<blob_log::Writer> new_writer;

  if (!no_relocation) {
    newfile = newBlobFile();
    new_writer = checkOrCreateWriter_locked(newfile);
    newfile->setHeader_locked(header);
    newfile->file_size_ = blob_log::BlobLogHeader::kHeaderSize;
    s = new_writer->WriteHeader(newfile->header_);

    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, db_options_.info_log,
          "File: %s - header writing failed", newfile->PathName().c_str());
      no_relocation = true;
      // should be a much severe error
    }

    // read the blob because you have to write it back to
    // new file
    shallow = blob_log::Reader::READ_LEVEL_HDR_FOOTER_KEY_BLOB;
  }

  uint32_t num_deletes = 0;

  while (reader->ReadRecord(&record, shallow).ok()) {
    ++blob_count;

    // this particular TTL has expired
    if (no_relocation_ttl || (has_ttl && tt > record.GetTTL())) {
      txn->Delete(cfh, record.Key());
      num_deletes++;
      continue;
    }

    SequenceNumber seq = kMaxSequenceNumber;
    bool found_record_for_key = false;
    s = db_impl_->GetLatestSequenceForKey(sv, record.Key(), false,
      &seq, &found_record_for_key);

    bool del_this = s.ok() && (!found_record_for_key || seq !=
      record.GetSN());

    if (del_this) {
      // stil could have a TOCTOU
      txn->Delete(cfh, record.Key());
      num_deletes++;
      continue;
    }

    {
      std::string index_entry;
      PutVarint64(&index_entry, newfile->BlobFileNumber());

      BlockHandle handle;
      auto raw_block_size = record.Blob().size();
      handle.set_size(raw_block_size);

      // these are returned values
      uint64_t blob_offset = 0;
      uint64_t key_offset = 0;
      // write the blob to the blob log.
      s = new_writer->AddRecord(record.Key(), record.Blob(),
        &key_offset, &blob_offset, record.GetTTL());

      new_writer->AddRecordFooter(record.GetSN());
      newfile->blob_count_++;
      newfile->file_size_ += blob_log::BlobLogRecord::kHeaderSize +
        record.Key().size() + record.Blob().size() +
        blob_log::BlobLogRecord::kFooterSize;

      handle.set_offset(blob_offset);
      handle.EncodeTo(&index_entry);

      txn->Put(cfh, record.Key(), index_entry);
    }
  }

  if (!no_relocation)
    total_blob_space_ += newfile->file_size_;

  if (num_deletes != 0) {
    Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
        "File: %s Number of deletes %d", bfptr->PathName().c_str(), num_deletes);
  }

  // Now write the
  db_impl_->ReturnAndCleanupSuperVersion(cfd, sv);

  s = txn->Commit();

  delete txn;

  // if this fails, we should try to preserve the write batch.
  if (s.IsBusy()) {
    Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
        "Optimistic transaction failed: %s", bfptr->PathName().c_str());
    return s;
  }

  if (s.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
        "Successfully added deletes back into LSM: %s",
        bfptr->PathName().c_str());
    // we are done.
  }

  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
bool BlobDBImpl::shouldGCFile_locked(std::shared_ptr<BlobFile> bfile,
  std::time_t tt, uint64_t last_id, std::string *reason) {
  if (bfile->HasTTL()) {
    ttlrange_t ttl_range = bfile->GetTTLRange();
    if (tt > ttl_range.second) {
      *reason = "entire file ttl expired";
      return true;
    }

    if (bdb_options_.ttl_range < bdb_options_.partial_expiration_gc_range) {
      *reason = "has ttl but partial expiration not turned on";
      return false;
    }

    if (!bfile->file_size_.load()) {
      Log(InfoLogLevel::ERROR_LEVEL, db_options_.info_log,
          "Invalid file size = 0 %s", bfile->PathName().c_str());
      *reason = "file is empty";
      return false;
    }

    bool ret = ((bfile->deleted_size_ * 100.0 / bfile->file_size_.load()) >
            bdb_options_.partial_expiration_pct);
    if (ret)
      *reason = "deleted blobs beyond threshold";
    else
      *reason = "deleted blobs below threshold";
    return ret;
  }

  if ((bfile->deleted_size_ * 100.0 / bfile->file_size_.load()) >
      bdb_options_.partial_expiration_pct) {
    *reason = "deleted simple blobs beyond threshold";
    return true;
  }

  // if we haven't reached limits of disk space, don't DELETE
  if (total_blob_space_.load() < bdb_options_.blob_dir_size) {
    *reason = "disk space not exceeded";
    return false;
  }

  bool ret = bfile->BlobFileNumber() == last_id;
  if (ret)
    *reason = "eligible last simple blob file";
  else
    *reason = "not eligible since not last simple blob file";
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::pair<bool, int64_t> BlobDBImpl::deleteObsFiles(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  if (obsolete_files_.empty()) return std::make_pair(true, -1);

  std::list<std::shared_ptr<BlobFile>> tobsolete;
  {
    WriteLock wl(&mutex_);
    tobsolete.swap(obsolete_files_);
  }

  for (auto iter = tobsolete.begin(); iter != tobsolete.end(); ) {
    auto bfile = *iter;
    {
      ReadLock rl(&bfile->mutex_);
      if (!FileDeleteOk_SnapshotCheck_locked(bfile)) {
        ++iter;
        continue;
      }
    }

    Status s = myenv_->DeleteFile(bfile->PathName());
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, db_options_.info_log,
        "File failed to be deleted as obsolete %s",
        bfile->PathName().c_str());
      ++iter;
      continue;
    }

    total_blob_space_ -= bfile->file_size_;
    Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
      "File deleted as obsolete from blob dir %s",
      bfile->PathName().c_str());

    iter = tobsolete.erase(iter);
  }

  if (!tobsolete.empty()) {
    WriteLock wl(&mutex_);
    for (auto bfile : tobsolete)
      obsolete_files_.push_front(bfile);
  }

  if (aborted)
    return std::make_pair(false, -1);
  else
    return std::make_pair(true, -1);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
std::pair<bool, int64_t> BlobDBImpl::runGC(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  current_epoch_++;
  // collect the ID of the last regular file, in case
  // we need to GC it.
  uint64_t last_id = std::numeric_limits<uint64_t>::max();
  std::vector<std::shared_ptr<BlobFile> > blob_files;
  {
    // take a copy
    ReadLock l(&mutex_);
    blob_files.reserve(blob_files_.size());
    for (auto const& ent : blob_files_) {
      blob_files.push_back(ent.second);

      // has ttl is immutable, once set, hence no locks
      if (!ent.second->HasTTL())
        last_id = ent.second->BlobFileNumber();
    }
  }

  if (!blob_files.size()) return std::make_pair(true, -1);

  // 100.0 / 15.0 = 7
  uint64_t next_epoch_increment = static_cast<uint64_t>(
      std::ceil(100 / static_cast<double>(bdb_options_.gc_file_pct)));

  // 15% of files
  uint32_t files_to_collect =
      (bdb_options_.gc_file_pct * blob_files.size()) / 100;

  system_clock::time_point now = system_clock::now();
  std::time_t tt = system_clock::to_time_t(now);

  uint32_t files_processed = 0;

  std::vector<std::shared_ptr<BlobFile> > to_process;
  for (auto bfile : blob_files) {
    // if this is the first time processing the file
    // i.e. gc_epoch == -1, process it.
    // else process the file if its processing epoch matches
    // the current epoch. Typically the #of epochs should be
    // around 5-10
    if (!(bfile->gc_epoch_ == -1 ||
          (uint64_t)bfile->gc_epoch_ == current_epoch_)) {
      continue;
    }

    files_processed++;
    // reset the epoch
    bfile->gc_epoch_ = current_epoch_ + next_epoch_increment;
    to_process.push_back(bfile);
    if (files_processed >= files_to_collect) break;
  }

  // in this collect the set of files, which became obsolete
  std::vector<std::shared_ptr<BlobFile> > obsoletes;
  for (auto bfile : to_process) {
    // File can be obsolete. File can be Open for writes, or closed
    std::string pn = bfile->PathName();

    {
      ReadLock rl_f(&bfile->mutex_);
      // in a previous pass, this file was marked obsolete
      // or this file is still active for appends.
      if (bfile->Obsolete() || !bfile->Immutable()) continue;

      std::string reason;
      bool shouldgc = shouldGCFile_locked(bfile, tt, last_id, &reason);
      if (!shouldgc) {
        Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
          "File has been skipped for GC ttl %s %d %d reason='%s'",
          pn.c_str(), tt, bfile->GetTTLRange().second, reason.c_str());
        continue;
      }

      Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
        "File has been chosen for GC ttl %s %d %d reason='%s'",
        pn.c_str(), tt, bfile->GetTTLRange().second, reason.c_str());
    }

    Status s = writeBatchOfDeleteKeys(bfile, tt);
    if (!s.ok()) continue;

    bfile->setCanBeDeleted();
    obsoletes.push_back(bfile);
  }

  if (!obsoletes.empty()) {
    WriteLock wl(&mutex_);
    for (auto bfile : obsoletes) {
      obsolete_files_.push_front(bfile);
      // remove from global list so writers
      blob_files_.erase(bfile->BlobFileNumber());
    }
  }

  // reschedule
  return std::make_pair(true, -1);
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
BlobFile::BlobFile()
    : file_number_(0),
      blob_count_(0),
      gc_epoch_(-1),
      file_size_(0),
      deleted_count_(0),
      deleted_size_(0),
      closed_(false),
      can_be_deleted_(false),
      ttl_range_(std::make_pair(0, 0)),
      time_range_(std::make_pair(0, 0)),
      sn_range_(std::make_pair(0, 0)),
      last_access_(-1),
      last_fsync_(0),
      header_valid_(false) {}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
BlobFile::BlobFile(const std::string& bdir, uint64_t fn)
    : path_to_dir_(bdir),
      file_number_(fn),
      blob_count_(0),
      gc_epoch_(-1),
      file_size_(0),
      deleted_count_(0),
      deleted_size_(0),
      closed_(false),
      can_be_deleted_(false),
      ttl_range_(std::make_pair(0, 0)),
      time_range_(std::make_pair(0, 0)),
      sn_range_(std::make_pair(0, 0)),
      last_access_(-1),
      last_fsync_(0),
      header_valid_(false) {}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
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
std::shared_ptr<blob_log::Reader> BlobFile::openSequentialReader(
  Env *env, const DBOptions& db_options, const EnvOptions& env_options) const {
  std::unique_ptr<SequentialFile> sfile;
  Status s = env->NewSequentialFile(PathName(), &sfile, env_options);
  if (!s.ok()) {
    // report something here.
    return nullptr;
  }

  std::unique_ptr<SequentialFileReader> sfile_reader;
  sfile_reader.reset(new SequentialFileReader(std::move(sfile)));

  std::shared_ptr<blob_log::Reader> log_reader =
    std::make_shared<blob_log::Reader>(db_options.info_log,
    std::move(sfile_reader), nullptr, true, 0, file_number_);

  return log_reader;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
bool BlobFile::NeedsFsync(bool hard, uint64_t bytes_per_sync) const {
  assert (last_fsync_ <= file_size_);
  return (hard) ? file_size_ > last_fsync_ :
    (file_size_ - last_fsync_) >= bytes_per_sync;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
Status BlobFile::writeFooterAndClose_locked()
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

  // delete the sequential writer
  log_writer_.reset();
  return s;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobFile::setHeader_locked(const blob_log::BlobLogHeader& hdr) {
  header_ = hdr;
  header_valid_ = true;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
void BlobFile::setFromFooter_locked(const blob_log::BlobLogFooter& footer)
{
  blob_log::BlobLogHeader hdr;
  hdr.setTTL(footer.HasTTL());
  hdr.setTimestamps(footer.HasTimestamps());
  setHeader_locked(hdr);

  // assume that file has been fully fsync'd
  last_fsync_.store(file_size_);
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
void BlobFile::Fsync() {
  if (log_writer_.get()) {
    log_writer_->Sync();
    last_fsync_.store(file_size_.load());
  }
}

////////////////////////////////////////////////////////////////////////////////
// Should take a WriteLock on the mutex_
//
////////////////////////////////////////////////////////////////////////////////
void BlobFile::closeRandomAccess_locked() {
  ra_file_reader_.reset();
  last_access_ = -1;
}

////////////////////////////////////////////////////////////////////////////////
// Should take a WriteLock on the mutex_
//
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<RandomAccessFileReader> BlobFile::openRandomAccess_locked(
  Env *env, const EnvOptions& env_options, bool* fresh_open) {
  *fresh_open = false;
  last_access_ = system_clock::to_time_t(system_clock::now());
  if (ra_file_reader_)
    return ra_file_reader_;

  std::unique_ptr<RandomAccessFile> rfile;
  Status s = env->NewRandomAccessFile(PathName(), &rfile, env_options);
  if (!s.ok()) {
    return nullptr;
  }

  ra_file_reader_ = std::make_shared<RandomAccessFileReader>(std::move(rfile));
  *fresh_open = true;
  return ra_file_reader_;
}

////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////
ColumnFamilyHandle* BlobFile::GetColumnFamily(DB *db) {
  return db->DefaultColumnFamily();
}

}  // namespace rocksdb
