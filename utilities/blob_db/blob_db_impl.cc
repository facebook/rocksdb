//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_db_impl.h"
#include <algorithm>
#include <cinttypes>
#include <iomanip>
#include <limits>
#include <memory>

#include "db/db_impl.h"
#include "db/write_batch_internal.h"
#include "monitoring/instrumented_mutex.h"
#include "monitoring/statistics.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/stackable_db.h"
#include "rocksdb/utilities/transaction.h"
#include "table/block.h"
#include "table/block_based_table_builder.h"
#include "table/block_builder.h"
#include "table/meta_blocks.h"
#include "util/cast_util.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"
#include "util/timer_queue.h"
#include "utilities/blob_db/blob_db_iterator.h"
#include "utilities/blob_db/blob_index.h"

namespace {
int kBlockBasedTableVersionFormat = 2;
}  // end namespace

namespace rocksdb {
namespace blob_db {

Random blob_rgen(static_cast<uint32_t>(time(nullptr)));

void BlobDBFlushBeginListener::OnFlushBegin(DB* db, const FlushJobInfo& info) {
  if (impl_) impl_->OnFlushBeginHandler(db, info);
}

WalFilter::WalProcessingOption BlobReconcileWalFilter::LogRecordFound(
    unsigned long long log_number, const std::string& log_file_name,
    const WriteBatch& batch, WriteBatch* new_batch, bool* batch_changed) {
  return WalFilter::WalProcessingOption::kContinueProcessing;
}

bool blobf_compare_ttl::operator()(const std::shared_ptr<BlobFile>& lhs,
                                   const std::shared_ptr<BlobFile>& rhs) const {
  if (lhs->expiration_range_.first < rhs->expiration_range_.first) {
    return true;
  }
  if (lhs->expiration_range_.first > rhs->expiration_range_.first) {
    return false;
  }
  return lhs->BlobFileNumber() < rhs->BlobFileNumber();
}

void EvictAllVersionsCompactionListener::InternalListener::OnCompaction(
    int level, const Slice& key,
    CompactionEventListener::CompactionListenerValueType value_type,
    const Slice& existing_value, const SequenceNumber& sn, bool is_new) {
  assert(impl_->bdb_options_.enable_garbage_collection);
  if (!is_new &&
      value_type ==
          CompactionEventListener::CompactionListenerValueType::kValue) {
    BlobIndex blob_index;
    Status s = blob_index.DecodeFrom(existing_value);
    if (s.ok()) {
      if (impl_->debug_level_ >= 3)
        ROCKS_LOG_INFO(
            impl_->db_options_.info_log,
            "CALLBACK COMPACTED OUT KEY: %s SN: %d "
            "NEW: %d FN: %" PRIu64 " OFFSET: %" PRIu64 " SIZE: %" PRIu64,
            key.ToString().c_str(), sn, is_new, blob_index.file_number(),
            blob_index.offset(), blob_index.size());

      impl_->override_vals_q_.enqueue({blob_index.file_number(), key.size(),
                                       blob_index.offset(), blob_index.size(),
                                       sn});
    }
  } else {
    if (impl_->debug_level_ >= 3)
      ROCKS_LOG_INFO(impl_->db_options_.info_log,
                     "CALLBACK NEW KEY: %s SN: %d NEW: %d",
                     key.ToString().c_str(), sn, is_new);
  }
}

BlobDBImpl::BlobDBImpl(const std::string& dbname,
                       const BlobDBOptions& blob_db_options,
                       const DBOptions& db_options)
    : BlobDB(nullptr),
      db_impl_(nullptr),
      env_(db_options.env),
      ttl_extractor_(blob_db_options.ttl_extractor.get()),
      bdb_options_(blob_db_options),
      db_options_(db_options),
      env_options_(db_options),
      statistics_(db_options_.statistics.get()),
      dir_change_(false),
      next_file_number_(1),
      epoch_of_(0),
      shutdown_(false),
      current_epoch_(0),
      open_file_count_(0),
      total_blob_space_(0),
      open_p1_done_(false),
      debug_level_(0),
      oldest_file_evicted_(false) {
  blob_dir_ = (bdb_options_.path_relative)
                  ? dbname + "/" + bdb_options_.blob_dir
                  : bdb_options_.blob_dir;
}

Status BlobDBImpl::LinkToBaseDB(DB* db) {
  assert(db_ == nullptr);
  assert(open_p1_done_);

  db_ = db;

  // the Base DB in-itself can be a stackable DB
  db_impl_ = static_cast_with_check<DBImpl, DB>(db_->GetRootDB());

  env_ = db_->GetEnv();

  Status s = env_->CreateDirIfMissing(blob_dir_);
  if (!s.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to create blob directory: %s status: '%s'",
                   blob_dir_.c_str(), s.ToString().c_str());
  }
  s = env_->NewDirectory(blob_dir_, &dir_ent_);
  if (!s.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to open blob directory: %s status: '%s'",
                   blob_dir_.c_str(), s.ToString().c_str());
  }

  if (!bdb_options_.disable_background_tasks) {
    StartBackgroundTasks();
  }
  return s;
}

BlobDBOptions BlobDBImpl::GetBlobDBOptions() const { return bdb_options_; }

BlobDBImpl::BlobDBImpl(DB* db, const BlobDBOptions& blob_db_options)
    : BlobDB(db),
      db_impl_(static_cast_with_check<DBImpl, DB>(db)),
      env_(nullptr),
      ttl_extractor_(nullptr),
      bdb_options_(blob_db_options),
      db_options_(db->GetOptions()),
      env_options_(db_->GetOptions()),
      statistics_(db_options_.statistics.get()),
      dir_change_(false),
      next_file_number_(1),
      epoch_of_(0),
      shutdown_(false),
      current_epoch_(0),
      open_file_count_(0),
      total_blob_space_(0),
      open_p1_done_(false),
      debug_level_(0),
      oldest_file_evicted_(false) {
  if (!bdb_options_.blob_dir.empty())
    blob_dir_ = (bdb_options_.path_relative)
                    ? db_->GetName() + "/" + bdb_options_.blob_dir
                    : bdb_options_.blob_dir;
}

BlobDBImpl::~BlobDBImpl() {
  // CancelAllBackgroundWork(db_, true);

  Shutdown();
}

Status BlobDBImpl::OpenPhase1() {
  assert(db_ == nullptr);
  if (blob_dir_.empty())
    return Status::NotSupported("No blob directory in options");

  std::unique_ptr<Directory> dir_ent;
  Status s = env_->NewDirectory(blob_dir_, &dir_ent);
  if (!s.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to open blob directory: %s status: '%s'",
                   blob_dir_.c_str(), s.ToString().c_str());
    open_p1_done_ = true;
    return Status::OK();
  }

  s = OpenAllFiles();
  open_p1_done_ = true;
  return s;
}

void BlobDBImpl::StartBackgroundTasks() {
  // store a call to a member function and object
  tqueue_.add(
      kReclaimOpenFilesPeriodMillisecs,
      std::bind(&BlobDBImpl::ReclaimOpenFiles, this, std::placeholders::_1));
  tqueue_.add(kGCCheckPeriodMillisecs,
              std::bind(&BlobDBImpl::RunGC, this, std::placeholders::_1));
  if (bdb_options_.enable_garbage_collection) {
    tqueue_.add(
        kDeleteCheckPeriodMillisecs,
        std::bind(&BlobDBImpl::EvictDeletions, this, std::placeholders::_1));
    tqueue_.add(
        kDeleteCheckPeriodMillisecs,
        std::bind(&BlobDBImpl::EvictCompacted, this, std::placeholders::_1));
  }
  tqueue_.add(
      kDeleteObsoleteFilesPeriodMillisecs,
      std::bind(&BlobDBImpl::DeleteObsoleteFiles, this, std::placeholders::_1));
  tqueue_.add(kSanityCheckPeriodMillisecs,
              std::bind(&BlobDBImpl::SanityCheck, this, std::placeholders::_1));
  tqueue_.add(kFSyncFilesPeriodMillisecs,
              std::bind(&BlobDBImpl::FsyncFiles, this, std::placeholders::_1));
  tqueue_.add(
      kCheckSeqFilesPeriodMillisecs,
      std::bind(&BlobDBImpl::CheckSeqFiles, this, std::placeholders::_1));
}

void BlobDBImpl::Shutdown() { shutdown_.store(true); }

void BlobDBImpl::OnFlushBeginHandler(DB* db, const FlushJobInfo& info) {
  if (shutdown_.load()) return;

  // a callback that happens too soon needs to be ignored
  if (!db_) return;

  FsyncFiles(false);
}

Status BlobDBImpl::GetAllLogFiles(
    std::set<std::pair<uint64_t, std::string>>* file_nums) {
  std::vector<std::string> all_files;
  Status status = env_->GetChildren(blob_dir_, &all_files);
  if (!status.ok()) {
    return status;
  }

  for (const auto& f : all_files) {
    uint64_t number;
    FileType type;
    bool psucc = ParseFileName(f, &number, &type);
    if (psucc && type == kBlobFile) {
      file_nums->insert(std::make_pair(number, f));
    } else {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Skipping file in blob directory %s parse: %d type: %d",
                     f.c_str(), psucc, ((psucc) ? type : -1));
    }
  }

  return status;
}

Status BlobDBImpl::OpenAllFiles() {
  WriteLock wl(&mutex_);

  std::set<std::pair<uint64_t, std::string>> file_nums;
  Status status = GetAllLogFiles(&file_nums);

  if (!status.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to collect files from blob dir: %s status: '%s'",
                    blob_dir_.c_str(), status.ToString().c_str());
    return status;
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "BlobDir files path: %s count: %d min: %" PRIu64
                 " max: %" PRIu64,
                 blob_dir_.c_str(), static_cast<int>(file_nums.size()),
                 (file_nums.empty()) ? -1 : file_nums.cbegin()->first,
                 (file_nums.empty()) ? -1 : file_nums.crbegin()->first);

  if (!file_nums.empty())
    next_file_number_.store((file_nums.rbegin())->first + 1);

  for (auto& f_iter : file_nums) {
    std::string bfpath = BlobFileName(blob_dir_, f_iter.first);
    uint64_t size_bytes;
    Status s1 = env_->GetFileSize(bfpath, &size_bytes);
    if (!s1.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "Unable to get size of %s. File skipped from open status: '%s'",
          bfpath.c_str(), s1.ToString().c_str());
      continue;
    }

    if (debug_level_ >= 1)
      ROCKS_LOG_INFO(db_options_.info_log, "Blob File open: %s size: %" PRIu64,
                     bfpath.c_str(), size_bytes);

    std::shared_ptr<BlobFile> bfptr =
        std::make_shared<BlobFile>(this, blob_dir_, f_iter.first);
    bfptr->SetFileSize(size_bytes);

    // since this file already existed, we will try to reconcile
    // deleted count with LSM
    bfptr->gc_once_after_open_ = true;

    // read header
    std::shared_ptr<Reader> reader;
    reader = bfptr->OpenSequentialReader(env_, db_options_, env_options_);
    s1 = reader->ReadHeader(&bfptr->header_);
    if (!s1.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Failure to read header for blob-file %s "
                      "status: '%s' size: %" PRIu64,
                      bfpath.c_str(), s1.ToString().c_str(), size_bytes);
      continue;
    }
    bfptr->SetHasTTL(bfptr->header_.has_ttl);
    bfptr->SetCompression(bfptr->header_.compression);
    bfptr->header_valid_ = true;

    std::shared_ptr<RandomAccessFileReader> ra_reader =
        GetOrOpenRandomAccessReader(bfptr, env_, env_options_);

    BlobLogFooter bf;
    s1 = bfptr->ReadFooter(&bf);

    bfptr->CloseRandomAccessLocked();
    if (s1.ok()) {
      s1 = bfptr->SetFromFooterLocked(bf);
      if (!s1.ok()) {
        ROCKS_LOG_ERROR(db_options_.info_log,
                        "Header Footer mismatch for blob-file %s "
                        "status: '%s' size: %" PRIu64,
                        bfpath.c_str(), s1.ToString().c_str(), size_bytes);
        continue;
      }
    } else {
      ROCKS_LOG_INFO(db_options_.info_log,
                     "File found incomplete (w/o footer) %s", bfpath.c_str());

      // sequentially iterate over the file and read all the records
      ExpirationRange expiration_range(std::numeric_limits<uint32_t>::max(),
                                       std::numeric_limits<uint32_t>::min());

      uint64_t blob_count = 0;
      BlobLogRecord record;
      Reader::ReadLevel shallow = Reader::kReadHeaderKey;

      uint64_t record_start = reader->GetNextByte();
      // TODO(arahut) - when we detect corruption, we should truncate
      while (reader->ReadRecord(&record, shallow).ok()) {
        ++blob_count;
        if (bfptr->HasTTL()) {
          expiration_range.first =
              std::min(expiration_range.first, record.expiration);
          expiration_range.second =
              std::max(expiration_range.second, record.expiration);
        }
        record_start = reader->GetNextByte();
      }

      if (record_start != bfptr->GetFileSize()) {
        ROCKS_LOG_ERROR(db_options_.info_log,
                        "Blob file is corrupted or crashed during write %s"
                        " good_size: %" PRIu64 " file_size: %" PRIu64,
                        bfpath.c_str(), record_start, bfptr->GetFileSize());
      }

      if (!blob_count) {
        ROCKS_LOG_INFO(db_options_.info_log, "BlobCount = 0 in file %s",
                       bfpath.c_str());
        continue;
      }

      bfptr->SetBlobCount(blob_count);
      bfptr->SetSequenceRange({0, 0});

      ROCKS_LOG_INFO(db_options_.info_log,
                     "Blob File: %s blob_count: %" PRIu64
                     " size_bytes: %" PRIu64 " has_ttl: %d",
                     bfpath.c_str(), blob_count, size_bytes, bfptr->HasTTL());

      if (bfptr->HasTTL()) {
        expiration_range.second = std::max(
            expiration_range.second,
            expiration_range.first + (uint32_t)bdb_options_.ttl_range_secs);
        bfptr->set_expiration_range(expiration_range);

        uint64_t now = EpochNow();
        if (expiration_range.second < now) {
          Status fstatus = CreateWriterLocked(bfptr);
          if (fstatus.ok()) fstatus = bfptr->WriteFooterAndCloseLocked();
          if (!fstatus.ok()) {
            ROCKS_LOG_ERROR(
                db_options_.info_log,
                "Failed to close Blob File: %s status: '%s'. Skipped",
                bfpath.c_str(), fstatus.ToString().c_str());
            continue;
          } else {
            ROCKS_LOG_ERROR(
                db_options_.info_log,
                "Blob File Closed: %s now: %d expiration_range: (%d, %d)",
                bfpath.c_str(), now, expiration_range.first,
                expiration_range.second);
          }
        } else {
          open_ttl_files_.insert(bfptr);
        }
      }
    }

    blob_files_.insert(std::make_pair(f_iter.first, bfptr));
  }

  return status;
}

void BlobDBImpl::CloseRandomAccessLocked(
    const std::shared_ptr<BlobFile>& bfile) {
  bfile->CloseRandomAccessLocked();
  open_file_count_--;
}

std::shared_ptr<RandomAccessFileReader> BlobDBImpl::GetOrOpenRandomAccessReader(
    const std::shared_ptr<BlobFile>& bfile, Env* env,
    const EnvOptions& env_options) {
  bool fresh_open = false;
  auto rar = bfile->GetOrOpenRandomAccessReader(env, env_options, &fresh_open);
  if (fresh_open) open_file_count_++;
  return rar;
}

std::shared_ptr<BlobFile> BlobDBImpl::NewBlobFile(const std::string& reason) {
  uint64_t file_num = next_file_number_++;
  auto bfile = std::make_shared<BlobFile>(this, blob_dir_, file_num);
  ROCKS_LOG_DEBUG(db_options_.info_log, "New blob file created: %s reason='%s'",
                  bfile->PathName().c_str(), reason.c_str());
  LogFlush(db_options_.info_log);
  return bfile;
}

Status BlobDBImpl::CreateWriterLocked(const std::shared_ptr<BlobFile>& bfile) {
  std::string fpath(bfile->PathName());
  std::unique_ptr<WritableFile> wfile;

  Status s = env_->ReopenWritableFile(fpath, &wfile, env_options_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to open blob file for write: %s status: '%s'"
                    " exists: '%s'",
                    fpath.c_str(), s.ToString().c_str(),
                    env_->FileExists(fpath).ToString().c_str());
    return s;
  }

  std::unique_ptr<WritableFileWriter> fwriter;
  fwriter.reset(new WritableFileWriter(std::move(wfile), env_options_));

  uint64_t boffset = bfile->GetFileSize();
  if (debug_level_ >= 2 && boffset) {
    ROCKS_LOG_DEBUG(db_options_.info_log, "Open blob file: %s with offset: %d",
                    fpath.c_str(), boffset);
  }

  Writer::ElemType et = Writer::kEtNone;
  if (bfile->file_size_ == BlobLogHeader::kSize) {
    et = Writer::kEtFileHdr;
  } else if (bfile->file_size_ > BlobLogHeader::kSize) {
    et = Writer::kEtRecord;
  } else if (bfile->file_size_) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Open blob file: %s with wrong size: %d", fpath.c_str(),
                   boffset);
    return Status::Corruption("Invalid blob file size");
  }

  bfile->log_writer_ = std::make_shared<Writer>(
      std::move(fwriter), env_, statistics_, bfile->file_number_,
      bdb_options_.bytes_per_sync, db_options_.use_fsync, boffset);
  bfile->log_writer_->last_elem_type_ = et;

  return s;
}

std::shared_ptr<BlobFile> BlobDBImpl::FindBlobFileLocked(
    uint64_t expiration) const {
  if (open_ttl_files_.empty()) return nullptr;

  std::shared_ptr<BlobFile> tmp = std::make_shared<BlobFile>();
  tmp->expiration_range_ = std::make_pair(expiration, 0);

  auto citr = open_ttl_files_.equal_range(tmp);
  if (citr.first == open_ttl_files_.end()) {
    assert(citr.second == open_ttl_files_.end());

    std::shared_ptr<BlobFile> check = *(open_ttl_files_.rbegin());
    return (check->expiration_range_.second < expiration) ? nullptr : check;
  }

  if (citr.first != citr.second) return *(citr.first);

  auto finditr = citr.second;
  if (finditr != open_ttl_files_.begin()) --finditr;

  bool b2 = (*finditr)->expiration_range_.second < expiration;
  bool b1 = (*finditr)->expiration_range_.first > expiration;

  return (b1 || b2) ? nullptr : (*finditr);
}

std::shared_ptr<Writer> BlobDBImpl::CheckOrCreateWriterLocked(
    const std::shared_ptr<BlobFile>& bfile) {
  std::shared_ptr<Writer> writer = bfile->GetWriter();
  if (writer) return writer;

  Status s = CreateWriterLocked(bfile);
  if (!s.ok()) return nullptr;

  writer = bfile->GetWriter();
  return writer;
}

std::shared_ptr<BlobFile> BlobDBImpl::SelectBlobFile() {
  {
    ReadLock rl(&mutex_);
    if (open_non_ttl_file_ != nullptr) {
      return open_non_ttl_file_;
    }
  }

  // CHECK again
  WriteLock wl(&mutex_);
  if (open_non_ttl_file_ != nullptr) {
    return open_non_ttl_file_;
  }

  std::shared_ptr<BlobFile> bfile = NewBlobFile("SelectBlobFile");
  assert(bfile);

  // file not visible, hence no lock
  std::shared_ptr<Writer> writer = CheckOrCreateWriterLocked(bfile);
  if (!writer) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to get writer from blob file: %s",
                    bfile->PathName().c_str());
    return nullptr;
  }

  bfile->file_size_ = BlobLogHeader::kSize;
  bfile->header_.compression = bdb_options_.compression;
  bfile->header_.has_ttl = false;
  bfile->header_.column_family_id =
      reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->GetID();
  bfile->header_valid_ = true;
  bfile->SetHasTTL(false);
  bfile->SetCompression(bdb_options_.compression);

  Status s = writer->WriteHeader(bfile->header_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to write header to new blob file: %s"
                    " status: '%s'",
                    bfile->PathName().c_str(), s.ToString().c_str());
    return nullptr;
  }

  dir_change_.store(true);
  blob_files_.insert(std::make_pair(bfile->BlobFileNumber(), bfile));
  open_non_ttl_file_ = bfile;
  return bfile;
}

std::shared_ptr<BlobFile> BlobDBImpl::SelectBlobFileTTL(uint64_t expiration) {
  assert(expiration != kNoExpiration);
  uint64_t epoch_read = 0;
  std::shared_ptr<BlobFile> bfile;
  {
    ReadLock rl(&mutex_);
    bfile = FindBlobFileLocked(expiration);
    epoch_read = epoch_of_.load();
  }

  if (bfile) {
    assert(!bfile->Immutable());
    return bfile;
  }

  uint64_t exp_low =
      (expiration / bdb_options_.ttl_range_secs) * bdb_options_.ttl_range_secs;
  uint64_t exp_high = exp_low + bdb_options_.ttl_range_secs;
  ExpirationRange expiration_range = std::make_pair(exp_low, exp_high);

  bfile = NewBlobFile("SelectBlobFileTTL");
  assert(bfile);

  ROCKS_LOG_INFO(db_options_.info_log, "New blob file TTL range: %s %d %d",
                 bfile->PathName().c_str(), exp_low, exp_high);
  LogFlush(db_options_.info_log);

  // we don't need to take lock as no other thread is seeing bfile yet
  std::shared_ptr<Writer> writer = CheckOrCreateWriterLocked(bfile);
  if (!writer) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to get writer from blob file with TTL: %s",
                    bfile->PathName().c_str());
    return nullptr;
  }

  bfile->header_.expiration_range = expiration_range;
  bfile->header_.compression = bdb_options_.compression;
  bfile->header_.has_ttl = true;
  bfile->header_.column_family_id =
      reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->GetID();
  ;
  bfile->header_valid_ = true;
  bfile->SetHasTTL(true);
  bfile->SetCompression(bdb_options_.compression);
  bfile->file_size_ = BlobLogHeader::kSize;

  // set the first value of the range, since that is
  // concrete at this time.  also necessary to add to open_ttl_files_
  bfile->expiration_range_ = expiration_range;

  WriteLock wl(&mutex_);
  // in case the epoch has shifted in the interim, then check
  // check condition again - should be rare.
  if (epoch_of_.load() != epoch_read) {
    auto bfile2 = FindBlobFileLocked(expiration);
    if (bfile2) return bfile2;
  }

  Status s = writer->WriteHeader(bfile->header_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to write header to new blob file: %s"
                    " status: '%s'",
                    bfile->PathName().c_str(), s.ToString().c_str());
    return nullptr;
  }

  dir_change_.store(true);
  blob_files_.insert(std::make_pair(bfile->BlobFileNumber(), bfile));
  open_ttl_files_.insert(bfile);
  epoch_of_++;

  return bfile;
}

Status BlobDBImpl::Delete(const WriteOptions& options, const Slice& key) {
  SequenceNumber lsn = db_impl_->GetLatestSequenceNumber();
  Status s = db_->Delete(options, key);

  if (bdb_options_.enable_garbage_collection) {
    // add deleted key to list of keys that have been deleted for book-keeping
    delete_keys_q_.enqueue({DefaultColumnFamily(), key.ToString(), lsn});
  }
  return s;
}

class BlobDBImpl::BlobInserter : public WriteBatch::Handler {
 private:
  const WriteOptions& options_;
  BlobDBImpl* blob_db_impl_;
  uint32_t default_cf_id_;
  SequenceNumber sequence_;
  WriteBatch batch_;

 public:
  BlobInserter(const WriteOptions& options, BlobDBImpl* blob_db_impl,
               uint32_t default_cf_id, SequenceNumber seq)
      : options_(options),
        blob_db_impl_(blob_db_impl),
        default_cf_id_(default_cf_id),
        sequence_(seq) {}

  SequenceNumber sequence() { return sequence_; }

  WriteBatch* batch() { return &batch_; }

  virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                       const Slice& value) override {
    if (column_family_id != default_cf_id_) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }
    std::string new_value;
    Slice value_slice;
    uint64_t expiration =
        blob_db_impl_->ExtractExpiration(key, value, &value_slice, &new_value);
    Status s = blob_db_impl_->PutBlobValue(options_, key, value_slice,
                                           expiration, sequence_, &batch_);
    sequence_++;
    return s;
  }

  virtual Status DeleteCF(uint32_t column_family_id,
                          const Slice& key) override {
    if (column_family_id != default_cf_id_) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }
    Status s = WriteBatchInternal::Delete(&batch_, column_family_id, key);
    sequence_++;
    return s;
  }

  virtual Status DeleteRange(uint32_t column_family_id, const Slice& begin_key,
                             const Slice& end_key) {
    if (column_family_id != default_cf_id_) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }
    Status s = WriteBatchInternal::DeleteRange(&batch_, column_family_id,
                                               begin_key, end_key);
    sequence_++;
    return s;
  }

  virtual Status SingleDeleteCF(uint32_t /*column_family_id*/,
                                const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in blob db.");
  }

  virtual Status MergeCF(uint32_t /*column_family_id*/, const Slice& /*key*/,
                         const Slice& /*value*/) override {
    return Status::NotSupported("Not supported operation in blob db.");
  }

  virtual void LogData(const Slice& blob) override { batch_.PutLogData(blob); }
};

Status BlobDBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  StopWatch write_sw(env_, statistics_, BLOB_DB_WRITE_MICROS);
  RecordTick(statistics_, BLOB_DB_NUM_WRITE);
  uint32_t default_cf_id =
      reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->GetID();
  // TODO(yiwu): In case there are multiple writers the latest sequence would
  // not be the actually sequence we are writting. Need to get the sequence
  // from write batch after DB write instead.
  SequenceNumber current_seq = GetLatestSequenceNumber() + 1;
  Status s;
  BlobInserter blob_inserter(options, this, default_cf_id, current_seq);
  {
    // Release write_mutex_ before DB write to avoid race condition with
    // flush begin listener, which also require write_mutex_ to sync
    // blob files.
    MutexLock l(&write_mutex_);
    s = updates->Iterate(&blob_inserter);
  }
  if (!s.ok()) {
    return s;
  }
  s = db_->Write(options, blob_inserter.batch());
  if (!s.ok()) {
    return s;
  }

  // add deleted key to list of keys that have been deleted for book-keeping
  class DeleteBookkeeper : public WriteBatch::Handler {
   public:
    explicit DeleteBookkeeper(BlobDBImpl* impl, const SequenceNumber& seq)
        : impl_(impl), sequence_(seq) {}

    virtual Status PutCF(uint32_t /*column_family_id*/, const Slice& /*key*/,
                         const Slice& /*value*/) override {
      sequence_++;
      return Status::OK();
    }

    virtual Status DeleteCF(uint32_t column_family_id,
                            const Slice& key) override {
      ColumnFamilyHandle* cfh =
          impl_->db_impl_->GetColumnFamilyHandleUnlocked(column_family_id);

      impl_->delete_keys_q_.enqueue({cfh, key.ToString(), sequence_});
      sequence_++;
      return Status::OK();
    }

   private:
    BlobDBImpl* impl_;
    SequenceNumber sequence_;
  };

  if (bdb_options_.enable_garbage_collection) {
    // add deleted key to list of keys that have been deleted for book-keeping
    DeleteBookkeeper delete_bookkeeper(this, current_seq);
    s = updates->Iterate(&delete_bookkeeper);
  }

  return s;
}

Status BlobDBImpl::GetLiveFiles(std::vector<std::string>& ret,
                                uint64_t* manifest_file_size,
                                bool flush_memtable) {
  // Hold a lock in the beginning to avoid updates to base DB during the call
  ReadLock rl(&mutex_);
  Status s = db_->GetLiveFiles(ret, manifest_file_size, flush_memtable);
  if (!s.ok()) {
    return s;
  }
  ret.reserve(ret.size() + blob_files_.size());
  for (auto bfile_pair : blob_files_) {
    auto blob_file = bfile_pair.second;
    ret.emplace_back(blob_file->PathName());
  }
  return Status::OK();
}

void BlobDBImpl::GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) {
  // Hold a lock in the beginning to avoid updates to base DB during the call
  ReadLock rl(&mutex_);
  db_->GetLiveFilesMetaData(metadata);
  for (auto bfile_pair : blob_files_) {
    auto blob_file = bfile_pair.second;
    LiveFileMetaData filemetadata;
    filemetadata.size = blob_file->GetFileSize();
    filemetadata.name = blob_file->PathName();
    auto cfh =
        reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily());
    filemetadata.column_family_name = cfh->GetName();
    metadata->emplace_back(filemetadata);
  }
}

Status BlobDBImpl::Put(const WriteOptions& options, const Slice& key,
                       const Slice& value) {
  std::string new_value;
  Slice value_slice;
  uint64_t expiration = ExtractExpiration(key, value, &value_slice, &new_value);
  return PutUntil(options, key, value_slice, expiration);
}

Status BlobDBImpl::PutWithTTL(const WriteOptions& options,
                              const Slice& key, const Slice& value,
                              uint64_t ttl) {
  uint64_t now = EpochNow();
  uint64_t expiration = kNoExpiration - now > ttl ? now + ttl : kNoExpiration;
  return PutUntil(options, key, value, expiration);
}

Status BlobDBImpl::PutUntil(const WriteOptions& options, const Slice& key,
                            const Slice& value, uint64_t expiration) {
  StopWatch write_sw(env_, statistics_, BLOB_DB_WRITE_MICROS);
  RecordTick(statistics_, BLOB_DB_NUM_PUT);
  TEST_SYNC_POINT("BlobDBImpl::PutUntil:Start");
  Status s;
  WriteBatch batch;
  {
    // Release write_mutex_ before DB write to avoid race condition with
    // flush begin listener, which also require write_mutex_ to sync
    // blob files.
    MutexLock l(&write_mutex_);
    // TODO(yiwu): In case there are multiple writers the latest sequence would
    // not be the actually sequence we are writting. Need to get the sequence
    // from write batch after DB write instead.
    SequenceNumber sequence = GetLatestSequenceNumber() + 1;
    s = PutBlobValue(options, key, value, expiration, sequence, &batch);
  }
  if (s.ok()) {
    s = db_->Write(options, &batch);
  }
  TEST_SYNC_POINT("BlobDBImpl::PutUntil:Finish");
  return s;
}

Status BlobDBImpl::PutBlobValue(const WriteOptions& options, const Slice& key,
                                const Slice& value, uint64_t expiration,
                                SequenceNumber sequence, WriteBatch* batch) {
  Status s;
  std::string index_entry;
  uint32_t column_family_id =
      reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->GetID();
  if (value.size() < bdb_options_.min_blob_size) {
    if (expiration == kNoExpiration) {
      // Put as normal value
      s = batch->Put(key, value);
      RecordTick(statistics_, BLOB_DB_WRITE_INLINED);
    } else {
      // Inlined with TTL
      BlobIndex::EncodeInlinedTTL(&index_entry, expiration, value);
      s = WriteBatchInternal::PutBlobIndex(batch, column_family_id, key,
                                           index_entry);
      RecordTick(statistics_, BLOB_DB_WRITE_INLINED_TTL);
    }
  } else {
    std::shared_ptr<BlobFile> bfile = (expiration != kNoExpiration)
                                          ? SelectBlobFileTTL(expiration)
                                          : SelectBlobFile();
    if (!bfile) {
      return Status::NotFound("Blob file not found");
    }

    assert(bfile->compression() == bdb_options_.compression);
    std::string compression_output;
    Slice value_compressed = GetCompressedSlice(value, &compression_output);

    std::string headerbuf;
    Writer::ConstructBlobHeader(&headerbuf, key, value_compressed, expiration);

    s = AppendBlob(bfile, headerbuf, key, value_compressed, expiration,
                   &index_entry);
    if (expiration == kNoExpiration) {
      RecordTick(statistics_, BLOB_DB_WRITE_BLOB);
    } else {
      RecordTick(statistics_, BLOB_DB_WRITE_BLOB_TTL);
    }

    if (s.ok()) {
      bfile->ExtendSequenceRange(sequence);
      if (expiration != kNoExpiration) {
        bfile->ExtendExpirationRange(expiration);
      }
      s = CloseBlobFileIfNeeded(bfile);
      if (s.ok()) {
        s = WriteBatchInternal::PutBlobIndex(batch, column_family_id, key,
                                             index_entry);
      }
    } else {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Failed to append blob to FILE: %s: KEY: %s VALSZ: %d"
                      " status: '%s' blob_file: '%s'",
                      bfile->PathName().c_str(), key.ToString().c_str(),
                      value.size(), s.ToString().c_str(),
                      bfile->DumpState().c_str());
    }
  }

  RecordTick(statistics_, BLOB_DB_NUM_KEYS_WRITTEN);
  RecordTick(statistics_, BLOB_DB_BYTES_WRITTEN, key.size() + value.size());
  MeasureTime(statistics_, BLOB_DB_KEY_SIZE, key.size());
  MeasureTime(statistics_, BLOB_DB_VALUE_SIZE, value.size());

  return s;
}

Slice BlobDBImpl::GetCompressedSlice(const Slice& raw,
                                     std::string* compression_output) const {
  if (bdb_options_.compression == kNoCompression) {
    return raw;
  }
  StopWatch compression_sw(env_, statistics_, BLOB_DB_COMPRESSION_MICROS);
  CompressionType ct = bdb_options_.compression;
  CompressionOptions compression_opts;
  CompressBlock(raw, compression_opts, &ct, kBlockBasedTableVersionFormat,
                Slice(), compression_output);
  return *compression_output;
}

uint64_t BlobDBImpl::ExtractExpiration(const Slice& key, const Slice& value,
                                       Slice* value_slice,
                                       std::string* new_value) {
  uint64_t expiration = kNoExpiration;
  bool has_expiration = false;
  bool value_changed = false;
  if (ttl_extractor_ != nullptr) {
    has_expiration = ttl_extractor_->ExtractExpiration(
        key, value, EpochNow(), &expiration, new_value, &value_changed);
  }
  *value_slice = value_changed ? Slice(*new_value) : value;
  return has_expiration ? expiration : kNoExpiration;
}

std::shared_ptr<BlobFile> BlobDBImpl::GetOldestBlobFile() {
  std::vector<std::shared_ptr<BlobFile>> blob_files;
  CopyBlobFiles(&blob_files, [](const std::shared_ptr<BlobFile>& f) {
    return !f->Obsolete() && f->Immutable();
  });
  blobf_compare_ttl compare;
  return *std::min_element(blob_files.begin(), blob_files.end(), compare);
}

bool BlobDBImpl::EvictOldestBlobFile() {
  auto oldest_file = GetOldestBlobFile();
  if (oldest_file == nullptr) {
    return false;
  }

  WriteLock wl(&mutex_);
  // Double check the file is not obsolete by others
  if (oldest_file_evicted_ == false && !oldest_file->Obsolete()) {
    auto expiration_range = oldest_file->GetExpirationRange();
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Evict oldest blob file since DB out of space. Current "
                   "space used: %" PRIu64 ", blob dir size: %" PRIu64
                   ", evicted blob file #%" PRIu64
                   " with expiration range (%" PRIu64 ", %" PRIu64 ").",
                   total_blob_space_.load(), bdb_options_.blob_dir_size,
                   oldest_file->BlobFileNumber(), expiration_range.first,
                   expiration_range.second);
    oldest_file->MarkObsolete(oldest_file->GetSequenceRange().second);
    obsolete_files_.push_back(oldest_file);
    oldest_file_evicted_.store(true);
    RecordTick(statistics_, BLOB_DB_FIFO_NUM_FILES_EVICTED);
    RecordTick(statistics_, BLOB_DB_FIFO_NUM_KEYS_EVICTED,
               oldest_file->BlobCount());
    RecordTick(statistics_, BLOB_DB_FIFO_BYTES_EVICTED,
               oldest_file->GetFileSize());
    return true;
  }

  return false;
}

Status BlobDBImpl::CheckSize(size_t blob_size) {
  uint64_t new_space_util = total_blob_space_.load() + blob_size;
  if (bdb_options_.blob_dir_size > 0) {
    if (!bdb_options_.is_fifo &&
        (new_space_util > bdb_options_.blob_dir_size)) {
      return Status::NoSpace(
          "Write failed, as writing it would exceed blob_dir_size limit.");
    }
    if (bdb_options_.is_fifo && !oldest_file_evicted_.load() &&
        (new_space_util >
         kEvictOldestFileAtSize * bdb_options_.blob_dir_size)) {
      EvictOldestBlobFile();
    }
  }

  return Status::OK();
}

Status BlobDBImpl::AppendBlob(const std::shared_ptr<BlobFile>& bfile,
                              const std::string& headerbuf, const Slice& key,
                              const Slice& value, uint64_t expiration,
                              std::string* index_entry) {
  auto size_put = BlobLogRecord::kHeaderSize + key.size() + value.size();
  Status s = CheckSize(size_put);
  if (!s.ok()) {
    return s;
  }

  uint64_t blob_offset = 0;
  uint64_t key_offset = 0;
  {
    WriteLock lockbfile_w(&bfile->mutex_);
    std::shared_ptr<Writer> writer = CheckOrCreateWriterLocked(bfile);
    if (!writer) return Status::IOError("Failed to create blob writer");

    // write the blob to the blob log.
    s = writer->EmitPhysicalRecord(headerbuf, key, value, &key_offset,
                                   &blob_offset);
  }

  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Invalid status in AppendBlob: %s status: '%s'",
                    bfile->PathName().c_str(), s.ToString().c_str());
    return s;
  }

  // increment blob count
  bfile->blob_count_++;

  bfile->file_size_ += size_put;
  total_blob_space_ += size_put;

  if (expiration == kNoExpiration) {
    BlobIndex::EncodeBlob(index_entry, bfile->BlobFileNumber(), blob_offset,
                          value.size(), bdb_options_.compression);
  } else {
    BlobIndex::EncodeBlobTTL(index_entry, expiration, bfile->BlobFileNumber(),
                             blob_offset, value.size(),
                             bdb_options_.compression);
  }

  return s;
}

std::vector<Status> BlobDBImpl::MultiGet(
    const ReadOptions& read_options,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  StopWatch multiget_sw(env_, statistics_, BLOB_DB_MULTIGET_MICROS);
  RecordTick(statistics_, BLOB_DB_NUM_MULTIGET);
  // Get a snapshot to avoid blob file get deleted between we
  // fetch and index entry and reading from the file.
  ReadOptions ro(read_options);
  bool snapshot_created = SetSnapshotIfNeeded(&ro);

  std::vector<Status> statuses;
  statuses.reserve(keys.size());
  values->clear();
  values->reserve(keys.size());
  PinnableSlice value;
  for (size_t i = 0; i < keys.size(); i++) {
    statuses.push_back(Get(ro, DefaultColumnFamily(), keys[i], &value));
    values->push_back(value.ToString());
    value.Reset();
  }
  if (snapshot_created) {
    db_->ReleaseSnapshot(ro.snapshot);
  }
  return statuses;
}

bool BlobDBImpl::SetSnapshotIfNeeded(ReadOptions* read_options) {
  assert(read_options != nullptr);
  if (read_options->snapshot != nullptr) {
    return false;
  }
  read_options->snapshot = db_->GetSnapshot();
  return true;
}

Status BlobDBImpl::GetBlobValue(const Slice& key, const Slice& index_entry,
                                PinnableSlice* value) {
  assert(value != nullptr);
  BlobIndex blob_index;
  Status s = blob_index.DecodeFrom(index_entry);
  if (!s.ok()) {
    return s;
  }
  if (blob_index.HasTTL() && blob_index.expiration() <= EpochNow()) {
    return Status::NotFound("Key expired");
  }
  if (blob_index.IsInlined()) {
    // TODO(yiwu): If index_entry is a PinnableSlice, we can also pin the same
    // memory buffer to avoid extra copy.
    value->PinSelf(blob_index.value());
    return Status::OK();
  }
  if (blob_index.size() == 0) {
    value->PinSelf("");
    return Status::OK();
  }

  // offset has to have certain min, as we will read CRC
  // later from the Blob Header, which needs to be also a
  // valid offset.
  if (blob_index.offset() <
      (BlobLogHeader::kSize + BlobLogRecord::kHeaderSize + key.size())) {
    if (debug_level_ >= 2) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Invalid blob index file_number: %" PRIu64
                      " blob_offset: %" PRIu64 " blob_size: %" PRIu64
                      " key: %s",
                      blob_index.file_number(), blob_index.offset(),
                      blob_index.size(), key.data());
    }
    return Status::NotFound("Invalid blob offset");
  }

  std::shared_ptr<BlobFile> bfile;
  {
    ReadLock rl(&mutex_);
    auto hitr = blob_files_.find(blob_index.file_number());

    // file was deleted
    if (hitr == blob_files_.end()) {
      return Status::NotFound("Blob Not Found as blob file missing");
    }

    bfile = hitr->second;
  }

  if (blob_index.size() == 0 && value != nullptr) {
    value->PinSelf("");
    return Status::OK();
  }

  // takes locks when called
  std::shared_ptr<RandomAccessFileReader> reader =
      GetOrOpenRandomAccessReader(bfile, env_, env_options_);

  std::string* valueptr = value->GetSelf();
  std::string value_c;
  if (bdb_options_.compression != kNoCompression) {
    valueptr = &value_c;
  }

  // Allocate the buffer. This is safe in C++11
  // Note that std::string::reserved() does not work, since previous value
  // of the buffer can be larger than blob_index.size().
  valueptr->resize(blob_index.size());
  char* buffer = &(*valueptr)[0];

  Slice blob_value;
  {
    StopWatch read_sw(env_, statistics_, BLOB_DB_BLOB_FILE_READ_MICROS);
    s = reader->Read(blob_index.offset(), blob_index.size(), &blob_value,
                     buffer);
    RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, blob_value.size());
  }
  if (!s.ok() || blob_value.size() != blob_index.size()) {
    if (debug_level_ >= 2) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Failed to read blob from file: %s blob_offset: %" PRIu64
                      " blob_size: %" PRIu64 " read: %d key: %s status: '%s'",
                      bfile->PathName().c_str(), blob_index.offset(),
                      blob_index.size(), static_cast<int>(blob_value.size()),
                      key.data(), s.ToString().c_str());
    }
    return Status::NotFound("Blob Not Found as couldnt retrieve Blob");
  }

  // TODO(yiwu): Add an option to skip crc checking.
  Slice crc_slice;
  uint32_t crc_exp;
  std::string crc_str;
  crc_str.resize(sizeof(uint32_t));
  char* crc_buffer = &(crc_str[0]);
  s = reader->Read(blob_index.offset() - (key.size() + sizeof(uint32_t)),
                   sizeof(uint32_t), &crc_slice, crc_buffer);
  if (!s.ok() || !GetFixed32(&crc_slice, &crc_exp)) {
    if (debug_level_ >= 2) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Failed to fetch blob crc file: %s blob_offset: %" PRIu64
                      " blob_size: %" PRIu64 " key: %s status: '%s'",
                      bfile->PathName().c_str(), blob_index.offset(),
                      blob_index.size(), key.data(), s.ToString().c_str());
    }
    return Status::NotFound("Blob Not Found as couldnt retrieve CRC");
  }

  uint32_t crc = crc32c::Value(key.data(), key.size());
  crc = crc32c::Extend(crc, blob_value.data(), blob_value.size());
  crc = crc32c::Mask(crc);  // Adjust for storage
  if (crc != crc_exp) {
    if (debug_level_ >= 2) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Blob crc mismatch file: %s blob_offset: %" PRIu64
                      " blob_size: %" PRIu64 " key: %s status: '%s'",
                      bfile->PathName().c_str(), blob_index.offset(),
                      blob_index.size(), key.data(), s.ToString().c_str());
    }
    return Status::Corruption("Corruption. Blob CRC mismatch");
  }

  if (bfile->compression() != kNoCompression) {
    BlockContents contents;
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily());
    {
      StopWatch decompression_sw(env_, statistics_,
                                 BLOB_DB_DECOMPRESSION_MICROS);
      s = UncompressBlockContentsForCompressionType(
          blob_value.data(), blob_value.size(), &contents,
          kBlockBasedTableVersionFormat, Slice(), bfile->compression(),
          *(cfh->cfd()->ioptions()));
    }
    *(value->GetSelf()) = contents.data.ToString();
  }

  value->PinSelf();

  return s;
}

Status BlobDBImpl::Get(const ReadOptions& read_options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       PinnableSlice* value) {
  StopWatch get_sw(env_, statistics_, BLOB_DB_GET_MICROS);
  RecordTick(statistics_, BLOB_DB_NUM_GET);
  return GetImpl(read_options, column_family, key, value);
}

Status BlobDBImpl::GetImpl(const ReadOptions& read_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           PinnableSlice* value) {
  if (column_family != DefaultColumnFamily()) {
    return Status::NotSupported(
        "Blob DB doesn't support non-default column family.");
  }
  // Get a snapshot to avoid blob file get deleted between we
  // fetch and index entry and reading from the file.
  // TODO(yiwu): For Get() retry if file not found would be a simpler strategy.
  ReadOptions ro(read_options);
  bool snapshot_created = SetSnapshotIfNeeded(&ro);

  Status s;
  bool is_blob_index = false;
  s = db_impl_->GetImpl(ro, column_family, key, value,
                        nullptr /*value_found*/, nullptr /*read_callback*/,
                        &is_blob_index);
  TEST_SYNC_POINT("BlobDBImpl::Get:AfterIndexEntryGet:1");
  TEST_SYNC_POINT("BlobDBImpl::Get:AfterIndexEntryGet:2");
  if (s.ok() && is_blob_index) {
    std::string index_entry = value->ToString();
    value->Reset();
    s = GetBlobValue(key, index_entry, value);
  }
  if (snapshot_created) {
    db_->ReleaseSnapshot(ro.snapshot);
  }
  RecordTick(statistics_, BLOB_DB_NUM_KEYS_READ);
  RecordTick(statistics_, BLOB_DB_BYTES_READ, value->size());
  return s;
}

std::pair<bool, int64_t> BlobDBImpl::SanityCheck(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  ROCKS_LOG_INFO(db_options_.info_log, "Starting Sanity Check");

  ROCKS_LOG_INFO(db_options_.info_log, "Number of files %" PRIu64,
                 blob_files_.size());

  ROCKS_LOG_INFO(db_options_.info_log, "Number of open files %" PRIu64,
                 open_ttl_files_.size());

  for (auto bfile : open_ttl_files_) {
    assert(!bfile->Immutable());
  }

  uint64_t epoch_now = EpochNow();

  for (auto bfile_pair : blob_files_) {
    auto bfile = bfile_pair.second;
    ROCKS_LOG_INFO(
        db_options_.info_log,
        "Blob File %s %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64,
        bfile->PathName().c_str(), bfile->GetFileSize(), bfile->BlobCount(),
        bfile->deleted_count_, bfile->deleted_size_,
        (bfile->expiration_range_.second - epoch_now));
  }

  // reschedule
  return std::make_pair(true, -1);
}

Status BlobDBImpl::CloseBlobFile(std::shared_ptr<BlobFile> bfile) {
  assert(bfile != nullptr);
  Status s;
  ROCKS_LOG_INFO(db_options_.info_log,
                 "Closing blob file %" PRIu64 ". Path: %s",
                 bfile->BlobFileNumber(), bfile->PathName().c_str());
  {
    WriteLock wl(&mutex_);

    if (bfile->HasTTL()) {
      size_t erased __attribute__((__unused__));
      erased = open_ttl_files_.erase(bfile);
      assert(erased == 1);
    } else {
      assert(bfile == open_non_ttl_file_);
      open_non_ttl_file_ = nullptr;
    }
  }

  if (!bfile->closed_.load()) {
    WriteLock lockbfile_w(&bfile->mutex_);
    s = bfile->WriteFooterAndCloseLocked();
  }

  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to close blob file %" PRIu64 "with error: %s",
                    bfile->BlobFileNumber(), s.ToString().c_str());
  }

  return s;
}

Status BlobDBImpl::CloseBlobFileIfNeeded(std::shared_ptr<BlobFile>& bfile) {
  // atomic read
  if (bfile->GetFileSize() < bdb_options_.blob_file_size) {
    return Status::OK();
  }
  return CloseBlobFile(bfile);
}

bool BlobDBImpl::VisibleToActiveSnapshot(
    const std::shared_ptr<BlobFile>& bfile) {
  assert(bfile->Obsolete());
  SequenceNumber first_sequence = bfile->GetSequenceRange().first;
  SequenceNumber obsolete_sequence = bfile->GetObsoleteSequence();
  return db_impl_->HasActiveSnapshotInRange(first_sequence, obsolete_sequence);
}

bool BlobDBImpl::FindFileAndEvictABlob(uint64_t file_number, uint64_t key_size,
                                       uint64_t blob_offset,
                                       uint64_t blob_size) {
  assert(bdb_options_.enable_garbage_collection);
  (void)blob_offset;
  std::shared_ptr<BlobFile> bfile;
  {
    ReadLock rl(&mutex_);
    auto hitr = blob_files_.find(file_number);

    // file was deleted
    if (hitr == blob_files_.end()) {
      return false;
    }

    bfile = hitr->second;
  }

  WriteLock lockbfile_w(&bfile->mutex_);

  bfile->deleted_count_++;
  bfile->deleted_size_ += key_size + blob_size + BlobLogRecord::kHeaderSize;
  return true;
}

bool BlobDBImpl::MarkBlobDeleted(const Slice& key, const Slice& index_entry) {
  assert(bdb_options_.enable_garbage_collection);
  BlobIndex blob_index;
  Status s = blob_index.DecodeFrom(index_entry);
  if (!s.ok()) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Could not parse lsm val in MarkBlobDeleted %s",
                   index_entry.ToString().c_str());
    return false;
  }
  bool succ = FindFileAndEvictABlob(blob_index.file_number(), key.size(),
                                    blob_index.offset(), blob_index.size());
  return succ;
}

std::pair<bool, int64_t> BlobDBImpl::EvictCompacted(bool aborted) {
  assert(bdb_options_.enable_garbage_collection);
  if (aborted) return std::make_pair(false, -1);

  override_packet_t packet;
  size_t total_vals = 0;
  size_t mark_evicted = 0;
  while (override_vals_q_.dequeue(&packet)) {
    bool succeeded =
        FindFileAndEvictABlob(packet.file_number_, packet.key_size_,
                              packet.blob_offset_, packet.blob_size_);
    total_vals++;
    if (succeeded) {
      mark_evicted++;
    }
  }
  ROCKS_LOG_INFO(db_options_.info_log,
                 "Mark %" ROCKSDB_PRIszt
                 " values to evict, out of %" ROCKSDB_PRIszt
                 " compacted values.",
                 mark_evicted, total_vals);
  return std::make_pair(true, -1);
}

std::pair<bool, int64_t> BlobDBImpl::EvictDeletions(bool aborted) {
  assert(bdb_options_.enable_garbage_collection);
  if (aborted) return std::make_pair(false, -1);

  ColumnFamilyHandle* last_cfh = nullptr;
  Options last_op;

  Arena arena;
  ScopedArenaIterator iter;

  // we will use same RangeDelAggregator for all cf's.
  // essentially we do not support Range Deletes now
  std::unique_ptr<RangeDelAggregator> range_del_agg;
  delete_packet_t dpacket;
  while (delete_keys_q_.dequeue(&dpacket)) {
    if (last_cfh != dpacket.cfh_) {
      if (!range_del_agg) {
        auto cfhi = reinterpret_cast<ColumnFamilyHandleImpl*>(dpacket.cfh_);
        auto cfd = cfhi->cfd();
        range_del_agg.reset(new RangeDelAggregator(cfd->internal_comparator(),
                                                   kMaxSequenceNumber));
      }

      // this can be expensive
      last_cfh = dpacket.cfh_;
      last_op = db_impl_->GetOptions(last_cfh);
      iter.set(db_impl_->NewInternalIterator(&arena, range_del_agg.get(),
                                             dpacket.cfh_));
      // this will not work for multiple CF's.
    }

    Slice user_key(dpacket.key_);
    InternalKey target(user_key, dpacket.dsn_, kTypeValue);

    Slice eslice = target.Encode();
    iter->Seek(eslice);

    if (!iter->status().ok()) {
      ROCKS_LOG_INFO(db_options_.info_log, "Invalid iterator seek %s",
                     dpacket.key_.c_str());
      continue;
    }

    const Comparator* bwc = BytewiseComparator();
    while (iter->Valid()) {
      if (!bwc->Equal(ExtractUserKey(iter->key()), ExtractUserKey(eslice)))
        break;

      ParsedInternalKey ikey(Slice(), 0, kTypeValue);
      if (!ParseInternalKey(iter->key(), &ikey)) {
        continue;
      }

      // once you hit a DELETE, assume the keys below have been
      // processed previously
      if (ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion) break;

      Slice val = iter->value();
      MarkBlobDeleted(ikey.user_key, val);

      iter->Next();
    }
  }
  return std::make_pair(true, -1);
}

std::pair<bool, int64_t> BlobDBImpl::CheckSeqFiles(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  std::vector<std::shared_ptr<BlobFile>> process_files;
  {
    uint64_t epoch_now = EpochNow();

    ReadLock rl(&mutex_);
    for (auto bfile : open_ttl_files_) {
      {
        ReadLock lockbfile_r(&bfile->mutex_);

        if (bfile->expiration_range_.second > epoch_now) continue;
        process_files.push_back(bfile);
      }
    }
  }

  for (auto bfile : process_files) {
    CloseBlobFile(bfile);
  }

  return std::make_pair(true, -1);
}

std::pair<bool, int64_t> BlobDBImpl::FsyncFiles(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  MutexLock l(&write_mutex_);

  std::vector<std::shared_ptr<BlobFile>> process_files;
  {
    ReadLock rl(&mutex_);
    for (auto fitr : open_ttl_files_) {
      if (fitr->NeedsFsync(true, bdb_options_.bytes_per_sync))
        process_files.push_back(fitr);
    }

    if (open_non_ttl_file_ != nullptr &&
        open_non_ttl_file_->NeedsFsync(true, bdb_options_.bytes_per_sync)) {
      process_files.push_back(open_non_ttl_file_);
    }
  }

  for (auto fitr : process_files) {
    if (fitr->NeedsFsync(true, bdb_options_.bytes_per_sync)) fitr->Fsync();
  }

  bool expected = true;
  if (dir_change_.compare_exchange_weak(expected, false)) dir_ent_->Fsync();

  return std::make_pair(true, -1);
}

std::pair<bool, int64_t> BlobDBImpl::ReclaimOpenFiles(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  if (open_file_count_.load() < kOpenFilesTrigger) {
    return std::make_pair(true, -1);
  }

  // in the future, we should sort by last_access_
  // instead of closing every file
  ReadLock rl(&mutex_);
  for (auto const& ent : blob_files_) {
    auto bfile = ent.second;
    if (bfile->last_access_.load() == -1) continue;

    WriteLock lockbfile_w(&bfile->mutex_);
    CloseRandomAccessLocked(bfile);
  }

  return std::make_pair(true, -1);
}

// Write callback for garbage collection to check if key has been updated
// since last read. Similar to how OptimisticTransaction works. See inline
// comment in GCFileAndUpdateLSM().
class BlobDBImpl::GarbageCollectionWriteCallback : public WriteCallback {
 public:
  GarbageCollectionWriteCallback(ColumnFamilyData* cfd, const Slice& key,
                                 SequenceNumber upper_bound)
      : cfd_(cfd), key_(key), upper_bound_(upper_bound) {}

  virtual Status Callback(DB* db) override {
    auto* db_impl = reinterpret_cast<DBImpl*>(db);
    auto* sv = db_impl->GetAndRefSuperVersion(cfd_);
    SequenceNumber latest_seq = 0;
    bool found_record_for_key = false;
    bool is_blob_index = false;
    Status s = db_impl->GetLatestSequenceForKey(
        sv, key_, false /*cache_only*/, &latest_seq, &found_record_for_key,
        &is_blob_index);
    db_impl->ReturnAndCleanupSuperVersion(cfd_, sv);
    if (!s.ok() && !s.IsNotFound()) {
      // Error.
      assert(!s.IsBusy());
      return s;
    }
    if (s.IsNotFound()) {
      assert(!found_record_for_key);
      return Status::Busy("Key deleted");
    }
    assert(found_record_for_key);
    assert(is_blob_index);
    if (latest_seq > upper_bound_) {
      return Status::Busy("Key overwritten");
    }
    return s;
  }

  virtual bool AllowWriteBatching() override { return false; }

 private:
  ColumnFamilyData* cfd_;
  // Key to check
  Slice key_;
  // Upper bound of sequence number to proceed.
  SequenceNumber upper_bound_;
};

// iterate over the blobs sequentially and check if the blob sequence number
// is the latest. If it is the latest, preserve it, otherwise delete it
// if it is TTL based, and the TTL has expired, then
// we can blow the entity if the key is still the latest or the Key is not
// found
// WHAT HAPPENS IF THE KEY HAS BEEN OVERRIDEN. Then we can drop the blob
// without doing anything if the earliest snapshot is not
// referring to that sequence number, i.e. it is later than the sequence number
// of the new key
//
// if it is not TTL based, then we can blow the key if the key has been
// DELETED in the LSM
Status BlobDBImpl::GCFileAndUpdateLSM(const std::shared_ptr<BlobFile>& bfptr,
                                      GCStats* gc_stats) {
  StopWatch gc_sw(env_, statistics_, BLOB_DB_GC_MICROS);
  uint64_t now = EpochNow();

  std::shared_ptr<Reader> reader =
      bfptr->OpenSequentialReader(env_, db_options_, env_options_);
  if (!reader) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "File sequential reader could not be opened",
                    bfptr->PathName().c_str());
    return Status::IOError("failed to create sequential reader");
  }

  BlobLogHeader header;
  Status s = reader->ReadHeader(&header);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failure to read header for blob-file %s",
                    bfptr->PathName().c_str());
    return s;
  }

  bool first_gc = bfptr->gc_once_after_open_;

  auto* cfh =
      db_impl_->GetColumnFamilyHandleUnlocked(bfptr->column_family_id());
  auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh)->cfd();
  auto column_family_id = cfd->GetID();
  bool has_ttl = header.has_ttl;

  // this reads the key but skips the blob
  Reader::ReadLevel shallow = Reader::kReadHeaderKey;

  bool no_relocation_ttl =
      (has_ttl && now >= bfptr->GetExpirationRange().second);

  bool no_relocation_lsmdel = false;
  {
    ReadLock lockbfile_r(&bfptr->mutex_);
    no_relocation_lsmdel =
        (bfptr->GetFileSize() ==
         (BlobLogHeader::kSize + bfptr->deleted_size_ + BlobLogFooter::kSize));
  }

  bool no_relocation = no_relocation_ttl || no_relocation_lsmdel;
  if (!no_relocation) {
    // read the blob because you have to write it back to new file
    shallow = Reader::kReadHeaderKeyBlob;
  }

  BlobLogRecord record;
  std::shared_ptr<BlobFile> newfile;
  std::shared_ptr<Writer> new_writer;
  uint64_t blob_offset = 0;

  while (true) {
    assert(s.ok());

    // Read the next blob record.
    Status read_record_status =
        reader->ReadRecord(&record, shallow, &blob_offset);
    // Exit if we reach the end of blob file.
    // TODO(yiwu): properly handle ReadRecord error.
    if (!read_record_status.ok()) {
      break;
    }
    gc_stats->blob_count++;

    // Similar to OptimisticTransaction, we obtain latest_seq from
    // base DB, which is guaranteed to be no smaller than the sequence of
    // current key. We use a WriteCallback on write to check the key sequence
    // on write. If the key sequence is larger than latest_seq, we know
    // a new versions is inserted and the old blob can be disgard.
    //
    // We cannot use OptimisticTransaction because we need to pass
    // is_blob_index flag to GetImpl.
    SequenceNumber latest_seq = GetLatestSequenceNumber();
    bool is_blob_index = false;
    PinnableSlice index_entry;
    Status get_status = db_impl_->GetImpl(
        ReadOptions(), cfh, record.key, &index_entry, nullptr /*value_found*/,
        nullptr /*read_callback*/, &is_blob_index);
    TEST_SYNC_POINT("BlobDBImpl::GCFileAndUpdateLSM:AfterGetFromBaseDB");
    if (!get_status.ok() && !get_status.IsNotFound()) {
      // error
      s = get_status;
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Error while getting index entry: %s",
                      s.ToString().c_str());
      break;
    }
    if (get_status.IsNotFound() || !is_blob_index) {
      // Either the key is deleted or updated with a newer version whish is
      // inlined in LSM.
      gc_stats->num_keys_overwritten++;
      gc_stats->bytes_overwritten += record.record_size();
      continue;
    }

    BlobIndex blob_index;
    s = blob_index.DecodeFrom(index_entry);
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Error while decoding index entry: %s",
                      s.ToString().c_str());
      break;
    }
    if (blob_index.IsInlined() ||
        blob_index.file_number() != bfptr->BlobFileNumber() ||
        blob_index.offset() != blob_offset) {
      // Key has been overwritten. Drop the blob record.
      gc_stats->num_keys_overwritten++;
      gc_stats->bytes_overwritten += record.record_size();
      continue;
    }

    GarbageCollectionWriteCallback callback(cfd, record.key, latest_seq);

    // If key has expired, remove it from base DB.
    // TODO(yiwu): Blob indexes will be remove by BlobIndexCompactionFilter.
    // We can just drop the blob record.
    if (no_relocation_ttl || (has_ttl && now >= record.expiration)) {
      gc_stats->num_keys_expired++;
      gc_stats->bytes_expired += record.record_size();
      TEST_SYNC_POINT("BlobDBImpl::GCFileAndUpdateLSM:BeforeDelete");
      WriteBatch delete_batch;
      Status delete_status = delete_batch.Delete(record.key);
      if (delete_status.ok()) {
        delete_status = db_impl_->WriteWithCallback(WriteOptions(),
                                                    &delete_batch, &callback);
      }
      if (!delete_status.ok() && !delete_status.IsBusy()) {
        // We hit an error.
        s = delete_status;
        ROCKS_LOG_ERROR(db_options_.info_log,
                        "Error while deleting expired key: %s",
                        s.ToString().c_str());
        break;
      }
      // Continue to next blob record or retry.
      continue;
    }

    if (first_gc) {
      // Do not relocate blob record for initial GC.
      continue;
    }

    // Relocate the blob record to new file.
    if (!newfile) {
      // new file
      std::string reason("GC of ");
      reason += bfptr->PathName();
      newfile = NewBlobFile(reason);

      new_writer = CheckOrCreateWriterLocked(newfile);
      newfile->header_ = std::move(header);
      // Can't use header beyond this point
      newfile->header_valid_ = true;
      newfile->file_size_ = BlobLogHeader::kSize;
      s = new_writer->WriteHeader(newfile->header_);

      if (!s.ok()) {
        ROCKS_LOG_ERROR(db_options_.info_log,
                        "File: %s - header writing failed",
                        newfile->PathName().c_str());
        break;
      }

      WriteLock wl(&mutex_);

      dir_change_.store(true);
      blob_files_.insert(std::make_pair(newfile->BlobFileNumber(), newfile));
    }

    std::string new_index_entry;
    uint64_t new_blob_offset = 0;
    uint64_t new_key_offset = 0;
    // write the blob to the blob log.
    s = new_writer->AddRecord(record.key, record.value, record.expiration,
                              &new_key_offset, &new_blob_offset);

    BlobIndex::EncodeBlob(&new_index_entry, newfile->BlobFileNumber(),
                          new_blob_offset, record.value.size(),
                          bdb_options_.compression);

    newfile->blob_count_++;
    newfile->file_size_ +=
        BlobLogRecord::kHeaderSize + record.key.size() + record.value.size();

    TEST_SYNC_POINT("BlobDBImpl::GCFileAndUpdateLSM:BeforeRelocate");
    WriteBatch rewrite_batch;
    Status rewrite_status = WriteBatchInternal::PutBlobIndex(
        &rewrite_batch, column_family_id, record.key, new_index_entry);
    if (rewrite_status.ok()) {
      rewrite_status = db_impl_->WriteWithCallback(WriteOptions(),
                                                   &rewrite_batch, &callback);
    }
    if (rewrite_status.ok()) {
      newfile->ExtendSequenceRange(
          WriteBatchInternal::Sequence(&rewrite_batch));
      gc_stats->num_keys_relocated++;
      gc_stats->bytes_relocated += record.record_size();
    } else if (rewrite_status.IsBusy()) {
      // The key is overwritten in the meanwhile. Drop the blob record.
      gc_stats->num_keys_overwritten++;
      gc_stats->bytes_overwritten += record.record_size();
    } else {
      // We hit an error.
      s = rewrite_status;
      ROCKS_LOG_ERROR(db_options_.info_log, "Error while relocating key: %s",
                      s.ToString().c_str());
      break;
    }
  }  // end of ReadRecord loop

  if (s.ok()) {
    SequenceNumber obsolete_sequence =
        newfile == nullptr ? bfptr->GetSequenceRange().second + 1
                           : newfile->GetSequenceRange().second;
    bfptr->MarkObsolete(obsolete_sequence);
    if (!first_gc) {
      WriteLock wl(&mutex_);
      obsolete_files_.push_back(bfptr);
    }
  }

  ROCKS_LOG_INFO(
      db_options_.info_log,
      "%s blob file %" PRIu64 ". Total blob records: %" PRIu64
      ", Expired: %" PRIu64 " keys/%" PRIu64 " bytes, Overwritten: %" PRIu64
      " keys/%" PRIu64 " bytes.",
      s.ok() ? "Successfully garbage collected" : "Failed to garbage collect",
      bfptr->BlobFileNumber(), gc_stats->blob_count, gc_stats->num_keys_expired,
      gc_stats->bytes_expired, gc_stats->num_keys_overwritten,
      gc_stats->bytes_overwritten, gc_stats->num_keys_relocated,
      gc_stats->bytes_relocated);
  RecordTick(statistics_, BLOB_DB_GC_NUM_FILES);
  RecordTick(statistics_, BLOB_DB_GC_NUM_KEYS_OVERWRITTEN,
             gc_stats->num_keys_overwritten);
  RecordTick(statistics_, BLOB_DB_GC_NUM_KEYS_EXPIRED,
             gc_stats->num_keys_expired);
  RecordTick(statistics_, BLOB_DB_GC_BYTES_OVERWRITTEN,
             gc_stats->bytes_overwritten);
  RecordTick(statistics_, BLOB_DB_GC_BYTES_EXPIRED, gc_stats->bytes_expired);
  if (newfile != nullptr) {
    total_blob_space_ += newfile->file_size_;
    ROCKS_LOG_INFO(db_options_.info_log, "New blob file %" PRIu64 ".",
                   newfile->BlobFileNumber());
    RecordTick(statistics_, BLOB_DB_GC_NUM_NEW_FILES);
    RecordTick(statistics_, BLOB_DB_GC_NUM_KEYS_RELOCATED,
               gc_stats->num_keys_relocated);
    RecordTick(statistics_, BLOB_DB_GC_BYTES_RELOCATED,
               gc_stats->bytes_relocated);
  }
  if (!s.ok()) {
    RecordTick(statistics_, BLOB_DB_GC_FAILURES);
  }
  return s;
}

// Ideally we should hold the lock during the entire function,
// but under the asusmption that this is only called when a
// file is Immutable, we can reduce the critical section
bool BlobDBImpl::ShouldGCFile(std::shared_ptr<BlobFile> bfile, uint64_t now,
                              bool is_oldest_non_ttl_file,
                              std::string* reason) {
  if (bfile->HasTTL()) {
    ExpirationRange expiration_range = bfile->GetExpirationRange();
    if (now > expiration_range.second) {
      *reason = "entire file ttl expired";
      return true;
    }

    if (!bfile->file_size_.load()) {
      ROCKS_LOG_ERROR(db_options_.info_log, "Invalid file size = 0 %s",
                      bfile->PathName().c_str());
      *reason = "file is empty";
      return false;
    }

    if (bfile->gc_once_after_open_.load()) {
      return true;
    }

    if (bdb_options_.ttl_range_secs < kPartialExpirationGCRangeSecs) {
      *reason = "has ttl but partial expiration not turned on";
      return false;
    }

    ReadLock lockbfile_r(&bfile->mutex_);
    bool ret = ((bfile->deleted_size_ * 100.0 / bfile->file_size_.load()) >
                kPartialExpirationPercentage);
    if (ret) {
      *reason = "deleted blobs beyond threshold";
    } else {
      *reason = "deleted blobs below threshold";
    }
    return ret;
  }

  // when crash happens, we lose the in-memory account of deleted blobs.
  // we are therefore forced to do one GC to make sure delete accounting
  // is OK
  if (bfile->gc_once_after_open_.load()) {
    return true;
  }

  ReadLock lockbfile_r(&bfile->mutex_);

  if (bdb_options_.enable_garbage_collection) {
    if ((bfile->deleted_size_ * 100.0 / bfile->file_size_.load()) >
        kPartialExpirationPercentage) {
      *reason = "deleted simple blobs beyond threshold";
      return true;
    }
  }

  // if we haven't reached limits of disk space, don't DELETE
  if (bdb_options_.blob_dir_size == 0 ||
      total_blob_space_.load() < bdb_options_.blob_dir_size) {
    *reason = "disk space not exceeded";
    return false;
  }

  if (is_oldest_non_ttl_file) {
    *reason = "out of space and is the oldest simple blob file";
    return true;
  }
  *reason = "out of space but is not the oldest simple blob file";
  return false;
}

std::pair<bool, int64_t> BlobDBImpl::DeleteObsoleteFiles(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  {
    ReadLock rl(&mutex_);
    if (obsolete_files_.empty()) return std::make_pair(true, -1);
  }

  std::list<std::shared_ptr<BlobFile>> tobsolete;
  {
    WriteLock wl(&mutex_);
    tobsolete.swap(obsolete_files_);
  }

  bool file_deleted = false;
  for (auto iter = tobsolete.begin(); iter != tobsolete.end();) {
    auto bfile = *iter;
    {
      ReadLock lockbfile_r(&bfile->mutex_);
      if (VisibleToActiveSnapshot(bfile)) {
        ROCKS_LOG_INFO(db_options_.info_log,
                       "Could not delete file due to snapshot failure %s",
                       bfile->PathName().c_str());
        ++iter;
        continue;
      }
    }
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Will delete file due to snapshot success %s",
                   bfile->PathName().c_str());

    blob_files_.erase(bfile->BlobFileNumber());
    Status s = env_->DeleteFile(bfile->PathName());
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "File failed to be deleted as obsolete %s",
                      bfile->PathName().c_str());
      ++iter;
      continue;
    }

    file_deleted = true;
    total_blob_space_ -= bfile->file_size_;
    ROCKS_LOG_INFO(db_options_.info_log,
                   "File deleted as obsolete from blob dir %s",
                   bfile->PathName().c_str());

    iter = tobsolete.erase(iter);
  }

  // directory change. Fsync
  if (file_deleted) {
    dir_ent_->Fsync();

    // reset oldest_file_evicted flag
    oldest_file_evicted_.store(false);
  }

  // put files back into obsolete if for some reason, delete failed
  if (!tobsolete.empty()) {
    WriteLock wl(&mutex_);
    for (auto bfile : tobsolete) {
      obsolete_files_.push_front(bfile);
    }
  }

  return std::make_pair(!aborted, -1);
}

void BlobDBImpl::CopyBlobFiles(
    std::vector<std::shared_ptr<BlobFile>>* bfiles_copy,
    std::function<bool(const std::shared_ptr<BlobFile>&)> predicate) {
  ReadLock rl(&mutex_);

  for (auto const& p : blob_files_) {
    bool pred_value = true;
    if (predicate) {
      pred_value = predicate(p.second);
    }
    if (pred_value) {
      bfiles_copy->push_back(p.second);
    }
  }
}

void BlobDBImpl::FilterSubsetOfFiles(
    const std::vector<std::shared_ptr<BlobFile>>& blob_files,
    std::vector<std::shared_ptr<BlobFile>>* to_process, uint64_t epoch,
    size_t files_to_collect) {
  // 100.0 / 15.0 = 7
  uint64_t next_epoch_increment = static_cast<uint64_t>(
      std::ceil(100 / static_cast<double>(kGCFilePercentage)));
  uint64_t now = EpochNow();

  size_t files_processed = 0;
  bool non_ttl_file_found = false;
  for (auto bfile : blob_files) {
    if (files_processed >= files_to_collect) break;
    // if this is the first time processing the file
    // i.e. gc_epoch == -1, process it.
    // else process the file if its processing epoch matches
    // the current epoch. Typically the #of epochs should be
    // around 5-10
    if (bfile->gc_epoch_ != -1 && (uint64_t)bfile->gc_epoch_ != epoch) {
      continue;
    }

    files_processed++;
    // reset the epoch
    bfile->gc_epoch_ = epoch + next_epoch_increment;

    // file has already been GC'd or is still open for append,
    // then it should not be GC'd
    if (bfile->Obsolete() || !bfile->Immutable()) continue;

    bool is_oldest_non_ttl_file = false;
    if (!non_ttl_file_found && !bfile->HasTTL()) {
      is_oldest_non_ttl_file = true;
      non_ttl_file_found = true;
    }

    std::string reason;
    bool shouldgc = ShouldGCFile(bfile, now, is_oldest_non_ttl_file, &reason);
    if (!shouldgc) {
      ROCKS_LOG_DEBUG(db_options_.info_log,
                      "File has been skipped for GC ttl %s %" PRIu64 " %" PRIu64
                      " reason='%s'",
                      bfile->PathName().c_str(), now,
                      bfile->GetExpirationRange().second, reason.c_str());
      continue;
    }

    ROCKS_LOG_INFO(db_options_.info_log,
                   "File has been chosen for GC ttl %s %" PRIu64 " %" PRIu64
                   " reason='%s'",
                   bfile->PathName().c_str(), now,
                   bfile->GetExpirationRange().second, reason.c_str());
    to_process->push_back(bfile);
  }
}

std::pair<bool, int64_t> BlobDBImpl::RunGC(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  current_epoch_++;

  std::vector<std::shared_ptr<BlobFile>> blob_files;
  CopyBlobFiles(&blob_files);

  if (!blob_files.size()) return std::make_pair(true, -1);

  // 15% of files are collected each call to space out the IO and CPU
  // consumption.
  size_t files_to_collect = (kGCFilePercentage * blob_files.size()) / 100;

  std::vector<std::shared_ptr<BlobFile>> to_process;
  FilterSubsetOfFiles(blob_files, &to_process, current_epoch_,
                      files_to_collect);

  for (auto bfile : to_process) {
    GCStats gc_stats;
    Status s = GCFileAndUpdateLSM(bfile, &gc_stats);
    if (!s.ok()) {
      continue;
    }

    if (bfile->gc_once_after_open_.load()) {
      WriteLock lockbfile_w(&bfile->mutex_);

      bfile->deleted_size_ =
          gc_stats.bytes_overwritten + gc_stats.bytes_expired;
      bfile->deleted_count_ =
          gc_stats.num_keys_overwritten + gc_stats.num_keys_expired;
      bfile->gc_once_after_open_ = false;
    }
  }

  // reschedule
  return std::make_pair(true, -1);
}

Iterator* BlobDBImpl::NewIterator(const ReadOptions& read_options) {
  auto* cfd =
      reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->cfd();
  // Get a snapshot to avoid blob file get deleted between we
  // fetch and index entry and reading from the file.
  ManagedSnapshot* own_snapshot = nullptr;
  const Snapshot* snapshot = read_options.snapshot;
  if (snapshot == nullptr) {
    own_snapshot = new ManagedSnapshot(db_);
    snapshot = own_snapshot->snapshot();
  }
  auto* iter = db_impl_->NewIteratorImpl(
      read_options, cfd, snapshot->GetSequenceNumber(),
      nullptr /*read_callback*/, true /*allow_blob*/);
  return new BlobDBIterator(own_snapshot, iter, this, env_, statistics_);
}

Status DestroyBlobDB(const std::string& dbname, const Options& options,
                     const BlobDBOptions& bdb_options) {
  const ImmutableDBOptions soptions(SanitizeOptions(dbname, options));
  Env* env = soptions.env;

  Status status;
  std::string blobdir;
  blobdir = (bdb_options.path_relative) ? dbname + "/" + bdb_options.blob_dir
                                        : bdb_options.blob_dir;

  std::vector<std::string> filenames;
  env->GetChildren(blobdir, &filenames);

  for (const auto& f : filenames) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kBlobFile) {
      Status del = env->DeleteFile(blobdir + "/" + f);
      if (status.ok() && !del.ok()) {
        status = del;
      }
    }
  }
  env->DeleteDir(blobdir);

  Status destroy = DestroyDB(dbname, options);
  if (status.ok() && !destroy.ok()) {
    status = destroy;
  }

  return status;
}

#ifndef NDEBUG
Status BlobDBImpl::TEST_GetBlobValue(const Slice& key, const Slice& index_entry,
                                     PinnableSlice* value) {
  return GetBlobValue(key, index_entry, value);
}

std::vector<std::shared_ptr<BlobFile>> BlobDBImpl::TEST_GetBlobFiles() const {
  ReadLock l(&mutex_);
  std::vector<std::shared_ptr<BlobFile>> blob_files;
  for (auto& p : blob_files_) {
    blob_files.emplace_back(p.second);
  }
  return blob_files;
}

std::vector<std::shared_ptr<BlobFile>> BlobDBImpl::TEST_GetObsoleteFiles()
    const {
  ReadLock l(&mutex_);
  std::vector<std::shared_ptr<BlobFile>> obsolete_files;
  for (auto& bfile : obsolete_files_) {
    obsolete_files.emplace_back(bfile);
  }
  return obsolete_files;
}

void BlobDBImpl::TEST_DeleteObsoleteFiles() {
  DeleteObsoleteFiles(false /*abort*/);
}

Status BlobDBImpl::TEST_CloseBlobFile(std::shared_ptr<BlobFile>& bfile) {
  return CloseBlobFile(bfile);
}

Status BlobDBImpl::TEST_GCFileAndUpdateLSM(std::shared_ptr<BlobFile>& bfile,
                                           GCStats* gc_stats) {
  return GCFileAndUpdateLSM(bfile, gc_stats);
}

void BlobDBImpl::TEST_RunGC() { RunGC(false /*abort*/); }
#endif  //  !NDEBUG

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
