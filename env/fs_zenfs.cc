// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "rocksdb/utilities/object_registry.h"
#include "util/crc32c.h"

#include "env/fs_zenfs.h"

#define DEFAULT_ZENV_LOG_PATH "/tmp/"

namespace ROCKSDB_NAMESPACE {

IOStatus ZenMetaLog::AddRecord(const Slice& slice) {
  uint32_t record_sz = slice.size();
  const char* data = slice.data();
  size_t phys_sz;
  uint32_t crc = 0;
  char* buffer;
  int ret;
  IOStatus s;

  phys_sz = record_sz + zMetaHeaderSize;

  if (phys_sz % bs_) phys_sz += bs_ - phys_sz % bs_;

  assert(data != nullptr);
  assert((phys_sz % bs_) == 0);

  ret = posix_memalign((void**)&buffer, bs_, phys_sz);
  if (ret) return IOStatus::IOError("Failed to allocate memory");

  memset(buffer, 0, phys_sz);

  crc = crc32c::Extend(crc, (const char*)&record_sz, sizeof(uint32_t));
  crc = crc32c::Extend(crc, data, record_sz);
  crc = crc32c::Mask(crc);

  EncodeFixed32(buffer, crc);
  EncodeFixed32(buffer + sizeof(uint32_t), record_sz);
  memcpy(buffer + sizeof(uint32_t) * 2, data, record_sz);

  s = zone_->Append(buffer, phys_sz);

  free(buffer);
  return s;
}

IOStatus ZenMetaLog::Read(Slice* slice) {
  int f = zbd_->GetReadFD();
  const char* data = slice->data();
  size_t read = 0;
  size_t to_read = slice->size();
  int ret;

  if (read_pos_ >= zone_->wp_) {
    // EOF
    slice->clear();
    return IOStatus::OK();
  }

  if ((read_pos_ + to_read) > (zone_->start_ + zone_->max_capacity_)) {
    return IOStatus::IOError("Read across zone");
  }

  while (read < to_read) {
    ret = pread(f, (void*)(data + read), to_read - read, read_pos_);

    if (ret == -1 && errno == EINTR) continue;
    if (ret < 0) return IOStatus::IOError("Read failed");

    read += ret;
    read_pos_ += ret;
  }

  return IOStatus::OK();
}

IOStatus ZenMetaLog::ReadRecord(Slice* record, std::string* scratch) {
  Slice header;
  uint32_t record_sz = 0;
  uint32_t record_crc = 0;
  uint32_t actual_crc;
  IOStatus s;

  /* TODO: discern different types of errors/corruption? */
  scratch->clear();
  record->clear();

  scratch->append(zMetaHeaderSize, 0);
  header = Slice(scratch->c_str(), zMetaHeaderSize);

  s = Read(&header);
  if (!s.ok()) return s;

  // EOF?
  if (header.size() == 0) {
    record->clear();
    return IOStatus::OK();
  }

  GetFixed32(&header, &record_crc);
  GetFixed32(&header, &record_sz);

  scratch->clear();
  scratch->append(record_sz, 0);

  *record = Slice(scratch->c_str(), record_sz);
  s = Read(record);
  if (!s.ok()) return s;

  actual_crc = crc32c::Value((const char*)&record_sz, sizeof(uint32_t));
  actual_crc = crc32c::Extend(actual_crc, record->data(), record->size());

  if (actual_crc != crc32c::Unmask(record_crc)) {
    return IOStatus::IOError("Not a valid record");
  }

  /* Next record starts on a block boundary */
  if (read_pos_ % bs_) read_pos_ += bs_ - (read_pos_ % bs_);

  return IOStatus::OK();
}

ZenFS::ZenFS(ZonedBlockDevice* zbd, std::shared_ptr<FileSystem> aux_fs,
             std::shared_ptr<Logger> logger)
    : FileSystemWrapper(aux_fs), zbd_(zbd), logger_(logger) {
  Info(logger_, "ZenFS initializing");
  Info(logger_, "ZenFS parameters: block device: %s, aux filesystem: %s",
       zbd_->GetFilename().c_str(), target()->Name());

  Info(logger_, "ZenFS initializing");
  next_file_id_ = 1;
  metadata_writer_.zenFS = this;
}

ZenFS::~ZenFS() {
  Status s;
  Info(logger_, "ZenFS shutting down");
  zbd_->LogZoneUsage();
  LogFiles();

  meta_log_.reset(nullptr);
  ClearFiles();
  delete zbd_;
}

void ZenFS::LogFiles() {
  std::map<std::string, ZoneFile*>::iterator it;
  uint64_t total_size = 0;

  Info(logger_, "  Files:\n");
  for (it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zFile = it->second;
    std::vector<ZoneExtent*> extents = zFile->GetExtents();

    Info(logger_, "    %-45s sz: %lu lh: %d", it->first.c_str(),
         zFile->GetFileSize(), zFile->GetWriteLifeTimeHint());
    for (unsigned int i = 0; i < extents.size(); i++) {
      ZoneExtent* extent = extents[i];
      Info(logger_, "          Extent %u {start=0x%lx, zone=%u, len=%u} ", i,
           extent->start_,
           (uint32_t)(extent->zone_->start_ / zbd_->GetZoneSize()),
           extent->length_);

      total_size += extent->length_;
    }
  }
  Info(logger_, "Sum of all files: %lu MB of data \n",
       total_size / (1024 * 1024));
}

void ZenFS::ClearFiles() {
  std::map<std::string, ZoneFile*>::iterator it;
  for (it = files_.begin(); it != files_.end(); it++) delete it->second;
  files_.clear();
}

IOStatus ZenFS::WriteSnapshot(ZenMetaLog* meta_log) {
  std::string snapshot;

  EncodeSnapshotTo(&snapshot);
  return meta_log->AddRecord(snapshot);
}

IOStatus ZenFS::RollMetaZone() {
  ZenMetaLog* new_meta_log;
  Zone* new_meta_zone;
  IOStatus s;

  new_meta_zone = zbd_->AllocateMetaZone();
  if (!new_meta_zone) {
    assert(false);  // TMP
    Error(logger_, "Out of metadata zones, we should go to read only now.");
    return IOStatus::NoSpace("Out of metadata zones");
  }

  Info(logger_, "Rolling to metazone %d\n", (int)new_meta_zone->GetZoneNr());
  new_meta_log = new ZenMetaLog(zbd_, new_meta_zone);
  meta_log_.reset(new_meta_log);

  std::string super_string;
  superblock_->EncodeTo(&super_string);

  s = meta_log_->AddRecord(super_string);
  if (!s.ok()) {
    Info(logger_, "Current meta zone full, rolling to next meta zone");
    return IOStatus::IOError("Failed writing a new superblock");
  }

  return WriteSnapshot(meta_log_.get());
}

IOStatus ZenFS::PersistSnapshot(ZenMetaLog* meta_writer) {
  IOStatus s;

  files_mtx_.lock();
  metadata_sync_mtx_.lock();

  s = WriteSnapshot(meta_writer);
  if (s == IOStatus::NoSpace()) {
    Info(logger_, "Current meta zone full, rolling to next meta zone");
    s = RollMetaZone();
  }

  if (!s.ok()) {
    assert(false);  // TMP
    Error(logger_,
          "Failed persisting a snapshot, we should go to read only now!");
  }

  metadata_sync_mtx_.unlock();
  files_mtx_.unlock();

  return s;
}

IOStatus ZenFS::SyncFileMetadata(ZoneFile* /*zoneFile*/) {
  /* Write a whole snapshot for now */
  return PersistSnapshot(meta_log_.get());
}

ZoneFile* ZenFS::GetFile(std::string fname) {
  ZoneFile* zoneFile = nullptr;
  files_mtx_.lock();
  if (files_.find(fname) != files_.end()) {
    zoneFile = files_[fname];
  }
  files_mtx_.unlock();
  return zoneFile;
}

bool ZenFS::DeleteFile(std::string fname) {
  ZoneFile* zoneFile = nullptr;
  files_mtx_.lock();
  if (files_.find(fname) != files_.end()) {
    zoneFile = files_[fname];
    files_.erase(fname);
    delete (zoneFile);
  }
  files_mtx_.unlock();
  return (zoneFile != nullptr);
}

IOStatus ZenFS::NewSequentialFile(const std::string& fname,
                                  const FileOptions& file_opts,
                                  std::unique_ptr<FSSequentialFile>* result,
                                  IODebugContext* /*dbg*/) {
  ZoneFile* zoneFile = GetFile(fname);

  Debug(logger_, "New sequential file: %s direct: %d\n", fname.c_str(),
        file_opts.use_direct_reads);

  if (zoneFile == nullptr) {
    return IOStatus::IOError("File does not exist!\n");
  }

  result->reset(new ZonedSequentialFile(zoneFile));
  return IOStatus::OK();
}

IOStatus ZenFS::NewRandomAccessFile(const std::string& fname,
                                    const FileOptions& file_opts,
                                    std::unique_ptr<FSRandomAccessFile>* result,
                                    IODebugContext* /*dbg*/) {
  ZoneFile* zoneFile = GetFile(fname);

  Debug(logger_, "New random access file: %s direct: %d\n", fname.c_str(),
        file_opts.use_direct_reads);

  if (zoneFile == nullptr) {
    return IOStatus::NotFound("File does not exist\n");
  }

  result->reset(new ZonedRandomAccessFile(files_[fname]));
  return IOStatus::OK();
}

IOStatus ZenFS::NewWritableFile(const std::string& fname,
                                const FileOptions& file_opts,
                                std::unique_ptr<FSWritableFile>* result,
                                IODebugContext* /*dbg*/) {
  ZoneFile* zoneFile;
  IOStatus s;
  bool overwrites = false;

  Debug(logger_, "New writable file: %s direct: %d\n", fname.c_str(),
        file_opts.use_direct_writes);

  overwrites = DeleteFile(fname);
  zoneFile = new ZoneFile(zbd_, fname, next_file_id_++);

  files_mtx_.lock();
  files_.insert(std::make_pair(fname.c_str(), zoneFile));
  files_mtx_.unlock();

  result->reset(new ZonedWritableFile(zbd_, true, zoneFile, &metadata_writer_));

  if (overwrites) {
    if (!PersistSnapshot(meta_log_.get()).ok())
      return IOStatus::IOError("Failed to persist file metadata\n");
  }

  return s;
}

IOStatus ZenFS::ReuseWritableFile(const std::string& fname,
                                  const std::string& old_fname,
                                  const FileOptions& file_opts,
                                  std::unique_ptr<FSWritableFile>* result,
                                  IODebugContext* dbg) {
  Debug(logger_, "Reuse writable file: %s old name: %s\n", fname.c_str(),
        old_fname.c_str());

  if (!DeleteFile(old_fname))
    return IOStatus::NotFound("Old file does not exist");

  return NewWritableFile(fname, file_opts, result, dbg);
}

IOStatus ZenFS::FileExists(const std::string& fname, const IOOptions& options,
                           IODebugContext* dbg) {
  Debug(logger_, "FileExists: %s \n", fname.c_str());

  if (GetFile(fname) == nullptr) {
    return target()->FileExists(ToAuxPath(fname), options, dbg);
  } else {
    return IOStatus::OK();
  }
}

IOStatus ZenFS::GetChildren(const std::string& dir, const IOOptions& options,
                            std::vector<std::string>* result,
                            IODebugContext* dbg) {
  std::map<std::string, ZoneFile*>::iterator it;
  std::vector<std::string> auxfiles;
  IOStatus s;

  Debug(logger_, "GetChildren: %s \n", dir.c_str());

  target()->GetChildren(ToAuxPath(dir), options, &auxfiles, dbg);
  for (const auto f : auxfiles) {
    if (f != "." && f != "..") result->push_back(f);
  }

  files_mtx_.lock();
  for (it = files_.begin(); it != files_.end(); it++) {
    std::string fname = it->first;

    if (fname.rfind(dir, 0) == 0) {
      fname.erase(0, dir.length() + 1);
      // Don't report grandchildren
      if (fname.find("/") == std::string::npos) {
        result->push_back(fname);
      }
    }
  }
  files_mtx_.unlock();

  return s;
}

IOStatus ZenFS::DeleteFile(const std::string& fname, const IOOptions& options,
                           IODebugContext* dbg) {
  Debug(logger_, "Delete file: %s \n", fname.c_str());

  if (!DeleteFile(fname)) {
    return target()->DeleteFile(ToAuxPath(fname), options, dbg);
  }

  zbd_->LogZoneStats();

  if (!PersistSnapshot(meta_log_.get()).ok())
    return IOStatus::IOError("Failed to persist file metadata");

  return IOStatus::OK();
}

IOStatus ZenFS::GetFileSize(const std::string& f, const IOOptions& /*options*/,
                            uint64_t* size, IODebugContext* /*dbg*/) {
  ZoneFile* zoneFile;
  IOStatus s;

  Debug(logger_, "GetFileSize: %s \n", f.c_str());

  files_mtx_.lock();
  if (files_.find(f) != files_.end()) {
    zoneFile = files_[f];
    *size = zoneFile->GetFileSize();
  } else {
    s = IOStatus::IOError("file does not exist\n");
  }
  files_mtx_.unlock();

  return s;
}

IOStatus ZenFS::RenameFile(const std::string& f, const std::string& t,
                           const IOOptions& options, IODebugContext* dbg) {
  ZoneFile* zoneFile;
  IOStatus s;

  Debug(logger_, "Rename file: %s to : %s\n", f.c_str(), t.c_str());

  files_mtx_.lock();
  if (files_.find(f) != files_.end()) {
    zoneFile = files_[f];
    files_.erase(f);
    zoneFile->Rename(t);
    if (files_.find(t) != files_.end()) {
      ZoneFile* to_delete = files_[t];

      files_.erase(t);
      delete to_delete;
    }
    files_.insert(std::make_pair(t, zoneFile));
  } else {
    s = target()->RenameFile(ToAuxPath(f), ToAuxPath(t), options, dbg);
  }
  files_mtx_.unlock();

  if (!s.ok()) return s;

  if (!PersistSnapshot(meta_log_.get()).ok())
    return IOStatus::IOError("Failed to persist file metadata");

  return IOStatus::OK();
}

void ZenFS::EncodeSnapshotTo(std::string* output) {
  std::map<std::string, ZoneFile*>::iterator it;
  std::string files_string;
  PutFixed32(output, kCompleteFilesSnapshot);
  for (it = files_.begin(); it != files_.end(); it++) {
    std::string file_string;
    ZoneFile* zFile = it->second;

    zFile->EncodeTo(&file_string);
    PutLengthPrefixedSlice(&files_string, Slice(file_string));
  }
  PutLengthPrefixedSlice(output, Slice(files_string));
}

Status ZenFS::DecodeSnapshotFrom(Slice* input) {
  Slice slice;

  assert(files_.size() == 0);

  while (GetLengthPrefixedSlice(input, &slice)) {
    ZoneFile* zoneFile = new ZoneFile(zbd_, "not_set", 0);
    Status s = zoneFile->DecodeFrom(&slice);

    if (!s.ok()) return s;

    files_.insert(std::make_pair(zoneFile->GetFilename(), zoneFile));
  }

  return Status::OK();
}

Status ZenFS::RecoverFrom(ZenMetaLog* log) {
  bool at_least_one_snapshot = false;
  std::string scratch;
  uint32_t tag = 0;
  Slice record;
  Slice data;
  Status s;

  while (1) {
    IOStatus rs = log->ReadRecord(&record, &scratch);
    if (!rs.ok()) {
      Error(logger_, "Read recovery record failed with error: %s",
            rs.ToString().c_str());
      return Status::Corruption("ZenFS", "Metadata corruption");
    }

    if (!GetFixed32(&record, &tag)) break;

    if (!GetLengthPrefixedSlice(&record, &data))
      return Status::Corruption("ZenFS", "No recovery record data");

    switch (tag) {
      case kCompleteFilesSnapshot:

        ClearFiles();
        s = DecodeSnapshotFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode snapshot: %s", s.ToString().c_str());
          return s;
        }
        at_least_one_snapshot = true;
        break;
      default:
        Warn(logger_, "Unexpected metadata record tag: %u", tag);
        return Status::Corruption("ZenFS", "Unexpected tag");
    }
  }

  if (at_least_one_snapshot)
    return Status::OK();
  else
    return Status::NotFound("ZenFS", "No snapshot found");
}

#define ZENV_URI_PATTERN "zenfs://"

Status ZenFS::Mount() {
  std::vector<Zone*> metazones = zbd_->GetMetaZones();
  std::vector<std::unique_ptr<Superblock>> valid_superblocks;
  std::vector<std::unique_ptr<ZenMetaLog>> valid_logs;
  std::vector<Zone*> valid_zones;
  std::vector<std::pair<uint32_t, uint32_t>> seq_map;

  Status s;

  /* We need a minimum of two non-offline meta data zones */
  if (metazones.size() < 2) {
    Error(logger_,
          "Need at least two non-offline meta zones to open for write");
    return Status::NotSupported();
  }

  /* Find all valid superblocks */
  for (const auto z : metazones) {
    std::unique_ptr<ZenMetaLog> log;
    std::string scratch;
    Slice super_record;

    log.reset(new ZenMetaLog(zbd_, z));

    if (!log->ReadRecord(&super_record, &scratch).ok()) continue;

    if (super_record.size() == 0) continue;

    std::unique_ptr<Superblock> super_block;

    super_block.reset(new Superblock());
    s = super_block->DecodeFrom(&super_record);
    if (s.ok()) s = super_block->CompatibleWith(zbd_);
    if (!s.ok()) return s;

    Info(logger_, "Found OK superblock in zone %lu seq: %u\n", z->GetZoneNr(),
         super_block->GetSeq());

    seq_map.push_back(std::make_pair(super_block->GetSeq(), seq_map.size()));
    valid_superblocks.push_back(std::move(super_block));
    valid_logs.push_back(std::move(log));
    valid_zones.push_back(z);
  }

  if (!seq_map.size()) return Status::NotFound("No valid superblock found");

  /* Sort superblocks by descending sequence number */
  std::sort(seq_map.begin(), seq_map.end(),
            std::greater<std::pair<uint32_t, uint32_t>>());

  bool recovery_ok = false;
  unsigned int r = 0;

  for (const auto sm : seq_map) {
    uint32_t i = sm.second;
    std::string scratch;
    std::unique_ptr<ZenMetaLog> log = std::move(valid_logs[i]);

    s = RecoverFrom(log.get());
    if (!s.ok()) {
      if (s.IsNotFound()) {
        Warn(logger_,
             "Did not find a valid snapshot, trying next meta zone. Error: %s",
             s.ToString().c_str());
        continue;
      }

      Error(logger_, "Metadata corruption. Error: %s", s.ToString().c_str());
      return s;
    }

    r = i;
    recovery_ok = true;
    meta_log_ = std::move(log);
    break;
  }

  if (!recovery_ok) {
    return Status::IOError("Failed to mount filesystem");
  }

  Info(logger_, "Recovered from zone: %d", (int)valid_zones[r]->GetZoneNr());
  superblock_ = std::move(valid_superblocks[r]);
  zbd_->SetFinishTreshold(superblock_->GetFinishTreshold());

  IOOptions foo;
  IODebugContext bar;
  s = target()->CreateDirIfMissing(superblock_->GetAuxFsPath(), foo, &bar);
  if (!s.ok()) {
    Error(logger_, "Failed to create aux filesystem directory.");
    return s;
  }

  /* Free up old metadata zones, to get ready to roll */
  for (const auto sm : seq_map) {
    uint32_t i = sm.second;
    if (i != r) {
      valid_logs[i].reset();
    }
  }
  s = RollMetaZone();
  if (!s.ok()) {
    Error(logger_, "Failed to roll metadata zone.");
    return s;
  }

  Info(logger_, "Superblock sequence %d", (int)superblock_->GetSeq());
  Info(logger_, "Finish threshold %u", superblock_->GetFinishTreshold());
  Info(logger_, "Filesystem mount OK");
  Info(logger_, "Resetting unused IO Zones..");
  zbd_->ResetUnusedIOZones();
  Info(logger_, "  Done");

  LogFiles();

  return Status::OK();
}

Status ZenFS::MkFS(std::string aux_fs_path, uint32_t finish_threshold) {
  std::vector<Zone*> metazones = zbd_->GetMetaZones();
  std::unique_ptr<ZenMetaLog> log;
  Zone* meta_zone = nullptr;
  IOStatus s;

  if (aux_fs_path.length() > 255) {
    return Status::InvalidArgument(
        "Aux filesystem path must be less than 256 bytes\n");
  }

  ClearFiles();

  for (const auto mz : metazones) {
    if (mz->Reset().ok()) {
      if (!meta_zone) meta_zone = mz;
    } else {
      Warn(logger_, "Failed to reset meta zone\n");
    }
  }

  if (!meta_zone) {
    return Status::IOError("Not available meta zones\n");
  }

  log.reset(new ZenMetaLog(zbd_, meta_zone));

  Superblock* super = new Superblock(zbd_, aux_fs_path, finish_threshold);
  std::string super_string;
  super->EncodeTo(&super_string);

  s = log->AddRecord(super_string);
  if (!s.ok()) return s;

  /* Write an empty snapshot to make the metadata zone valid */
  s = PersistSnapshot(log.get());
  if (!s.ok()) {
    Error(logger_, "Failed to persist snapshot: %s", s.ToString().c_str());
    return Status::IOError("Failed persist snapshot");
  }

  Info(logger_, "Empty filesystem created");
  return Status::OK();
}

static std::string GetLogFilename(std::string bdev) {
  std::ostringstream ss;
  time_t t = time(0);
  struct tm* log_start = std::localtime(&t);
  char buf[40];

  std::strftime(buf, sizeof(buf), "%Y-%m-%d_%H:%M:%S.log", log_start);
  ss << DEFAULT_ZENV_LOG_PATH << std::string("zenfs_") << bdev << "_" << buf;

  return ss.str();
}

static FactoryFunc<FileSystem> zoned_reg =
    ObjectLibrary::Default()->Register<FileSystem>(
        ZENV_URI_PATTERN ".*",
        [](const std::string& uri, std::unique_ptr<FileSystem>* env_guard,
           std::string* errmsg) {
          std::string bdev = uri;
          std::string bdevname = uri;
          std::shared_ptr<Logger> logger;
          Status s;

          bdevname.replace(0, strlen(ZENV_URI_PATTERN), "");
          s = Env::Default()->NewLogger(GetLogFilename(bdevname), &logger);

          if (!s.ok()) {
            fprintf(stderr, "ZenFS: Could not create logger");
          } else {
#ifdef NDEBUG
            logger->SetInfoLogLevel(INFO_LEVEL);
#else
            logger->SetInfoLogLevel(DEBUG_LEVEL);
#endif
          }

          bdev.replace(0, strlen(ZENV_URI_PATTERN), "/dev/");

          ZonedBlockDevice* zbd = new ZonedBlockDevice(bdev, logger);
          IOStatus zbd_status = zbd->Open();
          if (!zbd_status.ok()) {
            Error(logger, "Failed to open zoned block device: %s",
                  zbd_status.ToString().c_str());
            *errmsg = zbd_status.ToString();
            env_guard->reset(nullptr);
            return env_guard->get();
          }

          ZenFS* zenFS = new ZenFS(zbd, FileSystem::Default(), logger);
          s = zenFS->Mount();
          if (!s.ok()) {
            Error(logger, "Mount failed: %s", s.ToString().c_str());
            *errmsg = s.ToString();
            env_guard->reset(nullptr);
          } else {
            env_guard->reset(zenFS);
          }

          return env_guard->get();
        });
};  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)
