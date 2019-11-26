// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)

#include "env/fs_zenfs.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <utility>
#include <vector>

#include "rocksdb/utilities/object_registry.h"
#include "util/coding.h"
#include "util/crc32c.h"

#define DEFAULT_ZENV_LOG_PATH "/tmp/"

namespace ROCKSDB_NAMESPACE {

/* Construct a superblock compatible with a zoned block device */
Superblock::Superblock(ZonedBlockDevice* zbd, const std::string& aux_fs_path,
                       uint32_t finish_threshold) {
  std::string uuid = Env::Default()->GenerateUniqueId();
  int uuid_len = std::min(
      uuid.length(), sizeof(uuid_) - 1); /* make sure uuid is nullterminated */
  memcpy((void*)uuid_, uuid.c_str(), uuid_len);
  magic_ = MAGIC;
  version_ = CURRENT_VERSION;
  flags_ = DEFAULT_FLAGS;
  finish_threshold_ = finish_threshold;

  block_size_ = zbd->GetBlockSize();
  zone_size_ = zbd->GetZoneSize() / block_size_;
  nr_zones_ = zbd->GetNrZones();

  strncpy(aux_fs_path_, aux_fs_path.c_str(), sizeof(aux_fs_path_) - 1);
}

/* Decode a superblock from stored metadata */
Status Superblock::DecodeFrom(Slice* input) {
  if (input->size() != ENCODED_SIZE) {
    return Status::Corruption("ZenFS Superblock",
                              "Error: Superblock size missmatch");
  }

  GetFixed32(input, &magic_);
  memcpy(&uuid_, input->data(), sizeof(uuid_));
  input->remove_prefix(sizeof(uuid_));
  GetFixed32(input, &sequence_);
  GetFixed32(input, &version_);
  GetFixed32(input, &flags_);
  GetFixed32(input, &block_size_);
  GetFixed32(input, &zone_size_);
  GetFixed32(input, &nr_zones_);
  GetFixed32(input, &finish_threshold_);
  memcpy(&aux_fs_path_, input->data(), sizeof(aux_fs_path_));
  input->remove_prefix(sizeof(aux_fs_path_));
  memcpy(&reserved_, input->data(), sizeof(reserved_));
  input->remove_prefix(sizeof(reserved_));
  assert(input->size() == 0);

  if (magic_ != MAGIC)
    return Status::Corruption("ZenFS Superblock", "Error: Magic missmatch");
  if (version_ != CURRENT_VERSION)
    return Status::Corruption("ZenFS Superblock", "Error: Version missmatch");

  return Status::OK();
}

/* Encode a superblock for metadata storage */
void Superblock::EncodeTo(std::string* output) {
  sequence_++; /* Ensure that this superblock representation is unique */
  output->clear();
  PutFixed32(output, magic_);
  output->append(uuid_, sizeof(uuid_));
  PutFixed32(output, sequence_);
  PutFixed32(output, version_);
  PutFixed32(output, flags_);
  PutFixed32(output, block_size_);
  PutFixed32(output, zone_size_);
  PutFixed32(output, nr_zones_);
  PutFixed32(output, finish_threshold_);
  output->append(aux_fs_path_, sizeof(aux_fs_path_));
  output->append(reserved_, sizeof(reserved_));
  assert(output->length() == ENCODED_SIZE);
}

/* Check that the superblock is compatible with a zoned block device */
Status Superblock::CompatibleWith(ZonedBlockDevice* zbd) {
  if (block_size_ != zbd->GetBlockSize())
    return Status::Corruption("ZenFS Superblock",
                              "Error: block size missmatch");
  if (zone_size_ != (zbd->GetZoneSize() / block_size_))
    return Status::Corruption("ZenFS Superblock", "Error: zone size missmatch");
  if (nr_zones_ > zbd->GetNrZones())
    return Status::Corruption("ZenFS Superblock",
                              "Error: nr of zones missmatch");

  return Status::OK();
}

/* Store a metadata record
   Metadata records as a CRC(covering the rest) + record lenght + record data
   Padding is added to make every record block-aligned
*/
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

/* Read metadata from a zone(up to the zone write pointer) */
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
/* Read a metadata record, checking the CRC and ensuring
   that the next read starts on a block boundary */
IOStatus ZenMetaLog::ReadRecord(Slice* record, std::string* scratch) {
  Slice header;
  uint32_t record_sz = 0;
  uint32_t record_crc = 0;
  uint32_t actual_crc;
  IOStatus s;

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

/* Log information about all files and their extents in the file system */
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

/* Remove all files from memory */
void ZenFS::ClearFiles() {
  std::map<std::string, ZoneFile*>::iterator it;
  files_mtx_.lock();
  for (it = files_.begin(); it != files_.end(); it++) delete it->second;
  files_.clear();
  files_mtx_.unlock();
}

/* Write a complete file system snapshot of all files
  Assumes that files_mutex_ is held */
IOStatus ZenFS::WriteSnapshotLocked(ZenMetaLog* meta_log) {
  IOStatus s;
  std::string snapshot;

  EncodeSnapshotTo(&snapshot);
  s = meta_log->AddRecord(snapshot);
  if (s.ok()) {
    for (auto it = files_.begin(); it != files_.end(); it++) {
      ZoneFile* zoneFile = it->second;
      zoneFile->MetadataSynced();
    }
  }
  return s;
}

/* Write an end record marking the end of the metadata in a zone */
IOStatus ZenFS::WriteEndRecord(ZenMetaLog* meta_log) {
  std::string endRecord;

  PutFixed32(&endRecord, kEndRecord);
  return meta_log->AddRecord(endRecord);
}

/* Switch to a new metadata zone by storing the superblock (with increased
   sequence number followed by a complete snapshot of all files
   effectively compressing the log in the current metadata zone and
   pointing the log writer to the new zone.
   Assumes the files_mtx_ is held
*/
IOStatus ZenFS::RollMetaZoneLocked() {
  ZenMetaLog* new_meta_log;
  Zone *new_meta_zone, *old_meta_zone;
  IOStatus s;

  new_meta_zone = zbd_->AllocateMetaZone();
  if (!new_meta_zone) {
    assert(false);  // TMP
    Error(logger_, "Out of metadata zones, we should go to read only now.");
    return IOStatus::NoSpace("Out of metadata zones");
  }

  Info(logger_, "Rolling to metazone %d\n", (int)new_meta_zone->GetZoneNr());
  new_meta_log = new ZenMetaLog(zbd_, new_meta_zone);

  old_meta_zone = meta_log_->GetZone();
  old_meta_zone->open_for_write_ = false;

  /* Write an end record and finish the meta data zone if there is space left */
  if (old_meta_zone->GetCapacityLeft()) WriteEndRecord(meta_log_.get());
  if (old_meta_zone->GetCapacityLeft()) old_meta_zone->Finish();

  meta_log_.reset(new_meta_log);

  std::string super_string;
  superblock_->EncodeTo(&super_string);

  s = meta_log_->AddRecord(super_string);
  if (!s.ok()) {
    Error(logger_,
          "Could not write super block when rolling to a new meta zone");
    return IOStatus::IOError("Failed writing a new superblock");
  }

  s = WriteSnapshotLocked(meta_log_.get());

  /* We've rolled successfully, we can reset the old zone now */
  if (s.ok()) old_meta_zone->Reset();

  return s;
}

/* Persist a competete snapshot of all files */
IOStatus ZenFS::PersistSnapshot(ZenMetaLog* meta_writer) {
  IOStatus s;

  files_mtx_.lock();
  metadata_sync_mtx_.lock();

  s = WriteSnapshotLocked(meta_writer);
  if (s == IOStatus::NoSpace()) {
    Info(logger_, "Current meta zone full, rolling to next meta zone");
    s = RollMetaZoneLocked();
  }

  if (!s.ok()) {
    Error(logger_,
          "Failed persisting a snapshot, we should go to read only now!");
  }

  metadata_sync_mtx_.unlock();
  files_mtx_.unlock();

  return s;
}

IOStatus ZenFS::PersistRecord(std::string record) {
  IOStatus s;

  metadata_sync_mtx_.lock();
  s = meta_log_->AddRecord(record);
  if (s == IOStatus::NoSpace()) {
    Info(logger_, "Current meta zone full, rolling to next meta zone");
    s = RollMetaZoneLocked();
    /* After a successfull roll, a complete snapshot has been persisted
     * - no need to write the record update */
  }
  metadata_sync_mtx_.unlock();

  return s;
}

/* Encode file metadata and store it in the current metadata zone */
IOStatus ZenFS::SyncFileMetadata(ZoneFile* zoneFile) {
  std::string fileRecord;
  std::string output;

  IOStatus s;

  files_mtx_.lock();

  PutFixed32(&output, kFileUpdate);
  zoneFile->EncodeUpdateTo(&fileRecord);
  PutLengthPrefixedSlice(&output, Slice(fileRecord));

  s = PersistRecord(output);
  if (s.ok()) zoneFile->MetadataSynced();

  files_mtx_.unlock();

  return s;
}

/* Look up a file based on name */
ZoneFile* ZenFS::GetFile(std::string fname) {
  ZoneFile* zoneFile = nullptr;
  files_mtx_.lock();
  if (files_.find(fname) != files_.end()) {
    zoneFile = files_[fname];
  }
  files_mtx_.unlock();
  return zoneFile;
}

/* Delete a file in the file list and persist a file deletion
   Roll back the delete if the delete metadata record was not written */
IOStatus ZenFS::DeleteFile(std::string fname) {
  ZoneFile* zoneFile = nullptr;
  IOStatus s;

  zoneFile = GetFile(fname);
  files_mtx_.lock();
  if (zoneFile != nullptr) {
    std::string record;

    zoneFile = files_[fname];
    files_.erase(fname);

    EncodeFileDeletionTo(zoneFile, &record);
    s = PersistRecord(record);
    if (!s.ok()) {
      /* Failed to persist the delete, return to a consistent state */
      files_.insert(std::make_pair(fname.c_str(), zoneFile));
    } else {
      delete (zoneFile);
    }
  }
  files_mtx_.unlock();
  return s;
}

IOStatus ZenFS::NewSequentialFile(const std::string& fname,
                                  const FileOptions& file_opts,
                                  std::unique_ptr<FSSequentialFile>* result,
                                  IODebugContext* dbg) {
  ZoneFile* zoneFile = GetFile(fname);

  Debug(logger_, "New sequential file: %s direct: %d\n", fname.c_str(),
        file_opts.use_direct_reads);

  if (zoneFile == nullptr) {
    return target()->NewSequentialFile(ToAuxPath(fname), file_opts, result,
                                       dbg);
  }

  result->reset(new ZonedSequentialFile(zoneFile, file_opts));
  return IOStatus::OK();
}

IOStatus ZenFS::NewRandomAccessFile(const std::string& fname,
                                    const FileOptions& file_opts,
                                    std::unique_ptr<FSRandomAccessFile>* result,
                                    IODebugContext* dbg) {
  ZoneFile* zoneFile = GetFile(fname);

  Debug(logger_, "New random access file: %s direct: %d\n", fname.c_str(),
        file_opts.use_direct_reads);

  if (zoneFile == nullptr) {
    return target()->NewRandomAccessFile(ToAuxPath(fname), file_opts, result,
                                         dbg);
  }

  result->reset(new ZonedRandomAccessFile(files_[fname], file_opts));
  return IOStatus::OK();
}

IOStatus ZenFS::NewWritableFile(const std::string& fname,
                                const FileOptions& file_opts,
                                std::unique_ptr<FSWritableFile>* result,
                                IODebugContext* /*dbg*/) {
  ZoneFile* zoneFile;
  IOStatus s;

  Debug(logger_, "New writable file: %s direct: %d\n", fname.c_str(),
        file_opts.use_direct_writes);

  if (GetFile(fname) != nullptr) {
    s = DeleteFile(fname);
    if (!s.ok()) return s;
  }

  zoneFile = new ZoneFile(zbd_, fname, next_file_id_++);

  files_mtx_.lock();
  files_.insert(std::make_pair(fname.c_str(), zoneFile));
  files_mtx_.unlock();

  result->reset(new ZonedWritableFile(zbd_, true, zoneFile, &metadata_writer_));

  return s;
}

IOStatus ZenFS::ReuseWritableFile(const std::string& fname,
                                  const std::string& old_fname,
                                  const FileOptions& file_opts,
                                  std::unique_ptr<FSWritableFile>* result,
                                  IODebugContext* dbg) {
  Debug(logger_, "Reuse writable file: %s old name: %s\n", fname.c_str(),
        old_fname.c_str());

  if (GetFile(fname) == nullptr)
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

IOStatus ZenFS::ReopenWritableFile(const std::string& fname,
                                   const FileOptions& options,
                                   std::unique_ptr<FSWritableFile>* result,
                                   IODebugContext* dbg) {
  Debug(logger_, "Reopen writable file: %s \n", fname.c_str());

  if (GetFile(fname) != nullptr)
    return NewWritableFile(fname, options, result, dbg);

  return target()->NewWritableFile(fname, options, result, dbg);
}

/* List children of a specified directory. The directory structure is maintained
   under aux_path in the aux file system */
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
      if (dir.back() == '/') {
        fname.erase(0, dir.length());
      } else {
        fname.erase(0, dir.length() + 1);
      }
      // Don't report grandchildren
      if (fname.find("/") == std::string::npos) {
        result->push_back(fname);
      }
    }
  }
  files_mtx_.unlock();

  return s;
}

/* Delete a file in zenfs or in the aux file system */
IOStatus ZenFS::DeleteFile(const std::string& fname, const IOOptions& options,
                           IODebugContext* dbg) {
  IOStatus s;
  Debug(logger_, "Delete file: %s \n", fname.c_str());

  if (GetFile(fname) == nullptr) {
    return target()->DeleteFile(ToAuxPath(fname), options, dbg);
  }

  s = DeleteFile(fname);
  zbd_->LogZoneStats();

  return s;
}

/* Get the file size for a file in zenfs or in the aux file system */
IOStatus ZenFS::GetFileSize(const std::string& f, const IOOptions& options,
                            uint64_t* size, IODebugContext* dbg) {
  ZoneFile* zoneFile;
  IOStatus s;

  Debug(logger_, "GetFileSize: %s \n", f.c_str());

  files_mtx_.lock();
  if (files_.find(f) != files_.end()) {
    zoneFile = files_[f];
    *size = zoneFile->GetFileSize();
  } else {
    s = target()->GetFileSize(ToAuxPath(f), options, size, dbg);
  }
  files_mtx_.unlock();

  return s;
}

/* Rename a file in zenfs or in the aux file system */
IOStatus ZenFS::RenameFile(const std::string& f, const std::string& t,
                           const IOOptions& options, IODebugContext* dbg) {
  ZoneFile* zoneFile;
  IOStatus s;

  Debug(logger_, "Rename file: %s to : %s\n", f.c_str(), t.c_str());

  zoneFile = GetFile(f);
  if (zoneFile != nullptr) {
    s = DeleteFile(t);
    if (s.ok()) {
      files_mtx_.lock();
      files_.erase(f);
      zoneFile->Rename(t);
      files_.insert(std::make_pair(t, zoneFile));
      files_mtx_.unlock();

      s = SyncFileMetadata(zoneFile);
      if (!s.ok()) {
        /* Failed to persist the rename, roll back */
        files_mtx_.lock();
        files_.erase(t);
        zoneFile->Rename(f);
        files_.insert(std::make_pair(f, zoneFile));
        files_mtx_.unlock();
      }
    }
  } else {
    s = target()->RenameFile(ToAuxPath(f), ToAuxPath(t), options, dbg);
  }

  return s;
}

/* Encode a full file system snapshot for metadata storage */
void ZenFS::EncodeSnapshotTo(std::string* output) {
  std::map<std::string, ZoneFile*>::iterator it;
  std::string files_string;
  PutFixed32(output, kCompleteFilesSnapshot);
  for (it = files_.begin(); it != files_.end(); it++) {
    std::string file_string;
    ZoneFile* zFile = it->second;

    zFile->EncodeSnapshotTo(&file_string);
    PutLengthPrefixedSlice(&files_string, Slice(file_string));
  }
  PutLengthPrefixedSlice(output, Slice(files_string));
}

/* Decode file metadata. This might be a an existing file
   or an update to an existing file */
Status ZenFS::DecodeFileUpdateFrom(Slice* slice) {
  ZoneFile* update = new ZoneFile(zbd_, "not_set", 0);
  uint64_t id;
  Status s;

  s = update->DecodeFrom(slice);
  if (!s.ok()) return s;

  id = update->GetID();
  if (id >= next_file_id_) next_file_id_ = id + 1;

  /* Check if this is an update to an existing file */
  for (auto it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zFile = it->second;
    if (id == zFile->GetID()) {
      std::string oldName = zFile->GetFilename();

      s = zFile->MergeUpdate(update);
      delete update;

      if (!s.ok()) return s;

      if (zFile->GetFilename() != oldName) {
        files_.erase(oldName);
        files_.insert(std::make_pair(zFile->GetFilename(), zFile));
      }

      return Status::OK();
    }
  }

  /* The update is a new file */
  assert(GetFile(update->GetFilename()) == nullptr);
  files_.insert(std::make_pair(update->GetFilename(), update));

  return Status::OK();
}

/* Decode a complete snapshot*/
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

/* Encode a file deletion for metadata storage*/
void ZenFS::EncodeFileDeletionTo(ZoneFile* zoneFile, std::string* output) {
  std::string file_string;

  PutFixed64(&file_string, zoneFile->GetID());
  PutLengthPrefixedSlice(&file_string, Slice(zoneFile->GetFilename()));

  PutFixed32(output, kFileDeletion);
  PutLengthPrefixedSlice(output, Slice(file_string));
}

/* Decode a file deletion record */
Status ZenFS::DecodeFileDeletionFrom(Slice* input) {
  uint64_t fileID;
  std::string fileName;
  Slice slice;

  if (!GetFixed64(input, &fileID))
    return Status::Corruption("Zone file deletion: file id missing");

  if (!GetLengthPrefixedSlice(input, &slice))
    return Status::Corruption("Zone file deletion: file name missing");

  fileName = slice.ToString();
  if (files_.find(fileName) == files_.end())
    return Status::Corruption("Zone file deletion: no such file");

  ZoneFile* zoneFile = files_[fileName];
  if (zoneFile->GetID() != fileID)
    return Status::Corruption("Zone file deletion: file ID missmatch");

  files_.erase(fileName);
  delete zoneFile;

  return Status::OK();
}

/* Recover/Recreate the file system state from a log stored in
   a metadata zone */
Status ZenFS::RecoverFrom(ZenMetaLog* log) {
  bool at_least_one_snapshot = false;
  std::string scratch;
  uint32_t tag = 0;
  Slice record;
  Slice data;
  Status s;
  bool done = false;

  while (!done) {
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
          Warn(logger_, "Could not decode complete snapshot: %s",
               s.ToString().c_str());
          return s;
        }
        at_least_one_snapshot = true;
        break;

      case kFileUpdate:
        s = DecodeFileUpdateFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode file snapshot: %s",
               s.ToString().c_str());
          return s;
        }
        break;

      case kFileDeletion:
        s = DecodeFileDeletionFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode file deletion: %s",
               s.ToString().c_str());
          return s;
        }
        break;

      case kEndRecord:
        done = true;
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

/* Mount the filesystem by recovering form the latest valid metadata zone */
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

  /* Recover from the zone with the highest superblock sequence number.
     If that fails go to the previous as we might have crashed when rolling
     metadata zone.
  */
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
    /* Don't reset the current metadata zone */
    if (i != r) {
      /* Metadata zones are not marked as having valid data, so they can be
       * reset */
      valid_logs[i].reset();
    }
  }

  files_mtx_.lock();
  s = RollMetaZoneLocked();
  if (!s.ok()) {
    files_mtx_.unlock();
    Error(logger_, "Failed to roll metadata zone.");
    return s;
  }
  files_mtx_.unlock();

  Info(logger_, "Superblock sequence %d", (int)superblock_->GetSeq());
  Info(logger_, "Finish threshold %u", superblock_->GetFinishTreshold());
  Info(logger_, "Filesystem mount OK");
  Info(logger_, "Resetting unused IO Zones..");
  zbd_->ResetUnusedIOZones();
  Info(logger_, "  Done");

  LogFiles();

  return Status::OK();
}

/* Create a zenfs file system by resetting all metada data zones
   and storing a superblock followed by an empty snapshot
*/
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
  zbd_->ResetUnusedIOZones();

  for (const auto mz : metazones) {
    if (mz->Reset().ok()) {
      if (!meta_zone) meta_zone = mz;
    } else {
      Warn(logger_, "Failed to reset meta zone\n");
    }
  }

  if (!meta_zone) {
    return Status::IOError("No available meta zones\n");
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

#ifndef NDEBUG
static std::string GetLogFilename(std::string bdev) {
  std::ostringstream ss;
  time_t t = time(0);
  struct tm* log_start = std::localtime(&t);
  char buf[40];

  std::strftime(buf, sizeof(buf), "%Y-%m-%d_%H:%M:%S.log", log_start);
  ss << DEFAULT_ZENV_LOG_PATH << std::string("zenfs_") << bdev << "_" << buf;

  return ss.str();
}
#endif

/* Set up an mount a ZenFS file system stored on a zoned block device */
Status NewZenFS(FileSystem** fs, const std::string& bdevname,
                const std::shared_ptr<FileSystem>& auxFS) {
  std::shared_ptr<Logger> logger;
  Status s;

#ifndef NDEBUG
  IOOptions options;
  IODebugContext dbg;
  s = auxFS->NewLogger(GetLogFilename(bdevname), options, &logger, &dbg);
  if (!s.ok()) {
    fprintf(stderr, "ZenFS: Could not create logger");
  } else {
    logger->SetInfoLogLevel(DEBUG_LEVEL);
  }
#endif

  ZonedBlockDevice* zbd = new ZonedBlockDevice(bdevname, logger);
  IOStatus zbd_status = zbd->Open();
  if (!zbd_status.ok()) {
    free(zbd);
    Error(logger, "Failed to open zoned block device: %s",
          zbd_status.ToString().c_str());
    return Status::IOError(zbd_status.ToString());
  }

  ZenFS* zenFS = new ZenFS(zbd, auxFS, logger);
  s = zenFS->Mount();
  if (!s.ok()) {
    delete zenFS;
    return s;
  }

  *fs = zenFS;
  return Status::OK();
}

/* List all ZenFS file systems on the system */
std::map<std::string, std::string> ListZenFileSystems() {
  std::map<std::string, std::string> zenFileSystems;
  DIR* dir = opendir("/sys/class/block");
  struct dirent* entry;

  while (NULL != (entry = readdir(dir))) {
    if (entry->d_type == DT_LNK) {
      std::string zbdName = std::string(entry->d_name);
      ZonedBlockDevice zbd(zbdName, nullptr);
      IOStatus zbd_status = zbd.Open(true);

      if (zbd_status.ok()) {
        std::vector<Zone*> metazones = zbd.GetMetaZones();
        std::string scratch;
        Slice super_record;
        Status s;

        for (const auto z : metazones) {
          Superblock super_block;
          ZenMetaLog log(&zbd, z);

          if (!log.ReadRecord(&super_record, &scratch).ok()) continue;
          s = super_block.DecodeFrom(&super_record);
          if (s.ok()) {
            /* Map the uuid to the device-mapped (i.g dm-linear) block device to
               avoid trying to mount the whole block device in case of a split
               device */
            if (zenFileSystems.find(super_block.GetUUID()) !=
                    zenFileSystems.end() &&
                zenFileSystems[super_block.GetUUID()].rfind("dm-", 0) == 0) {
              break;
            }
            zenFileSystems[super_block.GetUUID()] = zbdName;
            break;
          }
        }
        continue;
      }
    }
  }

  closedir(dir);
  return zenFileSystems;
}

};  // namespace ROCKSDB_NAMESPACE

#else

#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
Status NewZenFS(FileSystem** /*fs*/, const std::string& /*bdevname*/,
                const std::shared_ptr<FileSystem>& /*auxFS*/) {
  return Status::NotSupported("Not built with ZenFS support\n");
}
std::map<std::string, std::string> ListZenFileSystems() {
  std::map<std::string, std::string> zenFileSystems;
  return zenFileSystems;
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)
