// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"
#include "util/coding.h"

#include "env/io_zenfs.h"
#include "env/zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

class Superblock {
  uint32_t magic_ = 0;
  char uuid_[37] = {0};
  uint32_t sequence_ = 0;
  uint32_t version_ = 0;
  uint32_t flags_ = 0;
  uint32_t block_size_ = 0; /* in bytes */
  uint32_t zone_size_ = 0;  /* in blocks */
  uint32_t nr_zones_ = 0;
  char aux_fs_path_[256] = {0};
  uint32_t finish_treshold_ = 0;
  char reserved_[187] = {0};

 public:
  const uint32_t MAGIC = 0x5a454e46; /* ZENF */
  const uint32_t ENCODED_SIZE = 512;
  const uint32_t CURRENT_VERSION = 0; /* Experimental version */
  const uint32_t DEFAULT_FLAGS = 0;

  Superblock() {}

  /* Create a superblock for a filesystem covering the entire zoned block device
   */
  Superblock(ZonedBlockDevice* zbd, std::string aux_fs_path = "",
             uint32_t finish_threshold = 0) {
    std::string uuid = Env::Default()->GenerateUniqueId();
    assert(uuid.length() == sizeof(uuid_));
    magic_ = MAGIC;
    memcpy((void*)uuid_, uuid.c_str(), sizeof(uuid_));
    version_ = CURRENT_VERSION;
    flags_ = DEFAULT_FLAGS;
    finish_treshold_ = finish_threshold;

    block_size_ = zbd->GetBlockSize();
    zone_size_ = zbd->GetZoneSize() / block_size_;
    nr_zones_ = zbd->GetNrZones();

    assert(aux_fs_path.length() < sizeof(aux_fs_path_));
    strcpy(aux_fs_path_, aux_fs_path.c_str());
  }

  Status DecodeFrom(Slice* input) {
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
    GetFixed32(input, &finish_treshold_);
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

  void EncodeTo(std::string* output) {
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
    PutFixed32(output, finish_treshold_);
    output->append(aux_fs_path_, sizeof(aux_fs_path_));
    output->append(reserved_, sizeof(reserved_));
    assert(output->length() == ENCODED_SIZE);
  }

  Status CompatibleWith(ZonedBlockDevice* zbd) {
    if (block_size_ != zbd->GetBlockSize())
      return Status::Corruption("ZenFS Superblock",
                                "Error: block size missmatch");
    if (zone_size_ != (zbd->GetZoneSize() / block_size_))
      return Status::Corruption("ZenFS Superblock",
                                "Error: zone size missmatch");
    if (nr_zones_ > zbd->GetNrZones())
      return Status::Corruption("ZenFS Superblock",
                                "Error: nr of zones missmatch");

    return Status::OK();
  }

  uint32_t GetSeq() { return sequence_; }
  std::string GetAuxFsPath() { return std::string(aux_fs_path_); }
  uint32_t GetFinishTreshold() { return finish_treshold_; }
};

class ZenMetaLog {
  uint64_t read_pos_;
  Zone* zone_;
  ZonedBlockDevice* zbd_;
  size_t bs_;

  /* Every meta log record is prefixed with a CRC(32 bits) and record length (32
   * bits) */
  const size_t zMetaHeaderSize = sizeof(uint32_t) * 2;

 public:
  ZenMetaLog(ZonedBlockDevice* zbd, Zone* zone) {
    zbd_ = zbd;
    zone_ = zone;
    zone_->open_for_write_ = true;
    bs_ = zbd_->GetBlockSize();
    read_pos_ = zone->start_;
  }

  virtual ~ZenMetaLog() { zone_->open_for_write_ = false; }

  IOStatus AddRecord(const Slice& slice);
  IOStatus ReadRecord(Slice* record, std::string* scratch);

 private:
  IOStatus Read(Slice* slice);
};

class ZenFS : public FileSystemWrapper {
  ZonedBlockDevice* zbd_;
  std::map<std::string, ZoneFile*> files_;
  std::mutex files_mtx_;
  std::shared_ptr<Logger> logger_;
  std::atomic<uint64_t> next_file_id_;

  Zone* cur_meta_zone_ = nullptr;
  std::unique_ptr<ZenMetaLog> meta_log_;
  std::mutex metadata_sync_mtx_;
  std::unique_ptr<Superblock> superblock_;

  std::shared_ptr<Logger> GetLogger() { return logger_; }

  struct MetadataWriter : public ZonedWritableFile::MetadataWriter {
    ZenFS* zenFS;
    IOStatus Persist(ZoneFile* zoneFile) {
      Debug(zenFS->GetLogger(), "Syncing metadata for: %s",
            zoneFile->GetFilename().c_str());
      return zenFS->SyncFileMetadata(zoneFile);
    }
  };

  MetadataWriter metadata_writer_;

  enum ZenFSTag : uint32_t {
    kCompleteFilesSnapshot = 1,
  };

  void LogFiles();
  void ClearFiles();
  IOStatus WriteSnapshot(ZenMetaLog* meta_log);
  IOStatus RollMetaZone();
  IOStatus PersistSnapshot(ZenMetaLog* meta_writer);
  IOStatus SyncFileMetadata(ZoneFile* zoneFile);

  void EncodeSnapshotTo(std::string* output);
  Status DecodeSnapshotFrom(Slice* input);
  Status RecoverFrom(ZenMetaLog* log);

  std::string ToAuxPath(std::string path) {
    return superblock_->GetAuxFsPath() + path;
  }

  std::string ToZenFSPath(std::string aux_path) {
    std::string path = aux_path;
    path.erase(0, superblock_->GetAuxFsPath().length());
    return path;
  }

  ZoneFile* GetFile(std::string fname);
  bool DeleteFile(std::string fname);

 public:
  explicit ZenFS(ZonedBlockDevice* zbd, std::shared_ptr<FileSystem> aux_fs,
                 std::shared_ptr<Logger> logger);
  virtual ~ZenFS();

  Status Mount();
  Status MkFS(std::string aux_fs_path, uint32_t finish_threshold);

  const char* Name() const override {
    return "ZenFS - The Zoned-enabled File System";
  }

  virtual IOStatus NewSequentialFile(const std::string& fname,
                                     const FileOptions& file_opts,
                                     std::unique_ptr<FSSequentialFile>* result,
                                     IODebugContext* dbg) override;
  virtual IOStatus NewRandomAccessFile(
      const std::string& fname, const FileOptions& file_opts,
      std::unique_ptr<FSRandomAccessFile>* result,
      IODebugContext* dbg) override;
  virtual IOStatus NewWritableFile(const std::string& fname,
                                   const FileOptions& file_opts,
                                   std::unique_ptr<FSWritableFile>* result,
                                   IODebugContext* dbg) override;
  virtual IOStatus ReuseWritableFile(const std::string& fname,
                                     const std::string& old_fname,
                                     const FileOptions& file_opts,
                                     std::unique_ptr<FSWritableFile>* result,
                                     IODebugContext* dbg) override;
  virtual IOStatus FileExists(const std::string& fname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;
  virtual IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                               std::vector<std::string>* result,
                               IODebugContext* dbg) override;
  virtual IOStatus DeleteFile(const std::string& fname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;
  IOStatus GetFileSize(const std::string& f, const IOOptions& options,
                       uint64_t* size, IODebugContext* dbg) override;
  IOStatus RenameFile(const std::string& f, const std::string& t,
                      const IOOptions& options, IODebugContext* dbg) override;

  IOStatus GetFreeSpace(const std::string& /*path*/,
                        const IOOptions& /*options*/, uint64_t* diskfree,
                        IODebugContext* /*dbg*/) override {
    *diskfree = zbd_->GetFreeSpace();
    return IOStatus::OK();
  }

  // The directory structure is stored in the aux file system
  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    Debug(logger_, "NewDirectory: %s to aux: %s\n", name.c_str(),
          ToAuxPath(name).c_str());
    return target()->NewDirectory(ToAuxPath(name), io_opts, result, dbg);
  }

  IOStatus CreateDir(const std::string& d, const IOOptions& options,
                     IODebugContext* dbg) override {
    Debug(logger_, "CreatDir: %s to aux: %s\n", d.c_str(),
          ToAuxPath(d).c_str());
    return target()->CreateDir(ToAuxPath(d), options, dbg);
  }

  IOStatus CreateDirIfMissing(const std::string& d, const IOOptions& options,
                              IODebugContext* dbg) override {
    Debug(logger_, "CreatDirIfMissing: %s to aux: %s\n", d.c_str(),
          ToAuxPath(d).c_str());
    return target()->CreateDirIfMissing(ToAuxPath(d), options, dbg);
  }

  IOStatus DeleteDir(const std::string& d, const IOOptions& options,
                     IODebugContext* dbg) override {
    std::vector<std::string> children;
    IOStatus s;

    Debug(logger_, "DeleteDir: %s aux: %s\n", d.c_str(), ToAuxPath(d).c_str());

    s = GetChildren(d, options, &children, dbg);
    if (children.size() != 0)
      return IOStatus::IOError("Directory has children");

    return target()->DeleteDir(ToAuxPath(d), options, dbg);
  }

  // We might want to override these in the future
  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override {
    return target()->GetAbsolutePath(db_path, options, output_path, dbg);
  }

  IOStatus LockFile(const std::string& f, const IOOptions& options,
                    FileLock** l, IODebugContext* dbg) override {
    return target()->LockFile(ToAuxPath(f), options, l, dbg);
  }

  IOStatus UnlockFile(FileLock* l, const IOOptions& options,
                      IODebugContext* dbg) override {
    return target()->UnlockFile(l, options, dbg);
  }

  IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                            IODebugContext* dbg) override {
    *path = "/rocksdbtest";
    Debug(logger_, "GetTestDirectory: %s aux: %s\n", path->c_str(),
          ToAuxPath(*path).c_str());
    return target()->CreateDirIfMissing(ToAuxPath(*path), options, dbg);
  }

  IOStatus NewLogger(const std::string& fname, const IOOptions& options,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override {
    return target()->NewLogger(ToAuxPath(fname), options, result, dbg);
  }

  // Not supported (at least not yet)
  IOStatus Truncate(const std::string& /*fname*/, size_t /*size*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("Truncate is not implemented in ZenFS");
    return IOStatus::OK();
  }

  virtual IOStatus ReopenWritableFile(
      const std::string& /*fname*/, const FileOptions& /*options*/,
      std::unique_ptr<FSWritableFile>* /*result*/, IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported(
        "ReopenWritableFile is not implemented in ZenFS");
  }

  virtual IOStatus NewRandomRWFile(const std::string& /*fname*/,
                                   const FileOptions& /*options*/,
                                   std::unique_ptr<FSRandomRWFile>* /*result*/,
                                   IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("RandomRWFile is not implemented in ZenFS");
  }

  virtual IOStatus NewMemoryMappedFileBuffer(
      const std::string& /*fname*/,
      std::unique_ptr<MemoryMappedFileBuffer>* /*result*/) override {
    return IOStatus::NotSupported(
        "MemoryMappedFileBuffer is not implemented in ZenFS");
  }

  IOStatus GetFileModificationTime(const std::string& /*fname*/,
                                   const IOOptions& /*options*/,
                                   uint64_t* /*file_mtime*/,
                                   IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported(
        "GetFileModificationTime is not implemented in ZenFS");
  }

  virtual IOStatus LinkFile(const std::string& /*src*/,
                            const std::string& /*target*/,
                            const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("LinkFile is not supported in ZenFS");
  }

  virtual IOStatus NumFileLinks(const std::string& /*fname*/,
                                const IOOptions& /*options*/,
                                uint64_t* /*count*/, IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported(
        "Getting number of file links is not supported in ZenFS");
  }

  virtual IOStatus AreFilesSame(const std::string& /*first*/,
                                const std::string& /*second*/,
                                const IOOptions& /*options*/, bool* /*res*/,
                                IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("AreFilesSame is not supported in ZenFS");
  }
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)
