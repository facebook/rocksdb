// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/file_system.h"
#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

class ZoneExtent {
 public:
  uint64_t start_;
  uint32_t length_;
  Zone* zone_;

  explicit ZoneExtent(uint64_t start, uint32_t length, Zone* zone);
  Status DecodeFrom(Slice* input);
  void EncodeTo(std::string* output);
};

class ZoneFile {
 protected:
  ZonedBlockDevice* zbd_;
  std::vector<ZoneExtent*> extents_;
  Zone* active_zone_;
  uint64_t extent_start_;
  uint64_t extent_filepos_;

  Env::WriteLifeTimeHint lifetime_;
  bool openforwrite_;
  uint64_t fileSize;
  std::string filename_;
  uint64_t file_id_;

 public:
  /* Constructor for data files */
  explicit ZoneFile(ZonedBlockDevice* zbd, std::string filename,
                    uint64_t file_id_);

  virtual ~ZoneFile();

  IOStatus OpenWR();
  IOStatus CloseWR();
  IOStatus Append(void* data, int data_size, int valid_size);
  IOStatus SetWriteLifeTimeHint(Env::WriteLifeTimeHint lifetime);
  std::string GetFilename();
  void Rename(std::string name);
  uint64_t GetFileSize();
  void SetFileSize(uint64_t sz);

  uint32_t GetBlockSize() { return zbd_->GetBlockSize(); }
  std::vector<ZoneExtent*> GetExtents() { return extents_; }
  Env::WriteLifeTimeHint GetWriteLifeTimeHint() { return lifetime_; }

  IOStatus PositionedRead(uint64_t offset, size_t n, Slice* result,
                          char* scratch);
  ZoneExtent* GetExtent(uint64_t file_offset, uint64_t* dev_offset);
  void PushExtent();

  void EncodeTo(std::string* output);
  Status DecodeFrom(Slice* input);

  uint64_t GetID() { return file_id_; }
};

class ZonedWritableFile : public FSWritableFile {
 public:
  /* Interface for persisting metadata for files */
  class MetadataWriter {
   public:
    virtual ~MetadataWriter();
    virtual IOStatus Persist(ZoneFile* zoneFile) = 0;
  };

  explicit ZonedWritableFile(ZonedBlockDevice* zbd, bool buffered,
                             ZoneFile* zoneFile,
                             MetadataWriter* metadata_writer = nullptr);
  virtual ~ZonedWritableFile();

  virtual IOStatus Append(const Slice& data, const IOOptions& options,
                          IODebugContext* dbg) override;
  virtual IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                                    const IOOptions& options,
                                    IODebugContext* dbg) override;

  virtual IOStatus Truncate(uint64_t size, const IOOptions& options,
                            IODebugContext* dbg) override;

  virtual IOStatus Close(const IOOptions& options,
                         IODebugContext* dbg) override;
  virtual IOStatus Flush(const IOOptions& options,
                         IODebugContext* dbg) override;
  virtual IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;

  virtual IOStatus RangeSync(uint64_t offset, uint64_t nbytes,
                             const IOOptions& options,
                             IODebugContext* dbg) override;

  virtual IOStatus Fsync(const IOOptions& options,
                         IODebugContext* dbg) override;

  void SetPreallocationBlockSize(size_t size) override;
  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& options,
                    IODebugContext* dbg) override;
  void PrepareWrite(size_t offset, size_t len, const IOOptions& options,
                    IODebugContext* dbg) override;

  bool use_direct_io() const override { return !buffered; }
  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }

  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;

 private:
  IOStatus BufferedWrite(const Slice& data);
  IOStatus FlushBuffer();

  bool buffered;
  char* buffer;
  size_t buffer_sz;
  uint32_t block_sz;
  uint32_t buffer_pos;
  uint64_t wp;
  int write_temp;

  ZoneFile* zoneFile_;
  MetadataWriter* metadata_writer_;
};

class ZonedSequentialFile : public FSSequentialFile {
 private:
  ZoneFile* zoneFile_;
  uint64_t rp;

 public:
  explicit ZonedSequentialFile(ZoneFile* zoneFile)
      : zoneFile_(zoneFile), rp(0) {}

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;
  IOStatus Skip(uint64_t n);

  bool use_direct_io() const override { /*return target_->use_direct_io(); */
    return true;
  }

  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }

  IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
    /* TODO: Implement ?*/
    return IOStatus::OK();
  }
};

class ZonedRandomAccessFile : public FSRandomAccessFile {
 private:
  ZoneFile* zoneFile_;

 public:
  explicit ZonedRandomAccessFile(ZoneFile* zoneFile) : zoneFile_(zoneFile) {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus MultiRead(FSReadRequest* /*reqs*/, size_t /*num_reqs*/,
                     const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return IOStatus::IOError("Not implemented");
  }

  IOStatus Prefetch(uint64_t /*offset*/, size_t /*n*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    /* TODO: Implement ?*/
    return IOStatus::OK();
  }

  size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const override {
    return 0; /* TODO: implement - dev inode + file inode should be good enough
               */
  };

  void Hint(AccessPattern /*pattern*/) override { /* TODO: Implement ? */
  }

  bool use_direct_io() const override { return true; }

  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }

  IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
    /* TODO: Implement ?*/
    return IOStatus::OK();
  }
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)
