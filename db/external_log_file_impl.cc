//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/external_log_file_edit.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "rocksdb/external_log_file.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class ExternalLogFileManagerImpl;

namespace {

bool ExternalLogIsAsciiAlpha(char c) {
  return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
}

bool ExternalLogIsAbsolutePath(const std::string& path) {
  if (path.empty()) {
    return false;
  }
  if (path[0] == '/' || path[0] == '\\') {
    return true;
  }
  return path.size() >= 3 && ExternalLogIsAsciiAlpha(path[0]) &&
         path[1] == ':' && (path[2] == '/' || path[2] == '\\');
}

bool EndsWithPathSeparator(const std::string& path) {
  return !path.empty() && (path.back() == '/' || path.back() == '\\');
}

std::string ExternalLogParentPath(const std::string& path) {
  const size_t slash = path.find_last_of("/\\");
  if (slash == std::string::npos) {
    return std::string();
  }
  if (slash == 0) {
    return path.substr(0, 1);
  }
  if (slash == 2 && path.size() >= 3 && ExternalLogIsAsciiAlpha(path[0]) &&
      path[1] == ':') {
    return path.substr(0, 3);
  }
  return path.substr(0, slash);
}

std::string ExternalLogBaseName(const std::string& path) {
  const size_t slash = path.find_last_of("/\\");
  if (slash == std::string::npos) {
    return path;
  }
  return path.substr(slash + 1);
}

Status ValidateExternalLogFilePathComponents(const std::string& name,
                                             bool allow_dot_dot) {
  if (EndsWithPathSeparator(name)) {
    return Status::InvalidArgument("External log file name must name a file");
  }

  size_t component_start = 0;
  if (ExternalLogIsAbsolutePath(name)) {
    if (name[0] == '/' || name[0] == '\\') {
      component_start = 1;
    } else {
      component_start = 3;
    }
  }

  while (component_start <= name.size()) {
    const size_t component_end = name.find_first_of("/\\", component_start);
    const size_t component_size =
        (component_end == std::string::npos ? name.size() : component_end) -
        component_start;
    if (component_size == 0) {
      return Status::InvalidArgument(
          "External log file name must not contain empty path components");
    }
    Slice component(name.data() + component_start, component_size);
    if (component == ".") {
      return Status::InvalidArgument(
          "External log file name must not contain '.' components");
    }
    if (component == "..") {
      if (!allow_dot_dot) {
        return Status::InvalidArgument(
            "External log file name must not contain '..' components");
      }
      if (component_end == std::string::npos) {
        return Status::InvalidArgument(
            "External log file name must not end with a '..' component");
      }
    }
    if (component_end == std::string::npos) {
      break;
    }
    component_start = component_end + 1;
  }
  return Status::OK();
}

Status ValidateExternalLogFileManagedNameCollision(const std::string& name) {
  if (ExternalLogIsAbsolutePath(name)) {
    return Status::OK();
  }

  uint64_t number = 0;
  FileType type;
  const size_t first_slash = name.find('/');
  const std::string first_component = name.substr(
      0, first_slash == std::string::npos ? name.size() : first_slash);
  if (ParseFileName(first_component, &number, &type)) {
    return Status::InvalidArgument(
        "External log file top-level path component collides with a "
        "RocksDB-managed filename");
  }
  if (ParseFileName(name, &number, &type)) {
    return Status::InvalidArgument(
        "External log file name collides with a RocksDB-managed filename");
  }
  return Status::OK();
}

Status ValidateDBRelativeExternalLogFileName(const std::string& name) {
  if (name.empty()) {
    return Status::InvalidArgument("External log file name is empty");
  }
  if (ExternalLogIsAbsolutePath(name)) {
    return Status::InvalidArgument(
        "External log file name must be a DB-relative path");
  }
  if (name.find('\\') != std::string::npos) {
    return Status::InvalidArgument(
        "External log file name must use '/' path separators");
  }
  if (name.find(':') != std::string::npos) {
    return Status::InvalidArgument(
        "External log file name must not contain ':'");
  }

  Status s = ValidateExternalLogFilePathComponents(name, false);
  if (!s.ok()) {
    return s;
  }
  return ValidateExternalLogFileManagedNameCollision(name);
}

Status ValidateExplicitExternalLogFileName(const std::string& name) {
  if (name.empty()) {
    return Status::InvalidArgument("External log file name is empty");
  }

  const bool absolute = ExternalLogIsAbsolutePath(name);
  if (!absolute && name.find('\\') != std::string::npos) {
    return Status::InvalidArgument(
        "External log file relative name must use '/' path separators");
  }
  if (!absolute && name.find(':') != std::string::npos) {
    return Status::InvalidArgument(
        "External log file relative name must not contain ':'");
  }

  Status s = ValidateExternalLogFilePathComponents(name, true);
  if (!s.ok()) {
    return s;
  }
  return ValidateExternalLogFileManagedNameCollision(name);
}

Status ValidateExternalLogFileName(ExternalLogFilePathType path_type,
                                   const std::string& name) {
  switch (path_type) {
    case ExternalLogFilePathType::kDBRelativePath:
      return ValidateDBRelativeExternalLogFileName(name);
    case ExternalLogFilePathType::kExternalPath:
      return ValidateExplicitExternalLogFileName(name);
  }
  return Status::InvalidArgument("Unknown external log file path type");
}

Status ValidateExternalLogFileLookupName(const std::string& name) {
  return ValidateExplicitExternalLogFileName(name);
}

std::string ExternalLogJoinPath(const std::string& dirname,
                                const std::string& relative_path) {
  if (dirname.empty()) {
    return relative_path;
  }
  if (relative_path.empty()) {
    return dirname;
  }
  if (dirname.back() == '/') {
    return dirname + relative_path;
  }
  return dirname + "/" + relative_path;
}

struct ResolvedExternalLogFilePath {
  std::string io_path;
  std::string parent_dir;
  bool db_relative = true;
};

Status ResolveExternalLogFilePath(const std::string& dbname,
                                  ExternalLogFilePathType path_type,
                                  const std::string& name,
                                  ResolvedExternalLogFilePath* resolved) {
  assert(resolved != nullptr);
  *resolved = ResolvedExternalLogFilePath();
  switch (path_type) {
    case ExternalLogFilePathType::kDBRelativePath: {
      Status s = ValidateDBRelativeExternalLogFileName(name);
      if (!s.ok()) {
        return s;
      }
      resolved->io_path = ExternalLogJoinPath(dbname, name);
      const std::string parent = ExternalLogParentPath(name);
      resolved->parent_dir =
          parent.empty() ? dbname : ExternalLogJoinPath(dbname, parent);
      resolved->db_relative = true;
      return Status::OK();
    }
    case ExternalLogFilePathType::kExternalPath: {
      Status s = ValidateExplicitExternalLogFileName(name);
      if (!s.ok()) {
        return s;
      }
      resolved->io_path = ExternalLogIsAbsolutePath(name)
                              ? name
                              : ExternalLogJoinPath(dbname, name);
      const std::string parent = ExternalLogParentPath(resolved->io_path);
      if (parent.empty()) {
        return Status::InvalidArgument(
            "External log file name has no parent directory");
      }
      resolved->parent_dir = parent;
      resolved->db_relative = false;
      return Status::OK();
    }
  }
  return Status::InvalidArgument("Unknown external log file path type");
}

std::string ExternalLogFilePath(const std::string& dbname,
                                const ExternalLogFileMetadata& metadata) {
  if (metadata.GetPathType() == ExternalLogFilePathType::kExternalPath &&
      ExternalLogIsAbsolutePath(metadata.GetName())) {
    return metadata.GetName();
  }
  return ExternalLogJoinPath(dbname, metadata.GetName());
}

std::string ExternalLogFileParentPath(const std::string& dbname,
                                      const ExternalLogFileMetadata& metadata) {
  const std::string path = ExternalLogFilePath(dbname, metadata);
  std::string parent = ExternalLogParentPath(path);
  if (!parent.empty()) {
    return parent;
  }
  return dbname;
}

Status SyncDirectory(FileSystem* fs, const std::string& dirname,
                     const DirFsyncOptions& dir_fsync_options) {
  assert(fs != nullptr);
  std::unique_ptr<FSDirectory> dir;
  Status s = fs->NewDirectory(dirname, IOOptions(), &dir, nullptr);
  if (!s.ok()) {
    return s;
  }
  return dir->FsyncWithDirOptions(IOOptions(), nullptr, dir_fsync_options);
}

Status CreateExternalLogFileParentDirs(FileSystem* fs,
                                       const std::string& dbname,
                                       const std::string& name) {
  assert(fs != nullptr);
  std::string current = dbname;
  size_t component_start = 0;
  while (true) {
    const size_t slash = name.find('/', component_start);
    if (slash == std::string::npos) {
      return Status::OK();
    }
    const std::string parent = current;
    current = ExternalLogJoinPath(
        current, name.substr(component_start, slash - component_start));
    Status s = fs->CreateDirIfMissing(current, IOOptions(), nullptr);
    if (!s.ok()) {
      return s;
    }
    s = SyncDirectory(
        fs, parent,
        DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
    if (!s.ok()) {
      return s;
    }
    component_start = slash + 1;
  }
}

Status CheckExternalLogFileDoesNotExist(FileSystem* fs,
                                        const std::string& path) {
  assert(fs != nullptr);
  Status s = fs->FileExists(path, IOOptions(), nullptr);
  if (s.ok()) {
    return Status::InvalidArgument(
        "External log file physical path already exists", path);
  }
  if (s.IsNotFound()) {
    return Status::OK();
  }
  return s;
}

Status ValidateExternalLogFileBackupSupport(bool include_in_backup) {
  if (include_in_backup) {
    return Status::NotSupported(
        "Including external log files in BackupEngine backups is not "
        "supported");
  }
  return Status::OK();
}

std::string NormalizeRequestedChecksumFuncName(
    const std::string& requested_checksum_func_name) {
  if (requested_checksum_func_name == kUnknownFileChecksumFuncName) {
    return std::string();
  }
  return requested_checksum_func_name;
}

FileOptions ExternalLogFileOptionsForIO(const FileOptions& base_options,
                                        Temperature temperature) {
  FileOptions file_options = base_options;
  file_options.temperature = temperature;
  return file_options;
}

FileOptions ExternalLogFileOptionsForRead(const FileOptions& base_options,
                                          Temperature temperature) {
  FileOptions file_options =
      ExternalLogFileOptionsForIO(base_options, temperature);
  // The public read API accepts arbitrary offsets and caller scratch buffers,
  // so do not inherit direct-I/O alignment requirements.
  file_options.use_direct_reads = false;
  // External log checksum metadata describes the committed logical prefix, not
  // necessarily the whole physical file opened below.
  file_options.file_checksum = kUnknownFileChecksum;
  file_options.file_checksum_func_name = kNoFileChecksumFuncName;
  return file_options;
}

Status PrepareReadIO(const ReadOptions& read_options, SystemClock* clock,
                     IOOptions* io_options, IODebugContext* dbg) {
  assert(io_options != nullptr);
  return PrepareIOFromReadOptions(read_options, clock, *io_options, dbg);
}

Status PrepareWriteIO(const WriteOptions& write_options,
                      IOOptions* io_options) {
  assert(io_options != nullptr);
  return PrepareIOFromWriteOptions(write_options, *io_options);
}

Status SyncWritableFile(FSWritableFile* file, bool use_fsync) {
  assert(file != nullptr);
  IOOptions io_options;
  Status s = file->Flush(io_options, nullptr);
  if (!s.ok()) {
    return s;
  }
  if (use_fsync) {
    return file->Fsync(io_options, nullptr);
  }
  return file->Sync(io_options, nullptr);
}

Status ComputeExternalLogFileChecksumPrefix(
    FileSystem* fs, const std::string& path,
    FileChecksumGenFactory* checksum_factory,
    const std::string& requested_checksum_func_name, uint64_t size,
    const ReadOptions& read_options, const FileOptions& file_options,
    SystemClock* clock, std::string* checksum, std::string* checksum_func_name,
    std::unique_ptr<FileChecksumGenerator>* rolling_generator = nullptr) {
  assert(fs != nullptr);
  assert(checksum != nullptr);
  assert(checksum_func_name != nullptr);

  if (checksum_factory == nullptr) {
    return Status::InvalidArgument(
        "External log file checksum computation requires "
        "options.file_checksum_gen_factory");
  }

  FileChecksumGenContext gen_context;
  const std::string requested_func_name =
      NormalizeRequestedChecksumFuncName(requested_checksum_func_name);
  gen_context.file_name = path;
  gen_context.requested_checksum_func_name = requested_func_name;
  std::unique_ptr<FileChecksumGenerator> checksum_generator =
      checksum_factory->CreateFileChecksumGenerator(gen_context);
  if (checksum_generator == nullptr) {
    return Status::InvalidArgument(
        "Cannot create file checksum generator for external log file");
  }
  if (!requested_func_name.empty() &&
      requested_func_name != checksum_generator->Name()) {
    return Status::InvalidArgument(
        "External log file checksum function does not match checksum factory");
  }

  std::unique_ptr<FileChecksumGenerator> unfinalized_generator;
  if (rolling_generator != nullptr) {
    FileChecksumGenContext rolling_gen_context;
    rolling_gen_context.file_name = path;
    rolling_gen_context.requested_checksum_func_name =
        checksum_generator->Name();
    unfinalized_generator =
        checksum_factory->CreateFileChecksumGenerator(rolling_gen_context);
    if (unfinalized_generator == nullptr ||
        rolling_gen_context.requested_checksum_func_name !=
            unfinalized_generator->Name()) {
      return Status::InvalidArgument(
          "Cannot create matching rolling checksum generator for external log "
          "file");
    }
  }

  FileOptions read_file_options = file_options;
  read_file_options.use_direct_reads = false;
  read_file_options.file_checksum_func_name = kNoFileChecksumFuncName;

  std::unique_ptr<FSSequentialFile> file;
  Status s = fs->NewSequentialFile(path, read_file_options, &file, nullptr);
  if (!s.ok()) {
    return s;
  }

  constexpr size_t kBufferSize = 256 * 1024;
  std::unique_ptr<char[]> scratch(new char[kBufferSize]);
  IOOptions io_options;
  IODebugContext dbg;
  s = PrepareReadIO(read_options, clock, &io_options, &dbg);
  if (!s.ok()) {
    return s;
  }

  uint64_t remaining = size;
  while (remaining > 0) {
    const size_t bytes_to_read =
        static_cast<size_t>(std::min<uint64_t>(remaining, kBufferSize));
    Slice result;
    s = file->Read(bytes_to_read, io_options, &result, scratch.get(), &dbg);
    if (!s.ok()) {
      return s;
    }
    if (result.empty()) {
      return Status::Corruption(
          "External log file is smaller than MANIFEST metadata");
    }
    checksum_generator->Update(result.data(), result.size());
    if (unfinalized_generator != nullptr) {
      unfinalized_generator->Update(result.data(), result.size());
    }
    remaining -= result.size();
  }

  checksum_generator->Finalize();
  *checksum = checksum_generator->GetChecksum();
  *checksum_func_name = checksum_generator->Name();
  if (rolling_generator != nullptr) {
    *rolling_generator = std::move(unfinalized_generator);
  }
  return Status::OK();
}

struct ExternalLogFileHandleState {
  explicit ExternalLogFileHandleState(uint64_t visible_size)
      : visible_size_(visible_size) {}

  mutable std::mutex mutex_;
  uint64_t visible_size_ = 0;
  bool closed_ = false;
  bool sealed_ = false;
};

ExternalLogFileInfo BuildExternalLogFileInfo(
    const std::string& dbname, ExternalLogFileNumber file_number,
    const ExternalLogFileMetadata& metadata) {
  ExternalLogFileInfo info;
  if (metadata.GetPathType() == ExternalLogFilePathType::kExternalPath) {
    const std::string path = ExternalLogFilePath(dbname, metadata);
    info.relative_filename = ExternalLogBaseName(path);
    info.directory = ExternalLogParentPath(path);
  } else {
    info.relative_filename = metadata.GetName();
    info.directory = dbname;
  }
  info.file_number = file_number;
  info.file_type = kExternalLogFile;
  info.size = metadata.GetDurableSize();
  info.temperature = metadata.GetTemperature();
  info.file_checksum = metadata.GetFileChecksum();
  info.file_checksum_func_name = metadata.GetFileChecksumFuncName();
  info.name = metadata.GetName();
  info.path_type = metadata.GetPathType();
  info.state = metadata.GetState();
  info.durable_size = metadata.GetDurableSize();
  return info;
}

class ExternalLogFileReaderImpl : public ExternalLogFileReader {
 public:
  ExternalLogFileReaderImpl(
      ExternalLogFileManagerImpl* manager, std::string name,
      ExternalLogFileNumber file_number, ExternalLogFileInfo info,
      std::unique_ptr<FSRandomAccessFile>&& file,
      ExternalLogFileReaderVisibility visibility,
      std::shared_ptr<ExternalLogFileHandleState> handle_state)
      : manager_(manager),
        name_(std::move(name)),
        file_number_(file_number),
        info_(std::move(info)),
        file_(std::move(file)),
        visibility_(visibility),
        handle_state_(std::move(handle_state)),
        snapshot_size_(info_.durable_size) {
    if (handle_state_) {
      std::lock_guard<std::mutex> lock(handle_state_->mutex_);
      snapshot_size_ = handle_state_->visible_size_;
    }
  }

  ~ExternalLogFileReaderImpl() override;

  ExternalLogFileReaderImpl(const ExternalLogFileReaderImpl&) = delete;
  ExternalLogFileReaderImpl& operator=(const ExternalLogFileReaderImpl&) =
      delete;
  ExternalLogFileReaderImpl(ExternalLogFileReaderImpl&&) = delete;
  ExternalLogFileReaderImpl& operator=(ExternalLogFileReaderImpl&&) = delete;

  const std::string& Name() const override { return name_; }

  Status Read(const ReadOptions& options, uint64_t offset, size_t n,
              Slice* result, char* scratch) override {
    assert(result != nullptr);
    assert(scratch != nullptr);
    const uint64_t visible_size = VisibleSize();
    if (offset >= visible_size || n == 0) {
      *result = Slice();
      return Status::OK();
    }

    const size_t bytes_to_read =
        static_cast<size_t>(std::min<uint64_t>(n, visible_size - offset));
    IOOptions io_options;
    IODebugContext dbg;
    Status s = PrepareIOFromReadOptions(options, SystemClock::Default().get(),
                                        io_options, &dbg);
    if (!s.ok()) {
      return s;
    }
    return file_->Read(offset, bytes_to_read, io_options, result, scratch,
                       &dbg);
  }

  uint64_t VisibleSize() const override {
    if (visibility_ == ExternalLogFileReaderVisibility::kFollowWriter &&
        handle_state_) {
      std::lock_guard<std::mutex> lock(handle_state_->mutex_);
      return handle_state_->visible_size_;
    }
    return snapshot_size_;
  }

  Status GetFileInfo(ExternalLogFileInfo* info) const override {
    if (info == nullptr) {
      return Status::InvalidArgument("ExternalLogFileInfo is null");
    }
    *info = info_;
    info->physical_size = VisibleSize();
    return Status::OK();
  }

 private:
  ExternalLogFileManagerImpl* manager_;
  std::string name_;
  ExternalLogFileNumber file_number_;
  ExternalLogFileInfo info_;
  std::unique_ptr<FSRandomAccessFile> file_;
  ExternalLogFileReaderVisibility visibility_;
  std::shared_ptr<ExternalLogFileHandleState> handle_state_;
  uint64_t snapshot_size_;
};

class ExternalLogFileWriterImpl : public ExternalLogFileWriter {
 public:
  ExternalLogFileWriterImpl(
      ExternalLogFileManagerImpl* manager, ExternalLogFileNumber file_number,
      ExternalLogFileMetadata metadata, std::string path,
      std::unique_ptr<FSWritableFile>&& file,
      std::shared_ptr<ExternalLogFileHandleState> handle_state,
      std::unique_ptr<FileChecksumGenerator>&& checksum_generator,
      bool active_writer_registered);

  ~ExternalLogFileWriterImpl() override;

  ExternalLogFileWriterImpl(const ExternalLogFileWriterImpl&) = delete;
  ExternalLogFileWriterImpl& operator=(const ExternalLogFileWriterImpl&) =
      delete;
  ExternalLogFileWriterImpl(ExternalLogFileWriterImpl&&) = delete;
  ExternalLogFileWriterImpl& operator=(ExternalLogFileWriterImpl&&) = delete;

  const std::string& Name() const override { return name_; }

  Status Append(const WriteOptions& options, const Slice& data,
                uint64_t* offset) override;

  Status Flush(const WriteOptions& options) override;

  Status Sync(const WriteOptions& options,
              ExternalLogFileInfo* info = nullptr) override;

  Status Seal(const WriteOptions& options,
              const SealExternalLogFileOptions& seal_options,
              ExternalLogFileInfo* info = nullptr) override;

  Status Close(const WriteOptions& options) override;

  Status NewReader(const ExternalLogFileReaderOptions& options,
                   std::unique_ptr<ExternalLogFileReader>* reader) override;

  Status GetFileInfo(ExternalLogFileInfo* info) const override;

 private:
  uint64_t VisibleSizeLocked() const;
  Status FlushLocked(const WriteOptions& options);
  Status SyncWritableFileLocked(const WriteOptions& options);
  Status PersistMetadataLocked(const WriteOptions& options,
                               ExternalLogFileState state,
                               uint64_t durable_size, bool recompute_checksum,
                               ExternalLogFileInfo* info);
  Status ChecksumForMetadata(uint64_t durable_size, uint64_t visible_size,
                             bool recompute_checksum, std::string* checksum,
                             std::string* checksum_func_name);
  void UnregisterActiveWriter();

  ExternalLogFileManagerImpl* manager_;
  ExternalLogFileNumber file_number_;
  std::string name_;
  ExternalLogFileMetadata metadata_;
  std::string path_;
  std::unique_ptr<FSWritableFile> file_;
  std::shared_ptr<ExternalLogFileHandleState> handle_state_;
  std::unique_ptr<FileChecksumGenerator> checksum_generator_;
  bool checksum_generator_finalized_ = false;
  bool active_writer_registered_ = false;
};

}  // namespace

class ExternalLogFileManagerImpl : public ExternalLogFileManager {
 public:
  explicit ExternalLogFileManagerImpl(DBImpl* db) : db_(db) {
    assert(db_ != nullptr);
  }

  Status ListExternalLogFiles(
      const ListExternalLogFilesOptions& options,
      std::vector<ExternalLogFileInfo>* files) override {
    if (files == nullptr) {
      return Status::InvalidArgument("External log file output vector is null");
    }

    std::vector<ExternalLogFileInfo> result;
    std::vector<ExternalLogFileNumber> active_refs;
    {
      InstrumentedMutexLock lock(&db_->mutex_);
      for (const auto& file :
           db_->versions_->GetExternalLogFileSet().GetFiles()) {
        const ExternalLogFileMetadata& metadata = file.second;
        if (metadata.GetState() == ExternalLogFileState::kUnsealed &&
            !options.include_unsealed) {
          continue;
        }
        if (metadata.GetState() == ExternalLogFileState::kSealed &&
            !options.include_sealed) {
          continue;
        }
        result.push_back(
            BuildExternalLogFileInfo(db_->dbname_, file.first, metadata));
        if (options.get_physical_size) {
          RefActiveFileLocked(file.first);
          active_refs.push_back(file.first);
        }
      }
    }

    if (options.get_physical_size) {
      TEST_SYNC_POINT(
          "ExternalLogFileManagerImpl::ListExternalLogFiles:"
          "AfterRegisterReaders");
      IOOptions io_options;
      for (auto& info : result) {
        Status s = db_->fs_->GetFileSize(
            ExternalLogJoinPath(info.directory, info.relative_filename),
            io_options, &info.physical_size, nullptr);
        if (!s.ok()) {
          for (const auto file_number : active_refs) {
            UnregisterActiveReader(file_number);
          }
          return s;
        }
      }
    }

    for (const auto file_number : active_refs) {
      UnregisterActiveReader(file_number);
    }
    *files = std::move(result);
    return Status::OK();
  }

  Status CreateExternalLogFile(
      const ExternalLogFileOptions& options,
      std::unique_ptr<ExternalLogFileWriter>* writer) override {
    if (writer == nullptr) {
      return Status::InvalidArgument("External log file writer is null");
    }
    writer->reset();
    Status s = CheckDBMutable();
    if (!s.ok()) {
      return s;
    }
    s = ValidateExternalLogFileBackupSupport(options.include_in_backup);
    if (!s.ok()) {
      return s;
    }

    ResolvedExternalLogFilePath resolved_path;
    s = ResolveExternalLogFilePath(db_->dbname_, options.path_type,
                                   options.name, &resolved_path);
    if (!s.ok()) {
      return s;
    }
    if (resolved_path.db_relative) {
      s = CreateExternalLogFileParentDirs(db_->fs_.get(), db_->dbname_,
                                          options.name);
      if (!s.ok()) {
        return s;
      }
    }
    const std::string path = resolved_path.io_path;
    s = CheckExternalLogFileDoesNotExist(db_->fs_.get(), path);
    if (!s.ok()) {
      return s;
    }

    ExternalLogFileNumber file_number = 0;
    std::unique_ptr<std::list<uint64_t>::iterator> pending_outputs_elem;
    {
      InstrumentedMutexLock lock(&db_->mutex_);
      if (FileExistsLocked(options.name)) {
        return Status::InvalidArgument("External log file already exists",
                                       options.name);
      }
      if (FilePathExistsLocked(resolved_path.io_path)) {
        return Status::InvalidArgument(
            "External log file physical path already exists",
            resolved_path.io_path);
      }
      pending_outputs_elem.reset(new std::list<uint64_t>::iterator(
          db_->CaptureCurrentFileNumberInPendingOutputs()));
      file_number = db_->versions_->FetchAddFileNumber(1);
      VersionEdit reserve_edit;
      // Persist next_file_number_ so this reserved number is not reused after
      // a crash before the external log file addition is logged.
      s = LogAndApplyLocked(WriteOptions(), &reserve_edit);
      if (!s.ok()) {
        db_->ReleaseFileNumberFromPendingOutputs(pending_outputs_elem);
        return s;
      }
    }

    std::unique_ptr<FileChecksumGenerator> checksum_generator;
    s = CreateRollingChecksumGenerator(path, kUnknownFileChecksumFuncName,
                                       &checksum_generator);
    if (!s.ok()) {
      ReleasePendingOutput(&pending_outputs_elem);
      return s;
    }

    FileOptions file_options =
        ExternalLogFileOptionsForIO(db_->file_options_, options.temperature);
    std::unique_ptr<FSWritableFile> file;
    s = db_->fs_->NewWritableFile(path, file_options, &file, nullptr);
    if (!s.ok()) {
      ReleasePendingOutput(&pending_outputs_elem);
      return s;
    }
    s = SyncWritableFile(file.get(), db_->immutable_db_options_.use_fsync);
    if (!s.ok()) {
      ReleasePendingOutput(&pending_outputs_elem);
      file->Close(IOOptions(), nullptr).PermitUncheckedError();
      db_->fs_->DeleteFile(path, IOOptions(), nullptr).PermitUncheckedError();
      return s;
    }
    s = SyncExternalLogDirectory(resolved_path.parent_dir);
    if (!s.ok()) {
      ReleasePendingOutput(&pending_outputs_elem);
      file->Close(IOOptions(), nullptr).PermitUncheckedError();
      db_->fs_->DeleteFile(path, IOOptions(), nullptr).PermitUncheckedError();
      return s;
    }

    ExternalLogFileMetadata metadata(
        options.name, options.path_type, ExternalLogFileState::kUnsealed, 0,
        kUnknownFileChecksum, kUnknownFileChecksumFuncName,
        options.temperature);
    std::shared_ptr<ExternalLogFileHandleState> handle_state =
        std::make_shared<ExternalLogFileHandleState>(0);
    s = RegisterActiveWriter(file_number, handle_state);
    if (!s.ok()) {
      ReleasePendingOutput(&pending_outputs_elem);
      file->Close(IOOptions(), nullptr).PermitUncheckedError();
      db_->fs_->DeleteFile(path, IOOptions(), nullptr).PermitUncheckedError();
      return s;
    }
    bool unregister_active_writer = true;
    auto cleanup_active_writer = [&]() {
      if (unregister_active_writer) {
        UnregisterActiveWriter(file_number);
        unregister_active_writer = false;
      }
    };

    VersionEdit edit;
    edit.AddExternalLogFile(file_number, metadata);
    {
      InstrumentedMutexLock lock(&db_->mutex_);
      if (FileExistsLocked(options.name)) {
        s = Status::InvalidArgument("External log file already exists",
                                    options.name);
      } else if (FilePathExistsLocked(resolved_path.io_path)) {
        s = Status::InvalidArgument(
            "External log file physical path already exists",
            resolved_path.io_path);
      } else {
        s = LogAndApplyLocked(WriteOptions(), &edit);
      }
      db_->ReleaseFileNumberFromPendingOutputs(pending_outputs_elem);
    }

    if (!s.ok()) {
      cleanup_active_writer();
      file->Close(IOOptions(), nullptr).PermitUncheckedError();
      db_->fs_->DeleteFile(path, IOOptions(), nullptr).PermitUncheckedError();
      return s;
    }

    TEST_SYNC_POINT(
        "ExternalLogFileManagerImpl::CreateExternalLogFile:"
        "AfterManifest");

    std::unique_ptr<ExternalLogFileWriterImpl> impl(
        new ExternalLogFileWriterImpl(
            this, file_number, std::move(metadata), path, std::move(file),
            std::move(handle_state), std::move(checksum_generator),
            true /* active_writer_registered */));
    writer->reset(impl.release());
    unregister_active_writer = false;
    return Status::OK();
  }

  Status IngestExternalLogFile(const IngestExternalLogFileOptions& options,
                               ExternalLogFileInfo* info) override {
    std::vector<IngestExternalLogFileOptions> options_vec;
    options_vec.push_back(options);
    std::vector<ExternalLogFileInfo> infos;
    Status s =
        IngestExternalLogFiles(options_vec, info != nullptr ? &infos : nullptr);
    if (s.ok() && info != nullptr) {
      assert(infos.size() == 1);
      *info = infos[0];
    }
    return s;
  }

  Status IngestExternalLogFiles(
      const std::vector<IngestExternalLogFileOptions>& options,
      std::vector<ExternalLogFileInfo>* infos) override {
    if (options.empty()) {
      if (infos != nullptr) {
        infos->clear();
      }
      return Status::OK();
    }

    Status s = CheckDBMutable();
    if (!s.ok()) {
      return s;
    }

    std::vector<std::string> names;
    names.reserve(options.size());
    std::vector<ResolvedExternalLogFilePath> resolved_paths;
    resolved_paths.reserve(options.size());
    for (const auto& option : options) {
      s = ValidateIngestOptions(option);
      if (!s.ok()) {
        return s;
      }
      ResolvedExternalLogFilePath resolved_path;
      s = ResolveExternalLogFilePath(db_->dbname_, option.path_type,
                                     option.name, &resolved_path);
      if (!s.ok()) {
        return s;
      }
      if (std::find(names.begin(), names.end(), option.name) != names.end()) {
        return Status::InvalidArgument(
            "Duplicate external log file name in ingest request");
      }
      bool duplicate_physical_path = false;
      for (const auto& existing : resolved_paths) {
        if (existing.io_path == resolved_path.io_path) {
          duplicate_physical_path = true;
          break;
        }
      }
      if (duplicate_physical_path) {
        return Status::InvalidArgument(
            "Duplicate external log file physical path in ingest request");
      }
      names.push_back(option.name);
      resolved_paths.push_back(std::move(resolved_path));
    }

    for (size_t i = 0; i < options.size(); ++i) {
      if (resolved_paths[i].db_relative) {
        s = CreateExternalLogFileParentDirs(db_->fs_.get(), db_->dbname_,
                                            options[i].name);
        if (!s.ok()) {
          return s;
        }
      }
      if (options[i].source_path != resolved_paths[i].io_path) {
        s = CheckExternalLogFileDoesNotExist(db_->fs_.get(),
                                             resolved_paths[i].io_path);
        if (!s.ok()) {
          return s;
        }
      }
    }

    std::vector<ExternalLogFileNumber> file_numbers;
    file_numbers.reserve(options.size());
    std::unique_ptr<std::list<uint64_t>::iterator> pending_outputs_elem;
    {
      InstrumentedMutexLock lock(&db_->mutex_);
      for (const auto& option : options) {
        if (FileExistsLocked(option.name)) {
          return Status::InvalidArgument("External log file already exists",
                                         option.name);
        }
      }
      for (const auto& resolved_path : resolved_paths) {
        if (FilePathExistsLocked(resolved_path.io_path)) {
          return Status::InvalidArgument(
              "External log file physical path already exists",
              resolved_path.io_path);
        }
      }
      pending_outputs_elem.reset(new std::list<uint64_t>::iterator(
          db_->CaptureCurrentFileNumberInPendingOutputs()));
      const ExternalLogFileNumber first_file_number =
          db_->versions_->FetchAddFileNumber(
              static_cast<uint64_t>(options.size()));
      for (size_t i = 0; i < options.size(); ++i) {
        file_numbers.push_back(first_file_number + i);
      }
      VersionEdit reserve_edit;
      // Persist next_file_number_ so these reserved numbers are not reused
      // after a crash before the external log file additions are logged.
      s = LogAndApplyLocked(WriteOptions(), &reserve_edit);
      if (!s.ok()) {
        db_->ReleaseFileNumberFromPendingOutputs(pending_outputs_elem);
        return s;
      }
    }

    struct TransferState {
      std::string source;
      std::string destination;
      bool moved = false;
      bool created_destination = false;
    };
    std::vector<TransferState> transfers(options.size());

    for (size_t i = 0; s.ok() && i < options.size(); ++i) {
      transfers[i].source = options[i].source_path;
      transfers[i].destination = resolved_paths[i].io_path;
      s = TransferIngestedFile(options[i], transfers[i].destination,
                               &transfers[i].moved);
      transfers[i].created_destination =
          s.ok() && transfers[i].source != transfers[i].destination;
    }

    if (s.ok()) {
      for (const auto& resolved_path : resolved_paths) {
        s = SyncExternalLogDirectory(resolved_path.parent_dir);
        if (!s.ok()) {
          break;
        }
      }
    }
    if (s.ok()) {
      TEST_SYNC_POINT(
          "ExternalLogFileManagerImpl::IngestExternalLogFiles:AfterTransfer");
    }

    VersionEdit edit;
    std::vector<ExternalLogFileInfo> result_infos;
    if (s.ok()) {
      result_infos.reserve(options.size());
      for (size_t i = 0; i < options.size(); ++i) {
        std::string checksum = options[i].file_checksum;
        std::string checksum_func_name = options[i].file_checksum_func_name;
        if (options[i].verify_file_checksum) {
          std::string actual_checksum;
          std::string actual_checksum_func_name;
          s = ComputeChecksum(transfers[i].destination, options[i].logical_size,
                              options[i].file_checksum_func_name, ReadOptions(),
                              &actual_checksum, &actual_checksum_func_name);
          if (!s.ok()) {
            break;
          }
          if (actual_checksum != options[i].file_checksum ||
              actual_checksum_func_name != options[i].file_checksum_func_name) {
            s = Status::Corruption(
                "External log file checksum mismatch during ingest");
            break;
          }
        } else if (checksum_func_name.empty()) {
          checksum = kUnknownFileChecksum;
          checksum_func_name = kUnknownFileChecksumFuncName;
        }

        ExternalLogFileMetadata metadata(
            options[i].name, options[i].path_type,
            ExternalLogFileState::kSealed, options[i].logical_size, checksum,
            checksum_func_name, options[i].temperature);
        edit.AddExternalLogFile(file_numbers[i], metadata);
        result_infos.push_back(
            BuildExternalLogFileInfo(db_->dbname_, file_numbers[i], metadata));
      }
    }

    if (s.ok()) {
      InstrumentedMutexLock lock(&db_->mutex_);
      for (const auto& option : options) {
        if (FileExistsLocked(option.name)) {
          s = Status::InvalidArgument("External log file already exists",
                                      option.name);
          break;
        }
      }
      if (s.ok()) {
        for (const auto& resolved_path : resolved_paths) {
          if (FilePathExistsLocked(resolved_path.io_path)) {
            s = Status::InvalidArgument(
                "External log file physical path already exists",
                resolved_path.io_path);
            break;
          }
        }
      }
      if (s.ok()) {
        s = LogAndApplyLocked(WriteOptions(), &edit);
      }
      if (s.ok()) {
        db_->ReleaseFileNumberFromPendingOutputs(pending_outputs_elem);
      }
    }

    if (!s.ok()) {
      Status rollback_s;
      for (const auto& transfer : transfers) {
        if (!transfer.created_destination) {
          continue;
        }
        Status transfer_s;
        if (transfer.moved) {
          transfer_s = db_->fs_->RenameFile(
              transfer.destination, transfer.source, IOOptions(), nullptr);
        } else {
          transfer_s =
              db_->fs_->DeleteFile(transfer.destination, IOOptions(), nullptr);
        }
        rollback_s.UpdateIfOk(transfer_s);
      }
      ReleasePendingOutput(&pending_outputs_elem);
      if (!rollback_s.ok()) {
        return Status::CopyAppendMessage(
            s, "; external log ingest rollback error: ", rollback_s.ToString());
      }
      return s;
    }

    if (infos != nullptr) {
      *infos = std::move(result_infos);
    }
    return Status::OK();
  }

  Status ReopenExternalLogFile(
      const std::string& name, const ReopenExternalLogFileOptions& options,
      std::unique_ptr<ExternalLogFileWriter>* writer) override {
    if (writer == nullptr) {
      return Status::InvalidArgument("External log file writer is null");
    }
    writer->reset();

    Status s = CheckDBMutable();
    if (!s.ok()) {
      return s;
    }

    ExternalLogFileNumber file_number = 0;
    ExternalLogFileMetadata metadata;
    std::shared_ptr<ExternalLogFileHandleState> handle_state;
    s = GetFileMetadataAndRegisterActiveWriter(name, &file_number, &metadata,
                                               &handle_state);
    if (!s.ok()) {
      return s;
    }
    bool unregister_active_writer = true;
    auto cleanup_active_writer = [&]() {
      if (unregister_active_writer) {
        UnregisterActiveWriter(file_number);
        unregister_active_writer = false;
      }
    };
    TEST_SYNC_POINT(
        "ExternalLogFileManagerImpl::ReopenExternalLogFile:"
        "AfterRegisterActiveWriter");

    const std::string path = ExternalLogFilePath(db_->dbname_, metadata);
    uint64_t physical_size = 0;
    s = db_->fs_->GetFileSize(path, IOOptions(), &physical_size, nullptr);
    if (!s.ok()) {
      cleanup_active_writer();
      return s;
    }
    const uint64_t visible_size = options.has_recovered_size
                                      ? options.recovered_size
                                      : metadata.GetDurableSize();
    if (visible_size < metadata.GetDurableSize()) {
      cleanup_active_writer();
      return Status::InvalidArgument(
          "External log file recovery size is smaller than MANIFEST metadata");
    }
    if (visible_size > physical_size) {
      cleanup_active_writer();
      return Status::Corruption(
          "External log file recovery size exceeds physical file size");
    }
    // Serialize truncation and visible-size publication with readers taking
    // their snapshot from the active writer state. Readers that register after
    // this point cannot return to the caller until visible_size_ is updated.
    std::lock_guard<std::mutex> handle_lock(handle_state->mutex_);
    if (visible_size < physical_size && HasActiveReaders(file_number)) {
      cleanup_active_writer();
      return Status::Busy(
          "External log file has active readers that could observe the "
          "truncated tail",
          name);
    }

    FileOptions file_options = ExternalLogFileOptionsForIO(
        db_->file_options_, metadata.GetTemperature());
    std::unique_ptr<FSWritableFile> file;
    s = db_->fs_->ReopenWritableFile(path, file_options, &file, nullptr);
    if (!s.ok()) {
      cleanup_active_writer();
      return s;
    }

    if (visible_size < physical_size) {
      s = file->Truncate(visible_size, IOOptions(), nullptr);
      if (!s.ok()) {
        cleanup_active_writer();
        return s;
      }
    }
    std::unique_ptr<FileChecksumGenerator> checksum_generator;
    if (options.recompute_checksum) {
      if (!has_checksum_factory()) {
        cleanup_active_writer();
        return Status::InvalidArgument(
            "External log file checksum recomputation requires "
            "options.file_checksum_gen_factory");
      }
      std::string checksum;
      std::string checksum_func_name;
      s = ComputeChecksum(path, visible_size,
                          metadata.GetFileChecksumFuncName(), ReadOptions(),
                          &checksum, &checksum_func_name, &checksum_generator);
      if (!s.ok()) {
        cleanup_active_writer();
        return s;
      }
    } else if (visible_size == 0) {
      s = CreateRollingChecksumGenerator(
          path, metadata.GetFileChecksumFuncName(), &checksum_generator);
      if (!s.ok()) {
        cleanup_active_writer();
        return s;
      }
    }

    handle_state->visible_size_ = visible_size;

    std::unique_ptr<ExternalLogFileWriterImpl> impl(
        new ExternalLogFileWriterImpl(
            this, file_number, std::move(metadata), path, std::move(file),
            std::move(handle_state), std::move(checksum_generator),
            true /* active_writer_registered */));
    unregister_active_writer = false;
    writer->reset(impl.release());
    return Status::OK();
  }

  Status OpenExternalLogFileForRead(
      const ReadOptions& read_options, const std::string& name,
      const ExternalLogFileReaderOptions& reader_options,
      std::unique_ptr<ExternalLogFileReader>* reader) override {
    if (reader == nullptr) {
      return Status::InvalidArgument("External log file reader is null");
    }
    reader->reset();

    ExternalLogFileNumber file_number = 0;
    ExternalLogFileMetadata metadata;
    std::shared_ptr<ExternalLogFileHandleState> handle_state;
    Status s = GetFileMetadataAndRegisterActiveReader(name, &file_number,
                                                      &metadata, &handle_state);
    if (!s.ok()) {
      return s;
    }

    const std::string path = ExternalLogFilePath(db_->dbname_, metadata);
    FileOptions file_options = ExternalLogFileOptionsForRead(
        db_->file_options_, metadata.GetTemperature());
    IODebugContext dbg;
    s = PrepareReadIO(read_options, db_->immutable_db_options_.clock,
                      &file_options.io_options, &dbg);
    if (!s.ok()) {
      UnregisterActiveReader(file_number);
      return s;
    }
    TEST_SYNC_POINT(
        "ExternalLogFileManagerImpl::OpenExternalLogFileForRead:"
        "AfterRegisterReader");
    std::unique_ptr<FSRandomAccessFile> file;
    s = db_->fs_->NewRandomAccessFile(path, file_options, &file, &dbg);
    if (!s.ok()) {
      UnregisterActiveReader(file_number);
      return s;
    }

    reader->reset(new ExternalLogFileReaderImpl(
        this, name, file_number,
        BuildExternalLogFileInfo(db_->dbname_, file_number, metadata),
        std::move(file), reader_options.visibility, std::move(handle_state)));
    return Status::OK();
  }

  Status VerifyExternalLogFileChecksum(
      const ReadOptions& read_options, const std::string& name,
      const VerifyExternalLogFileChecksumOptions& verify_options,
      ExternalLogFileChecksumInfo* info) override {
    ExternalLogFileNumber file_number = 0;
    ExternalLogFileMetadata metadata;
    std::shared_ptr<ExternalLogFileHandleState> handle_state;
    Status s = GetFileMetadataAndRegisterActiveReader(name, &file_number,
                                                      &metadata, &handle_state);
    if (!s.ok()) {
      return s;
    }
    TEST_SYNC_POINT(
        "ExternalLogFileManagerImpl::VerifyExternalLogFileChecksum:"
        "AfterRegisterReader");
    if (metadata.GetState() == ExternalLogFileState::kUnsealed &&
        !verify_options.include_unsealed) {
      if (info != nullptr) {
        *info = ExternalLogFileChecksumInfo();
        info->file_info =
            BuildExternalLogFileInfo(db_->dbname_, file_number, metadata);
      }
    } else {
      s = VerifyMetadataChecksum(read_options, file_number, metadata,
                                 verify_options, info);
    }
    UnregisterActiveReader(file_number);
    return s;
  }

  Status VerifyExternalLogFileChecksums(
      const ReadOptions& read_options,
      const VerifyExternalLogFileChecksumOptions& verify_options,
      std::vector<ExternalLogFileChecksumInfo>* infos) override {
    std::vector<std::pair<ExternalLogFileNumber, ExternalLogFileMetadata>>
        files;
    std::vector<ExternalLogFileNumber> active_refs;
    {
      InstrumentedMutexLock lock(&db_->mutex_);
      for (const auto& file :
           db_->versions_->GetExternalLogFileSet().GetFiles()) {
        if (file.second.GetState() == ExternalLogFileState::kUnsealed &&
            !verify_options.include_unsealed) {
          continue;
        }
        files.emplace_back(file.first, file.second);
        RefActiveFileLocked(file.first);
        active_refs.push_back(file.first);
      }
    }
    TEST_SYNC_POINT(
        "ExternalLogFileManagerImpl::VerifyExternalLogFileChecksums:"
        "AfterRegisterReaders");

    std::vector<ExternalLogFileChecksumInfo> result;
    Status s;
    for (const auto& file : files) {
      ExternalLogFileChecksumInfo info;
      s = VerifyMetadataChecksum(read_options, file.first, file.second,
                                 verify_options,
                                 infos != nullptr ? &info : nullptr);
      if (!s.ok()) {
        break;
      }
      if (infos != nullptr) {
        result.push_back(std::move(info));
      }
    }

    for (const auto file_number : active_refs) {
      UnregisterActiveReader(file_number);
    }
    if (!s.ok()) {
      return s;
    }
    if (infos != nullptr) {
      *infos = std::move(result);
    }
    return Status::OK();
  }

  Status DeleteExternalLogFile(const WriteOptions& options,
                               const std::string& name) override {
    return DeleteExternalLogFiles(options, std::vector<std::string>{name});
  }

  Status DeleteExternalLogFiles(
      const WriteOptions& options,
      const std::vector<std::string>& names) override {
    if (names.empty()) {
      return Status::OK();
    }

    Status s = CheckDBMutable();
    if (!s.ok()) {
      return s;
    }

    std::unordered_set<std::string> unique_names;
    unique_names.reserve(names.size());
    for (const auto& name : names) {
      s = ValidateExternalLogFileLookupName(name);
      if (!s.ok()) {
        return s;
      }
      if (!unique_names.insert(name).second) {
        return Status::InvalidArgument(
            "Duplicate external log file name in delete request");
      }
    }

    std::vector<ExternalLogFileMetadata> metadata_list;
    metadata_list.reserve(names.size());
    {
      InstrumentedMutexLock lock(&db_->mutex_);
      for (const auto& name : names) {
        ExternalLogFileNumber file_number = 0;
        const ExternalLogFileMetadata* found =
            db_->versions_->GetExternalLogFileSet().FindFile(name,
                                                             &file_number);
        if (found == nullptr) {
          return Status::NotFound("External log file", name);
        }
        if (HasActiveWriterLocked(file_number)) {
          return Status::Busy("External log file has an active writer", name);
        }
        if (HasActiveRefsLocked(file_number)) {
          return Status::Busy("External log file has active readers", name);
        }
        metadata_list.push_back(*found);
      }

      VersionEdit edit;
      for (const auto& name : names) {
        edit.DeleteExternalLogFile(name);
      }
      s = LogAndApplyLocked(options, &edit);
    }
    if (!s.ok()) {
      return s;
    }

    Status delete_status;
    for (const auto& metadata : metadata_list) {
      const std::string path = ExternalLogFilePath(db_->dbname_, metadata);
      Status delete_s = DeleteUnaccountedDBFile(
          &db_->immutable_db_options_, path,
          ExternalLogFileParentPath(db_->dbname_, metadata),
          /*force_bg=*/false, /*force_fg=*/false, /*bucket=*/std::nullopt);
      if (!delete_s.IsNotFound()) {
        delete_status.UpdateIfOk(delete_s);
      }
    }
    return delete_status;
  }

  Status GetFileMetadata(const std::string& name,
                         ExternalLogFileNumber* file_number,
                         ExternalLogFileMetadata* metadata) {
    Status s = ValidateExternalLogFileLookupName(name);
    if (!s.ok()) {
      return s;
    }
    InstrumentedMutexLock lock(&db_->mutex_);
    const ExternalLogFileMetadata* found =
        db_->versions_->GetExternalLogFileSet().FindFile(name, file_number);
    if (found == nullptr) {
      return Status::NotFound("External log file", name);
    }
    if (metadata != nullptr) {
      *metadata = *found;
    }
    return Status::OK();
  }

  Status LogAndApplyLocked(const WriteOptions& write_options,
                           VersionEdit* edit) {
    db_->mutex_.AssertHeld();
    Status s = db_->versions_->LogAndApplyToDefaultColumnFamily(
        ReadOptions(), write_options, edit, &db_->mutex_,
        db_->directories_.GetDbDir());
    if (!s.ok() && db_->versions_->io_status().IsIOError()) {
      db_->error_handler_.SetBGError(db_->versions_->io_status(),
                                     BackgroundErrorReason::kManifestWrite);
    }
    return s;
  }

  Status LogAndApply(const WriteOptions& write_options, VersionEdit* edit) {
    InstrumentedMutexLock lock(&db_->mutex_);
    return LogAndApplyLocked(write_options, edit);
  }

  Status CreateRollingChecksumGenerator(
      const std::string& path, const std::string& requested_checksum_func_name,
      std::unique_ptr<FileChecksumGenerator>* checksum_generator) const {
    assert(checksum_generator != nullptr);
    checksum_generator->reset();
    FileChecksumGenFactory* checksum_factory =
        db_->immutable_db_options_.file_checksum_gen_factory.get();
    if (checksum_factory == nullptr) {
      return Status::OK();
    }

    const std::string requested_func_name =
        NormalizeRequestedChecksumFuncName(requested_checksum_func_name);
    FileChecksumGenContext gen_context;
    gen_context.file_name = path;
    gen_context.requested_checksum_func_name = requested_func_name;
    *checksum_generator =
        checksum_factory->CreateFileChecksumGenerator(gen_context);
    if (*checksum_generator == nullptr) {
      return Status::InvalidArgument(
          "Cannot create file checksum generator for external log file");
    }
    if (!requested_func_name.empty() &&
        requested_func_name != (*checksum_generator)->Name()) {
      checksum_generator->reset();
      return Status::InvalidArgument(
          "External log file checksum function does not match checksum "
          "factory");
    }
    return Status::OK();
  }

  Status ComputeChecksum(
      const std::string& path, uint64_t size,
      const std::string& requested_checksum_func_name,
      const ReadOptions& read_options, std::string* checksum,
      std::string* checksum_func_name,
      std::unique_ptr<FileChecksumGenerator>* rolling_generator = nullptr) {
    return ComputeExternalLogFileChecksumPrefix(
        db_->fs_.get(), path,
        db_->immutable_db_options_.file_checksum_gen_factory.get(),
        requested_checksum_func_name, size, read_options, db_->file_options_,
        db_->immutable_db_options_.clock, checksum, checksum_func_name,
        rolling_generator);
  }

  Status SyncExternalLogDirectory(const std::string& directory) {
    return SyncDirectory(
        db_->fs_.get(), directory,
        DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
  }

  Status NewReaderForWriter(
      ExternalLogFileNumber file_number,
      const ExternalLogFileMetadata& metadata,
      const ExternalLogFileReaderOptions& options,
      std::shared_ptr<ExternalLogFileHandleState> handle_state,
      std::unique_ptr<ExternalLogFileReader>* reader) {
    assert(reader != nullptr);
    // The writer registers the reader while holding the writer mutex so Close()
    // and DeleteExternalLogFile() cannot create an unprotected purge window.
    reader->reset();
    const std::string path = ExternalLogFilePath(db_->dbname_, metadata);
    FileOptions file_options = ExternalLogFileOptionsForRead(
        db_->file_options_, metadata.GetTemperature());
    std::unique_ptr<FSRandomAccessFile> file;
    Status s =
        db_->fs_->NewRandomAccessFile(path, file_options, &file, nullptr);
    if (!s.ok()) {
      UnregisterActiveReader(file_number);
      return s;
    }
    reader->reset(new ExternalLogFileReaderImpl(
        this, metadata.GetName(), file_number,
        BuildExternalLogFileInfo(db_->dbname_, file_number, metadata),
        std::move(file), options.visibility, std::move(handle_state)));
    return Status::OK();
  }

  Status GetFileMetadataAndRegisterActiveReader(
      const std::string& name, ExternalLogFileNumber* file_number,
      ExternalLogFileMetadata* metadata,
      std::shared_ptr<ExternalLogFileHandleState>* handle_state) {
    assert(file_number != nullptr);
    assert(metadata != nullptr);
    assert(handle_state != nullptr);
    handle_state->reset();
    Status s = ValidateExternalLogFileLookupName(name);
    if (!s.ok()) {
      return s;
    }
    InstrumentedMutexLock lock(&db_->mutex_);
    const ExternalLogFileMetadata* found =
        db_->versions_->GetExternalLogFileSet().FindFile(name, file_number);
    if (found == nullptr) {
      return Status::NotFound("External log file", name);
    }
    *metadata = *found;
    auto iter = db_->active_external_log_writers_.find(*file_number);
    if (iter != db_->active_external_log_writers_.end()) {
      std::shared_ptr<void> erased_state = iter->second.lock();
      if (erased_state == nullptr) {
        db_->active_external_log_writers_.erase(iter);
      } else {
        *handle_state =
            std::static_pointer_cast<ExternalLogFileHandleState>(erased_state);
      }
    }
    RefActiveFileLocked(*file_number);
    return Status::OK();
  }

  Status GetFileMetadataAndRegisterActiveWriter(
      const std::string& name, ExternalLogFileNumber* file_number,
      ExternalLogFileMetadata* metadata,
      std::shared_ptr<ExternalLogFileHandleState>* handle_state) {
    assert(file_number != nullptr);
    assert(metadata != nullptr);
    assert(handle_state != nullptr);
    handle_state->reset();
    Status s = ValidateExternalLogFileLookupName(name);
    if (!s.ok()) {
      return s;
    }
    InstrumentedMutexLock lock(&db_->mutex_);
    const ExternalLogFileMetadata* found =
        db_->versions_->GetExternalLogFileSet().FindFile(name, file_number);
    if (found == nullptr) {
      return Status::NotFound("External log file", name);
    }
    if (found->GetState() != ExternalLogFileState::kUnsealed) {
      return Status::InvalidArgument(
          "Only unsealed external log files can be reopened for append");
    }
    *metadata = *found;
    *handle_state = std::make_shared<ExternalLogFileHandleState>(
        metadata->GetDurableSize());
    return RegisterActiveWriterLocked(*file_number, *handle_state);
  }

  Status RegisterActiveWriter(
      ExternalLogFileNumber file_number,
      const std::shared_ptr<ExternalLogFileHandleState>& handle_state) {
    assert(handle_state != nullptr);
    InstrumentedMutexLock lock(&db_->mutex_);
    return RegisterActiveWriterLocked(file_number, handle_state);
  }

  Status RegisterActiveWriterLocked(
      ExternalLogFileNumber file_number,
      const std::shared_ptr<ExternalLogFileHandleState>& handle_state) {
    db_->mutex_.AssertHeld();
    auto writer_iter = db_->active_external_log_writers_.find(file_number);
    if (writer_iter != db_->active_external_log_writers_.end() &&
        writer_iter->second.lock() != nullptr) {
      return Status::Busy("External log file already has an active writer");
    }
    db_->active_external_log_writers_[file_number] =
        std::static_pointer_cast<void>(handle_state);
    RefActiveFileLocked(file_number);
    return Status::OK();
  }

  void UnregisterActiveWriter(ExternalLogFileNumber file_number) {
    InstrumentedMutexLock lock(&db_->mutex_);
    db_->active_external_log_writers_.erase(file_number);
    UnrefActiveFileLocked(file_number);
  }

  void RegisterActiveReader(ExternalLogFileNumber file_number) {
    InstrumentedMutexLock lock(&db_->mutex_);
    RefActiveFileLocked(file_number);
  }

  void UnregisterActiveReader(ExternalLogFileNumber file_number) {
    InstrumentedMutexLock lock(&db_->mutex_);
    UnrefActiveFileLocked(file_number);
  }

  bool HasActiveWriterLocked(ExternalLogFileNumber file_number) {
    db_->mutex_.AssertHeld();
    auto writer_iter = db_->active_external_log_writers_.find(file_number);
    if (writer_iter == db_->active_external_log_writers_.end()) {
      return false;
    }
    if (writer_iter->second.lock() == nullptr) {
      db_->active_external_log_writers_.erase(writer_iter);
      return false;
    }
    return true;
  }

  bool HasActiveRefsLocked(ExternalLogFileNumber file_number) const {
    db_->mutex_.AssertHeld();
    auto ref_iter = db_->active_external_log_file_refs_.find(file_number);
    return ref_iter != db_->active_external_log_file_refs_.end() &&
           ref_iter->second > 0;
  }

  bool HasActiveReaders(ExternalLogFileNumber file_number) {
    InstrumentedMutexLock lock(&db_->mutex_);
    auto ref_iter = db_->active_external_log_file_refs_.find(file_number);
    // ReopenExternalLogFile() already registered itself as the active writer.
    // Other temporary refs, such as physical-size listing and checksum
    // verification, are conservatively treated like readers because truncation
    // must not race with code that can open or inspect the physical tail.
    return ref_iter != db_->active_external_log_file_refs_.end() &&
           ref_iter->second > 1;
  }

  DBImpl* db() const { return db_; }
  const std::string& dbname() const { return db_->dbname_; }
  bool use_fsync() const { return db_->immutable_db_options_.use_fsync; }
  bool has_checksum_factory() const {
    return db_->immutable_db_options_.file_checksum_gen_factory != nullptr;
  }

 private:
  Status CheckDBMutable() const {
    if (db_->versions_->unchanging()) {
      return Status::NotSupported(
          "External log file mutation is not supported in read-only mode");
    }
    return Status::OK();
  }

  bool FileExistsLocked(const std::string& name) const {
    db_->mutex_.AssertHeld();
    ExternalLogFileNumber ignored = 0;
    return db_->versions_->GetExternalLogFileSet().FindFile(name, &ignored) !=
           nullptr;
  }

  bool FilePathExistsLocked(const std::string& io_path) const {
    db_->mutex_.AssertHeld();
    for (const auto& file :
         db_->versions_->GetExternalLogFileSet().GetFiles()) {
      if (ExternalLogFilePath(db_->dbname_, file.second) == io_path) {
        return true;
      }
    }
    return false;
  }

  void RefActiveFileLocked(ExternalLogFileNumber file_number) {
    db_->mutex_.AssertHeld();
    ++db_->active_external_log_file_refs_[file_number];
  }

  void UnrefActiveFileLocked(ExternalLogFileNumber file_number) {
    db_->mutex_.AssertHeld();
    auto ref_iter = db_->active_external_log_file_refs_.find(file_number);
    if (ref_iter == db_->active_external_log_file_refs_.end()) {
      return;
    }
    assert(ref_iter->second > 0);
    --ref_iter->second;
    if (ref_iter->second == 0) {
      db_->active_external_log_file_refs_.erase(ref_iter);
    }
  }

  void ReleasePendingOutput(
      std::unique_ptr<std::list<uint64_t>::iterator>* pending_outputs_elem) {
    assert(pending_outputs_elem != nullptr);
    if (pending_outputs_elem->get() == nullptr) {
      return;
    }
    InstrumentedMutexLock lock(&db_->mutex_);
    db_->ReleaseFileNumberFromPendingOutputs(*pending_outputs_elem);
  }

  Status ValidateIngestOptions(
      const IngestExternalLogFileOptions& options) const {
    Status s = ValidateExternalLogFileName(options.path_type, options.name);
    if (!s.ok()) {
      return s;
    }
    s = ValidateExternalLogFileBackupSupport(options.include_in_backup);
    if (!s.ok()) {
      return s;
    }
    if (options.source_path.empty()) {
      return Status::InvalidArgument(
          "External log ingest source path is empty");
    }
    if (options.file_checksum_func_name.empty() &&
        options.verify_file_checksum) {
      return Status::InvalidArgument(
          "External log ingest checksum function name is empty");
    }
    const bool unknown_checksum_func =
        options.file_checksum_func_name.empty() ||
        options.file_checksum_func_name == kUnknownFileChecksumFuncName;
    if (unknown_checksum_func && !options.file_checksum.empty()) {
      return Status::InvalidArgument(
          "External log ingest checksum value requires a checksum function "
          "name");
    }
    if (!unknown_checksum_func && options.file_checksum.empty()) {
      return Status::InvalidArgument(
          "External log ingest checksum function name requires a checksum "
          "value");
    }
    if (options.verify_file_checksum &&
        options.file_checksum_func_name == kUnknownFileChecksumFuncName) {
      return Status::InvalidArgument(
          "External log ingest checksum function name is unknown");
    }
    if (options.verify_file_checksum &&
        db_->immutable_db_options_.file_checksum_gen_factory == nullptr) {
      return Status::InvalidArgument(
          "External log ingest checksum verification requires "
          "options.file_checksum_gen_factory");
    }
    uint64_t physical_size = 0;
    s = db_->fs_->GetFileSize(options.source_path, IOOptions(), &physical_size,
                              nullptr);
    if (!s.ok()) {
      return s;
    }
    if (options.logical_size > physical_size) {
      return Status::InvalidArgument(
          "External log ingest logical size exceeds source file size");
    }
    if (!unknown_checksum_func &&
        db_->immutable_db_options_.file_checksum_gen_factory != nullptr) {
      std::unique_ptr<FileChecksumGenerator> checksum_generator;
      s = CreateRollingChecksumGenerator(options.source_path,
                                         options.file_checksum_func_name,
                                         &checksum_generator);
      if (!s.ok()) {
        return s;
      }
      if (checksum_generator == nullptr ||
          options.file_checksum_func_name != checksum_generator->Name()) {
        return Status::InvalidArgument(
            "External log ingest checksum function name is not recognized by "
            "options.file_checksum_gen_factory");
      }
    }
    return Status::OK();
  }

  Status TransferIngestedFile(const IngestExternalLogFileOptions& options,
                              const std::string& destination, bool* moved) {
    assert(moved != nullptr);
    *moved = false;
    if (options.source_path == destination) {
      return Status::OK();
    }
    switch (options.ingestion_mode) {
      case ExternalLogFileIngestionMode::kCopy:
        if (options.logical_size == 0) {
          return CreateFile(db_->fs_.get(), destination, "",
                            db_->immutable_db_options_.use_fsync);
        }
        return CopyFile(db_->fs_.get(), options.source_path,
                        options.temperature, destination, options.temperature,
                        options.logical_size,
                        db_->immutable_db_options_.use_fsync, db_->io_tracer_);
      case ExternalLogFileIngestionMode::kMove: {
        Status s = db_->fs_->RenameFile(options.source_path, destination,
                                        IOOptions(), nullptr);
        if (s.ok()) {
          *moved = true;
        }
        return s;
      }
      case ExternalLogFileIngestionMode::kLink:
        return db_->fs_->LinkFile(options.source_path, destination, IOOptions(),
                                  nullptr);
      case ExternalLogFileIngestionMode::kLinkOrCopy: {
        Status s = db_->fs_->LinkFile(options.source_path, destination,
                                      IOOptions(), nullptr);
        if (s.ok()) {
          return s;
        }
        if (!s.IsNotSupported()) {
          return s;
        }
        s.PermitUncheckedError();
        if (options.logical_size == 0) {
          return CreateFile(db_->fs_.get(), destination, "",
                            db_->immutable_db_options_.use_fsync);
        }
        return CopyFile(db_->fs_.get(), options.source_path,
                        options.temperature, destination, options.temperature,
                        options.logical_size,
                        db_->immutable_db_options_.use_fsync, db_->io_tracer_);
      }
    }
    return Status::InvalidArgument("Unknown external log file ingestion mode");
  }

  Status VerifyMetadataChecksum(
      const ReadOptions& read_options, ExternalLogFileNumber file_number,
      const ExternalLogFileMetadata& metadata,
      const VerifyExternalLogFileChecksumOptions& verify_options,
      ExternalLogFileChecksumInfo* info) {
    if (metadata.GetFileChecksumFuncName() == kUnknownFileChecksumFuncName) {
      if (!verify_options.fail_if_checksum_unavailable) {
        if (info != nullptr) {
          *info = ExternalLogFileChecksumInfo();
          info->file_info =
              BuildExternalLogFileInfo(db_->dbname_, file_number, metadata);
        }
        return Status::OK();
      }
      return Status::InvalidArgument(
          "External log file checksum metadata is unavailable");
    }
    if (!has_checksum_factory()) {
      if (!verify_options.fail_if_checksum_unavailable) {
        if (info != nullptr) {
          *info = ExternalLogFileChecksumInfo();
          info->file_info =
              BuildExternalLogFileInfo(db_->dbname_, file_number, metadata);
        }
        return Status::OK();
      }
      return Status::InvalidArgument(
          "External log file checksum verification requires "
          "options.file_checksum_gen_factory");
    }

    const std::string path = ExternalLogFilePath(db_->dbname_, metadata);
    std::string actual_checksum;
    std::string actual_checksum_func_name;
    Status s = ComputeChecksum(path, metadata.GetDurableSize(),
                               metadata.GetFileChecksumFuncName(), read_options,
                               &actual_checksum, &actual_checksum_func_name);
    if (!s.ok()) {
      return s;
    }
    if (actual_checksum != metadata.GetFileChecksum() ||
        actual_checksum_func_name != metadata.GetFileChecksumFuncName()) {
      return Status::Corruption("External log file checksum mismatch");
    }
    if (info != nullptr) {
      *info = ExternalLogFileChecksumInfo();
      info->file_info =
          BuildExternalLogFileInfo(db_->dbname_, file_number, metadata);
      info->verified = true;
      info->verified_size = metadata.GetDurableSize();
      info->expected_checksum = metadata.GetFileChecksum();
      info->actual_checksum = std::move(actual_checksum);
      info->checksum_func_name = std::move(actual_checksum_func_name);
    }
    return Status::OK();
  }

  DBImpl* db_;
};

ExternalLogFileReaderImpl::~ExternalLogFileReaderImpl() {
  manager_->UnregisterActiveReader(file_number_);
}

ExternalLogFileWriterImpl::ExternalLogFileWriterImpl(
    ExternalLogFileManagerImpl* manager, ExternalLogFileNumber file_number,
    ExternalLogFileMetadata metadata, std::string path,
    std::unique_ptr<FSWritableFile>&& file,
    std::shared_ptr<ExternalLogFileHandleState> handle_state,
    std::unique_ptr<FileChecksumGenerator>&& checksum_generator,
    bool active_writer_registered)
    : manager_(manager),
      file_number_(file_number),
      name_(metadata.GetName()),
      metadata_(std::move(metadata)),
      path_(std::move(path)),
      file_(std::move(file)),
      handle_state_(std::move(handle_state)),
      checksum_generator_(std::move(checksum_generator)),
      active_writer_registered_(active_writer_registered) {
  assert(handle_state_ != nullptr);
}

ExternalLogFileWriterImpl::~ExternalLogFileWriterImpl() {
  if (file_ != nullptr) {
    file_->Close(IOOptions(), nullptr).PermitUncheckedError();
  }
  UnregisterActiveWriter();
}

void ExternalLogFileWriterImpl::UnregisterActiveWriter() {
  if (active_writer_registered_) {
    manager_->UnregisterActiveWriter(file_number_);
    active_writer_registered_ = false;
  }
}

Status ExternalLogFileWriterImpl::Append(const WriteOptions& options,
                                         const Slice& data, uint64_t* offset) {
  TEST_SYNC_POINT("ExternalLogFileWriterImpl::Append:BeforeLock");
  std::lock_guard<std::mutex> lock(handle_state_->mutex_);
  if (handle_state_->closed_ || handle_state_->sealed_) {
    return Status::InvalidArgument("External log file writer is closed");
  }
  IOOptions io_options;
  Status s = PrepareWriteIO(options, &io_options);
  if (!s.ok()) {
    return s;
  }
  const uint64_t append_offset = handle_state_->visible_size_;
  if (data.size() >
      std::numeric_limits<uint64_t>::max() - handle_state_->visible_size_) {
    return Status::InvalidArgument("External log file append size overflow");
  }
  s = file_->Append(data, io_options, nullptr);
  if (!s.ok()) {
    return s;
  }
  if (checksum_generator_ != nullptr && data.size() != 0) {
    if (checksum_generator_finalized_) {
      // A failed Seal() can leave a finalized generator available for retry.
      // Once more data is appended, that checksum is stale and must be rebuilt
      // explicitly if the caller later seals the file.
      checksum_generator_.reset();
      checksum_generator_finalized_ = false;
    } else {
      checksum_generator_->Update(data.data(), data.size());
    }
  }
  handle_state_->visible_size_ += data.size();
  if (offset != nullptr) {
    *offset = append_offset;
  }
  return Status::OK();
}

Status ExternalLogFileWriterImpl::FlushLocked(const WriteOptions& options) {
  IOOptions io_options;
  Status s = PrepareWriteIO(options, &io_options);
  if (!s.ok()) {
    return s;
  }
  return file_->Flush(io_options, nullptr);
}

Status ExternalLogFileWriterImpl::Flush(const WriteOptions& options) {
  std::lock_guard<std::mutex> lock(handle_state_->mutex_);
  if (handle_state_->closed_) {
    return Status::InvalidArgument("External log file writer is closed");
  }
  return FlushLocked(options);
}

Status ExternalLogFileWriterImpl::SyncWritableFileLocked(
    const WriteOptions& options) {
  IOOptions io_options;
  Status s = PrepareWriteIO(options, &io_options);
  if (!s.ok()) {
    return s;
  }
  s = file_->Flush(io_options, nullptr);
  if (!s.ok()) {
    return s;
  }
  if (manager_->use_fsync()) {
    return file_->Fsync(io_options, nullptr);
  }
  return file_->Sync(io_options, nullptr);
}

Status ExternalLogFileWriterImpl::Sync(const WriteOptions& options,
                                       ExternalLogFileInfo* info) {
  std::lock_guard<std::mutex> lock(handle_state_->mutex_);
  if (handle_state_->closed_ || handle_state_->sealed_) {
    return Status::InvalidArgument("External log file writer is closed");
  }
  TEST_SYNC_POINT("ExternalLogFileWriterImpl::Sync:BeforeWritableFileSync");
  Status s = SyncWritableFileLocked(options);
  if (!s.ok()) {
    return s;
  }
  if (info != nullptr) {
    *info =
        BuildExternalLogFileInfo(manager_->dbname(), file_number_, metadata_);
    info->physical_size = VisibleSizeLocked();
  }
  return Status::OK();
}

Status ExternalLogFileWriterImpl::Seal(
    const WriteOptions& options, const SealExternalLogFileOptions& seal_options,
    ExternalLogFileInfo* info) {
  std::lock_guard<std::mutex> lock(handle_state_->mutex_);
  if (handle_state_->closed_ || handle_state_->sealed_) {
    return Status::InvalidArgument("External log file writer is closed");
  }
  uint64_t logical_size = seal_options.has_logical_size
                              ? seal_options.logical_size
                              : handle_state_->visible_size_;
  if (logical_size > handle_state_->visible_size_) {
    return Status::InvalidArgument(
        "External log file seal size exceeds append position");
  }
  Status s = SyncWritableFileLocked(options);
  if (!s.ok()) {
    return s;
  }
  s = PersistMetadataLocked(options, ExternalLogFileState::kSealed,
                            logical_size, seal_options.recompute_checksum,
                            info);
  if (!s.ok()) {
    return s;
  }

  IOOptions io_options;
  s = PrepareWriteIO(options, &io_options);
  if (s.ok()) {
    s = file_->Close(io_options, nullptr);
  }
  handle_state_->closed_ = true;
  handle_state_->sealed_ = true;
  file_.reset();
  UnregisterActiveWriter();
  return s;
}

Status ExternalLogFileWriterImpl::Close(const WriteOptions& options) {
  std::lock_guard<std::mutex> lock(handle_state_->mutex_);
  if (handle_state_->closed_) {
    return Status::OK();
  }
  IOOptions io_options;
  Status s = PrepareWriteIO(options, &io_options);
  if (s.ok()) {
    s = file_->Close(io_options, nullptr);
  }
  handle_state_->closed_ = true;
  file_.reset();
  UnregisterActiveWriter();
  return s;
}

Status ExternalLogFileWriterImpl::NewReader(
    const ExternalLogFileReaderOptions& options,
    std::unique_ptr<ExternalLogFileReader>* reader) {
  if (reader == nullptr) {
    return Status::InvalidArgument("External log file reader is null");
  }
  ExternalLogFileMetadata metadata;
  {
    std::lock_guard<std::mutex> lock(handle_state_->mutex_);
    if (handle_state_->closed_) {
      return Status::InvalidArgument("External log file writer is closed");
    }
    metadata = metadata_;
    manager_->RegisterActiveReader(file_number_);
  }
  return manager_->NewReaderForWriter(file_number_, metadata, options,
                                      handle_state_, reader);
}

Status ExternalLogFileWriterImpl::GetFileInfo(ExternalLogFileInfo* info) const {
  if (info == nullptr) {
    return Status::InvalidArgument("ExternalLogFileInfo is null");
  }
  std::lock_guard<std::mutex> lock(handle_state_->mutex_);
  *info = BuildExternalLogFileInfo(manager_->dbname(), file_number_, metadata_);
  info->physical_size = VisibleSizeLocked();
  return Status::OK();
}

uint64_t ExternalLogFileWriterImpl::VisibleSizeLocked() const {
  return handle_state_->visible_size_;
}

Status ExternalLogFileWriterImpl::ChecksumForMetadata(
    uint64_t durable_size, uint64_t visible_size, bool recompute_checksum,
    std::string* checksum, std::string* checksum_func_name) {
  assert(checksum != nullptr);
  assert(checksum_func_name != nullptr);

  if (recompute_checksum) {
    if (!manager_->has_checksum_factory()) {
      return Status::InvalidArgument(
          "External log file checksum recomputation requires "
          "options.file_checksum_gen_factory");
    }
    std::unique_ptr<FileChecksumGenerator> rolling_generator;
    Status s = manager_->ComputeChecksum(
        path_, durable_size, metadata_.GetFileChecksumFuncName(), ReadOptions(),
        checksum, checksum_func_name,
        durable_size == visible_size ? &rolling_generator : nullptr);
    if (!s.ok()) {
      return s;
    }
    if (rolling_generator != nullptr) {
      TEST_SYNC_POINT(
          "ExternalLogFileWriterImpl::ChecksumForMetadata:"
          "InstallRollingGenerator");
      checksum_generator_ = std::move(rolling_generator);
      checksum_generator_finalized_ = false;
    }
    return Status::OK();
  }

  if (metadata_.GetDurableSize() == durable_size &&
      metadata_.GetFileChecksumFuncName() != kUnknownFileChecksumFuncName) {
    *checksum = metadata_.GetFileChecksum();
    *checksum_func_name = metadata_.GetFileChecksumFuncName();
    return Status::OK();
  }

  if (durable_size == visible_size && checksum_generator_ != nullptr) {
    if (!checksum_generator_finalized_) {
      checksum_generator_->Finalize();
      checksum_generator_finalized_ = true;
    }
    *checksum = checksum_generator_->GetChecksum();
    *checksum_func_name = checksum_generator_->Name();
    return Status::OK();
  }

  if (!manager_->has_checksum_factory()) {
    // Writers created or sealed without checksum support intentionally record
    // unknown checksum metadata. Reopen with recompute_checksum still requires
    // a factory because it must rebuild appendable rolling checksum state.
    *checksum = kUnknownFileChecksum;
    *checksum_func_name = kUnknownFileChecksumFuncName;
    return Status::OK();
  }

  return Status::InvalidArgument(
      "External log file checksum state is unavailable; retry with checksum "
      "recomputation");
}

Status ExternalLogFileWriterImpl::PersistMetadataLocked(
    const WriteOptions& options, ExternalLogFileState state,
    uint64_t durable_size, bool recompute_checksum, ExternalLogFileInfo* info) {
  if (durable_size < metadata_.GetDurableSize()) {
    return Status::InvalidArgument(
        "External log file durable size is smaller than MANIFEST metadata");
  }

  std::string checksum;
  std::string checksum_func_name;
  Status s =
      ChecksumForMetadata(durable_size, VisibleSizeLocked(), recompute_checksum,
                          &checksum, &checksum_func_name);
  if (!s.ok()) {
    return s;
  }

  ExternalLogFileMetadata new_metadata = metadata_;
  new_metadata.SetState(state);
  new_metadata.SetDurableSize(durable_size);
  new_metadata.SetFileChecksum(std::move(checksum),
                               std::move(checksum_func_name));

  VersionEdit edit;
  edit.AddExternalLogFile(file_number_, new_metadata);
  s = manager_->LogAndApply(options, &edit);
  if (!s.ok()) {
    return s;
  }

  metadata_ = std::move(new_metadata);
  checksum_generator_.reset();
  checksum_generator_finalized_ = false;
  if (info != nullptr) {
    *info =
        BuildExternalLogFileInfo(manager_->dbname(), file_number_, metadata_);
    info->physical_size = VisibleSizeLocked();
  }
  return Status::OK();
}

Status DBImpl::NewExternalLogFileManager(
    std::unique_ptr<ExternalLogFileManager>* manager) {
  if (manager == nullptr) {
    return Status::InvalidArgument("External log file manager is null");
  }
  manager->reset(new ExternalLogFileManagerImpl(this));
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
