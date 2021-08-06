//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <cstdlib>
#include <functional>
#include <future>
#include <limits>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "env/fs_readonly.h"
#include "env/fs_remap.h"
#include "file/filename.h"
#include "file/line_file_reader.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "port/port.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/transaction_log.h"
#include "table/sst_file_dumper.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/channel.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/string_util.h"
#include "utilities/backupable/backupable_db_impl.h"
#include "utilities/checkpoint/checkpoint_impl.h"

namespace ROCKSDB_NAMESPACE {

namespace {
using ShareFilesNaming = BackupEngineOptions::ShareFilesNaming;

constexpr BackupID kLatestBackupIDMarker = static_cast<BackupID>(-2);

inline uint32_t ChecksumHexToInt32(const std::string& checksum_hex) {
  std::string checksum_str;
  Slice(checksum_hex).DecodeHex(&checksum_str);
  return EndianSwapValue(DecodeFixed32(checksum_str.c_str()));
}
inline std::string ChecksumStrToHex(const std::string& checksum_str) {
  return Slice(checksum_str).ToString(true);
}
inline std::string ChecksumInt32ToHex(const uint32_t& checksum_value) {
  std::string checksum_str;
  PutFixed32(&checksum_str, EndianSwapValue(checksum_value));
  return ChecksumStrToHex(checksum_str);
}

const std::string kPrivateDirName = "private";
const std::string kMetaDirName = "meta";
const std::string kSharedDirName = "shared";
const std::string kSharedChecksumDirName = "shared_checksum";
const std::string kPrivateDirSlash = kPrivateDirName + "/";
const std::string kMetaDirSlash = kMetaDirName + "/";
const std::string kSharedDirSlash = kSharedDirName + "/";
const std::string kSharedChecksumDirSlash = kSharedChecksumDirName + "/";

}  // namespace

void BackupStatistics::IncrementNumberSuccessBackup() {
  number_success_backup++;
}
void BackupStatistics::IncrementNumberFailBackup() {
  number_fail_backup++;
}

uint32_t BackupStatistics::GetNumberSuccessBackup() const {
  return number_success_backup;
}
uint32_t BackupStatistics::GetNumberFailBackup() const {
  return number_fail_backup;
}

std::string BackupStatistics::ToString() const {
  char result[50];
  snprintf(result, sizeof(result), "# success backup: %u, # fail backup: %u",
           GetNumberSuccessBackup(), GetNumberFailBackup());
  return result;
}

void BackupEngineOptions::Dump(Logger* logger) const {
  ROCKS_LOG_INFO(logger, "               Options.backup_dir: %s",
                 backup_dir.c_str());
  ROCKS_LOG_INFO(logger, "               Options.backup_env: %p", backup_env);
  ROCKS_LOG_INFO(logger, "        Options.share_table_files: %d",
                 static_cast<int>(share_table_files));
  ROCKS_LOG_INFO(logger, "                 Options.info_log: %p", info_log);
  ROCKS_LOG_INFO(logger, "                     Options.sync: %d",
                 static_cast<int>(sync));
  ROCKS_LOG_INFO(logger, "         Options.destroy_old_data: %d",
                 static_cast<int>(destroy_old_data));
  ROCKS_LOG_INFO(logger, "         Options.backup_log_files: %d",
                 static_cast<int>(backup_log_files));
  ROCKS_LOG_INFO(logger, "        Options.backup_rate_limit: %" PRIu64,
                 backup_rate_limit);
  ROCKS_LOG_INFO(logger, "       Options.restore_rate_limit: %" PRIu64,
                 restore_rate_limit);
  ROCKS_LOG_INFO(logger, "Options.max_background_operations: %d",
                 max_background_operations);
}

// -------- BackupEngineImpl class ---------
class BackupEngineImpl {
 public:
  BackupEngineImpl(const BackupEngineOptions& options, Env* db_env,
                   bool read_only = false);
  ~BackupEngineImpl();

  Status CreateNewBackupWithMetadata(const CreateBackupOptions& options, DB* db,
                                     const std::string& app_metadata,
                                     BackupID* new_backup_id_ptr);

  Status PurgeOldBackups(uint32_t num_backups_to_keep);

  Status DeleteBackup(BackupID backup_id);

  void StopBackup() { stop_backup_.store(true, std::memory_order_release); }

  Status GarbageCollect();

  // The returned BackupInfos are in chronological order, which means the
  // latest backup comes last.
  void GetBackupInfo(std::vector<BackupInfo>* backup_info,
                     bool include_file_details) const;

  Status GetBackupInfo(BackupID backup_id, BackupInfo* backup_info,
                       bool include_file_details = false) const;

  void GetCorruptedBackups(std::vector<BackupID>* corrupt_backup_ids) const;

  Status RestoreDBFromBackup(const RestoreOptions& options, BackupID backup_id,
                             const std::string& db_dir,
                             const std::string& wal_dir) const;

  Status RestoreDBFromLatestBackup(const RestoreOptions& options,
                                   const std::string& db_dir,
                                   const std::string& wal_dir) const {
    // Note: don't read latest_valid_backup_id_ outside of lock
    return RestoreDBFromBackup(options, kLatestBackupIDMarker, db_dir, wal_dir);
  }

  Status VerifyBackup(BackupID backup_id,
                      bool verify_with_checksum = false) const;

  Status Initialize();

  ShareFilesNaming GetNamingNoFlags() const {
    return options_.share_files_with_checksum_naming &
           BackupEngineOptions::kMaskNoNamingFlags;
  }
  ShareFilesNaming GetNamingFlags() const {
    return options_.share_files_with_checksum_naming &
           BackupEngineOptions::kMaskNamingFlags;
  }

 private:
  void DeleteChildren(const std::string& dir,
                      uint32_t file_type_filter = 0) const;
  Status DeleteBackupNoGC(BackupID backup_id);

  // Extends the "result" map with pathname->size mappings for the contents of
  // "dir" in "env". Pathnames are prefixed with "dir".
  Status ReadChildFileCurrentSizes(
      const std::string& dir, Env* env,
      std::unordered_map<std::string, uint64_t>* result) const;

  struct FileInfo {
    FileInfo(const std::string& fname, uint64_t sz, const std::string& checksum,
             const std::string& id = "", const std::string& sid = "")
        : refs(0),
          filename(fname),
          size(sz),
          checksum_hex(checksum),
          db_id(id),
          db_session_id(sid) {}

    FileInfo(const FileInfo&) = delete;
    FileInfo& operator=(const FileInfo&) = delete;

    int refs;
    const std::string filename;
    const uint64_t size;
    // crc32c checksum as hex. empty == unknown / unavailable
    std::string checksum_hex;
    // DB identities
    // db_id is obtained for potential usage in the future but not used
    // currently
    const std::string db_id;
    // db_session_id appears in the backup SST filename if the table naming
    // option is kUseDbSessionId
    const std::string db_session_id;

    std::string GetDbFileName() {
      std::string rv;
      // extract the filename part
      size_t slash = filename.find_last_of('/');
      // file will either be shared/<file>, shared_checksum/<file_crc32c_size>,
      // shared_checksum/<file_session>, shared_checksum/<file_crc32c_session>,
      // or private/<number>/<file>
      assert(slash != std::string::npos);
      rv = filename.substr(slash + 1);

      // if the file was in shared_checksum, extract the real file name
      // in this case the file is <number>_<checksum>_<size>.<type>,
      // <number>_<session>.<type>, or <number>_<checksum>_<session>.<type>
      if (filename.substr(0, slash) == kSharedChecksumDirName) {
        rv = GetFileFromChecksumFile(rv);
      }
      return rv;
    }
  };

  static inline std::string WithoutTrailingSlash(const std::string& path) {
    if (path.empty() || path.back() != '/') {
      return path;
    } else {
      return path.substr(path.size() - 1);
    }
  }

  static inline std::string WithTrailingSlash(const std::string& path) {
    if (path.empty() || path.back() != '/') {
      return path + '/';
    } else {
      return path;
    }
  }

  // A filesystem wrapper that makes shared backup files appear to be in the
  // private backup directory (dst_dir), so that the private backup dir can
  // be opened as a read-only DB.
  class RemapSharedFileSystem : public RemapFileSystem {
   public:
    RemapSharedFileSystem(const std::shared_ptr<FileSystem>& base,
                          const std::string& dst_dir,
                          const std::string& src_base_dir,
                          const std::vector<std::shared_ptr<FileInfo>>& files)
        : RemapFileSystem(base),
          dst_dir_(WithoutTrailingSlash(dst_dir)),
          dst_dir_slash_(WithTrailingSlash(dst_dir)),
          src_base_dir_(WithTrailingSlash(src_base_dir)) {
      for (auto& info : files) {
        if (!StartsWith(info->filename, kPrivateDirSlash)) {
          assert(StartsWith(info->filename, kSharedDirSlash) ||
                 StartsWith(info->filename, kSharedChecksumDirSlash));
          remaps_[info->GetDbFileName()] = info;
        }
      }
    }

    const char* Name() const override {
      return "BackupEngineImpl::RemapSharedFileSystem";
    }

    // Sometimes a directory listing is required in opening a DB
    IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                         std::vector<std::string>* result,
                         IODebugContext* dbg) override {
      IOStatus s = RemapFileSystem::GetChildren(dir, options, result, dbg);
      if (s.ok() && (dir == dst_dir_ || dir == dst_dir_slash_)) {
        // Assume remapped files exist
        for (auto& r : remaps_) {
          result->push_back(r.first);
        }
      }
      return s;
    }

    // Sometimes a directory listing is required in opening a DB
    IOStatus GetChildrenFileAttributes(const std::string& dir,
                                       const IOOptions& options,
                                       std::vector<FileAttributes>* result,
                                       IODebugContext* dbg) override {
      IOStatus s =
          RemapFileSystem::GetChildrenFileAttributes(dir, options, result, dbg);
      if (s.ok() && (dir == dst_dir_ || dir == dst_dir_slash_)) {
        // Assume remapped files exist with recorded size
        for (auto& r : remaps_) {
          result->emplace_back();  // clean up with C++20
          FileAttributes& attr = result->back();
          attr.name = r.first;
          attr.size_bytes = r.second->size;
        }
      }
      return s;
    }

   protected:
    // When a file in dst_dir is requested, see if we need to remap to shared
    // file path.
    std::pair<IOStatus, std::string> EncodePath(
        const std::string& path) override {
      if (path.empty() || path[0] != '/') {
        return {IOStatus::InvalidArgument(path, "Not an absolute path"), ""};
      }
      std::pair<IOStatus, std::string> rv{IOStatus(), path};
      if (StartsWith(path, dst_dir_slash_)) {
        std::string relative = path.substr(dst_dir_slash_.size());
        auto it = remaps_.find(relative);
        if (it != remaps_.end()) {
          rv.second = src_base_dir_ + it->second->filename;
        }
      }
      return rv;
    }

   private:
    // Absolute path to a directory that some extra files will be mapped into.
    const std::string dst_dir_;
    // Includes a trailing slash.
    const std::string dst_dir_slash_;
    // Absolute path to a directory containing some files to be mapped into
    // dst_dir_. Includes a trailing slash.
    const std::string src_base_dir_;
    // If remaps_[x] exists, attempt to read dst_dir_ / x should instead read
    // src_base_dir_ / remaps_[x]->filename. FileInfo is used to maximize
    // sharing with other backup data in memory.
    std::unordered_map<std::string, std::shared_ptr<FileInfo>> remaps_;
  };

  class BackupMeta {
   public:
    BackupMeta(
        const std::string& meta_filename, const std::string& meta_tmp_filename,
        std::unordered_map<std::string, std::shared_ptr<FileInfo>>* file_infos,
        Env* env)
        : timestamp_(0),
          sequence_number_(0),
          size_(0),
          meta_filename_(meta_filename),
          meta_tmp_filename_(meta_tmp_filename),
          file_infos_(file_infos),
          env_(env) {}

    BackupMeta(const BackupMeta&) = delete;
    BackupMeta& operator=(const BackupMeta&) = delete;

    ~BackupMeta() {}

    void RecordTimestamp() {
      // Best effort
      Status s = env_->GetCurrentTime(&timestamp_);
      if (!s.ok()) {
        timestamp_ = /* something clearly fabricated */ 1;
      }
    }
    int64_t GetTimestamp() const {
      return timestamp_;
    }
    uint64_t GetSize() const {
      return size_;
    }
    uint32_t GetNumberFiles() const {
      return static_cast<uint32_t>(files_.size());
    }
    void SetSequenceNumber(uint64_t sequence_number) {
      sequence_number_ = sequence_number;
    }
    uint64_t GetSequenceNumber() const { return sequence_number_; }

    const std::string& GetAppMetadata() const { return app_metadata_; }

    void SetAppMetadata(const std::string& app_metadata) {
      app_metadata_ = app_metadata;
    }

    Status AddFile(std::shared_ptr<FileInfo> file_info);

    Status Delete(bool delete_meta = true);

    bool Empty() const { return files_.empty(); }

    std::shared_ptr<FileInfo> GetFile(const std::string& filename) const {
      auto it = file_infos_->find(filename);
      if (it == file_infos_->end())
        return nullptr;
      return it->second;
    }

    const std::vector<std::shared_ptr<FileInfo>>& GetFiles() const {
      return files_;
    }

    // @param abs_path_to_size Pre-fetched file sizes (bytes).
    Status LoadFromFile(
        const std::string& backup_dir,
        const std::unordered_map<std::string, uint64_t>& abs_path_to_size,
        Logger* info_log,
        std::unordered_set<std::string>* reported_ignored_fields);
    Status StoreToFile(
        bool sync, const TEST_FutureSchemaVersion2Options* test_future_options);

    std::string GetInfoString() {
      std::ostringstream ss;
      ss << "Timestamp: " << timestamp_ << std::endl;
      char human_size[16];
      AppendHumanBytes(size_, human_size, sizeof(human_size));
      ss << "Size: " << human_size << std::endl;
      ss << "Files:" << std::endl;
      for (const auto& file : files_) {
        AppendHumanBytes(file->size, human_size, sizeof(human_size));
        ss << file->filename << ", size " << human_size << ", refs "
           << file->refs << std::endl;
      }
      return ss.str();
    }

    const std::shared_ptr<Env>& GetEnvForOpen() const {
      if (!env_for_open_) {
        // Lazy initialize
        // Find directories
        std::string dst_dir = meta_filename_;
        auto i = dst_dir.rfind(kMetaDirSlash);
        assert(i != std::string::npos);
        std::string src_base_dir = dst_dir.substr(0, i);
        dst_dir.replace(i, kMetaDirSlash.size(), kPrivateDirSlash);
        // Make the RemapSharedFileSystem
        std::shared_ptr<FileSystem> remap_fs =
            std::make_shared<RemapSharedFileSystem>(
                env_->GetFileSystem(), dst_dir, src_base_dir, files_);
        // Make it read-only for safety
        remap_fs = std::make_shared<ReadOnlyFileSystem>(remap_fs);
        // Make an Env wrapper
        env_for_open_ = std::make_shared<CompositeEnvWrapper>(env_, remap_fs);
      }
      return env_for_open_;
    }

   private:
    int64_t timestamp_;
    // sequence number is only approximate, should not be used
    // by clients
    uint64_t sequence_number_;
    uint64_t size_;
    std::string app_metadata_;
    std::string const meta_filename_;
    std::string const meta_tmp_filename_;
    // files with relative paths (without "/" prefix!!)
    std::vector<std::shared_ptr<FileInfo>> files_;
    std::unordered_map<std::string, std::shared_ptr<FileInfo>>* file_infos_;
    Env* env_;
    mutable std::shared_ptr<Env> env_for_open_;
  };  // BackupMeta

  void SetBackupInfoFromBackupMeta(BackupID id, const BackupMeta& meta,
                                   BackupInfo* backup_info,
                                   bool include_file_details) const;

  inline std::string GetAbsolutePath(
      const std::string &relative_path = "") const {
    assert(relative_path.size() == 0 || relative_path[0] != '/');
    return options_.backup_dir + "/" + relative_path;
  }
  inline std::string GetPrivateFileRel(BackupID backup_id,
                                       bool tmp = false,
                                       const std::string& file = "") const {
    assert(file.size() == 0 || file[0] != '/');
    return kPrivateDirSlash + ROCKSDB_NAMESPACE::ToString(backup_id) +
           (tmp ? ".tmp" : "") + "/" + file;
  }
  inline std::string GetSharedFileRel(const std::string& file = "",
                                      bool tmp = false) const {
    assert(file.size() == 0 || file[0] != '/');
    return kSharedDirSlash + std::string(tmp ? "." : "") + file +
           (tmp ? ".tmp" : "");
  }
  inline std::string GetSharedFileWithChecksumRel(const std::string& file = "",
                                                  bool tmp = false) const {
    assert(file.size() == 0 || file[0] != '/');
    return kSharedChecksumDirSlash + std::string(tmp ? "." : "") + file +
           (tmp ? ".tmp" : "");
  }
  inline bool UseLegacyNaming(const std::string& sid) const {
    return GetNamingNoFlags() ==
               BackupEngineOptions::kLegacyCrc32cAndFileSize ||
           sid.empty();
  }
  inline std::string GetSharedFileWithChecksum(
      const std::string& file, const std::string& checksum_hex,
      const uint64_t file_size, const std::string& db_session_id) const {
    assert(file.size() == 0 || file[0] != '/');
    std::string file_copy = file;
    if (UseLegacyNaming(db_session_id)) {
      assert(!checksum_hex.empty());
      file_copy.insert(file_copy.find_last_of('.'),
                       "_" + ToString(ChecksumHexToInt32(checksum_hex)) + "_" +
                           ToString(file_size));
    } else {
      file_copy.insert(file_copy.find_last_of('.'), "_s" + db_session_id);
      if (GetNamingFlags() & BackupEngineOptions::kFlagIncludeFileSize) {
        file_copy.insert(file_copy.find_last_of('.'),
                         "_" + ToString(file_size));
      }
    }
    return file_copy;
  }
  static inline std::string GetFileFromChecksumFile(const std::string& file) {
    assert(file.size() == 0 || file[0] != '/');
    std::string file_copy = file;
    size_t first_underscore = file_copy.find_first_of('_');
    return file_copy.erase(first_underscore,
                           file_copy.find_last_of('.') - first_underscore);
  }
  inline std::string GetBackupMetaFile(BackupID backup_id, bool tmp) const {
    return GetAbsolutePath(kMetaDirName) + "/" + (tmp ? "." : "") +
           ROCKSDB_NAMESPACE::ToString(backup_id) + (tmp ? ".tmp" : "");
  }

  // If size_limit == 0, there is no size limit, copy everything.
  //
  // Exactly one of src and contents must be non-empty.
  //
  // @param src If non-empty, the file is copied from this pathname.
  // @param contents If non-empty, the file will be created with these contents.
  Status CopyOrCreateFile(const std::string& src, const std::string& dst,
                          const std::string& contents, Env* src_env,
                          Env* dst_env, const EnvOptions& src_env_options,
                          bool sync, RateLimiter* rate_limiter,
                          uint64_t* size = nullptr,
                          std::string* checksum_hex = nullptr,
                          uint64_t size_limit = 0,
                          std::function<void()> progress_callback = []() {});

  Status ReadFileAndComputeChecksum(const std::string& src, Env* src_env,
                                    const EnvOptions& src_env_options,
                                    uint64_t size_limit,
                                    std::string* checksum_hex) const;

  // Obtain db_id and db_session_id from the table properties of file_path
  Status GetFileDbIdentities(Env* src_env, const EnvOptions& src_env_options,
                             const std::string& file_path, std::string* db_id,
                             std::string* db_session_id);

  struct CopyOrCreateResult {
    ~CopyOrCreateResult() {
      // The Status needs to be ignored here for two reasons.
      // First, if the BackupEngineImpl shuts down with jobs outstanding, then
      // it is possible that the Status in the future/promise is never read,
      // resulting in an unchecked Status. Second, if there are items in the
      // channel when the BackupEngineImpl is shutdown, these will also have
      // Status that have not been checked.  This
      // TODO: Fix those issues so that the Status
      status.PermitUncheckedError();
    }
    uint64_t size;
    std::string checksum_hex;
    std::string db_id;
    std::string db_session_id;
    Status status;
  };

  // Exactly one of src_path and contents must be non-empty. If src_path is
  // non-empty, the file is copied from this pathname. Otherwise, if contents is
  // non-empty, the file will be created at dst_path with these contents.
  struct CopyOrCreateWorkItem {
    std::string src_path;
    std::string dst_path;
    std::string contents;
    Env* src_env;
    Env* dst_env;
    EnvOptions src_env_options;
    bool sync;
    RateLimiter* rate_limiter;
    uint64_t size_limit;
    std::promise<CopyOrCreateResult> result;
    std::function<void()> progress_callback;
    std::string src_checksum_func_name;
    std::string src_checksum_hex;
    std::string db_id;
    std::string db_session_id;

    CopyOrCreateWorkItem()
        : src_path(""),
          dst_path(""),
          contents(""),
          src_env(nullptr),
          dst_env(nullptr),
          src_env_options(),
          sync(false),
          rate_limiter(nullptr),
          size_limit(0),
          src_checksum_func_name(kUnknownFileChecksumFuncName),
          src_checksum_hex(""),
          db_id(""),
          db_session_id("") {}

    CopyOrCreateWorkItem(const CopyOrCreateWorkItem&) = delete;
    CopyOrCreateWorkItem& operator=(const CopyOrCreateWorkItem&) = delete;

    CopyOrCreateWorkItem(CopyOrCreateWorkItem&& o) ROCKSDB_NOEXCEPT {
      *this = std::move(o);
    }

    CopyOrCreateWorkItem& operator=(CopyOrCreateWorkItem&& o) ROCKSDB_NOEXCEPT {
      src_path = std::move(o.src_path);
      dst_path = std::move(o.dst_path);
      contents = std::move(o.contents);
      src_env = o.src_env;
      dst_env = o.dst_env;
      src_env_options = std::move(o.src_env_options);
      sync = o.sync;
      rate_limiter = o.rate_limiter;
      size_limit = o.size_limit;
      result = std::move(o.result);
      progress_callback = std::move(o.progress_callback);
      src_checksum_func_name = std::move(o.src_checksum_func_name);
      src_checksum_hex = std::move(o.src_checksum_hex);
      db_id = std::move(o.db_id);
      db_session_id = std::move(o.db_session_id);
      return *this;
    }

    CopyOrCreateWorkItem(
        std::string _src_path, std::string _dst_path, std::string _contents,
        Env* _src_env, Env* _dst_env, EnvOptions _src_env_options, bool _sync,
        RateLimiter* _rate_limiter, uint64_t _size_limit,
        std::function<void()> _progress_callback = []() {},
        const std::string& _src_checksum_func_name =
            kUnknownFileChecksumFuncName,
        const std::string& _src_checksum_hex = "",
        const std::string& _db_id = "", const std::string& _db_session_id = "")
        : src_path(std::move(_src_path)),
          dst_path(std::move(_dst_path)),
          contents(std::move(_contents)),
          src_env(_src_env),
          dst_env(_dst_env),
          src_env_options(std::move(_src_env_options)),
          sync(_sync),
          rate_limiter(_rate_limiter),
          size_limit(_size_limit),
          progress_callback(_progress_callback),
          src_checksum_func_name(_src_checksum_func_name),
          src_checksum_hex(_src_checksum_hex),
          db_id(_db_id),
          db_session_id(_db_session_id) {}
  };

  struct BackupAfterCopyOrCreateWorkItem {
    std::future<CopyOrCreateResult> result;
    bool shared;
    bool needed_to_copy;
    Env* backup_env;
    std::string dst_path_tmp;
    std::string dst_path;
    std::string dst_relative;
    BackupAfterCopyOrCreateWorkItem()
      : shared(false),
        needed_to_copy(false),
        backup_env(nullptr),
        dst_path_tmp(""),
        dst_path(""),
        dst_relative("") {}

    BackupAfterCopyOrCreateWorkItem(BackupAfterCopyOrCreateWorkItem&& o)
        ROCKSDB_NOEXCEPT {
      *this = std::move(o);
    }

    BackupAfterCopyOrCreateWorkItem& operator=(
        BackupAfterCopyOrCreateWorkItem&& o) ROCKSDB_NOEXCEPT {
      result = std::move(o.result);
      shared = o.shared;
      needed_to_copy = o.needed_to_copy;
      backup_env = o.backup_env;
      dst_path_tmp = std::move(o.dst_path_tmp);
      dst_path = std::move(o.dst_path);
      dst_relative = std::move(o.dst_relative);
      return *this;
    }

    BackupAfterCopyOrCreateWorkItem(std::future<CopyOrCreateResult>&& _result,
                                    bool _shared, bool _needed_to_copy,
                                    Env* _backup_env, std::string _dst_path_tmp,
                                    std::string _dst_path,
                                    std::string _dst_relative)
        : result(std::move(_result)),
          shared(_shared),
          needed_to_copy(_needed_to_copy),
          backup_env(_backup_env),
          dst_path_tmp(std::move(_dst_path_tmp)),
          dst_path(std::move(_dst_path)),
          dst_relative(std::move(_dst_relative)) {}
  };

  struct RestoreAfterCopyOrCreateWorkItem {
    std::future<CopyOrCreateResult> result;
    std::string from_file;
    std::string to_file;
    std::string checksum_hex;
    RestoreAfterCopyOrCreateWorkItem() : checksum_hex("") {}
    RestoreAfterCopyOrCreateWorkItem(std::future<CopyOrCreateResult>&& _result,
                                     const std::string& _from_file,
                                     const std::string& _to_file,
                                     const std::string& _checksum_hex)
        : result(std::move(_result)),
          from_file(_from_file),
          to_file(_to_file),
          checksum_hex(_checksum_hex) {}
    RestoreAfterCopyOrCreateWorkItem(RestoreAfterCopyOrCreateWorkItem&& o)
        ROCKSDB_NOEXCEPT {
      *this = std::move(o);
    }

    RestoreAfterCopyOrCreateWorkItem& operator=(
        RestoreAfterCopyOrCreateWorkItem&& o) ROCKSDB_NOEXCEPT {
      result = std::move(o.result);
      checksum_hex = std::move(o.checksum_hex);
      return *this;
    }
  };

  bool initialized_;
  std::mutex byte_report_mutex_;
  mutable channel<CopyOrCreateWorkItem> files_to_copy_or_create_;
  std::vector<port::Thread> threads_;
  std::atomic<CpuPriority> threads_cpu_priority_;

  // Certain operations like PurgeOldBackups and DeleteBackup will trigger
  // automatic GarbageCollect (true) unless we've already done one in this
  // session and have not failed to delete backup files since then (false).
  bool might_need_garbage_collect_ = true;

  // Adds a file to the backup work queue to be copied or created if it doesn't
  // already exist.
  //
  // Exactly one of src_dir and contents must be non-empty.
  //
  // @param src_dir If non-empty, the file in this directory named fname will be
  //    copied.
  // @param fname Name of destination file and, in case of copy, source file.
  // @param contents If non-empty, the file will be created with these contents.
  Status AddBackupFileWorkItem(
      std::unordered_set<std::string>& live_dst_paths,
      std::vector<BackupAfterCopyOrCreateWorkItem>& backup_items_to_finish,
      BackupID backup_id, bool shared, const std::string& src_dir,
      const std::string& fname,  // starts with "/"
      const EnvOptions& src_env_options, RateLimiter* rate_limiter,
      FileType file_type, uint64_t size_bytes, uint64_t size_limit = 0,
      bool shared_checksum = false,
      std::function<void()> progress_callback = []() {},
      const std::string& contents = std::string(),
      const std::string& src_checksum_func_name = kUnknownFileChecksumFuncName,
      const std::string& src_checksum_str = kUnknownFileChecksum);

  // backup state data
  BackupID latest_backup_id_;
  BackupID latest_valid_backup_id_;
  std::map<BackupID, std::unique_ptr<BackupMeta>> backups_;
  std::map<BackupID, std::pair<Status, std::unique_ptr<BackupMeta>>>
      corrupt_backups_;
  std::unordered_map<std::string,
                     std::shared_ptr<FileInfo>> backuped_file_infos_;
  std::atomic<bool> stop_backup_;

  // options data
  BackupEngineOptions options_;
  Env* db_env_;
  Env* backup_env_;

  // directories
  std::unique_ptr<Directory> backup_directory_;
  std::unique_ptr<Directory> shared_directory_;
  std::unique_ptr<Directory> meta_directory_;
  std::unique_ptr<Directory> private_directory_;

  static const size_t kDefaultCopyFileBufferSize = 5 * 1024 * 1024LL;  // 5MB
  mutable size_t copy_file_buffer_size_;
  bool read_only_;
  BackupStatistics backup_statistics_;
  std::unordered_set<std::string> reported_ignored_fields_;
  static const size_t kMaxAppMetaSize = 1024 * 1024;  // 1MB

 public:
  std::unique_ptr<TEST_FutureSchemaVersion2Options> test_future_options_;
};

// -------- BackupEngineImplThreadSafe class ---------
// This locking layer for thread safety in the public API is layered on
// top to prevent accidental recursive locking with RWMutex, which is UB.
// Note: BackupEngineReadOnlyBase inherited twice, but has no fields
class BackupEngineImplThreadSafe : public BackupEngine,
                                   public BackupEngineReadOnly {
 public:
  BackupEngineImplThreadSafe(const BackupEngineOptions& options, Env* db_env,
                             bool read_only = false)
      : impl_(options, db_env, read_only) {}
  ~BackupEngineImplThreadSafe() override {}

  using BackupEngine::CreateNewBackupWithMetadata;
  Status CreateNewBackupWithMetadata(const CreateBackupOptions& options, DB* db,
                                     const std::string& app_metadata,
                                     BackupID* new_backup_id) override {
    WriteLock lock(&mutex_);
    return impl_.CreateNewBackupWithMetadata(options, db, app_metadata,
                                             new_backup_id);
  }

  Status PurgeOldBackups(uint32_t num_backups_to_keep) override {
    WriteLock lock(&mutex_);
    return impl_.PurgeOldBackups(num_backups_to_keep);
  }

  Status DeleteBackup(BackupID backup_id) override {
    WriteLock lock(&mutex_);
    return impl_.DeleteBackup(backup_id);
  }

  void StopBackup() override {
    // No locking needed
    impl_.StopBackup();
  }

  Status GarbageCollect() override {
    WriteLock lock(&mutex_);
    return impl_.GarbageCollect();
  }

  Status GetLatestBackupInfo(BackupInfo* backup_info,
                             bool include_file_details = false) const override {
    ReadLock lock(&mutex_);
    return impl_.GetBackupInfo(kLatestBackupIDMarker, backup_info,
                               include_file_details);
  }

  Status GetBackupInfo(BackupID backup_id, BackupInfo* backup_info,
                       bool include_file_details = false) const override {
    ReadLock lock(&mutex_);
    return impl_.GetBackupInfo(backup_id, backup_info, include_file_details);
  }

  void GetBackupInfo(std::vector<BackupInfo>* backup_info,
                     bool include_file_details) const override {
    ReadLock lock(&mutex_);
    impl_.GetBackupInfo(backup_info, include_file_details);
  }

  void GetCorruptedBackups(
      std::vector<BackupID>* corrupt_backup_ids) const override {
    ReadLock lock(&mutex_);
    impl_.GetCorruptedBackups(corrupt_backup_ids);
  }

  using BackupEngine::RestoreDBFromBackup;
  Status RestoreDBFromBackup(const RestoreOptions& options, BackupID backup_id,
                             const std::string& db_dir,
                             const std::string& wal_dir) const override {
    ReadLock lock(&mutex_);
    return impl_.RestoreDBFromBackup(options, backup_id, db_dir, wal_dir);
  }

  using BackupEngine::RestoreDBFromLatestBackup;
  Status RestoreDBFromLatestBackup(const RestoreOptions& options,
                                   const std::string& db_dir,
                                   const std::string& wal_dir) const override {
    // Defer to above function, which locks
    return RestoreDBFromBackup(options, kLatestBackupIDMarker, db_dir, wal_dir);
  }

  Status VerifyBackup(BackupID backup_id,
                      bool verify_with_checksum = false) const override {
    ReadLock lock(&mutex_);
    return impl_.VerifyBackup(backup_id, verify_with_checksum);
  }

  // Not public API but needed
  Status Initialize() {
    // No locking needed
    return impl_.Initialize();
  }

  // Not public API but used in testing
  void TEST_EnableWriteFutureSchemaVersion2(
      const TEST_FutureSchemaVersion2Options& options) {
    impl_.test_future_options_.reset(
        new TEST_FutureSchemaVersion2Options(options));
  }

 private:
  mutable port::RWMutex mutex_;
  BackupEngineImpl impl_;
};

Status BackupEngine::Open(const BackupEngineOptions& options, Env* env,
                          BackupEngine** backup_engine_ptr) {
  std::unique_ptr<BackupEngineImplThreadSafe> backup_engine(
      new BackupEngineImplThreadSafe(options, env));
  auto s = backup_engine->Initialize();
  if (!s.ok()) {
    *backup_engine_ptr = nullptr;
    return s;
  }
  *backup_engine_ptr = backup_engine.release();
  return Status::OK();
}

BackupEngineImpl::BackupEngineImpl(const BackupEngineOptions& options,
                                   Env* db_env, bool read_only)
    : initialized_(false),
      threads_cpu_priority_(),
      latest_backup_id_(0),
      latest_valid_backup_id_(0),
      stop_backup_(false),
      options_(options),
      db_env_(db_env),
      backup_env_(options.backup_env != nullptr ? options.backup_env : db_env_),
      copy_file_buffer_size_(kDefaultCopyFileBufferSize),
      read_only_(read_only) {
  if (options_.backup_rate_limiter == nullptr &&
      options_.backup_rate_limit > 0) {
    options_.backup_rate_limiter.reset(
        NewGenericRateLimiter(options_.backup_rate_limit));
  }
  if (options_.restore_rate_limiter == nullptr &&
      options_.restore_rate_limit > 0) {
    options_.restore_rate_limiter.reset(
        NewGenericRateLimiter(options_.restore_rate_limit));
  }
}

BackupEngineImpl::~BackupEngineImpl() {
  files_to_copy_or_create_.sendEof();
  for (auto& t : threads_) {
    t.join();
  }
  LogFlush(options_.info_log);
  for (const auto& it : corrupt_backups_) {
    it.second.first.PermitUncheckedError();
  }
}

Status BackupEngineImpl::Initialize() {
  assert(!initialized_);
  initialized_ = true;
  if (read_only_) {
    ROCKS_LOG_INFO(options_.info_log, "Starting read_only backup engine");
  }
  options_.Dump(options_.info_log);

  auto meta_path = GetAbsolutePath(kMetaDirName);

  if (!read_only_) {
    // we might need to clean up from previous crash or I/O errors
    might_need_garbage_collect_ = true;

    if (options_.max_valid_backups_to_open != port::kMaxInt32) {
      options_.max_valid_backups_to_open = port::kMaxInt32;
      ROCKS_LOG_WARN(
          options_.info_log,
          "`max_valid_backups_to_open` is not set to the default value. Ignoring "
          "its value since BackupEngine is not read-only.");
    }

    // gather the list of directories that we need to create
    std::vector<std::pair<std::string, std::unique_ptr<Directory>*>>
        directories;
    directories.emplace_back(GetAbsolutePath(), &backup_directory_);
    if (options_.share_table_files) {
      if (options_.share_files_with_checksum) {
        directories.emplace_back(
            GetAbsolutePath(GetSharedFileWithChecksumRel()),
            &shared_directory_);
      } else {
        directories.emplace_back(GetAbsolutePath(GetSharedFileRel()),
                                 &shared_directory_);
      }
    }
    directories.emplace_back(GetAbsolutePath(kPrivateDirName),
                             &private_directory_);
    directories.emplace_back(meta_path, &meta_directory_);
    // create all the dirs we need
    for (const auto& d : directories) {
      auto s = backup_env_->CreateDirIfMissing(d.first);
      if (s.ok()) {
        s = backup_env_->NewDirectory(d.first, d.second);
      }
      if (!s.ok()) {
        return s;
      }
    }
  }

  std::vector<std::string> backup_meta_files;
  {
    auto s = backup_env_->GetChildren(meta_path, &backup_meta_files);
    if (s.IsNotFound()) {
      return Status::NotFound(meta_path + " is missing");
    } else if (!s.ok()) {
      return s;
    }
  }
  // create backups_ structure
  for (auto& file : backup_meta_files) {
    ROCKS_LOG_INFO(options_.info_log, "Detected backup %s", file.c_str());
    BackupID backup_id = 0;
    sscanf(file.c_str(), "%u", &backup_id);
    if (backup_id == 0 || file != ROCKSDB_NAMESPACE::ToString(backup_id)) {
      // Invalid file name, will be deleted with auto-GC when user
      // initiates an append or write operation. (Behave as read-only until
      // then.)
      ROCKS_LOG_INFO(options_.info_log, "Skipping unrecognized meta file %s",
                     file.c_str());
      continue;
    }
    assert(backups_.find(backup_id) == backups_.end());
    // Insert all the (backup_id, BackupMeta) that will be loaded later
    // The loading performed later will check whether there are corrupt backups
    // and move the corrupt backups to corrupt_backups_
    backups_.insert(std::make_pair(
        backup_id, std::unique_ptr<BackupMeta>(new BackupMeta(
                       GetBackupMetaFile(backup_id, false /* tmp */),
                       GetBackupMetaFile(backup_id, true /* tmp */),
                       &backuped_file_infos_, backup_env_))));
  }

  latest_backup_id_ = 0;
  latest_valid_backup_id_ = 0;
  if (options_.destroy_old_data) {  // Destroy old data
    assert(!read_only_);
    ROCKS_LOG_INFO(
        options_.info_log,
        "Backup Engine started with destroy_old_data == true, deleting all "
        "backups");
    auto s = PurgeOldBackups(0);
    if (s.ok()) {
      s = GarbageCollect();
    }
    if (!s.ok()) {
      return s;
    }
  } else {  // Load data from storage
    // abs_path_to_size: maps absolute paths of files in backup directory to
    // their corresponding sizes
    std::unordered_map<std::string, uint64_t> abs_path_to_size;
    // Insert files and their sizes in backup sub-directories (shared and
    // shared_checksum) to abs_path_to_size
    for (const auto& rel_dir :
         {GetSharedFileRel(), GetSharedFileWithChecksumRel()}) {
      const auto abs_dir = GetAbsolutePath(rel_dir);
      Status s =
          ReadChildFileCurrentSizes(abs_dir, backup_env_, &abs_path_to_size);
      if (!s.ok()) {
        // I/O error likely impacting all backups
        return s;
      }
    }
    // load the backups if any, until valid_backups_to_open of the latest
    // non-corrupted backups have been successfully opened.
    int valid_backups_to_open = options_.max_valid_backups_to_open;
    for (auto backup_iter = backups_.rbegin();
         backup_iter != backups_.rend();
         ++backup_iter) {
      assert(latest_backup_id_ == 0 || latest_backup_id_ > backup_iter->first);
      if (latest_backup_id_ == 0) {
        latest_backup_id_ = backup_iter->first;
      }
      if (valid_backups_to_open == 0) {
        break;
      }

      // Insert files and their sizes in backup sub-directories
      // (private/backup_id) to abs_path_to_size
      Status s = ReadChildFileCurrentSizes(
          GetAbsolutePath(GetPrivateFileRel(backup_iter->first)), backup_env_,
          &abs_path_to_size);
      if (s.ok()) {
        s = backup_iter->second->LoadFromFile(
            options_.backup_dir, abs_path_to_size, options_.info_log,
            &reported_ignored_fields_);
      }
      if (s.IsCorruption() || s.IsNotSupported()) {
        ROCKS_LOG_INFO(options_.info_log, "Backup %u corrupted -- %s",
                       backup_iter->first, s.ToString().c_str());
        corrupt_backups_.insert(
            std::make_pair(backup_iter->first,
                           std::make_pair(s, std::move(backup_iter->second))));
      } else if (!s.ok()) {
        // Distinguish corruption errors from errors in the backup Env.
        // Errors in the backup Env (i.e., this code path) will cause Open() to
        // fail, whereas corruption errors would not cause Open() failures.
        return s;
      } else {
        ROCKS_LOG_INFO(options_.info_log, "Loading backup %" PRIu32 " OK:\n%s",
                       backup_iter->first,
                       backup_iter->second->GetInfoString().c_str());
        assert(latest_valid_backup_id_ == 0 ||
               latest_valid_backup_id_ > backup_iter->first);
        if (latest_valid_backup_id_ == 0) {
          latest_valid_backup_id_ = backup_iter->first;
        }
        --valid_backups_to_open;
      }
    }

    for (const auto& corrupt : corrupt_backups_) {
      backups_.erase(backups_.find(corrupt.first));
    }
    // erase the backups before max_valid_backups_to_open
    int num_unopened_backups;
    if (options_.max_valid_backups_to_open == 0) {
      num_unopened_backups = 0;
    } else {
      num_unopened_backups =
          std::max(0, static_cast<int>(backups_.size()) -
                          options_.max_valid_backups_to_open);
    }
    for (int i = 0; i < num_unopened_backups; ++i) {
      assert(backups_.begin()->second->Empty());
      backups_.erase(backups_.begin());
    }
  }

  ROCKS_LOG_INFO(options_.info_log, "Latest backup is %u", latest_backup_id_);
  ROCKS_LOG_INFO(options_.info_log, "Latest valid backup is %u",
                 latest_valid_backup_id_);

  // set up threads perform copies from files_to_copy_or_create_ in the
  // background
  threads_cpu_priority_ = CpuPriority::kNormal;
  threads_.reserve(options_.max_background_operations);
  for (int t = 0; t < options_.max_background_operations; t++) {
    threads_.emplace_back([this]() {
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
      pthread_setname_np(pthread_self(), "backup_engine");
#endif
#endif
      CpuPriority current_priority = CpuPriority::kNormal;
      CopyOrCreateWorkItem work_item;
      while (files_to_copy_or_create_.read(work_item)) {
        CpuPriority priority = threads_cpu_priority_;
        if (current_priority != priority) {
          TEST_SYNC_POINT_CALLBACK(
              "BackupEngineImpl::Initialize:SetCpuPriority", &priority);
          port::SetCpuPriority(0, priority);
          current_priority = priority;
        }
        CopyOrCreateResult result;
        result.status = CopyOrCreateFile(
            work_item.src_path, work_item.dst_path, work_item.contents,
            work_item.src_env, work_item.dst_env, work_item.src_env_options,
            work_item.sync, work_item.rate_limiter, &result.size,
            &result.checksum_hex, work_item.size_limit,
            work_item.progress_callback);
        result.db_id = work_item.db_id;
        result.db_session_id = work_item.db_session_id;
        if (result.status.ok() && !work_item.src_checksum_hex.empty()) {
          // unknown checksum function name implies no db table file checksum in
          // db manifest; work_item.src_checksum_hex not empty means
          // backup engine has calculated its crc32c checksum for the table
          // file; therefore, we are able to compare the checksums.
          if (work_item.src_checksum_func_name ==
                  kUnknownFileChecksumFuncName ||
              work_item.src_checksum_func_name == kDbFileChecksumFuncName) {
            if (work_item.src_checksum_hex != result.checksum_hex) {
              std::string checksum_info(
                  "Expected checksum is " + work_item.src_checksum_hex +
                  " while computed checksum is " + result.checksum_hex);
              result.status =
                  Status::Corruption("Checksum mismatch after copying to " +
                                     work_item.dst_path + ": " + checksum_info);
            }
          } else {
            // FIXME(peterd): dead code?
            std::string checksum_function_info(
                "Existing checksum function is " +
                work_item.src_checksum_func_name +
                " while provided checksum function is " +
                kBackupFileChecksumFuncName);
            ROCKS_LOG_INFO(
                options_.info_log,
                "Unable to verify checksum after copying to %s: %s\n",
                work_item.dst_path.c_str(), checksum_function_info.c_str());
          }
        }
        work_item.result.set_value(std::move(result));
      }
    });
  }
  ROCKS_LOG_INFO(options_.info_log, "Initialized BackupEngine");

  return Status::OK();
}

Status BackupEngineImpl::CreateNewBackupWithMetadata(
    const CreateBackupOptions& options, DB* db, const std::string& app_metadata,
    BackupID* new_backup_id_ptr) {
  assert(initialized_);
  assert(!read_only_);
  if (app_metadata.size() > kMaxAppMetaSize) {
    return Status::InvalidArgument("App metadata too large");
  }

  if (options.decrease_background_thread_cpu_priority) {
    if (options.background_thread_cpu_priority < threads_cpu_priority_) {
      threads_cpu_priority_.store(options.background_thread_cpu_priority);
    }
  }

  BackupID new_backup_id = latest_backup_id_ + 1;

  assert(backups_.find(new_backup_id) == backups_.end());

  auto private_dir = GetAbsolutePath(GetPrivateFileRel(new_backup_id));
  Status s = backup_env_->FileExists(private_dir);
  if (s.ok()) {
    // maybe last backup failed and left partial state behind, clean it up.
    // need to do this before updating backups_ such that a private dir
    // named after new_backup_id will be cleaned up.
    // (If an incomplete new backup is followed by an incomplete delete
    // of the latest full backup, then there could be more than one next
    // id with a private dir, the last thing to be deleted in delete
    // backup, but all will be cleaned up with a GarbageCollect.)
    s = GarbageCollect();
  } else if (s.IsNotFound()) {
    // normal case, the new backup's private dir doesn't exist yet
    s = Status::OK();
  }

  auto ret = backups_.insert(std::make_pair(
      new_backup_id, std::unique_ptr<BackupMeta>(new BackupMeta(
                         GetBackupMetaFile(new_backup_id, false /* tmp */),
                         GetBackupMetaFile(new_backup_id, true /* tmp */),
                         &backuped_file_infos_, backup_env_))));
  assert(ret.second == true);
  auto& new_backup = ret.first->second;
  new_backup->RecordTimestamp();
  new_backup->SetAppMetadata(app_metadata);

  auto start_backup = backup_env_->NowMicros();

  ROCKS_LOG_INFO(options_.info_log,
                 "Started the backup process -- creating backup %u",
                 new_backup_id);

  if (options_.share_table_files && !options_.share_files_with_checksum) {
    ROCKS_LOG_WARN(options_.info_log,
                   "BackupEngineOptions::share_files_with_checksum=false is "
                   "DEPRECATED and could lead to data loss.");
  }

  if (s.ok()) {
    s = backup_env_->CreateDir(private_dir);
  }

  RateLimiter* rate_limiter = options_.backup_rate_limiter.get();
  if (rate_limiter) {
    copy_file_buffer_size_ = static_cast<size_t>(rate_limiter->GetSingleBurstBytes());
  }

  // A set into which we will insert the dst_paths that are calculated for live
  // files and live WAL files.
  // This is used to check whether a live files shares a dst_path with another
  // live file.
  std::unordered_set<std::string> live_dst_paths;

  std::vector<BackupAfterCopyOrCreateWorkItem> backup_items_to_finish;
  // Add a CopyOrCreateWorkItem to the channel for each live file
  Status disabled = db->DisableFileDeletions();
  if (s.ok()) {
    CheckpointImpl checkpoint(db);
    uint64_t sequence_number = 0;
    DBOptions db_options = db->GetDBOptions();
    FileChecksumGenFactory* db_checksum_factory =
        db_options.file_checksum_gen_factory.get();
    const std::string kFileChecksumGenFactoryName =
        "FileChecksumGenCrc32cFactory";
    bool compare_checksum =
        db_checksum_factory != nullptr &&
                db_checksum_factory->Name() == kFileChecksumGenFactoryName
            ? true
            : false;
    EnvOptions src_raw_env_options(db_options);
    s = checkpoint.CreateCustomCheckpoint(
        db_options,
        [&](const std::string& /*src_dirname*/, const std::string& /*fname*/,
            FileType) {
          // custom checkpoint will switch to calling copy_file_cb after it sees
          // NotSupported returned from link_file_cb.
          return Status::NotSupported();
        } /* link_file_cb */,
        [&](const std::string& src_dirname, const std::string& fname,
            uint64_t size_limit_bytes, FileType type,
            const std::string& checksum_func_name,
            const std::string& checksum_val) {
          if (type == kWalFile && !options_.backup_log_files) {
            return Status::OK();
          }
          Log(options_.info_log, "add file for backup %s", fname.c_str());
          uint64_t size_bytes = 0;
          Status st;
          if (type == kTableFile || type == kBlobFile) {
            st = db_env_->GetFileSize(src_dirname + fname, &size_bytes);
          }
          EnvOptions src_env_options;
          switch (type) {
            case kWalFile:
              src_env_options =
                  db_env_->OptimizeForLogRead(src_raw_env_options);
              break;
            case kTableFile:
              src_env_options = db_env_->OptimizeForCompactionTableRead(
                  src_raw_env_options, ImmutableDBOptions(db_options));
              break;
            case kDescriptorFile:
              src_env_options =
                  db_env_->OptimizeForManifestRead(src_raw_env_options);
              break;
            case kBlobFile:
              src_env_options = db_env_->OptimizeForBlobFileRead(
                  src_raw_env_options, ImmutableDBOptions(db_options));
              break;
            default:
              // Other backed up files (like options file) are not read by live
              // DB, so don't need to worry about avoiding mixing buffered and
              // direct I/O. Just use plain defaults.
              src_env_options = src_raw_env_options;
              break;
          }
          if (st.ok()) {
            st = AddBackupFileWorkItem(
                live_dst_paths, backup_items_to_finish, new_backup_id,
                options_.share_table_files &&
                    (type == kTableFile || type == kBlobFile),
                src_dirname, fname, src_env_options, rate_limiter, type,
                size_bytes, size_limit_bytes,
                options_.share_files_with_checksum &&
                    (type == kTableFile || type == kBlobFile),
                options.progress_callback, "" /* contents */,
                checksum_func_name, checksum_val);
          }
          return st;
        } /* copy_file_cb */,
        [&](const std::string& fname, const std::string& contents,
            FileType type) {
          Log(options_.info_log, "add file for backup %s", fname.c_str());
          return AddBackupFileWorkItem(
              live_dst_paths, backup_items_to_finish, new_backup_id,
              false /* shared */, "" /* src_dir */, fname,
              EnvOptions() /* src_env_options */, rate_limiter, type,
              contents.size(), 0 /* size_limit */, false /* shared_checksum */,
              options.progress_callback, contents);
        } /* create_file_cb */,
        &sequence_number, options.flush_before_backup ? 0 : port::kMaxUint64,
        compare_checksum);
    if (s.ok()) {
      new_backup->SetSequenceNumber(sequence_number);
    }
  }
  ROCKS_LOG_INFO(options_.info_log, "add files for backup done, wait finish.");
  Status item_status;
  for (auto& item : backup_items_to_finish) {
    item.result.wait();
    auto result = item.result.get();
    item_status = result.status;
    if (item_status.ok() && item.shared && item.needed_to_copy) {
      item_status =
          item.backup_env->RenameFile(item.dst_path_tmp, item.dst_path);
    }
    if (item_status.ok()) {
      item_status = new_backup.get()->AddFile(std::make_shared<FileInfo>(
          item.dst_relative, result.size, result.checksum_hex, result.db_id,
          result.db_session_id));
    }
    if (!item_status.ok()) {
      s = item_status;
    }
  }

  // we copied all the files, enable file deletions
  if (disabled.ok()) {  // If we successfully disabled file deletions
    db->EnableFileDeletions(false).PermitUncheckedError();
  }
  auto backup_time = backup_env_->NowMicros() - start_backup;

  if (s.ok()) {
    // persist the backup metadata on the disk
    s = new_backup->StoreToFile(options_.sync, test_future_options_.get());
  }
  if (s.ok() && options_.sync) {
    std::unique_ptr<Directory> backup_private_directory;
    backup_env_->NewDirectory(
        GetAbsolutePath(GetPrivateFileRel(new_backup_id, false)),
        &backup_private_directory);
    if (backup_private_directory != nullptr) {
      s = backup_private_directory->Fsync();
    }
    if (s.ok() && private_directory_ != nullptr) {
      s = private_directory_->Fsync();
    }
    if (s.ok() && meta_directory_ != nullptr) {
      s = meta_directory_->Fsync();
    }
    if (s.ok() && shared_directory_ != nullptr) {
      s = shared_directory_->Fsync();
    }
    if (s.ok() && backup_directory_ != nullptr) {
      s = backup_directory_->Fsync();
    }
  }

  if (s.ok()) {
    backup_statistics_.IncrementNumberSuccessBackup();
  }
  if (!s.ok()) {
    backup_statistics_.IncrementNumberFailBackup();
    // clean all the files we might have created
    ROCKS_LOG_INFO(options_.info_log, "Backup failed -- %s",
                   s.ToString().c_str());
    ROCKS_LOG_INFO(options_.info_log, "Backup Statistics %s\n",
                   backup_statistics_.ToString().c_str());
    // delete files that we might have already written
    might_need_garbage_collect_ = true;
    DeleteBackup(new_backup_id).PermitUncheckedError();
    return s;
  }

  // here we know that we succeeded and installed the new backup
  // in the LATEST_BACKUP file
  latest_backup_id_ = new_backup_id;
  latest_valid_backup_id_ = new_backup_id;
  if (new_backup_id_ptr) {
    *new_backup_id_ptr = new_backup_id;
  }
  ROCKS_LOG_INFO(options_.info_log, "Backup DONE. All is good");

  // backup_speed is in byte/second
  double backup_speed = new_backup->GetSize() / (1.048576 * backup_time);
  ROCKS_LOG_INFO(options_.info_log, "Backup number of files: %u",
                 new_backup->GetNumberFiles());
  char human_size[16];
  AppendHumanBytes(new_backup->GetSize(), human_size, sizeof(human_size));
  ROCKS_LOG_INFO(options_.info_log, "Backup size: %s", human_size);
  ROCKS_LOG_INFO(options_.info_log, "Backup time: %" PRIu64 " microseconds",
                 backup_time);
  ROCKS_LOG_INFO(options_.info_log, "Backup speed: %.3f MB/s", backup_speed);
  ROCKS_LOG_INFO(options_.info_log, "Backup Statistics %s",
                 backup_statistics_.ToString().c_str());
  return s;
}

Status BackupEngineImpl::PurgeOldBackups(uint32_t num_backups_to_keep) {
  assert(initialized_);
  assert(!read_only_);

  // Best effort deletion even with errors
  Status overall_status = Status::OK();

  ROCKS_LOG_INFO(options_.info_log, "Purging old backups, keeping %u",
                 num_backups_to_keep);
  std::vector<BackupID> to_delete;
  auto itr = backups_.begin();
  while ((backups_.size() - to_delete.size()) > num_backups_to_keep) {
    to_delete.push_back(itr->first);
    itr++;
  }
  for (auto backup_id : to_delete) {
    // Do not GC until end
    auto s = DeleteBackupNoGC(backup_id);
    if (!s.ok()) {
      overall_status = s;
    }
  }
  // Clean up after any incomplete backup deletion, potentially from
  // earlier session.
  if (might_need_garbage_collect_) {
    auto s = GarbageCollect();
    if (!s.ok() && overall_status.ok()) {
      overall_status = s;
    }
  }
  return overall_status;
}

Status BackupEngineImpl::DeleteBackup(BackupID backup_id) {
  auto s1 = DeleteBackupNoGC(backup_id);
  auto s2 = Status::OK();

  // Clean up after any incomplete backup deletion, potentially from
  // earlier session.
  if (might_need_garbage_collect_) {
    s2 = GarbageCollect();
  }

  if (!s1.ok()) {
    // Any failure in the primary objective trumps any failure in the
    // secondary objective.
    s2.PermitUncheckedError();
    return s1;
  } else {
    return s2;
  }
}

// Does not auto-GarbageCollect nor lock
Status BackupEngineImpl::DeleteBackupNoGC(BackupID backup_id) {
  assert(initialized_);
  assert(!read_only_);

  ROCKS_LOG_INFO(options_.info_log, "Deleting backup %u", backup_id);
  auto backup = backups_.find(backup_id);
  if (backup != backups_.end()) {
    auto s = backup->second->Delete();
    if (!s.ok()) {
      return s;
    }
    backups_.erase(backup);
  } else {
    auto corrupt = corrupt_backups_.find(backup_id);
    if (corrupt == corrupt_backups_.end()) {
      return Status::NotFound("Backup not found");
    }
    auto s = corrupt->second.second->Delete();
    if (!s.ok()) {
      return s;
    }
    corrupt->second.first.PermitUncheckedError();
    corrupt_backups_.erase(corrupt);
  }

  // After removing meta file, best effort deletion even with errors.
  // (Don't delete other files if we can't delete the meta file right
  // now.)
  std::vector<std::string> to_delete;
  for (auto& itr : backuped_file_infos_) {
    if (itr.second->refs == 0) {
      Status s = backup_env_->DeleteFile(GetAbsolutePath(itr.first));
      ROCKS_LOG_INFO(options_.info_log, "Deleting %s -- %s", itr.first.c_str(),
                     s.ToString().c_str());
      to_delete.push_back(itr.first);
      if (!s.ok()) {
        // Trying again later might work
        might_need_garbage_collect_ = true;
      }
    }
  }
  for (auto& td : to_delete) {
    backuped_file_infos_.erase(td);
  }

  // take care of private dirs -- GarbageCollect() will take care of them
  // if they are not empty
  std::string private_dir = GetPrivateFileRel(backup_id);
  Status s = backup_env_->DeleteDir(GetAbsolutePath(private_dir));
  ROCKS_LOG_INFO(options_.info_log, "Deleting private dir %s -- %s",
                 private_dir.c_str(), s.ToString().c_str());
  if (!s.ok()) {
    // Full gc or trying again later might work
    might_need_garbage_collect_ = true;
  }
  return Status::OK();
}

void BackupEngineImpl::SetBackupInfoFromBackupMeta(
    BackupID id, const BackupMeta& meta, BackupInfo* backup_info,
    bool include_file_details) const {
  *backup_info = BackupInfo(id, meta.GetTimestamp(), meta.GetSize(),
                            meta.GetNumberFiles(), meta.GetAppMetadata());
  if (include_file_details) {
    auto& file_details = backup_info->file_details;
    file_details.reserve(meta.GetFiles().size());
    for (auto& file_ptr : meta.GetFiles()) {
      BackupFileInfo& finfo = *file_details.emplace(file_details.end());
      finfo.relative_filename = file_ptr->filename;
      finfo.size = file_ptr->size;
    }
    backup_info->name_for_open = GetAbsolutePath(GetPrivateFileRel(id));
    backup_info->name_for_open.pop_back();  // remove trailing '/'
    backup_info->env_for_open = meta.GetEnvForOpen();
  }
}

Status BackupEngineImpl::GetBackupInfo(BackupID backup_id,
                                       BackupInfo* backup_info,
                                       bool include_file_details) const {
  assert(initialized_);
  if (backup_id == kLatestBackupIDMarker) {
    // Note: Read latest_valid_backup_id_ inside of lock
    backup_id = latest_valid_backup_id_;
  }
  auto corrupt_itr = corrupt_backups_.find(backup_id);
  if (corrupt_itr != corrupt_backups_.end()) {
    return Status::Corruption(corrupt_itr->second.first.ToString());
  }
  auto backup_itr = backups_.find(backup_id);
  if (backup_itr == backups_.end()) {
    return Status::NotFound("Backup not found");
  }
  auto& backup = backup_itr->second;
  if (backup->Empty()) {
    return Status::NotFound("Backup not found");
  }

  SetBackupInfoFromBackupMeta(backup_id, *backup, backup_info,
                              include_file_details);
  return Status::OK();
}

void BackupEngineImpl::GetBackupInfo(std::vector<BackupInfo>* backup_info,
                                     bool include_file_details) const {
  assert(initialized_);
  backup_info->resize(backups_.size());
  size_t i = 0;
  for (auto& backup : backups_) {
    const BackupMeta& meta = *backup.second;
    if (!meta.Empty()) {
      SetBackupInfoFromBackupMeta(backup.first, meta, &backup_info->at(i++),
                                  include_file_details);
    }
  }
}

void BackupEngineImpl::GetCorruptedBackups(
    std::vector<BackupID>* corrupt_backup_ids) const {
  assert(initialized_);
  corrupt_backup_ids->reserve(corrupt_backups_.size());
  for (auto& backup : corrupt_backups_) {
    corrupt_backup_ids->push_back(backup.first);
  }
}

Status BackupEngineImpl::RestoreDBFromBackup(const RestoreOptions& options,
                                             BackupID backup_id,
                                             const std::string& db_dir,
                                             const std::string& wal_dir) const {
  assert(initialized_);
  if (backup_id == kLatestBackupIDMarker) {
    // Note: Read latest_valid_backup_id_ inside of lock
    backup_id = latest_valid_backup_id_;
  }
  auto corrupt_itr = corrupt_backups_.find(backup_id);
  if (corrupt_itr != corrupt_backups_.end()) {
    return corrupt_itr->second.first;
  }
  auto backup_itr = backups_.find(backup_id);
  if (backup_itr == backups_.end()) {
    return Status::NotFound("Backup not found");
  }
  auto& backup = backup_itr->second;
  if (backup->Empty()) {
    return Status::NotFound("Backup not found");
  }

  ROCKS_LOG_INFO(options_.info_log, "Restoring backup id %u\n", backup_id);
  ROCKS_LOG_INFO(options_.info_log, "keep_log_files: %d\n",
                 static_cast<int>(options.keep_log_files));

  // just in case. Ignore errors
  db_env_->CreateDirIfMissing(db_dir).PermitUncheckedError();
  db_env_->CreateDirIfMissing(wal_dir).PermitUncheckedError();

  if (options.keep_log_files) {
    // delete files in db_dir, but keep all the log files
    DeleteChildren(db_dir, 1 << kWalFile);
    // move all the files from archive dir to wal_dir
    std::string archive_dir = ArchivalDirectory(wal_dir);
    std::vector<std::string> archive_files;
    db_env_->GetChildren(archive_dir, &archive_files)
        .PermitUncheckedError();  // ignore errors
    for (const auto& f : archive_files) {
      uint64_t number;
      FileType type;
      bool ok = ParseFileName(f, &number, &type);
      if (ok && type == kWalFile) {
        ROCKS_LOG_INFO(options_.info_log,
                       "Moving log file from archive/ to wal_dir: %s",
                       f.c_str());
        Status s =
            db_env_->RenameFile(archive_dir + "/" + f, wal_dir + "/" + f);
        if (!s.ok()) {
          // if we can't move log file from archive_dir to wal_dir,
          // we should fail, since it might mean data loss
          return s;
        }
      }
    }
  } else {
    DeleteChildren(wal_dir);
    DeleteChildren(ArchivalDirectory(wal_dir));
    DeleteChildren(db_dir);
  }

  RateLimiter* rate_limiter = options_.restore_rate_limiter.get();
  if (rate_limiter) {
    copy_file_buffer_size_ =
        static_cast<size_t>(rate_limiter->GetSingleBurstBytes());
  }
  Status s;
  std::vector<RestoreAfterCopyOrCreateWorkItem> restore_items_to_finish;
  for (const auto& file_info : backup->GetFiles()) {
    const std::string& file = file_info->filename;
    // 1. get DB filename
    std::string dst = file_info->GetDbFileName();

    // 2. find the filetype
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(dst, &number, &type);
    if (!ok) {
      return Status::Corruption("Backup corrupted: Fail to parse filename " +
                                dst);
    }
    // 3. Construct the final path
    // kWalFile lives in wal_dir and all the rest live in db_dir
    dst = ((type == kWalFile) ? wal_dir : db_dir) + "/" + dst;

    ROCKS_LOG_INFO(options_.info_log, "Restoring %s to %s\n", file.c_str(),
                   dst.c_str());
    CopyOrCreateWorkItem copy_or_create_work_item(
        GetAbsolutePath(file), dst, "" /* contents */, backup_env_, db_env_,
        EnvOptions() /* src_env_options */, false, rate_limiter,
        0 /* size_limit */);
    RestoreAfterCopyOrCreateWorkItem after_copy_or_create_work_item(
        copy_or_create_work_item.result.get_future(), file, dst,
        file_info->checksum_hex);
    files_to_copy_or_create_.write(std::move(copy_or_create_work_item));
    restore_items_to_finish.push_back(
        std::move(after_copy_or_create_work_item));
  }
  Status item_status;
  for (auto& item : restore_items_to_finish) {
    item.result.wait();
    auto result = item.result.get();
    item_status = result.status;
    // Note: It is possible that both of the following bad-status cases occur
    // during copying. But, we only return one status.
    if (!item_status.ok()) {
      s = item_status;
      break;
    } else if (!item.checksum_hex.empty() &&
               item.checksum_hex != result.checksum_hex) {
      s = Status::Corruption(
          "While restoring " + item.from_file + " -> " + item.to_file +
          ": expected checksum is " + item.checksum_hex +
          " while computed checksum is " + result.checksum_hex);
      break;
    }
  }

  ROCKS_LOG_INFO(options_.info_log, "Restoring done -- %s\n",
                 s.ToString().c_str());
  return s;
}

Status BackupEngineImpl::VerifyBackup(BackupID backup_id,
                                      bool verify_with_checksum) const {
  assert(initialized_);
  // Check if backup_id is corrupted, or valid and registered
  auto corrupt_itr = corrupt_backups_.find(backup_id);
  if (corrupt_itr != corrupt_backups_.end()) {
    return corrupt_itr->second.first;
  }

  auto backup_itr = backups_.find(backup_id);
  if (backup_itr == backups_.end()) {
    return Status::NotFound();
  }

  auto& backup = backup_itr->second;
  if (backup->Empty()) {
    return Status::NotFound();
  }

  ROCKS_LOG_INFO(options_.info_log, "Verifying backup id %u\n", backup_id);

  // Find all existing backup files belong to backup_id
  std::unordered_map<std::string, uint64_t> curr_abs_path_to_size;
  for (const auto& rel_dir : {GetPrivateFileRel(backup_id), GetSharedFileRel(),
                              GetSharedFileWithChecksumRel()}) {
    const auto abs_dir = GetAbsolutePath(rel_dir);
    // Shared directories allowed to be missing in some cases. Expected but
    // missing files will be reported a few lines down.
    ReadChildFileCurrentSizes(abs_dir, backup_env_, &curr_abs_path_to_size)
        .PermitUncheckedError();
  }

  // For all files registered in backup
  for (const auto& file_info : backup->GetFiles()) {
    const auto abs_path = GetAbsolutePath(file_info->filename);
    // check existence of the file
    if (curr_abs_path_to_size.find(abs_path) == curr_abs_path_to_size.end()) {
      return Status::NotFound("File missing: " + abs_path);
    }
    // verify file size
    if (file_info->size != curr_abs_path_to_size[abs_path]) {
      std::string size_info("Expected file size is " +
                            ToString(file_info->size) +
                            " while found file size is " +
                            ToString(curr_abs_path_to_size[abs_path]));
      return Status::Corruption("File corrupted: File size mismatch for " +
                                abs_path + ": " + size_info);
    }
    if (verify_with_checksum && !file_info->checksum_hex.empty()) {
      // verify file checksum
      std::string checksum_hex;
      ROCKS_LOG_INFO(options_.info_log, "Verifying %s checksum...\n",
                     abs_path.c_str());
      Status s = ReadFileAndComputeChecksum(abs_path, backup_env_, EnvOptions(),
                                            0 /* size_limit */, &checksum_hex);
      if (!s.ok()) {
        return s;
      } else if (file_info->checksum_hex != checksum_hex) {
        std::string checksum_info(
            "Expected checksum is " + file_info->checksum_hex +
            " while computed checksum is " + checksum_hex);
        return Status::Corruption("File corrupted: Checksum mismatch for " +
                                  abs_path + ": " + checksum_info);
      }
    }
  }
  return Status::OK();
}

Status BackupEngineImpl::CopyOrCreateFile(
    const std::string& src, const std::string& dst, const std::string& contents,
    Env* src_env, Env* dst_env, const EnvOptions& src_env_options, bool sync,
    RateLimiter* rate_limiter, uint64_t* size, std::string* checksum_hex,
    uint64_t size_limit, std::function<void()> progress_callback) {
  assert(src.empty() != contents.empty());
  Status s;
  std::unique_ptr<FSWritableFile> dst_file;
  std::unique_ptr<FSSequentialFile> src_file;
  FileOptions dst_file_options;
  dst_file_options.use_mmap_writes = false;
  // TODO:(gzh) maybe use direct reads/writes here if possible
  if (size != nullptr) {
    *size = 0;
  }
  uint32_t checksum_value = 0;

  // Check if size limit is set. if not, set it to very big number
  if (size_limit == 0) {
    size_limit = std::numeric_limits<uint64_t>::max();
  }

  s = dst_env->GetFileSystem()->NewWritableFile(dst, dst_file_options,
                                                &dst_file, nullptr);
  if (s.ok() && !src.empty()) {
    s = src_env->GetFileSystem()->NewSequentialFile(
        src, FileOptions(src_env_options), &src_file, nullptr);
  }
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<WritableFileWriter> dest_writer(
      new WritableFileWriter(std::move(dst_file), dst, dst_file_options));
  std::unique_ptr<SequentialFileReader> src_reader;
  std::unique_ptr<char[]> buf;
  if (!src.empty()) {
    src_reader.reset(new SequentialFileReader(std::move(src_file), src));
    buf.reset(new char[copy_file_buffer_size_]);
  }

  Slice data;
  uint64_t processed_buffer_size = 0;
  do {
    if (stop_backup_.load(std::memory_order_acquire)) {
      return Status::Incomplete("Backup stopped");
    }
    if (!src.empty()) {
      size_t buffer_to_read = (copy_file_buffer_size_ < size_limit)
                                  ? copy_file_buffer_size_
                                  : static_cast<size_t>(size_limit);
      s = src_reader->Read(buffer_to_read, &data, buf.get());
      processed_buffer_size += buffer_to_read;
    } else {
      data = contents;
    }
    size_limit -= data.size();
    TEST_SYNC_POINT_CALLBACK(
        "BackupEngineImpl::CopyOrCreateFile:CorruptionDuringBackup",
        (src.length() > 4 && src.rfind(".sst") == src.length() - 4) ? &data
                                                                    : nullptr);

    if (!s.ok()) {
      return s;
    }

    if (size != nullptr) {
      *size += data.size();
    }
    if (checksum_hex != nullptr) {
      checksum_value = crc32c::Extend(checksum_value, data.data(), data.size());
    }
    s = dest_writer->Append(data);
    if (rate_limiter != nullptr) {
      rate_limiter->Request(data.size(), Env::IO_LOW, nullptr /* stats */,
                            RateLimiter::OpType::kWrite);
    }
    if (processed_buffer_size > options_.callback_trigger_interval_size) {
      processed_buffer_size -= options_.callback_trigger_interval_size;
      std::lock_guard<std::mutex> lock(byte_report_mutex_);
      progress_callback();
    }
  } while (s.ok() && contents.empty() && data.size() > 0 && size_limit > 0);

  // Convert uint32_t checksum to hex checksum
  if (checksum_hex != nullptr) {
    checksum_hex->assign(ChecksumInt32ToHex(checksum_value));
  }

  if (s.ok() && sync) {
    s = dest_writer->Sync(false);
  }
  if (s.ok()) {
    s = dest_writer->Close();
  }
  return s;
}

// fname will always start with "/"
Status BackupEngineImpl::AddBackupFileWorkItem(
    std::unordered_set<std::string>& live_dst_paths,
    std::vector<BackupAfterCopyOrCreateWorkItem>& backup_items_to_finish,
    BackupID backup_id, bool shared, const std::string& src_dir,
    const std::string& fname, const EnvOptions& src_env_options,
    RateLimiter* rate_limiter, FileType file_type, uint64_t size_bytes,
    uint64_t size_limit, bool shared_checksum,
    std::function<void()> progress_callback, const std::string& contents,
    const std::string& src_checksum_func_name,
    const std::string& src_checksum_str) {
  assert(!fname.empty() && fname[0] == '/');
  assert(contents.empty() != src_dir.empty());

  std::string dst_relative = fname.substr(1);
  std::string dst_relative_tmp;
  std::string db_id;
  std::string db_session_id;
  // crc32c checksum in hex. empty == unavailable / unknown
  std::string checksum_hex;

  // Whenever a default checksum function name is passed in, we will compares
  // the corresponding checksum values after copying. Note that only table and
  // blob files may have a known checksum function name passed in.
  //
  // If no default checksum function name is passed in and db session id is not
  // available, we will calculate the checksum *before* copying in two cases
  // (we always calcuate checksums when copying or creating for any file types):
  // a) share_files_with_checksum is true and file type is table;
  // b) share_table_files is true and the file exists already.
  //
  // Step 0: Check if default checksum function name is passed in
  if (kDbFileChecksumFuncName == src_checksum_func_name) {
    if (src_checksum_str == kUnknownFileChecksum) {
      return Status::Aborted("Unknown checksum value for " + fname);
    }
    checksum_hex = ChecksumStrToHex(src_checksum_str);
  }

  // Step 1: Prepare the relative path to destination
  if (shared && shared_checksum) {
    if (GetNamingNoFlags() != BackupEngineOptions::kLegacyCrc32cAndFileSize &&
        file_type != kBlobFile) {
      // Prepare db_session_id to add to the file name
      // Ignore the returned status
      // In the failed cases, db_id and db_session_id will be empty
      GetFileDbIdentities(db_env_, src_env_options, src_dir + fname, &db_id,
                          &db_session_id)
          .PermitUncheckedError();
    }
    // Calculate checksum if checksum and db session id are not available.
    // If db session id is available, we will not calculate the checksum
    // since the session id should suffice to avoid file name collision in
    // the shared_checksum directory.
    if (checksum_hex.empty() && db_session_id.empty()) {
      Status s = ReadFileAndComputeChecksum(
          src_dir + fname, db_env_, src_env_options, size_limit, &checksum_hex);
      if (!s.ok()) {
        return s;
      }
    }
    if (size_bytes == port::kMaxUint64) {
      return Status::NotFound("File missing: " + src_dir + fname);
    }
    // dst_relative depends on the following conditions:
    // 1) the naming scheme is kUseDbSessionId,
    // 2) db_session_id is not empty,
    // 3) checksum is available in the DB manifest.
    // If 1,2,3) are satisfied, then dst_relative will be of the form:
    // shared_checksum/<file_number>_<checksum>_<db_session_id>.sst
    // If 1,2) are satisfied, then dst_relative will be of the form:
    // shared_checksum/<file_number>_<db_session_id>.sst
    // Otherwise, dst_relative is of the form
    // shared_checksum/<file_number>_<checksum>_<size>.sst
    //
    // For blob files, db_session_id is not supported with the blob file format.
    // It uses original/legacy naming scheme.
    // dst_relative will be of the form:
    // shared_checksum/<file_number>_<checksum>_<size>.blob
    dst_relative = GetSharedFileWithChecksum(dst_relative, checksum_hex,
                                             size_bytes, db_session_id);
    dst_relative_tmp = GetSharedFileWithChecksumRel(dst_relative, true);
    dst_relative = GetSharedFileWithChecksumRel(dst_relative, false);
  } else if (shared) {
    dst_relative_tmp = GetSharedFileRel(dst_relative, true);
    dst_relative = GetSharedFileRel(dst_relative, false);
  } else {
    dst_relative = GetPrivateFileRel(backup_id, false, dst_relative);
  }

  // We copy into `temp_dest_path` and, once finished, rename it to
  // `final_dest_path`. This allows files to atomically appear at
  // `final_dest_path`. We can copy directly to the final path when atomicity
  // is unnecessary, like for files in private backup directories.
  const std::string* copy_dest_path;
  std::string temp_dest_path;
  std::string final_dest_path = GetAbsolutePath(dst_relative);
  if (!dst_relative_tmp.empty()) {
    temp_dest_path = GetAbsolutePath(dst_relative_tmp);
    copy_dest_path = &temp_dest_path;
  } else {
    copy_dest_path = &final_dest_path;
  }

  // Step 2: Determine whether to copy or not
  // if it's shared, we also need to check if it exists -- if it does, no need
  // to copy it again.
  bool need_to_copy = true;
  // true if final_dest_path is the same path as another live file
  const bool same_path =
      live_dst_paths.find(final_dest_path) != live_dst_paths.end();

  bool file_exists = false;
  if (shared && !same_path) {
    // Should be in shared directory but not a live path, check existence in
    // shared directory
    Status exist = backup_env_->FileExists(final_dest_path);
    if (exist.ok()) {
      file_exists = true;
    } else if (exist.IsNotFound()) {
      file_exists = false;
    } else {
      return exist;
    }
  }

  if (!contents.empty()) {
    need_to_copy = false;
  } else if (shared && (same_path || file_exists)) {
    need_to_copy = false;
    auto find_result = backuped_file_infos_.find(dst_relative);
    if (find_result == backuped_file_infos_.end() && !same_path) {
      // file exists but not referenced
      ROCKS_LOG_INFO(
          options_.info_log,
          "%s already present, but not referenced by any backup. We will "
          "overwrite the file.",
          fname.c_str());
      need_to_copy = true;
      // Defer any failure reporting to when we try to write the file
      backup_env_->DeleteFile(final_dest_path).PermitUncheckedError();
    } else {
      // file exists and referenced
      if (checksum_hex.empty()) {
        // same_path should not happen for a standard DB, so OK to
        // read file contents to check for checksum mismatch between
        // two files from same DB getting same name.
        // For compatibility with future meta file that might not have
        // crc32c checksum available, consider it might be empty, but
        // we don't currently generate meta file without crc32c checksum.
        // Therefore we have to read & compute it if we don't have it.
        if (!same_path && !find_result->second->checksum_hex.empty()) {
          assert(find_result != backuped_file_infos_.end());
          // Note: to save I/O on incremental backups, we copy prior known
          // checksum of the file instead of reading entire file contents
          // to recompute it.
          checksum_hex = find_result->second->checksum_hex;
          // Regarding corruption detection, consider:
          // (a) the DB file is corrupt (since previous backup) and the backup
          // file is OK: we failed to detect, but the backup is safe. DB can
          // be repaired/restored once its corruption is detected.
          // (b) the backup file is corrupt (since previous backup) and the
          // db file is OK: we failed to detect, but the backup is corrupt.
          // CreateNewBackup should support fast incremental backups and
          // there's no way to support that without reading all the files.
          // We might add an option for extra checks on incremental backup,
          // but until then, use VerifyBackups to check existing backup data.
          // (c) file name collision with legitimately different content.
          // This is almost inconceivable with a well-generated DB session
          // ID, but even in that case, we double check the file sizes in
          // BackupMeta::AddFile.
        } else {
          Status s = ReadFileAndComputeChecksum(src_dir + fname, db_env_,
                                                src_env_options, size_limit,
                                                &checksum_hex);
          if (!s.ok()) {
            return s;
          }
        }
      }
      if (!db_session_id.empty()) {
        ROCKS_LOG_INFO(options_.info_log,
                       "%s already present, with checksum %s, size %" PRIu64
                       " and DB session identity %s",
                       fname.c_str(), checksum_hex.c_str(), size_bytes,
                       db_session_id.c_str());
      } else {
        ROCKS_LOG_INFO(options_.info_log,
                       "%s already present, with checksum %s and size %" PRIu64,
                       fname.c_str(), checksum_hex.c_str(), size_bytes);
      }
    }
  }
  live_dst_paths.insert(final_dest_path);

  // Step 3: Add work item
  if (!contents.empty() || need_to_copy) {
    ROCKS_LOG_INFO(options_.info_log, "Copying %s to %s", fname.c_str(),
                   copy_dest_path->c_str());
    CopyOrCreateWorkItem copy_or_create_work_item(
        src_dir.empty() ? "" : src_dir + fname, *copy_dest_path, contents,
        db_env_, backup_env_, src_env_options, options_.sync, rate_limiter,
        size_limit, progress_callback, src_checksum_func_name, checksum_hex,
        db_id, db_session_id);
    BackupAfterCopyOrCreateWorkItem after_copy_or_create_work_item(
        copy_or_create_work_item.result.get_future(), shared, need_to_copy,
        backup_env_, temp_dest_path, final_dest_path, dst_relative);
    files_to_copy_or_create_.write(std::move(copy_or_create_work_item));
    backup_items_to_finish.push_back(std::move(after_copy_or_create_work_item));
  } else {
    std::promise<CopyOrCreateResult> promise_result;
    BackupAfterCopyOrCreateWorkItem after_copy_or_create_work_item(
        promise_result.get_future(), shared, need_to_copy, backup_env_,
        temp_dest_path, final_dest_path, dst_relative);
    backup_items_to_finish.push_back(std::move(after_copy_or_create_work_item));
    CopyOrCreateResult result;
    result.status = Status::OK();
    result.size = size_bytes;
    result.checksum_hex = std::move(checksum_hex);
    result.db_id = std::move(db_id);
    result.db_session_id = std::move(db_session_id);
    promise_result.set_value(std::move(result));
  }
  return Status::OK();
}

Status BackupEngineImpl::ReadFileAndComputeChecksum(
    const std::string& src, Env* src_env, const EnvOptions& src_env_options,
    uint64_t size_limit, std::string* checksum_hex) const {
  if (checksum_hex == nullptr) {
    return Status::Aborted("Checksum pointer is null");
  }
  uint32_t checksum_value = 0;
  if (size_limit == 0) {
    size_limit = std::numeric_limits<uint64_t>::max();
  }

  std::unique_ptr<SequentialFileReader> src_reader;
  Status s = SequentialFileReader::Create(src_env->GetFileSystem(), src,
                                          FileOptions(src_env_options),
                                          &src_reader, nullptr);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<char[]> buf(new char[copy_file_buffer_size_]);
  Slice data;

  do {
    if (stop_backup_.load(std::memory_order_acquire)) {
      return Status::Incomplete("Backup stopped");
    }
    size_t buffer_to_read = (copy_file_buffer_size_ < size_limit) ?
      copy_file_buffer_size_ : static_cast<size_t>(size_limit);
    s = src_reader->Read(buffer_to_read, &data, buf.get());

    if (!s.ok()) {
      return s;
    }

    size_limit -= data.size();
    checksum_value = crc32c::Extend(checksum_value, data.data(), data.size());
  } while (data.size() > 0 && size_limit > 0);

  checksum_hex->assign(ChecksumInt32ToHex(checksum_value));

  return s;
}

Status BackupEngineImpl::GetFileDbIdentities(Env* src_env,
                                             const EnvOptions& src_env_options,
                                             const std::string& file_path,
                                             std::string* db_id,
                                             std::string* db_session_id) {
  assert(db_id != nullptr || db_session_id != nullptr);

  Options options;
  options.env = src_env;
  SstFileDumper sst_reader(options, file_path,
                           2 * 1024 * 1024
                           /* readahead_size */,
                           false /* verify_checksum */, false /* output_hex */,
                           false /* decode_blob_index */, src_env_options,
                           true /* silent */);

  const TableProperties* table_properties = nullptr;
  std::shared_ptr<const TableProperties> tp;
  Status s = sst_reader.getStatus();

  if (s.ok()) {
    // Try to get table properties from the table reader of sst_reader
    if (!sst_reader.ReadTableProperties(&tp).ok()) {
      // Try to use table properites from the initialization of sst_reader
      table_properties = sst_reader.GetInitTableProperties();
    } else {
      table_properties = tp.get();
    }
  } else {
    ROCKS_LOG_INFO(options_.info_log, "Failed to read %s: %s",
                   file_path.c_str(), s.ToString().c_str());
    return s;
  }

  if (table_properties != nullptr) {
    if (db_id != nullptr) {
      db_id->assign(table_properties->db_id);
    }
    if (db_session_id != nullptr) {
      db_session_id->assign(table_properties->db_session_id);
      if (db_session_id->empty()) {
        s = Status::NotFound("DB session identity not found in " + file_path);
        ROCKS_LOG_INFO(options_.info_log, "%s", s.ToString().c_str());
        return s;
      }
    }
    return Status::OK();
  } else {
    s = Status::Corruption("Table properties missing in " + file_path);
    ROCKS_LOG_INFO(options_.info_log, "%s", s.ToString().c_str());
    return s;
  }
}

void BackupEngineImpl::DeleteChildren(const std::string& dir,
                                      uint32_t file_type_filter) const {
  std::vector<std::string> children;
  db_env_->GetChildren(dir, &children).PermitUncheckedError();  // ignore errors

  for (const auto& f : children) {
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(f, &number, &type);
    if (ok && (file_type_filter & (1 << type))) {
      // don't delete this file
      continue;
    }
    db_env_->DeleteFile(dir + "/" + f).PermitUncheckedError();  // ignore errors
  }
}

Status BackupEngineImpl::ReadChildFileCurrentSizes(
    const std::string& dir, Env* env,
    std::unordered_map<std::string, uint64_t>* result) const {
  assert(result != nullptr);
  std::vector<Env::FileAttributes> files_attrs;
  Status status = env->FileExists(dir);
  if (status.ok()) {
    status = env->GetChildrenFileAttributes(dir, &files_attrs);
  } else if (status.IsNotFound()) {
    // Insert no entries can be considered success
    status = Status::OK();
  }
  const bool slash_needed = dir.empty() || dir.back() != '/';
  for (const auto& file_attrs : files_attrs) {
    result->emplace(dir + (slash_needed ? "/" : "") + file_attrs.name,
                    file_attrs.size_bytes);
  }
  return status;
}

Status BackupEngineImpl::GarbageCollect() {
  assert(!read_only_);

  // We will make a best effort to remove all garbage even in the presence
  // of inconsistencies or I/O failures that inhibit finding garbage.
  Status overall_status = Status::OK();
  // If all goes well, we don't need another auto-GC this session
  might_need_garbage_collect_ = false;

  ROCKS_LOG_INFO(options_.info_log, "Starting garbage collection");

  // delete obsolete shared files
  for (bool with_checksum : {false, true}) {
    std::vector<std::string> shared_children;
    {
      std::string shared_path;
      if (with_checksum) {
        shared_path = GetAbsolutePath(GetSharedFileWithChecksumRel());
      } else {
        shared_path = GetAbsolutePath(GetSharedFileRel());
      }
      auto s = backup_env_->FileExists(shared_path);
      if (s.ok()) {
        s = backup_env_->GetChildren(shared_path, &shared_children);
      } else if (s.IsNotFound()) {
        s = Status::OK();
      }
      if (!s.ok()) {
        overall_status = s;
        // Trying again later might work
        might_need_garbage_collect_ = true;
      }
    }
    for (auto& child : shared_children) {
      std::string rel_fname;
      if (with_checksum) {
        rel_fname = GetSharedFileWithChecksumRel(child);
      } else {
        rel_fname = GetSharedFileRel(child);
      }
      auto child_itr = backuped_file_infos_.find(rel_fname);
      // if it's not refcounted, delete it
      if (child_itr == backuped_file_infos_.end() ||
          child_itr->second->refs == 0) {
        // this might be a directory, but DeleteFile will just fail in that
        // case, so we're good
        Status s = backup_env_->DeleteFile(GetAbsolutePath(rel_fname));
        ROCKS_LOG_INFO(options_.info_log, "Deleting %s -- %s",
                       rel_fname.c_str(), s.ToString().c_str());
        backuped_file_infos_.erase(rel_fname);
        if (!s.ok()) {
          // Trying again later might work
          might_need_garbage_collect_ = true;
        }
      }
    }
  }

  // delete obsolete private files
  std::vector<std::string> private_children;
  {
    auto s = backup_env_->GetChildren(GetAbsolutePath(kPrivateDirName),
                                      &private_children);
    if (!s.ok()) {
      overall_status = s;
      // Trying again later might work
      might_need_garbage_collect_ = true;
    }
  }
  for (auto& child : private_children) {
    BackupID backup_id = 0;
    bool tmp_dir = child.find(".tmp") != std::string::npos;
    sscanf(child.c_str(), "%u", &backup_id);
    if (!tmp_dir &&  // if it's tmp_dir, delete it
        (backup_id == 0 || backups_.find(backup_id) != backups_.end())) {
      // it's either not a number or it's still alive. continue
      continue;
    }
    // here we have to delete the dir and all its children
    std::string full_private_path =
        GetAbsolutePath(GetPrivateFileRel(backup_id));
    std::vector<std::string> subchildren;
    if (backup_env_->GetChildren(full_private_path, &subchildren).ok()) {
      for (auto& subchild : subchildren) {
        Status s = backup_env_->DeleteFile(full_private_path + subchild);
        ROCKS_LOG_INFO(options_.info_log, "Deleting %s -- %s",
                       (full_private_path + subchild).c_str(),
                       s.ToString().c_str());
        if (!s.ok()) {
          // Trying again later might work
          might_need_garbage_collect_ = true;
        }
      }
    }
    // finally delete the private dir
    Status s = backup_env_->DeleteDir(full_private_path);
    ROCKS_LOG_INFO(options_.info_log, "Deleting dir %s -- %s",
                   full_private_path.c_str(), s.ToString().c_str());
    if (!s.ok()) {
      // Trying again later might work
      might_need_garbage_collect_ = true;
    }
  }

  assert(overall_status.ok() || might_need_garbage_collect_);
  return overall_status;
}

// ------- BackupMeta class --------

Status BackupEngineImpl::BackupMeta::AddFile(
    std::shared_ptr<FileInfo> file_info) {
  auto itr = file_infos_->find(file_info->filename);
  if (itr == file_infos_->end()) {
    auto ret = file_infos_->insert({file_info->filename, file_info});
    if (ret.second) {
      itr = ret.first;
      itr->second->refs = 1;
    } else {
      // if this happens, something is seriously wrong
      return Status::Corruption("In memory metadata insertion error");
    }
  } else {
    // Compare sizes, because we scanned that off the filesystem on both
    // ends. This is like a check in VerifyBackup.
    if (itr->second->size != file_info->size) {
      std::string msg = "Size mismatch for existing backup file: ";
      msg.append(file_info->filename);
      msg.append(" Size in backup is " + ToString(itr->second->size) +
                 " while size in DB is " + ToString(file_info->size));
      msg.append(
          " If this DB file checks as not corrupt, try deleting old"
          " backups or backing up to a different backup directory.");
      return Status::Corruption(msg);
    }
    if (file_info->checksum_hex.empty()) {
      // No checksum available to check
    } else if (itr->second->checksum_hex.empty()) {
      // Remember checksum if newly acquired
      itr->second->checksum_hex = file_info->checksum_hex;
    } else if (itr->second->checksum_hex != file_info->checksum_hex) {
      // Note: to save I/O, these will be equal trivially on already backed
      // up files that don't have the checksum in their name. And it should
      // never fail for files that do have checksum in their name.

      // Should never reach here, but produce an appropriate corruption
      // message in case we do in a release build.
      assert(false);
      std::string msg = "Checksum mismatch for existing backup file: ";
      msg.append(file_info->filename);
      msg.append(" Expected checksum is " + itr->second->checksum_hex +
                 " while computed checksum is " + file_info->checksum_hex);
      msg.append(
          " If this DB file checks as not corrupt, try deleting old"
          " backups or backing up to a different backup directory.");
      return Status::Corruption(msg);
    }
    ++itr->second->refs;  // increase refcount if already present
  }

  size_ += file_info->size;
  files_.push_back(itr->second);

  return Status::OK();
}

Status BackupEngineImpl::BackupMeta::Delete(bool delete_meta) {
  Status s;
  for (const auto& file : files_) {
    --file->refs;  // decrease refcount
  }
  files_.clear();
  // delete meta file
  if (delete_meta) {
    s = env_->FileExists(meta_filename_);
    if (s.ok()) {
      s = env_->DeleteFile(meta_filename_);
    } else if (s.IsNotFound()) {
      s = Status::OK();  // nothing to delete
    }
  }
  timestamp_ = 0;
  return s;
}

// Constants for backup meta file schema (see LoadFromFile)
namespace {

const std::string kSchemaVersionPrefix{"schema_version "};
const std::string kFooterMarker{"// FOOTER"};

const std::string kAppMetaDataFieldName{"metadata"};

// WART: The checksums are crc32c but named "crc32"
const std::string kFileCrc32cFieldName{"crc32"};
const std::string kFileSizeFieldName{"size"};

// Marks a (future) field that should cause failure if not recognized.
// Other fields are assumed to be ignorable. For example, in the future
// we might add
//  ni::file_name_escape uri_percent
// to indicate all file names have had spaces and special characters
// escaped using a URI percent encoding.
const std::string kNonIgnorableFieldPrefix{"ni::"};
}  // namespace

// Each backup meta file is of the format (schema version 1):
//----------------------------------------------------------
// <timestamp>
// <seq number>
// metadata <metadata> (optional)
// <number of files>
// <file1> crc32 <crc32c_as_unsigned_decimal>
// <file2> crc32 <crc32c_as_unsigned_decimal>
// ...
//----------------------------------------------------------
//
// For schema version 2.x (not in public APIs, but
// forward-compatibility started):
//----------------------------------------------------------
// schema_version <ver>
// <timestamp>
// <seq number>
// [<field name> <field data>]
// ...
// <number of files>
// <file1>( <field name> <field data no spaces>)*
// <file2>( <field name> <field data no spaces>)*
// ...
// [// FOOTER]
// [<field name> <field data>]
// ...
//----------------------------------------------------------
// where
// <ver> ::= [0-9]+([.][0-9]+)
// <field name> ::= [A-Za-z_][A-Za-z_0-9.]+
// <field data> is anything but newline
// <field data no spaces> is anything but space and newline
// Although "// FOOTER" wouldn't strictly be required as a delimiter
// given the number of files is included, it is there for parsing
// sanity in case of corruption. It is only required if followed
// by footer fields, such as a checksum of the meta file (so far).
// Unrecognized fields are ignored, to support schema evolution on
// non-critical features with forward compatibility. Update schema
// major version for breaking changes. Schema minor versions are indicated
// only for diagnostic/debugging purposes.
//
// Fields in schema version 2.0:
// * Top-level meta fields:
//   * Only "metadata" as in schema version 1
// * File meta fields:
//   * "crc32" - a crc32c checksum as in schema version 1
//   * "size" - the size of the file (new)
// * Footer meta fields:
//   * None yet (future use for meta file checksum anticipated)
//
Status BackupEngineImpl::BackupMeta::LoadFromFile(
    const std::string& backup_dir,
    const std::unordered_map<std::string, uint64_t>& abs_path_to_size,
    Logger* info_log,
    std::unordered_set<std::string>* reported_ignored_fields) {
  assert(reported_ignored_fields);
  assert(Empty());

  std::unique_ptr<LineFileReader> backup_meta_reader;
  {
    Status s =
        LineFileReader::Create(env_->GetFileSystem(), meta_filename_,
                               FileOptions(), &backup_meta_reader, nullptr);
    if (!s.ok()) {
      return s;
    }
  }

  // If we don't read an explicit schema_version, that implies version 1,
  // which is what we call the original backup meta schema.
  int schema_major_version = 1;

  // Failures handled at the end
  std::string line;
  if (backup_meta_reader->ReadLine(&line)) {
    if (StartsWith(line, kSchemaVersionPrefix)) {
      std::string ver = line.substr(kSchemaVersionPrefix.size());
      if (ver == "2" || StartsWith(ver, "2.")) {
        schema_major_version = 2;
      } else {
        return Status::NotSupported(
            "Unsupported/unrecognized schema version: " + ver);
      }
      line.clear();
    } else if (line.empty()) {
      return Status::Corruption("Unexpected empty line");
    }
  }
  if (!line.empty() || backup_meta_reader->ReadLine(&line)) {
    timestamp_ = std::strtoull(line.c_str(), nullptr, /*base*/ 10);
  }
  if (backup_meta_reader->ReadLine(&line)) {
    sequence_number_ = std::strtoull(line.c_str(), nullptr, /*base*/ 10);
  }
  uint32_t num_files = UINT32_MAX;
  while (backup_meta_reader->ReadLine(&line)) {
    if (line.empty()) {
      return Status::Corruption("Unexpected empty line");
    }
    // Number -> number of files -> exit loop reading optional meta fields
    if (line[0] >= '0' && line[0] <= '9') {
      num_files = static_cast<uint32_t>(strtoul(line.c_str(), nullptr, 10));
      break;
    }
    // else, must be a meta field assignment
    auto space_pos = line.find_first_of(' ');
    if (space_pos == std::string::npos) {
      return Status::Corruption("Expected number of files or meta field");
    }
    std::string field_name = line.substr(0, space_pos);
    std::string field_data = line.substr(space_pos + 1);
    if (field_name == kAppMetaDataFieldName) {
      // app metadata present
      bool decode_success = Slice(field_data).DecodeHex(&app_metadata_);
      if (!decode_success) {
        return Status::Corruption(
            "Failed to decode stored hex encoded app metadata");
      }
    } else if (schema_major_version < 2) {
      return Status::Corruption("Expected number of files or \"" +
                                kAppMetaDataFieldName + "\" field");
    } else if (StartsWith(field_name, kNonIgnorableFieldPrefix)) {
      return Status::NotSupported("Unrecognized non-ignorable meta field " +
                                  field_name + " (from future version?)");
    } else {
      // Warn the first time we see any particular unrecognized meta field
      if (reported_ignored_fields->insert("meta:" + field_name).second) {
        ROCKS_LOG_WARN(info_log, "Ignoring unrecognized backup meta field %s",
                       field_name.c_str());
      }
    }
  }
  std::vector<std::shared_ptr<FileInfo>> files;
  bool footer_present = false;
  while (backup_meta_reader->ReadLine(&line)) {
    std::vector<std::string> components = StringSplit(line, ' ');

    if (components.size() < 1) {
      return Status::Corruption("Empty line instead of file entry.");
    }
    if (schema_major_version >= 2 && components.size() == 2 &&
        line == kFooterMarker) {
      footer_present = true;
      break;
    }

    const std::string& filename = components[0];

    uint64_t actual_size;
    const std::shared_ptr<FileInfo> file_info = GetFile(filename);
    if (file_info) {
      actual_size = file_info->size;
    } else {
      std::string abs_path = backup_dir + "/" + filename;
      auto e = abs_path_to_size.find(abs_path);
      if (e == abs_path_to_size.end()) {
        return Status::Corruption("Pathname in meta file not found on disk: " +
                                  abs_path);
      }
      actual_size = e->second;
    }

    if (schema_major_version >= 2) {
      if (components.size() % 2 != 1) {
        return Status::Corruption(
            "Bad number of line components for file entry.");
      }
    } else {
      // Check restricted original schema
      if (components.size() < 3) {
        return Status::Corruption("File checksum is missing for " + filename +
                                  " in " + meta_filename_);
      }
      if (components[1] != kFileCrc32cFieldName) {
        return Status::Corruption("Unknown checksum type for " + filename +
                                  " in " + meta_filename_);
      }
      if (components.size() > 3) {
        return Status::Corruption("Extra data for entry " + filename + " in " +
                                  meta_filename_);
      }
    }

    std::string checksum_hex;
    for (unsigned i = 1; i < components.size(); i += 2) {
      const std::string& field_name = components[i];
      const std::string& field_data = components[i + 1];

      if (field_name == kFileCrc32cFieldName) {
        uint32_t checksum_value =
            static_cast<uint32_t>(strtoul(field_data.c_str(), nullptr, 10));
        if (field_data != ROCKSDB_NAMESPACE::ToString(checksum_value)) {
          return Status::Corruption("Invalid checksum value for " + filename +
                                    " in " + meta_filename_);
        }
        checksum_hex = ChecksumInt32ToHex(checksum_value);
      } else if (field_name == kFileSizeFieldName) {
        uint64_t ex_size =
            std::strtoull(field_data.c_str(), nullptr, /*base*/ 10);
        if (ex_size != actual_size) {
          return Status::Corruption("For file " + filename + " expected size " +
                                    ToString(ex_size) + " but found size" +
                                    ToString(actual_size));
        }
      } else if (StartsWith(field_name, kNonIgnorableFieldPrefix)) {
        return Status::NotSupported("Unrecognized non-ignorable file field " +
                                    field_name + " (from future version?)");
      } else {
        // Warn the first time we see any particular unrecognized file field
        if (reported_ignored_fields->insert("file:" + field_name).second) {
          ROCKS_LOG_WARN(info_log, "Ignoring unrecognized backup file field %s",
                         field_name.c_str());
        }
      }
    }

    files.emplace_back(new FileInfo(filename, actual_size, checksum_hex));
  }

  if (footer_present) {
    assert(schema_major_version >= 2);
    while (backup_meta_reader->ReadLine(&line)) {
      if (line.empty()) {
        return Status::Corruption("Unexpected empty line");
      }
      auto space_pos = line.find_first_of(' ');
      if (space_pos == std::string::npos) {
        return Status::Corruption("Expected footer field");
      }
      std::string field_name = line.substr(0, space_pos);
      std::string field_data = line.substr(space_pos + 1);
      if (StartsWith(field_name, kNonIgnorableFieldPrefix)) {
        return Status::NotSupported("Unrecognized non-ignorable field " +
                                    field_name + " (from future version?)");
      } else if (reported_ignored_fields->insert("footer:" + field_name)
                     .second) {
        // Warn the first time we see any particular unrecognized footer field
        ROCKS_LOG_WARN(info_log,
                       "Ignoring unrecognized backup meta footer field %s",
                       field_name.c_str());
      }
    }
  }

  {
    Status s = backup_meta_reader->GetStatus();
    if (!s.ok()) {
      return s;
    }
  }

  if (num_files != files.size()) {
    return Status::Corruption(
        "Inconsistent number of files or missing/incomplete header in " +
        meta_filename_);
  }

  files_.reserve(files.size());
  for (const auto& file_info : files) {
    Status s = AddFile(file_info);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status BackupEngineImpl::BackupMeta::StoreToFile(
    bool sync, const TEST_FutureSchemaVersion2Options* test_future_options) {
  Status s;
  std::unique_ptr<WritableFile> backup_meta_file;
  EnvOptions env_options;
  env_options.use_mmap_writes = false;
  env_options.use_direct_writes = false;
  s = env_->NewWritableFile(meta_tmp_filename_, &backup_meta_file, env_options);
  if (!s.ok()) {
    return s;
  }

  std::ostringstream buf;
  if (test_future_options) {
    buf << kSchemaVersionPrefix << test_future_options->version << "\n";
  }
  buf << static_cast<unsigned long long>(timestamp_) << "\n";
  buf << sequence_number_ << "\n";

  if (!app_metadata_.empty()) {
    std::string hex_encoded_metadata =
        Slice(app_metadata_).ToString(/* hex */ true);
    buf << kAppMetaDataFieldName << " " << hex_encoded_metadata << "\n";
  }
  if (test_future_options) {
    for (auto& e : test_future_options->meta_fields) {
      buf << e.first << " " << e.second << "\n";
    }
  }
  buf << files_.size() << "\n";

  for (const auto& file : files_) {
    buf << file->filename;
    if (test_future_options == nullptr ||
        test_future_options->crc32c_checksums) {
      // use crc32c for now, switch to something else if needed
      buf << " " << kFileCrc32cFieldName << " "
          << ChecksumHexToInt32(file->checksum_hex);
    }
    if (test_future_options && test_future_options->file_sizes) {
      buf << " " << kFileSizeFieldName << " " << ToString(file->size);
    }
    if (test_future_options) {
      for (auto& e : test_future_options->file_fields) {
        buf << " " << e.first << " " << e.second;
      }
    }
    buf << "\n";
  }

  if (test_future_options && !test_future_options->footer_fields.empty()) {
    buf << kFooterMarker << "\n";
    for (auto& e : test_future_options->footer_fields) {
      buf << e.first << " " << e.second << "\n";
    }
  }

  s = backup_meta_file->Append(Slice(buf.str()));
  if (s.ok() && sync) {
    s = backup_meta_file->Sync();
  }
  if (s.ok()) {
    s = backup_meta_file->Close();
  }
  if (s.ok()) {
    s = env_->RenameFile(meta_tmp_filename_, meta_filename_);
  }
  return s;
}

Status BackupEngineReadOnly::Open(const BackupEngineOptions& options, Env* env,
                                  BackupEngineReadOnly** backup_engine_ptr) {
  if (options.destroy_old_data) {
    return Status::InvalidArgument(
        "Can't destroy old data with ReadOnly BackupEngine");
  }
  std::unique_ptr<BackupEngineImplThreadSafe> backup_engine(
      new BackupEngineImplThreadSafe(options, env, true /*read_only*/));
  auto s = backup_engine->Initialize();
  if (!s.ok()) {
    *backup_engine_ptr = nullptr;
    return s;
  }
  *backup_engine_ptr = backup_engine.release();
  return Status::OK();
}

void TEST_EnableWriteFutureSchemaVersion2(
    BackupEngine* engine, const TEST_FutureSchemaVersion2Options& options) {
  BackupEngineImplThreadSafe* impl =
      static_cast_with_check<BackupEngineImplThreadSafe>(engine);
  impl->TEST_EnableWriteFutureSchemaVersion2(options);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
