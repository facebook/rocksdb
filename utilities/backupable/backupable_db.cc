//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/backupable_db.h"

#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <cinttypes>
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

#include "db/log_reader.h"
#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "port/port.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/transaction_log.h"
#include "table/sst_file_dumper.h"
#include "test_util/sync_point.h"
#include "util/channel.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_checksum_helper.h"
#include "util/string_util.h"
#include "utilities/checkpoint/checkpoint_impl.h"

namespace ROCKSDB_NAMESPACE {

namespace {
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
// Checks if the checksum function names are the same. Note that both the
// backup default checksum function and the db standard checksum function are
// crc32c although they have different names. So We treat the db standard
// checksum function name and the backup default checksum function name as
// the same name.
inline bool IsSameChecksumFunc(const std::string& dst_checksum_func_name,
                               const std::string& src_checksum_func_name) {
  return (dst_checksum_func_name == src_checksum_func_name) ||
         ((dst_checksum_func_name == kDefaultBackupFileChecksumFuncName) &&
          (src_checksum_func_name == kStandardDbFileChecksumFuncName)) ||
         ((src_checksum_func_name == kDefaultBackupFileChecksumFuncName) &&
          (dst_checksum_func_name == kStandardDbFileChecksumFuncName));
}
inline bool IsSstFile(const std::string& fname) {
  return fname.length() > 4 && fname.rfind(".sst") == fname.length() - 4;
}
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

void BackupableDBOptions::Dump(Logger* logger) const {
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
class BackupEngineImpl : public BackupEngine {
 public:
  BackupEngineImpl(const BackupableDBOptions& options, Env* db_env,
                   bool read_only = false);
  ~BackupEngineImpl() override;

  using BackupEngine::CreateNewBackupWithMetadata;
  Status CreateNewBackupWithMetadata(const CreateBackupOptions& options, DB* db,
                                     const std::string& app_metadata) override;

  Status PurgeOldBackups(uint32_t num_backups_to_keep) override;

  Status DeleteBackup(BackupID backup_id) override;

  void StopBackup() override {
    stop_backup_.store(true, std::memory_order_release);
  }

  Status GarbageCollect() override;

  // The returned BackupInfos are in chronological order, which means the
  // latest backup comes last.
  void GetBackupInfo(std::vector<BackupInfo>* backup_info) override;

  void GetCorruptedBackups(std::vector<BackupID>* corrupt_backup_ids) override;

  using BackupEngine::RestoreDBFromBackup;
  Status RestoreDBFromBackup(const RestoreOptions& options, BackupID backup_id,
                             const std::string& db_dir,
                             const std::string& wal_dir) override;

  using BackupEngine::RestoreDBFromLatestBackup;
  Status RestoreDBFromLatestBackup(const RestoreOptions& options,
                                   const std::string& db_dir,
                                   const std::string& wal_dir) override {
    return RestoreDBFromBackup(options, latest_valid_backup_id_, db_dir,
                               wal_dir);
  }

  Status VerifyBackup(BackupID backup_id,
                      bool verify_with_checksum = false) override;

  Status Initialize();

  // Obtain the naming option for backup table files
  BackupTableNameOption GetTableNamingOption() const {
    return options_.share_files_with_checksum_naming;
  }

 private:
  void DeleteChildren(const std::string& dir, uint32_t file_type_filter = 0);
  Status DeleteBackupInternal(BackupID backup_id);

  // Extends the "result" map with pathname->size mappings for the contents of
  // "dir" in "env". Pathnames are prefixed with "dir".
  Status InsertPathnameToSizeBytes(
      const std::string& dir, Env* env,
      std::unordered_map<std::string, uint64_t>* result);

  struct FileInfo {
    FileInfo(const std::string& fname, uint64_t sz, const std::string& checksum,
             const std::string& custom_checksum,
             const std::string& checksum_name, const std::string& id = "",
             const std::string& sid = "")
        : refs(0),
          filename(fname),
          size(sz),
          checksum_hex(checksum),
          custom_checksum_hex(custom_checksum),
          checksum_func_name(checksum_name),
          db_id(id),
          db_session_id(sid) {}

    FileInfo(const FileInfo&) = delete;
    FileInfo& operator=(const FileInfo&) = delete;

    int refs;
    const std::string filename;
    const uint64_t size;
    const std::string checksum_hex;
    const std::string custom_checksum_hex;
    const std::string checksum_func_name;
    // DB identities
    // db_id is obtained for potential usage in the future but not used
    // currently
    const std::string db_id;
    // db_session_id appears in the backup SST filename if the table naming
    // option is kOptionalChecksumAndDbSessionId
    const std::string db_session_id;
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
      env_->GetCurrentTime(&timestamp_);
    }
    int64_t GetTimestamp() const {
      return timestamp_;
    }
    uint64_t GetSize() const {
      return size_;
    }
    uint32_t GetNumberFiles() { return static_cast<uint32_t>(files_.size()); }
    void SetSequenceNumber(uint64_t sequence_number) {
      sequence_number_ = sequence_number;
    }
    uint64_t GetSequenceNumber() {
      return sequence_number_;
    }

    const std::string& GetAppMetadata() const { return app_metadata_; }

    void SetAppMetadata(const std::string& app_metadata) {
      app_metadata_ = app_metadata;
    }

    Status AddFile(std::shared_ptr<FileInfo> file_info);

    Status Delete(bool delete_meta = true);

    bool Empty() {
      return files_.empty();
    }

    std::shared_ptr<FileInfo> GetFile(const std::string& filename) const {
      auto it = file_infos_->find(filename);
      if (it == file_infos_->end())
        return nullptr;
      return it->second;
    }

    const std::vector<std::shared_ptr<FileInfo>>& GetFiles() {
      return files_;
    }

    // @param abs_path_to_size Pre-fetched file sizes (bytes).
    Status LoadFromFile(
        const std::string& backup_dir,
        const std::unordered_map<std::string, uint64_t>& abs_path_to_size);
    Status StoreToFile(bool sync);

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

    static const size_t max_backup_meta_file_size_ = 10 * 1024 * 1024;  // 10MB
  };  // BackupMeta

  inline std::string GetAbsolutePath(
      const std::string &relative_path = "") const {
    assert(relative_path.size() == 0 || relative_path[0] != '/');
    return options_.backup_dir + "/" + relative_path;
  }
  inline std::string GetPrivateDirRel() const {
    return "private";
  }
  inline std::string GetSharedDirRel() const { return "shared"; }
  inline std::string GetSharedChecksumDirRel() const {
    return "shared_checksum";
  }
  inline std::string GetPrivateFileRel(BackupID backup_id,
                                       bool tmp = false,
                                       const std::string& file = "") const {
    assert(file.size() == 0 || file[0] != '/');
    return GetPrivateDirRel() + "/" + ROCKSDB_NAMESPACE::ToString(backup_id) +
           (tmp ? ".tmp" : "") + "/" + file;
  }
  inline std::string GetSharedFileRel(const std::string& file = "",
                                      bool tmp = false) const {
    assert(file.size() == 0 || file[0] != '/');
    return GetSharedDirRel() + "/" + (tmp ? "." : "") + file +
           (tmp ? ".tmp" : "");
  }
  inline std::string GetSharedFileWithChecksumRel(const std::string& file = "",
                                                  bool tmp = false) const {
    assert(file.size() == 0 || file[0] != '/');
    return GetSharedChecksumDirRel() + "/" + (tmp ? "." : "") + file +
           (tmp ? ".tmp" : "");
  }
  inline bool UseSessionId(const std::string& sid) const {
    return GetTableNamingOption() == kOptionalChecksumAndDbSessionId &&
           !sid.empty();
  }
  inline std::string GetSharedFileWithChecksum(
      const std::string& file, bool has_checksum,
      const std::string& checksum_hex, const uint64_t file_size,
      const std::string& db_session_id) const {
    assert(file.size() == 0 || file[0] != '/');
    std::string file_copy = file;
    if (UseSessionId(db_session_id)) {
      if (has_checksum) {
        return file_copy.insert(file_copy.find_last_of('.'),
                                "_" + checksum_hex + "_" + db_session_id);
      } else {
        return file_copy.insert(file_copy.find_last_of('.'),
                                "_" + db_session_id);
      }
    } else {
      return file_copy.insert(file_copy.find_last_of('.'),
                              "_" + ToString(ChecksumHexToInt32(checksum_hex)) +
                                  "_" + ToString(file_size));
    }
  }
  inline std::string GetFileFromChecksumFile(const std::string& file) const {
    assert(file.size() == 0 || file[0] != '/');
    std::string file_copy = file;
    size_t first_underscore = file_copy.find_first_of('_');
    return file_copy.erase(first_underscore,
                           file_copy.find_last_of('.') - first_underscore);
  }
  inline std::string GetBackupMetaDir() const {
    return GetAbsolutePath("meta");
  }
  inline std::string GetBackupMetaFile(BackupID backup_id, bool tmp) const {
    return GetBackupMetaDir() + "/" + (tmp ? "." : "") +
           ROCKSDB_NAMESPACE::ToString(backup_id) + (tmp ? ".tmp" : "");
  }
  inline Status GetFileNameInfo(const std::string& file,
                                std::string& local_name, uint64_t& number,
                                FileType& type) const {
    // 1. extract the filename
    size_t last_slash = file.find_last_of('/');
    // file will either be shared/<file>, shared_checksum/<file_crc32c_size>,
    // shared_checksum/<file_session>, shared_checksum/<file_crc32c_session>,
    // or private/<number>/<file>
    assert(last_slash != std::string::npos);
    local_name = file.substr(last_slash + 1);

    // if the file was in shared_checksum, extract the real file name
    // in this case the file is <number>_<checksum>_<size>.<type>,
    // <number>_<session>.<type>, or <number>_<checksum>_<session>.<type>
    if (file.substr(0, last_slash) == GetSharedChecksumDirRel()) {
      local_name = GetFileFromChecksumFile(local_name);
    }

    // 2. find the filetype
    bool ok = ParseFileName(local_name, &number, &type);
    if (!ok) {
      return Status::Corruption("Backup corrupted: Fail to parse filename " +
                                local_name);
    }
    return Status::OK();
  }
  inline bool HasCustomChecksumGenFactory() const {
    return options_.file_checksum_gen_factory != nullptr;
  }
  // Returns nullptr if file_checksum_gen_factory is not set or
  // file_checksum_gen_factory is not able to create a generator with
  // name being requested_checksum_func_name
  inline std::unique_ptr<FileChecksumGenerator> GetCustomChecksumGenerator(
      const std::string& requested_checksum_func_name = "") const {
    std::shared_ptr<FileChecksumGenFactory> checksum_factory =
        options_.file_checksum_gen_factory;
    if (checksum_factory == nullptr) {
      return nullptr;
    } else {
      FileChecksumGenContext gen_context;
      gen_context.requested_checksum_func_name = requested_checksum_func_name;
      return checksum_factory->CreateFileChecksumGenerator(gen_context);
    }
  }
  // Set the checksum generator by the requested checksum function name
  inline Status SetChecksumGenerator(
      const std::string& requested_checksum_func_name,
      std::unique_ptr<FileChecksumGenerator>& checksum_func) {
    if (requested_checksum_func_name != kDefaultBackupFileChecksumFuncName) {
      if (!HasCustomChecksumGenFactory()) {
        // No custom checksum factory indicates users would like to use the
        // backup default checksum function and accept the degraded data
        // integrity checking
        return Status::OK();
      } else {
        checksum_func =
            GetCustomChecksumGenerator(requested_checksum_func_name);
        // we will use the default backup checksum function if the custom
        // checksum functions is the db standard checksum function but is not
        // found in the checksum factory passed in; otherwise, we return
        // Status::InvalidArgument()
        if (checksum_func == nullptr &&
            requested_checksum_func_name != kStandardDbFileChecksumFuncName) {
          return Status::InvalidArgument("Checksum checksum function " +
                                         requested_checksum_func_name +
                                         " not found");
        }
      }
    }
    // The requested checksum function is the default backup checksum function
    return Status::OK();
  }

  // If size_limit == 0, there is no size limit, copy everything.
  //
  // Exactly one of src and contents must be non-empty.
  //
  // @param src If non-empty, the file is copied from this pathname.
  // @param contents If non-empty, the file will be created with these contents.
  Status CopyOrCreateFile(
      const std::string& src, const std::string& dst,
      const std::string& contents, Env* src_env, Env* dst_env,
      const EnvOptions& src_env_options, bool sync, RateLimiter* rate_limiter,
      const std::string& backup_checksum_func_name, uint64_t* size = nullptr,
      std::string* checksum_hex = nullptr,
      std::string* custom_checksum_hex = nullptr, uint64_t size_limit = 0,
      std::function<void()> progress_callback = []() {});

  Status CalculateChecksum(
      const std::string& src, Env* src_env, const EnvOptions& src_env_options,
      uint64_t size_limit, std::string* checksum_hex,
      const std::unique_ptr<FileChecksumGenerator>& checksum_func = nullptr,
      std::string* custom_checksum_hex = nullptr);

  // Obtain db_id and db_session_id from the table properties of file_path
  Status GetFileDbIdentities(Env* src_env, const EnvOptions& src_env_options,
                             const std::string& file_path, std::string* db_id,
                             std::string* db_session_id);

  Status GetFileChecksumsFromManifestInBackup(Env* src_env,
                                              const BackupID& backup_id,
                                              const BackupMeta* backup,
                                              FileChecksumList* checksum_list);

  Status VerifyFileWithCrc32c(Env* src_env, const BackupMeta* backup,
                              const std::string& rel_path);

  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t /*bytes*/, const Status& s) override {
      if (status->ok()) {
        *status = s;
      }
    }
  };

  struct CopyOrCreateResult {
    uint64_t size;
    std::string checksum_hex;
    std::string custom_checksum_hex;
    std::string checksum_func_name;
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
    bool verify_checksum_after_work;
    std::string src_checksum_func_name;
    std::string src_checksum_hex;
    std::string backup_checksum_func_name;
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
          verify_checksum_after_work(false),
          src_checksum_func_name(kUnknownFileChecksumFuncName),
          src_checksum_hex(""),
          backup_checksum_func_name(kUnknownFileChecksumFuncName),
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
      verify_checksum_after_work = o.verify_checksum_after_work;
      src_checksum_func_name = std::move(o.src_checksum_func_name);
      src_checksum_hex = std::move(o.src_checksum_hex);
      backup_checksum_func_name = std::move(o.backup_checksum_func_name);
      db_id = std::move(o.db_id);
      db_session_id = std::move(o.db_session_id);
      return *this;
    }

    CopyOrCreateWorkItem(
        std::string _src_path, std::string _dst_path, std::string _contents,
        Env* _src_env, Env* _dst_env, EnvOptions _src_env_options, bool _sync,
        RateLimiter* _rate_limiter, uint64_t _size_limit,
        std::function<void()> _progress_callback = []() {},
        bool _verify_checksum_after_work = false,
        const std::string& _src_checksum_func_name =
            kUnknownFileChecksumFuncName,
        const std::string& _src_checksum_hex = "",
        const std::string& _backup_checksum_func_name =
            kUnknownFileChecksumFuncName,
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
          verify_checksum_after_work(_verify_checksum_after_work),
          src_checksum_func_name(_src_checksum_func_name),
          src_checksum_hex(_src_checksum_hex),
          backup_checksum_func_name(_backup_checksum_func_name),
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
    std::string checksum_hex;
    RestoreAfterCopyOrCreateWorkItem() : checksum_hex("") {}
    RestoreAfterCopyOrCreateWorkItem(std::future<CopyOrCreateResult>&& _result,
                                     const std::string& _checksum_hex)
        : result(std::move(_result)), checksum_hex(_checksum_hex) {}
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
  channel<CopyOrCreateWorkItem> files_to_copy_or_create_;
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
      uint64_t size_bytes, uint64_t size_limit = 0,
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
  BackupableDBOptions options_;
  Env* db_env_;
  Env* backup_env_;

  // directories
  std::unique_ptr<Directory> backup_directory_;
  std::unique_ptr<Directory> shared_directory_;
  std::unique_ptr<Directory> meta_directory_;
  std::unique_ptr<Directory> private_directory_;

  static const size_t kDefaultCopyFileBufferSize = 5 * 1024 * 1024LL;  // 5MB
  size_t copy_file_buffer_size_;
  bool read_only_;
  BackupStatistics backup_statistics_;
  static const size_t kMaxAppMetaSize = 1024 * 1024;  // 1MB
};

Status BackupEngine::Open(const BackupableDBOptions& options, Env* env,
                          BackupEngine** backup_engine_ptr) {
  std::unique_ptr<BackupEngineImpl> backup_engine(
      new BackupEngineImpl(options, env));
  auto s = backup_engine->Initialize();
  if (!s.ok()) {
    *backup_engine_ptr = nullptr;
    return s;
  }
  *backup_engine_ptr = backup_engine.release();
  return Status::OK();
}

BackupEngineImpl::BackupEngineImpl(const BackupableDBOptions& options,
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
}

Status BackupEngineImpl::Initialize() {
  assert(!initialized_);
  initialized_ = true;
  if (read_only_) {
    ROCKS_LOG_INFO(options_.info_log, "Starting read_only backup engine");
  }
  options_.Dump(options_.info_log);

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
    directories.emplace_back(GetAbsolutePath(GetPrivateDirRel()),
                             &private_directory_);
    directories.emplace_back(GetBackupMetaDir(), &meta_directory_);
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
    auto s = backup_env_->GetChildren(GetBackupMetaDir(), &backup_meta_files);
    if (s.IsNotFound()) {
      return Status::NotFound(GetBackupMetaDir() + " is missing");
    } else if (!s.ok()) {
      return s;
    }
  }
  // create backups_ structure
  for (auto& file : backup_meta_files) {
    if (file == "." || file == "..") {
      continue;
    }
    ROCKS_LOG_INFO(options_.info_log, "Detected backup %s", file.c_str());
    BackupID backup_id = 0;
    sscanf(file.c_str(), "%u", &backup_id);
    if (backup_id == 0 || file != ROCKSDB_NAMESPACE::ToString(backup_id)) {
      if (!read_only_) {
        // invalid file name, delete that
        auto s = backup_env_->DeleteFile(GetBackupMetaDir() + "/" + file);
        ROCKS_LOG_INFO(options_.info_log,
                       "Unrecognized meta file %s, deleting -- %s",
                       file.c_str(), s.ToString().c_str());
      }
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
      InsertPathnameToSizeBytes(abs_dir, backup_env_, &abs_path_to_size);
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
      InsertPathnameToSizeBytes(
          GetAbsolutePath(GetPrivateFileRel(backup_iter->first)), backup_env_,
          &abs_path_to_size);
      Status s = backup_iter->second->LoadFromFile(options_.backup_dir,
                                                   abs_path_to_size);
      if (s.IsCorruption()) {
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
            work_item.sync, work_item.rate_limiter,
            work_item.backup_checksum_func_name, &result.size,
            &result.checksum_hex, &result.custom_checksum_hex,
            work_item.size_limit, work_item.progress_callback);
        result.checksum_func_name = work_item.backup_checksum_func_name;
        result.db_id = work_item.db_id;
        result.db_session_id = work_item.db_session_id;
        if (result.status.ok() && work_item.verify_checksum_after_work) {
          // work_item.verify_checksum_after_work being true means backup engine
          // has obtained its crc32c and/or custom checksum for the table file.
          // Therefore, we can try to compare the checksums if possible.
          if (work_item.src_checksum_func_name ==
                  kUnknownFileChecksumFuncName ||
              IsSameChecksumFunc(result.checksum_func_name,
                                 work_item.src_checksum_func_name)) {
            std::string checksum_to_compare;
            std::string checksum_func_name_used;
            if (work_item.src_checksum_func_name ==
                    kUnknownFileChecksumFuncName ||
                work_item.src_checksum_func_name ==
                    kStandardDbFileChecksumFuncName) {
              // kUnknownFileChecksumFuncName implies no table file checksums in
              // db manifest, but we can compare using the crc32c checksum
              checksum_to_compare = result.checksum_hex;
              checksum_func_name_used = kStandardDbFileChecksumFuncName;
            } else {
              checksum_to_compare = result.custom_checksum_hex;
              checksum_func_name_used = work_item.src_checksum_func_name;
            }
            if (work_item.src_checksum_hex != checksum_to_compare) {
              std::string checksum_info(
                  "Expected checksum is " + work_item.src_checksum_hex +
                  " while computed checksum is " + checksum_to_compare);
              result.status = Status::Corruption(
                  checksum_func_name_used + " mismatch after copying to " +
                  work_item.dst_path + ": " + checksum_info);
            }
          } else {
            std::string checksum_function_info(
                "Existing checksum function is " +
                work_item.src_checksum_func_name +
                " while provided checksum function is " +
                result.checksum_func_name);
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
    const CreateBackupOptions& options, DB* db,
    const std::string& app_metadata) {
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
  db->DisableFileDeletions();
  if (s.ok()) {
    CheckpointImpl checkpoint(db);
    uint64_t sequence_number = 0;
    DBOptions db_options = db->GetDBOptions();
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
          if (type == kLogFile && !options_.backup_log_files) {
            return Status::OK();
          }
          Log(options_.info_log, "add file for backup %s", fname.c_str());
          uint64_t size_bytes = 0;
          Status st;
          if (type == kTableFile) {
            st = db_env_->GetFileSize(src_dirname + fname, &size_bytes);
          }
          EnvOptions src_env_options;
          switch (type) {
            case kLogFile:
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
                options_.share_table_files && type == kTableFile, src_dirname,
                fname, src_env_options, rate_limiter, size_bytes,
                size_limit_bytes,
                options_.share_files_with_checksum && type == kTableFile,
                options.progress_callback, "" /* contents */,
                checksum_func_name, checksum_val);
          }
          return st;
        } /* copy_file_cb */,
        [&](const std::string& fname, const std::string& contents, FileType) {
          Log(options_.info_log, "add file for backup %s", fname.c_str());
          return AddBackupFileWorkItem(
              live_dst_paths, backup_items_to_finish, new_backup_id,
              false /* shared */, "" /* src_dir */, fname,
              EnvOptions() /* src_env_options */, rate_limiter, contents.size(),
              0 /* size_limit */, false /* shared_checksum */,
              options.progress_callback, contents);
        } /* create_file_cb */,
        &sequence_number, options.flush_before_backup ? 0 : port::kMaxUint64,
        db_options.file_checksum_gen_factory == nullptr ? false : true);
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
          item.dst_relative, result.size, result.checksum_hex,
          result.custom_checksum_hex, result.checksum_func_name, result.db_id,
          result.db_session_id));
    }
    if (!item_status.ok()) {
      s = item_status;
    }
  }

  // we copied all the files, enable file deletions
  db->EnableFileDeletions(false);

  auto backup_time = backup_env_->NowMicros() - start_backup;

  if (s.ok()) {
    // persist the backup metadata on the disk
    s = new_backup->StoreToFile(options_.sync);
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
    DeleteBackup(new_backup_id);
    return s;
  }

  // here we know that we succeeded and installed the new backup
  // in the LATEST_BACKUP file
  latest_backup_id_ = new_backup_id;
  latest_valid_backup_id_ = new_backup_id;
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
    auto s = DeleteBackupInternal(backup_id);
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
  auto s1 = DeleteBackupInternal(backup_id);
  auto s2 = Status::OK();

  // Clean up after any incomplete backup deletion, potentially from
  // earlier session.
  if (might_need_garbage_collect_) {
    s2 = GarbageCollect();
  }

  if (!s1.ok()) {
    return s1;
  } else {
    return s2;
  }
}

// Does not auto-GarbageCollect
Status BackupEngineImpl::DeleteBackupInternal(BackupID backup_id) {
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

void BackupEngineImpl::GetBackupInfo(std::vector<BackupInfo>* backup_info) {
  assert(initialized_);
  backup_info->reserve(backups_.size());
  for (auto& backup : backups_) {
    if (!backup.second->Empty()) {
      backup_info->push_back(BackupInfo(
          backup.first, backup.second->GetTimestamp(), backup.second->GetSize(),
          backup.second->GetNumberFiles(), backup.second->GetAppMetadata()));
    }
  }
}

void
BackupEngineImpl::GetCorruptedBackups(
    std::vector<BackupID>* corrupt_backup_ids) {
  assert(initialized_);
  corrupt_backup_ids->reserve(corrupt_backups_.size());
  for (auto& backup : corrupt_backups_) {
    corrupt_backup_ids->push_back(backup.first);
  }
}

Status BackupEngineImpl::RestoreDBFromBackup(const RestoreOptions& options,
                                             BackupID backup_id,
                                             const std::string& db_dir,
                                             const std::string& wal_dir) {
  assert(initialized_);
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
  db_env_->CreateDirIfMissing(db_dir);
  db_env_->CreateDirIfMissing(wal_dir);

  if (options.keep_log_files) {
    // delete files in db_dir, but keep all the log files
    DeleteChildren(db_dir, 1 << kLogFile);
    // move all the files from archive dir to wal_dir
    std::string archive_dir = ArchivalDirectory(wal_dir);
    std::vector<std::string> archive_files;
    db_env_->GetChildren(archive_dir, &archive_files);  // ignore errors
    for (const auto& f : archive_files) {
      uint64_t number;
      FileType type;
      bool ok = ParseFileName(f, &number, &type);
      if (ok && type == kLogFile) {
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

  Status s;
  // Try to obtain checksum info from backuped DB MANIFEST
  // The checksum info will be used for validating the checksums of the table
  // files after restoration, in addtion to the default backup engine crc32c
  // checksums.
  std::unique_ptr<FileChecksumList> checksum_list(NewFileChecksumList());
  s = GetFileChecksumsFromManifestInBackup(backup_env_, backup_id, backup.get(),
                                           checksum_list.get());
  if (!s.ok()) {
    return s;
  }

  RateLimiter* rate_limiter = options_.restore_rate_limiter.get();
  if (rate_limiter) {
    copy_file_buffer_size_ =
        static_cast<size_t>(rate_limiter->GetSingleBurstBytes());
  }
  std::vector<RestoreAfterCopyOrCreateWorkItem> restore_items_to_finish;
  for (const auto& file_info : backup->GetFiles()) {
    const std::string& file = file_info->filename;
    std::string dst;
    uint64_t number;
    FileType type;
    s = GetFileNameInfo(file, dst, number, type);
    if (!s.ok()) {
      return s;
    }

    std::string src_checksum_func_name = kUnknownFileChecksumFuncName;
    std::string src_checksum_str = kUnknownFileChecksum;
    std::string src_checksum_hex;
    bool has_manifest_checksum = false;
    if (type == kTableFile) {
      Status file_checksum_status = checksum_list->SearchOneFileChecksum(
          number, &src_checksum_str, &src_checksum_func_name);
      if (file_checksum_status.ok() &&
          src_checksum_str != kUnknownFileChecksum &&
          src_checksum_func_name != kUnknownFileChecksumFuncName) {
        src_checksum_hex = ChecksumStrToHex(src_checksum_str);
        has_manifest_checksum = true;
      }
    }

    // Construct the final path
    // kLogFile lives in wal_dir and all the rest live in db_dir
    dst = ((type == kLogFile) ? wal_dir : db_dir) +
      "/" + dst;

    ROCKS_LOG_INFO(options_.info_log, "Restoring %s to %s\n", file.c_str(),
                   dst.c_str());

    std::string backup_checksum_func_name = file_info->checksum_func_name;
    std::unique_ptr<FileChecksumGenerator> checksum_func;
    if (src_checksum_func_name != kUnknownFileChecksumFuncName) {
      s = SetChecksumGenerator(src_checksum_func_name, checksum_func);
      if (!s.ok()) {
        return s;
      }
      if (checksum_func != nullptr) {
        backup_checksum_func_name = checksum_func->Name();
      }
    }
    CopyOrCreateWorkItem copy_or_create_work_item(
        GetAbsolutePath(file), dst, "" /* contents */, backup_env_, db_env_,
        EnvOptions() /* src_env_options */, false, rate_limiter,
        0 /* size_limit */, []() {} /* progress_callback */,
        has_manifest_checksum, src_checksum_func_name, src_checksum_hex,
        backup_checksum_func_name);
    RestoreAfterCopyOrCreateWorkItem after_copy_or_create_work_item(
        copy_or_create_work_item.result.get_future(), file_info->checksum_hex);
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
    } else if (item.checksum_hex != result.checksum_hex) {
      // Compare crc32c checksums (especially for non-table files)
      std::string checksum_info("Expected checksum is " + item.checksum_hex +
                                " while computed checksum is " +
                                result.checksum_hex);
      s = Status::Corruption("Crc32c checksum check failed: " + checksum_info);
      break;
    }
  }

  ROCKS_LOG_INFO(options_.info_log, "Restoring done -- %s\n",
                 s.ToString().c_str());
  return s;
}

Status BackupEngineImpl::VerifyBackup(BackupID backup_id,
                                      bool verify_with_checksum) {
  // Check if backup_id is corrupted, or valid and registered
  assert(initialized_);
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
    InsertPathnameToSizeBytes(abs_dir, backup_env_, &curr_abs_path_to_size);
  }

  Status s;
  std::unique_ptr<FileChecksumList> checksum_list(NewFileChecksumList());
  if (verify_with_checksum) {
    // Try to obtain checksum info from backuped DB MANIFEST
    s = GetFileChecksumsFromManifestInBackup(backup_env_, backup_id,
                                             backup.get(), checksum_list.get());
    if (!s.ok()) {
      return s;
    }
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
    if (verify_with_checksum) {
      // verify file checksum
      // try setting checksum_func
      std::unique_ptr<FileChecksumGenerator> checksum_func;
      std::string src_checksum_func_name = kUnknownFileChecksumFuncName;
      std::string src_checksum_str = kUnknownFileChecksum;
      std::string src_checksum_hex;
      if (IsSstFile(file_info->filename)) {
        const std::string& file = file_info->filename;
        std::string local_name;
        uint64_t number;
        FileType type;
        s = GetFileNameInfo(file, local_name, number, type);
        if (!s.ok()) {
          return s;
        }
        assert(type == kTableFile);

        // Try to get checksum for the table file
        Status file_checksum_status = checksum_list->SearchOneFileChecksum(
            number, &src_checksum_str, &src_checksum_func_name);
        if (file_checksum_status.ok() &&
            src_checksum_str != kUnknownFileChecksum &&
            src_checksum_func_name != kUnknownFileChecksumFuncName) {
          s = SetChecksumGenerator(src_checksum_func_name, checksum_func);
          if (!s.ok()) {
            return s;
          }
          src_checksum_hex = ChecksumStrToHex(src_checksum_str);
        }
      }

      ROCKS_LOG_INFO(options_.info_log, "Verifying %s checksum...\n",
                     abs_path.c_str());
      std::string checksum_hex;
      std::string custom_checksum_hex;
      CalculateChecksum(abs_path, backup_env_, EnvOptions(), 0 /* size_limit */,
                        &checksum_hex, checksum_func, &custom_checksum_hex);
      if (file_info->checksum_hex != checksum_hex) {
        std::string checksum_info(
            "Expected checksum is " + file_info->checksum_hex +
            " while computed checksum is " + checksum_hex);
        return Status::Corruption("File corrupted: crc32c mismatch for " +
                                  abs_path + ": " + checksum_info);
      }
      if (checksum_func != nullptr && src_checksum_hex != custom_checksum_hex) {
        std::string checksum_info("Expected checksum is " + src_checksum_hex +
                                  " while computed checksum is " +
                                  custom_checksum_hex);
        return Status::Corruption("File corrupted: " + src_checksum_func_name +
                                  " mismatch for " + abs_path + ": " +
                                  checksum_info);
      }
    }
  }

  return Status::OK();
}

Status BackupEngineImpl::CopyOrCreateFile(
    const std::string& src, const std::string& dst, const std::string& contents,
    Env* src_env, Env* dst_env, const EnvOptions& src_env_options, bool sync,
    RateLimiter* rate_limiter, const std::string& backup_checksum_func_name,
    uint64_t* size, std::string* checksum_hex, std::string* custom_checksum_hex,
    uint64_t size_limit, std::function<void()> progress_callback) {
  assert(src.empty() != contents.empty());
  Status s;
  std::unique_ptr<WritableFile> dst_file;
  std::unique_ptr<SequentialFile> src_file;
  EnvOptions dst_env_options;
  dst_env_options.use_mmap_writes = false;
  // TODO:(gzh) maybe use direct reads/writes here if possible
  if (size != nullptr) {
    *size = 0;
  }
  uint32_t checksum_value = 0;

  // Get custom checksum function
  std::unique_ptr<FileChecksumGenerator> checksum_func;
  s = SetChecksumGenerator(backup_checksum_func_name, checksum_func);
  if (!s.ok()) {
    return s;
  }

  // Check if size limit is set. if not, set it to very big number
  if (size_limit == 0) {
    size_limit = std::numeric_limits<uint64_t>::max();
  }

  s = dst_env->NewWritableFile(dst, &dst_file, dst_env_options);
  if (s.ok() && !src.empty()) {
    s = src_env->NewSequentialFile(src, &src_file, src_env_options);
  }
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<WritableFileWriter> dest_writer(new WritableFileWriter(
      NewLegacyWritableFileWrapper(std::move(dst_file)), dst, dst_env_options));
  std::unique_ptr<SequentialFileReader> src_reader;
  std::unique_ptr<char[]> buf;
  if (!src.empty()) {
    src_reader.reset(new SequentialFileReader(
        NewLegacySequentialFileWrapper(src_file), src));
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
    if (checksum_func != nullptr && custom_checksum_hex != nullptr) {
      checksum_func->Update(data.data(), data.size());
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

  if (checksum_hex != nullptr) {
    // Convert uint32_t checksum to hex checksum
    checksum_hex->assign(ChecksumInt32ToHex(checksum_value));
  }
  if (checksum_func != nullptr && custom_checksum_hex != nullptr) {
    checksum_func->Finalize();
    custom_checksum_hex->assign(ChecksumStrToHex(checksum_func->GetChecksum()));
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
    RateLimiter* rate_limiter, uint64_t size_bytes, uint64_t size_limit,
    bool shared_checksum, std::function<void()> progress_callback,
    const std::string& contents, const std::string& src_checksum_func_name,
    const std::string& src_checksum_str) {
  assert(!fname.empty() && fname[0] == '/');
  assert(contents.empty() != src_dir.empty());

  std::string dst_relative = fname.substr(1);
  std::string dst_relative_tmp;
  Status s;
  std::string checksum_hex;
  std::string custom_checksum_hex;
  // The function name of backup checksum function.
  std::string backup_checksum_func_name = kDefaultBackupFileChecksumFuncName;
  std::string db_id;
  std::string db_session_id;
  // whether a default or custom checksum for a table file is available
  bool has_checksum = false;

  // Set up the custom checksum function.
  // A nullptr checksum_func indicates the default backup checksum function
  // will be used. If checksum_func is not nullptr, then both the default
  // backup checksum function and checksum_func will be used.
  std::unique_ptr<FileChecksumGenerator> checksum_func;
  if (src_checksum_func_name != kUnknownFileChecksumFuncName) {
    // DB files have checksum functions
    s = SetChecksumGenerator(src_checksum_func_name, checksum_func);
    if (!s.ok()) {
      return s;
    }
    if (checksum_func != nullptr) {
      backup_checksum_func_name = checksum_func->Name();
    }
  }

  // Whenever the db checksum function name matches the backup engine custom
  // checksum function name, we will compare the checksum values after copying.
  // Note that only table files may have a known checksum name passed in.
  //
  // If the checksum function names do not match and db session id is not
  // available, we will calculate the checksum *before* copying in two cases
  // (we always calcuate checksums when copying or creating for any file types):
  // a) share_files_with_checksum is true and file type is table;
  // b) share_table_files is true and the file exists already.
  //
  // Step 0: Check if a known checksum function name is passed in
  if (IsSameChecksumFunc(backup_checksum_func_name, src_checksum_func_name)) {
    if (src_checksum_str == kUnknownFileChecksum) {
      return Status::Aborted("Unknown checksum value for " + fname);
    }
    if (checksum_func == nullptr) {
      checksum_hex = ChecksumStrToHex(src_checksum_str);
    } else {
      custom_checksum_hex = ChecksumStrToHex(src_checksum_str);
    }
    has_checksum = true;
  }

  // Step 1: Prepare the relative path to destination
  if (shared && shared_checksum) {
    if (GetTableNamingOption() == kOptionalChecksumAndDbSessionId) {
      // Prepare db_session_id to add to the file name
      // Ignore the returned status
      // In the failed cases, db_id and db_session_id will be empty
      GetFileDbIdentities(db_env_, src_env_options, src_dir + fname, &db_id,
                          &db_session_id);
    }
    // Calculate checksum if checksum and db session id are not available.
    // If db session id is available, we will not calculate the checksum
    // since the session id should suffice to avoid file name collision in
    // the shared_checksum directory.
    if (!has_checksum && db_session_id.empty()) {
      s = CalculateChecksum(src_dir + fname, db_env_, src_env_options,
                            size_limit, &checksum_hex, checksum_func,
                            &custom_checksum_hex);
      if (!s.ok()) {
        return s;
      }
      has_checksum = true;
    }
    if (size_bytes == port::kMaxUint64) {
      return Status::NotFound("File missing: " + src_dir + fname);
    }
    // dst_relative depends on the following conditions:
    // 1) the naming scheme is kOptionalChecksumAndDbSessionId,
    // 2) db_session_id is not empty,
    // 3) checksum is available in the DB manifest.
    // If 1,2,3) are satisfied, then dst_relative will be of the form:
    // shared_checksum/<file_number>_<checksum>_<db_session_id>.sst
    // If 1,2) are satisfied, then dst_relative will be of the form:
    // shared_checksum/<file_number>_<db_session_id>.sst
    // Otherwise, dst_relative is of the form
    // shared_checksum/<file_number>_<checksum>_<size>.sst
    //
    // Also, we display custom checksums in the name if possible.
    dst_relative = GetSharedFileWithChecksum(
        dst_relative, has_checksum,
        checksum_func == nullptr || !UseSessionId(db_session_id)
            ? checksum_hex
            : custom_checksum_hex,
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
      assert(s.IsIOError());
      return exist;
    }
  }

  if (!contents.empty()) {
    need_to_copy = false;
  } else if (shared && (same_path || file_exists)) {
    need_to_copy = false;
    if (shared_checksum) {
      if (backuped_file_infos_.find(dst_relative) ==
              backuped_file_infos_.end() &&
          !same_path) {
        // file exists but not referenced
        ROCKS_LOG_INFO(
            options_.info_log,
            "%s already present, but not referenced by any backup. We will "
            "overwrite the file.",
            fname.c_str());
        need_to_copy = true;
        backup_env_->DeleteFile(final_dest_path);
      } else {
        // file exists and referenced
        if (!has_checksum || checksum_hex.empty()) {
          // Either both checksum_hex and custom_checksum_hex need recalculating
          // or only checksum_hex needs recalculating
          s = CalculateChecksum(
              src_dir + fname, db_env_, src_env_options, size_limit,
              &checksum_hex, checksum_func,
              checksum_hex.empty() ? nullptr : &custom_checksum_hex);
          if (!s.ok()) {
            return s;
          }
          has_checksum = true;
        }
        if (UseSessionId(db_session_id)) {
          ROCKS_LOG_INFO(options_.info_log,
                         "%s already present, with checksum %s, size %" PRIu64
                         " and DB session identity %s",
                         fname.c_str(), checksum_hex.c_str(), size_bytes,
                         db_session_id.c_str());
        } else {
          ROCKS_LOG_INFO(
              options_.info_log,
              "%s already present, with checksum %s and size %" PRIu64,
              fname.c_str(), checksum_hex.c_str(), size_bytes);
        }
      }
      if (checksum_func != nullptr) {
        ROCKS_LOG_INFO(options_.info_log, "%s checksum is %s",
                       backup_checksum_func_name.c_str(),
                       custom_checksum_hex.c_str());
      }
    } else if (backuped_file_infos_.find(dst_relative) ==
                   backuped_file_infos_.end() &&
               !same_path) {
      // file already exists, but it's not referenced by any backup. overwrite
      // the file
      ROCKS_LOG_INFO(
          options_.info_log,
          "%s already present, but not referenced by any backup. We will "
          "overwrite the file.",
          fname.c_str());
      need_to_copy = true;
      backup_env_->DeleteFile(final_dest_path);
    } else {
      // the file is present and referenced by a backup
      ROCKS_LOG_INFO(options_.info_log,
                     "%s already present, calculate checksum", fname.c_str());
      if (!has_checksum || checksum_hex.empty()) {
        // Either both checksum_hex and custom_checksum_hex need recalculating
        // or only checksum_hex needs recalculating
        s = CalculateChecksum(
            src_dir + fname, db_env_, src_env_options, size_limit,
            &checksum_hex, checksum_func,
            checksum_hex.empty() ? nullptr : &custom_checksum_hex);
        if (!s.ok()) {
          return s;
        }
        has_checksum = true;
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
        size_limit, progress_callback, has_checksum, src_checksum_func_name,
        checksum_func == nullptr ? checksum_hex : custom_checksum_hex,
        backup_checksum_func_name, db_id, db_session_id);
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
    result.status = s;
    result.size = size_bytes;
    result.checksum_hex = std::move(checksum_hex);
    result.custom_checksum_hex = std::move(custom_checksum_hex);
    result.checksum_func_name = std::move(backup_checksum_func_name);
    result.db_id = std::move(db_id);
    result.db_session_id = std::move(db_session_id);
    promise_result.set_value(std::move(result));
  }
  return s;
}

Status BackupEngineImpl::CalculateChecksum(
    const std::string& src, Env* src_env, const EnvOptions& src_env_options,
    uint64_t size_limit, std::string* checksum_hex,
    const std::unique_ptr<FileChecksumGenerator>& checksum_func,
    std::string* custom_checksum_hex) {
  if (checksum_hex == nullptr) {
    return Status::InvalidArgument("Checksum pointer is null");
  }
  uint32_t checksum_value = 0;

  if (size_limit == 0) {
    size_limit = std::numeric_limits<uint64_t>::max();
  }

  std::unique_ptr<SequentialFile> src_file;
  Status s = src_env->NewSequentialFile(src, &src_file, src_env_options);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<SequentialFileReader> src_reader(
      new SequentialFileReader(NewLegacySequentialFileWrapper(src_file), src));
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
    if (checksum_func != nullptr && custom_checksum_hex != nullptr) {
      checksum_func->Update(data.data(), data.size());
    }
  } while (data.size() > 0 && size_limit > 0);

  checksum_hex->assign(ChecksumInt32ToHex(checksum_value));
  if (checksum_func != nullptr && custom_checksum_hex != nullptr) {
    checksum_func->Finalize();
    custom_checksum_hex->assign(ChecksumStrToHex(checksum_func->GetChecksum()));
  }

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

Status BackupEngineImpl::GetFileChecksumsFromManifestInBackup(
    Env* src_env, const BackupID& backup_id, const BackupMeta* backup,
    FileChecksumList* checksum_list) {
  if (checksum_list == nullptr) {
    return Status::InvalidArgument("checksum_list is nullptr");
  }

  checksum_list->reset();
  Status s;

  // Read CURRENT file to get the latest DB MANIFEST filename in backup_id
  // and then read the the MANIFEST file to obtain the checksum info stored
  // in the file.
  std::string current_rel_path =
      GetPrivateFileRel(backup_id, false /* tmp */, "CURRENT");
  s = VerifyFileWithCrc32c(src_env, backup, current_rel_path);
  if (!s.ok()) {
    return s;
  }

  std::string manifest_filename;
  s = ReadFileToString(src_env, GetAbsolutePath(current_rel_path),
                       &manifest_filename);
  if (!s.ok()) {
    return s;
  }
  // Remove tailing '\n' if any
  while (!manifest_filename.empty() && manifest_filename.back() == '\n') {
    manifest_filename.pop_back();
  }

  std::string manifest_rel_path =
      GetPrivateFileRel(backup_id, false /* tmp */, manifest_filename);
  s = VerifyFileWithCrc32c(src_env, backup, manifest_rel_path);
  if (!s.ok()) {
    return s;
  }

  // Read whole manifest file in backup
  s = GetFileChecksumsFromManifest(
      src_env, GetAbsolutePath(manifest_rel_path),
      std::numeric_limits<uint64_t>::max() /*manifest_file_size*/,
      checksum_list);
  return s;
}

Status BackupEngineImpl::VerifyFileWithCrc32c(Env* src_env,
                                              const BackupMeta* backup,
                                              const std::string& rel_path) {
  const std::shared_ptr<FileInfo> file_info = backup->GetFile(rel_path);
  if (file_info == nullptr) {
    return Status::Corruption(rel_path + " is missing");
  }

  std::string abs_path = GetAbsolutePath(rel_path);
  std::string expected_checksum = file_info->checksum_hex;
  std::string actual_checksum;
  Status s = CalculateChecksum(abs_path, src_env, EnvOptions(),
                               0 /* size_limit */, &actual_checksum);
  if (!s.ok()) {
    return s;
  }
  if (actual_checksum != expected_checksum) {
    std::string checksum_info("Expected checksum is " + expected_checksum +
                              " while computed checksum is " + actual_checksum);
    return Status::Corruption("crc32c mismatch for " + rel_path + ": " +
                              checksum_info);
  }
  return s;
}

void BackupEngineImpl::DeleteChildren(const std::string& dir,
                                      uint32_t file_type_filter) {
  std::vector<std::string> children;
  db_env_->GetChildren(dir, &children);  // ignore errors

  for (const auto& f : children) {
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(f, &number, &type);
    if (ok && (file_type_filter & (1 << type))) {
      // don't delete this file
      continue;
    }
    db_env_->DeleteFile(dir + "/" + f);  // ignore errors
  }
}

Status BackupEngineImpl::InsertPathnameToSizeBytes(
    const std::string& dir, Env* env,
    std::unordered_map<std::string, uint64_t>* result) {
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
      if (child == "." || child == "..") {
        continue;
      }
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
    auto s = backup_env_->GetChildren(GetAbsolutePath(GetPrivateDirRel()),
                                      &private_children);
    if (!s.ok()) {
      overall_status = s;
      // Trying again later might work
      might_need_garbage_collect_ = true;
    }
  }
  for (auto& child : private_children) {
    if (child == "." || child == "..") {
      continue;
    }

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
    backup_env_->GetChildren(full_private_path, &subchildren);
    for (auto& subchild : subchildren) {
      if (subchild == "." || subchild == "..") {
        continue;
      }
      Status s = backup_env_->DeleteFile(full_private_path + subchild);
      ROCKS_LOG_INFO(options_.info_log, "Deleting %s -- %s",
                     (full_private_path + subchild).c_str(),
                     s.ToString().c_str());
      if (!s.ok()) {
        // Trying again later might work
        might_need_garbage_collect_ = true;
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
    if (itr->second->checksum_hex != file_info->checksum_hex) {
      return Status::Corruption(
          "Checksum mismatch for existing backup file. Delete old backups and "
          "try again.");
    } else if (IsSameChecksumFunc(itr->second->checksum_func_name,
                                  file_info->checksum_func_name) &&
               !itr->second->custom_checksum_hex.empty() &&
               itr->second->custom_checksum_hex !=
                   file_info->custom_checksum_hex) {
      return Status::Corruption(
          "Custom checksum mismatch for existing backup file. Delete old "
          "backups and try again.");
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

Slice kMetaDataPrefix("metadata ");

// each backup meta file is of the format:
// <timestamp>
// <seq number>
// <metadata(literal string)> <metadata> (optional)
// <number of files>
// <file1> <crc32(literal string)> <crc32c_value>
// <file2> <crc32(literal string)> <crc32c_value>
// ...
Status BackupEngineImpl::BackupMeta::LoadFromFile(
    const std::string& backup_dir,
    const std::unordered_map<std::string, uint64_t>& abs_path_to_size) {
  assert(Empty());
  Status s;
  std::unique_ptr<SequentialFile> backup_meta_file;
  s = env_->NewSequentialFile(meta_filename_, &backup_meta_file, EnvOptions());
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<SequentialFileReader> backup_meta_reader(
      new SequentialFileReader(NewLegacySequentialFileWrapper(backup_meta_file),
                               meta_filename_));
  std::unique_ptr<char[]> buf(new char[max_backup_meta_file_size_ + 1]);
  Slice data;
  s = backup_meta_reader->Read(max_backup_meta_file_size_, &data, buf.get());

  if (!s.ok() || data.size() == max_backup_meta_file_size_) {
    return s.ok() ? Status::Corruption("File size too big") : s;
  }
  buf[data.size()] = 0;

  uint32_t num_files = 0;
  char *next;
  timestamp_ = strtoull(data.data(), &next, 10);
  data.remove_prefix(next - data.data() + 1); // +1 for '\n'
  sequence_number_ = strtoull(data.data(), &next, 10);
  data.remove_prefix(next - data.data() + 1); // +1 for '\n'

  if (data.starts_with(kMetaDataPrefix)) {
    // app metadata present
    data.remove_prefix(kMetaDataPrefix.size());
    Slice hex_encoded_metadata = GetSliceUntil(&data, '\n');
    bool decode_success = hex_encoded_metadata.DecodeHex(&app_metadata_);
    if (!decode_success) {
      return Status::Corruption(
          "Failed to decode stored hex encoded app metadata");
    }
  }

  num_files = static_cast<uint32_t>(strtoul(data.data(), &next, 10));
  data.remove_prefix(next - data.data() + 1); // +1 for '\n'

  std::vector<std::shared_ptr<FileInfo>> files;

  // WART: The checksums are crc32c, not original crc32
  Slice checksum_prefix("crc32 ");

  for (uint32_t i = 0; s.ok() && i < num_files; ++i) {
    auto line = GetSliceUntil(&data, '\n');
    // filename is relative, i.e., shared/number.sst,
    // shared_checksum/number.sst, or private/backup_id/number.sst
    std::string filename = GetSliceUntil(&line, ' ').ToString();

    uint64_t size;
    const std::shared_ptr<FileInfo> file_info = GetFile(filename);
    if (file_info) {
      size = file_info->size;
    } else {
      std::string abs_path = backup_dir + "/" + filename;
      try {
        size = abs_path_to_size.at(abs_path);
      } catch (std::out_of_range&) {
        return Status::Corruption("Size missing for pathname: " + abs_path);
      }
    }

    if (line.empty()) {
      return Status::Corruption("File checksum is missing for " + filename +
                                " in " + meta_filename_);
    }

    uint32_t checksum_value = 0;
    std::string checksum_func_name = kUnknownFileChecksumFuncName;
    if (line.starts_with(checksum_prefix)) {
      line.remove_prefix(checksum_prefix.size());
      checksum_func_name = kDefaultBackupFileChecksumFuncName;
      checksum_value = static_cast<uint32_t>(strtoul(line.data(), nullptr, 10));
      if (line != ROCKSDB_NAMESPACE::ToString(checksum_value)) {
        return Status::Corruption("Invalid crc32c checksum value for " +
                                  filename + " in " + meta_filename_);
      }
    } else {
      return Status::Corruption("Unknown checksum type for " + filename +
                                " in " + meta_filename_);
    }

    files.emplace_back(
        new FileInfo(filename, size, ChecksumInt32ToHex(checksum_value),
                     "" /* custom_checksum_hex */, checksum_func_name));
  }

  if (s.ok() && data.size() > 0) {
    // file has to be read completely. if not, we count it as corruption
    s = Status::Corruption("Tailing data in backup meta file in " +
                           meta_filename_);
  }

  if (s.ok()) {
    files_.reserve(files.size());
    for (const auto& file_info : files) {
      s = AddFile(file_info);
      if (!s.ok()) {
        break;
      }
    }
  }

  return s;
}

Status BackupEngineImpl::BackupMeta::StoreToFile(bool sync) {
  Status s;
  std::unique_ptr<WritableFile> backup_meta_file;
  EnvOptions env_options;
  env_options.use_mmap_writes = false;
  env_options.use_direct_writes = false;
  s = env_->NewWritableFile(meta_tmp_filename_, &backup_meta_file, env_options);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<char[]> buf(new char[max_backup_meta_file_size_]);
  size_t len = 0, buf_size = max_backup_meta_file_size_;
  len += snprintf(buf.get(), buf_size, "%" PRId64 "\n", timestamp_);
  len += snprintf(buf.get() + len, buf_size - len, "%" PRIu64 "\n",
                  sequence_number_);
  if (!app_metadata_.empty()) {
    std::string hex_encoded_metadata =
        Slice(app_metadata_).ToString(/* hex */ true);

    // +1 to accommodate newline character
    size_t hex_meta_strlen =
        kMetaDataPrefix.ToString().length() + hex_encoded_metadata.length() + 1;
    if (hex_meta_strlen >= buf_size) {
      return Status::Corruption("Buffer too small to fit backup metadata");
    }
    else if (len + hex_meta_strlen >= buf_size) {
      backup_meta_file->Append(Slice(buf.get(), len));
      buf.reset();
      std::unique_ptr<char[]> new_reset_buf(
          new char[max_backup_meta_file_size_]);
      buf.swap(new_reset_buf);
      len = 0;
    }
    len += snprintf(buf.get() + len, buf_size - len, "%s%s\n",
                    kMetaDataPrefix.ToString().c_str(),
                    hex_encoded_metadata.c_str());
  }

  char writelen_temp[19];
  if (len + snprintf(writelen_temp, sizeof(writelen_temp),
                     "%" ROCKSDB_PRIszt "\n", files_.size()) >= buf_size) {
    backup_meta_file->Append(Slice(buf.get(), len));
    buf.reset();
    std::unique_ptr<char[]> new_reset_buf(new char[max_backup_meta_file_size_]);
    buf.swap(new_reset_buf);
    len = 0;
  }
  {
    const char *const_write = writelen_temp;
    len += snprintf(buf.get() + len, buf_size - len, "%s", const_write);
  }

  for (const auto& file : files_) {
    // use crc32c for now, switch to something else if needed
    // WART: The checksums are crc32c, not original crc32

    size_t newlen =
        len + file->filename.length() +
        snprintf(writelen_temp, sizeof(writelen_temp), " crc32 %u\n",
                 ChecksumHexToInt32(file->checksum_hex));
    const char* const_write = writelen_temp;
    if (newlen >= buf_size) {
      backup_meta_file->Append(Slice(buf.get(), len));
      buf.reset();
      std::unique_ptr<char[]> new_reset_buf(
          new char[max_backup_meta_file_size_]);
      buf.swap(new_reset_buf);
      len = 0;
    }
    len += snprintf(buf.get() + len, buf_size - len, "%s%s",
                    file->filename.c_str(), const_write);
  }

  s = backup_meta_file->Append(Slice(buf.get(), len));
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

// -------- BackupEngineReadOnlyImpl ---------
class BackupEngineReadOnlyImpl : public BackupEngineReadOnly {
 public:
  BackupEngineReadOnlyImpl(const BackupableDBOptions& options, Env* db_env)
      : backup_engine_(new BackupEngineImpl(options, db_env, true)) {}

  ~BackupEngineReadOnlyImpl() override {}

  // The returned BackupInfos are in chronological order, which means the
  // latest backup comes last.
  void GetBackupInfo(std::vector<BackupInfo>* backup_info) override {
    backup_engine_->GetBackupInfo(backup_info);
  }

  void GetCorruptedBackups(std::vector<BackupID>* corrupt_backup_ids) override {
    backup_engine_->GetCorruptedBackups(corrupt_backup_ids);
  }

  using BackupEngineReadOnly::RestoreDBFromBackup;
  Status RestoreDBFromBackup(const RestoreOptions& options, BackupID backup_id,
                             const std::string& db_dir,
                             const std::string& wal_dir) override {
    return backup_engine_->RestoreDBFromBackup(options, backup_id, db_dir,
                                               wal_dir);
  }

  using BackupEngineReadOnly::RestoreDBFromLatestBackup;
  Status RestoreDBFromLatestBackup(const RestoreOptions& options,
                                   const std::string& db_dir,
                                   const std::string& wal_dir) override {
    return backup_engine_->RestoreDBFromLatestBackup(options, db_dir, wal_dir);
  }

  Status VerifyBackup(BackupID backup_id,
                      bool verify_with_checksum = false) override {
    return backup_engine_->VerifyBackup(backup_id, verify_with_checksum);
  }

  Status Initialize() { return backup_engine_->Initialize(); }

 private:
  std::unique_ptr<BackupEngineImpl> backup_engine_;
};

Status BackupEngineReadOnly::Open(const BackupableDBOptions& options, Env* env,
                                  BackupEngineReadOnly** backup_engine_ptr) {
  if (options.destroy_old_data) {
    return Status::InvalidArgument(
        "Can't destroy old data with ReadOnly BackupEngine");
  }
  std::unique_ptr<BackupEngineReadOnlyImpl> backup_engine(
      new BackupEngineReadOnlyImpl(options, env));
  auto s = backup_engine->Initialize();
  if (!s.ok()) {
    *backup_engine_ptr = nullptr;
    return s;
  }
  *backup_engine_ptr = backup_engine.release();
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
