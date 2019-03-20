#pragma once

#include <stdint.h>
#include <atomic>

#include "db/log_reader.h"
#include "db/log_writer.h"
#include "port/port_posix.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "util/mutexlock.h"
#include "utilities/titandb/blob_file_cache.h"
#include "utilities/titandb/options.h"
#include "utilities/titandb/version.h"
#include "utilities/titandb/version_builder.h"
#include "utilities/titandb/version_edit.h"

namespace rocksdb {
namespace titandb {

struct ObsoleteFiles {
  ObsoleteFiles() = default;

  ObsoleteFiles(const ObsoleteFiles&) = delete;
  ObsoleteFiles& operator=(const ObsoleteFiles&) = delete;
  ObsoleteFiles(ObsoleteFiles&&) = delete;
  ObsoleteFiles& operator=(ObsoleteFiles&&) = delete;

  void Swap(ObsoleteFiles* obsolete_file) {
    blob_files.swap(obsolete_file->blob_files);
    manifests.swap(obsolete_file->manifests);
  }

  std::vector<uint32_t> blob_files;
  std::vector<std::string> manifests;
};

class VersionSet {
 public:
  explicit VersionSet(const TitanDBOptions& options);

  // Sets up the storage specified in "options.dirname".
  // If the manifest doesn't exist, it will create one.
  // If the manifest exists, it will recover from the latest one.
  // It is a corruption if the persistent storage contains data
  // outside of the provided column families.
  Status Open(const std::map<uint32_t, TitanCFOptions>& column_families);

  // Applies *edit on the current version to form a new version that is
  // both saved to the manifest and installed as the new current version.
  // REQUIRES: *mutex is held
  Status LogAndApply(VersionEdit* edit, port::Mutex* mutex);

  // Adds some column families with the specified options.
  // REQUIRES: mutex is held
  void AddColumnFamilies(
      const std::map<uint32_t, TitanCFOptions>& column_families);
  // Drops some column families. The obsolete files will be deleted in
  // background when they will not be accessed anymore.
  // REQUIRES: mutex is held
  void DropColumnFamilies(const std::vector<uint32_t>& column_families);

  // Returns the current version.
  Version* current() { return versions_.current(); }

  // Allocates a new file number.
  uint64_t NewFileNumber() { return next_file_number_.fetch_add(1); }

  // REQUIRES: mutex is held
  void GetObsoleteFiles(ObsoleteFiles* obsolete_files) {
    obsolete_files->Swap(&obsolete_files_);
  }

  void AddObsoleteBlobFiles(const std::vector<uint32_t>& blob_files) {
    obsolete_files_.blob_files.insert(obsolete_files_.blob_files.end(),
                                      blob_files.begin(), blob_files.end());
  }

 private:
  friend class BlobFileSizeCollectorTest;
  friend class VersionTest;

  Status Recover();

  Status OpenManifest(uint64_t number);

  Status WriteSnapshot(log::Writer* log);

  std::string dirname_;
  Env* env_;
  EnvOptions env_options_;
  TitanDBOptions db_options_;
  std::shared_ptr<Cache> file_cache_;
  // This field will be call when Version is destructed, so we have to make
  // sure this field is destructed after Version does.
  ObsoleteFiles obsolete_files_;

  VersionList versions_;
  std::unique_ptr<log::Writer> manifest_;
  std::atomic<uint64_t> next_file_number_{1};
};

}  // namespace titandb
}  // namespace rocksdb
