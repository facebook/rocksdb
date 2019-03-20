#pragma once

#include "utilities/titandb/version.h"
#include "utilities/titandb/version_edit.h"

namespace rocksdb {
namespace titandb {

class VersionBuilder {
 public:
  // Constructs a builder to build on the base version. The
  // intermediate result is kept in the builder and the base version
  // is left unchanged.
  VersionBuilder(Version* base);

  ~VersionBuilder();

  // Applies "*edit" on the current state.
  void Apply(VersionEdit* edit);

  // Saves the current state to the version "*v".
  void SaveTo(Version* v);

 private:
  friend class VersionTest;

  class Builder {
   public:
    Builder(std::shared_ptr<BlobStorage> base) : base_(base) {}

    void AddFile(const std::shared_ptr<BlobFileMeta>& file);
    void DeleteFile(uint64_t number);

    std::shared_ptr<BlobStorage> Build();

   private:
    std::weak_ptr<BlobStorage> base_;
    std::map<uint64_t, std::shared_ptr<BlobFileMeta>> added_files_;
    std::set<uint64_t> deleted_files_;
  };

  Version* base_;
  std::map<uint32_t, Builder> column_families_;
};

}  // namespace titandb
}  // namespace rocksdb
