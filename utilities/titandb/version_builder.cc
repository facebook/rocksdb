#include "utilities/titandb/version_builder.h"

#include <inttypes.h>

namespace rocksdb {
namespace titandb {

void VersionBuilder::Builder::AddFile(
    const std::shared_ptr<BlobFileMeta>& file) {
  auto number = file->file_number();
  auto sb = base_.lock();
  if (sb->files_.find(number) != sb->files_.end() ||
      added_files_.find(number) != added_files_.end()) {
    fprintf(stderr, "blob file %" PRIu64 " has been added before\n", number);
    abort();
  }
  if (deleted_files_.find(number) != deleted_files_.end()) {
    fprintf(stderr, "blob file %" PRIu64 " has been deleted before\n", number);
    abort();
  }
  added_files_.emplace(number, file);
}

void VersionBuilder::Builder::DeleteFile(uint64_t number) {
  auto sb = base_.lock();
  if (sb->files_.find(number) == sb->files_.end() &&
      added_files_.find(number) == added_files_.end()) {
    fprintf(stderr, "blob file %" PRIu64 " doesn't exist before\n", number);
    abort();
  }
  if (deleted_files_.find(number) != deleted_files_.end()) {
    fprintf(stderr, "blob file %" PRIu64 " has been deleted before\n", number);
    abort();
  }
  deleted_files_.emplace(number);
}

std::shared_ptr<BlobStorage> VersionBuilder::Builder::Build() {
  // If nothing is changed, we can reuse the base;
  if (added_files_.empty() && deleted_files_.empty()) {
    return base_.lock();
  }

  auto vs = std::make_shared<BlobStorage>(*base_.lock());
  vs->AddBlobFiles(added_files_);
  vs->DeleteBlobFiles(deleted_files_);
  vs->ComputeGCScore();
  return vs;
}

VersionBuilder::VersionBuilder(Version* base) : base_(base) {
  base_->Ref();
  for (auto& it : base_->column_families_) {
    column_families_.emplace(it.first, Builder(it.second));
  }
}

VersionBuilder::~VersionBuilder() { base_->Unref(); }

void VersionBuilder::Apply(VersionEdit* edit) {
  auto cf_id = edit->column_family_id_;
  auto it = column_families_.find(cf_id);
  if (it == column_families_.end()) {
    // Ignore unknown column families.
    return;
  }
  auto& builder = it->second;

  for (auto& file : edit->deleted_files_) {
    builder.DeleteFile(file);
  }
  for (auto& file : edit->added_files_) {
    builder.AddFile(file);
  }
}

void VersionBuilder::SaveTo(Version* v) {
  v->column_families_.clear();
  for (auto& it : column_families_) {
    v->column_families_.emplace(it.first, it.second.Build());
  }
}

}  // namespace titandb
}  // namespace rocksdb
