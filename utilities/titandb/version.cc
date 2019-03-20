#include "utilities/titandb/version.h"
#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

Status BlobStorage::Get(const ReadOptions& options, const BlobIndex& index,
                        BlobRecord* record, PinnableSlice* buffer) {
  auto sfile = FindFile(index.file_number).lock();
  if (!sfile)
    return Status::Corruption("Missing blob file: " +
                              std::to_string(index.file_number));
  return file_cache_->Get(options, sfile->file_number(), sfile->file_size(),
                          index.blob_handle, record, buffer);
}

Status BlobStorage::NewPrefetcher(uint64_t file_number,
                                  std::unique_ptr<BlobFilePrefetcher>* result) {
  auto sfile = FindFile(file_number).lock();
  if (!sfile)
    return Status::Corruption("Missing blob wfile: " +
                              std::to_string(file_number));
  return file_cache_->NewPrefetcher(sfile->file_number(), sfile->file_size(),
                                    result);
}

std::weak_ptr<BlobFileMeta> BlobStorage::FindFile(uint64_t file_number) {
  auto it = files_.find(file_number);
  if (it != files_.end()) {
    assert(file_number == it->second->file_number());
    return it->second;
  }
  return std::weak_ptr<BlobFileMeta>();
}

void BlobStorage::ComputeGCScore() {
  gc_score_.clear();
  for (auto& file : files_) {
    gc_score_.push_back({});
    auto& gcs = gc_score_.back();
    gcs.file_number = file.first;
    //    if (file.second->marked_for_gc_) {
    //      gcs.score = 1;
    //      file.second->marked_for_gc_ = false;
    //    } else if (file.second->file_size() <
    if (file.second->file_size() <
        titan_cf_options_.merge_small_file_threshold) {
      gcs.score = 1;
    } else {
      gcs.score = file.second->GetDiscardableRatio();
    }
  }

  std::sort(gc_score_.begin(), gc_score_.end(),
            [](const GCScore& first, const GCScore& second) {
              return first.score > second.score;
            });
}

void BlobStorage::AddBlobFiles(
    const std::map<uint64_t, std::shared_ptr<BlobFileMeta>>& files) {
  files_.insert(files.begin(), files.end());
}

void BlobStorage::DeleteBlobFiles(const std::set<uint64_t>& files) {
  for (const auto& f : files) {
    files_.erase(f);
    file_cache_->Evict(f);
  }
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  // Close DB will also destruct this class and add live file to here.
  // But don't worry, ~Version will call after all our code executed.
  std::vector<uint32_t> obsolete_blob_files;
  for (auto& b : this->column_families_) {
    if (b.second.use_count() > 1) continue;
    for (auto& f : b.second->files_) {
      if (f.second.use_count() > 1) continue;
      // Maybe shutting down, so the file state is undefine
      // assert(f.second->file_state() == BlobFileMeta::FileState::kNormal);
      obsolete_blob_files.emplace_back(f.second->file_number());
    }
  }
  if (vset_ != nullptr) vset_->AddObsoleteBlobFiles(obsolete_blob_files);
}

void Version::Ref() { refs_.fetch_add(1, std::memory_order_relaxed); }

void Version::Unref() {
  int previous_refs = refs_.fetch_sub(1);
  assert(previous_refs > 0);
  if (previous_refs == 1) {
    delete this;
  }
}

std::weak_ptr<BlobStorage> Version::GetBlobStorage(uint32_t cf_id) {
  auto it = column_families_.find(cf_id);
  if (it != column_families_.end()) {
    return it->second;
  }
  return std::weak_ptr<BlobStorage>();
}

VersionList::VersionList() { Append(new Version(nullptr)); }

VersionList::~VersionList() {
  current_->Unref();
  assert(list_.prev_ == &list_);
  assert(list_.next_ == &list_);
}

void VersionList::Append(Version* v) {
  assert(v->refs_ == 0);
  assert(v != current_);

  if (current_) {
    current_->Unref();
  }
  current_ = v;
  current_->Ref();

  v->prev_ = list_.prev_;
  v->next_ = &list_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

}  // namespace titandb
}  // namespace rocksdb
