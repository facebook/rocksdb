#include "utilities/titandb/version_set.h"

#include <inttypes.h>

#include "util/autovector.h"
#include "util/filename.h"

namespace rocksdb {
namespace titandb {

const size_t kMaxFileCacheSize = 1024 * 1024;

VersionSet::VersionSet(const TitanDBOptions& options)
    : dirname_(options.dirname),
      env_(options.env),
      env_options_(options),
      db_options_(options) {
  auto file_cache_size = db_options_.max_open_files;
  if (file_cache_size < 0) {
    file_cache_size = kMaxFileCacheSize;
  }
  file_cache_ = NewLRUCache(file_cache_size);
}

Status VersionSet::Open(
    const std::map<uint32_t, TitanCFOptions>& column_families) {
  // Sets up initial column families.
  AddColumnFamilies(column_families);

  Status s = env_->FileExists(CurrentFileName(dirname_));
  if (s.ok()) {
    return Recover();
  }
  if (!s.IsNotFound()) {
    return s;
  }
  return OpenManifest(NewFileNumber());
}

Status VersionSet::Recover() {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t, const Status& s) override {
      if (status->ok()) *status = s;
    }
  };

  // Reads "CURRENT" file, which contains the name of the current manifest file.
  std::string manifest;
  Status s = ReadFileToString(env_, CurrentFileName(dirname_), &manifest);
  if (!s.ok()) return s;
  if (manifest.empty() || manifest.back() != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  manifest.resize(manifest.size() - 1);

  // Opens the current manifest file.
  auto file_name = dirname_ + "/" + manifest;
  std::unique_ptr<SequentialFileReader> file;
  {
    std::unique_ptr<SequentialFile> f;
    s = env_->NewSequentialFile(file_name, &f,
                                env_->OptimizeForManifestRead(env_options_));
    if (!s.ok()) return s;
    file.reset(new SequentialFileReader(std::move(f), file_name));
  }

  bool has_next_file_number = false;
  uint64_t next_file_number = 0;

  // Reads edits from the manifest and applies them one by one.
  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(nullptr, std::move(file), &reporter, true /*checksum*/,
                       0 /*initial_offset*/, 0);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = DecodeInto(record, &edit);
      if (!s.ok()) return s;
      Apply(&edit);
      if (edit.has_next_file_number_) {
        assert(edit.next_file_number_ >= next_file_number);
        next_file_number = edit.next_file_number_;
        has_next_file_number = true;
      }
    }
  }

  if (!has_next_file_number) {
    return Status::Corruption("no next file number in manifest file");
  }
  next_file_number_.store(next_file_number);

  auto new_manifest_file_number = NewFileNumber();
  s = OpenManifest(new_manifest_file_number);
  if (!s.ok()) return s;

  // Purge inactive files at start
  std::set<uint64_t> alive_files;
  alive_files.insert(new_manifest_file_number);
  for (const auto& bs : column_families_) {
    autovector<uint64_t> obsolete_files;
     for (const auto& f : bs.second->files_) {
      if (f.second->is_obsolete()) {
        // delete already obsoleted files at reopen
        obsolete_files.push_back(f.second->file_number());
        for (auto it = obsolete_files_.blob_files.begin(); it != obsolete_files_.blob_files.end(); ++it) {
          if (std::get<0>(*it) == f.second->file_number())  {
            it = this->obsolete_files_.blob_files.erase(it);
            break;
          }
        }
      } else {
        alive_files.insert(f.second->file_number());
      }
    }
    for (uint64_t obsolete_file : obsolete_files) {
      bs.second->DeleteBlobFile(obsolete_file);
    }
  }
  std::vector<std::string> files;
  env_->GetChildren(dirname_, &files);
  for (const auto& f : files) {
    uint64_t file_number;
    FileType file_type;
    if (!ParseFileName(f, &file_number, &file_type)) continue;
    if (alive_files.find(file_number) != alive_files.end()) continue;
    if (file_type != FileType::kBlobFile &&
        file_type != FileType::kDescriptorFile)
      continue;

    env_->DeleteFile(dirname_ + "/" + f);
  }

  // Make sure perform gc on all files at the beginning
  MarkAllFilesForGC();

  return Status::OK();
}

Status VersionSet::OpenManifest(uint64_t file_number) {
  Status s;

  auto file_name = DescriptorFileName(dirname_, file_number);
  std::unique_ptr<WritableFileWriter> file;
  {
    std::unique_ptr<WritableFile> f;
    s = env_->NewWritableFile(file_name, &f, env_options_);
    if (!s.ok()) return s;
    file.reset(new WritableFileWriter(std::move(f), file_name, env_options_));
  }

  manifest_.reset(new log::Writer(std::move(file), 0, false));

  // Saves current snapshot
  s = WriteSnapshot(manifest_.get());
  if (s.ok()) {
    ImmutableDBOptions ioptions(db_options_);
    s = SyncManifest(env_, &ioptions, manifest_->file());
  }
  if (s.ok()) {
    // Makes "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dirname_, file_number, nullptr);
  }

  if (!s.ok()) {
    manifest_.reset();
    obsolete_files_.manifests.emplace_back(file_name);
  }
  return s;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  Status s;
  // Saves global information
  {
    VersionEdit edit;
    edit.SetNextFileNumber(next_file_number_.load());
    std::string record;
    edit.EncodeTo(&record);
    s = log->AddRecord(record);
    if (!s.ok()) return s;
  }
  // Saves column families information
  for (auto& it : this->column_families_) {
    VersionEdit edit;
    edit.SetColumnFamilyID(it.first);
    for (auto& file : it.second->files_) {
      // skip obsolete file
      if (file.second->is_obsolete()) {
        continue;
      }
      edit.AddBlobFile(file.second);
    }
    std::string record;
    edit.EncodeTo(&record);
    s = log->AddRecord(record);
    if (!s.ok()) return s;
  }
  return s;
}

Status VersionSet::LogAndApply(VersionEdit* edit) {
  // TODO(@huachao): write manifest file unlocked
  std::string record;
  edit->SetNextFileNumber(next_file_number_.load());
  edit->EncodeTo(&record);
  Status s = manifest_->AddRecord(record);
  if (s.ok()) {
    ImmutableDBOptions ioptions(db_options_);
    s = SyncManifest(env_, &ioptions, manifest_->file());
  }
  if (!s.ok()) return s;

  Apply(edit);
  return s;
}

void VersionSet::Apply(VersionEdit* edit) {
  auto cf_id = edit->column_family_id_;
  auto it = column_families_.find(cf_id);
  if (it == column_families_.end()) {
    // Ignore unknown column families.
    return;
  }
  auto& files = it->second->files_;

  for (auto& file : edit->deleted_files_) {
    auto number = file.first;
    auto blob_it = files.find(number);
    if (blob_it == files.end()) {
      fprintf(stderr, "blob file %" PRIu64 " doesn't exist before\n", number);
      abort();
    } else if (blob_it->second->is_obsolete()) {
      fprintf(stderr, "blob file %" PRIu64 " has been deleted before\n", number);
      abort();
    }
    MarkFileObsolete(blob_it->second, file.second, cf_id);
  }

  for (auto& file : edit->added_files_) {
    auto number = file->file_number();
    auto blob_it = files.find(number);
    if (blob_it != files.end()) {
      if (blob_it->second->is_obsolete()) {
        fprintf(stderr, "blob file %" PRIu64 " has been deleted before\n", number);
      } else {
        fprintf(stderr, "blob file %" PRIu64 " has been added before\n", number);
      }
      abort();
    }
    it->second->AddBlobFile(file);
  }

  it->second->ComputeGCScore();
}

void VersionSet::AddColumnFamilies(const std::map<uint32_t, TitanCFOptions>& column_families) {
  for (auto& cf : column_families) {
    auto file_cache =
        std::make_shared<BlobFileCache>(db_options_, cf.second, file_cache_);
    auto blob_storage = std::make_shared<BlobStorage>(cf.second, file_cache);
    column_families_.emplace(cf.first, blob_storage);
  }
}

void VersionSet::DropColumnFamilies(const std::vector<uint32_t>& column_families, SequenceNumber obsolete_sequence) {
  for (auto& cf : column_families) {
    auto it = column_families_.find(cf);
    if (it != column_families_.end()) {
      VersionEdit edit;
      edit.SetColumnFamilyID(it->first);
      for (auto& file: it->second->files_) {
        ROCKS_LOG_INFO(db_options_.info_log, "Titan add obsolete file [%llu]",
          file.second->file_number());
        edit.DeleteBlobFile(file.first, obsolete_sequence);
      }
      // TODO: check status
      LogAndApply(&edit);
    }
    obsolete_columns_.insert(cf);
  }       
}

void VersionSet::MarkFileObsolete(std::shared_ptr<BlobFileMeta> file, SequenceNumber obsolete_sequence, uint32_t cf_id) {
    obsolete_files_.blob_files.push_back(std::make_tuple(file->file_number(), obsolete_sequence, cf_id));
    file->FileStateTransit(BlobFileMeta::FileEvent::kDelete);
}

void VersionSet::GetObsoleteFiles(ObsoleteFiles* obsolete_files, SequenceNumber oldest_sequence) {
  for (auto tuple_it = obsolete_files_.blob_files.begin(); tuple_it != obsolete_files_.blob_files.end();) {
    auto& obsolete_sequence = std::get<1>(*tuple_it);
    // We check whether the oldest snapshot is no less than the last sequence
    // by the time the blob file become obsolete. If so, the blob file is not
    // visible to all existing snapshots.
    if (oldest_sequence > obsolete_sequence) {
      auto& file_number = std::get<0>(*tuple_it);
      auto& cf_id = std::get<2>(*tuple_it);
      ROCKS_LOG_INFO(db_options_.info_log,
        "Obsolete blob file %" PRIu64 " (obsolete at %" PRIu64
        ") not visible to oldest snapshot %" PRIu64 ", delete it.",
        file_number, obsolete_sequence, oldest_sequence);
      // Cleanup obsolete column family when all the blob files for that are deleted.
      auto it = column_families_.find(cf_id);
      if (it != column_families_.end()) {
        it->second->DeleteBlobFile(file_number);
        if (it->second->files_.empty() && obsolete_columns_.find(cf_id) != obsolete_columns_.end()) {
          column_families_.erase(it);
          obsolete_columns_.erase(cf_id);
        }
      } else {
        fprintf(stderr, "column %u not found when deleting obsolete file%" PRIu64 "\n", 
          cf_id, file_number);
        abort();        
      }
      auto now = tuple_it++;
      obsolete_files->blob_files.splice(obsolete_files->blob_files.end(), obsolete_files_.blob_files, now);
    } else {
      ++tuple_it;
    }
  }
  obsolete_files_.manifests.swap(obsolete_files->manifests);
}

}  // namespace titandb
}  // namespace rocksdb
