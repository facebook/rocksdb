#include "utilities/titandb/db_impl.h"

#include "utilities/titandb/base_db_listener.h"
#include "utilities/titandb/blob_file_builder.h"
#include "utilities/titandb/blob_file_iterator.h"
#include "utilities/titandb/blob_file_size_collector.h"
#include "utilities/titandb/blob_gc.h"
#include "utilities/titandb/db_iter.h"
#include "utilities/titandb/table_factory.h"

namespace rocksdb {
namespace titandb {

class TitanDBImpl::FileManager : public BlobFileManager {
 public:
  FileManager(TitanDBImpl* db) : db_(db) {}

  Status NewFile(std::unique_ptr<BlobFileHandle>* handle) override {
    auto number = db_->vset_->NewFileNumber();
    auto name = BlobFileName(db_->dirname_, number);

    Status s;
    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<WritableFile> f;
      s = db_->env_->NewWritableFile(name, &f, db_->env_options_);
      if (!s.ok()) return s;
      file.reset(new WritableFileWriter(std::move(f), name, db_->env_options_));
    }

    handle->reset(new FileHandle(number, name, std::move(file)));
    {
      MutexLock l(&db_->mutex_);
      db_->pending_outputs_.insert(number);
    }
    return s;
  }

  Status BatchFinishFiles(
      uint32_t cf_id,
      const std::vector<std::pair<std::shared_ptr<BlobFileMeta>,
                                  std::unique_ptr<BlobFileHandle>>>& files)
      override {
    Status s;
    VersionEdit edit;
    edit.SetColumnFamilyID(cf_id);
    for (auto& file : files) {
      s = file.second->GetFile()->Sync(false);
      if (s.ok()) {
        s = file.second->GetFile()->Close();
      }
      if (!s.ok()) return s;

      edit.AddBlobFile(file.first);
    }

    {
      MutexLock l(&db_->mutex_);
      s = db_->vset_->LogAndApply(&edit, &db_->mutex_);
      for (const auto& file : files)
        db_->pending_outputs_.erase(file.second->GetNumber());
    }
    return s;
  }

  Status BatchDeleteFiles(
      const std::vector<std::unique_ptr<BlobFileHandle>>& handles) override {
    Status s;
    for (auto& handle : handles) s = db_->env_->DeleteFile(handle->GetName());
    {
      MutexLock l(&db_->mutex_);
      for (const auto& handle : handles)
        db_->pending_outputs_.erase(handle->GetNumber());
    }
    return s;
  }

 private:
  class FileHandle : public BlobFileHandle {
   public:
    FileHandle(uint64_t number, const std::string& name,
               std::unique_ptr<WritableFileWriter> file)
        : number_(number), name_(name), file_(std::move(file)) {}

    uint64_t GetNumber() const override { return number_; }

    const std::string& GetName() const override { return name_; }

    WritableFileWriter* GetFile() const override { return file_.get(); }

   private:
    uint64_t number_;
    std::string name_;
    std::unique_ptr<WritableFileWriter> file_;
  };

  TitanDBImpl* db_;
};

TitanDBImpl::TitanDBImpl(const TitanDBOptions& options,
                         const std::string& dbname)
    : TitanDB(),
      mutex_(),
      bg_cv_(&mutex_),
      dbname_(dbname),
      env_(options.env),
      env_options_(options),
      db_options_(options) {
  if (db_options_.dirname.empty()) {
    db_options_.dirname = dbname_ + "/titandb";
  }
  dirname_ = db_options_.dirname;
  vset_.reset(new VersionSet(db_options_));
  blob_manager_.reset(new FileManager(this));
}

TitanDBImpl::~TitanDBImpl() { Close(); }

Status TitanDBImpl::Open(const std::vector<TitanCFDescriptor>& descs,
                         std::vector<ColumnFamilyHandle*>* handles) {
  // Sets up directories for base DB and Titan.
  Status s = env_->CreateDirIfMissing(dbname_);
  if (!s.ok()) return s;
  if (!db_options_.info_log) {
    s = CreateLoggerFromOptions(dbname_, db_options_, &db_options_.info_log);
    if (!s.ok()) return s;
  }
  s = env_->CreateDirIfMissing(dirname_);
  if (!s.ok()) return s;
  s = env_->LockFile(LockFileName(dirname_), &lock_);
  if (!s.ok()) return s;

  std::vector<ColumnFamilyDescriptor> base_descs;
  for (auto& desc : descs) {
    base_descs.emplace_back(desc.name, desc.options);
  }
  std::map<uint32_t, TitanCFOptions> column_families;

  // Opens the base DB first to collect the column families information.
  // Avoid flush here because we haven't replaced the table factory yet.
  db_options_.avoid_flush_during_recovery = true;
  s = DB::Open(db_options_, dbname_, base_descs, handles, &db_);
  if (s.ok()) {
    for (size_t i = 0; i < descs.size(); i++) {
      auto handle = (*handles)[i];
      column_families.emplace(handle->GetID(), descs[i].options);
      db_->DestroyColumnFamilyHandle(handle);
      // Replaces the provided table factory with TitanTableFactory.
      // While we need to preserve original table_factory for SstFileWriter
      base_descs[i].options.original_table_factory =
          base_descs[i].options.table_factory;
      base_descs[i].options.table_factory = std::make_shared<TitanTableFactory>(
          db_options_, descs[i].options, blob_manager_);
      // Add TableProperties for collecting statistics GC
      base_descs[i].options.table_properties_collector_factories.emplace_back(
          std::make_shared<BlobFileSizeCollectorFactory>());
    }
    handles->clear();
    s = db_->Close();
    delete db_;
  }
  if (!s.ok()) return s;

  s = vset_->Open(column_families);
  if (!s.ok()) return s;

  // Add EventListener to collect statistics for GC
  db_options_.listeners.emplace_back(std::make_shared<BaseDbListener>(this));

  static bool has_init_background_threads = false;
  if (!has_init_background_threads) {
    auto low_pri_threads_num = env_->GetBackgroundThreads(Env::Priority::LOW);
    assert(low_pri_threads_num > 0);
    if (!db_options_.disable_background_gc &&
        db_options_.max_background_gc > 0) {
      env_->IncBackgroundThreadsIfNeeded(
          db_options_.max_background_gc + low_pri_threads_num,
          Env::Priority::LOW);
      assert(env_->GetBackgroundThreads(Env::Priority::LOW) ==
             low_pri_threads_num + db_options_.max_background_gc);
    }
    has_init_background_threads = true;
  }

  s = DB::Open(db_options_, dbname_, base_descs, handles, &db_);
  if (s.ok()) {
    db_impl_ = reinterpret_cast<DBImpl*>(db_->GetRootDB());
    for (auto handle : *handles) {
      auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(handle)->cfd();
      cfds_.insert(cfd);
    }
  }
  return s;
}

Status TitanDBImpl::Close() {
  Status s;
  CloseImpl();
  if (db_) {
    s = db_->Close();
    delete db_;
    db_ = nullptr;
    db_impl_ = nullptr;
  }
  if (lock_) {
    env_->UnlockFile(lock_);
    lock_ = nullptr;
  }
  return s;
}

Status TitanDBImpl::CloseImpl() {
  {
    MutexLock l(&mutex_);
    // Although `shuting_down_` is atomic bool object, we should set it under
    // the protection of mutex_, otherwise, there maybe something wrong with it,
    // like:
    // 1, A thread: shuting_down_.load = false
    // 2, B thread: shuting_down_.store(true)
    // 3, B thread: unschedule all bg work
    // 4, A thread: schedule bg work
    shuting_down_.store(true, std::memory_order_release);
  }

  int gc_unscheduled = env_->UnSchedule(this, Env::Priority::LOW);
  {
    MutexLock l(&mutex_);
    bg_gc_scheduled_ -= gc_unscheduled;
    while (bg_gc_scheduled_ > 0) {
      bg_cv_.Wait();
    }
  }

  return Status::OK();
}

Status TitanDBImpl::CreateColumnFamilies(
    const std::vector<TitanCFDescriptor>& descs,
    std::vector<ColumnFamilyHandle*>* handles) {
  std::vector<ColumnFamilyDescriptor> base_descs;
  for (auto& desc : descs) {
    ColumnFamilyOptions options = desc.options;
    // Replaces the provided table factory with TitanTableFactory.
    options.table_factory.reset(
        new TitanTableFactory(db_options_, desc.options, blob_manager_));
    base_descs.emplace_back(desc.name, options);
  }

  MutexLock l(&mutex_);

  Status s = db_impl_->CreateColumnFamilies(base_descs, handles);
  assert(handles->size() == descs.size());
  if (s.ok()) {
    std::map<uint32_t, TitanCFOptions> column_families;
    for (size_t i = 0; i < descs.size(); i++) {
      column_families.emplace((*handles)[i]->GetID(), descs[i].options);
      auto* cfd =
          reinterpret_cast<ColumnFamilyHandleImpl*>((*handles)[i])->cfd();
      cfds_.insert(cfd);
    }
    vset_->AddColumnFamilies(column_families);
  }
  return s;
}

Status TitanDBImpl::DropColumnFamilies(
    const std::vector<ColumnFamilyHandle*>& handles) {
  std::vector<uint32_t> column_families;
  std::vector<ColumnFamilyData*> cfds;
  for (auto& handle : handles) {
    column_families.push_back(handle->GetID());
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(handle)->cfd();
    cfds.push_back(cfd);
  }

  MutexLock l(&mutex_);

  Status s = db_impl_->DropColumnFamilies(handles);
  if (s.ok()) {
    vset_->DropColumnFamilies(column_families);
    for (auto cfd : cfds) {
      cfds_.erase(cfd);
    }
  }
  return s;
}

Status TitanDBImpl::CompactFiles(
    const CompactionOptions& compact_options, ColumnFamilyHandle* column_family,
    const std::vector<std::string>& input_file_names, const int output_level,
    const int output_path_id, std::vector<std::string>* const output_file_names,
    CompactionJobInfo* compaction_job_info) {
  std::unique_ptr<CompactionJobInfo> compaction_job_info_ptr;
  if (compaction_job_info == nullptr) {
    compaction_job_info_ptr.reset(new CompactionJobInfo());
    compaction_job_info = compaction_job_info_ptr.get();
  }
  auto s = db_impl_->CompactFiles(
      compact_options, column_family, input_file_names, output_level,
      output_path_id, output_file_names, compaction_job_info);
  if (s.ok()) {
    OnCompactionCompleted(*compaction_job_info);
  }

  return s;
}

Status TitanDBImpl::Get(const ReadOptions& options, ColumnFamilyHandle* handle,
                        const Slice& key, PinnableSlice* value) {
  if (options.snapshot) {
    return GetImpl(options, handle, key, value);
  }
  ReadOptions ro(options);
  ManagedSnapshot snapshot(this);
  ro.snapshot = snapshot.snapshot();
  return GetImpl(ro, handle, key, value);
}

Status TitanDBImpl::GetImpl(const ReadOptions& options,
                            ColumnFamilyHandle* handle, const Slice& key,
                            PinnableSlice* value) {
  auto snap = reinterpret_cast<const TitanSnapshot*>(options.snapshot);
  auto storage = snap->current()->GetBlobStorage(handle->GetID()).lock();

  auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(handle)->cfd();
  auto* sv = snap->GetSuperVersion(cfd);
  if (sv == nullptr) {
    fprintf(stderr, "GetSuperVersion failed\n");
    abort();
  }

  Status s;
  bool is_blob_index = false;
  s = db_impl_->GetImpl(options, handle, key, value, nullptr /*value_found*/,
                        nullptr /*read_callback*/, &is_blob_index, sv);
  if (!s.ok() || !is_blob_index) return s;

  BlobIndex index;
  s = index.DecodeFrom(value);
  assert(s.ok());
  if (!s.ok()) return s;

  BlobRecord record;
  PinnableSlice buffer;
  s = storage->Get(options, index, &record, &buffer);
  if (s.IsCorruption()) {
    fprintf(stderr, "Key:%s Snapshot:%lu GetBlobFile err:%s\n",
            key.ToString(true).c_str(),
            static_cast<std::size_t>(options.snapshot->GetSequenceNumber()),
            s.ToString().c_str());
    abort();
  }
  if (s.ok()) {
    value->Reset();
    value->PinSelf(record.value);
  }
  return s;
}

std::vector<Status> TitanDBImpl::MultiGet(
    const ReadOptions& options, const std::vector<ColumnFamilyHandle*>& handles,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  auto options_copy = options;
  options_copy.total_order_seek = true;
  if (options_copy.snapshot) {
    return MultiGetImpl(options_copy, handles, keys, values);
  }
  ReadOptions ro(options_copy);
  ManagedSnapshot snapshot(this);
  ro.snapshot = snapshot.snapshot();
  return MultiGetImpl(ro, handles, keys, values);
}

std::vector<Status> TitanDBImpl::MultiGetImpl(
    const ReadOptions& options, const std::vector<ColumnFamilyHandle*>& handles,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  std::vector<Status> res;
  res.resize(keys.size());
  values->resize(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    auto value = &(*values)[i];
    PinnableSlice pinnable_value(value);
    res[i] = GetImpl(options, handles[i], keys[i], &pinnable_value);
    if (res[i].ok() && pinnable_value.IsPinned()) {
      value->assign(pinnable_value.data(), pinnable_value.size());
    }
  }
  return res;
}

Iterator* TitanDBImpl::NewIterator(const ReadOptions& options,
                                   ColumnFamilyHandle* handle) {
  ReadOptions options_copy = options;
  options_copy.total_order_seek = true;
  std::shared_ptr<ManagedSnapshot> snapshot;
  if (options_copy.snapshot) {
    return NewIteratorImpl(options_copy, handle, snapshot);
  }
  ReadOptions ro(options_copy);
  snapshot.reset(new ManagedSnapshot(this));
  ro.snapshot = snapshot->snapshot();
  return NewIteratorImpl(ro, handle, snapshot);
}

Iterator* TitanDBImpl::NewIteratorImpl(
    const ReadOptions& options, ColumnFamilyHandle* handle,
    std::shared_ptr<ManagedSnapshot> snapshot) {
  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(handle)->cfd();
  auto snap = reinterpret_cast<const TitanSnapshot*>(options.snapshot);
  auto storage = snap->current()->GetBlobStorage(handle->GetID());
  auto* sv = snap->GetSuperVersion(cfd);
  if (sv == nullptr) {
    return nullptr;
  }
  std::unique_ptr<ArenaWrappedDBIter> iter(db_impl_->NewIteratorImpl(
      options, cfd, snap->GetSequenceNumber(), nullptr /*read_callback*/,
      true /*allow_blob*/, true /*allow_refresh*/, sv));
  return new TitanDBIterator(options, storage.lock().get(), snapshot,
                             std::move(iter));
}

Status TitanDBImpl::NewIterators(
    const ReadOptions& options, const std::vector<ColumnFamilyHandle*>& handles,
    std::vector<Iterator*>* iterators) {
  ReadOptions ro(options);
  ro.total_order_seek = true;
  std::shared_ptr<ManagedSnapshot> snapshot;
  if (!ro.snapshot) {
    snapshot.reset(new ManagedSnapshot(this));
    ro.snapshot = snapshot->snapshot();
  }
  iterators->clear();
  iterators->reserve(handles.size());
  for (auto& handle : handles) {
    iterators->emplace_back(NewIteratorImpl(ro, handle, snapshot));
  }
  return Status::OK();
}

const Snapshot* TitanDBImpl::GetSnapshot() {
  Version* current;
  const Snapshot* snapshot;
  std::map<ColumnFamilyData*, SuperVersion*> svs;
  {
    MutexLock l(&mutex_);
    current = vset_->current();
    current->Ref();
    snapshot = db_->GetSnapshot();
    for (auto cfd : cfds_) {
      svs.emplace(cfd, db_impl_->GetReferencedSuperVersion(cfd));
    }
  }
  return new TitanSnapshot(current, snapshot, &svs);
}

void TitanDBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  auto s = reinterpret_cast<const TitanSnapshot*>(snapshot);
  db_->ReleaseSnapshot(s->snapshot());
  for (auto sv : s->svs()) {
    db_impl_->CleanupSuperVersion(sv.second);
  }

  s->current()->Unref();

  delete s;
}

void TitanDBImpl::OnFlushCompleted(const FlushJobInfo& flush_job_info) {
  const auto& tps = flush_job_info.table_properties;
  auto ucp_iter = tps.user_collected_properties.find(
      BlobFileSizeCollector::kPropertiesName);
  // sst file doesn't contain any blob index
  if (ucp_iter == tps.user_collected_properties.end()) {
    return;
  }
  std::map<uint64_t, uint64_t> blob_files_size;
  Slice src{ucp_iter->second};
  if (!BlobFileSizeCollector::Decode(&src, &blob_files_size)) {
    fprintf(stderr, "BlobFileSizeCollector::Decode failed size:%lu\n",
            ucp_iter->second.size());
    abort();
  }
  assert(!blob_files_size.empty());
  std::set<uint64_t> outputs;
  for (const auto f : blob_files_size) {
    outputs.insert(f.first);
  }

  {
    MutexLock l(&mutex_);

    Version* current = vset_->current();
    current->Ref();
    auto blob_storage = current->GetBlobStorage(flush_job_info.cf_id).lock();
    if (!blob_storage) {
      fprintf(stderr, "Column family id:%u Not Found\n", flush_job_info.cf_id);
      abort();
    }
    for (const auto& file_number : outputs) {
      auto file = blob_storage->FindFile(file_number).lock();
      // This file maybe output of a gc job, and it's been gced out.
      if (!file) {
        continue;
      }
      file->FileStateTransit(BlobFileMeta::FileEvent::kFlushCompleted);
    }
    current->Unref();
  }
}

void TitanDBImpl::OnCompactionCompleted(
    const CompactionJobInfo& compaction_job_info) {
  std::map<uint64_t, int64_t> blob_files_size;
  std::set<uint64_t> outputs;
  std::set<uint64_t> inputs;
  auto calc_bfs = [&compaction_job_info, &blob_files_size, &outputs, &inputs](
                      const std::vector<std::string>& files, int coefficient,
                      bool output) {
    for (const auto& file : files) {
      auto tp_iter = compaction_job_info.table_properties.find(file);
      if (tp_iter == compaction_job_info.table_properties.end()) {
        if (output) {
          fprintf(stderr, "can't find property for output\n");
          abort();
        }
        continue;
      }
      auto ucp_iter = tp_iter->second->user_collected_properties.find(
          BlobFileSizeCollector::kPropertiesName);
      // this sst file doesn't contain any blob index
      if (ucp_iter == tp_iter->second->user_collected_properties.end()) {
        continue;
      }
      std::map<uint64_t, uint64_t> input_blob_files_size;
      std::string s = ucp_iter->second;
      Slice slice{s};
      if (!BlobFileSizeCollector::Decode(&slice, &input_blob_files_size)) {
        fprintf(stderr, "BlobFileSizeCollector::Decode failed\n");
        abort();
      }
      for (const auto& input_bfs : input_blob_files_size) {
        if (output) {
          if (inputs.find(input_bfs.first) == inputs.end()) {
            outputs.insert(input_bfs.first);
          }
        } else {
          inputs.insert(input_bfs.first);
        }
        auto bfs_iter = blob_files_size.find(input_bfs.first);
        if (bfs_iter == blob_files_size.end()) {
          blob_files_size[input_bfs.first] = coefficient * input_bfs.second;
        } else {
          bfs_iter->second += coefficient * input_bfs.second;
        }
      }
    }
  };

  calc_bfs(compaction_job_info.input_files, -1, false);
  calc_bfs(compaction_job_info.output_files, 1, true);

  {
    MutexLock l(&mutex_);
    Version* current = vset_->current();
    current->Ref();
    auto bs = current->GetBlobStorage(compaction_job_info.cf_id).lock();
    if (!bs) {
      fprintf(stderr, "Column family id:%u Not Found\n",
              compaction_job_info.cf_id);
      current->Unref();
      return;
    }
    for (const auto& o : outputs) {
      auto file = bs->FindFile(o).lock();
      if (!file) {
        fprintf(stderr, "OnCompactionCompleted get file failed\n");
        abort();
      }
      file->FileStateTransit(BlobFileMeta::FileEvent::kCompactionCompleted);
    }

    for (const auto& bfs : blob_files_size) {
      // blob file size < 0 means discardable size > 0
      if (bfs.second >= 0) {
        continue;
      }
      auto file = bs->FindFile(bfs.first).lock();
      if (!file) {
        // file has been gc out
        continue;
      }
      file->AddDiscardableSize(static_cast<uint64_t>(-bfs.second));
    }
    bs->ComputeGCScore();
    current->Unref();

    AddToGCQueue(compaction_job_info.cf_id);
    MaybeScheduleGC();
  }
}

}  // namespace titandb
}  // namespace rocksdb
