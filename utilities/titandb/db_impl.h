#pragma once

#include "db/db_impl.h"
#include "rocksdb/utilities/titandb/db.h"
#include "utilities/titandb/blob_file_manager.h"
#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

class TitanDBImpl : public TitanDB {
 public:
  TitanDBImpl(const TitanDBOptions& options, const std::string& dbname);

  ~TitanDBImpl();

  Status Open(const std::vector<TitanCFDescriptor>& descs,
              std::vector<ColumnFamilyHandle*>* handles);

  Status Close() override;

  using TitanDB::CreateColumnFamilies;
  Status CreateColumnFamilies(
      const std::vector<TitanCFDescriptor>& descs,
      std::vector<ColumnFamilyHandle*>* handles) override;

  Status DropColumnFamilies(
      const std::vector<ColumnFamilyHandle*>& handles) override;

  using TitanDB::CompactFiles;
  Status CompactFiles(
      const CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr,
      CompactionJobInfo* compaction_job_info = nullptr) override;

  Status CloseImpl();

  using TitanDB::Get;
  Status Get(const ReadOptions& options, ColumnFamilyHandle* handle,
             const Slice& key, PinnableSlice* value) override;

  using TitanDB::MultiGet;
  std::vector<Status> MultiGet(const ReadOptions& options,
                               const std::vector<ColumnFamilyHandle*>& handles,
                               const std::vector<Slice>& keys,
                               std::vector<std::string>* values) override;

  using TitanDB::NewIterator;
  Iterator* NewIterator(const ReadOptions& options,
                        ColumnFamilyHandle* handle) override;

  Status NewIterators(const ReadOptions& options,
                      const std::vector<ColumnFamilyHandle*>& handles,
                      std::vector<Iterator*>* iterators) override;

  const Snapshot* GetSnapshot() override;

  void ReleaseSnapshot(const Snapshot* snapshot) override;

  void OnFlushCompleted(const FlushJobInfo& flush_job_info);

  void OnCompactionCompleted(const CompactionJobInfo& compaction_job_info);

 private:
  class FileManager;
  friend class FileManager;
  friend class BlobGCJobTest;
  friend class BaseDbListener;

  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* handle,
                 const Slice& key, PinnableSlice* value);

  std::vector<Status> MultiGetImpl(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& handles,
      const std::vector<Slice>& keys, std::vector<std::string>* values);

  Iterator* NewIteratorImpl(const ReadOptions& options,
                            ColumnFamilyHandle* handle,
                            std::shared_ptr<ManagedSnapshot> snapshot);

  // REQUIRE: mutex_ held
  void AddToGCQueue(uint32_t column_family_id) {
    gc_queue_.push_back(column_family_id);
  }

  // REQUIRE: gc_queue_ not empty
  // REQUIRE: mutex_ held
  uint32_t PopFirstFromGCQueue() {
    assert(!gc_queue_.empty());
    auto column_family_id = *gc_queue_.begin();
    gc_queue_.pop_front();
    return column_family_id;
  }

  // REQUIRE: mutex_ held
  void MaybeScheduleGC();

  static void BGWorkGC(void* db);
  void BackgroundCallGC();
  Status BackgroundGC(LogBuffer* log_buffer);

  // REQUIRES: mutex_ held;
  void PurgeObsoleteFiles();

  FileLock* lock_{nullptr};
  // The lock sequence must be Titan.mutex_.Lock() -> Base DB mutex_.Lock()
  // while the unlock sequence must be Base DB mutex.Unlock() ->
  // Titan.mutex_.Unlock() Only if we all obey these sequence, we can prevent
  // potential dead lock.
  port::Mutex mutex_;
  // This condition variable is signaled on these conditions:
  // * whenever bg_gc_scheduled_ goes down to 0
  port::CondVar bg_cv_;

  std::string dbname_;
  std::string dirname_;
  Env* env_;
  EnvOptions env_options_;
  DBImpl* db_impl_;
  TitanDBOptions db_options_;

  std::unique_ptr<VersionSet> vset_;
  std::set<uint64_t> pending_outputs_;
  std::shared_ptr<BlobFileManager> blob_manager_;

  // gc_queue_ hold column families that we need to gc.
  // pending_gc_ hold column families that already on gc_queue_.
  std::deque<uint32_t> gc_queue_;

  std::atomic_int bg_gc_scheduled_{0};

  std::atomic_bool shuting_down_{false};

  std::unordered_set<ColumnFamilyData*> cfds_;
};

}  // namespace titandb
}  // namespace rocksdb
