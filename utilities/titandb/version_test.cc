#include "utilities/titandb/version.h"
#include "util/filename.h"
#include "util/testharness.h"
#include "utilities/titandb/testutil.h"
#include "utilities/titandb/util.h"
#include "utilities/titandb/version_builder.h"
#include "utilities/titandb/version_edit.h"
#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

void DeleteDir(Env* env, const std::string& dirname) {
  std::vector<std::string> filenames;
  env->GetChildren(dirname, &filenames);
  for (auto& fname : filenames) {
    uint64_t number;
    FileType type;
    if (ParseFileName(fname, &number, &type)) {
      ASSERT_OK(env->DeleteFile(dirname + "/" + fname));
    }
  }
  env->DeleteDir(dirname);
}

class VersionTest : public testing::Test {
 public:
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  std::unique_ptr<VersionList> versions_;
  std::shared_ptr<BlobFileCache> file_cache_;
  std::map<uint32_t, std::shared_ptr<BlobStorage>> column_families_;
  std::unique_ptr<VersionSet> vset_;
  port::Mutex mutex_;
  std::string dbname_;
  Env* env_;

  VersionTest() : dbname_(test::TmpDir()), env_(Env::Default()) {
    db_options_.dirname = dbname_ + "/titandb";
    db_options_.create_if_missing = true;
    env_->CreateDirIfMissing(dbname_);
    env_->CreateDirIfMissing(db_options_.dirname);
    auto cache = NewLRUCache(db_options_.max_open_files);
    file_cache_.reset(new BlobFileCache(db_options_, cf_options_, cache));
    Reset();
  }

  void Reset() {
    DeleteDir(env_, dbname_);
    vset_.reset(new VersionSet(db_options_));
    ASSERT_OK(vset_->Open({}));
    versions_.reset(new VersionList);
    column_families_.clear();
    // Sets up some column families.
    auto v = new Version(nullptr);
    for (uint32_t id = 0; id < 10; id++) {
      std::shared_ptr<BlobStorage> storage;
      storage.reset(new BlobStorage(cf_options_, file_cache_));
      column_families_.emplace(id, storage);
      storage.reset(new BlobStorage(cf_options_, file_cache_));
      v->column_families_.emplace(id, storage);
    }
    versions_->Append(v);
  }

  void AddBlobFiles(uint32_t cf_id, uint64_t start, uint64_t end) {
    auto storage = column_families_[cf_id];
    for (auto i = start; i < end; i++) {
      auto file = std::make_shared<BlobFileMeta>(i, i);
      storage->files_.emplace(i, file);
    }
  }

  void DeleteBlobFiles(uint32_t cf_id, uint64_t start, uint64_t end) {
    auto& storage = column_families_[cf_id];
    for (auto i = start; i < end; i++) {
      storage->files_.erase(i);
    }
  }

  void BuildAndCheck(std::vector<VersionEdit> edits) {
    VersionBuilder builder(versions_->current());
    for (auto& edit : edits) {
      builder.Apply(&edit);
    }
    Version* v = new Version(vset_.get());
    builder.SaveTo(v);
    versions_->Append(v);
    for (auto& it : v->column_families_) {
      auto& storage = column_families_[it.first];
      ASSERT_EQ(storage->files_.size(), it.second->files_.size());
      for (auto& f : storage->files_) {
        auto iter = it.second->files_.find(f.first);
        ASSERT_TRUE(iter != it.second->files_.end());
        ASSERT_EQ(*f.second, *(iter->second));
      }
    }
  }
};

TEST_F(VersionTest, VersionEdit) {
  VersionEdit input;
  CheckCodec(input);
  input.SetNextFileNumber(1);
  input.SetColumnFamilyID(2);
  CheckCodec(input);
  auto file1 = std::make_shared<BlobFileMeta>(3, 4);
  auto file2 = std::make_shared<BlobFileMeta>(5, 6);
  input.AddBlobFile(file1);
  input.AddBlobFile(file2);
  input.DeleteBlobFile(7);
  input.DeleteBlobFile(8);
  CheckCodec(input);
}

VersionEdit AddBlobFilesEdit(uint32_t cf_id, uint64_t start, uint64_t end) {
  VersionEdit edit;
  edit.SetColumnFamilyID(cf_id);
  for (auto i = start; i < end; i++) {
    auto file = std::make_shared<BlobFileMeta>(i, i);
    edit.AddBlobFile(file);
  }
  return edit;
}

VersionEdit DeleteBlobFilesEdit(uint32_t cf_id, uint64_t start, uint64_t end) {
  VersionEdit edit;
  edit.SetColumnFamilyID(cf_id);
  for (auto i = start; i < end; i++) {
    edit.DeleteBlobFile(i);
  }
  return edit;
}

TEST_F(VersionTest, VersionBuilder) {
  // {(0, 4)}, {}
  auto add1_0_4 = AddBlobFilesEdit(1, 0, 4);
  AddBlobFiles(1, 0, 4);
  BuildAndCheck({add1_0_4});

  // {(0, 8)}, {(4, 8)}
  auto add1_4_8 = AddBlobFilesEdit(1, 4, 8);
  auto add2_4_8 = AddBlobFilesEdit(2, 4, 8);
  AddBlobFiles(1, 4, 8);
  AddBlobFiles(2, 4, 8);
  BuildAndCheck({add1_4_8, add2_4_8});

  // {(0, 4), (6, 8)}, {(4, 8)}
  auto del1_4_6 = DeleteBlobFilesEdit(1, 4, 6);
  DeleteBlobFiles(1, 4, 6);
  BuildAndCheck({del1_4_6});

  // {(0, 4)}, {(4, 6)}
  auto del1_6_8 = DeleteBlobFilesEdit(1, 6, 8);
  auto del2_6_8 = DeleteBlobFilesEdit(2, 6, 8);
  DeleteBlobFiles(1, 6, 8);
  DeleteBlobFiles(2, 6, 8);
  BuildAndCheck({del1_6_8, del2_6_8});
  BuildAndCheck({add1_4_8, del1_4_6, del1_6_8});

  // {(0, 4)}, {(4, 6)}
  Reset();
  AddBlobFiles(1, 0, 4);
  AddBlobFiles(2, 4, 6);
  BuildAndCheck({add1_0_4, add1_4_8, del1_4_6, del1_6_8, add2_4_8, del2_6_8});
}

TEST_F(VersionTest, ObsoleteFiles) {
  std::map<uint32_t, TitanCFOptions> m;
  m.insert({1, TitanCFOptions()});
  vset_->AddColumnFamilies(m);
  {
    auto add1_0_4 = AddBlobFilesEdit(1, 0, 4);
    MutexLock l(&mutex_);
    vset_->LogAndApply(&add1_0_4, &mutex_);
  }
  ObsoleteFiles of;
  vset_->GetObsoleteFiles(&of);
  ASSERT_EQ(of.blob_files.size(), 0);
  {
    auto del1_3_4 = DeleteBlobFilesEdit(1, 3, 4);
    MutexLock l(&mutex_);
    vset_->LogAndApply(&del1_3_4, &mutex_);
  }
  vset_->GetObsoleteFiles(&of);
  ASSERT_EQ(of.blob_files.size(), 1);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
