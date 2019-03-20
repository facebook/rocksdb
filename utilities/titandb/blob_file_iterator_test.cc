#include "utilities/titandb/blob_file_iterator.h"

#include <cinttypes>

#include "util/filename.h"
#include "util/testharness.h"
#include "utilities/titandb/blob_file_builder.h"
#include "utilities/titandb/blob_file_cache.h"
#include "utilities/titandb/blob_file_reader.h"

namespace rocksdb {
namespace titandb {

class BlobFileIteratorTest : public testing::Test {
 public:
  Env* env_{Env::Default()};
  TitanOptions titan_options_;
  EnvOptions env_options_;
  std::string dirname_;
  std::string file_name_;
  uint64_t file_number_;
  std::unique_ptr<BlobFileBuilder> builder_;
  std::unique_ptr<WritableFileWriter> writable_file_;
  std::unique_ptr<BlobFileIterator> blob_file_iterator_;
  std::unique_ptr<RandomAccessFileReader> readable_file_;

  BlobFileIteratorTest() : dirname_(test::TmpDir(env_)) {
    titan_options_.dirname = dirname_;
    file_number_ = Random::GetTLSInstance()->Next();
    file_name_ = BlobFileName(dirname_, file_number_);
  }

  ~BlobFileIteratorTest() {
    env_->DeleteFile(file_name_);
    env_->DeleteDir(dirname_);
  }

  std::string GenKey(uint64_t i) {
    char buf[64];
    snprintf(buf, sizeof(buf), "k-%08" PRIu64, i);
    return buf;
  }

  std::string GenValue(uint64_t k) {
    if (k % 2 == 0) {
      return std::string(titan_options_.min_blob_size - 1, 'v');
    } else {
      return std::string(titan_options_.min_blob_size + 1, 'v');
    }
  }

  void NewBuiler() {
    TitanDBOptions db_options(titan_options_);
    TitanCFOptions cf_options(titan_options_);
    BlobFileCache cache(db_options, cf_options, {NewLRUCache(128)});

    {
      std::unique_ptr<WritableFile> f;
      ASSERT_OK(env_->NewWritableFile(file_name_, &f, env_options_));
      writable_file_.reset(new WritableFileWriter(std::move(f), env_options_));
    }
    builder_.reset(new BlobFileBuilder(cf_options, writable_file_.get()));
  }

  void AddKeyValue(const std::string& key, const std::string& value,
                   BlobHandle* blob_handle) {
    BlobRecord record;
    record.key = key;
    record.value = value;
    builder_->Add(record, blob_handle);
    ASSERT_OK(builder_->status());
  }

  void FinishBuiler() {
    ASSERT_OK(builder_->Finish());
    ASSERT_OK(builder_->status());
  }

  void NewBlobFileIterator() {
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file_name_, &file_size));
    NewBlobFileReader(file_number_, 0, titan_options_, env_options_, env_,
                      &readable_file_);
    blob_file_iterator_.reset(new BlobFileIterator{
        std::move(readable_file_), file_number_, file_size, TitanCFOptions()});
  }

  void TestBlobFileIterator() {
    NewBuiler();

    const int n = 1000;
    std::vector<BlobHandle> handles(n);
    for (int i = 0; i < n; i++) {
      auto id = std::to_string(i);
      AddKeyValue(id, id, &handles[i]);
    }

    FinishBuiler();

    NewBlobFileIterator();

    blob_file_iterator_->SeekToFirst();
    for (int i = 0; i < n; blob_file_iterator_->Next(), i++) {
      ASSERT_OK(blob_file_iterator_->status());
      ASSERT_EQ(blob_file_iterator_->Valid(), true);
      auto id = std::to_string(i);
      ASSERT_EQ(id, blob_file_iterator_->key());
      ASSERT_EQ(id, blob_file_iterator_->value());
      BlobIndex blob_index = blob_file_iterator_->GetBlobIndex();
      ASSERT_EQ(handles[i], blob_index.blob_handle);
    }
  }
};

TEST_F(BlobFileIteratorTest, Basic) {
  TitanOptions options;
  TestBlobFileIterator();
}

TEST_F(BlobFileIteratorTest, IterateForPrev) {
  NewBuiler();
  const int n = 1000;
  std::vector<BlobHandle> handles(n);
  for (int i = 0; i < n; i++) {
    auto id = std::to_string(i);
    AddKeyValue(id, id, &handles[i]);
  }

  FinishBuiler();

  NewBlobFileIterator();

  int i = n / 2;
  blob_file_iterator_->IterateForPrev(handles[i].offset);
  ASSERT_OK(blob_file_iterator_->status());
  for (blob_file_iterator_->Next(); i < n; i++, blob_file_iterator_->Next()) {
    ASSERT_OK(blob_file_iterator_->status());
    ASSERT_EQ(blob_file_iterator_->Valid(), true);
    BlobIndex blob_index;
    blob_index = blob_file_iterator_->GetBlobIndex();
    ASSERT_EQ(handles[i], blob_index.blob_handle);
    auto id = std::to_string(i);
    ASSERT_EQ(id, blob_file_iterator_->key());
    ASSERT_EQ(id, blob_file_iterator_->value());
  }

  auto idx = Random::GetTLSInstance()->Uniform(n);
  blob_file_iterator_->IterateForPrev(handles[idx].offset);
  ASSERT_OK(blob_file_iterator_->status());
  blob_file_iterator_->Next();
  ASSERT_OK(blob_file_iterator_->status());
  ASSERT_TRUE(blob_file_iterator_->Valid());
  BlobIndex blob_index;
  blob_index = blob_file_iterator_->GetBlobIndex();
  ASSERT_EQ(handles[idx], blob_index.blob_handle);

  while ((idx = Random::GetTLSInstance()->Uniform(n)) == 0)
    ;
  blob_file_iterator_->IterateForPrev(handles[idx].offset - kBlobHeaderSize -
                                      1);
  ASSERT_OK(blob_file_iterator_->status());
  blob_file_iterator_->Next();
  ASSERT_OK(blob_file_iterator_->status());
  ASSERT_TRUE(blob_file_iterator_->Valid());
  blob_index = blob_file_iterator_->GetBlobIndex();
  ASSERT_EQ(handles[idx - 1], blob_index.blob_handle);

  idx = Random::GetTLSInstance()->Uniform(n);
  blob_file_iterator_->IterateForPrev(handles[idx].offset + 1);
  ASSERT_OK(blob_file_iterator_->status());
  blob_file_iterator_->Next();
  ASSERT_OK(blob_file_iterator_->status());
  ASSERT_TRUE(blob_file_iterator_->Valid());
  blob_index = blob_file_iterator_->GetBlobIndex();
  ASSERT_EQ(handles[idx], blob_index.blob_handle);
}

TEST_F(BlobFileIteratorTest, MergeIterator) {
  const int kMaxKeyNum = 1000;
  std::vector<BlobHandle> handles(kMaxKeyNum);
  std::vector<std::unique_ptr<BlobFileIterator>> iters;
  NewBuiler();
  for (int i = 1; i < kMaxKeyNum; i++) {
    AddKeyValue(GenKey(i), GenValue(i), &handles[i]);
    if (i % 100 == 0) {
      FinishBuiler();
      uint64_t file_size = 0;
      ASSERT_OK(env_->GetFileSize(file_name_, &file_size));
      NewBlobFileReader(file_number_, 0, titan_options_, env_options_, env_,
                        &readable_file_);
      iters.emplace_back(std::unique_ptr<BlobFileIterator>(
          new BlobFileIterator{std::move(readable_file_), file_number_,
                               file_size, TitanCFOptions()}));
      file_number_ = Random::GetTLSInstance()->Next();
      file_name_ = BlobFileName(dirname_, file_number_);
      NewBuiler();
    }
  }

  FinishBuiler();
  uint64_t file_size = 0;
  ASSERT_OK(env_->GetFileSize(file_name_, &file_size));
  NewBlobFileReader(file_number_, 0, titan_options_, env_options_, env_,
                    &readable_file_);
  iters.emplace_back(std::unique_ptr<BlobFileIterator>(new BlobFileIterator{
      std::move(readable_file_), file_number_, file_size, TitanCFOptions()}));
  BlobFileMergeIterator iter(std::move(iters));

  iter.SeekToFirst();
  for (int i = 1; i < kMaxKeyNum; i++, iter.Next()) {
    ASSERT_OK(iter.status());
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.key(), GenKey(i));
    ASSERT_EQ(iter.value(), GenValue(i));
    ASSERT_EQ(iter.GetBlobIndex().blob_handle, handles[i]);
  }
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
