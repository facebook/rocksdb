#include "table/table_builder.h"
#include "table/table_reader.h"
#include "util/filename.h"
#include "util/testharness.h"
#include "utilities/titandb/blob_file_manager.h"
#include "utilities/titandb/blob_file_reader.h"
#include "utilities/titandb/table_factory.h"

namespace rocksdb {
namespace titandb {

const uint64_t kMinBlobSize = 128;
const uint64_t kTestFileNumber = 123;

class FileManager : public BlobFileManager {
 public:
  FileManager(const TitanDBOptions& db_options) : db_options_(db_options) {}

  Status NewFile(std::unique_ptr<BlobFileHandle>* handle) override {
    auto number = kTestFileNumber;
    auto name = BlobFileName(db_options_.dirname, number);
    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<WritableFile> f;
      Status s = env_->NewWritableFile(name, &f, env_options_);
      if (!s.ok()) return s;
      file.reset(new WritableFileWriter(std::move(f), env_options_));
    }
    handle->reset(new FileHandle(number, name, std::move(file)));
    return Status::OK();
  }

  Status FinishFile(uint32_t /*cf_id*/, std::shared_ptr<BlobFileMeta> /*file*/,
                    std::unique_ptr<BlobFileHandle>&& handle) override {
    Status s = handle->GetFile()->Sync(true);
    if (s.ok()) {
      s = handle->GetFile()->Close();
    }
    return s;
  }

  Status DeleteFile(std::unique_ptr<BlobFileHandle>&& handle) override {
    return env_->DeleteFile(handle->GetName());
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
    friend class FileManager;

    uint64_t number_;
    std::string name_;
    std::unique_ptr<WritableFileWriter> file_;
  };

  Env* env_{Env::Default()};
  EnvOptions env_options_;
  TitanDBOptions db_options_;
};

class TableBuilderTest : public testing::Test {
 public:
  TableBuilderTest()
      : cf_moptions_(cf_options_),
        cf_ioptions_(options_),
        tmpdir_(test::TmpDir(env_)),
        base_name_(tmpdir_ + "/base"),
        blob_name_(BlobFileName(tmpdir_, kTestFileNumber)) {
    db_options_.dirname = tmpdir_;
    cf_options_.min_blob_size = kMinBlobSize;
    blob_manager_.reset(new FileManager(db_options_));
    table_factory_.reset(new TitanTableFactory(db_options_, cf_options_, blob_manager_));
  }

  ~TableBuilderTest() {
    env_->DeleteFile(base_name_);
    env_->DeleteFile(blob_name_);
    env_->DeleteDir(tmpdir_);
  }

  void BlobFileExists(bool exists) {
    Status s = env_->FileExists(blob_name_);
    if (exists) {
      ASSERT_TRUE(s.ok());
    } else {
      ASSERT_TRUE(s.IsNotFound());
    }
  }

  void NewFileWriter(const std::string& fname,
                     std::unique_ptr<WritableFileWriter>* result) {
    std::unique_ptr<WritableFile> file;
    ASSERT_OK(env_->NewWritableFile(fname, &file, env_options_));
    result->reset(new WritableFileWriter(std::move(file), env_options_));
  }

  void NewFileReader(const std::string& fname,
                     std::unique_ptr<RandomAccessFileReader>* result) {
    std::unique_ptr<RandomAccessFile> file;
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file, env_options_));
    result->reset(new RandomAccessFileReader(std::move(file), fname, env_));
  }

  void NewBaseFileWriter(std::unique_ptr<WritableFileWriter>* result) {
    NewFileWriter(base_name_, result);
  }

  void NewBaseFileReader(std::unique_ptr<RandomAccessFileReader>* result) {
    NewFileReader(base_name_, result);
  }

  void NewBlobFileReader(std::unique_ptr<BlobFileReader>* result) {
    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(blob_name_, &file);
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(blob_name_, &file_size));
    ASSERT_OK(
        BlobFileReader::Open(cf_options_, std::move(file), file_size, result));
  }

  void NewTableReader(std::unique_ptr<TableReader>* result) {
    std::unique_ptr<RandomAccessFileReader> file;
    NewBaseFileReader(&file);
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file->file_name(), &file_size));
    TableReaderOptions options(cf_ioptions_, nullptr, env_options_,
                               cf_ioptions_.internal_comparator);
    ASSERT_OK(table_factory_->NewTableReader(options, std::move(file),
                                             file_size, result));
  }

  void NewTableBuilder(WritableFileWriter* file,
                       std::unique_ptr<TableBuilder>* result) {
    TableBuilderOptions options(cf_ioptions_, cf_moptions_,
                                cf_ioptions_.internal_comparator, &collectors_,
                                kNoCompression, CompressionOptions(), nullptr,
                                false, kDefaultColumnFamilyName, 0);
    result->reset(table_factory_->NewTableBuilder(options, 0, file));
  }

  Env* env_{Env::Default()};
  EnvOptions env_options_;
  Options options_;
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  MutableCFOptions cf_moptions_;
  ImmutableCFOptions cf_ioptions_;
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>> collectors_;

  std::string tmpdir_;
  std::string base_name_;
  std::string blob_name_;
  std::unique_ptr<TableFactory> table_factory_;
  std::shared_ptr<BlobFileManager> blob_manager_;
};

TEST_F(TableBuilderTest, Basic) {
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file.get(), &table_builder);

  // Build a base table and a blob file.
  const int n = 100;
  for (char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value;
    if (i % 2 == 0) {
      value = std::string(1, i);
    } else {
      value = std::string(kMinBlobSize, i);
    }
    table_builder->Add(ikey.Encode(), value);
  }
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(base_file->Sync(true));
  ASSERT_OK(base_file->Close());

  std::unique_ptr<TableReader> base_reader;
  NewTableReader(&base_reader);
  std::unique_ptr<BlobFileReader> blob_reader;
  NewBlobFileReader(&blob_reader);

  ReadOptions ro;
  std::unique_ptr<InternalIterator> iter;
  iter.reset(base_reader->NewIterator(ro, nullptr));
  iter->SeekToFirst();
  for (char i = 0; i < n; i++) {
    ASSERT_TRUE(iter->Valid());
    std::string key(1, i);
    ParsedInternalKey ikey;
    ASSERT_TRUE(ParseInternalKey(iter->key(), &ikey));
    ASSERT_EQ(ikey.user_key, key);
    if (i % 2 == 0) {
      ASSERT_EQ(ikey.type, kTypeValue);
      ASSERT_EQ(iter->value(), std::string(1, i));
    } else {
      ASSERT_EQ(ikey.type, kTypeBlobIndex);
      BlobIndex index;
      ASSERT_OK(DecodeInto(iter->value(), &index));
      ASSERT_EQ(index.file_number, kTestFileNumber);
      BlobRecord record;
      PinnableSlice buffer;
      ASSERT_OK(blob_reader->Get(ro, index.blob_handle, &record, &buffer));
      ASSERT_EQ(record.key, key);
      ASSERT_EQ(record.value, std::string(kMinBlobSize, i));
    }
    iter->Next();
  }
}

TEST_F(TableBuilderTest, NoBlob) {
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file.get(), &table_builder);

  const int n = 100;
  for (char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value(1, i);
    table_builder->Add(ikey.Encode(), value);
  }
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(base_file->Sync(true));
  ASSERT_OK(base_file->Close());
  BlobFileExists(false);

  std::unique_ptr<TableReader> base_reader;
  NewTableReader(&base_reader);

  ReadOptions ro;
  std::unique_ptr<InternalIterator> iter;
  iter.reset(base_reader->NewIterator(ro, nullptr));
  iter->SeekToFirst();
  for (char i = 0; i < n; i++) {
    ASSERT_TRUE(iter->Valid());
    std::string key(1, i);
    ParsedInternalKey ikey;
    ASSERT_TRUE(ParseInternalKey(iter->key(), &ikey));
    ASSERT_EQ(ikey.user_key, key);
    ASSERT_EQ(ikey.type, kTypeValue);
    ASSERT_EQ(iter->value(), std::string(1, i));
    iter->Next();
  }
}

TEST_F(TableBuilderTest, Abandon) {
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file.get(), &table_builder);

  const int n = 100;
  for (char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value;
    if (i % 2 == 0) {
      value = std::string(1, i);
    } else {
      value = std::string(kMinBlobSize, i);
    }
    table_builder->Add(ikey.Encode(), value);
  }
  BlobFileExists(true);
  table_builder->Abandon();
  BlobFileExists(false);
}

TEST_F(TableBuilderTest, NumEntries) {
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file.get(), &table_builder);

  // Build a base table and a blob file.
  const int n = 100;
  for (char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value;
    if (i % 2 == 0) {
      value = std::string(1, i);
    } else {
      value = std::string(kMinBlobSize, i);
    }
    table_builder->Add(ikey.Encode(), value);
  }
  ASSERT_EQ(n, table_builder->NumEntries());
  ASSERT_OK(table_builder->Finish());
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
