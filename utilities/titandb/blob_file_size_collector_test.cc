#include "utilities/titandb/blob_file_size_collector.h"
#include "util/testharness.h"

namespace rocksdb {
namespace titandb {

class BlobFileSizeCollectorTest : public testing::Test {
 public:
  Env* env_{Env::Default()};
  EnvOptions env_options_;
  Options options_;
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  MutableCFOptions cf_moptions_;
  ImmutableCFOptions cf_ioptions_;
  std::unique_ptr<TableFactory> table_factory_;
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>> collectors_;

  std::string tmpdir_;
  std::string file_name_;

  BlobFileSizeCollectorTest()
      : cf_moptions_(cf_options_),
        cf_ioptions_(options_),
        table_factory_(NewBlockBasedTableFactory()),
        tmpdir_(test::TmpDir(env_)),
        file_name_(tmpdir_ + "/TEST") {
    db_options_.dirname = tmpdir_;
    auto blob_file_size_collector_factory =
        std::make_shared<BlobFileSizeCollectorFactory>();
    collectors_.emplace_back(new UserKeyTablePropertiesCollectorFactory(
        blob_file_size_collector_factory));
  }

  ~BlobFileSizeCollectorTest() {
    env_->DeleteFile(file_name_);
    env_->DeleteDir(tmpdir_);
  }

  void NewFileWriter(std::unique_ptr<WritableFileWriter>* result) {
    std::unique_ptr<WritableFile> writable_file;
    ASSERT_OK(env_->NewWritableFile(file_name_, &writable_file, env_options_));
    result->reset(
        new WritableFileWriter(std::move(writable_file), env_options_));
    ASSERT_TRUE(*result);
  }

  void NewTableBuilder(WritableFileWriter* file,
                       std::unique_ptr<TableBuilder>* result) {
    TableBuilderOptions options(cf_ioptions_, cf_moptions_,
                                cf_ioptions_.internal_comparator, &collectors_,
                                kNoCompression, CompressionOptions(), nullptr,
                                false, kDefaultColumnFamilyName, 0);
    result->reset(table_factory_->NewTableBuilder(options, 0, file));
    ASSERT_TRUE(*result);
  }

  void NewFileReader(std::unique_ptr<RandomAccessFileReader>* result) {
    std::unique_ptr<RandomAccessFile> file;
    ASSERT_OK(env_->NewRandomAccessFile(file_name_, &file, env_options_));
    result->reset(
        new RandomAccessFileReader(std::move(file), file_name_, env_));
    ASSERT_TRUE(*result);
  }

  void NewTableReader(std::unique_ptr<RandomAccessFileReader>&& file,
                      std::unique_ptr<TableReader>* result) {
    TableReaderOptions options(cf_ioptions_, nullptr, env_options_,
                               cf_ioptions_.internal_comparator);
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file->file_name(), &file_size));
    ASSERT_TRUE(file_size > 0);
    ASSERT_OK(table_factory_->NewTableReader(options, std::move(file),
                                             file_size, result));
    ASSERT_TRUE(*result);
  }
};

TEST_F(BlobFileSizeCollectorTest, Basic) {
  std::unique_ptr<WritableFileWriter> wfile;
  NewFileWriter(&wfile);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(wfile.get(), &table_builder);

  const int kNumEntries = 100;
  char buf[16];
  for (int i = 0; i < kNumEntries; i++) {
    ParsedInternalKey ikey;
    snprintf(buf, sizeof(buf), "%15d", i);
    ikey.user_key = buf;
    ikey.type = kTypeBlobIndex;
    std::string key;
    AppendInternalKey(&key, ikey);

    BlobIndex index;
    if (i % 2 == 0) {
      index.file_number = 0ULL;
    } else {
      index.file_number = 1ULL;
    }
    index.blob_handle.size = 10;
    std::string value;
    index.EncodeTo(&value);

    table_builder->Add(key, value);
  }
  ASSERT_OK(table_builder->status());
  ASSERT_EQ(kNumEntries, table_builder->NumEntries());
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(wfile->Flush());
  ASSERT_OK(wfile->Sync(true));

  std::unique_ptr<RandomAccessFileReader> rfile;
  NewFileReader(&rfile);
  std::unique_ptr<TableReader> table_reader;
  NewTableReader(std::move(rfile), &table_reader);

  auto table_properties = table_reader->GetTableProperties();
  ASSERT_TRUE(table_properties);
  auto iter = table_properties->user_collected_properties.find(
      BlobFileSizeCollector::kPropertiesName);
  ASSERT_TRUE(iter != table_properties->user_collected_properties.end());

  Slice raw_blob_file_size_prop(iter->second);
  std::map<uint64_t, uint64_t> result;
  BlobFileSizeCollector::Decode(&raw_blob_file_size_prop, &result);

  ASSERT_EQ(2, result.size());

  ASSERT_EQ(kNumEntries / 2 * 10, result[0]);
  ASSERT_EQ(kNumEntries / 2 * 10, result[1]);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
