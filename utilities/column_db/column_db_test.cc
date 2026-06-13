//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/column_db.h"

#include <memory>
#include <string>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "file/file_util.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_writer.h"

namespace ROCKSDB_NAMESPACE {

class ColumnDBTest : public DBTestBase {
 protected:
  ColumnDBTest() : DBTestBase("column_db_test", /* env_do_fsync */ false) {}

  void WriteEntityReferencingBlob(const Options& options,
                                  const std::string& entity_key,
                                  const std::string& sst_file_name) {
    constexpr char blob_key[] = "blob-source";
    constexpr char blob_value[] = "0123456789abcdef";
    ASSERT_OK(Put(blob_key, blob_value));
    ASSERT_OK(Flush());

    DBImpl* const db_impl = dynamic_cast<DBImpl*>(db_.get());
    ASSERT_NE(db_impl, nullptr);

    PinnableSlice blob_index_value;
    bool is_blob_index = false;
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = db_->DefaultColumnFamily();
    get_impl_options.value = &blob_index_value;
    get_impl_options.is_blob_index = &is_blob_index;
    ASSERT_OK(db_impl->GetImpl(ReadOptions(), blob_key, get_impl_options));
    ASSERT_TRUE(is_blob_index);

    BlobIndex blob_index;
    Slice blob_index_slice(blob_index_value.data(), blob_index_value.size());
    ASSERT_OK(blob_index.DecodeFrom(blob_index_slice));
    ASSERT_FALSE(blob_index.IsInlined());
    ASSERT_FALSE(blob_index.HasTTL());

    const std::string file_path = dbname_ + "/" + sst_file_name;
    {
      SstFileWriter writer(EnvOptions(), options);
      ASSERT_OK(writer.Open(file_path));

      WideColumns entity{{"schema", "schema-v1"}, {"blob", ""}};
      Slice blob_index_slice_for_sst(blob_index_value.data(),
                                     blob_index_value.size());
      std::vector<SstFileWriterBlobColumn> blob_columns{
          {1, blob_index_slice_for_sst}};
      ASSERT_OK(
          writer.PutEntityWithBlobIndexes(entity_key, entity, blob_columns));
      ASSERT_OK(writer.Finish());
    }

    IngestExternalFileOptions ingest_options;
    ASSERT_OK(db_->IngestExternalFile({file_path}, ingest_options));
  }
};

TEST_F(ColumnDBTest, ProjectedGetReadsBlobRanges) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  Reopen(options);

  ASSERT_NO_FATAL_FAILURE(
      WriteEntityReferencingBlob(options, "entity", "column_db.sst"));

  ColumnDBOptions column_options;
  column_options.schema_column_name = "schema";
  column_options.blob_column_name = "blob";
  column_options.translate =
      [](const Slice& schema, const std::vector<Slice>& columns,
         std::vector<ColumnDBBlobRange>* ranges) -> Status {
    if (schema != Slice("schema-v1")) {
      return Status::Corruption("unexpected schema");
    }
    ranges->clear();
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i] == Slice("f1")) {
        ranges->push_back({i, 2, 3});
      } else if (columns[i] == Slice("f2")) {
        ranges->push_back({i, 5, 4});
      } else {
        return Status::InvalidArgument("unknown projected column");
      }
    }
    return Status::OK();
  };

  ColumnDB* column_db = nullptr;
  ASSERT_OK(ColumnDB::Open(column_options, std::move(db_), &column_db));
  db_.reset(column_db);

  ReadOptions read_options;
  read_options.verify_checksums = false;

  std::vector<PinnableSlice> values;
  ASSERT_OK(column_db->Get(read_options, "entity",
                           std::vector<Slice>{Slice("f1"), Slice("f2")},
                           &values));

  ASSERT_EQ(values.size(), 2);
  ASSERT_EQ(values[0], Slice("234"));
  ASSERT_EQ(values[1], Slice("5678"));
}

TEST_F(ColumnDBTest, PartialReadsBypassWholeBlobCache) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  LRUCacheOptions cache_options;
  cache_options.capacity = 2 << 20;
  cache_options.num_shard_bits = 2;
  cache_options.metadata_charge_policy = kDontChargeCacheMetadata;
  options.blob_cache = NewLRUCache(cache_options);

  Reopen(options);

  ASSERT_NO_FATAL_FAILURE(
      WriteEntityReferencingBlob(options, "entity", "column_db_cache.sst"));

  ColumnDBOptions column_options;
  column_options.schema_column_name = "schema";
  column_options.blob_column_name = "blob";
  column_options.translate =
      [](const Slice& schema, const std::vector<Slice>& columns,
         std::vector<ColumnDBBlobRange>* ranges) -> Status {
    if (schema != Slice("schema-v1")) {
      return Status::Corruption("unexpected schema");
    }
    ranges->clear();
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i] == Slice("short")) {
        ranges->push_back({i, 0, 3});
      } else if (columns[i] == Slice("long")) {
        ranges->push_back({i, 0, 8});
      } else {
        return Status::InvalidArgument("unknown projected column");
      }
    }
    return Status::OK();
  };

  ColumnDB* column_db = nullptr;
  ASSERT_OK(ColumnDB::Open(column_options, std::move(db_), &column_db));
  db_.reset(column_db);

  ReadOptions read_options;
  read_options.fill_cache = true;
  read_options.verify_checksums = false;

  std::vector<PinnableSlice> values;
  ASSERT_OK(column_db->Get(read_options, "entity",
                           std::vector<Slice>{Slice("short")}, &values));
  ASSERT_EQ(values.size(), 1);
  ASSERT_EQ(values[0], Slice("012"));

  values.clear();
  ASSERT_OK(column_db->Get(read_options, "entity",
                           std::vector<Slice>{Slice("long")}, &values));
  ASSERT_EQ(values.size(), 1);
  ASSERT_EQ(values[0], Slice("01234567"));
}

TEST_F(ColumnDBTest, ProjectedGetReadsBlobGeneratedBySstWriter) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  Reopen(options);

  const std::string external_dir = dbname_ + "/external_column_db";
  ASSERT_OK(DestroyDir(env_, external_dir));
  ASSERT_OK(env_->CreateDir(external_dir));

  const std::string file_path = external_dir + "/column_db_generated.sst";
  std::vector<ExternalBlobFileInfo> blob_file_infos;
  {
    SstFileWriter writer(EnvOptions(), options, db_->DefaultColumnFamily());
    SstFileWriterBlobOptions blob_options;
    blob_options.blob_file_dir = external_dir;
    ASSERT_OK(writer.OpenWithBlobFiles(file_path, blob_options));

    WideColumns entity{{"schema", "schema-v1"}, {"blob", ""}};
    std::vector<SstFileWriterBlobValue> blob_columns{
        {1, Slice("0123456789abcdef")}};
    ASSERT_OK(writer.PutEntityWithBlobValues("entity", entity, blob_columns));

    ExternalSstFileInfo sst_file_info;
    ASSERT_OK(writer.Finish(&sst_file_info, &blob_file_infos));
  }
  ASSERT_EQ(blob_file_infos.size(), 1);

  IngestExternalFileArg arg;
  arg.column_family = db_->DefaultColumnFamily();
  arg.external_files = {file_path};
  arg.external_blob_files = blob_file_infos;
  ASSERT_OK(db_->IngestExternalFiles({arg}));

  Reopen(options);

  ColumnDBOptions column_options;
  column_options.schema_column_name = "schema";
  column_options.blob_column_name = "blob";
  column_options.translate =
      [](const Slice& schema, const std::vector<Slice>& columns,
         std::vector<ColumnDBBlobRange>* ranges) -> Status {
    if (schema != Slice("schema-v1")) {
      return Status::Corruption("unexpected schema");
    }
    ranges->clear();
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i] == Slice("f1")) {
        ranges->push_back({i, 1, 4});
      } else if (columns[i] == Slice("f2")) {
        ranges->push_back({i, 8, 3});
      } else {
        return Status::InvalidArgument("unknown projected column");
      }
    }
    return Status::OK();
  };

  ColumnDB* column_db = nullptr;
  ASSERT_OK(ColumnDB::Open(column_options, std::move(db_), &column_db));
  db_.reset(column_db);

  ReadOptions read_options;
  read_options.verify_checksums = false;

  std::vector<PinnableSlice> values;
  ASSERT_OK(column_db->Get(read_options, "entity",
                           std::vector<Slice>{Slice("f1"), Slice("f2")},
                           &values));

  ASSERT_EQ(values.size(), 2);
  ASSERT_EQ(values[0], Slice("1234"));
  ASSERT_EQ(values[1], Slice("89a"));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
