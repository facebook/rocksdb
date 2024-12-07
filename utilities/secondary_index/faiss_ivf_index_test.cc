//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/secondary_index/faiss_ivf_index.h"

#include <charconv>
#include <memory>
#include <string>
#include <vector>

#include "faiss/IndexFlat.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/utils/random.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/testharness.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

TEST(FaissIVFIndexTest, Basic) {
  constexpr size_t dim = 128;
  auto quantizer = std::make_unique<faiss::IndexFlatL2>(dim);

  constexpr size_t num_lists = 16;
  auto index =
      std::make_unique<faiss::IndexIVFFlat>(quantizer.get(), dim, num_lists);

  constexpr faiss::idx_t num_vectors = 1024;
  std::vector<float> embeddings(dim * num_vectors);
  faiss::float_rand(embeddings.data(), dim * num_vectors, 42);

  index->train(num_vectors, embeddings.data());

  index->nprobe = 2;

  const std::string db_name = test::PerThreadDBPath("faiss_ivf_index_test");
  EXPECT_OK(DestroyDB(db_name, Options()));

  Options options;
  options.create_if_missing = true;

  TransactionDBOptions txn_db_options;
  const std::string primary_column_name = "embedding";
  txn_db_options.secondary_indices.emplace_back(
      std::make_shared<FaissIVFIndex>(std::move(index), primary_column_name));

  TransactionDB* db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_db_options, db_name, &db));

  std::unique_ptr<TransactionDB> db_guard(db);

  ColumnFamilyOptions cf1_opts;
  ColumnFamilyHandle* cfh1 = nullptr;
  ASSERT_OK(db->CreateColumnFamily(cf1_opts, "cf1", &cfh1));
  std::unique_ptr<ColumnFamilyHandle> cfh1_guard(cfh1);

  ColumnFamilyOptions cf2_opts;
  ColumnFamilyHandle* cfh2 = nullptr;
  ASSERT_OK(db->CreateColumnFamily(cf2_opts, "cf2", &cfh2));
  std::unique_ptr<ColumnFamilyHandle> cfh2_guard(cfh2);

  const auto& secondary_index = txn_db_options.secondary_indices.back();
  secondary_index->SetPrimaryColumnFamily(cfh1);
  secondary_index->SetSecondaryColumnFamily(cfh2);

  {
    std::unique_ptr<Transaction> txn(db->BeginTransaction(WriteOptions()));

    for (faiss::idx_t i = 0; i < num_vectors; ++i) {
      const std::string primary_key = std::to_string(i);

      ASSERT_OK(txn->PutEntity(
          cfh1, primary_key,
          WideColumns{
              {primary_column_name,
               Slice(reinterpret_cast<const char*>(embeddings.data() + i * dim),
                     dim * sizeof(float))}}));
    }

    ASSERT_OK(txn->Commit());
  }

  {
    size_t num_found = 0;

    std::unique_ptr<Iterator> it(db->NewIterator(ReadOptions(), cfh2));

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      Slice key = it->key();
      faiss::idx_t label = -1;
      ASSERT_TRUE(GetVarsignedint64(&key, &label));
      ASSERT_GE(label, 0);
      ASSERT_LT(label, num_lists);

      faiss::idx_t id = -1;
      ASSERT_EQ(std::from_chars(key.data(), key.data() + key.size(), id).ec,
                std::errc());
      ASSERT_GE(id, 0);
      ASSERT_LT(id, num_vectors);

      // Since we use IndexIVFFlat, there is no fine quantization, so the code
      // is actually just the original embedding
      ASSERT_EQ(
          it->value(),
          Slice(reinterpret_cast<const char*>(embeddings.data() + id * dim),
                dim * sizeof(float)));

      ++num_found;
    }

    ASSERT_OK(it->status());
    ASSERT_EQ(num_found, num_vectors);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
