//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <charconv>
#include <memory>
#include <string>
#include <vector>

#include "faiss/IndexFlat.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/utils/random.h"
#include "rocksdb/utilities/secondary_index_faiss.h"
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

  const std::string primary_column_name = "embedding";
  auto faiss_ivf_index =
      std::make_shared<FaissIVFIndex>(std::move(index), primary_column_name);

  const std::string db_name = test::PerThreadDBPath("faiss_ivf_index_test");
  EXPECT_OK(DestroyDB(db_name, Options()));

  Options options;
  options.create_if_missing = true;

  TransactionDBOptions txn_db_options;
  txn_db_options.secondary_indices.emplace_back(faiss_ivf_index);

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

  // Write the embeddings to the primary column family, indexing them in the
  // process
  {
    std::unique_ptr<Transaction> txn(db->BeginTransaction(WriteOptions()));

    for (faiss::idx_t i = 0; i < num_vectors; ++i) {
      const std::string primary_key = std::to_string(i);

      ASSERT_OK(txn->PutEntity(
          cfh1, primary_key,
          WideColumns{
              {primary_column_name,
               ConvertFloatsToSlice(embeddings.data() + i * dim, dim)}}));
    }

    ASSERT_OK(txn->Commit());
  }

  // Verify the raw index data in the secondary column family
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
      ASSERT_EQ(it->value(),
                ConvertFloatsToSlice(embeddings.data() + id * dim, dim));

      ++num_found;
    }

    ASSERT_OK(it->status());
    ASSERT_EQ(num_found, num_vectors);
  }

  // Query the index with some of the original embeddings
  std::unique_ptr<Iterator> underlying_it(db->NewIterator(ReadOptions(), cfh2));
  auto secondary_it = std::make_unique<SecondaryIndexIterator>(
      faiss_ivf_index.get(), std::move(underlying_it));

  auto get_id = [](const Slice& key) -> faiss::idx_t {
    faiss::idx_t id = -1;

    if (std::from_chars(key.data(), key.data() + key.size(), id).ec !=
        std::errc()) {
      return -1;
    }

    return id;
  };

  constexpr size_t neighbors = 8;

  auto verify = [&](faiss::idx_t id) {
    // Search for a vector from the original set; we expect to find the vector
    // itself as the closest match, since we're performing an exhaustive search
    std::vector<std::pair<std::string, float>> result;
    ASSERT_OK(faiss_ivf_index->FindKNearestNeighbors(
        secondary_it.get(),
        ConvertFloatsToSlice(embeddings.data() + id * dim, dim), neighbors,
        num_lists, &result));

    ASSERT_EQ(result.size(), neighbors);

    const faiss::idx_t first_id = get_id(result[0].first);

    ASSERT_GE(first_id, 0);
    ASSERT_LT(first_id, num_vectors);
    ASSERT_EQ(first_id, id);

    ASSERT_EQ(result[0].second, 0.0f);

    // Iterate over the rest of the results
    for (size_t i = 1; i < neighbors; ++i) {
      const faiss::idx_t other_id = get_id(result[i].first);
      ASSERT_GE(other_id, 0);
      ASSERT_LT(other_id, num_vectors);
      ASSERT_NE(other_id, id);

      ASSERT_GE(result[i].second, result[i - 1].second);
    }
  };

  verify(0);
  verify(16);
  verify(32);
  verify(64);

  // Sanity checks
  {
    // No secondary index iterator
    constexpr SecondaryIndexIterator* bad_secondary_it = nullptr;
    std::vector<std::pair<std::string, float>> result;
    ASSERT_TRUE(faiss_ivf_index
                    ->FindKNearestNeighbors(
                        bad_secondary_it,
                        ConvertFloatsToSlice(embeddings.data(), dim), neighbors,
                        num_lists, &result)
                    .IsInvalidArgument());
  }

  {
    // Invalid target
    std::vector<std::pair<std::string, float>> result;
    ASSERT_TRUE(faiss_ivf_index
                    ->FindKNearestNeighbors(secondary_it.get(), "foo",
                                            neighbors, num_lists, &result)
                    .IsInvalidArgument());
  }

  {
    // Invalid value for neighbors
    constexpr size_t bad_neighbors = 0;
    std::vector<std::pair<std::string, float>> result;
    ASSERT_TRUE(faiss_ivf_index
                    ->FindKNearestNeighbors(
                        secondary_it.get(),
                        ConvertFloatsToSlice(embeddings.data(), dim),
                        bad_neighbors, num_lists, &result)
                    .IsInvalidArgument());
  }

  {
    // Invalid value for neighbors
    constexpr size_t bad_probes = 0;
    std::vector<std::pair<std::string, float>> result;
    ASSERT_TRUE(faiss_ivf_index
                    ->FindKNearestNeighbors(
                        secondary_it.get(),
                        ConvertFloatsToSlice(embeddings.data(), dim), neighbors,
                        bad_probes, &result)
                    .IsInvalidArgument());
  }

  {
    // No result parameter
    constexpr std::vector<std::pair<std::string, float>>* bad_result = nullptr;
    ASSERT_TRUE(faiss_ivf_index
                    ->FindKNearestNeighbors(
                        secondary_it.get(),
                        ConvertFloatsToSlice(embeddings.data(), dim), neighbors,
                        num_lists, bad_result)
                    .IsInvalidArgument());
  }
}

TEST(FaissIVFIndexTest, Compare) {
  // Train two copies of the same index; hand over one to FaissIVFIndex and use
  // the other one as a baseline for comparison
  constexpr size_t dim = 128;
  auto quantizer_cmp = std::make_unique<faiss::IndexFlatL2>(dim);
  auto quantizer = std::make_unique<faiss::IndexFlatL2>(dim);

  constexpr size_t num_lists = 16;
  auto index_cmp = std::make_unique<faiss::IndexIVFFlat>(quantizer_cmp.get(),
                                                         dim, num_lists);
  auto index =
      std::make_unique<faiss::IndexIVFFlat>(quantizer.get(), dim, num_lists);

  {
    constexpr faiss::idx_t num_train = 1024;
    std::vector<float> embeddings_train(dim * num_train);
    faiss::float_rand(embeddings_train.data(), dim * num_train, 42);

    index_cmp->train(num_train, embeddings_train.data());
    index->train(num_train, embeddings_train.data());
  }

  auto faiss_ivf_index = std::make_shared<FaissIVFIndex>(
      std::move(index), kDefaultWideColumnName.ToString());

  const std::string db_name = test::PerThreadDBPath("faiss_ivf_index_test");
  EXPECT_OK(DestroyDB(db_name, Options()));

  Options options;
  options.create_if_missing = true;

  TransactionDBOptions txn_db_options;
  txn_db_options.secondary_indices.emplace_back(faiss_ivf_index);

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

  // Add the same set of database vectors to both indices
  constexpr faiss::idx_t num_db = 4096;

  {
    std::vector<float> embeddings_db(dim * num_db);
    faiss::float_rand(embeddings_db.data(), dim * num_db, 123);

    for (faiss::idx_t i = 0; i < num_db; ++i) {
      const float* const embedding = embeddings_db.data() + i * dim;

      index_cmp->add(1, embedding);

      const std::string primary_key = std::to_string(i);
      ASSERT_OK(db->Put(WriteOptions(), cfh1, primary_key,
                        ConvertFloatsToSlice(embedding, dim)));
    }
  }

  // Search both indices with the same set of query vectors and make sure the
  // results match
  {
    constexpr faiss::idx_t num_query = 32;
    std::vector<float> embeddings_query(dim * num_query);
    faiss::float_rand(embeddings_query.data(), dim * num_query, 456);

    std::unique_ptr<Iterator> underlying_it(
        db->NewIterator(ReadOptions(), cfh2));
    auto secondary_it = std::make_unique<SecondaryIndexIterator>(
        faiss_ivf_index.get(), std::move(underlying_it));

    auto get_id = [](const Slice& key) -> faiss::idx_t {
      faiss::idx_t id = -1;

      if (std::from_chars(key.data(), key.data() + key.size(), id).ec !=
          std::errc()) {
        return -1;
      }

      return id;
    };

    for (size_t neighbors : {1, 2, 4}) {
      for (size_t probes : {1, 2, 4}) {
        for (faiss::idx_t i = 0; i < num_query; ++i) {
          const float* const embedding = embeddings_query.data() + i * dim;

          std::vector<float> distances(neighbors, 0.0f);
          std::vector<faiss::idx_t> ids(neighbors, -1);

          faiss::SearchParametersIVF params;
          params.nprobe = probes;

          index_cmp->search(1, embedding, neighbors, distances.data(),
                            ids.data(), &params);

          size_t result_size_cmp = 0;
          for (faiss::idx_t id_cmp : ids) {
            if (id_cmp < 0) {
              break;
            }

            ++result_size_cmp;
          }

          std::vector<std::pair<std::string, float>> result;
          ASSERT_OK(faiss_ivf_index->FindKNearestNeighbors(
              secondary_it.get(), ConvertFloatsToSlice(embedding, dim),
              neighbors, probes, &result));

          ASSERT_EQ(result.size(), result_size_cmp);

          for (size_t j = 0; j < result.size(); ++j) {
            const faiss::idx_t id = get_id(result[j].first);
            ASSERT_GE(id, 0);
            ASSERT_LT(id, num_db);
            ASSERT_EQ(id, ids[j]);
            ASSERT_EQ(result[j].second, distances[j]);
          }
        }
      }
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
