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
               Slice(reinterpret_cast<const char*>(embeddings.data() + i * dim),
                     dim * sizeof(float))}}));
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
      ASSERT_EQ(
          it->value(),
          Slice(reinterpret_cast<const char*>(embeddings.data() + id * dim),
                dim * sizeof(float)));

      ++num_found;
    }

    ASSERT_OK(it->status());
    ASSERT_EQ(num_found, num_vectors);
  }

  // Query the index with some of the original embeddings
  std::unique_ptr<Iterator> underlying_it(db->NewIterator(ReadOptions(), cfh2));

  SecondaryIndexReadOptions read_options;
  read_options.similarity_search_neighbors = 8;
  read_options.similarity_search_probes = num_lists;

  std::unique_ptr<Iterator> it =
      txn_db_options.secondary_indices.back()->NewIterator(
          read_options, std::move(underlying_it));

  auto get_id = [&]() -> faiss::idx_t {
    Slice key = it->key();
    faiss::idx_t id = -1;

    if (std::from_chars(key.data(), key.data() + key.size(), id).ec !=
        std::errc()) {
      return -1;
    }

    return id;
  };

  auto get_distance = [&]() -> float {
    std::string distance_str;
    float distance = 0.0f;

    if (!it->GetProperty("rocksdb.faiss.ivf.index.distance", &distance_str)
             .ok()) {
      return -1.0f;
    }

    if (std::from_chars(distance_str.data(),
                        distance_str.data() + distance_str.size(), distance)
            .ec != std::errc()) {
      return -1.0f;
    }

    return distance;
  };

  auto verify = [&](faiss::idx_t id) {
    // Search for a vector from the original set; we expect to find the vector
    // itself as the closest match, since we're performing an exhaustive search
    {
      it->Seek(
          Slice(reinterpret_cast<const char*>(embeddings.data() + id * dim),
                dim * sizeof(float)));
      ASSERT_TRUE(it->Valid());
      ASSERT_OK(it->status());
      ASSERT_EQ(get_id(), id);
      ASSERT_TRUE(it->value().empty());
      ASSERT_TRUE(it->columns().empty());
      ASSERT_EQ(get_distance(), 0.0f);
    }

    // Take a step forward then a step back to get back to our original position
    {
      it->Next();
      ASSERT_TRUE(it->Valid());
      ASSERT_OK(it->status());

      it->Prev();
      ASSERT_TRUE(it->Valid());
      ASSERT_OK(it->status());
      ASSERT_EQ(get_id(), id);
      ASSERT_TRUE(it->value().empty());
      ASSERT_TRUE(it->columns().empty());
      ASSERT_EQ(get_distance(), 0.0f);
    }

    // Iterate over the rest of the results
    float prev_distance = 0.0f;
    size_t num_found = 1;

    for (it->Next(); it->Valid(); it->Next()) {
      ASSERT_OK(it->status());

      const faiss::idx_t other_id = get_id();
      ASSERT_GE(other_id, 0);
      ASSERT_LT(other_id, num_vectors);
      ASSERT_NE(other_id, id);

      ASSERT_TRUE(it->value().empty());
      ASSERT_TRUE(it->columns().empty());

      const float distance = get_distance();
      ASSERT_GE(distance, prev_distance);

      prev_distance = distance;
      ++num_found;
    }

    ASSERT_OK(it->status());
    ASSERT_EQ(num_found, *read_options.similarity_search_neighbors);
  };

  verify(0);
  verify(16);
  verify(32);
  verify(64);

  // Sanity check unsupported APIs
  it->SeekToFirst();
  ASSERT_FALSE(it->Valid());
  ASSERT_TRUE(it->status().IsNotSupported());

  it->SeekToLast();
  ASSERT_FALSE(it->Valid());
  ASSERT_TRUE(it->status().IsNotSupported());

  it->SeekForPrev(Slice(reinterpret_cast<const char*>(embeddings.data()),
                        dim * sizeof(float)));
  ASSERT_FALSE(it->Valid());
  ASSERT_TRUE(it->status().IsNotSupported());

  it->Seek("foo");  // incorrect size
  ASSERT_FALSE(it->Valid());
  ASSERT_TRUE(it->status().IsInvalidArgument());

  {
    SecondaryIndexReadOptions bad_options;
    bad_options.similarity_search_probes = 1;

    // similarity_search_neighbors not set
    {
      std::unique_ptr<Iterator> bad_under_it(
          db->NewIterator(ReadOptions(), cfh2));
      std::unique_ptr<Iterator> bad_it =
          txn_db_options.secondary_indices.back()->NewIterator(
              bad_options, std::move(bad_under_it));
      ASSERT_TRUE(bad_it->status().IsInvalidArgument());
    }

    // similarity_search_neighbors set to zero
    bad_options.similarity_search_neighbors = 0;

    {
      std::unique_ptr<Iterator> bad_under_it(
          db->NewIterator(ReadOptions(), cfh2));
      std::unique_ptr<Iterator> bad_it =
          txn_db_options.secondary_indices.back()->NewIterator(
              bad_options, std::move(bad_under_it));
      ASSERT_TRUE(bad_it->status().IsInvalidArgument());
    }
  }

  {
    SecondaryIndexReadOptions bad_options;
    bad_options.similarity_search_neighbors = 1;

    // similarity_search_probes not set
    {
      std::unique_ptr<Iterator> bad_under_it(
          db->NewIterator(ReadOptions(), cfh2));
      std::unique_ptr<Iterator> bad_it =
          txn_db_options.secondary_indices.back()->NewIterator(
              bad_options, std::move(bad_under_it));
      ASSERT_TRUE(bad_it->status().IsInvalidArgument());
    }

    // similarity_search_probes set to zero
    bad_options.similarity_search_probes = 0;

    {
      std::unique_ptr<Iterator> bad_under_it(
          db->NewIterator(ReadOptions(), cfh2));
      std::unique_ptr<Iterator> bad_it =
          txn_db_options.secondary_indices.back()->NewIterator(
              bad_options, std::move(bad_under_it));
      ASSERT_TRUE(bad_it->status().IsInvalidArgument());
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
