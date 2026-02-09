//  Copyright (c) Meta Platforms, Inc. and affiliates.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/tool_hooks.h"

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/blob_db/blob_db.h"

namespace ROCKSDB_NAMESPACE {

Status DefaultHooks::Open(const Options& db_options, const std::string& name,
                          DB** dbptr) {
  return DB::Open(db_options, name, dbptr);
};

Status DefaultHooks::Open(
    const DBOptions& db_options, const std::string& name,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr) {
  return DB::Open(db_options, name, column_families, handles, dbptr);
};

Status DefaultHooks::OpenForReadOnly(const Options& options,
                                     const std::string& name, DB** dbptr,
                                     bool error_if_wal_file_exists = false) {
  return DB::OpenForReadOnly(options, name, dbptr, error_if_wal_file_exists);
};

Status DefaultHooks::OpenForReadOnly(
    const Options& options, const std::string& name,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr) {
  return DB::OpenForReadOnly(options, name, column_families, handles, dbptr);
};
Status DefaultHooks::OpenTransactionDB(
    const Options& db_options, const TransactionDBOptions& txn_db_options,
    const std::string& dbname, TransactionDB** dbptr) {
  return TransactionDB::Open(db_options, txn_db_options, dbname, dbptr);
};

Status DefaultHooks::OpenTransactionDB(
    const DBOptions& db_options, const TransactionDBOptions& txn_db_options,
    const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, TransactionDB** dbptr) {
  return TransactionDB::Open(db_options, txn_db_options, dbname,
                             column_families, handles, dbptr);
};

Status DefaultHooks::OpenOptimisticTransactionDB(
    const Options& options, const std::string& dbname,
    OptimisticTransactionDB** dbptr) {
  return OptimisticTransactionDB::Open(options, dbname, dbptr);
};

Status DefaultHooks::OpenOptimisticTransactionDB(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    OptimisticTransactionDB** dbptr) {
  return OptimisticTransactionDB::Open(db_options, dbname, column_families,
                                       handles, dbptr);
}

Status DefaultHooks::OpenAsSecondary(const Options& options,
                                     const std::string& name,
                                     const std::string& secondary_path,
                                     DB** dbptr) {
  return DB::OpenAsSecondary(options, name, secondary_path, dbptr);
}
Status DefaultHooks::OpenAsFollower(const Options& options,
                                    const std::string& name,
                                    const std::string& leader_path,
                                    std::unique_ptr<DB>* dbptr) {
  return DB::OpenAsFollower(options, name, leader_path, dbptr);
};

Status DefaultHooks::Open(const Options& options,
                          const blob_db::BlobDBOptions& bdb_options,
                          const std::string& dbname,
                          blob_db::BlobDB** blob_db) {
  return blob_db::BlobDB::Open(options, bdb_options, dbname, blob_db);
}

DefaultHooks defaultHooks;

}  // namespace ROCKSDB_NAMESPACE
