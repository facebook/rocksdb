#include "rocksdb/wal_dump.h"
#include <memory>
#include <thread>

#include "rocksdb/env.h"
#include "db/wal_manager.h"
#include "db/version_set.h"
#include "options/db_options.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE

Status WalDump::CreateWalIter(std::string dbname,
                                   SequenceNumber start_seq,
                                   SequenceNumber end_seq,
                                   std::unique_ptr<TransactionLogIterator>* iter,
                                   const TransactionLogIterator::ReadOptions& read_options) {
  thread_local ImmutableDBOptions db_options;
  thread_local EnvOptions env_options;

  db_options.db_paths.emplace_back(dbname, std::numeric_limits<uint64_t>::max());
  db_options.wal_dir = dbname;
  db_options.env = Env::Default();

  std::unique_ptr<WalManager> wal_manager(new WalManager(db_options, env_options, nullptr));
  thread_local std::unique_ptr<VersionSet> versions;
  versions.reset(new VersionSet(dbname, &db_options, env_options, nullptr, nullptr, nullptr, nullptr, nullptr, ""));
  versions->SetLastSequence(end_seq);

  auto s = wal_manager->GetUpdatesSince(start_seq, iter, read_options, versions.get());
  if (!s.ok()) {
    return Status::Corruption("failed to get updates of wal manager, Err: " + s.ToString());
  }
  return Status::OK();
}

#endif  // ROCKSDB_LITE
}  // namespace rocksdb
