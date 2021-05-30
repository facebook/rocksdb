// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include <cinttypes>

#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_env_wrapper.h"
#include "cloud/db_cloud_impl.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

void CloudEnvOptions::Dump(Logger* log) const {
  Header(log, "                         COptions.cloud_type: %u", cloud_type);
  Header(log, "                           COptions.log_type: %u", log_type);
  Header(log, "               COptions.keep_local_sst_files: %d",
         keep_local_sst_files);
  Header(log, "               COptions.keep_local_log_files: %d",
         keep_local_log_files);
  Header(log, "             COptions.server_side_encryption: %d",
         server_side_encryption);
  Header(log, "                  COptions.encryption_key_id: %s",
         encryption_key_id.c_str());
  Header(log, "           COptions.create_bucket_if_missing: %s",
         create_bucket_if_missing ? "true" : "false");
  Header(log, "                         COptions.run_purger: %s",
         run_purger ? "true" : "false");
  Header(log, "           COptions.ephemeral_resync_on_open: %s",
         ephemeral_resync_on_open ? "true" : "false");
  Header(log, "             COptions.skip_dbid_verification: %s",
         skip_dbid_verification ? "true" : "false");
  Header(log, "           COptions.use_aws_transfer_manager: %s",
         use_aws_transfer_manager ? "true" : "false");
  Header(log, "           COptions.number_objects_listed_in_one_iteration: %d",
         number_objects_listed_in_one_iteration);
  if (sst_file_cache != nullptr) {
    Header(log, "           COptions.sst_file_cache size: %ld bytes",
           sst_file_cache->GetCapacity());
  }
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
