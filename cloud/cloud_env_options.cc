// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "rocksdb/cloud/cloud_env_options.h"

#include <cinttypes>

#include "cloud/db_cloud_impl.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"

namespace ROCKSDB_NAMESPACE {

void CloudEnvOptions::Dump(Logger* log) const {
  auto provider = storage_provider.get();
  auto controller = cloud_log_controller.get();
  Header(log, "                         COptions.cloud_type: %s", (provider != nullptr) ? provider->Name() : "Unknown");
  Header(log, "                    COptions.src_bucket_name: %s",
         src_bucket.GetBucketName().c_str());
  Header(log, "                    COptions.src_object_path: %s",
         src_bucket.GetObjectPath().c_str());
  Header(log, "                  COptions.src_bucket_region: %s",
         src_bucket.GetRegion().c_str());
  Header(log, "                   COptions.dest_bucket_name: %s",
         dest_bucket.GetBucketName().c_str());
  Header(log, "                   COptions.dest_object_path: %s",
         dest_bucket.GetObjectPath().c_str());
  Header(log, "                 COptions.dest_bucket_region: %s",
         dest_bucket.GetRegion().c_str());
  Header(log, "                           COptions.log_type: %s", (controller != nullptr) ? controller->Name() : "None");
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
  Header(log, "           COptions.resync_on_open: %s",
         resync_on_open ? "true" : "false");
  Header(log, "             COptions.skip_dbid_verification: %s",
         skip_dbid_verification ? "true" : "false");
  Header(log, "           COptions.use_aws_transfer_manager: %s",
         use_aws_transfer_manager ? "true" : "false");
  Header(log, "           COptions.number_objects_listed_in_one_iteration: %d",
         number_objects_listed_in_one_iteration);
  Header(log, "   COptions.use_direct_io_for_cloud_download: %d",
         use_direct_io_for_cloud_download);
  Header(log, "        COptions.roll_cloud_manifest_on_open: %d",
         roll_cloud_manifest_on_open);
  Header(log, "                     COptions.cookie_on_open: %s",
         cookie_on_open.c_str());
  Header(log, "                 COptions.new_cookie_on_open: %s",
         new_cookie_on_open.c_str());
  if (sst_file_cache != nullptr) {
    Header(log, "           COptions.sst_file_cache size: %ld bytes",
           sst_file_cache->GetCapacity());
  }
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
