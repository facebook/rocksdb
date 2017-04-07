// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include <cinttypes>
#include "cloud/aws/aws_env.h"
#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_env_wrapper.h"
#include "cloud/db_cloud_impl.h"
#include "rocksdb/env.h"

namespace rocksdb {

void CloudEnvOptions::Dump(Logger* log) const {
  Header(log, "                               COptions.type: %u", cloud_type);
  Header(log, "                             COptions.region: %s",
         region.c_str());
  Header(log, "               COptions.keep_local_sst_files: %d",
         keep_local_sst_files);
  Header(log, "               COptions.keep_local_log_files: %d",
         keep_local_log_files);
  Header(log,
         "COptions.manifest_durable_periodicity_millis: %" PRIu64 " millis",
         manifest_durable_periodicity_millis);
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
