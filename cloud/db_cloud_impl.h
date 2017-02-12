// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/cloud/db_cloud.h"

namespace rocksdb {

//
// All writes to this DB can be configured to be persisted
// in cloud storage.
//
class DBCloudImpl : public DBCloud {
 friend DBCloud;
 public:
  explicit DBCloudImpl(DB* db);

  virtual ~DBCloudImpl();

 protected:
  // The CloudEnv used by this open instance.
  CloudEnv* cenv_;
};

}
#endif  // ROCKSDB_LITE
