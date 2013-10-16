//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

namespace rocksdb {

class StatsLogger {

 public:

  virtual void Log_Deploy_Stats(const std::string& db_version,
                                const std::string& machine_info,
                                const std::string& data_dir,
                                const uint64_t data_size,
                                const uint32_t file_number,
                                const std::string& data_size_per_level,
                                const std::string& file_number_per_level,
                                const int64_t& ts_unix) = 0;
  virtual ~StatsLogger() {}

};

}
