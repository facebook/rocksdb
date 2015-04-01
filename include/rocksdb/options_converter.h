//  Copyright (c) 2015, Microsoft, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_ROCKSDB_INCLUDE_OPTIONS_CONVERTER_H_
#define STORAGE_ROCKSDB_INCLUDE_OPTIONS_CONVERTER_H_

#include "rocksdb/options.h"

namespace rocksdb {

Options ConvertStringToOptions(const std::string& optionsStr);

// argv must be in "db_bench --<key1>=<value1> --<key2>=<value2>" format
Options ConvertStringToOptions(int argc, char** argv);
}

#endif  // STORAGE_ROCKSDB_INCLUDE_OPTIONS_CONVERTER_H_