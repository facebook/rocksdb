//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <sstream>
#include <string>
#include <vector>

#pragma once
namespace rocksdb {

extern std::vector<std::string> stringSplit(std::string arg, char delim);

}  // namespace rocksdb
