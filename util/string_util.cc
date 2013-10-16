//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <sstream>
#include <string>
#include <vector>

namespace rocksdb {

using namespace std;
using std::string;
using std::vector;
using std::stringstream;

vector<string> stringSplit(string arg, char delim) {
  vector<string> splits;
  stringstream ss(arg);
  string item;
  while(getline(ss, item, delim)) {
    splits.push_back(item);
  }
  return splits;
}
}
