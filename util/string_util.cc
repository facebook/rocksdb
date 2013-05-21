// Copyright (c) 2013 Facebook.

#include <sstream>
#include <string>
#include <vector>

namespace leveldb {

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
