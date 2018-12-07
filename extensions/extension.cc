// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "rocksdb/extensions.h"
#include "rocksdb/extension_loader.h"
#include "rocksdb/status.h"
#include "options/options_helper.h"

namespace rocksdb {
const std::string ExtensionConsts::kPropNameValue = "name";
const std::string ExtensionConsts::kPropOptValue = "options";  
bool ExtensionConsts::kIgnoreUnknownOptions = false;
bool ExtensionConsts::kInputStringsEscaped = false;
  
Status Extension::ConfigureFromMap(
          const std::unordered_map<std::string, std::string> & opts_map,
	  bool input_strings_escaped,
	  const DBOptions & dbOpts,
	  const ColumnFamilyOptions * cfOpts,
	  std::unordered_set<std::string> *unused_opts) {
  Status s;
  unused_opts->clear();
  bool foundOne = false;
  for (const auto& o : opts_map) {
    s = SetOption(o.first, o.second, input_strings_escaped,
		       dbOpts, cfOpts, false);
    if (s.IsNotFound()) {
      unused_opts->insert(o.first);
    } else if (! s.ok()) {
      return s;
    } else {
      foundOne = true;
    }
  }
  while (foundOne  && ! unused_opts->empty()) {
    foundOne = false;
    for (auto it = unused_opts->begin(); it != unused_opts->end(); ) {
      s = SetOption(*it, opts_map.at(*it), input_strings_escaped,
			 dbOpts, cfOpts, false);
      if (s.ok()) {
	foundOne = true;
	it = unused_opts->erase(it);
      } else if (s.IsNotFound()) {
	++it;
      } else {
	return s;
      }
    }
  }
  return Status::OK();
}
  
Status Extension::ConfigureFromMap(
          const std::unordered_map<std::string, std::string> & opts_map,
	  bool input_strings_escaped,
	  const DBOptions & dbOpts,
	  const ColumnFamilyOptions * cfOpts,
	  bool ignore_unused_options) {
  
  std::unordered_set<std::string> unused_opts;
  Status s = ConfigureFromMap(opts_map, input_strings_escaped,
			      dbOpts, cfOpts, &unused_opts);
  if (! s.ok()) {
    return s;
  } else if (! ignore_unused_options && ! unused_opts.empty()) {
    auto iter = unused_opts.begin();
    return Status::NotFound("Could not find property:", *iter);
  } else {
    return s;
  }
}
  
Status Extension::ConfigureFromString(
		     const std::string & opt_str,
		     bool input_strings_escaped,
		     const DBOptions & dbOpts,
		     const ColumnFamilyOptions * cfOpts,
		     std::unordered_set<std::string> *unused_opts) {
  std::unordered_map<std::string, std::string> opt_map;
  Status s = StringToMap(opt_str, &opt_map);
  if (s.ok()) {
    s = ConfigureFromMap(opt_map, input_strings_escaped, dbOpts, 
			 cfOpts, unused_opts);
  }
  return s;
}

Status Extension::ConfigureFromString(
		     const std::string & opt_str,
		     bool input_strings_escaped,
		     const DBOptions & dbOpts,
		     const ColumnFamilyOptions * cfOpts,
		     bool ignore_unused_options) {
  std::unordered_set<std::string> unused_opts;
  Status status = ConfigureFromString(opt_str, input_strings_escaped, 
				      dbOpts, cfOpts, &unused_opts);
  if (! status.ok()) {
    return status;
  } else if (! ignore_unused_options && ! unused_opts.empty()) {
    return Status::NotFound("Unrecognized property: ", *(unused_opts.begin()));
  } else {
    return status;
  }
}
  
Status Extension::SetOption(const std::string & name,
				 const std::string &,
				 bool ) {
  return Status::NotFound("Unrecognized property: ", name);
}

Status Extension::SetExtensionOption(const std::string & prefix,
				     const std::string & name,
				     const std::string & value,
				     bool input_strings_escaped,
				     Extension * extension) {
  bool isExact;
  if (ExtensionLoader::PropertyMatchesPrefix(prefix, name, &isExact)) {
    if (isExact && extension != nullptr && value == extension->Name()) {
      return Status::OK();
    }
    return Status::InvalidArgument();
  } else if (extension != nullptr) {
    return extension->SetOption(name, value, input_strings_escaped);
  } else {
    return Status::NotFound("Unrecognized property: ", name);
  }
}
} // End namespace rocksdb
#endif  // ROCKSDB_LITE
