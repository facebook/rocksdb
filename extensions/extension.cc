// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "rocksdb/extensions.h"
#include "rocksdb/status.h"
#include "options/options_helper.h"

#include <iostream> // MJR

namespace rocksdb {
const std::string ExtensionConsts::kPropNameValue = "name";
const std::string ExtensionConsts::kPropOptValue = "options";  
bool ExtensionConsts::kIgnoreUnknownOptions = false;
bool ExtensionConsts::kInputStringsEscaped = false;
  
bool Extension::MatchesProperty(const std::string & propName,
				const std::string & prefix,
				const std::string & suffix) {
  size_t propLen = propName.size();
  size_t prefLen  = prefix.size();
  size_t suffLen  = suffix.size();
  if (propLen != prefLen + suffLen) {
    return false;
  } else if (propName.compare(0, prefLen, prefix) != 0) {
    return false;
  } else if (propName.compare(prefLen, suffLen, suffix) != 0) {
    return false;
  } else {
    return true;
  }
}

Status Extension::ConfigureFromMap(
          const std::unordered_map<std::string, std::string> & opts_map,
	  const DBOptions & dbOpts,
	  const ColumnFamilyOptions * cfOpts,
	  bool input_strings_escaped,
	  std::unordered_set<std::string> *unused_opts) {
  Status s;
  Status saved = Status::OK();
  unused_opts->clear();
  for (const auto& o : opts_map) {
    s = SetOption(o.first, o.second, dbOpts, cfOpts,
		  false, input_strings_escaped);
    if (s.IsInvalidArgument() || s.IsNotFound()) {
      saved = s;
      unused_opts->insert(o.first);
    } else if (! s.ok()) {
      return s;
    }
  }
  return saved;
}
Status Extension::ConfigureFromMap(
          const std::unordered_map<std::string, std::string> & opts_map,
	  const DBOptions & dbOpts,
	  const ColumnFamilyOptions * cfOpts,
	  bool ignore_unknown_options,
	  bool input_strings_escaped) {
  std::unordered_set<std::string> unused_opts;
  Status saved = ConfigureFromMap(opts_map, dbOpts, cfOpts,
				  input_strings_escaped, &unused_opts);
  if ((saved.IsInvalidArgument() || saved.IsNotFound()) && ! unused_opts.empty() &&
      unused_opts.size() != opts_map.size())  {
    bool found = false;
    do {
      found = false;
      for (auto it = unused_opts.begin(); it != unused_opts.end(); ) {
	Status s = SetOption(*it, opts_map.at(*it), dbOpts, cfOpts,
			     false, input_strings_escaped);
	if (s.ok()) {
	  found = true;
	  it = unused_opts.erase(it);
	} else if (s.IsInvalidArgument() || s.IsNotFound()) {
	  saved = s;
	  ++it;
	} else {
	  return s;
	}
      }
    } while (found && ! unused_opts.empty());
  }
  if (ignore_unknown_options || saved.ok() || unused_opts.empty()) {
    if (cfOpts != nullptr) {
      return SanitizeOptions(dbOpts, *cfOpts);
    } else {
      return SanitizeOptions(dbOpts);
    }
  } else {
    return saved;
  }
}
  
Status Extension::ConfigureFromString(
	  const std::string & opt_str,
	  const DBOptions & dbOpts,
	  const ColumnFamilyOptions * cfOpts,
	  bool ignore_unknown_options,
	  bool input_strings_escaped) {
  std::unordered_map<std::string, std::string> opt_map;
  Status s = StringToMap(opt_str, &opt_map);
  if (s.ok()) {
    s = ConfigureFromMap(opt_map, dbOpts, cfOpts, 
			 ignore_unknown_options,
			 input_strings_escaped);
  }
  return s;
}

Status Extension::SetOption(const std::string & name,
			    const std::string &,
			    bool ignore_unknown_options,
			    bool ) {
  
  if (! ignore_unknown_options) {
    return Status::InvalidArgument("Unrecognized option: ", name);
  } else {
    return Status::OK();
  }
}

}
#endif  // ROCKSDB_LITE
