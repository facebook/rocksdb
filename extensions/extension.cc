// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "rocksdb/extensions.h"
#include "rocksdb/status.h"
#include "options/options_helper.h"

namespace rocksdb {
Status Extension::ConfigureOption(const OptionTypeMap *type_map,
				  char *opt_base,
				  const DBOptions & dbOpts,
				  const ColumnFamilyOptions * cfOpts,
				  const std::string & name,
				  const std::string & value,
				  bool input_strings_escaped) {
  if (opt_base == nullptr || type_map == nullptr) { // If there is no type map or options
    return SetOption(dbOpts, cfOpts, name, value, input_strings_escaped); // Simply call SetOption
  } else {                                          // Have a type map
    auto info = FindOption(*type_map, name);            // Look up the value in the map
    if (info == nullptr) {       // Didn't find it, use SetOption
      return SetOption(dbOpts, cfOpts, name, value, input_strings_escaped);
    } else if (info->verification == OptionVerificationType::kDeprecated ||
	       ParseOptionHelper(opt_base + info->offset, info->type, value)) {
      return Status::OK();
    } else {
      return SetOption(dbOpts, cfOpts, name, value, input_strings_escaped);
    }
  }
}
  
Status Extension::ConfigureFromMap(
	  const DBOptions & dbOpts,
	  const ColumnFamilyOptions * cfOpts,
          const std::unordered_map<std::string, std::string> & opts_map,
	  bool input_strings_escaped,
	  std::unordered_set<std::string> *unused_opts) {
  Status s;
  char *opts_base = reinterpret_cast<char*>(GetOptions());
  const OptionTypeMap *type_map = GetOptionTypeMap();
  unused_opts->clear();
  bool foundOne = false;
  for (const auto& o : opts_map) {
    s = ConfigureOption(type_map, opts_base, dbOpts, cfOpts,
			o.first, o.second, input_strings_escaped);
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
      s = ConfigureOption(type_map, opts_base, dbOpts, cfOpts,
			  *it, opts_map.at(*it), input_strings_escaped);
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
	  const DBOptions & dbOpts,
	  const ColumnFamilyOptions * cfOpts,
          const std::unordered_map<std::string, std::string> & opts_map,
	  bool input_strings_escaped,
	  bool ignore_unused_options) {
  std::unordered_set<std::string> unused_opts;
  Status s = ConfigureFromMap(dbOpts, cfOpts, opts_map, input_strings_escaped,
			      &unused_opts);
  if (! s.ok()) {
    return s;
  } else if (! ignore_unused_options && ! unused_opts.empty()) {
    auto iter = unused_opts.begin();
    return Status::NotFound("Could not find property:", *iter);
  } else {
    return s;
  }
}

Status Extension::ConfigureFromMap(
	  const DBOptions & dbOpts,
	  const ColumnFamilyOptions * cfOpts,
          const std::unordered_map<std::string, std::string> & opts_map,
	  bool input_strings_escaped) {
  return ConfigureFromMap(dbOpts, cfOpts, opts_map, input_strings_escaped,
			  Configurable::kIgnoreUnknownOptions);
}
  
  
Status Extension::ConfigureFromMap(
			   const DBOptions & dbOpts,
			   const ColumnFamilyOptions * cfOpts,
			   const std::unordered_map<std::string, std::string> & map) {
  return ConfigureFromMap(dbOpts, cfOpts, map,
			  Configurable::kInputStringsEscaped,
			  Configurable::kIgnoreUnknownOptions);
}

Status Extension::ConfigureFromMap(
			   const DBOptions & dbOpts,
			   const std::unordered_map<std::string, std::string> & map) {
  return ConfigureFromMap(dbOpts, nullptr, map);
}

Status Extension::ConfigureFromString(
		     const DBOptions & dbOpts,
		     const ColumnFamilyOptions * cfOpts,
		     const std::string & opt_str,
		     bool input_strings_escaped,
		     std::unordered_set<std::string> *unused_opts) {
  std::unordered_map<std::string, std::string> opt_map;
  Status s = StringToMap(opt_str, &opt_map);
  if (s.ok()) {
    s = ConfigureFromMap(dbOpts, cfOpts, opt_map, input_strings_escaped, 
			 unused_opts);
  }
  return s;
}

Status Extension::ConfigureFromString(
		     const DBOptions & dbOpts,
		     const ColumnFamilyOptions * cfOpts,
		     const std::string & opt_str,
		     bool input_strings_escaped,
		     bool ignore_unused_options) {
  std::unordered_map<std::string, std::string> opt_map;
  Status status = StringToMap(opt_str, &opt_map);
  if (status.ok()) {
    status = ConfigureFromMap(dbOpts, cfOpts, opt_map,
			      input_strings_escaped,
			      ignore_unused_options);
  }
  return status;
}

Status Extension::ConfigureFromString(
		     const DBOptions & dbOpts,
		     const ColumnFamilyOptions * cfOpts,
		     const std::string & opt_str,
		     bool input_strings_escaped) {
  return ConfigureFromString(dbOpts, cfOpts, opt_str, input_strings_escaped,
			     Configurable::kIgnoreUnknownOptions);
}
  
Status Extension::ConfigureFromString(
			   const DBOptions & dbOpts,
			   const ColumnFamilyOptions * cfOpts,
			   const std::string & opt_str) {
  return ConfigureFromString(dbOpts, cfOpts, opt_str,
			     Configurable::kInputStringsEscaped,
			     Configurable::kIgnoreUnknownOptions);
}

  
Status Extension::ConfigureFromString(
			   const DBOptions & dbOpts,
			   const std::string & opt_str,
			   bool input_strings_escaped) {
  return ConfigureFromString(dbOpts, nullptr, opt_str,
			     input_strings_escaped,
			     Configurable::kIgnoreUnknownOptions);
}

Status Extension::ConfigureFromString(
			   const DBOptions & dbOpts,
			   const std::string & opt_str) {
  return ConfigureFromString(dbOpts, opt_str,
			     Configurable::kInputStringsEscaped);
}

  
Status Extension::SetExtensionOption(Extension * extension,
				     const std::string & prefix,
				     const std::string & name,
				     const std::string & value,
				     bool input_strings_escaped) {
  Status s = Status::NotFound();
  if (extension != nullptr) {
    // If there is a valid extension, see if this option is for it
    s = extension->SetOption(name, value, input_strings_escaped);
  }
  if (s.IsNotFound()) {
    std::string extName, extProps;
    s = PrefixMatchesOption(prefix, name, value, &extName, &extProps);
    if (s.ok()) {
      if (extension != nullptr && extension->Name() == extName) {
	s = extension->ConfigureFromString(extProps);
      } else {
	s = Status::InvalidArgument("Cannot configure extension:", name);
      }
    }
  }
  return s;
}

} // End namespace rocksdb
#endif  // ROCKSDB_LITE
