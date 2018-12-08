// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "rocksdb/extensions.h"
#include "rocksdb/status.h"
#include "options/options_helper.h"

namespace rocksdb {
const std::string ExtensionConsts::kPropNameValue = "name";
const std::string ExtensionConsts::kPropOptValue = "options";  
bool ExtensionConsts::kIgnoreUnknownOptions = false;
bool ExtensionConsts::kInputStringsEscaped = false;
  
Status Extension::ConfigureFromMap(
	  const DBOptions & dbOpts,
	  const ColumnFamilyOptions * cfOpts,
          const std::unordered_map<std::string, std::string> & opts_map,
	  bool input_strings_escaped,
	  std::unordered_set<std::string> *unused_opts) {
  Status s;
  unused_opts->clear();
  bool foundOne = false;
  for (const auto& o : opts_map) {
    s = SetOption(dbOpts, cfOpts, o.first, o.second, input_strings_escaped);
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
      s = SetOption(dbOpts, cfOpts, *it, opts_map.at(*it), input_strings_escaped);
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
			  ExtensionConsts::kIgnoreUnknownOptions);
}
  
  
Status Extension::ConfigureFromMap(
			   const DBOptions & dbOpts,
			   const ColumnFamilyOptions * cfOpts,
			   const std::unordered_map<std::string, std::string> & map) {
  return ConfigureFromMap(dbOpts, cfOpts, map,
			  ExtensionConsts::kInputStringsEscaped,
			  ExtensionConsts::kIgnoreUnknownOptions);
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
			     ExtensionConsts::kIgnoreUnknownOptions);
}
  
Status Extension::ConfigureFromString(
			   const DBOptions & dbOpts,
			   const ColumnFamilyOptions * cfOpts,
			   const std::string & opt_str) {
  return ConfigureFromString(dbOpts, cfOpts, opt_str,
			     ExtensionConsts::kInputStringsEscaped,
			     ExtensionConsts::kIgnoreUnknownOptions);
}

  
Status Extension::ConfigureFromString(
			   const DBOptions & dbOpts,
			   const std::string & opt_str,
			   bool input_strings_escaped) {
  return ConfigureFromString(dbOpts, nullptr, opt_str,
			     input_strings_escaped,
			     ExtensionConsts::kIgnoreUnknownOptions);
}

Status Extension::ConfigureFromString(
			   const DBOptions & dbOpts,
			   const std::string & opt_str) {
  return ConfigureFromString(dbOpts, opt_str,
			     ExtensionConsts::kInputStringsEscaped);
}

Status Extension::ConfigureFromMap(
          const std::unordered_map<std::string, std::string> & opts_map,
	  bool input_strings_escaped,
	  std::unordered_set<std::string> *unused_opts) {
  Status s;
  unused_opts->clear();
  bool foundOne = false;
  for (const auto& o : opts_map) {
    s = SetOption(o.first, o.second, input_strings_escaped);
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
      s = SetOption(*it, opts_map.at(*it), input_strings_escaped);
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
	  bool ignore_unused_options) {
  
  std::unordered_set<std::string> unused_opts;
  Status s = ConfigureFromMap(opts_map, input_strings_escaped, &unused_opts);
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
			   const std::unordered_map<std::string, std::string> & map,
			   bool input_strings_escaped) {
  return ConfigureFromMap(map, input_strings_escaped,
			  ExtensionConsts::kIgnoreUnknownOptions);
}

  Status Extension::ConfigureFromMap(
		             const std::unordered_map<std::string, std::string> & map) {
    return ConfigureFromMap(map, ExtensionConsts::kInputStringsEscaped);
}
  
  
Status Extension::ConfigureFromString(const std::string & opt_str,
				      bool input_strings_escaped,
				      std::unordered_set<std::string> *unused_opts) {
  std::unordered_map<std::string, std::string> opt_map;
  Status s = StringToMap(opt_str, &opt_map);
  if (s.ok()) {
    s = ConfigureFromMap(opt_map, input_strings_escaped, unused_opts);
  }
  return s;
}

Status Extension::ConfigureFromString(const std::string & opt_str,
				      bool input_strings_escaped,
				      bool ignore_unused_options) {
  std::unordered_map<std::string, std::string> opt_map;
  Status status = StringToMap(opt_str, &opt_map);
  if (status.ok()) {
    status = ConfigureFromMap(opt_map,
			      input_strings_escaped,
			      ignore_unused_options);
  }
  return status;
}

Status Extension::ConfigureFromString(const std::string & opt_str,
				      bool input_strings_escaped) {
  return ConfigureFromString(opt_str, input_strings_escaped,
			     ExtensionConsts::kIgnoreUnknownOptions);
}
  
Status Extension::ConfigureFromString(const std::string & opt_str) {
  return ConfigureFromString(opt_str, ExtensionConsts::kInputStringsEscaped);
}
    
Status Extension::SetOption(const std::string & name,
			    const std::string &,
			    bool ) {
  return Status::NotFound("Unrecognized property: ", name);
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

Status Extension::PrefixMatchesOption(const std::string & prefix,
				      const std::string & option,
				      const std::string & value,
				      std::string * name,
				      std::string * props) {
  size_t prefixLen = prefix.size();
  size_t optionLen = option.size();
  Status s = Status::NotFound();
  name->clear();
  props->clear();

  if (optionLen >= prefixLen) {
    if (option.compare(0, prefixLen, prefix) == 0) {
      if (prefixLen == optionLen) {
	std::unordered_map<std::string, std::string> map;
	s = StringToMap(value, &map);
	if (s.ok()) {
	  bool isValid = true;
	  for (auto it : map) {
	    if (it.first == ExtensionConsts::kPropNameValue) {
	      isValid = true;
	      *name = it.second;
	    } else if (it.first == ExtensionConsts::kPropOptValue) {
	      *props = it.second;
	    } else {
	      isValid = false;
	      break;
	    }
	  }
	  if (! isValid) {
	    s = Status::InvalidArgument("Invalid property value:", option);
	  }
	}
      } else if (option.at(prefixLen) == '.' &&
		 option.compare(prefixLen+1,
				ExtensionConsts::kPropNameValue.size(),
				ExtensionConsts::kPropNameValue) == 0) {
	*name = value;
	s = Status::OK();
      }
    }
  }
  return s;
}
} // End namespace rocksdb
#endif  // ROCKSDB_LITE
