// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/configurable.h"
#include "rocksdb/status.h"
#include "options/options_helper.h"

#include <iostream>

namespace rocksdb {

const bool Configurable::kIgnoreUnknownOptions = false;
const bool Configurable::kInputStringsEscaped = false;
const std::string Configurable::kPropNameValue = "name";
const std::string Configurable::kPropOptValue = "options";
const std::string Configurable::kOptionsPrefix = "rocksdb.";
  

bool Configurable::ParseOption(const std::string &,
			       const std::string & value,
			       const OptionTypeInfo *opt_info,
			       char *opt_base) {
  if (opt_info == nullptr || opt_base == nullptr) {       // Didn't find it, use SetOption
    return false;
  } else if (opt_info->verification == OptionVerificationType::kDeprecated) {
    return true;
  } else {
    return ParseOptionHelper(opt_base + opt_info->offset, opt_info->type, value);
  }
}
  
const OptionTypeInfo *Configurable::FindOption(const OptionTypeMap & type_map,
					       const std::string & option) const {
  auto iter = type_map.find(option);  // Look up the value in the map
  std::cout << "MJR: Looking for option: " << option << "; found=" <<(iter == type_map.end()) << std::endl;
  if (iter == type_map.end()) {
    const std::string & prefix = GetOptionPrefix();
    size_t length = prefix.size();
    if (option.compare(0, length, prefix) == 0) {
      iter = type_map.find(option.substr(length));
      std::cout << "MJR: Looking for short option: " << option .substr(length) << "; found=" <<(iter == type_map.end()) << std::endl;
    } else {
      iter = type_map.find(prefix + option);
      std::cout << "MJR: Looking for long option: " << (prefix+option) << "; found=" << (iter == type_map.end()) << std::endl;
    }
  }
  if (iter != type_map.end()) {
    return &iter->second;
  } else {
    return nullptr;
  }
}

bool Configurable::OptionMatchesName(const std::string & option,
				     const std::string & name) const {
  if (option == name) {
    return true;
  } else {
    const std::string & prefix = GetOptionPrefix();
    size_t length = prefix.size();
    return ((option.compare(0, length, prefix) == 0) && 
	    (option.compare(length, name.size(), name) == 0));
  }
}
  

Status Configurable::ConfigureOption(const OptionTypeMap *type_map,
				     char *opt_base,
				     const std::string & name,
				     const std::string & value,
				     bool input_strings_escaped) {
  if (opt_base == nullptr || type_map == nullptr) { // If there is no type map or options
    return SetOption(name, value, input_strings_escaped); // Simply call SetOption
  } else {                                          // Have a type map
    auto opt_info = FindOption(*type_map, name);  // Look up the value in the map
    if (ParseOption(name, value, opt_info, opt_base)) {
      return Status::OK();
    } else {
      return SetOption(name, value, input_strings_escaped);
    }
  }
}
  
Status Configurable::ConfigureFromMap(
          const std::unordered_map<std::string, std::string> & opts_map,
	  bool input_strings_escaped,
	  std::unordered_set<std::string> *unused_opts) {
  Status s;
  char *opts_base = reinterpret_cast<char*>(GetOptions());
  const OptionTypeMap *type_map = GetOptionTypeMap();
  unused_opts->clear();
  bool foundOne = false;
  for (const auto& o : opts_map) {
    s = ConfigureOption(type_map, opts_base, o.first, o.second, input_strings_escaped);
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
      s = ConfigureOption(type_map, opts_base, *it, opts_map.at(*it), input_strings_escaped);
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

Status Configurable::ConfigureFromMap(
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

Status Configurable::ConfigureFromMap(
			   const std::unordered_map<std::string, std::string> & map,
			   bool input_strings_escaped) {
  return ConfigureFromMap(map, input_strings_escaped, kIgnoreUnknownOptions);
}

Status Configurable::ConfigureFromMap(
		             const std::unordered_map<std::string, std::string> & map) {
    return ConfigureFromMap(map, kInputStringsEscaped);
}
Status Configurable::ConfigureFromString(const std::string & opt_str,
				      bool input_strings_escaped,
				      std::unordered_set<std::string> *unused_opts) {
  std::unordered_map<std::string, std::string> opt_map;
  Status s = StringToMap(opt_str, &opt_map);
  if (s.ok()) {
    s = ConfigureFromMap(opt_map, input_strings_escaped, unused_opts);
  }
  return s;
}

Status Configurable::ConfigureFromString(const std::string & opt_str,
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

Status Configurable::ConfigureFromString(const std::string & opt_str,
				      bool input_strings_escaped) {
  return ConfigureFromString(opt_str, input_strings_escaped, kIgnoreUnknownOptions);
}
  
Status Configurable::ConfigureFromString(const std::string & opt_str) {
  return ConfigureFromString(opt_str, kInputStringsEscaped);
}
    
Status Configurable::SetOption(const std::string & name,
			    const std::string & value,
			    bool ) {
  const OptionTypeMap *type_map = GetOptionTypeMap();
  char *opt_base = reinterpret_cast<char*>(GetOptions());
  if (type_map != nullptr && opt_base != nullptr) {
    auto opt_info = FindOption(*type_map, name);  // Look up the value in the map
    if (ParseOption(name, value, opt_info, opt_base)) {
      return Status::OK();
    }
  }
  return Status::NotFound("Unrecognized property: ", name);
}

Status Configurable::PrefixMatchesOption(const std::string & prefix,
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
	    if (it.first == kPropNameValue) {
	      isValid = true;
	      *name = it.second;
	    } else if (it.first == kPropOptValue) {
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
				kPropNameValue.size(),
				kPropNameValue) == 0) {
	*name = value;
	s = Status::OK();
      }
    }
  }
  return s;
}
}  // namespace rocksdb
