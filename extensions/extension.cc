// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "rocksdb/extensions.h"
#include "rocksdb/status.h"
#include "options/options_helper.h"
#include "util/string_util.h"

namespace rocksdb {
Status Extension::ParseExtension(const DBOptions &, const ColumnFamilyOptions *,
				 const std::string & name, const std::string &) {
  return Status::NotFound("Could not find property:", name);
}

Status Extension::ParseUnknown(const DBOptions &, const ColumnFamilyOptions *,
			       const std::string & name, const std::string & value) {
  return ParseUnknown(name, value);
}


Status Extension::SetOption(const DBOptions & dbOpts, const ColumnFamilyOptions *cfOpts,
			    const OptionType & optType, char *optAddr,
			    const std::string & name, const std::string & value) {
  if (optType == OptionType::kExtension) {
    return ParseExtension(dbOpts, cfOpts, name, value);
  } else if (optType == OptionType::kUnknown) {
    return ParseUnknown(dbOpts, cfOpts, name, value);
  } else {
    return SetOption(optType, optAddr, name, value);
  }
}

Status Extension::ConfigureOption(const DBOptions & dbOpts,
				  const ColumnFamilyOptions * cfOpts,
				  const std::string & name,
				  const std::string & value,
				  bool input_strings_escaped) {
  const std::string & optionName  = GetOptionName(name);
  const std::string & optionValue = input_strings_escaped ? UnescapeOptionString(value) : value;
  void * optionsPtr  = GetOptionsPtr();
  if (optionsPtr != nullptr && optionsMap_ != nullptr) {
    auto optInfo = FindOption(optionName);  // Look up the value in the map
    if (optInfo != nullptr) {
      if (optInfo->verification == OptionVerificationType::kDeprecated) {
	return Status::OK();
      } else {
	char * optAddr = reinterpret_cast<char *>(optionsPtr) + optInfo->offset;
	return SetOption(dbOpts, cfOpts, optInfo->type, optAddr, optionName, optionValue);
      }
    }
  }
  return SetOption(dbOpts, cfOpts, OptionType::kUnknown, nullptr, optionName, optionValue);
}
  
Status Extension::SetOptions(const DBOptions & dbOpts,
			     const ColumnFamilyOptions * cfOpts,
			     const std::unordered_map<std::string, std::string> & opts_map,
			     bool input_strings_escaped,
			     std::unordered_set<std::string> *unused_opts) {
  Status s;
  bool foundOne = false;
  for (const auto& o : opts_map) {
    s = ConfigureOption(dbOpts, cfOpts, o.first, o.second, input_strings_escaped);
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
      s = ConfigureOption(dbOpts, cfOpts, *it, opts_map.at(*it), input_strings_escaped);
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
	  std::unordered_set<std::string> *unused_opts) {
  unused_opts->clear();
  return SetOptions(dbOpts, cfOpts, opts_map, input_strings_escaped, unused_opts);
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


bool Extension::ConfigureExtension(const std::string & property,
				   Extension *extension,
				   const std::string & name,
				   const std::string & value,
				   Status *status) {
  std::string extName;
  std::unordered_map<std::string, std::string> extOpts;
  if (IsExtensionOption(property, name, value, &extName, &extOpts)) {
    *status = ConfigureExtension(extension, extName, extOpts);
    return true;
  } else {
    return false;
  }
}

Status Extension::ConfigureExtension(Extension *extension,
				     const std::string & name,
				     const std::unordered_map<std::string, std::string> & options) {
  if (! name.empty()) {
    if (extension == nullptr || extension->Name() != name) {
      return Status::NotFound("Cannot create extension: ", name);
    }
  }
  if (! options.empty()) {
    if (extension != nullptr) {
      Status s = extension->ConfigureFromMap(options);
      if (s.IsNotFound()) {
	return Status::InvalidArgument("Invalid option for extension:", s.getState());
      } else {
	return s;
      }
    } else {
      return Status::NotFound("Cannot change null extension: ", name);
    } 
  }
  return Status::OK();
}
  
bool Extension::IsExtensionOption(const std::string & property,
				  const std::string & option,
				  const std::string & value,
				  std::string * extName,
				  std::unordered_map<std::string, std::string> * extOpts) {
  extOpts->clear();
  extName->clear();
  auto propLen = property.length();
  auto optLen = option.length();
  if (optLen < propLen || option.compare(0, propLen, property) != 0) {
    return false;
  } else if (optLen == propLen) {
    Status s = StringToMap(value, extOpts);
    if (s.ok()) {
      auto nameIter = extOpts->find(kPropNameValue);
      if (nameIter != extOpts->end()) {
	*extName = nameIter->second;
	extOpts->erase(nameIter);
      }
      return true;
    }
  } else if (option.at(propLen) == '.' && optLen > propLen + 1) {
    const char *suffix = option.c_str() + propLen + 1;
    if (kPropNameValue == suffix) { // "option.name"
      *extName = value;
      return true;
    } else if (kPropOptValue == suffix) { // "option.options"
      Status s = StringToMap(value, extOpts);
      return s.ok();
    }
  }
  return false;
}

} // End namespace rocksdb
#endif  // ROCKSDB_LITE
