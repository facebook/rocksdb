// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/configurable.h"
#include "rocksdb/status.h"
#include "options/options_helper.h"
#include "util/string_util.h"

namespace rocksdb {

const bool Configurable::kIgnoreUnknownOptions = false;
const bool Configurable::kInputStringsEscaped = false;
const std::string Configurable::kPropNameValue = "name";
const std::string Configurable::kPropOptValue = "options";
const std::string Configurable::kOptionsPrefix = "rocksdb.";
  
std::string Configurable::GetOptionName(const std::string & longName) const {
  auto prefixLen = optionsPrefix_.length();
  if (longName.compare(0, prefixLen, optionsPrefix_) == 0) {
    return longName.substr(prefixLen);
  } else {
    return longName;
  }
}
  
const OptionTypeInfo *Configurable::FindOption(const std::string & option) const {
  if (optionsMap_ != nullptr) {
    auto iter = optionsMap_->find(option);  // Look up the value in the map
    if (iter == optionsMap_->end()) {
      auto period = option.find_last_of('.');
      if (period != std::string::npos && period < option.length()) {
	const char *suffix = option.c_str() + period + 1;
	if (kPropNameValue == suffix || kPropOptValue == suffix) {
	  iter = optionsMap_->find(option.substr(0, period));
	}
      }
    }
    if (iter != optionsMap_->end()) {
      return &iter->second;
    }
  }
  return nullptr;
}


Status Configurable::ParseEnum(const std::string & name, const std::string &, char *) {
  return Status::NotFound("Could not find enum property:", name);
}

Status Configurable::ParseExtension(const std::string & name, const std::string &) {
  return Status::NotFound("Could not find extension property:", name);
}

Status Configurable::ParseUnknown(const std::string & name, const std::string &) {
  return Status::NotFound("Could not find property:", name);
}
  
  
Status Configurable::SetOption(const OptionType & optType, char *optAddr,
			       const std::string & name, const std::string & value) {
  try {
    if (optType == OptionType::kUnknown) {
      return ParseUnknown(name, value);
    } else if (optAddr == nullptr) {
      return Status::NotFound("Could not find property:", name);
    } else if (optType == OptionType::kExtension) {
      return ParseExtension(name, value);
    } else if (optType == OptionType::kEnum) {
      return ParseEnum(name, value, optAddr);
    } else if (ParseOptionHelper(optAddr, optType, value)) {
      return Status::OK();
    } else {
      return Status::InvalidArgument("Error parsing:", name);
    }
  } catch (std::exception& e) {
    return Status::InvalidArgument("Error parsing " + name + ":" +
				   std::string(e.what()));
  }
}
  
Status Configurable::ConfigureOption(const std::string & name,
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
	return SetOption(optInfo->type, optAddr, optionName, optionValue);
      }
    }
  }
  return SetOption(OptionType::kUnknown, nullptr, optionName, optionValue);
}
  

Status Configurable::ConfigureOption(const std::string & name,
				     const std::string & value) {
  return ConfigureOption(name, value, kInputStringsEscaped);
}
  
Status Configurable::SetOptions(const std::unordered_map<std::string, std::string> & opts_map,
				bool input_strings_escaped,
				std::unordered_set<std::string> *unused_opts) {
  Status s;
  bool foundOne = false;
  for (const auto& o : opts_map) {
    s = ConfigureOption(o.first, o.second, input_strings_escaped);
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
      s = ConfigureOption(*it, opts_map.at(*it), input_strings_escaped);
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
    
Status Configurable::ConfigureFromMap(const std::unordered_map<std::string, std::string> & opts_map,
				      bool input_strings_escaped,
				      std::unordered_set<std::string> *unused_opts) {
  unused_opts->clear();
  return SetOptions(opts_map, input_strings_escaped, unused_opts);
}
  
Status Configurable::ConfigureFromMap(const std::unordered_map<std::string, std::string> & opts_map,
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
    

}  // namespace rocksdb
