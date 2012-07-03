/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef THRIFT_UTIL_ENUMUTILS_H_
#define THRIFT_UTIL_ENUMUTILS_H_ 1

namespace apache { namespace thrift {

namespace util {

/**
 * Parses an enum name to the enum type
 */
template<typename String, typename EnumType>
bool tryParseEnum(const String& name, EnumType* out) {
  return TEnumTraits<EnumType>::findValue(name.c_str(), out);
};

template<typename EnumType>
bool tryParseEnum(const char* name, EnumType* out) {
  return TEnumTraits<EnumType>::findValue(name, out);
};

/**
 * Returns the human-readable name for an Enum type.
 */
template<typename EnumType>
const char* enumName(EnumType value,
                     const char* defaultName = nullptr) {
  const char* name = TEnumTraits<EnumType>::findName(value);
  if (name) {
    return name;
  } else {
    return defaultName;
  }
};

}}} // apache::thrift::util

#endif // THRIFT_UTIL_ENUMUTILS_H_ 1
