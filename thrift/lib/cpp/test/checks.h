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

#ifndef THRIFT_TEST_CHECKS_H_
#define THRIFT_TEST_CHECKS_H_ 1

#include <sstream>
#include "common/logging/logging.h"
#include "thrift/lib/cpp/protocol/TDebugProtocol.h"


namespace apache { namespace thrift { namespace test {

template<typename T1, typename T2>
inline std::string* formatString(const T1& v1, const T2& v2,
                                 const std::string& op) {
  std::stringstream ss;
  ss << ThriftDebugString(v1) << " "<< op <<" " << ThriftDebugString(v2);
  return new std::string(ss.str());
}

template<typename T1, typename T2>
inline std::string* checkThriftEqImpl(const T1& val1, const T2& val2)
{
  if (val1 == val2) {
    return NULL;
  }
  return formatString(val1, val2, "==");
}

template<typename T1, typename T2>
inline std::string* checkThriftNeImpl(const T1& val1, const T2& val2)
{
  if (val1 != val2) {
    return NULL;
  }
  return formatString(val1, val2, "!=");
}

template<typename T1, typename T2>
inline std::string* checkThriftLeImpl(const T1& val1, const T2& val2)
{
  if (val1 <= val2) {
    return NULL;
  }
  return formatString(val1, val2, "<=");
}

template<typename T1, typename T2>
inline std::string* checkThriftLtImpl(const T1& val1, const T2& val2) {

  if (val1 <  val2) {
    return NULL;
  }
  return formatString(val1, val2, "<");
}

template<typename T1, typename T2>
inline std::string* checkThriftGeImpl(const T1& val1, const T2& val2)
{
  if (val1 >= val2) {
    return NULL;
  }
  return formatString(val1, val2, ">=");
}

template<typename T1, typename T2>
inline std::string* checkThriftGtImpl(const T1& val1, const T2& val2)
{
  if (val1 >  val2) {
    return NULL;
  }
  return formatString(val1, val2, ">");
}

}}}

#define THRIFT_CHECK_OP(name, val1, val2) \
    while (std::string* _checkResult = \
             apache::thrift::test::checkThrift##name##Impl((val1), (val2))) \
      google::LogMessageFatal(__FILE__, __LINE__, \
                              google::CheckOpString(_checkResult)).stream()

#define THRIFT_CHECK_EQ(val1, val2) THRIFT_CHECK_OP(Eq, val1, val2)
#define THRIFT_CHECK_NE(val1, val2) THRIFT_CHECK_OP(Ne, val1, val2)
#define THRIFT_CHECK_LE(val1, val2) THRIFT_CHECK_OP(Le, val1, val2)
#define THRIFT_CHECK_LT(val1, val2) THRIFT_CHECK_OP(Lt, val1, val2)
#define THRIFT_CHECK_GE(val1, val2) THRIFT_CHECK_OP(Ge, val1, val2)
#define THRIFT_CHECK_GT(val1, val2) THRIFT_CHECK_OP(Gt, val1, val2)

#endif
