#ifndef COMMON_CLIENT_MGMT_AGGR_UTILTEST
#define COMMON_CLIENT_MGMT_AGGR_UTILTEST 1

#include <math.h>
#include <signal.h>
#include <time.h>
#include <stdint.h>
#include <iostream>

#include <string>
#include <vector>

#include <boost/lexical_cast.hpp>

#include "thrift/lib/cpp/server/test/gen-cpp/AggregatorTest_types.h"
#include "thrift/lib/cpp/server/test/gen-cpp/AggregatorTest.h"

#include "common/logging/logging.h"

static inline void defaultData(apache::thrift::async::StructRequest* res) {
  res->i32Val = 32;
  res->i64Val = 64;
  res->doubleVal = -12.34;
  res->stringVal = "string";
}

static inline void randomData(apache::thrift::async::StructRequest* res) {
  res->i32Val = ::random();
  res->i64Val = ::random();
  res->doubleVal = ::random() * ((::random() % 2) == 0 ? -1 : 1);
  res->stringVal = boost::lexical_cast<std::string>(::random());
}

static inline void toAnswerString(std::string* dest,
                          const int32_t i32Val,
                          const int64_t i64Val,
                          const double doubleVal,
                          const std::string& stringVal) {
  *dest +=
    boost::lexical_cast<std::string>(i32Val) +
    ";" +
    boost::lexical_cast<std::string>(i64Val) +
    ";" +
    boost::lexical_cast<std::string>(doubleVal) +
    ";" +
    boost::lexical_cast<std::string>(stringVal);
}

static inline void toAnswerString(std::string* dest,
                         const apache::thrift::async::StructRequest& request) {
  toAnswerString(dest, request.i32Val,
                      request.i64Val,
                      request.doubleVal,
                      request.stringVal);
}

static inline void toAnswerString(std::string* dest,
                      const int32_t i32Val,
                      const int64_t i64Val,
                      const double doubleVal,
                      const std::string& stringVal,
                      const apache::thrift::async::StructRequest& structVal) {
  toAnswerString(dest, i32Val,
                      i64Val,
                      doubleVal,
                      stringVal);
  *dest += "*";
  toAnswerString(dest, structVal);
}

static inline void zeroResponse(apache::thrift::async::StructResponse* dest) {
  dest->request.i32Val = 0;
  dest->request.i64Val = 0;
  dest->request.doubleVal = 0.0;
  dest->request.stringVal = "";
  dest->errorCode = 0;
  dest->answerString = "";
}

static inline void printResponse(
                        const apache::thrift::async::StructResponse& x) {
  LOG(INFO) << "request.i32Val: " << x.request.i32Val
            << ", request.i64Val: " << x.request.i64Val
            << ", request.doubleVal: " << x.request.doubleVal
            << ", request.stringVal: " << x.request.stringVal
            << ", request.stringVal: " << x.request.stringVal
            << ", errorCode: " << x.errorCode
            << ", answerString: " << x.answerString;
}

static inline bool equalResult(
                        const apache::thrift::async::StructResponse& ethalon,
                        const apache::thrift::async::StructResponse& response) {
  bool res = ethalon.request.i32Val == response.request.i32Val
  && ethalon.request.i64Val == response.request.i64Val
  && fabs(ethalon.request.doubleVal - response.request.doubleVal) < 0.0001
  && ethalon.request.stringVal == response.request.stringVal
  && ethalon.errorCode == response.errorCode
  && ethalon.answerString == response.answerString;
  if (!res) {
    printResponse(ethalon);
    printResponse(response);
  }
  return res;
}

static inline void addResponse(apache::thrift::async::StructResponse* dest,
                          const apache::thrift::async::StructResponse& src) {
  dest->request.i32Val += src.request.i32Val;
  dest->request.i64Val += src.request.i64Val;
  dest->request.doubleVal += src.request.doubleVal;
  dest->request.stringVal += src.request.stringVal;
  dest->errorCode += src.errorCode;
  dest->answerString += "/";
  dest->answerString += src.answerString;
}

static inline void addRequest(apache::thrift::async::StructResponse* dest,
                           const apache::thrift::async::StructRequest& src) {
  apache::thrift::async::StructResponse response;
  response.request = src;
  response.errorCode = 0;
  response.answerString = "";
  toAnswerString(&response.answerString, response.request);
  addResponse(dest, response);
}

static inline void addRequest(apache::thrift::async::StructResponse* dest,
                      const int32_t i32Val,
                      const int64_t i64Val,
                      const double doubleVal,
                      const std::string& stringVal,
                      const apache::thrift::async::StructRequest& structVal) {
  apache::thrift::async::StructResponse response;
  response.request = structVal;
  response.errorCode = 0;
  response.answerString = "";
  toAnswerString(&response.answerString,
      i32Val, i64Val, doubleVal, stringVal, response.request);
  addResponse(dest, response);
}

#endif
