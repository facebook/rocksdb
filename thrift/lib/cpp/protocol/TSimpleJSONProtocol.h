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

#ifndef THRIFT_PROTOCOL_TSIMPLEJSONPROTOCOL_H_
#define THRIFT_PROTOCOL_TSIMPLEJSONPROTOCOL_H_ 1

#include "TJSONProtocol.h"

namespace apache { namespace thrift { namespace protocol {


/*
 * TsimpleJSONProtocol overrides parts of the regular JSON serialization to
 * comply with the Simple JSON format.
 * Namely, spitting only field names without verbose field type output
 */

class TSimpleJSONProtocol : public TVirtualProtocol<TSimpleJSONProtocol,
                                                    TJSONProtocol>{

 public:

  TSimpleJSONProtocol(boost::shared_ptr<TTransport> ptrans);

  ~TSimpleJSONProtocol();


 public:

  uint32_t writeFieldBegin(const char* name,
                                        const TType fieldType,
                                        const int16_t fieldId);


  uint32_t writeFieldEnd();

  uint32_t writeMapBegin(const TType keyType, const TType valType,
                               const uint32_t size);

  uint32_t writeMapEnd();

  uint32_t writeListBegin(const TType elemType, const uint32_t size);

  uint32_t writeSetBegin(const TType elemType, const uint32_t size);

  uint32_t writeBool(const bool value);
};

/**
 * Constructs input and output protocol objects given transports.
 */
class TSimpleJSONProtocolFactory : public TProtocolFactory {
 public:
  TSimpleJSONProtocolFactory() {}

  virtual ~TSimpleJSONProtocolFactory() {}

  boost::shared_ptr<TProtocol> getProtocol(
                               boost::shared_ptr<TTransport> trans) {
    return boost::shared_ptr<TProtocol>(new TSimpleJSONProtocol(trans));
  }
};

}}} // apache::thrift::protocol

#endif // #define THRIFT_PROTOCOL_TJSONPROTOCOL_H_ 1
