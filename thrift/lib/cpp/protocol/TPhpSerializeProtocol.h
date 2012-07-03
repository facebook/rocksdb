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

#ifndef _THRIFT_PROTOCOL_TPHPSERIALIZEPROTOCOL_H_
#define _THRIFT_PROTOCOL_TPHPSERIALIZEPROTOCOL_H_ 1

#include "thrift/lib/cpp/protocol/TVirtualProtocol.h"
#include "thrift/lib/cpp/transport/TBufferTransports.h"

#include <stack>
#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift { namespace protocol {


/**
 * A Thrift protocol for serializing Thrift objects into PHP's
 * "serialize" format.  Should work properly for objects that
 * PHP can properly express.  Currently, it can silently corrupt
 * data that PHP cannot properly express (lists or bools as map keys,
 * very large integers on 32-bit systems, and possibly others).
 */

class TPhpSerializeProtocol : public TVirtualProtocol<TPhpSerializeProtocol> {
 public:
  TPhpSerializeProtocol(boost::shared_ptr<TTransport> trans)
    : TVirtualProtocol<TPhpSerializeProtocol>(trans)
    , trans_(trans.get())
  {}

  uint32_t writeMessageBegin(const std::string& name,
                             const TMessageType messageType,
                             const int32_t seqid);

  uint32_t writeMessageEnd();

  uint32_t writeStructBegin(const char* name);

  uint32_t writeStructEnd();

  uint32_t writeFieldBegin(const char* name,
                           const TType fieldType,
                           const int16_t fieldId);

  uint32_t writeFieldEnd();

  uint32_t writeFieldStop();

  uint32_t writeListBegin(const TType elemType,
                          const uint32_t size);

  uint32_t writeListEnd();

  uint32_t writeSetBegin(const TType elemType,
                         const uint32_t size);

  uint32_t writeSetEnd();

  uint32_t writeMapBegin(const TType keyType,
                         const TType valType,
                         const uint32_t size);

  uint32_t writeMapEnd();

  uint32_t writeBool(const bool value);

  uint32_t writeByte(const int8_t byte);

  uint32_t writeI16(const int16_t i16);

  uint32_t writeI32(const int32_t i32);

  uint32_t writeI64(const int64_t i64);

  uint32_t writeDouble(const double dub);

  uint32_t writeString(const std::string& str);

  uint32_t writeBinary(const std::string& str);

 protected:
  uint32_t doWriteInt(const int64_t i64);
  uint32_t doWriteInt(const std::string& val);
  uint32_t doWriteString(const std::string& str, bool is_class);
  uint32_t doWriteListBegin(uint32_t size, bool is_map);
  uint32_t listKey();
  uint32_t write3(const char* v1, const char* v2, const char* v3);
  uint32_t write(const char* buf, uint32_t len);

  std::stack<int> listPosStack_;
  std::stack<int> structSizeStack_;
  std::stack< boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> > structBufferStack_;

  TTransport* trans_;
};

}}} // apache::thrift::protocol

#endif
