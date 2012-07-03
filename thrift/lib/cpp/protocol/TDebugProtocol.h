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

#ifndef THRIFT_PROTOCOL_TDEBUGPROTOCOL_H
#define THRIFT_PROTOCOL_TDEBUGPROTOCOL_H

#include "thrift/lib/cpp/protocol/TVirtualProtocol.h"

#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift { namespace protocol {

/*

!!! EXPERIMENTAL CODE !!!

This protocol is very much a work in progress.
It doesn't handle many cases properly.
It throws exceptions in many cases.
It probably segfaults in many cases.
Bug reports and feature requests are welcome.
Complaints are not. :R

*/


/**
 * Protocol that prints the payload in a nice human-readable format.
 * Reading from this protocol is not supported.
 *
 */
class TDebugProtocol : public TVirtualProtocol<TDebugProtocol> {
 private:
  enum write_state_t
  { UNINIT
  , STRUCT
  , LIST
  , SET
  , MAP_KEY
  , MAP_VALUE
  };

 public:
  TDebugProtocol(boost::shared_ptr<TTransport> trans)
    : TVirtualProtocol<TDebugProtocol>(trans)
    , trans_(trans.get())
    , string_limit_(DEFAULT_STRING_LIMIT)
    , string_prefix_size_(DEFAULT_STRING_PREFIX_SIZE)
  {
    write_state_.push_back(UNINIT);
  }

  static const int32_t DEFAULT_STRING_LIMIT = 256;
  static const int32_t DEFAULT_STRING_PREFIX_SIZE = 16;

  void setStringSizeLimit(int32_t string_limit) {
    string_limit_ = string_limit;
  }

  void setStringPrefixSize(int32_t string_prefix_size) {
    string_prefix_size_ = string_prefix_size;
  }


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

  uint32_t writeMapBegin(const TType keyType,
                         const TType valType,
                         const uint32_t size);

  uint32_t writeMapEnd();

  uint32_t writeListBegin(const TType elemType,
                          const uint32_t size);

  uint32_t writeListEnd();

  uint32_t writeSetBegin(const TType elemType,
                         const uint32_t size);

  uint32_t writeSetEnd();

  uint32_t writeBool(const bool value);

  uint32_t writeByte(const int8_t byte);

  uint32_t writeI16(const int16_t i16);

  uint32_t writeI32(const int32_t i32);

  uint32_t writeI64(const int64_t i64);

  uint32_t writeDouble(const double dub);

  uint32_t writeString(const std::string& str);

  template <class StrType>
  uint32_t writeString(const StrType& str) {
    return writeString(std::string(str.data(), str.size()));
  }

  uint32_t writeBinary(const std::string& str);


 private:
  void indentUp();
  void indentDown();
  uint32_t writePlain(const std::string& str);
  uint32_t writeIndented(const std::string& str);
  uint32_t startItem();
  uint32_t endItem();
  uint32_t writeItem(const std::string& str);

  static std::string fieldTypeName(TType type);

  TTransport* trans_;

  int32_t string_limit_;
  int32_t string_prefix_size_;

  std::string indent_str_;
  static const int indent_inc = 2;

  std::vector<write_state_t> write_state_;
  std::vector<int> list_idx_;
};

/**
 * Constructs debug protocol handlers
 */
class TDebugProtocolFactory : public TProtocolFactory {
 public:
  TDebugProtocolFactory() {}
  virtual ~TDebugProtocolFactory() {}

  boost::shared_ptr<TProtocol> getProtocol(boost::shared_ptr<TTransport> trans) {
    return boost::shared_ptr<TProtocol>(new TDebugProtocol(trans));
  }

};

}}} // apache::thrift::protocol


// TODO(dreiss): Move (part of) ThriftDebugString into a .cpp file and remove this.
#include "thrift/lib/cpp/transport/TBufferTransports.h"

namespace apache { namespace thrift {

template <typename T>
struct ThriftTypeTraits {
  static const apache::thrift::protocol::TType fieldType_ =
    apache::thrift::protocol::TType(99);
};

template<>
struct ThriftTypeTraits<bool> {
  static const apache::thrift::protocol::TType fieldType_ =
    apache::thrift::protocol::T_BOOL;
};

template<>
struct ThriftTypeTraits<int8_t> {
  static const apache::thrift::protocol::TType fieldType_ =
    apache::thrift::protocol::T_I08;
};

template<>
struct ThriftTypeTraits<int16_t> {
  static const apache::thrift::protocol::TType fieldType_ =
    apache::thrift::protocol::T_I16;
};

template<>
struct ThriftTypeTraits<int32_t> {
  static const apache::thrift::protocol::TType fieldType_ =
    apache::thrift::protocol::T_I32;
};

template<>
struct ThriftTypeTraits<int64_t> {
  static const apache::thrift::protocol::TType fieldType_ =
    apache::thrift::protocol::T_I64;
};

template<>
struct ThriftTypeTraits<double> {
  static const apache::thrift::protocol::TType fieldType_ =
    apache::thrift::protocol::T_DOUBLE;
};

template<>
struct ThriftTypeTraits<std::string> {
  static const apache::thrift::protocol::TType fieldType_ =
    apache::thrift::protocol::T_STRING;
};

class TDebugProtocolEx : public apache::thrift::protocol::TDebugProtocol {
public:
  TDebugProtocolEx(
    boost::shared_ptr<apache::thrift::protocol::TTransport> trans
  )
      : TDebugProtocol(trans) {
  }

  template<typename T>
  void write(const T& t) {
    t.write(this);
  }

  template<typename T>
  void write(const std::vector<T>& c) {
    writeListBegin(ThriftTypeTraits<T>::fieldType_, c.size());
    typeof(c.begin()) it = c.begin();
    for (; it != c.end(); it++) {
      write(*it);
    }
    writeListEnd();
  }

  template<typename K, typename V>
  void write(const std::map<K, V>& c) {
    writeMapBegin(
      ThriftTypeTraits<K>::fieldType_,
      ThriftTypeTraits<V>::fieldType_,
      c.size()
    );
    typeof(c.begin()) it = c.begin();
    for (; it != c.end(); it++) {
      write(it->first);
      write(it->second);
    }
    writeMapEnd();
  }

  template<typename K, typename V>
  void write(const std::multimap<K, V>& c) {
    writeMapBegin(
      ThriftTypeTraits<K>::fieldType_,
      ThriftTypeTraits<V>::fieldType_,
      c.size()
    );
    typeof(c.begin()) it = c.begin();
    for (; it != c.end(); it++) {
      write(it->first);
      write(it->second);
    }
    writeMapEnd();
  }


  template<typename T>
  void write(const std::set<T>& c) {
    writeSetBegin(
      ThriftTypeTraits<T>::fieldType_,
      c.size()
    );
    typeof(c.begin()) it = c.begin();
    for (; it != c.end(); it++) {
      write(*it);
    }
    writeSetEnd();
  }

  void write(const bool value) {
    writeBool(value);
  }

  void write(const int8_t byte) {
    writeByte(byte);
  }

  void write(const int16_t i16) {
    writeI16(i16);
  }

  void write(const int32_t i32) {
    writeI32(i32);
  }

  void write(const int64_t i64) {
    writeI64(i64);
  }

  void write(const double dub) {
    writeDouble(dub);
  }

  void write(const std::string& str) {
    writeString(str);
  }

  template<typename K, typename V>
  void write(const std::pair<K, V>& p) {
    writeStructBegin("pair");
    writeFieldBegin("first", ThriftTypeTraits<K>::fieldType_, 1);
    write(p.first);
    writeFieldEnd();
    writeFieldBegin("second", ThriftTypeTraits<V>::fieldType_, 2);
    write(p.second);
    writeFieldEnd();
    writeStructEnd();
  }
};

template<typename T>
std::string ThriftDebugString(const T& ts) {
  using namespace apache::thrift::transport;
  using namespace apache::thrift::protocol;
  TMemoryBuffer* buffer = new TMemoryBuffer;
  boost::shared_ptr<TTransport> trans(buffer);
  TDebugProtocolEx protocol(trans);

  protocol.write(ts);

  uint8_t* buf;
  uint32_t size;
  buffer->getBuffer(&buf, &size);
  return std::string((char*)buf, (unsigned int)size);
}

}} // apache::thrift


#endif // #ifndef _THRIFT_PROTOCOL_TDEBUGPROTOCOL_H_


