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

#ifndef THRIFT_PROTOCOL_TPROTOCOL_H_
#define THRIFT_PROTOCOL_TPROTOCOL_H_ 1

#include "thrift/lib/cpp/transport/TTransport.h"
#include "thrift/lib/cpp/protocol/TProtocolException.h"
#include "thrift/lib/cpp/util/BitwiseCast.h"
#include "thrift/lib/cpp/util/shared_ptr_util.h"

#include <boost/shared_ptr.hpp>

#include <netinet/in.h>
#include <sys/types.h>
#include <string>
#include <map>
#include <vector>

namespace apache { namespace thrift { namespace protocol {

using apache::thrift::transport::TTransport;

#ifdef THRIFT_HAVE_ENDIAN_H
#include <endian.h>
#endif

#ifndef __BYTE_ORDER
# if defined(BYTE_ORDER) && defined(LITTLE_ENDIAN) && defined(BIG_ENDIAN)
#  define __BYTE_ORDER BYTE_ORDER
#  define __LITTLE_ENDIAN LITTLE_ENDIAN
#  define __BIG_ENDIAN BIG_ENDIAN
# else
#  error "Cannot determine endianness"
# endif
#endif

#if __BYTE_ORDER == __BIG_ENDIAN
# if !defined(htonll) && !defined(ntohll)
#  define ntohll(n) (n)
#  define htonll(n) (n)
# endif /* !defined(htonll) && !defined(ntohll) */
# if defined(__GNUC__) && defined(__GLIBC__)
#  include <byteswap.h>
#  define htolell(n) bswap_64(n)
#  define letohll(n) bswap_64(n)
# else /* GNUC & GLIBC */
#  define bswap_64(n) \
      ( (((n) & 0xff00000000000000ull) >> 56) \
      | (((n) & 0x00ff000000000000ull) >> 40) \
      | (((n) & 0x0000ff0000000000ull) >> 24) \
      | (((n) & 0x000000ff00000000ull) >> 8)  \
      | (((n) & 0x00000000ff000000ull) << 8)  \
      | (((n) & 0x0000000000ff0000ull) << 24) \
      | (((n) & 0x000000000000ff00ull) << 40) \
      | (((n) & 0x00000000000000ffull) << 56) )
#  define htolell(n) bswap_64(n)
#  define letohll(n) bswap_64(n)
# endif /* GNUC & GLIBC */
#elif __BYTE_ORDER == __LITTLE_ENDIAN
#  define htolell(n) (n)
#  define letohll(n) (n)
# if !defined(htonll) && !defined(ntohll)
#  if defined(__GNUC__) && defined(__GLIBC__)
#   include <byteswap.h>
#   define ntohll(n) bswap_64(n)
#   define htonll(n) bswap_64(n)
#  else /* GNUC & GLIBC */
#   define ntohll(n) ( (((unsigned long long)ntohl(n)) << 32) + ntohl(n >> 32) )
#   define htonll(n) ( (((unsigned long long)htonl(n)) << 32) + htonl(n >> 32) )
#  endif /* GNUC & GLIBC */
# endif /* !defined(htonll) && !defined(ntohll) */
#else /* __BYTE_ORDER */
# error "Can't define htonll or ntohll!"
#endif

/**
 * Enumerated definition of the types that the Thrift protocol supports.
 * Take special note of the T_END type which is used specifically to mark
 * the end of a sequence of fields.
 */
enum TType {
  T_STOP       = 0,
  T_VOID       = 1,
  T_BOOL       = 2,
  T_BYTE       = 3,
  T_I08        = 3,
  T_I16        = 6,
  T_I32        = 8,
  T_U64        = 9,
  T_I64        = 10,
  T_DOUBLE     = 4,
  T_STRING     = 11,
  T_UTF7       = 11,
  T_STRUCT     = 12,
  T_MAP        = 13,
  T_SET        = 14,
  T_LIST       = 15,
  T_UTF8       = 16,
  T_UTF16      = 17
};

/**
 * Enumerated definition of the message types that the Thrift protocol
 * supports.
 */
enum TMessageType {
  T_CALL       = 1,
  T_REPLY      = 2,
  T_EXCEPTION  = 3,
  T_ONEWAY     = 4
};


/**
 * Helper template for implementing TProtocol::skip().
 *
 * Templatized to avoid having to make virtual function calls.
 */
template <class Protocol_>
uint32_t skip(Protocol_& prot, TType type) {
  switch (type) {
  case T_BOOL:
    {
      bool boolv;
      return prot.readBool(boolv);
    }
  case T_BYTE:
    {
      int8_t bytev = 0;
      return prot.readByte(bytev);
    }
  case T_I16:
    {
      int16_t i16;
      return prot.readI16(i16);
    }
  case T_I32:
    {
      int32_t i32;
      return prot.readI32(i32);
    }
  case T_I64:
    {
      int64_t i64;
      return prot.readI64(i64);
    }
  case T_DOUBLE:
    {
      double dub;
      return prot.readDouble(dub);
    }
  case T_STRING:
    {
      std::string str;
      return prot.readBinary(str);
    }
  case T_STRUCT:
    {
      uint32_t result = 0;
      std::string name;
      int16_t fid;
      TType ftype;
      result += prot.readStructBegin(name);
      while (true) {
        result += prot.readFieldBegin(name, ftype, fid);
        if (ftype == T_STOP) {
          break;
        }
        result += skip(prot, ftype);
        result += prot.readFieldEnd();
      }
      result += prot.readStructEnd();
      return result;
    }
  case T_MAP:
    {
      uint32_t result = 0;
      TType keyType;
      TType valType;
      uint32_t i, size;
      result += prot.readMapBegin(keyType, valType, size);
      for (i = 0; i < size; i++) {
        result += skip(prot, keyType);
        result += skip(prot, valType);
      }
      result += prot.readMapEnd();
      return result;
    }
  case T_SET:
    {
      uint32_t result = 0;
      TType elemType;
      uint32_t i, size;
      result += prot.readSetBegin(elemType, size);
      for (i = 0; i < size; i++) {
        result += skip(prot, elemType);
      }
      result += prot.readSetEnd();
      return result;
    }
  case T_LIST:
    {
      uint32_t result = 0;
      TType elemType;
      uint32_t i, size;
      result += prot.readListBegin(elemType, size);
      for (i = 0; i < size; i++) {
        result += skip(prot, elemType);
      }
      result += prot.readListEnd();
      return result;
    }
  default:
    return 0;
  }
}

/**
 * Abstract class for a thrift protocol driver. These are all the methods that
 * a protocol must implement. Essentially, there must be some way of reading
 * and writing all the base types, plus a mechanism for writing out structs
 * with indexed fields.
 *
 * TProtocol objects should not be shared across multiple encoding contexts,
 * as they may need to maintain internal state in some protocols (i.e. XML).
 * Note that is is acceptable for the TProtocol module to do its own internal
 * buffered reads/writes to the underlying TTransport where appropriate (i.e.
 * when parsing an input XML stream, reading should be batched rather than
 * looking ahead character by character for a close tag).
 *
 */
class TProtocol {
 public:
  virtual ~TProtocol() {}

  virtual void setVersion_virt(const int8_t version) = 0;

  void setVersion(const int8_t version) {
    T_VIRTUAL_CALL();
    return setVersion_virt(version);
  }

  /**
   * Writing functions.
   */

  virtual uint32_t writeMessageBegin_virt(const std::string& name,
                                          const TMessageType messageType,
                                          const int32_t seqid) = 0;

  virtual uint32_t writeMessageEnd_virt() = 0;


  virtual uint32_t writeStructBegin_virt(const char* name) = 0;

  virtual uint32_t writeStructEnd_virt() = 0;

  virtual uint32_t writeFieldBegin_virt(const char* name,
                                        const TType fieldType,
                                        const int16_t fieldId) = 0;

  virtual uint32_t writeFieldEnd_virt() = 0;

  virtual uint32_t writeFieldStop_virt() = 0;

  virtual uint32_t writeMapBegin_virt(const TType keyType,
                                      const TType valType,
                                      const uint32_t size) = 0;

  virtual uint32_t writeMapEnd_virt() = 0;

  virtual uint32_t writeListBegin_virt(const TType elemType,
                                       const uint32_t size) = 0;

  virtual uint32_t writeListEnd_virt() = 0;

  virtual uint32_t writeSetBegin_virt(const TType elemType,
                                      const uint32_t size) = 0;

  virtual uint32_t writeSetEnd_virt() = 0;

  virtual uint32_t writeBool_virt(const bool value) = 0;

  virtual uint32_t writeByte_virt(const int8_t byte) = 0;

  virtual uint32_t writeI16_virt(const int16_t i16) = 0;

  virtual uint32_t writeI32_virt(const int32_t i32) = 0;

  virtual uint32_t writeI64_virt(const int64_t i64) = 0;

  virtual uint32_t writeDouble_virt(const double dub) = 0;

  virtual uint32_t writeString_virt(const std::string& str) = 0;

  virtual uint32_t writeBinary_virt(const std::string& str) = 0;

  uint32_t writeMessageBegin(const std::string& name,
                             const TMessageType messageType,
                             const int32_t seqid) {
    T_VIRTUAL_CALL();
    return writeMessageBegin_virt(name, messageType, seqid);
  }

  uint32_t writeMessageEnd() {
    T_VIRTUAL_CALL();
    return writeMessageEnd_virt();
  }


  uint32_t writeStructBegin(const char* name) {
    T_VIRTUAL_CALL();
    return writeStructBegin_virt(name);
  }

  uint32_t writeStructEnd() {
    T_VIRTUAL_CALL();
    return writeStructEnd_virt();
  }

  uint32_t writeFieldBegin(const char* name,
                           const TType fieldType,
                           const int16_t fieldId) {
    T_VIRTUAL_CALL();
    return writeFieldBegin_virt(name, fieldType, fieldId);
  }

  uint32_t writeFieldEnd() {
    T_VIRTUAL_CALL();
    return writeFieldEnd_virt();
  }

  uint32_t writeFieldStop() {
    T_VIRTUAL_CALL();
    return writeFieldStop_virt();
  }

  uint32_t writeMapBegin(const TType keyType,
                         const TType valType,
                         const uint32_t size) {
    T_VIRTUAL_CALL();
    return writeMapBegin_virt(keyType, valType, size);
  }

  uint32_t writeMapEnd() {
    T_VIRTUAL_CALL();
    return writeMapEnd_virt();
  }

  uint32_t writeListBegin(const TType elemType, const uint32_t size) {
    T_VIRTUAL_CALL();
    return writeListBegin_virt(elemType, size);
  }

  uint32_t writeListEnd() {
    T_VIRTUAL_CALL();
    return writeListEnd_virt();
  }

  uint32_t writeSetBegin(const TType elemType, const uint32_t size) {
    T_VIRTUAL_CALL();
    return writeSetBegin_virt(elemType, size);
  }

  uint32_t writeSetEnd() {
    T_VIRTUAL_CALL();
    return writeSetEnd_virt();
  }

  uint32_t writeBool(const bool value) {
    T_VIRTUAL_CALL();
    return writeBool_virt(value);
  }

  uint32_t writeByte(const int8_t byte) {
    T_VIRTUAL_CALL();
    return writeByte_virt(byte);
  }

  uint32_t writeI16(const int16_t i16) {
    T_VIRTUAL_CALL();
    return writeI16_virt(i16);
  }

  uint32_t writeI32(const int32_t i32) {
    T_VIRTUAL_CALL();
    return writeI32_virt(i32);
  }

  uint32_t writeI64(const int64_t i64) {
    T_VIRTUAL_CALL();
    return writeI64_virt(i64);
  }

  uint32_t writeDouble(const double dub) {
    T_VIRTUAL_CALL();
    return writeDouble_virt(dub);
  }

  uint32_t writeString(const std::string& str) {
    T_VIRTUAL_CALL();
    return writeString_virt(str);
  }

  uint32_t writeBinary(const std::string& str) {
    T_VIRTUAL_CALL();
    return writeBinary_virt(str);
  }

  /**
   * Reading functions
   */

  virtual uint32_t readMessageBegin_virt(std::string& name,
                                         TMessageType& messageType,
                                         int32_t& seqid) = 0;

  virtual uint32_t readMessageEnd_virt() = 0;

  virtual uint32_t readStructBegin_virt(std::string& name) = 0;

  virtual uint32_t readStructEnd_virt() = 0;

  virtual uint32_t readFieldBegin_virt(std::string& name,
                                       TType& fieldType,
                                       int16_t& fieldId) = 0;

  virtual uint32_t readFieldEnd_virt() = 0;

  virtual uint32_t readMapBegin_virt(TType& keyType,
                                     TType& valType,
                                     uint32_t& size) = 0;

  virtual uint32_t readMapEnd_virt() = 0;

  virtual uint32_t readListBegin_virt(TType& elemType,
                                      uint32_t& size) = 0;

  virtual uint32_t readListEnd_virt() = 0;

  virtual uint32_t readSetBegin_virt(TType& elemType,
                                     uint32_t& size) = 0;

  virtual uint32_t readSetEnd_virt() = 0;

  virtual uint32_t readBool_virt(bool& value) = 0;

  virtual uint32_t readBool_virt(std::vector<bool>::reference value) = 0;

  virtual uint32_t readByte_virt(int8_t& byte) = 0;

  virtual uint32_t readI16_virt(int16_t& i16) = 0;

  virtual uint32_t readI32_virt(int32_t& i32) = 0;

  virtual uint32_t readI64_virt(int64_t& i64) = 0;

  virtual uint32_t readDouble_virt(double& dub) = 0;

  virtual uint32_t readString_virt(std::string& str) = 0;

  virtual uint32_t readBinary_virt(std::string& str) = 0;

  uint32_t readMessageBegin(std::string& name,
                            TMessageType& messageType,
                            int32_t& seqid) {
    T_VIRTUAL_CALL();
    return readMessageBegin_virt(name, messageType, seqid);
  }

  uint32_t readMessageEnd() {
    T_VIRTUAL_CALL();
    return readMessageEnd_virt();
  }

  uint32_t readStructBegin(std::string& name) {
    T_VIRTUAL_CALL();
    return readStructBegin_virt(name);
  }

  uint32_t readStructEnd() {
    T_VIRTUAL_CALL();
    return readStructEnd_virt();
  }

  uint32_t readFieldBegin(std::string& name,
                          TType& fieldType,
                          int16_t& fieldId) {
    T_VIRTUAL_CALL();
    return readFieldBegin_virt(name, fieldType, fieldId);
  }

  uint32_t readFieldEnd() {
    T_VIRTUAL_CALL();
    return readFieldEnd_virt();
  }

  uint32_t readMapBegin(TType& keyType, TType& valType, uint32_t& size) {
    T_VIRTUAL_CALL();
    return readMapBegin_virt(keyType, valType, size);
  }

  uint32_t readMapEnd() {
    T_VIRTUAL_CALL();
    return readMapEnd_virt();
  }

  uint32_t readListBegin(TType& elemType, uint32_t& size) {
    T_VIRTUAL_CALL();
    return readListBegin_virt(elemType, size);
  }

  uint32_t readListEnd() {
    T_VIRTUAL_CALL();
    return readListEnd_virt();
  }

  uint32_t readSetBegin(TType& elemType, uint32_t& size) {
    T_VIRTUAL_CALL();
    return readSetBegin_virt(elemType, size);
  }

  uint32_t readSetEnd() {
    T_VIRTUAL_CALL();
    return readSetEnd_virt();
  }

  uint32_t readBool(bool& value) {
    T_VIRTUAL_CALL();
    return readBool_virt(value);
  }

  uint32_t readByte(int8_t& byte) {
    T_VIRTUAL_CALL();
    return readByte_virt(byte);
  }

  uint32_t readI16(int16_t& i16) {
    T_VIRTUAL_CALL();
    return readI16_virt(i16);
  }

  uint32_t readI32(int32_t& i32) {
    T_VIRTUAL_CALL();
    return readI32_virt(i32);
  }

  uint32_t readI64(int64_t& i64) {
    T_VIRTUAL_CALL();
    return readI64_virt(i64);
  }

  uint32_t readDouble(double& dub) {
    T_VIRTUAL_CALL();
    return readDouble_virt(dub);
  }

  uint32_t readString(std::string& str) {
    T_VIRTUAL_CALL();
    return readString_virt(str);
  }

  uint32_t readBinary(std::string& str) {
    T_VIRTUAL_CALL();
    return readBinary_virt(str);
  }

  /*
   * std::vector is specialized for bool, and its elements are individual bits
   * rather than bools.   We need to define a different version of readBool()
   * to work with std::vector<bool>.
   */
  uint32_t readBool(std::vector<bool>::reference value) {
    T_VIRTUAL_CALL();
    return readBool_virt(value);
  }

  /**
   * Method to arbitrarily skip over data.
   */
  uint32_t skip(TType type) {
    T_VIRTUAL_CALL();
    return skip_virt(type);
  }
  virtual uint32_t skip_virt(TType type) {
    return ::apache::thrift::protocol::skip(*this, type);
  }

  inline boost::shared_ptr<TTransport> getTransport() {
    return ptrans_;
  }

  // TODO: remove these two calls, they are for backwards
  // compatibility
  inline boost::shared_ptr<TTransport> getInputTransport() {
    return ptrans_;
  }
  inline boost::shared_ptr<TTransport> getOutputTransport() {
    return ptrans_;
  }

 protected:
  explicit TProtocol(boost::shared_ptr<TTransport> ptrans):
    ptrans_(ptrans) {
  }

  /**
   * Construct a TProtocol using a raw TTransport pointer.
   *
   * It is the callers responsibility to ensure that the TTransport remains
   * valid for the lifetime of the TProtocol object.
   */
  explicit TProtocol(TTransport* ptrans):
    ptrans_(ptrans, NoopPtrDestructor<TTransport>()) {
  }

  boost::shared_ptr<TTransport> ptrans_;

 private:
  TProtocol() {}
};

/**
 * Constructs protocol objects given transports.
 */
class TProtocolFactory {
 public:
  TProtocolFactory() {}

  virtual ~TProtocolFactory() {}

  virtual boost::shared_ptr<TProtocol> getProtocol(boost::shared_ptr<TTransport> trans) = 0;
};

/**
 * Constructs both input and output protocol objects with a given pair of
 * input and output transports.
 *
 * TProtocolPair.first = Input Protocol
 * TProtocolPair.second = Output Protocol
 */
typedef std::pair<boost::shared_ptr<TProtocol>,
                  boost::shared_ptr<TProtocol> > TProtocolPair;

class TDuplexProtocolFactory {
 public:
  TDuplexProtocolFactory() {}

  virtual ~TDuplexProtocolFactory() {}

  virtual TProtocolPair getProtocol(transport::TTransportPair transports) = 0;

  virtual boost::shared_ptr<TProtocolFactory> getInputProtocolFactory() {
    return boost::shared_ptr<TProtocolFactory>();
  }

  virtual boost::shared_ptr<TProtocolFactory> getOutputProtocolFactory() {
    return boost::shared_ptr<TProtocolFactory>();
  }
};

/**
 * Adapts a TProtocolFactory to a TDuplexProtocolFactory that returns
 * a new protocol object for both input and output
 */
template <class Factory_>
class TSingleProtocolFactory : public TDuplexProtocolFactory {
 public:
  TSingleProtocolFactory() {
    factory_.reset(new Factory_());
  }

  explicit TSingleProtocolFactory(boost::shared_ptr<Factory_> factory) :
      factory_(factory) {}

  virtual TProtocolPair getProtocol(transport::TTransportPair transports) {
    return std::make_pair(factory_->getProtocol(transports.first),
                          factory_->getProtocol(transports.second));
  }

  virtual boost::shared_ptr<TProtocolFactory> getInputProtocolFactory() {
    return factory_;
  }

  virtual boost::shared_ptr<TProtocolFactory> getOutputProtocolFactory() {
    return factory_;
  }

 private:

  boost::shared_ptr<Factory_> factory_;
};

/**
 * Use TDualProtocolFactory to construct input and output protocols from
 * different factories.
 */
class TDualProtocolFactory : public TDuplexProtocolFactory {
 public:
  TDualProtocolFactory(
    boost::shared_ptr<TProtocolFactory> inputFactory,
    boost::shared_ptr<TProtocolFactory> outputFactory) :
      inputFactory_(inputFactory),
      outputFactory_(outputFactory) {}

  virtual TProtocolPair getProtocol(transport::TTransportPair transports) {
    return std::make_pair(inputFactory_->getProtocol(transports.first),
                          outputFactory_->getProtocol(transports.second));
  }

  virtual boost::shared_ptr<TProtocolFactory> getInputProtocolFactory() {
    return inputFactory_;
  }

  virtual boost::shared_ptr<TProtocolFactory> getOutputProtocolFactory() {
    return outputFactory_;
  }

 private:

  boost::shared_ptr<TProtocolFactory> inputFactory_;
  boost::shared_ptr<TProtocolFactory> outputFactory_;
};

/**
 * Dummy protocol class.
 *
 * This class does nothing, and should never be instantiated.
 * It is used only by the generator code.
 */
class TDummyProtocol : public TProtocol {
};

}}} // apache::thrift::protocol

#endif // #define _THRIFT_PROTOCOL_TPROTOCOL_H_ 1
