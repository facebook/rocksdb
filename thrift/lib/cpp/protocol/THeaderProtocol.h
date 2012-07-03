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

#ifndef THRIFT_PROTOCOL_THEADERPROTOCOL_H_
#define THRIFT_PROTOCOL_THEADERPROTOCOL_H_ 1

#include "thrift/lib/cpp/protocol/TProtocol.h"
#include "thrift/lib/cpp/protocol/TProtocolTypes.h"
#include "thrift/lib/cpp/protocol/TVirtualProtocol.h"
#include "thrift/lib/cpp/transport/THeaderTransport.h"
#include "thrift/lib/cpp/util/shared_ptr_util.h"

#include <boost/shared_ptr.hpp>

#include <bitset>

using apache::thrift::transport::THeaderTransport;
using apache::thrift::transport::TTransportPair;

namespace apache { namespace thrift { namespace protocol {

/**
 * The header protocol for thrift. Reads unframed, framed, header format,
 * and http
 *
 */
class THeaderProtocol
  : public TVirtualProtocol<THeaderProtocol> {
 public:
  explicit THeaderProtocol(const boost::shared_ptr<TTransport>& trans,
                           std::bitset<CLIENT_TYPES_LEN>* clientTypes = NULL,
                           uint16_t protoId = T_COMPACT_PROTOCOL) :
      TVirtualProtocol<THeaderProtocol>(getTransportWrapper(trans,
                                                            clientTypes))
      , trans_(boost::dynamic_pointer_cast<THeaderTransport, TTransport>(
                   this->getTransport()))
      , protoId_(protoId)
  {
    trans_->setProtocolId(protoId);
    resetProtocol();
  }

  THeaderProtocol(const boost::shared_ptr<TTransport>& inTrans,
                  const boost::shared_ptr<TTransport>& outTrans,
                  std::bitset<CLIENT_TYPES_LEN>* clientTypes = NULL,
                  uint16_t protoId = T_COMPACT_PROTOCOL) :
      TVirtualProtocol<THeaderProtocol>(getInOutTransportWrapper(inTrans,
                                                                 outTrans,
                                                                 clientTypes))
      , trans_(boost::dynamic_pointer_cast<THeaderTransport, TTransport>(
                 this->getTransport()))
      , protoId_(protoId)
  {
    trans_->setProtocolId(protoId);
    resetProtocol();
  }

  /**
   * Construct a THeaderProtocol using a raw pointer to the transport.
   *
   * The caller is responsible for ensuring that the transport remains valid
   * for the lifetime of the protocol.
   */
  THeaderProtocol(TTransport* trans,
                  std::bitset<CLIENT_TYPES_LEN>* clientTypes,
                  uint16_t protoId = T_COMPACT_PROTOCOL) :
      TVirtualProtocol<THeaderProtocol>(
          getTransportWrapper(
              boost::shared_ptr<TTransport>(trans,
                                            NoopPtrDestructor<TTransport>()),
              clientTypes))
      , trans_(boost::dynamic_pointer_cast<THeaderTransport, TTransport>(
                 this->getTransport()))
      , protoId_(protoId)
  {
    trans_->setProtocolId(protoId);
    resetProtocol();
  }

  ~THeaderProtocol() {}

  /**
   * Functions to work with headers by calling into THeaderTransport
   */
  void setProtocolId(uint16_t protoId) {
    trans_->setProtocolId(protoId);
    resetProtocol();
  }

  void resetProtocol();

  typedef THeaderTransport::StringToStringMap StringToStringMap;

  // these work with write headers
  void setHeader(const std::string& key, const std::string& value) {
    trans_->setHeader(key, value);
  }

  void setPersistentHeader(const std::string& key, const std::string& value) {
    trans_->setPersistentHeader(key, value);
  }

  void clearHeaders() {
    trans_->clearHeaders();
  }

  void clearPersistentHeaders() {
    trans_->clearPersistentHeaders();
  }

  StringToStringMap& getWriteHeaders() {
    return trans_->getWriteHeaders();
  }

  StringToStringMap& getPersistentWriteHeaders() {
    return trans_->getPersistentWriteHeaders();
  }

  // these work with read headers
  const StringToStringMap& getHeaders() const {
    return trans_->getHeaders();
  }

  void setTransform(uint16_t trans) {
    trans_->setTransform(trans);
  }

  std::string getPeerIdentity() const {
    return trans_->getPeerIdentity();
  }
  void setIdentity(const std::string& identity) {
    trans_->setIdentity(identity);
  }

  void setHmac(THeaderTransport::MacCallback macCb,
               THeaderTransport::VerifyMacCallback verifyCb) {
    trans_->setHmac(macCb, verifyCb);
  }

  /**
   * Writing functions.
   */

  /*ol*/ uint32_t writeMessageBegin(const std::string& name,
                                    const TMessageType messageType,
                                    const int32_t seqId);

  /*ol*/ uint32_t writeMessageEnd();


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

  uint32_t writeListBegin(const TType elemType, const uint32_t size);

  uint32_t writeListEnd();

  uint32_t writeSetBegin(const TType elemType, const uint32_t size);

  uint32_t writeSetEnd();

  uint32_t writeBool(const bool value);

  uint32_t writeByte(const int8_t byte);

  uint32_t writeI16(const int16_t i16);

  uint32_t writeI32(const int32_t i32);

  uint32_t writeI64(const int64_t i64);

  uint32_t writeDouble(const double dub);

  uint32_t writeString(const std::string& str);

  uint32_t writeBinary(const std::string& str);

  /**
   * Reading functions
   */


  /*ol*/ uint32_t readMessageBegin(std::string& name,
                                   TMessageType& messageType,
                                   int32_t& seqId);

  /*ol*/ uint32_t readMessageEnd();

  uint32_t readStructBegin(std::string& name);

  uint32_t readStructEnd();

  uint32_t readFieldBegin(std::string& name,
                          TType& fieldType,
                          int16_t& fieldId);

  uint32_t readFieldEnd();

  uint32_t readMapBegin(TType& keyType,
                        TType& valType,
                        uint32_t& size);

  uint32_t readMapEnd();

  uint32_t readListBegin(TType& elemType, uint32_t& size);

  uint32_t readListEnd();

  uint32_t readSetBegin(TType& elemType, uint32_t& size);

  uint32_t readSetEnd();

  uint32_t readBool(bool& value);
  // Provide the default readBool() implementation for std::vector<bool>
  using TVirtualProtocol< THeaderProtocol >::readBool;

  uint32_t readByte(int8_t& byte);

  uint32_t readI16(int16_t& i16);

  uint32_t readI32(int32_t& i32);

  uint32_t readI64(int64_t& i64);

  uint32_t readDouble(double& dub);

  uint32_t readString(std::string& str);

  uint32_t readBinary(std::string& binary);

 protected:
  template<typename StrType>
  uint32_t readStringBody(StrType& str, int32_t sz);

  boost::shared_ptr<TTransport> getTransportWrapper(
    const boost::shared_ptr<TTransport>& trans,
    std::bitset<CLIENT_TYPES_LEN>* clientTypes) {
    if (dynamic_cast<THeaderTransport*>(trans.get()) != NULL) {
      return trans;
    } else {
      return boost::shared_ptr<THeaderTransport>(
        new THeaderTransport(trans, clientTypes));
    }
  }

  boost::shared_ptr<TTransport> getInOutTransportWrapper(
    const boost::shared_ptr<TTransport>& inTrans,
    const boost::shared_ptr<TTransport>& outTrans,
    std::bitset<CLIENT_TYPES_LEN>* clientTypes) {
    assert(dynamic_cast<THeaderTransport*>(inTrans.get()) == NULL
        && dynamic_cast<THeaderTransport*>(outTrans.get()) == NULL);

    return boost::shared_ptr<THeaderTransport>(
      new THeaderTransport(inTrans, outTrans, clientTypes)
    );
  }

  boost::shared_ptr<THeaderTransport> trans_;

  boost::shared_ptr<TProtocol> proto_;
  uint32_t protoId_;
};

/**
 * Constructs header protocol handlers
 */
class THeaderProtocolFactory : public TDuplexProtocolFactory {
 public:
  explicit THeaderProtocolFactory(uint16_t protoId = T_COMPACT_PROTOCOL,
                                  bool disableIdentity = false) {
    protoId_ = protoId;
    setIdentity_ = disableIdentity;
  }

  virtual ~THeaderProtocolFactory() {}

  void setClientTypes(std::bitset<CLIENT_TYPES_LEN>& clientTypes) {
    for (int i = 0; i < CLIENT_TYPES_LEN; i++) {
      this->clientTypes[i] = clientTypes[i];
    }
  }

  void setIdentity(const std::string& identity) {
    identity_ = identity;
    setIdentity_ = true;
  }

  void setTransform(uint16_t trans) {
    trans_.push_back(trans);
  }

  virtual TProtocolPair getProtocol(
      boost::shared_ptr<transport::TTransport> trans) {
    THeaderProtocol* prot = new THeaderProtocol(trans, &clientTypes, protoId_);

    if(setIdentity_) {
      prot->setIdentity(identity_);
    }

    for (auto& t : trans_) {
      prot->setTransform(t);
    }

    boost::shared_ptr<TProtocol> pprot(prot);
    return TProtocolPair(pprot, pprot);
  }

  virtual TProtocolPair getProtocol(TTransportPair transports) {
    THeaderProtocol* prot = new THeaderProtocol(transports.first,
                                          transports.second,
                                          &clientTypes,
                                          protoId_);

    if(setIdentity_) {
      prot->setIdentity(identity_);
    }

    for (auto& t : trans_) {
      prot->setTransform(t);
    }

    boost::shared_ptr<TProtocol> pprot(prot);
    return TProtocolPair(pprot, pprot);
  }

  // No implementation of getInputProtocolFactory/getOutputProtocolFactory
  // Using base class implementation which return NULL.

 private:
  std::bitset<CLIENT_TYPES_LEN> clientTypes;
  uint16_t protoId_;
  bool setIdentity_;
  std::vector<uint16_t> trans_;
  std::string identity_;
};

}}} // apache::thrift::protocol

#endif // #ifndef THRIFT_PROTOCOL_THEADERPROTOCOL_H_
