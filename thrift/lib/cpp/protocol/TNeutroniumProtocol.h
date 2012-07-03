/**
 * Copyright 2012 Facebook
 * @author Tudor Bosman (tudorb@fb.com)
 */

#ifndef THRIFT_LIB_CPP_PROTOCOL_TNEUTRONIUMPROTOCOL_H_
#define THRIFT_LIB_CPP_PROTOCOL_TNEUTRONIUMPROTOCOL_H_

#include "thrift/lib/cpp/protocol/TProtocol.h"
#include "thrift/lib/cpp/protocol/TVirtualProtocol.h"
#include "thrift/lib/cpp/protocol/neutronium/Encoder.h"
#include "thrift/lib/cpp/protocol/neutronium/Decoder.h"

namespace apache { namespace thrift { namespace protocol {

class TNeutroniumProtocol
  : public TVirtualProtocol<TNeutroniumProtocol> {

 public:
  TNeutroniumProtocol(const neutronium::Schema* schema,
                      neutronium::InternTable* internTable,
                      folly::IOBuf* buf)
    : TVirtualProtocol<TNeutroniumProtocol>(nullptr),
      enc_(schema, internTable, buf),
      dec_(schema, internTable, buf) {
  }

  void setRootType(int64_t rootType) {
    enc_.setRootType(rootType);
    dec_.setRootType(rootType);
  }

  uint32_t writeMessageBegin(const std::string& name,
                             const TMessageType messageType,
                             const int32_t seqid) {
    LOG(FATAL) << "Message encoding / decoding not implemented";
  }

  uint32_t writeMessageEnd() {
    LOG(FATAL) << "Message encoding / decoding not implemented";
  }

  uint32_t writeStructBegin(const char* name) {
    enc_.writeStructBegin(name);
    return 0;
  }

  uint32_t writeStructEnd() {
    enc_.writeStructEnd();
    return enc_.bytesWritten();
  }

  uint32_t writeFieldBegin(const char* name,
                           const TType fieldType,
                           const int16_t fieldId) {
    enc_.writeFieldBegin(name, fieldType, fieldId);
    return 0;
  }

  uint32_t writeFieldEnd() {
    enc_.writeFieldEnd();
    return 0;
  }

  uint32_t writeFieldStop() {
    enc_.writeFieldStop();
    return 0;
  }

  uint32_t writeMapBegin(const TType keyType,
                         const TType valType,
                         const uint32_t size) {
    enc_.writeMapBegin(keyType, valType, size);
    return 0;
  }

  uint32_t writeMapEnd() {
    enc_.writeMapEnd();
    return 0;
  }

  uint32_t writeListBegin(const TType elemType, const uint32_t size) {
    enc_.writeListBegin(elemType, size);
    return 0;
  }

  uint32_t writeListEnd() {
    enc_.writeListEnd();
    return 0;
  }

  uint32_t writeSetBegin(const TType elemType, const uint32_t size) {
    enc_.writeSetBegin(elemType, size);
    return 0;
  }

  uint32_t writeSetEnd() {
    enc_.writeSetEnd();
    return 0;
  }

  uint32_t writeBool(const bool value) {
    enc_.writeBool(value);
    return 0;
  }

  uint32_t writeByte(const int8_t byte) {
    enc_.writeByte(byte);
    return 0;
  }

  uint32_t writeI16(const int16_t i16) {
    enc_.writeI16(i16);
    return 0;
  }

  uint32_t writeI32(const int32_t i32) {
    enc_.writeI32(i32);
    return 0;
  }

  uint32_t writeI64(const int64_t i64) {
    enc_.writeI64(i64);
    return 0;
  }

  uint32_t writeDouble(const double dub) {
    enc_.writeDouble(dub);
    return 0;
  }

  template <typename StrType>
  uint32_t writeString(const StrType& str) {
    enc_.writeString(str);
    return 0;
  }

  uint32_t writeBinary(const std::string& str) {
    enc_.writeBinary(str);
    return 0;
  }


  /**
   * Reading functions
   */


  uint32_t readMessageBegin(std::string& name,
                            TMessageType& messageType,
                            int32_t& seqid) {
    LOG(FATAL) << "Message encoding / decoding not implemented";
  }

  uint32_t readMessageEnd() {
    LOG(FATAL) << "Message encoding / decoding not implemented";
  }

  uint32_t readStructBegin(std::string& name) {
    dec_.readStructBegin();
    return 0;
  }

  uint32_t readStructEnd() {
    dec_.readStructEnd();
    return dec_.bytesRead();
  }

  uint32_t readFieldBegin(std::string& name,
                          TType& fieldType,
                          int16_t& fieldId) {
    dec_.readFieldBegin(fieldType, fieldId);
    return 0;
  }

  uint32_t readFieldEnd() {
    dec_.readFieldEnd();
    return 0;
  }

  uint32_t readMapBegin(TType& keyType, TType& valType, uint32_t& size) {
    dec_.readMapBegin(keyType, valType, size);
    return 0;
  }

  uint32_t readMapEnd() {
    dec_.readMapEnd();
    return 0;
  }

  uint32_t readListBegin(TType& elemType, uint32_t& size) {
    dec_.readListBegin(elemType, size);
    return 0;
  }

  uint32_t readListEnd() {
    dec_.readListEnd();
    return 0;
  }

  uint32_t readSetBegin(TType& elemType, uint32_t& size) {
    dec_.readSetBegin(elemType, size);
    return 0;
  }

  uint32_t readSetEnd() {
    dec_.readSetEnd();
    return 0;
  }

  uint32_t readBool(bool& value) {
    dec_.readBool(value);
    return 0;
  }
  // Provide the default readBool() implementation for std::vector<bool>
  using TVirtualProtocol<TNeutroniumProtocol>::readBool;

  uint32_t readByte(int8_t& byte) {
    dec_.readByte(byte);
    return 0;
  }

  uint32_t readI16(int16_t& i16) {
    dec_.readI16(i16);
    return 0;
  }

  uint32_t readI32(int32_t& i32) {
    dec_.readI32(i32);
    return 0;
  }

  uint32_t readI64(int64_t& i64) {
    dec_.readI64(i64);
    return 0;
  }

  uint32_t readDouble(double& dub) {
    dec_.readDouble(dub);
    return 0;
  }

  template<typename StrType>
  uint32_t readString(StrType& str) {
    dec_.readString(str);
    return 0;
  }

  uint32_t readBinary(std::string& str) {
    dec_.readBinary(str);
    return 0;
  }

 private:
  neutronium::Encoder enc_;
  neutronium::Decoder dec_;
};

class Neutronium {
 public:
  explicit Neutronium(const neutronium::Schema* schema,
                      neutronium::InternTable* internTable,
                      folly::IOBuf* buf)
    : proto_(schema, internTable, buf) {
  }

  template <class T>
  uint32_t serialize(const T& obj) {
    proto_.setRootType(T::_reflection_id);
    return obj.write(&proto_);
  }

  template <class T>
  uint32_t deserialize(T& obj) {
    proto_.setRootType(T::_reflection_id);
    return obj.read(&proto_);
  }

 private:
  TNeutroniumProtocol proto_;
};


}}}  // apache::thrift::protocol

#endif /* THRIFT_LIB_CPP_PROTOCOL_TNEUTRONIUMPROTOCOL_H_ */

