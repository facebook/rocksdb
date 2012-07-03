/**
 * Copyright 2012 Facebook
 * @author Tudor Bosman (tudorb@fb.com)
 */

#ifndef THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_ENCODER_INL_H_
#define THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_ENCODER_INL_H_

#ifndef THRIFT_INCLUDE_ENCODER_INL
#error This file may only be included from Encoder.h
#endif

#include "folly/Conv.h"

namespace apache {
namespace thrift {
namespace protocol {
namespace neutronium {

inline void Encoder::writeFieldBegin(const char* name, TType fieldType,
                                     int16_t fieldId) {
  auto& s = top();
  DCHECK(s.state == IN_STRUCT);
  s.field = s.dataType->fields.at(fieldId);
  s.tag = fieldId;
  s.state = IN_FIELD;
}

inline void Encoder::writeFieldEnd() {
  auto& s = top();
  DCHECK(s.state == DONE_FIELD);
  s.tag = 0;
  s.field.clear();
  s.state = IN_STRUCT;
}

inline void Encoder::writeFieldStop() {
  auto& s = top();
  DCHECK(s.state == IN_STRUCT);
  flush();
}

inline bool Encoder::EncoderState::inDataState() const {
  return (state == IN_FIELD ||
          state == IN_MAP_KEY ||
          state == IN_MAP_VALUE ||
          state == IN_LIST_VALUE ||
          state == IN_SET_VALUE);
}

inline bool Encoder::EncoderState::inFlushableState() const {
  return (state == IN_STRUCT ||
          state == IN_MAP_KEY ||
          state == IN_LIST_VALUE ||
          state == IN_SET_VALUE);
}

inline void Encoder::EncoderState::dataWritten() {
  switch (state) {
  case IN_FIELD:
    state = DONE_FIELD;
    break;
  case IN_MAP_KEY:
    state = IN_MAP_VALUE;
    field.type = dataType->valueType;
    break;
  case IN_MAP_VALUE:
    state = IN_MAP_KEY;
    field.type = dataType->mapKeyType;
    break;
  case IN_LIST_VALUE:
    break;
  case IN_SET_VALUE:
    break;
  default:
    LOG(FATAL) << "Invalid state " << state;
  }
}

template <class C, class T>
inline size_t findIndex(const C& container, T val) {
  auto pos = container.find(val);
  if (pos == container.end()) throw std::out_of_range("not found");
  return pos - container.begin();
}

inline void Encoder::EncoderState::markFieldSet() {
  // only useful for structs
  if (state != IN_FIELD || field.isRequired) return;
  optionalSet[findIndex(dataType->optionalFields, tag)] = true;
}

template <typename... Args>
inline void Encoder::EncoderState::checkType() const {
  throw TLibraryException(folly::to<std::string>("Invalid type ", field.type));
}

template <typename... Args>
inline void Encoder::EncoderState::checkType(reflection::Type t,
                                             Args... tail) const {
  if (t == reflection::getType(field.type)) return;
  checkType(tail...);
}

template <class Vec>
inline void Encoder::EncoderState::appendToOutput(const Vec& vec) {
  size_t bytes = vec.size() * sizeof(typename Vec::value_type::second_type);
  for (auto& p : vec) {
    appender.writeBE(p.second);
  }
  bytesWritten += bytes;
}

inline void Encoder::writeMapBegin(TType keyType, TType valType,
                                   uint32_t size) {
  push(reflection::TYPE_MAP, topType(), size);
}

inline void Encoder::writeMapEnd() {
  DCHECK_EQ(top().state, IN_MAP_KEY);
  flush();
  pop();
}

inline void Encoder::writeListBegin(TType elemType, uint32_t size) {
  push(reflection::TYPE_LIST, topType(), size);
}

inline void Encoder::writeListEnd() {
  DCHECK_EQ(top().state, IN_LIST_VALUE);
  flush();
  pop();
}

inline void Encoder::writeSetBegin(TType elemType, uint32_t size) {
  push(reflection::TYPE_SET, topType(), size);
}

inline void Encoder::writeSetEnd() {
  DCHECK_EQ(top().state, IN_SET_VALUE);
  flush();
  pop();
}

inline void Encoder::writeStructBegin(const char* name) {
  push(reflection::TYPE_STRUCT, topType(), 0);
}

inline void Encoder::writeStructEnd() {
  DCHECK_EQ(top().state, FLUSHED);
  // writeFieldStop() called flush()
  pop();
}

inline void Encoder::push(reflection::Type rtype, int64_t type,
                          uint32_t size) {
  DCHECK(stack_.empty() || top().inDataState());
  if (reflection::getType(type) != rtype) {
    throw TLibraryException(folly::to<std::string>(
        "Invalid aggregate type ", reflection::getType(type),
        " expected ", rtype));
  }

  const DataType* dt = &(schema_->map().at(type));
  stack_.emplace_back(new EncoderState(type, dt, size));
}

inline void Encoder::pop() {
  auto& s = top();
  DCHECK_EQ(s.state, FLUSHED);
  int64_t size = s.dataType->fixedSize;
  DCHECK(size == -1 || size == s.buf->computeChainDataLength());
  size_t bytesWritten = s.bytesWritten;
  DCHECK_EQ(bytesWritten, s.buf->computeChainDataLength());
  auto buf = std::move(s.buf);
  stack_.pop_back();
  if (stack_.empty()) {
    outputBuf_->prependChain(std::move(buf));
    bytesWritten_ = bytesWritten;
  } else {
    writeData(std::move(buf));
    top().bytesWritten += bytesWritten;
  }
}

inline Encoder::EncoderState& Encoder::top() {
  DCHECK(!stack_.empty());
  DCHECK(stack_.back());
  return *stack_.back();
}

inline const Encoder::EncoderState& Encoder::top() const {
  DCHECK(!stack_.empty());
  DCHECK(stack_.back());
  return *stack_.back();
}

inline int64_t Encoder::topType() const {
  return (stack_.empty() ? rootType_ : top().field.type);
}

inline void Encoder::writeBool(bool v) {
  auto& s = top();
  DCHECK(s.inDataState());
  s.checkType(reflection::TYPE_BOOL);
  s.markFieldSet();
  s.bools.emplace_back(s.tag, v);
  s.dataWritten();
}

inline void Encoder::writeByte(int8_t v) {
  auto& s = top();
  DCHECK(s.inDataState());
  s.markFieldSet();
  s.checkType(reflection::TYPE_BYTE);
  s.bytes.emplace_back(s.tag, v);
  s.dataWritten();
}

inline void Encoder::writeI16(int16_t v) {
  auto& s = top();
  DCHECK(s.inDataState());
  s.checkType(reflection::TYPE_I16);
  s.markFieldSet();
  if (s.field.isFixed) {
    s.fixedInt16s.emplace_back(s.tag, v);
  } else {
    s.varInts.emplace_back(s.tag, v);
  }
  s.dataWritten();
}

inline void Encoder::writeI32(int32_t v) {
  auto& s = top();
  DCHECK(s.inDataState());
  s.checkType(reflection::TYPE_I32, reflection::TYPE_ENUM);
  s.markFieldSet();
  if (reflection::getType(s.field.type) == reflection::TYPE_ENUM &&
      s.field.isStrictEnum) {
    auto& dt = schema_->map().at(s.field.type);
    uint8_t nbits = dt.enumBits();
    uint32_t value = findIndex(dt.enumValues, v);
    s.strictEnums.push_back({s.tag, {nbits, value}});
    s.totalStrictEnumBits += nbits;
  } else {
    if (s.field.isFixed) {
      s.fixedInt32s.emplace_back(s.tag, v);
    } else {
      s.varInts.emplace_back(s.tag, v);
    }
  }
  s.dataWritten();
}

inline void Encoder::writeI64(int64_t v) {
  innerWriteI64(v, reflection::TYPE_I64);
}

inline void Encoder::writeDouble(double v) {
  innerWriteI64(bitwise_cast<int64_t>(v), reflection::TYPE_DOUBLE);
}

inline void Encoder::innerWriteI64(int64_t v, reflection::Type expected) {
  auto& s = top();
  DCHECK(s.inDataState());
  s.markFieldSet();
  s.checkType(expected);
  if (s.field.isFixed) {
    s.fixedInt64s.emplace_back(s.tag, v);
  } else {
    s.varInt64s.emplace_back(s.tag, v);
  }
  s.dataWritten();
}

namespace {
std::unique_ptr<folly::IOBuf> copyBufferToSize(
    const void* data, size_t dataSize, size_t outSize, char pad) {
  auto buf = folly::IOBuf::create(outSize);
  dataSize = std::min(dataSize, outSize);
  memcpy(buf->writableData(), data, dataSize);
  if (dataSize < outSize) {
    memset(buf->writableData() + dataSize, pad, outSize - dataSize);
  }
  buf->append(outSize);
  return buf;
}

std::unique_ptr<folly::IOBuf> copyBufferAndTerminate(
    const void* data, size_t dataSize, char terminator) {
  if (memchr(data, terminator, dataSize)) {
    throw TProtocolException("terminator found in terminated string");
  }
  // 1 byte of tailroom for the terminator
  auto buf = folly::IOBuf::copyBuffer(data, dataSize, 0, 1);
  buf->writableTail()[0] = terminator;
  buf->append(1);
  return buf;
}

}  // namespace


// TODO(tudorb): Zero-copy version
inline void Encoder::writeBytes(folly::StringPiece data) {
  auto& s = top();
  DCHECK(s.inDataState());
  s.checkType(reflection::TYPE_STRING);
  s.markFieldSet();
  if (s.field.isInterned) {
    CHECK(internTable_);
    s.varInternIds.emplace_back(s.tag, internTable_->add(data));
  } else if (s.field.isFixed) {
    s.strings.emplace_back(
        s.tag, copyBufferToSize(data.data(), data.size(),
                                s.field.fixedStringSize, s.field.pad));
    s.bytesWritten += s.field.fixedStringSize;
  } else if (s.field.isTerminated) {
    s.strings.emplace_back(
        s.tag, copyBufferAndTerminate(data.data(), data.size(),
                                      s.field.terminator));
    s.bytesWritten += data.size() + 1;
  } else {
    s.varLengths.emplace_back(s.tag, data.size());
    s.strings.emplace_back(
        s.tag, folly::IOBuf::copyBuffer(data.data(), data.size()));
    s.bytesWritten += data.size();
  }
  s.dataWritten();
}

inline void Encoder::writeData(std::unique_ptr<folly::IOBuf>&& data) {
  auto& s = top();
  DCHECK(s.inDataState());
  s.markFieldSet();
  s.strings.emplace_back(s.tag, std::move(data));
  s.dataWritten();
}

inline void Encoder::flush() {
  auto& s = top();
  DCHECK(s.inFlushableState());
  if (s.state == IN_STRUCT) {
    // TODO(tudorb): Check that all required fields were actually specified.
    flushStruct();
  }
  flushData(s.state == IN_STRUCT);
  s.state = FLUSHED;
}

}  // namespace neutronium
}  // namespace protocol
}  // namespace thrift
}  // namespace apache

#endif /* THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_ENCODER_INL_H_ */

