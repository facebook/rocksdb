/**
 * Copyright 2012 Facebook
 * @author Tudor Bosman (tudorb@fb.com)
 */

#ifndef THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_DECODER_INL_H_
#define THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_DECODER_INL_H_

#ifndef THRIFT_INCLUDE_DECODER_INL
#error This file may only be included from Decoder.h
#endif

#include "folly/GroupVarint.h"
#include "folly/Conv.h"

namespace apache {
namespace thrift {
namespace protocol {
namespace neutronium {

inline const char* CH(const uint8_t* p) {
  return reinterpret_cast<const char*>(p);
}
inline char* CH(uint8_t* p) {
  return reinterpret_cast<char*>(p);
}

template <class P>
inline auto CH(P p) -> std::pair<decltype(CH(p.first)), decltype(p.second)> {
  return std::make_pair(CH(p.first), p.second);
}

inline folly::StringPiece SP(std::pair<const uint8_t*, size_t> p) {
  return folly::StringPiece(CH(p.first), p.second);
}

inline void Decoder::setRootType(int64_t rootType) {
  CHECK(stack_.empty());
  rootType_ = rootType;
}

inline void Decoder::readStructBegin() {
  push(reflection::TYPE_STRUCT, nextType(), 0);
}

inline void Decoder::readStructEnd() {
  DCHECK_EQ(top().state, IN_STRUCT);
  // The last readFieldBegin returned T_STOP and so didn't change the state
  // to IN_FIELD
  pop();
}

inline int64_t Decoder::nextType() {
  int64_t type;
  if (!stack_.empty()) {
    auto& s = top();
    s.nextValue();
    type = s.type.typeVal;
  } else {
    type = rootType_;
  }
  return type;
}

inline void Decoder::readMapBegin(TType& keyType, TType& valType,
                                  uint32_t& size) {
  size = peekElementCount();
  push(reflection::TYPE_MAP, nextType(), size);
  keyType = top().list.mapKeyType.ttype();
  valType = top().list.valueType.ttype();
  size = top().list.remaining;
}

inline void Decoder::readMapEnd() {
  DCHECK_EQ(top().state, IN_MAP_VALUE);
  DCHECK_EQ(top().list.remaining, 0);
  pop();
}

inline void Decoder::readListBegin(TType& elemType, uint32_t& size) {
  size = peekElementCount();
  push(reflection::TYPE_LIST, nextType(), size);
  elemType = top().list.valueType.ttype();
  size = top().list.remaining;
}

inline void Decoder::readListEnd() {
  DCHECK_EQ(top().state, IN_LIST_VALUE);
  DCHECK_EQ(top().list.remaining, 0);
  pop();
}

inline void Decoder::readSetBegin(TType& elemType, uint32_t& size) {
  size = peekElementCount();
  push(reflection::TYPE_SET, nextType(), size);
  elemType = top().list.valueType.ttype();
  size = top().list.remaining;
}

inline void Decoder::readSetEnd() {
  DCHECK_EQ(top().state, IN_SET_VALUE);
  DCHECK_EQ(top().list.remaining, 0);
  pop();
}

inline void Decoder::push(reflection::Type rtype, int64_t type, uint32_t size) {
  if (rtype != reflection::getType(type)) {
    throw TProtocolException(folly::to<std::string>(
          "Invalid type ", reflection::getType(type), " expected ", rtype));
  }
  const DataType* dt = &(schema_->map().at(type));
  stack_.emplace_back(schema_, type, dt, size);
  read();
}

inline Decoder::TypeInfo::TypeInfo(const Schema* schema, int64_t t)
  : typeVal(t),
    length(kVariableLength),
    dataType(nullptr),
    terminator('\0') {
  if (!reflection::isBaseType(type())) {
    dataType = &(schema->map().at(t));
    length = dataType->fixedSize;
  }
}

inline Decoder::TypeInfo::TypeInfo(int64_t t)
  : typeVal(t),
    length(kVariableLength),
    dataType(nullptr),
    terminator('\0') {
  DCHECK(reflection::isBaseType(type()));
}

template <typename T>
inline void Decoder::DecoderState::setStateList(T& ts) {
  if (UNLIKELY(++ts.index == ts.count)) {
    throw TProtocolException("type mismatch");
  }
}

inline void Decoder::DecoderState::nextList() {
  switch (state) {
  case IN_MAP_KEY:
    state = IN_MAP_VALUE;
    type = list.valueType;
    break;  // don't advance to next
  case IN_MAP_VALUE:
    state = IN_MAP_KEY;
    type = list.mapKeyType;
    DCHECK_NE(list.remaining, 0);
    --list.remaining;
    break;
  case IN_LIST_VALUE:
  case IN_SET_VALUE:
    type = list.valueType;
    DCHECK_NE(list.remaining, 0);
    --list.remaining;
    break;
  default:
    LOG(FATAL) << "Invalid state " << state;
  }
  // Advance to next element

  switch (type.typeVal) {
  case reflection::TYPE_BOOL:
    setStateList(bools);
    break;
  case reflection::TYPE_BYTE:
    setStateList(bytes);
    break;
  case reflection::TYPE_I16:  // fallthrough
  case reflection::TYPE_I32:
    setStateList(ints);
    break;
  case reflection::TYPE_I64: // fallthrough
  case reflection::TYPE_DOUBLE:
    setStateList(int64s);
    break;
  case reflection::TYPE_STRING: // fallthrough
  default:
    setStateList(strings);
  }
}

template <typename TS, typename TV>
bool Decoder::DecoderState::setStateStruct(
    TS& ts,
    const TV& tv,
    FieldState nextState) {
  if (++ts.index == ts.count) {
    str.fieldState = nextState;
    return false;
  } else {
    auto& p = tv[ts.index];
    str.tag = p.first;
    type = p.second;
    return true;
  }
}

inline bool Decoder::DecoderState::nextStruct() {
  switch (str.fieldState) {
  case FS_START:
    str.fieldState = FS_INT;
    ints.index = -1;
    // fallthrough
  case FS_INT:
    if (setStateStruct(ints, str.intTags, FS_INT64)) {
      break;
    }
    // fallthrough
  case FS_INT64:
    if (setStateStruct(int64s, str.int64Tags, FS_BOOL)) {
      break;
    }
    // fallthrough
  case FS_BOOL:
    if (setStateStruct(bools, str.boolTags, FS_STRICT_ENUM)) {
      break;
    }
  case FS_STRICT_ENUM:
    if (setStateStruct(strictEnums, str.strictEnumTags, FS_BYTE)) {
      break;
    }
    // fallthrough
  case FS_BYTE:
    if (setStateStruct(bytes, str.byteTags, FS_INTERNED_STRING)) {
      break;
    }
    // fallthrough
  case FS_INTERNED_STRING:
    if (setStateStruct(internedStrings, str.internedStringTags, FS_STRING)) {
      break;
    }
  case FS_STRING:
    if (setStateStruct(strings, str.stringTags, FS_END)) {
      break;
    }
    // fallthrough
  case FS_END:
    return false;
  }

  return true;
}

inline bool Decoder::DecoderState::nextField() {
  DCHECK_EQ(state, IN_STRUCT);
  if (!nextStruct()) {
    return false;
  }
  state = IN_FIELD;
  setLength();
  return true;
}

inline void Decoder::DecoderState::nextValue() {
  DCHECK_NE(state, IN_STRUCT);
  if (state != IN_FIELD) {
    nextList();
    setLength();
  }
}

inline void Decoder::DecoderState::setLength() {
  if (type.type() == reflection::TYPE_STRING &&
      type.length == kVariableLength) {
    // Variable size, read length
    if (++vars.index == vars.count) {
      throw TProtocolException("too many vars");
    }
    type.length = vars.values[vars.index];
  }
}

inline uint32_t Decoder::peekElementCount() {
  uint32_t len;
  cursor_.gather(folly::GroupVarint32::maxSize(1));
  folly::GroupVarint32Decoder decoder(SP(cursor_.peek()));
  uint32_t count;
  if (!decoder.next(&count)) {
    throw TProtocolException("underflow");
  }
  return count;
}

inline void Decoder::readFieldBegin(TType& fieldType, int16_t& fieldId) {
  auto& s = top();
  if (s.nextField()) {
    fieldType = s.type.ttype();
    fieldId = s.str.tag;
  } else {
    fieldType = T_STOP;
    fieldId = 0;
  }
}

inline void Decoder::readFieldEnd() {
  auto& s = top();
  CHECK_EQ(s.state, IN_FIELD);
  s.state = IN_STRUCT;
}

inline void Decoder::readBool(bool& value) {
  auto& s = top();
  s.nextValue();
  if (UNLIKELY(s.type.typeVal != reflection::TYPE_BOOL)) {
    throw TProtocolException("invalid type");
  }
  value = testBit(s.bools.values, s.boolStartBit + s.bools.index);
}

inline void Decoder::readByte(int8_t& value) {
  auto& s = top();
  s.nextValue();
  if (UNLIKELY(s.type.typeVal != reflection::TYPE_BYTE)) {
    throw TProtocolException("invalid type");
  }
  value = s.bytes.values[s.bytes.index];
}

inline void Decoder::readI16(int16_t& value) {
  auto& s = top();
  s.nextValue();
  if (UNLIKELY(s.type.typeVal != reflection::TYPE_I16)) {
    throw TProtocolException("invalid type");
  }
  value = s.ints.values[s.ints.index];
}

inline void Decoder::readI32(int32_t& value) {
  auto& s = top();
  s.nextValue();
  // Strict enums only occur in structs
  // TODO(tudorb): Relax?
  if (s.state == IN_FIELD && s.str.fieldState == DecoderState::FS_STRICT_ENUM) {
    value = s.strictEnums.values[s.strictEnums.index];
  } else if (UNLIKELY(s.type.typeVal != reflection::TYPE_I32)) {
    throw TProtocolException("invalid type");
  } else {
    value = s.ints.values[s.ints.index];
  }
}

inline void Decoder::readI64(int64_t& value) {
  auto& s = top();
  s.nextValue();
  if (UNLIKELY(s.type.typeVal != reflection::TYPE_I64)) {
    throw TProtocolException("invalid type");
  }
  value = s.int64s.values[s.int64s.index];
}

inline void Decoder::readDouble(double& value) {
  auto& s = top();
  s.nextValue();
  if (UNLIKELY(s.type.typeVal != reflection::TYPE_DOUBLE)) {
    throw TProtocolException("invalid type");
  }
  int64_t i64 = s.int64s.values[s.int64s.index];
  value = bitwise_cast<double>(i64);
}

inline bool Decoder::beginReadString() {
  auto& s = top();
  s.nextValue();
  if (UNLIKELY(s.type.typeVal != reflection::TYPE_STRING)) {
    throw TProtocolException("invalid type");
  }
  // Interned strings only occur in structs,
  // TODO(tudorb): Relax?
  return (s.state == IN_FIELD &&
          s.str.fieldState == DecoderState::FS_INTERNED_STRING);
}

template <typename StrType>
inline void Decoder::readString(StrType& str) {
  if (beginReadString()) {
    // interned
    auto& s = top();
    auto sp = s.internedStrings.values[s.internedStrings.index];
    str.assign(sp.data(), sp.size());
  } else {
    // not interned
    str.clear();
    int64_t len = top().type.length;
    DCHECK(len >= 0 || len == kTerminated);
    if (len == kTerminated) {
      char terminator = top().type.terminator;
      const uint8_t* term = nullptr;
      while (!term) {
        auto p = cursor_.peek();
        if (p.second == 0) throw TLibraryException("eof");
        term = static_cast<const uint8_t*>(
            memchr(p.first, terminator, p.second));
        size_t n = term ? term - p.first : p.second;
        str.append(reinterpret_cast<const char*>(p.first), n);
        cursor_.skip(n);
        top().bytesRead += n;
      }
    } else {
      while (len) {
        auto p = cursor_.peek();
        if (p.second == 0) throw TLibraryException("eof");
        auto n = std::min(len, static_cast<int64_t>(p.second));
        str.append(reinterpret_cast<const char*>(p.first), n);
        cursor_.skip(n);
        top().bytesRead += n;
        len -= n;
      }
    }
  }
}

inline Decoder::DecoderState& Decoder::top() {
  DCHECK(!stack_.empty());
  return stack_.back();
}

inline void Decoder::pop() {
  DCHECK(!stack_.empty());
  size_t bytesRead = stack_.back().bytesRead;
  stack_.pop_back();
  if (stack_.empty()) {
    bytesRead_ = bytesRead;
  } else {
    top().bytesRead += bytesRead;
  }
}

}  // namespace neutronium
}  // namespace protocol
}  // namespace thrift
}  // namespace apache

#endif /* THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_DECODER_INL_H_ */

