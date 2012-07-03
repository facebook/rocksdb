/**
 * Copyright 2012 Facebook
 * @author Tudor Bosman (tudorb@fb.com)
 */

#ifndef THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_DECODER_H_
#define THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_DECODER_H_

#include "thrift/lib/cpp/protocol/neutronium/Utils.h"
#include "thrift/lib/cpp/protocol/neutronium/InternTable.h"
#include "thrift/lib/cpp/protocol/neutronium/Schema.h"
#include "thrift/lib/cpp/protocol/TProtocol.h"
#include "folly/FBString.h"
#include "folly/Range.h"
#include "folly/small_vector.h"
#include "folly/experimental/io/Cursor.h"

namespace apache {
namespace thrift {
namespace protocol {
namespace neutronium {

class Decoder {
 public:
  explicit Decoder(const Schema* schema, const InternTable* internTable,
                   folly::IOBuf* buf);
  void setRootType(int64_t type);

  void readStructBegin();
  void readStructEnd();

  void readFieldBegin(TType& fieldType, int16_t& fieldId);
  void readFieldEnd();

  void readMapBegin(TType& keyType, TType& valType, uint32_t& size);
  void readMapEnd();

  void readListBegin(TType& elemType, uint32_t& size);
  void readListEnd();

  void readSetBegin(TType& elemType, uint32_t& size);
  void readSetEnd();

  void readBool(bool& value);
  void readByte(int8_t& value);
  void readI16(int16_t& value);
  void readI32(int32_t& i32);
  void readI64(int64_t& i64);
  void readDouble(double& dub);

  template <typename StrType>
  void readString(StrType& str);

  void readBinary(std::string& str) {
    readString(str);
  }

  size_t bytesRead() const {
    return bytesRead_;
  }

 private:
  bool beginReadString();  // returns true if interned

  const Schema* schema_;
  const InternTable* internTable_;
  folly::io::RWPrivateCursor cursor_;
  int64_t rootType_;

  enum State {
    IN_STRUCT,
    IN_FIELD,
    IN_MAP_KEY,
    IN_MAP_VALUE,
    IN_LIST_VALUE,
    IN_SET_VALUE,
  };

  static const int64_t kVariableLength = -1;
  static const int64_t kTerminated = -2;

  struct TypeInfo {
    TypeInfo()
      : typeVal(reflection::TYPE_VOID),
        length(kVariableLength),
        dataType(nullptr),
        terminator('\0') { }
    TypeInfo(const Schema* schema, int64_t t);
    /* implicit */ TypeInfo(int64_t t);
    reflection::Type type() const {
      return reflection::getType(typeVal);
    }
    TType ttype() const {
      return toTType(type());
    }

    int64_t typeVal;
    int64_t length;
    const DataType* dataType;
    char terminator;
  };

  static const size_t kIntInline = 8;
  static const size_t kInt64Inline = 8;
  static const size_t kByteInline = 8;
  static const size_t kFixedInt16Inline = 8;
  static const size_t kFixedInt32Inline = 8;
  static const size_t kFixedInt64Inline = 8;
  static const size_t kBoolInline = 8;
  static const size_t kStringInline = 8;

  struct DecoderState {
    DecoderState(const Schema* schema, int64_t type,
                 const DataType* dataType, uint32_t size);
    DecoderState(DecoderState&&) = default;
    DecoderState& operator=(DecoderState&&) = default;

    enum FieldState {
      FS_START,
      FS_INT,
      FS_INT64,
      FS_BYTE,
      FS_BOOL,
      FS_STRICT_ENUM,
      FS_INTERNED_STRING,
      FS_STRING,
      FS_END
    };

    const DataType* dataType;
    State state;

    TypeInfo type;

    size_t bytesRead;

    struct TypeStateBase {
      TypeStateBase() : count(0), index(-1) { }
      size_t count;
      ssize_t index;
    };

    template <class T, size_t kInlineCount=8>
    struct TypeState : public TypeStateBase {
      folly::small_vector<T, kInlineCount> values;
    };

    TypeState<uint32_t> ints;     // int16_t, int32_t
    TypeState<uint64_t> int64s;   // int64_t, double
    TypeState<uint8_t> bytes;     // int8_t
    size_t boolStartBit;  // offset of first bit from bools in bools.values
    TypeState<uint8_t, byteCount(8)> bools;  // bool
    TypeState<uint32_t> strictEnums;
    size_t totalStrictEnumBits;
    TypeState<uint32_t> vars;      // variable-length
    TypeState<folly::StringPiece> internedStrings;
    TypeStateBase strings;  // non-interned strings and user-defined types

    // TODO(tudorb): Make Struct and List into a union, but as these types are
    // non-POD, the union would have a deleted copy/move constructor,
    // copy/move assignment operator, and destructor, which meanns that
    // DecoderState would have to have them user-defined in order to insert
    // DecoderState in containers, bleh.

    typedef
      folly::small_vector<std::pair<int16_t, reflection::Type>, 8>
      TagVec;

    typedef
      folly::small_vector<std::pair<int16_t, TypeInfo>, 8>
      FullTagVec;

    struct Struct {
      FieldState fieldState;
      int16_t tag;

      TagVec intTags;
      TagVec int64Tags;
      TagVec byteTags;
      TagVec boolTags;
      FullTagVec strictEnumTags;

      FullTagVec stringTags;
      FullTagVec internedStringTags;

      TagVec fixedInt16Tags;
      TagVec fixedInt32Tags;
      TagVec fixedInt64Tags;
    } str;
    struct List {
      uint32_t remaining;
      TypeInfo mapKeyType;
      TypeInfo valueType;
    } list;

    template <typename T>
    void setStateList(T& ts);

    template <typename TS, typename TV>
    bool setStateStruct(TS& ts, const TV& tv, FieldState nextState);

    bool nextField();
    void nextValue();
    void addType(TypeInfo& tinfo,
                 const StructField& field, int16_t tag, uint32_t count);

   private:
    void setLength();
    void nextList();
    bool nextStruct();
  };

  size_t bytesRead_;
  static const size_t kStackInline = 8;
  folly::small_vector<DecoderState, kStackInline> stack_;

  void readBoolsAndStrictEnums(size_t skipBits);
  void read();
  void push(reflection::Type expected, int64_t type, uint32_t size);
  void pop();
  DecoderState& top();
  uint32_t peekElementCount();
  int64_t nextType();
  std::pair<const uint8_t*, size_t> ensure(size_t n);
};


}  // namespace neutronium
}  // namespace protocol
}  // namespace thrift
}  // namespace apache

#define THRIFT_INCLUDE_DECODER_INL
#include "thrift/lib/cpp/protocol/neutronium/Decoder-inl.h"
#undef THRIFT_INCLUDE_DECODER_INL


#endif /* THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_DECODER_H_ */

