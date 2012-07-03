/**
 * Copyright 2012 Facebook
 * @author Tudor Bosman (tudorb@fb.com)
 */

#ifndef THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_ENCODER_H_
#define THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_ENCODER_H_

#include "thrift/lib/cpp/protocol/neutronium/Utils.h"
#include "thrift/lib/cpp/protocol/neutronium/Schema.h"
#include "thrift/lib/cpp/protocol/neutronium/InternTable.h"
#include "thrift/lib/cpp/protocol/TProtocol.h"
#include "folly/FBString.h"
#include "folly/Range.h"
#include "folly/experimental/io/IOBuf.h"
#include "folly/experimental/io/Cursor.h"

namespace apache {
namespace thrift {
namespace protocol {
namespace neutronium {

class Encoder {
 public:
  Encoder(const Schema* schema, InternTable* internTable, folly::IOBuf* buf);
  void setRootType(int64_t rootType);

  // Similar interface to the writing part of TProtocol
  void writeStructBegin(const char* name);
  void writeStructEnd();

  void writeFieldBegin(const char* name, TType fieldType, int16_t fieldId);
  void writeFieldEnd();
  void writeFieldStop();

  void writeMapBegin(TType keyType, TType valType, uint32_t size);
  void writeMapEnd();
  void writeListBegin(TType elemType, uint32_t size);
  void writeListEnd();
  void writeSetBegin(TType elemType, uint32_t size);
  void writeSetEnd();

  void writeBool(bool value);
  void writeByte(int8_t byte);
  void writeI16(int16_t i16);
  void writeI32(int32_t i32);
  void writeI64(int64_t i64);
  void writeDouble(double dub);

  void writeBinary(const std::string& str) {
    writeString(str);
  }
  template <typename StrType>
  void writeString(const StrType& str) {
    writeBytes(str);
  }

  /**
   * Return number of bytes written.  Non-zero only at root level, after
   * the final writeStructEnd().
   */
  size_t bytesWritten() const {
    return bytesWritten_;
  }

 private:
  void innerWriteI64(int64_t i64, reflection::Type expected);

  void writeData(std::unique_ptr<folly::IOBuf>&& data);
  void markFieldSet();

  void writeBytes(folly::StringPiece data);

  int32_t intern(folly::StringPiece data);

  const Schema* schema_;
  int64_t rootType_;
  size_t bytesWritten_;

  enum State {
    IN_STRUCT,
    IN_FIELD,
    DONE_FIELD,
    IN_MAP_KEY,
    IN_MAP_VALUE,
    IN_LIST_VALUE,
    IN_SET_VALUE,
    FLUSHED,
  };

  struct EncoderState {
    EncoderState(int64_t type, const DataType* dt, uint32_t size);

    const DataType* dataType;
    State state;
    // TODO(tudorb): Check type_
    int16_t tag;
    StructField field;
    std::unique_ptr<folly::IOBuf> buf;
    folly::io::Appender appender;

    std::vector<std::pair<int16_t, bool>> bools;    // bool
    struct StrictEnum {
      uint8_t bits;
      uint32_t value;
    };
    // strict (bit-field) enums
    std::vector<std::pair<int16_t, StrictEnum>> strictEnums;
    size_t totalStrictEnumBits;
    std::vector<std::pair<int16_t, int8_t>> bytes;  // byte

    // Integer fields that were requested to be represented as fixed-length
    std::vector<std::pair<int16_t, int16_t>> fixedInt16s;  // i16
    std::vector<std::pair<int16_t, int32_t>> fixedInt32s;  // i32
    std::vector<std::pair<int16_t, int64_t>> fixedInt64s;  // i64, double

    // Integer fields that are represented as variable-length (GroupVarint)
    std::vector<std::pair<int16_t, int32_t>> varInts;      // i16, i32
    std::vector<std::pair<int16_t, int64_t>> varInt64s;    // i64, double

    // Lengths of strings of unknown size, represented as GroupVarint
    std::vector<std::pair<int16_t, uint32_t>> varLengths;

    // Ids of interned strings, represented as GroupVarint
    // (see InternTable.h)
    std::vector<std::pair<int16_t, uint32_t>> varInternIds;

    // Strings AND child elements (structs, lists, sets, maps)
    // Note that only strings can be of unknown size (and thus use up one
    // entry in varLengths, above); we always fully decode children and so
    // we determine their size that way
    std::vector<std::pair<int16_t, std::unique_ptr<folly::IOBuf>>> strings;

    // Bitset (see Utils.h) of which optional fields are set; the index is
    // the index in DataType::optionalFields.
    std::vector<bool> optionalSet;

    size_t bytesWritten;

    bool inDataState() const;
    void dataWritten();

    bool inFlushableState() const;
    void markFieldSet();

    template <typename... Args> void checkType() const;
    template <typename... Args> void checkType(reflection::Type t,
                                               Args... tail) const;

    template <class Vec>
    void appendToOutput(const Vec& vec);
  };

  std::vector<std::unique_ptr<EncoderState>> stack_;
  void push(reflection::Type expected, int64_t type, uint32_t size);
  void pop();
  int64_t topType() const;
  EncoderState& top();
  const EncoderState& top() const;

  void flush();
  void flushBitValues();
  void flushStruct();
  void flushData(bool isStruct);

  InternTable* internTable_;
  folly::IOBuf* outputBuf_;
};

}  // namespace neutronium
}  // namespace protocol
}  // namespace thrift
}  // namespace apache


#define THRIFT_INCLUDE_ENCODER_INL
#include "thrift/lib/cpp/protocol/neutronium/Encoder-inl.h"
#undef THRIFT_INCLUDE_ENCODER_INL

#endif /* THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_ENCODER_H_ */
