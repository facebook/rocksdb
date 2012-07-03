/**
 * Copyright 2012 Facebook
 * @author Tudor Bosman (tudorb@fb.com)
 */

#ifndef THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_UTILS_H_
#define THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_UTILS_H_

#include <cstddef>
#include <cstdint>
#include <cassert>
#include <algorithm>

namespace apache {
namespace thrift {
namespace protocol {
namespace neutronium {

// Helper functions for using an array of uint8_t values as a bitset

// byte index
constexpr inline size_t byteIndex(size_t bit) {
  return bit / 8;
}

// offset within byte
constexpr inline size_t bitOffset(size_t bit) {
  return bit % 8;
}

// number of bytes needed to represent bitCount bits (round up)
constexpr inline size_t byteCount(size_t bitCount) {
  return bitCount / 8 + (bitCount % 8 != 0);
}

// set bit
inline void setBit(uint8_t* p, size_t bit) {
  p[byteIndex(bit)] |= (1 << bitOffset(bit));
}

inline void setBits(uint8_t* p, size_t bit, size_t count, uint32_t value) {
  assert(value < (1 << count));
  while (count) {
    size_t index = byteIndex(bit);
    size_t offset = bitOffset(bit);
    size_t countInThisByte = std::min(count, 8 - offset);
    // Extract "countInThisByte" bits from value
    uint8_t mask = (1 << countInThisByte) - 1;
    uint8_t v = value & mask;
    // Wipe out these bits, then set them to the new value
    p[index] = (p[index] & ~(mask << offset)) | (v << offset);

    value >>= countInThisByte;
    bit += countInThisByte;
    count -= countInThisByte;
  }
}

inline uint32_t getBits(uint8_t* p, size_t bit, size_t count) {
  uint32_t value = 0;
  size_t shift = 0;
  while (count) {
    size_t index = byteIndex(bit);
    size_t offset = bitOffset(bit);
    size_t countInThisByte = std::min(count, 8 - offset);
    uint8_t mask = (1 << countInThisByte) - 1;
    uint8_t v = (p[index] & (mask << offset)) >> offset;

    value |= (v << shift);
    shift += countInThisByte;
    bit += countInThisByte;
    count -= countInThisByte;
  }
  return value;
}

// clear bit
inline void clearBit(uint8_t* p, size_t bit) {
  p[byteIndex(bit)] &= ~(1 << bitOffset(bit));
}

// test bit
inline bool testBit(const uint8_t* p, size_t bit) {
  return p[byteIndex(bit)] & (1 << bitOffset(bit));
}

// Overloads for vector<uint8_t>, array<uint8_t>, or anything else that has
// a working operator[]
template <class T>
inline void setBit(T& p, size_t bit) {
  setBit(&p[0], bit);
}

template <class T>
inline void clearBit(T& p, size_t bit) {
  clearBit(&p[0], bit);
}

template <class T>
inline bool testBit(const T& p, size_t bit) {
  return testBit(&p[0], bit);
}

}  // namespace neutronium
}  // namespace protocol
}  // namespace thrift
}  // namespace apache

#endif /* THRIFT_LIB_CPP_PROTOCOL_NEUTRONIUM_UTILS_H_ */

