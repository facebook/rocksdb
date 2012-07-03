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

#include "folly/experimental/io/Cursor.h"

namespace apache { namespace thrift {

namespace util {

template <class T, class CursorT,
          typename std::enable_if<
            std::is_constructible<folly::io::Cursor, const CursorT&>::value,
            bool>::type = false>
T readVarint(CursorT& c) {
  T retVal = 0;
  uint8_t shift = 0;
  uint8_t rsize = 0;
  while (true) {
    uint8_t byte;
    c.pull(&byte, sizeof(byte));
    rsize++;
    retVal |= (uint64_t)(byte & 0x7f) << shift;
    shift += 7;
    if (!(byte & 0x80)) {
      return retVal;
    }
    if (rsize > sizeof(T)) {
      // Too big for return type
      throw std::out_of_range("invalid varint read");
    }
  }
}

template <class T>
void writeVarint(folly::io::RWPrivateCursor& c, T value) {
  while (true) {
    if ((value & ~0x7F) == 0) {
      c.write<uint8_t>((int8_t)value);
      break;
    } else {
      c.write<uint8_t>((int8_t)((value & 0x7F) | 0x80));
      value = (unsigned)value >> 7;
    }
  }
}

}}} // apache::thrift::util
