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
#ifndef THRIFT_UTIL_VARINTUTILS_H_
#define THRIFT_UTIL_VARINTUTILS_H_ 1

#include <stdint.h>

namespace apache { namespace thrift {

namespace util {

/**
 * Read an i16 from the wire as a varint. The MSB of each byte is set
 * if there is another byte to follow. This can read up to 3 bytes.
 */
uint32_t readVarint16(uint8_t const* ptr, int16_t* i16,
                      uint8_t const* boundary);

/**
 * Read an i32 from the wire as a varint. The MSB of each byte is set
 * if there is another byte to follow. This can read up to 5 bytes.
 */
uint32_t readVarint32(uint8_t const* ptr, int32_t* i32,
                      uint8_t const* boundary);

/**
 * Read an i64 from the wire as a proper varint. The MSB of each byte is set
 * if there is another byte to follow. This can read up to 10 bytes.
 * Caller is responsible for advancing ptr after call.
 */
uint32_t readVarint64(uint8_t const* ptr, int64_t* i64,
                      uint8_t const* boundary);

/**
 * Write an i32 as a varint. Results in 1-5 bytes on the wire.
 */
uint32_t writeVarint32(uint32_t n, uint8_t* pkt);

/**
 * Write an i16 as a varint. Results in 1-3 bytes on the wire.
 */
uint32_t writeVarint16(uint16_t n, uint8_t* pkt);

}}} // apache::thrift::util

#include "VarintUtils.tcc"

#endif // THRIFT_UTIL_VARINTUTILS_H_
