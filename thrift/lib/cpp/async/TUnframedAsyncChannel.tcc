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
#ifndef THRIFT_ASYNC_TUNFRAMEDASYNCCHANNEL_TCC_
#define THRIFT_ASYNC_TUNFRAMEDASYNCCHANNEL_TCC_ 1

#include "thrift/lib/cpp/async/TUnframedAsyncChannel.h"

#include "thrift/lib/cpp/transport/TBufferTransports.h"

namespace {
const uint32_t kInitialBufferSize = 4096;
}

namespace apache { namespace thrift { namespace async { namespace detail {

template<typename ProtocolTraits_>
TUnframedACReadState<ProtocolTraits_>::TUnframedACReadState()
  : maxMessageSize_(0x7fffffff)
  , memBuffer_(kInitialBufferSize)
  , callbackBuffer_(NULL)
  , protocolTraits_() {
}

template<typename ProtocolTraits_>
TUnframedACReadState<ProtocolTraits_>::~TUnframedACReadState() {
}

template<typename ProtocolTraits_>
void TUnframedACReadState<ProtocolTraits_>::getReadBuffer(void** bufReturn,
                                                          size_t* lenReturn) {
  uint32_t bytesAvailable = memBuffer_.available_write();
  if (bytesAvailable > 0) {
    // If there is room available in the buffer, just return it.
    *lenReturn = bytesAvailable;
    *bufReturn = memBuffer_.getWritePtr(bytesAvailable);
    return;
  }

  uint32_t bufferSize = memBuffer_.getBufferSize();
  uint32_t available_read = memBuffer_.available_read();
  // we get this much without growing the buffer capacity
  uint32_t additionalSpace = bufferSize - available_read;
  if (additionalSpace == 0) {
    // We need more room.  memBuffer_ will at least double it's capacity when
    // asked for even a single byte.
    additionalSpace = kInitialBufferSize;
  }

  // Don't allow more than maxMessageSize_.
  // Be careful not to over- or underflow uint32_t when checking.
  //
  // readDataAvailable() fails the read when we've already read maxMessageSize_
  // bytes, so available_read should always be less than maxMessageSize_ here.
  // (Unless maxMessageSize_ is 0, but that's a programmer bug.)
  assert(available_read < maxMessageSize_);
  if (available_read > maxMessageSize_ - additionalSpace) {
    // Don't ask for more than maxMessageSize_ total (but we might get more)
    additionalSpace = maxMessageSize_ - available_read;
  }

  try {
    uint8_t* newBuffer = memBuffer_.getWritePtr(additionalSpace);
    *lenReturn = memBuffer_.available_write();
    *bufReturn = newBuffer;
  } catch (std::exception &ex) {
    T_ERROR("TUnframedAsyncChannel: failed to allocate larger read buffer: %s",
            ex.what());
    *lenReturn = 0;
    *bufReturn = NULL;
  }
}

template<typename ProtocolTraits_>
bool TUnframedACReadState<ProtocolTraits_>::readDataAvailable(size_t len) {
  assert(memBuffer_.available_read() + len <= memBuffer_.getBufferSize());
  memBuffer_.wroteBytes(len);

  uint32_t messageLength = 0;
  uint32_t bytesRead = memBuffer_.available_read();
  uint8_t *buffer = (uint8_t *)memBuffer_.borrow(NULL, &bytesRead);
  if (!protocolTraits_.getMessageLength(buffer, bytesRead, &messageLength)) {
    // We're not at the end of the message yet.
    //
    // If we've hit maxMessageSize_ already, fail now instead of waiting until
    // getReadBuffer() is called again.
    if (bytesRead >= maxMessageSize_) {
      throw transport::TTransportException(
          transport::TTransportException::CORRUPTED_DATA,
          "TUnframedAsyncChannel: max message size exceeded");
    }
    return false;
  }

  // We've read a full message.
  // Swap the data into the callback's buffer.
  // Note that we may have actually read more than one message,
  // so we have to make sure to save any remaining data after the end of the
  // message.
  assert(messageLength <= bytesRead);

  callbackBuffer_->link(&memBuffer_, messageLength);
  memBuffer_.consume(messageLength);

  // We've put a new message in callbackBuffer_
  return true;
}

}}}} // apache::thrift::async::detail

#endif // THRIFT_ASYNC_TUNFRAMEDASYNCCHANNEL_TCC_
