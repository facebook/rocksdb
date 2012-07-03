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

#ifndef _THRIFT_CONCURRENCY_EXCEPTION_H_
#define _THRIFT_CONCURRENCY_EXCEPTION_H_ 1

#include <exception>
#include "thrift/lib/cpp/Thrift.h"

namespace apache { namespace thrift { namespace concurrency {

class NoSuchTaskException : public apache::thrift::TLibraryException {};

class UncancellableTaskException : public apache::thrift::TLibraryException {};

class InvalidArgumentException : public apache::thrift::TLibraryException {};

class IllegalStateException : public apache::thrift::TLibraryException {
public:
  IllegalStateException() {}
  IllegalStateException(const std::string& message) : TLibraryException(message) {}
};

class TimedOutException : public apache::thrift::TLibraryException {
public:
  TimedOutException():TLibraryException("TimedOutException"){};
  TimedOutException(const std::string& message ) :
    TLibraryException(message) {}
};

class TooManyPendingTasksException : public apache::thrift::TLibraryException {
public:
  TooManyPendingTasksException():TLibraryException("TooManyPendingTasksException"){};
  TooManyPendingTasksException(const std::string& message ) :
    TLibraryException(message) {}
};

class SystemResourceException : public apache::thrift::TLibraryException {
public:
    SystemResourceException() {}

    SystemResourceException(const std::string& message) :
        TLibraryException(message) {}
};

}}} // apache::thrift::concurrency

#endif // #ifndef _THRIFT_CONCURRENCY_EXCEPTION_H_
