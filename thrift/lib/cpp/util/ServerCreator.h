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
#ifndef THRIFT_UTIL_SERVERCREATOR_H_
#define THRIFT_UTIL_SERVERCREATOR_H_ 1

#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift {

namespace server {
class TServer;
}

namespace util {

/**
 * ServerCreator is an abstract class for creating a thrift server.
 */
class ServerCreator {
 public:
  virtual ~ServerCreator() {}

  /**
   * Create a new server.
   */
  virtual boost::shared_ptr<server::TServer> createServer() = 0;

 protected:
  ServerCreator() {}
};

}}} // apache::thrift::util

#endif // THRIFT_UTIL_SERVERCREATOR_H_
