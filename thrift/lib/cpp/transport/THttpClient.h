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

#ifndef THRIFT_TRANSPORT_THTTPCLIENT_H_
#define THRIFT_TRANSPORT_THTTPCLIENT_H_ 1

#include <transport/THttpTransport.h>

namespace apache { namespace thrift { namespace transport {

class THttpClient : public THttpTransport {
 public:
  /*
   * Create for a given host and port.  The version that doesn't take
   * a transport constructs its own TSocket as a transport.
   *
   * The path must be non-empty and start with "/".
   */
  THttpClient(const boost::shared_ptr<TTransport>& transport,
              const std::string& host,
              const std::string& path);
  THttpClient(const std::string& host, int port, const std::string& path);

  virtual ~THttpClient();

  void setUserAgent(const std::string&);

  virtual void flush();

  virtual void close() {
    connectionClosedByServer_ = false;
    THttpTransport::close();
  }

  virtual void init() {
    /*
     * HTTP requires that the `abs_path' component of a POST message start
     * with '/' (see rfc2616 and rfc2396).
     */
    assert(!path_.empty() && path_[0] == '/');

    THttpTransport::init();
  }

 protected:

  const std::string host_;
  const std::string path_;
  std::string userAgent_;
  bool connectionClosedByServer_;

  virtual void parseHeader(char* header);
  virtual bool parseStatusLine(char* status);

};

}}} // apache::thrift::transport

#endif // #ifndef THRIFT_TRANSPORT_THTTPCLIENT_H_
