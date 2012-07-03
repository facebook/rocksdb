// Copyright (c) 2009- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TRANSPORT_THDFSFILETRANSPORT_H_
#define _THRIFT_TRANSPORT_THDFSFILETRANSPORT_H_

#include "thrift/lib/cpp/transport/TTransport.h"
#include "thrift/lib/cpp/transport/TVirtualTransport.h"
#include "HDFS.h"
#include <string>

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift { namespace transport {
/**
 * Dead-simple wrapper around libhdfs.
 * THDFSFileTransport only takes care of read/write,
 * and leaves allocation/release to HDFS and HDFSFile.
 * @author Li Zhang <lzhang@facebook.com>
 */
class THDFSFileTransport : public TVirtualTransport<THDFSFileTransport> {
 public:

  THDFSFileTransport(boost::shared_ptr<HDFSFile> hdfsFile) : hdfsFile_(hdfsFile) {
  }

  ~THDFSFileTransport() {
  }

  bool isOpen() {
    return hdfsFile_->isOpen();
  }

  void open();

  void close();

  uint32_t read(uint8_t* buf, uint32_t len);

  void write(const uint8_t* buf, uint32_t len);

 protected:
  boost::shared_ptr<HDFSFile> hdfsFile_;
};

}}} // apache::thrift::transport

#endif //  _THRIFT_TRANSPORT_THDFSFILETRANSPORT_H_
