// Copyright (c) 2009- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TRANSPORT_HDFS_H
#define _THRIFT_TRANSPORT_HDFS_H

#include <string>
#include <boost/shared_ptr.hpp>

/**
 * Dead-simple wrappers around hdfs and hdfsFile descriptors.
 * The wrappers take responsibility of descriptor allocation/release in ctor/dtor.
 *
 * @author Li Zhang <lzhang@facebook.com>
 */

class HDFS {
 public:
  HDFS();
  HDFS(const std::string& host, uint16_t port);
  bool disconnect();
  ~HDFS();
  void* getHandle() const {
    return hdfs_;
  }
  bool isConnected() const {
    return hdfs_ != NULL;
  }  
 protected:
  void* hdfs_;
};

class HDFSFile {
 public:
  enum AccessPolicy {
    OPEN_FOR_READ = 0,
    CREATE_FOR_WRITE = 1,
    OPEN_FOR_APPEND = 2,
  };
  HDFSFile(boost::shared_ptr<HDFS> hdfs, const std::string& path, AccessPolicy ap,
            int bufferSize = 0, short replication = 0, int32_t blocksize = 0);
  bool close();
  void* getHandle() const {
    return file_;
  }
  boost::shared_ptr<HDFS> getFS() const {
    return hdfs_;
  }
  bool isOpen() const {
    return file_ != NULL;
  }
  ~HDFSFile();
 protected:
  boost::shared_ptr<HDFS> hdfs_;
  void* file_;
};

#endif
