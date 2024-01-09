// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * LoggerInterface is a thin interface that specifies the most basic
 * functionality for a Java wrapper around a RocksDB Logger. It provides a
 * method that returns the handle to the underlying logger, which can in
 * turn be provided to setLogger.
 */
public interface LoggerInterface {
    /**
     * Retrieves a pointer to the native C++ logger object.
     * 
     * @return a pointer to the C++ logger object.
     */
    long getNativeLoggerHandle();
}
