// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * LoggerInterface is a thin interface that specifies the most basic
 * functionality for a Java wrapper around a RocksDB Logger.
 */
public interface LoggerInterface {
  /**
   * Set the log level.
   *
   * @param logLevel the level at which to log.
   */
  void setInfoLogLevel(final InfoLogLevel logLevel);

  /**
   * Get the log level
   *
   * @return the level at which to log.
   */
  InfoLogLevel infoLogLevel();

  /**
   * Get the underlying Native Handle.
   *
   * @return the native handle.
   */
  long getNativeHandle();

  /**
   * Get the type of this logger.
   *
   * @return the type of this logger.
   */
  LoggerType getLoggerType();
}
