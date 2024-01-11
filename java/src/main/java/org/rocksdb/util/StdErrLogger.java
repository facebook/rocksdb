// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb.util;

import org.rocksdb.InfoLogLevel;
import org.rocksdb.LoggerInterface;
import org.rocksdb.LoggerType;
import org.rocksdb.RocksObject;

/**
 * Simply redirects all log messages to StdErr.
 */
public class StdErrLogger extends RocksObject implements LoggerInterface {
  /**
   * Constructs a new StdErrLogger.
   *
   * @param logLevel the level at which to log.
   */
  public StdErrLogger(final InfoLogLevel logLevel) {
    this(logLevel, null);
  }

  /**
   * Constructs a new StdErrLogger.
   *
   * @param logLevel the level at which to log.
   * @param logPrefix the string with which to prefix all log messages.
   */
  public StdErrLogger(final InfoLogLevel logLevel, /* @Nullable */ final String logPrefix) {
    super(newStdErrLogger(logLevel.getValue(), logPrefix));
  }

  @Override
  public void setInfoLogLevel(final InfoLogLevel logLevel) {
    setInfoLogLevel(nativeHandle_, logLevel.getValue());
  }

  @Override
  public InfoLogLevel infoLogLevel() {
    return InfoLogLevel.getInfoLogLevel(infoLogLevel(nativeHandle_));
  }

  @Override
  public LoggerType getLoggerType() {
    return LoggerType.STDERR_IMPLEMENTATION;
  }

  private static native long newStdErrLogger(
      final byte logLevel, /* @Nullable */ final String logPrefix);
  private static native void setInfoLogLevel(final long handle, final byte logLevel);
  private static native byte infoLogLevel(final long handle);

  @Override protected native void disposeInternal(final long handle);
}
