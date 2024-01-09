// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.rocksdb.InfoLogLevel;

/**
 * NativeLogger is used to create a wrapper around a native, C++
 * RocksDB logger. Native loggers are more efficient than Java
 * loggers, which are invoked over the JNI for every log.
 */
// @ThreadSafe
public final class NativeLogger extends RocksObject implements LoggerInterface {
  private enum NativeLoggerType {
    // In the future, we can add support for Auto-roll, Env logger, or
    // Win logger.
    DEVNULL,
    STDERR
  }

  /**
   * Constructs a new native stderr logger at the specified log level
   * with the specified logPrefix.
   *
   * @param logPrefix the string with which to prefix all logs.
   * @param logLevel  the log level at which to log to stderr.
   * @return the NativeLogger corresponding to the native stderr logger.
   */
  public static NativeLogger newStderrLogger(InfoLogLevel logLevel, String logPrefix) {
    return new NativeLogger(NativeLoggerType.STDERR, logLevel, logPrefix);
  }

  public static NativeLogger newDevnullLogger() {
    // Log level doesn't matter for devnull; DEBUG_LEVEL is arbitrary.
    return new NativeLogger(NativeLoggerType.DEVNULL, InfoLogLevel.DEBUG_LEVEL, "");
  }

  /**
   * Constructs a native logger of type loggerType using the specified logPrefix.
   * Users do not call this directly, since they should create their loggers via
   * the new*Logger static functions.
   *
   * @param loggerType the type of native logger to use
   * @param logPrefix  the string with which to prefix all logs
   */
  private NativeLogger(NativeLoggerType loggerType, InfoLogLevel logLevel, String logPrefix) {
    super(constructNativeLogger(loggerType, logLevel, logPrefix));
  }

  private static long constructNativeLogger(NativeLoggerType type, InfoLogLevel logLevel, String logPrefix) {
    switch (type) {
      case DEVNULL:
        return newNativeDevnullLogger();
      case STDERR:
        return newNativeStderrLogger(logLevel.getValue(), logPrefix);
      default:
        // Should never happen, since cases are exhaustive
        throw new IllegalArgumentException("Invalid native logger type provided: " + type);
    }
  }

  @Override
  public long getNativeLoggerHandle() {
    return nativeHandle_;
  }

  private static native long newNativeDevnullLogger();

  private static native long newNativeStderrLogger(byte logLevel, String logPrefix);

  @Override
  protected native void disposeInternal(final long handle);
}
