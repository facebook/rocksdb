// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * <p>This class provides a custom logger functionality
 * in Java which wraps {@code RocksDB} logging facilities.
 * </p>
 *
 * <p>Using this class RocksDB can log with common
 * Java logging APIs like Log4j or Slf4j without keeping
 * database logs in the filesystem.</p>
 *
 * <strong>Performance</strong>
 * <p>There are certain performance penalties using a Java
 * {@code Logger} implementation within production code.
 * </p>
 *
 * <p>
 * A log level can be set using {@link org.rocksdb.Options} or
 * {@link Logger#setInfoLogLevel(InfoLogLevel)}. The set log level
 * influences the underlying native code. Each log message is
 * checked against the set log level and if the log level is more
 * verbose as the set log level, native allocations will be made
 * and data structures are allocated.
 * </p>
 *
 * <p>Every log message which will be emitted by native code will
 * trigger expensive native to Java transitions. So the preferred
 * setting for production use is either
 * {@link org.rocksdb.InfoLogLevel#ERROR_LEVEL} or
 * {@link org.rocksdb.InfoLogLevel#FATAL_LEVEL}.
 * </p>
 */
public abstract class Logger extends RocksCallbackObject implements LoggerInterface {
  /**
   * <p>AbstractLogger constructor.</p>
   *
   * <p><strong>Important:</strong> the log level set within
   * the {@link org.rocksdb.Options} instance will be used as
   * maximum log level of RocksDB.</p>
   *
   * @param options {@link org.rocksdb.Options} instance.
   *
   * @deprecated Use {@link Logger#Logger(InfoLogLevel)} instead, e.g. {@code new
   *     Logger(options.infoLogLevel())}.
   */
  @Deprecated
  public Logger(final Options options) {
    this(options.infoLogLevel());
  }

  /**
   * <p>AbstractLogger constructor.</p>
   *
   * <p><strong>Important:</strong> the log level set within
   * the {@link org.rocksdb.DBOptions} instance will be used
   * as maximum log level of RocksDB.</p>
   *
   * @param dboptions {@link org.rocksdb.DBOptions} instance.
   *
   * @deprecated Use {@link Logger#Logger(InfoLogLevel)} instead, e.g. {@code new
   *     Logger(dbOptions.infoLogLevel())}.
   */
  @Deprecated
  public Logger(final DBOptions dboptions) {
    this(dboptions.infoLogLevel());
  }

  /**
   * <p>AbstractLogger constructor.</p>
   *
   * @param logLevel the log level.
   */
  public Logger(final InfoLogLevel logLevel) {
    super(logLevel.getValue());
  }

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    if (nativeParameterHandles.length == 1) {
      return newLogger(nativeParameterHandles[0]);
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public void setInfoLogLevel(final InfoLogLevel logLevel) {
    setInfoLogLevel(nativeHandle_, logLevel.getValue());
  }

  @Override
  public InfoLogLevel infoLogLevel() {
    return InfoLogLevel.getInfoLogLevel(
        infoLogLevel(nativeHandle_));
  }

  @Override
  public long getNativeHandle() {
    return nativeHandle_;
  }

  @Override
  public final LoggerType getLoggerType() {
    return LoggerType.JAVA_IMPLEMENTATION;
  }

  protected abstract void log(final InfoLogLevel logLevel, final String logMsg);

  protected native long newLogger(final long logLevel);
  protected native void setInfoLogLevel(final long handle, final byte logLevel);
  protected native byte infoLogLevel(final long handle);

  /**
   * We override {@link RocksCallbackObject#disposeInternal()}
   * as disposing of a rocksdb::LoggerJniCallback requires
   * a slightly different approach as it is a std::shared_ptr.
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  private native void disposeInternal(final long handle);
}
