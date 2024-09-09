//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import org.rocksdb.*;

/**
 *
 */
public class LoggerFromOptions extends RocksObject implements LoggerInterface {
  public LoggerFromOptions(final String dbName, final DBOptions dbOptions) {
    super(newLoggerFromOptions(dbName, dbOptions.getNativeHandle()));
  }

  @Override
  public void setInfoLogLevel(InfoLogLevel logLevel) {
    setInfoLogLevel(nativeHandle_, logLevel.getValue());
  }

  @Override
  public InfoLogLevel infoLogLevel() {
    return InfoLogLevel.getInfoLogLevel(infoLogLevel(nativeHandle_));
  }

  @Override
  public LoggerType getLoggerType() {
    return LoggerType.FROM_OPTIONS_IMPLEMENTATION;
  }

  @Override protected native void disposeInternal(long handle);

  private static native long newLoggerFromOptions(final String dbName, final long dbOptions);

  private static native void setInfoLogLevel(final long handle, final byte logLevel);
  private static native byte infoLogLevel(final long handle);
}
