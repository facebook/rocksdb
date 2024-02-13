//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class ConfigOptions extends RocksObject {
  /**
   * Construct with default Options
   */
  public ConfigOptions() {
    super(newConfigOptionsInstance());
  }

  public ConfigOptions setDelimiter(final String delimiter) {
    setDelimiter(nativeHandle_, delimiter);
    return this;
  }
  public ConfigOptions setIgnoreUnknownOptions(final boolean ignore) {
    setIgnoreUnknownOptions(nativeHandle_, ignore);
    return this;
  }

  public ConfigOptions setEnv(final Env env) {
    setEnv(nativeHandle_, env.nativeHandle_);
    return this;
  }

  public ConfigOptions setInputStringsEscaped(final boolean escaped) {
    setInputStringsEscaped(nativeHandle_, escaped);
    return this;
  }

  public ConfigOptions setSanityLevel(final SanityLevel level) {
    setSanityLevel(nativeHandle_, level.getValue());
    return this;
  }

  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }

  private static native void disposeInternalJni(final long handle);

  private static long newConfigOptionsInstance() {
    RocksDB.loadLibrary();
    return newConfigOptions();
  }
  private static native long newConfigOptions();
  private static native void setEnv(final long handle, final long envHandle);
  private static native void setDelimiter(final long handle, final String delimiter);
  private static native void setIgnoreUnknownOptions(final long handle, final boolean ignore);
  private static native void setInputStringsEscaped(final long handle, final boolean escaped);
  private static native void setSanityLevel(final long handle, final byte level);
}
