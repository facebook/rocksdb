//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class ConfigOptions extends RocksObject {
  static {
    RocksDB.loadLibrary();
  }

  /**
   * Construct with default Options
   */
  public ConfigOptions() {
    super(newConfigOptions());
  }

  public ConfigOptions(boolean unknown, boolean unsupported) {
    super(newConfigOptions(unknown, unsupported));
  }

  public ConfigOptions setDelimiter(final String delimiter) {
    setDelimiter(nativeHandle_, delimiter);
    return this;
  }
  public ConfigOptions setIgnoreUnknownOptions(final boolean ignore) {
    setIgnoreUnknownOptions(nativeHandle_, ignore);
    return this;
  }

  public ConfigOptions setIgnoreUnsupportedOptions(final boolean ignore) {
    setIgnoreUnsupportedOptions(nativeHandle_, ignore);
    return this;
  }

  public ConfigOptions setInvokePrepareOptions(final boolean prepare) {
    setInvokePrepareOptions(nativeHandle_, prepare);
    return this;
  }

  public ConfigOptions setMutableOptionsOnly(final boolean only) {
    setMutableOptionsOnly(nativeHandle_, only);
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

  @Override protected final native void disposeInternal(final long handle);

  private native static long newConfigOptions();
  private native static long newConfigOptions(boolean unknown, boolean unsupported);
  private native static void setEnv(final long handle, final long envHandle);
  private native static void setDelimiter(final long handle, final String delimiter);
  private native static void setIgnoreUnknownOptions(final long handle, final boolean ignore);
  private native static void setIgnoreUnsupportedOptions(final long handle, final boolean ignore);
  private native static void setInvokePrepareOptions(final long handle, final boolean prepare);
  private native static void setMutableOptionsOnly(final long handle, final boolean only);
  private native static void setInputStringsEscaped(final long handle, final boolean escaped);
  private native static void setSanityLevel(final long handle, final byte level);
}
