//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Configuration options.
 */
public class ConfigOptions extends RocksObject {
  /**
   * Constructs a new ConfigOptions.
   */
  public ConfigOptions() {
    super(newConfigOptionsInstance());
  }

  /**
   * Set the delimiter used between options.
   *
   * @param delimiter the delimiter
   *
   * @return the reference to the current options
   */
  public ConfigOptions setDelimiter(final String delimiter) {
    setDelimiter(nativeHandle_, delimiter);
    return this;
  }

  /**
   * Set whether to ignore unknown options.
   *
   * @param ignore true to ignore unknown options, otherwise raise an error.
   *
   * @return the reference to the current options
   */
  public ConfigOptions setIgnoreUnknownOptions(final boolean ignore) {
    setIgnoreUnknownOptions(nativeHandle_, ignore);
    return this;
  }

  /**
   * Set the environment.
   *
   * @param env the environment.
   *
   * @return the reference to the current options
   */
  public ConfigOptions setEnv(final Env env) {
    setEnv(nativeHandle_, env.nativeHandle_);
    return this;
  }

  /**
   * Set whether to escape input strings.
   *
   * @param escaped true to escape input strings, false otherwise.
   *
   * @return the reference to the current options
   */
  public ConfigOptions setInputStringsEscaped(final boolean escaped) {
    setInputStringsEscaped(nativeHandle_, escaped);
    return this;
  }

  /**
   * Set the sanity level.
   *
   * @param level the sanity level.
   *
   * @return the reference to the current options
   */
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
