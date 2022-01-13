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

  @Override protected final native void disposeInternal(final long handle);

  private native static long newConfigOptions();
  private native static void setEnv(final long handle, final long envHandle);
  private native static void setDelimiter(final long handle, final String delimiter);
  private native static void setIgnoreUnknownOptions(final long handle, final boolean ignore);
  private native static void setInputStringsEscaped(final long handle, final boolean escaped);
  private native static void setSanityLevel(final long handle, final byte level);
}
