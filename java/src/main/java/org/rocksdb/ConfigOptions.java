package org.rocksdb;

public class ConfigOptions extends RocksObject {
  static {
    RocksDB.loadLibrary();
  }
  public enum SanityCheckLevel { None, Loosely, Exact }
  ;

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

  public ConfigOptions setInputStringsEscaped(final boolean escaped) {
    setInputStringsEscaped(nativeHandle_, escaped);
    return this;
  }

  public ConfigOptions setStringMode(final int mode) {
    setStringMode(nativeHandle_, mode);
    return this;
  }
  public ConfigOptions setSanityLevel(final SanityCheckLevel level) {
    switch (level) {
      case None:
        setSanityLevel(nativeHandle_, 0);
        break;
      case Loosely:
        setSanityLevel(nativeHandle_, 1);
        break;
      case Exact:
        setSanityLevel(nativeHandle_, 0xFF);
        break;
    }
    return this;
  }

  @Override protected final native void disposeInternal(final long handle);

  private native static long newConfigOptions();
  private native static void setDelimiter(final long handle, final String delimiter);
  private native static void setIgnoreUnknownOptions(final long handle, final boolean ignore);
  private native static void setInputStringsEscaped(final long handle, final boolean escaped);
  private native static void setStringMode(final long handle, final int mode);
  private native static void setSanityLevel(final long handle, final int level);
}
