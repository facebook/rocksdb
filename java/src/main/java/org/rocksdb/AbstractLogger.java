package org.rocksdb;

/**
 * <p>This class provides a custom logger functionality
 * in Java which wraps {@code RocksDB} logging facilities.
 * </p>
 *
 * <p>Using this class RocksDB can log with common
 * Java logging APIs like Log4j or Slf4j without keeping
 * database logs in the filesystem.</p>
 */
public abstract class AbstractLogger extends RocksObject {

  /**
   * <p>AbstractLogger constructor.</p>
   *
   * <p><strong>Important:</strong> the log level set within
   * the {@link org.rocksdb.Options} instance will be used as
   * maximum log level of RocksDB.</p>
   *
   * @param options {@link org.rocksdb.Options} instance.
   */
  public AbstractLogger(Options options) {
    createNewLoggerOptions(options.nativeHandle_);
  }

  /**
   * <p>AbstractLogger constructor.</p>
   *
   * <p><strong>Important:</strong> the log level set within
   * the {@link org.rocksdb.DBOptions} instance will be used
   * as maximum log level of RocksDB.</p>
   *
   * @param dboptions {@link org.rocksdb.DBOptions} instance.
   */
  public AbstractLogger(DBOptions dboptions) {
    createNewLoggerDbOptions(dboptions.nativeHandle_);
  }

  /**
   * Set {@link org.rocksdb.InfoLogLevel} to AbstractLogger.
   *
   * @param infoLogLevel {@link org.rocksdb.InfoLogLevel} instance.
   */
  public void setInfoLogLevel(InfoLogLevel infoLogLevel) {
      setInfoLogLevel(nativeHandle_, infoLogLevel.getValue());
  }

  /**
   * Return the loggers log level.
   *
   * @return {@link org.rocksdb.InfoLogLevel} instance.
   */
  public InfoLogLevel infoLogLevel() {
    return InfoLogLevel.getInfoLogLevel(
        infoLogLevel(nativeHandle_));
  }

  protected abstract void log(InfoLogLevel infoLogLevel,
      String logMsg);

  /**
   * Deletes underlying C++ slice pointer.
   * Note that this function should be called only after all
   * RocksDB instances referencing the slice are closed.
   * Otherwise an undefined behavior will occur.
   */
  @Override
  protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }

  protected native void createNewLoggerOptions(
      long options);
  protected native void createNewLoggerDbOptions(
      long dbOptions);
  protected native void setInfoLogLevel(long handle,
      byte infoLogLevel);
  protected native byte infoLogLevel(long handle);
  private native void disposeInternal(long handle);
}
