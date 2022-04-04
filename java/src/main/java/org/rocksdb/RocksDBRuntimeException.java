package org.rocksdb;

public class RocksDBRuntimeException extends RuntimeException {
  /* @Nullable */ private final Status status;

  /**
   * The private construct used by a set of public static factory method.
   *
   * @param msg the specified error message.
   */
  public RocksDBRuntimeException(final String msg) {
    this(msg, null);
  }

  public RocksDBRuntimeException(final String msg, final Status status) {
    super(msg);
    this.status = status;
  }

  public RocksDBRuntimeException(final Status status) {
    super(status.getState() != null ? status.getState() : status.getCodeString());
    this.status = status;
  }

  /**
   * Get the status returned from RocksDB
   *
   * @return The status reported by RocksDB, or null if no status is available
   */
  public Status getStatus() {
    return status;
  }
}
