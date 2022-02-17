// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * A RocksDBException encapsulates the error of an operation.  This exception
 * type is used to describe an internal error from the c++ rocksdb library.
 */
@SuppressWarnings({"SerializableHasSerializationMethods", "UncheckedExceptionClass"})
public class RocksDBRuntimeException extends RuntimeException {

  private static final long serialVersionUID = -9108026450339418124L;
  /* @Nullable */ private final Status status;

  /**
   * The private construct used by a set of public static factory method.
   *
   * @param msg the specified error message.
   */
  @SuppressWarnings("unused")
  public RocksDBRuntimeException(final String msg) {
    this(msg, null);
  }

  @SuppressWarnings("WeakerAccess")
  public RocksDBRuntimeException(final String msg, final RocksDBException cause) {
    super(msg, cause);
    this.status = cause.getStatus();
  }

  @SuppressWarnings("unused")
  public RocksDBRuntimeException(final Status status) {
    super(status.getState() != null ? status.getState()
        : status.getCodeString());
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
