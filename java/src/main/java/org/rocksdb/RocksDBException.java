// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * A RocksDBException encapsulates the error of an operation.  This exception
 * type is used to describe an internal error from the c++ rocksdb library.
 */
public class RocksDBException extends Exception {
  private static final long serialVersionUID = -5187634878466267120L;

  /**
   * The error status that led to this exception.
   */
  /* @Nullable */ private final Status status;

  /**
   * The private construct used by a set of public static factory method.
   *
   * @param message the specified error message.
   */
  public RocksDBException(final String message) {
    this(message, null);
  }

  /**
   * Constructs a RocksDBException.
   *
   * @param message the detail message. The detail message is saved for later retrieval by the
   *     {@link #getMessage()} method.
   * @param status the error status that led to this exception.
   */
  public RocksDBException(final String message, final Status status) {
    super(message);
    this.status = status;
  }

  /**
   * Constructs a RocksDBException.
   *
   * @param status the error status that led to this exception.
   */
  public RocksDBException(final Status status) {
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
