// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents the status returned by a function call in RocksDB.
 * <p>
 * Currently only used with {@link RocksDBException} when the
 * status is not {@link Code#Ok}
 */
public class Status implements Serializable {
  private static final long serialVersionUID = -3794191127754280439L;

  /**
   * The status code.
   */
  private final Code code;

  /**
   * The status sub-code.
   */
  /* @Nullable */ private final SubCode subCode;

  /**
   * The state of the status.
   */
  /* @Nullable */ private final String state;

  /**
   * Constructs a Status.
   *
   * @param code the code.
   * @param subCode the sub-code.
   * @param state the state.
   */
  public Status(final Code code, final SubCode subCode, final String state) {
    this.code = code;
    this.subCode = subCode;
    this.state = state;
  }

  /**
   * Intentionally private as this will be called from JNI
   */
  private Status(final byte code, final byte subCode, final String state) {
    this.code = Code.getCode(code);
    this.subCode = SubCode.getSubCode(subCode);
    this.state = state;
  }

  /**
   * Get the status code.
   *
   * @return the status code.
   */
  public Code getCode() {
    return code;
  }

  /**
   * Get the status sub-code.
   *
   * @return the status sub-code.
   */
  public SubCode getSubCode() {
    return subCode;
  }

  /**
   * Get the state of the status.
   *
   * @return the status state.
   */
  public String getState() {
    return state;
  }

  /**
   * Get a string representation of the status code.
   *
   * @return a string representation of the status code.
   */
  public String getCodeString() {
    final StringBuilder builder = new StringBuilder()
        .append(code.name());
    if(subCode != null && subCode != SubCode.None) {
      builder.append("(")
          .append(subCode.name())
          .append(")");
    }
    return builder.toString();
  }

  /**
   * Status Code.
   * <p>
   * Should stay in sync with /include/rocksdb/status.h:Code and
   * /java/rocksjni/portal.h:toJavaStatusCode
   */
  public enum Code {
    /**
     * Success.
     */
    Ok(                 (byte)0x0),

    /**
     * Not found.
     */
    NotFound(           (byte)0x1),

    /**
     * Corruption detected.
     */
    Corruption(         (byte)0x2),

    /**
     * Not supported.
     */
    NotSupported(       (byte)0x3),

    /**
     * Invalid argument provided.
     */
    InvalidArgument(    (byte)0x4),

    /**
     * I/O error.
     */
    IOError(            (byte)0x5),

    /**
     * There is a merge in progress.
     */
    MergeInProgress(    (byte)0x6),

    /**
     * Incomplete.
     */
    Incomplete(         (byte)0x7),

    /**
     * There is a shutdown in progress.
     */
    ShutdownInProgress( (byte)0x8),

    /**
     * An operation timed out.
     */
    TimedOut(           (byte)0x9),

    /**
     * An operation was aborted.
     */
    Aborted(            (byte)0xA),

    /**
     * The system is busy.
     */
    Busy(               (byte)0xB),

    /**
     * The request expired.
     */
    Expired(            (byte)0xC),

    /**
     * The operation should be reattempted.
     */
    TryAgain(           (byte)0xD),

    /**
     * Undefined.
     */
    Undefined(          (byte)0x7F);

    private final byte value;

    Code(final byte value) {
      this.value = value;
    }

    /**
     * Get a code from its byte representation.
     *
     * @param value the byte representation of the code.
     *
     * @return the code
     *
     * @throws IllegalArgumentException if the {@code value} parameter does not represent a code.
     */
    public static Code getCode(final byte value) {
      for (final Code code : Code.values()) {
        if (code.value == value){
          return code;
        }
      }
      throw new IllegalArgumentException(
          "Illegal value provided for Code (" + value + ").");
    }

    /**
     * Returns the byte value of the enumerations value.
     *
     * @return byte representation
     */
    public byte getValue() {
      return value;
    }
  }

  /**
   * Status Sub-code.
   * <p>
   * should stay in sync with /include/rocksdb/status.h:SubCode and
   * /java/rocksjni/portal.h:toJavaStatusSubCode
   */
  public enum SubCode {
    /**
     * None.
     */
    None(         (byte)0x0),

    /**
     * Timeout whilst waiting on Mutex.
     */
    MutexTimeout( (byte)0x1),

    /**
     * Timeout whilst waiting on Lock.
     */
    LockTimeout(  (byte)0x2),

    /**
     * Maximum limit on number of locks reached.
     */
    LockLimit(    (byte)0x3),

    /**
     * No space remaining.
     */
    NoSpace(      (byte)0x4),

    /**
     * Deadlock detected.
     */
    Deadlock(     (byte)0x5),

    /**
     * Stale file detected.
     */
    StaleFile(    (byte)0x6),

    /**
     * Reached the maximum memory limit.
     */
    MemoryLimit(  (byte)0x7),

    /**
     * Undefined.
     */
    Undefined(    (byte)0x7F);

    private final byte value;

    SubCode(final byte value) {
      this.value = value;
    }

    /**
     * Get a sub-code from its byte representation.
     *
     * @param value the byte representation of the sub-code.
     *
     * @return the sub-code
     *
     * @throws IllegalArgumentException if the {@code value} parameter does not represent a
     *     sub-code.
     */
    public static SubCode getSubCode(final byte value) {
      for (final SubCode subCode : SubCode.values()) {
        if (subCode.value == value){
          return subCode;
        }
      }
      throw new IllegalArgumentException(
          "Illegal value provided for SubCode (" + value + ").");
    }

    /**
     * Returns the byte value of the enumerations value.
     *
     * @return byte representation
     */
    public byte getValue() {
      return value;
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final Status status = (Status) o;
    return code == status.code && subCode == status.subCode && Objects.equals(state, status.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(code, subCode, state);
  }
}
