// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * Represents the status returned by a function call in RocksDB.
 *
 * Currently only used with {@link RocksDBException} when the
 * status is not {@link Code#Ok}
 */
public class Status {
  private final Code code;
  /* @Nullable */ private final SubCode subCode;
  /* @Nullable */ private final String state;

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

  public Code getCode() {
    return code;
  }

  public SubCode getSubCode() {
    return subCode;
  }

  public String getState() {
    return state;
  }

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

  public enum Code {
    Ok(                 (byte)0x0),
    NotFound(           (byte)0x1),
    Corruption(         (byte)0x2),
    NotSupported(       (byte)0x3),
    InvalidArgument(    (byte)0x4),
    IOError(            (byte)0x5),
    MergeInProgress(    (byte)0x6),
    Incomplete(         (byte)0x7),
    ShutdownInProgress( (byte)0x8),
    TimedOut(           (byte)0x9),
    Aborted(            (byte)0xA),
    Busy(               (byte)0xB),
    Expired(            (byte)0xC),
    TryAgain(           (byte)0xD);

    private final byte value;

    Code(final byte value) {
      this.value = value;
    }

    public static Code getCode(final byte value) {
      for (final Code code : Code.values()) {
        if (code.value == value){
          return code;
        }
      }
      throw new IllegalArgumentException(
          "Illegal value provided for Code.");
    }
  }

  public enum SubCode {
    None(         (byte)0x0),
    MutexTimeout( (byte)0x1),
    LockTimeout(  (byte)0x2),
    LockLimit(    (byte)0x3),
    MaxSubCode(   (byte)0xFE);

    private final byte value;

    SubCode(final byte value) {
      this.value = value;
    }

    public static SubCode getSubCode(final byte value) {
      for (final SubCode subCode : SubCode.values()) {
        if (subCode.value == value){
          return subCode;
        }
      }
      throw new IllegalArgumentException(
          "Illegal value provided for SubCode.");
    }
  }
}
