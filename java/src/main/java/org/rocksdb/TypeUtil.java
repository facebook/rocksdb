// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import java.nio.ByteBuffer;

/**
 * Util class to get internal key for seeking from user key.
 */
public class TypeUtil {
  public static byte[] getInternalKey(byte[] userKey, Options options) {
    if (options == null || userKey == null) {
      throw new IllegalArgumentException("ByteBuffer and options parameters must not be null");
    }
    return getInternalKeyJni(userKey, userKey.length, options.getNativeHandle());
  }

  public static int getInternalKey(ByteBuffer userKey, ByteBuffer internalKey, Options options) {
    int result;
    if (options == null || userKey == null || internalKey == null) {
      throw new IllegalArgumentException("ByteBuffer and options parameters must not be null");
    }
    if (userKey.isDirect()) {
      if (internalKey.isDirect()) {
        result =
            getInternalKeyDirect0(userKey, userKey.position(), userKey.remaining(), internalKey,
                internalKey.position(), internalKey.remaining(), options.getNativeHandle());
      } else {
        result = getInternalKeyDirect1(userKey, userKey.position(), userKey.remaining(),
            internalKey.array(), internalKey.arrayOffset() + internalKey.position(),
            internalKey.remaining(), options.getNativeHandle());
      }
    } else {
      if (internalKey.isDirect()) {
        result = getInternalKeyByteArray0(userKey.array(),
            userKey.arrayOffset() + userKey.position(), userKey.remaining(), internalKey,
            internalKey.position(), internalKey.remaining(), options.getNativeHandle());
      } else {
        result = getInternalKeyByteArray1(userKey.array(),
            userKey.arrayOffset() + userKey.position(), userKey.remaining(), internalKey.array(),
            internalKey.arrayOffset() + internalKey.position(), internalKey.remaining(),
            options.getNativeHandle());
      }
    }
    userKey.position(userKey.limit());
    internalKey.limit(Math.min(internalKey.position() + result, internalKey.limit()));
    return result;
  }

  public static byte[] getInternalKeyForPrev(byte[] userKey, Options options) {
    if (options == null || userKey == null) {
      throw new IllegalArgumentException("Byte array and options parameters must not be null");
    }
    return getInternalKeyForPrevJni(userKey, userKey.length, options.getNativeHandle());
  }

  public static int getInternalKeyForPrev(
      ByteBuffer userKey, ByteBuffer internalKey, Options options) {
    if (options == null || userKey == null || internalKey == null) {
      throw new IllegalArgumentException("ByteBuffer and options parameters must not be null");
    }
    int result;
    if (userKey.isDirect()) {
      if (internalKey.isDirect()) {
        result = getInternalKeyDirectForPrev0(userKey, userKey.position(), userKey.remaining(),
            internalKey, internalKey.position(), internalKey.remaining(),
            options.getNativeHandle());
      } else {
        result = getInternalKeyDirectForPrev1(userKey, userKey.position(), userKey.remaining(),
            internalKey.array(), internalKey.arrayOffset() + internalKey.position(),
            internalKey.remaining(), options.getNativeHandle());
      }
    } else {
      if (internalKey.isDirect()) {
        result = getInternalKeyByteArrayForPrev0(userKey.array(),
            userKey.arrayOffset() + userKey.position(), userKey.remaining(), internalKey,
            internalKey.position(), internalKey.remaining(), options.getNativeHandle());
      } else {
        result = getInternalKeyByteArrayForPrev1(userKey.array(),
            userKey.arrayOffset() + userKey.position(), userKey.remaining(), internalKey.array(),
            internalKey.arrayOffset() + internalKey.position(), internalKey.remaining(),
            options.getNativeHandle());
      }
    }
    userKey.position(userKey.limit());
    internalKey.limit(Math.min(internalKey.position() + result, internalKey.limit()));
    return result;
  }

  private static native int getInternalKeyDirect0(ByteBuffer userKey, int userKeyOffset,
      int userKeyLen, ByteBuffer internalKey, int internalKeyOffset, int internalKeyLen,
      long optionsHandle);
  private static native int getInternalKeyByteArray0(byte[] userKey, int userKeyOffset,
      int userKeyLen, ByteBuffer internalKey, int internalKeyOffset, int internalKeyLen,
      long optionsHandle);
  private static native int getInternalKeyDirect1(ByteBuffer userKey, int userKeyOffset,
      int userKeyLen, byte[] internalKey, int internalKeyOffset, int internalKeyLen,
      long optionsHandle);
  private static native int getInternalKeyByteArray1(byte[] userKey, int userKeyOffset,
      int userKeyLen, byte[] internalKey, int internalKeyOffset, int internalKeyLen,
      long optionsHandle);
  private static native byte[] getInternalKeyJni(
      byte[] userKey, int userKeyLen, long optionsHandle);

  private static native int getInternalKeyDirectForPrev0(ByteBuffer userKey, int userKeyOffset,
      int userKeyLen, ByteBuffer internalKey, int internalKeyOffset, int internalKeyLen,
      long optionsHandle);
  private static native int getInternalKeyByteArrayForPrev0(byte[] userKey, int userKeyOffset,
      int userKeyLen, ByteBuffer internalKey, int internalKeyOffset, int internalKeyLen,
      long optionsHandle);
  private static native int getInternalKeyDirectForPrev1(ByteBuffer userKey, int userKeyOffset,
      int userKeyLen, byte[] internalKey, int internalKeyOffset, int internalKeyLen,
      long optionsHandle);
  private static native int getInternalKeyByteArrayForPrev1(byte[] userKey, int userKeyOffset,
      int userKeyLen, byte[] internalKey, int internalKeyOffset, int internalKeyLen,
      long optionsHandle);
  private static native byte[] getInternalKeyForPrevJni(
      byte[] userKey, int userKeyLen, long optionsHandle);
}
