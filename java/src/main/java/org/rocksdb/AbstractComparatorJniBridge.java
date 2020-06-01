// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;

/**
 * This class is intentionally private,
 * it holds methods which are called
 * from C++ to interact with a Comparator
 * written in Java.
 *
 * Placing these bridge methods in this
 * class keeps the API of the
 * {@link org.rocksdb.AbstractComparator} clean.
 */
class AbstractComparatorJniBridge {

    /**
     * Only called from JNI.
     *
     * Simply a bridge to calling
     * {@link AbstractComparator#compare(ByteBuffer, ByteBuffer)},
     * which ensures that the byte buffer lengths are correct
     * before and after the call.
     *
     * @param comparator the comparator object on which to
     *     call {@link AbstractComparator#compare(ByteBuffer, ByteBuffer)}
     * @param a buffer access to first key
     * @param aLen the length of the a key,
     *     may be smaller than the buffer {@code a}
     * @param b buffer access to second key
     * @param bLen the length of the b key,
     *     may be smaller than the buffer {@code b}
     *
     * @return the result of the comparison
     */
    private static int compareInternal(
            final AbstractComparator comparator,
            final ByteBuffer a, final int aLen,
            final ByteBuffer b, final int bLen) {
        if (aLen != -1) {
            a.mark();
            a.limit(aLen);
        }
        if (bLen != -1) {
            b.mark();
            b.limit(bLen);
        }

        final int c = comparator.compare(a, b);

        if (aLen != -1) {
            a.reset();
        }
        if (bLen != -1) {
            b.reset();
        }

        return c;
    }

    /**
     * Only called from JNI.
     *
     * Simply a bridge to calling
     * {@link AbstractComparator#findShortestSeparator(ByteBuffer, ByteBuffer)},
     * which ensures that the byte buffer lengths are correct
     * before the call.
     *
     * @param comparator the comparator object on which to
     *     call {@link AbstractComparator#findShortestSeparator(ByteBuffer, ByteBuffer)}
     * @param start buffer access to the start key
     * @param startLen the length of the start key,
     *     may be smaller than the buffer {@code start}
     * @param limit buffer access to the limit key
     * @param limitLen the length of the limit key,
     *     may be smaller than the buffer {@code limit}
     *
     * @return either {@code startLen} if the start key is unchanged, otherwise
     *     the new length of the start key
     */
    private static int findShortestSeparatorInternal(
            final AbstractComparator comparator,
            final ByteBuffer start, final int startLen,
            final ByteBuffer limit, final int limitLen) {
        if (startLen != -1) {
            start.limit(startLen);
        }
        if (limitLen != -1) {
            limit.limit(limitLen);
        }
        comparator.findShortestSeparator(start, limit);
        return start.remaining();
    }

    /**
     * Only called from JNI.
     *
     * Simply a bridge to calling
     * {@link AbstractComparator#findShortestSeparator(ByteBuffer, ByteBuffer)},
     * which ensures that the byte buffer length is correct
     * before the call.
     *
     * @param comparator the comparator object on which to
     *     call {@link AbstractComparator#findShortSuccessor(ByteBuffer)}
     * @param key buffer access to the key
     * @param keyLen the length of the key,
     *     may be smaller than the buffer {@code key}
     *
     * @return either keyLen if the key is unchanged, otherwise the new length of the key
     */
    private static int findShortSuccessorInternal(
            final AbstractComparator comparator,
            final ByteBuffer key, final int keyLen) {
        if (keyLen != -1) {
            key.limit(keyLen);
        }
        comparator.findShortSuccessor(key);
        return key.remaining();
    }
}
