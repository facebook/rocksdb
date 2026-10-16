//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Table Properties Collector Factory.
 */
public abstract class TablePropertiesCollectorFactory extends RocksObject {
  private TablePropertiesCollectorFactory(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * Creates a factory of a table property collector that marks a SST
   * file as need-compaction when it observes at least "D" deletion
   * entries in any "N" consecutive entries, or the ratio of tombstone
   * entries &gt;= deletion_ratio.
   *
   * @param slidingWindowSize "N".Note that this number will be
   *      round up to the smallest multiple of 128 that is no less
   *     than the specified size.
   * @param deletionTrigger "D". Note that even when "N" is changed,
   *     the specified number for "D" will not be changed.
   * @param deletionRatio if &lt;= 0 or &gt; 1, disable triggering compaction
   *     based on deletion ratio. Disabled by default.
   *
   * @return the new compact on deletion collector factory.
   */
  public static TablePropertiesCollectorFactory createNewCompactOnDeletionCollectorFactory(
      final long slidingWindowSize, final long deletionTrigger, final double deletionRatio) {
    final long handle =
        newCompactOnDeletionCollectorFactory(slidingWindowSize, deletionTrigger, deletionRatio);
    return new TablePropertiesCollectorFactory(handle) {
      @Override
      protected void disposeInternal(final long handle) {
        TablePropertiesCollectorFactory.deleteCompactOnDeletionCollectorFactory(handle);
      }
    };
  }

  /**
   * Internal API. Do not use.
   *
   * @param nativeHandle the native handle to wrap.
   *
   * @return the new TablePropertiesCollectorFactory.
   */
  static TablePropertiesCollectorFactory newWrapper(final long nativeHandle) {
    return new TablePropertiesCollectorFactory(nativeHandle) {
      @Override
      protected void disposeInternal(long handle) {
        TablePropertiesCollectorFactory.deleteCompactOnDeletionCollectorFactory(handle);
      }
    };
  }

  private static native long newCompactOnDeletionCollectorFactory(
      final long slidingWindowSize, final long deletionTrigger, final double deletionRatio);

  private static native void deleteCompactOnDeletionCollectorFactory(final long handle);
}
