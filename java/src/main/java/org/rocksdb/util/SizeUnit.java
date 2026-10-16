// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

/**
 * Simple factors of byte sizes.
 */
public class SizeUnit {
  /**
   * 1 Kilobyte.
   */
  public static final long KB = 1024L;

  /**
   * 1 Megabyte.
   */
  public static final long MB = KB * KB;

  /**
   * 1 Gigabyte.
   */
  public static final long GB = KB * MB;

  /**
   * 1 Terabyte.
   */
  public static final long TB = KB * GB;

  /**
   * 1 Petabyte.
   */
  public static final long PB = KB * TB;
}
