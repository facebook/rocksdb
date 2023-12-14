// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

/**
 * Simple factors of byte sizes.
 */
public interface SizeUnit {

  /**
   * 1 Kilobyte.
   */
  long KB = 1024L;

  /**
   * 1 Megabyte.
   */
  long MB = KB * KB;

  /**
   * 1 Gigabyte.
   */
  long GB = KB * MB;

  /**
   * 1 Terabyte.
   */
  long TB = KB * GB;

  /**
   * 1 Petabyte.
   */
  long PB = KB * TB;
}
