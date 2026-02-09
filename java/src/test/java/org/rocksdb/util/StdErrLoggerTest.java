// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import org.junit.ClassRule;
import org.junit.Test;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksNativeLibraryResource;

public class StdErrLoggerTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  // Logging with the stderr logger would pollute the console when tests were run (and
  // from Java, we can't redirect or close stderr).
  // So we just test creation of a StdErrLogger and setting it on Options
  // without opening the DB.
  @Test
  public void nativeLoggersWithOptions() {
    try (final Options options = new Options().setCreateIfMissing(true);
         final StdErrLogger stdErrLogger =
             new StdErrLogger(InfoLogLevel.DEBUG_LEVEL, "[Options prefix]")) {
      options.setLogger(stdErrLogger);
    }
  }

  // Logging with the stderr logger would pollute the console when tests were run (and
  // from Java, we can't redirect or close stderr).
  // So we just test creation of a StdErrLogger and setting it on DBOptions
  // without opening the DB.
  @Test
  public void nativeLoggersWithDBOptions() {
    try (final DBOptions options = new DBOptions().setCreateIfMissing(true);
         final StdErrLogger stdErrLogger =
             new StdErrLogger(InfoLogLevel.DEBUG_LEVEL, "[DBOptions prefix]")) {
      options.setLogger(stdErrLogger);
    }
  }
}
