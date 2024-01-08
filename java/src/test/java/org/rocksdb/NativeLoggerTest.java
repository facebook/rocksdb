// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.NativeLogger;
import org.rocksdb.InfoLogLevel;

public class NativeLoggerTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  // Logging with the stderr logger would pollute the console when tests were run (and
  // from Java, we can't redirect or close stderr).
  //
  // Instead, we create both an stderr and devnull logger, but only use the devnull
  // logger in the subsequently created DB.
  public void nativeDevNullLogger() throws RocksDBException {
    Options options = new Options().setCreateIfMissing(true);

    // Just verify that setting stderr logger doesn't throw
    NativeLogger stderrNativeLogger = NativeLogger.newStderrLogger(
      InfoLogLevel.DEBUG_LEVEL, "foo prefix");
    options.setNativeLogger(stderrNativeLogger);

    // But we actually set the native logger to be devnull
    NativeLogger devnullNativeLogger = NativeLogger.newDevnullLogger();
    options.setNativeLogger(devnullNativeLogger);

    stderrNativeLogger.close();
    devnullNativeLogger.close();

    try (final RocksDB db = RocksDB.open(options,
      dbFolder.getRoot().getAbsolutePath())) {
        db.put("key".getBytes(), "value".getBytes());
        db.flush(new FlushOptions().setWaitForFlush(true));
    } finally {
      options.close();
    }
  }
}
