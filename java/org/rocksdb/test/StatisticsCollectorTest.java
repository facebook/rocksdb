// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import java.util.Collections;
import org.rocksdb.*;

public class StatisticsCollectorTest {
  static final String db_path = "/tmp/backupablejni_db";
  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args) 
      throws InterruptedException, RocksDBException {
    Options opt = new Options().createStatistics().setCreateIfMissing(true);
    Statistics stats = opt.statisticsPtr();

    RocksDB db = RocksDB.open(db_path);

    StatsCallbackMock callback = new StatsCallbackMock();
    StatsCollectorInput statsInput = new StatsCollectorInput(stats, callback);
    
    StatisticsCollector statsCollector = new StatisticsCollector(
        Collections.singletonList(statsInput), 100);
    statsCollector.start();

    Thread.sleep(1000);

    assert(callback.tickerCallbackCount > 0);
    assert(callback.histCallbackCount > 0);

    statsCollector.shutDown(1000);

    db.close();
    opt.dispose();

    System.out.println("Stats collector test passed.!");
  }
}
