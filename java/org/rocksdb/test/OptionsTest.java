// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import java.util.Random;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;

public class OptionsTest {

  static {
    RocksDB.loadLibrary();
  }
  public static void main(String[] args) {
    Options opt = new Options();
    Random rand = PlatformRandomHelper.
        getPlatformSpecificRandomFactory();

    DBOptionsTest.testDBOptions(opt);
    ColumnFamilyOptionsTest.testCFOptions(opt);

    opt.dispose();
    System.out.println("Passed OptionsTest");
  }
}
