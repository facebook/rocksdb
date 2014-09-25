// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.rocksdb.ComparatorOptions;
import org.rocksdb.RocksDB;

import java.util.Random;

public class ComparatorOptionsTest {

  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args) {
    final ComparatorOptions copt = new ComparatorOptions();
    Random rand = new Random();

    { // UseAdaptiveMutex test
      copt.setUseAdaptiveMutex(true);
      assert(copt.useAdaptiveMutex() == true);

      copt.setUseAdaptiveMutex(false);
      assert(copt.useAdaptiveMutex() == false);
    }

    copt.dispose();
    System.out.println("Passed ComparatorOptionsTest");
  }
}
