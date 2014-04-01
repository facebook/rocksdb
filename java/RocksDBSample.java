// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

import java.util.*;
import java.lang.*;
import org.rocksdb.*;
import java.io.IOException;

public class RocksDBSample {
  static {
    System.loadLibrary("rocksdbjni");
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("usage: RocksDBSample db_path");
      return;
    }
    String db_path = args[0];
    String db_path_not_found = db_path + "_not_found";

    System.out.println("RocksDBSample");
    RocksDB db = null;
    Options options = new Options();
    try {
      db = RocksDB.open(options, db_path_not_found);
      assert(false);
    } catch (RocksDBException e) {
      System.out.format("caught the expceted exception -- %s\n", e);
      assert(db == null);
    }

    options.setCreateIfMissing(true);
    try {
      db = RocksDB.open(options, db_path_not_found);
      db.put("hello".getBytes(), "world".getBytes());
      byte[] value = db.get("hello".getBytes());
      assert("world".equals(new String(value)));
    } catch (RocksDBException e) {
      System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
      assert(db == null);
      assert(false);
    }

    // be sure to release the c++ pointer
    options.dispose();
    db.close();

    try {
      db = RocksDB.open(db_path);
      db.put("hello".getBytes(), "world".getBytes());
      byte[] value = db.get("hello".getBytes());
      System.out.format("Get('hello') = %s\n",
          new String(value));

      for (int i = 1; i <= 9; ++i) {
        for (int j = 1; j <= 9; ++j) {
          db.put(String.format("%dx%d", i, j).getBytes(),
                 String.format("%d", i * j).getBytes());
        }
      }

      for (int i = 1; i <= 9; ++i) {
        for (int j = 1; j <= 9; ++j) {
          System.out.format("%s ", new String(db.get(
              String.format("%dx%d", i, j).getBytes())));
        }
        System.out.println("");
      }

      value = db.get("1x1".getBytes());
      assert(value != null);
      value = db.get("world".getBytes());
      assert(value == null);

      byte[] testKey = "asdf".getBytes();
      byte[] testValue =
          "asdfghjkl;'?><MNBVCXZQWERTYUIOP{+_)(*&^%$#@".getBytes();
      db.put(testKey, testValue);
      byte[] testResult = db.get(testKey);
      assert(testResult != null);
      assert(Arrays.equals(testValue, testResult));
      assert(new String(testValue).equals(new String(testResult)));

      byte[] insufficientArray = new byte[10];
      byte[] enoughArray = new byte[50];
      int len;
      len = db.get(testKey, insufficientArray);
      assert(len > insufficientArray.length);
      len = db.get("asdfjkl;".getBytes(), enoughArray);
      assert(len == RocksDB.NOT_FOUND);
      len = db.get(testKey, enoughArray);
      assert(len == testValue.length);
    } catch (RocksDBException e) {
      System.err.println(e);
    }
    if (db != null) {
      db.close();
    }
  }
}
