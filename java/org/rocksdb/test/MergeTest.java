// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import java.util.Collections;
import org.rocksdb.*;

public class MergeTest {
  static final String db_path_string = "/tmp/mergestringjni_db";
  static final String db_path_function = "/tmp/mergefunctionjni_db";
  static {
	RocksDB.loadLibrary();
  }

  public static void testStringOption()
	  throws InterruptedException, RocksDBException {

	System.out.println("Testing merge function string option ===");

	Options opt = new Options();
	opt.setCreateIfMissing(true);
	opt.setMergeOperatorName("stringappend");

	RocksDB db = RocksDB.open(opt, db_path_string);

	System.out.println("Writing aa under key...");
	db.put("key".getBytes(), "aa".getBytes());

	System.out.println("Writing bb under key...");
	db.merge("key".getBytes(), "bb".getBytes());

	byte[] value = db.get("key".getBytes());
	String strValue = new String(value);

	System.out.println("Retrieved value: " + strValue);

	db.close();
	opt.dispose();

	assert(strValue.equals("aa,bb"));

	System.out.println("Merge function string option passed!");

  }

  public static void testOperatorOption()
	  throws InterruptedException, RocksDBException {

	System.out.println("Testing merge function operator option ===");

	Options opt = new Options();
	opt.setCreateIfMissing(true);

	StringAppendOperator stringAppendOperator = new StringAppendOperator();
	opt.setMergeOperator(stringAppendOperator);

	RocksDB db = RocksDB.open(opt, db_path_string);

	System.out.println("Writing aa under key...");
	db.put("key".getBytes(), "aa".getBytes());

	System.out.println("Writing bb under key...");
	db.merge("key".getBytes(), "bb".getBytes());

	byte[] value = db.get("key".getBytes());
	String strValue = new String(value);

	System.out.println("Retrieved value: " + strValue);

	db.close();
	opt.dispose();

	assert(strValue.equals("aa,bb"));

	System.out.println("Merge function operator option passed!");

  }

  public static void main(String[] args)
	  throws InterruptedException, RocksDBException {
	testStringOption();
	testOperatorOption();

  }
}
