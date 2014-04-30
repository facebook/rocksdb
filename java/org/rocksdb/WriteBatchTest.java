//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
package org.rocksdb;

import java.util.*;
import java.io.UnsupportedEncodingException;

/**
 * This class mimics the db/write_batch_test.cc in the c++ rocksdb library.
 */
public class WriteBatchTest {
  static {
    RocksDB.loadLibrary();
  }

  public static void main(String args[]) {
    System.out.println("Testing WriteBatchTest.Empty ===");
    Empty();

    System.out.println("Testing WriteBatchTest.Multiple ===");
    Multiple();

    System.out.println("Testing WriteBatchTest.Append ===");
    Append();

    System.out.println("Testing WriteBatchTest.Blob ===");
    Blob();

    // The following tests have not yet ported.
    // Continue();
    // PutGatherSlices();

    System.out.println("Passed all WriteBatchTest!");
  }

  static void Empty() {
    WriteBatch batch = new WriteBatch();
    assert(batch.count() == 0);
  }

  static void Multiple() {
    try {
      WriteBatch batch =  new WriteBatch();
      batch.put("foo".getBytes("US-ASCII"), "bar".getBytes("US-ASCII"));
      batch.remove("box".getBytes("US-ASCII"));
      batch.put("baz".getBytes("US-ASCII"), "boo".getBytes("US-ASCII"));
      WriteBatchInternal.setSequence(batch, 100);
      assert(100 == WriteBatchInternal.sequence(batch));
      assert(3 == batch.count());
      assert(new String("Put(baz, boo)@102" +
                        "Delete(box)@101" +
                        "Put(foo, bar)@100")
                .equals(new String(getContents(batch), "US-ASCII")));
    } catch (UnsupportedEncodingException e) {
      System.err.println(e);
      assert(false);
    }
  }

  static void Append() {
    WriteBatch b1 = new WriteBatch();
    WriteBatch b2 = new WriteBatch();
    WriteBatchInternal.setSequence(b1, 200);
    WriteBatchInternal.setSequence(b2, 300);
    WriteBatchInternal.append(b1, b2);
    assert(getContents(b1).length == 0);
    assert(b1.count() == 0);
    try {
      b2.put("a".getBytes("US-ASCII"), "va".getBytes("US-ASCII"));
      WriteBatchInternal.append(b1, b2);
      assert("Put(a, va)@200".equals(new String(getContents(b1), "US-ASCII")));
      assert(1 == b1.count());
      b2.clear();
      b2.put("b".getBytes("US-ASCII"), "vb".getBytes("US-ASCII"));
      WriteBatchInternal.append(b1, b2);
      assert(new String("Put(a, va)@200" +
                        "Put(b, vb)@201")
                .equals(new String(getContents(b1), "US-ASCII")));
      assert(2 == b1.count());
      b2.remove("foo".getBytes("US-ASCII"));
      WriteBatchInternal.append(b1, b2);
      assert(new String("Put(a, va)@200" +
                        "Put(b, vb)@202" +
                        "Put(b, vb)@201" +
                        "Delete(foo)@203")
                 .equals(new String(getContents(b1), "US-ASCII")));
      assert(4 == b1.count());
    } catch (UnsupportedEncodingException e) {
      System.err.println(e);
      assert(false);
    }
  }

  static void Blob() {
    WriteBatch batch = new WriteBatch();
    try {
      batch.put("k1".getBytes("US-ASCII"), "v1".getBytes("US-ASCII"));
      batch.put("k2".getBytes("US-ASCII"), "v2".getBytes("US-ASCII"));
      batch.put("k3".getBytes("US-ASCII"), "v3".getBytes("US-ASCII"));
      batch.putLogData("blob1".getBytes("US-ASCII"));
      batch.remove("k2".getBytes("US-ASCII"));
      batch.putLogData("blob2".getBytes("US-ASCII"));
      batch.merge("foo".getBytes("US-ASCII"), "bar".getBytes("US-ASCII"));
      assert(5 == batch.count());
      assert(new String("Merge(foo, bar)@4" +
                        "Put(k1, v1)@0" +
                        "Delete(k2)@3" +
                        "Put(k2, v2)@1" +
                        "Put(k3, v3)@2")
                .equals(new String(getContents(batch), "US-ASCII")));
    } catch (UnsupportedEncodingException e) {
      System.err.println(e);
      assert(false);
    }
  }

  static native byte[] getContents(WriteBatch batch);
}
