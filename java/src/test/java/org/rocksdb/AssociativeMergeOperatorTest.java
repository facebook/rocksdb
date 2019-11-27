//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class AssociativeMergeOperatorTest {
  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  private class StringAppendAssociativeMergeOperator extends AbstractAssociativeMergeOperator {

    public StringAppendAssociativeMergeOperator() throws RocksDBException { super(); }

    @Override
    public byte[] merge(byte[] key, byte[] oldvalue, byte[] newvalue, ReturnType rt) {
      if (oldvalue == null) {
        rt.isArgumentReference=true;
        return newvalue;
      }
      StringBuffer sb = new StringBuffer(new String(oldvalue));
      sb.append(',');
      sb.append(new String(newvalue));
      return sb.toString().getBytes();
    }

    @Override
    public String name() {
      return "StringAppendAssociativeMergeOperator";
    }
  }

  @Test
  public void mergeWithAbstractAssociativeOperator() throws RocksDBException, NoSuchMethodException, InterruptedException {
    try {
      try (final StringAppendAssociativeMergeOperator stringAppendOperator = new StringAppendAssociativeMergeOperator();
           final Options opt = new Options()
                     .setCreateIfMissing(true)
                     .setMergeOperator(stringAppendOperator);
           final WriteOptions wOpt = new WriteOptions();
           final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())
        ) {
          db.merge("key1".getBytes(), "value".getBytes());
          assertThat(db.get("key1".getBytes())).isEqualTo("value".getBytes());

          // merge key1 with another value portion
          db.merge("key1".getBytes(), "value2".getBytes());
          assertThat(db.get("key1".getBytes())).isEqualTo("value,value2".getBytes());

          // merge key1 with another value portion
          db.merge(wOpt, "key1".getBytes(), "value3".getBytes());
          assertThat(db.get("key1".getBytes())).isEqualTo("value,value2,value3".getBytes());
          db.merge(wOpt, "key1".getBytes(), "value4".getBytes());
          assertThat(db.get("key1".getBytes())).isEqualTo("value,value2,value3,value4".getBytes());

          // merge on non existent key shall insert the value
          db.merge(wOpt, "key2".getBytes(), "xxxx".getBytes());
          assertThat(db.get("key2".getBytes())).isEqualTo("xxxx".getBytes());
        }
      } catch (Exception e){
        throw new RuntimeException(e);
      } finally {
    }
  }
}