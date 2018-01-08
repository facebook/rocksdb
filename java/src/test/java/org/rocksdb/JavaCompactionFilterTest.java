// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the JNI layer for AbstractJavaCompactionFilter
 */
public class JavaCompactionFilterTest {

  /**
   * Static test inputs
   */
  private final String originalValueStr = "foobar";
  private final byte[] originalValueBytes = originalValueStr.getBytes();
  private final String[] keys = {"key1", "key2", "key3", "key4"};

  /**
   * Dummy compaction filter that lets us control the decision reached in the FilterV2 method
   */
  private static class DummyCompactionFilter extends AbstractJavaCompactionFilter {

    public static final String changedValue = "foobaz";
    public static final String skipUntil = "key3";

    private CompactionDecision decision = CompactionDecision.kKeep;

    public void setDecision(CompactionDecision newDecision) {
      this.decision = newDecision;
    }

    @Override
    public CompactionOutput FilterV2(int level, DirectSlice key, CompactionValueType valueType, DirectSlice existingValue) {
      if(this.decision.equals(CompactionDecision.kKeep) || this.decision.equals(CompactionDecision.kRemove)) {
        return new CompactionOutput(this.decision);
      } else if (this.decision.equals(CompactionDecision.kChangeValue)) {
        return new CompactionOutput(this.decision, this.changedValue);
      } else {
        return new CompactionOutput(this.decision, this.skipUntil);
      }
    }

    @Override
    public String Name() {
      return "DummyCompactionFilter";
    }
  }

  private void putOriginalValues(RocksDB db) throws RocksDBException {
    for(String key : this.keys) {
      db.put(key.getBytes(), this.originalValueBytes);
    }
  }

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  /**
   * Tests AbstractJavaCompactionFilter
   */
  @Test
  public void testAbstractJavaCompactionFilter() throws IOException, RocksDBException {

    // Create a compaction filter to use
    final DummyCompactionFilter compactionFilter = new DummyCompactionFilter();

    // Set up rocks options, wire in the compaction filter and disable auto-compaction
    DBOptions dbOptions = new DBOptions()
        .setCreateIfMissing(true);
    final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()
        .setCompactionFilter(compactionFilter)
        .setDisableAutoCompactions(true);
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    // Create a database with our one column family
    RocksDB db = RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(),
        columnFamilyDescriptors, columnFamilyHandles);

    // Put original values
    putOriginalValues(db);

    // Set the decision to keep and trigger a compaction
    compactionFilter.setDecision(CompactionDecision.kKeep);
    db.compactRange();

    // Assert that we have all KV pairs we expected - all should've been kept
    for(String key : this.keys) {
      assertThat(db.get(key.getBytes())).isEqualTo(this.originalValueBytes);
    }

    // Set the decision to remove and trigger a compaction
    compactionFilter.setDecision(CompactionDecision.kRemove);
    db.compactRange();

    // Assert that we have none of the KV pairs from earlier - all should've been removed
    for(String key : this.keys) {
      assertThat(db.get(key.getBytes())).isNull();
    }

    // Put original values
    putOriginalValues(db);

    // Set the decision to change values and trigger a compaction
    compactionFilter.setDecision(CompactionDecision.kChangeValue);
    db.compactRange();

    // Assert that the values have all been changed
    for(String key : this.keys) {
      assertThat(db.get(key.getBytes())).isEqualTo(DummyCompactionFilter.changedValue.getBytes());
    }

    // Put original values
    putOriginalValues(db);

    // Set the decision to remove and skip until and trigger a compaction
    compactionFilter.setDecision(CompactionDecision.kRemoveAndSkipUntil);
    db.compactRange();

    // Assert that we removed all keys until the skip marker
    for(String key : this.keys) {
      if(key.compareTo(DummyCompactionFilter.skipUntil) < 0) {
        assertThat(db.get(key.getBytes())).isNull();
      } else {
        assertThat(db.get(key.getBytes())).isEqualTo(this.originalValueBytes);
      }
    }

  }

}
