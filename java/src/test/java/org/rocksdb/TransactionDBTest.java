// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionDBTest {

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void open() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB tdb = TransactionDB.open(options, txnDbOptions,
                 dbFolder.getRoot().getAbsolutePath())) {
      assertThat(tdb).isNotNull();
    }
  }

  @Test
  public void open_columnFamilies() throws RocksDBException {
    try(final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions myCfOpts = new ColumnFamilyOptions()) {

      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("myCf".getBytes(), myCfOpts));

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      try (final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
           final TransactionDB tdb = TransactionDB.open(dbOptions, txnDbOptions,
               dbFolder.getRoot().getAbsolutePath(),
               columnFamilyDescriptors, columnFamilyHandles)) {
        try {
          assertThat(tdb).isNotNull();
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void beginTransaction() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB tdb = TransactionDB.open(options, txnDbOptions,
             dbFolder.getRoot().getAbsolutePath());
        final WriteOptions writeOptions = new WriteOptions()) {

      try(final Transaction txn = tdb.beginTransaction(writeOptions)) {
        assertThat(txn).isNotNull();
      }
    }
  }

  @Test
  public void beginTransaction_transactionOptions() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB tdb = TransactionDB.open(options, txnDbOptions,
             dbFolder.getRoot().getAbsolutePath());
         final WriteOptions writeOptions = new WriteOptions();
         final TransactionOptions txnOptions = new TransactionOptions()) {

      try(final Transaction txn = tdb.beginTransaction(writeOptions,
          txnOptions)) {
        assertThat(txn).isNotNull();
      }
    }
  }

  @Test
  public void beginTransaction_withOld() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB tdb = TransactionDB.open(options, txnDbOptions,
             dbFolder.getRoot().getAbsolutePath());
         final WriteOptions writeOptions = new WriteOptions()) {

      try(final Transaction txn = tdb.beginTransaction(writeOptions)) {
        final Transaction txnReused = tdb.beginTransaction(writeOptions, txn);
        assertThat(txnReused).isSameAs(txn);
      }
    }
  }

  @Test
  public void beginTransaction_withOld_transactionOptions()
      throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB tdb = TransactionDB.open(options, txnDbOptions,
             dbFolder.getRoot().getAbsolutePath());
         final WriteOptions writeOptions = new WriteOptions();
         final TransactionOptions txnOptions = new TransactionOptions()) {

      try(final Transaction txn = tdb.beginTransaction(writeOptions)) {
        final Transaction txnReused = tdb.beginTransaction(writeOptions,
            txnOptions, txn);
        assertThat(txnReused).isSameAs(txn);
      }
    }
  }
}
