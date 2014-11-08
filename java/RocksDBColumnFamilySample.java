// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;

public class RocksDBColumnFamilySample {
  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args) throws RocksDBException {
    if (args.length < 1) {
      System.out.println(
          "usage: RocksDBColumnFamilySample db_path");
      return;
    }
    String db_path = args[0];

    System.out.println("RocksDBColumnFamilySample");
    RocksDB db = null;
    DBOptions dbOptions = null;
    List<RocksIterator> iterators = new ArrayList<>();
    RocksIterator iterator = null;
    ColumnFamilyHandle cfHandle = null;
    WriteBatch wb = null;
    try {
      // Setup DBOptions
      dbOptions = new DBOptions().
          setCreateIfMissing(true).
          setCreateMissingColumnFamilies(true);
      // Setup ColumnFamily descriptors
      List<ColumnFamilyDescriptor> cfNames =
          new ArrayList<>();
      // Default column family
      cfNames.add(new ColumnFamilyDescriptor("default"));
      // New column families
      cfNames.add(new ColumnFamilyDescriptor("cf_green",
          new ColumnFamilyOptions().setComparator(
              BuiltinComparator.BYTEWISE_COMPARATOR)));
      cfNames.add(new ColumnFamilyDescriptor("cf_blue",
          new ColumnFamilyOptions().setComparator(
              BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR)));
      cfNames.add(new ColumnFamilyDescriptor("cf_red",
          new ColumnFamilyOptions().
              setMergeOperator(new StringAppendOperator())));

      List<ColumnFamilyHandle> cfHandles =
          new ArrayList<>();
      db = RocksDB.open(dbOptions,
          db_path, cfNames, cfHandles);
      // List column families in database
      System.out.println("List existent column families:");
      List<byte[]> cfListing = RocksDB.listColumnFamilies(
          new Options(), db_path);
      for (byte[] cf : cfListing) {
        System.out.format(" - %s\n", new String(cf));
      }
      // Bootstrapping values
      System.out.println("Writing values to database.");
      for (int i=0; i < cfNames.size(); i++) {
        for (int j=0; j < 10; j++) {
          db.put(cfHandles.get(i),
              String.valueOf(j).getBytes(),
              String.valueOf(j).getBytes());
        }
      }
      // Retrieve values using get
      System.out.println("Retrieve values with get.");
      for (int i=0; i < cfNames.size(); i++) {
        for (int j=0; j < 10; j++) {
          System.out.format(" %s", new String(
              db.get(cfHandles.get(i),
                  String.valueOf(j).getBytes())));
        }
        System.out.println("");
      }
      // Add a new column family to existing database
      System.out.println("Add new column family");
      cfHandle = db.createColumnFamily(new ColumnFamilyDescriptor(
          "cf_temp", new ColumnFamilyOptions().
          setMergeOperator(new StringAppendOperator())));
      System.out.println("Write key/value into new column family.");
      db.put(cfHandle, "key".getBytes(), "value".getBytes());
      System.out.format("Lookup 'key' retrieved value: %s\n", new String(
          db.get(cfHandle, "key".getBytes())));
      // Delete key
      System.out.println("Delete key/value in new column family.");
      db.remove(cfHandle, "key".getBytes());
      // WriteBatch with column family
      wb = new WriteBatch();
      wb.put(cfHandle, "key".getBytes(), "value".getBytes());
      wb.put(cfHandle, "key2".getBytes(), "value2".getBytes());
      wb.remove(cfHandle, "key2".getBytes());
      wb.merge(cfHandle, "key".getBytes(), "morevalues".getBytes());
      db.write(new WriteOptions(), wb);
      // Retrieve a single iterator with a cf handle
      System.out.println("Retrieve values using a iterator on" +
          " a column family.");
      iterator = db.newIterator(cfHandle);
      iterator.seekToFirst();
      while(iterator.isValid()) {
        System.out.format(" %s", new String(
            iterator.value()));
        iterator.next();
      }
      System.out.println("");
      // Delete column family
      System.out.println("Delete column family.");
      db.dropColumnFamily(cfHandle);
      // Retrieve values from cf using iterator
      System.out.println("Retrieve values with iterators");
      iterators = db.newIterators(cfHandles);
      assert(iterators.size() == 4);
      for (RocksIterator iter : iterators) {
        iter.seekToFirst();
        while(iter.isValid()) {
          System.out.format(" %s", new String(
              iter.value()));
          iter.next();
        }
        System.out.println("");
      }
    } finally {
      if (db != null) {
        db.close();
      }
      if (dbOptions != null) {
        dbOptions.dispose();
      }
      if (iterator != null) {
        iterator.dispose();
      }
      for (RocksIterator iter : iterators) {
        iter.dispose();
      }
      if (wb != null) {
        wb.dispose();
      }
    }
  }
}
