// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.rocksdb.*;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Random;

import static org.rocksdb.test.Types.byteToInt;
import static org.rocksdb.test.Types.intToByte;

/**
 * Abstract tests for both Comparator and DirectComparator
 */
public abstract class AbstractComparatorTest {

  /**
   * Get a comparator which will expect Integer keys
   * and determine an ascending order
   *
   * @return An integer ascending order key comparator
   */
  public abstract AbstractComparator getAscendingIntKeyComparator();

  /**
   * Test which stores random keys into the database
   * using an @see getAscendingIntKeyComparator
   * it then checks that these keys are read back in
   * ascending order
   *
   * @param db_path A path where we can store database
   *                files temporarily
   *
   * @throws java.io.IOException if IO error happens.
   */
  public void testRoundtrip(final Path db_path) throws IOException {

    Options opt = null;
    RocksDB db = null;

    try {
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setComparator(getAscendingIntKeyComparator());

      // store 10,000 random integer keys
      final int ITERATIONS = 10000;

      db = RocksDB.open(opt, db_path.toString());
      final Random random = new Random();
      for (int i = 0; i < ITERATIONS; i++) {
        final byte key[] = intToByte(random.nextInt());
        if (i > 0 && db.get(key) != null) { // does key already exist (avoid duplicates)
          i--; // generate a different key
        } else {
          db.put(key, "value".getBytes());
        }
      }
      db.close();


      // re-open db and read from start to end
      // integer keys should be in ascending
      // order as defined by SimpleIntComparator
      db = RocksDB.open(opt, db_path.toString());
      final RocksIterator it = db.newIterator();
      it.seekToFirst();
      int lastKey = Integer.MIN_VALUE;
      int count = 0;
      for (it.seekToFirst(); it.isValid(); it.next()) {
        final int thisKey = byteToInt(it.key());
        assert(thisKey > lastKey);
        lastKey = thisKey;
        count++;
      }
      db.close();

      assert(count == ITERATIONS);

    } catch (final RocksDBException e) {
      System.err.format("[ERROR]: %s%n", e);
      e.printStackTrace();
    } finally {
      if (db != null) {
        db.close();
      }

      if (opt != null) {
        opt.dispose();
      }

      removeDb(db_path); // cleanup after ourselves!
    }
  }

  /**
   * Compares integer keys
   * so that they are in ascending order
   *
   * @param a 4-bytes representing an integer key
   * @param b 4-bytes representing an integer key
   *
   * @return negative if a &lt; b, 0 if a == b, positive otherwise
   */
  protected final int compareIntKeys(final byte[] a, final byte[] b) {

    final int iA = byteToInt(a);
    final int iB = byteToInt(b);

    // protect against int key calculation overflow
    final double diff = (double)iA - iB;
    final int result;
    if (diff < Integer.MIN_VALUE) {
      result = Integer.MIN_VALUE;
    } else if(diff > Integer.MAX_VALUE) {
      result = Integer.MAX_VALUE;
    } else {
      result = (int)diff;
    }

    return result;
  }

  /**
   * Utility method for deleting database files
   *
   * @param db_path The path to the database to remove
   *                from the filesystem
   */
  private static void removeDb(final Path db_path) throws IOException {
    Files.walkFileTree(db_path, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
          throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(final Path file, IOException exc)
          throws IOException {
        // try to delete the file anyway, even if its attributes
        // could not be read, since delete-only access is
        // theoretically possible
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(final Path dir, IOException exc)
          throws IOException {
        if (exc == null) {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        } else {
          // directory iteration failed; propagate exception
          throw exc;
        }
      }
    });
  }
}
