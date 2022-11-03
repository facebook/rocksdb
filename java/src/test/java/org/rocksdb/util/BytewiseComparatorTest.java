// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.rocksdb.util.ByteUtil.bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.AbstractComparator;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksIteratorInterface;
import org.rocksdb.RocksNativeLibraryResource;
import org.rocksdb.WriteOptions;

/**
 * This is a direct port of various C++
 * tests from db/comparator_db_test.cc
 * and some code to adapt it to RocksJava
 */
@SuppressWarnings({"ObjectAllocationInLoop", "MultipleExceptionsDeclaredOnTestMethod"})
public class BytewiseComparatorTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  private final List<String> source_strings = Arrays.asList("b", "d", "f", "h", "j", "l");
  private final List<String> interleaving_strings =
      Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m");

  /**
   * Open the database using the C++ BytewiseComparatorImpl
   * and test the results against our Java BytewiseComparator
   */
  @Test
  public void java_vs_cpp_bytewiseComparator()
      throws IOException, RocksDBException {
    for(int rand_seed = 301; rand_seed < 306; rand_seed++) {
      final Path dbDir =
          FileSystems.getDefault().getPath(dbFolder.newFolder().getAbsolutePath());
      try(final RocksDB db = openDatabase(dbDir,
          BuiltinComparator.BYTEWISE_COMPARATOR)) {
        final Random rnd = new SecureRandom(
            new byte[] {(byte) (rand_seed & 0xff), (byte) (rand_seed & 0xff00 >> 8)});
        try(final ComparatorOptions copt2 = new ComparatorOptions()
            .setUseDirectBuffer(false);
            final AbstractComparator comparator2 = new BytewiseComparator(copt2)) {
          final java.util.Comparator<String> jComparator = toJavaComparator(comparator2);
          doRandomIterationTest(db, jComparator, rnd);
        }
      }
    }
  }

  /**
   * Open the database using the Java BytewiseComparator
   * and test the results against another Java BytewiseComparator
   */
  @Test
  public void java_vs_java_bytewiseComparator()
      throws IOException, RocksDBException {
    for(int rand_seed = 301; rand_seed < 306; rand_seed++) {
      final Path dbDir =
          FileSystems.getDefault().getPath(dbFolder.newFolder().getAbsolutePath());
      try(final ComparatorOptions copt = new ComparatorOptions()
          .setUseDirectBuffer(false);
          final AbstractComparator comparator = new BytewiseComparator(copt);
          final RocksDB db = openDatabase(dbDir, comparator)) {
        final Random rnd = new SecureRandom(
            new byte[] {(byte) (rand_seed & 0xff), (byte) (rand_seed & 0xff00 >> 8)});
        try(final ComparatorOptions copt2 = new ComparatorOptions()
            .setUseDirectBuffer(false);
            final AbstractComparator comparator2 = new BytewiseComparator(copt2)) {
          final java.util.Comparator<String> jComparator = toJavaComparator(comparator2);
          doRandomIterationTest(db, jComparator, rnd);
        }
      }
    }
  }

  /**
   * Open the database using the C++ BytewiseComparatorImpl
   * and test the results against our Java DirectBytewiseComparator
   */
  @Test
  public void java_vs_cpp_directBytewiseComparator()
      throws IOException, RocksDBException {
    for(int rand_seed = 301; rand_seed < 306; rand_seed++) {
      final Path dbDir =
          FileSystems.getDefault().getPath(dbFolder.newFolder().getAbsolutePath());
      try(final RocksDB db = openDatabase(dbDir,
          BuiltinComparator.BYTEWISE_COMPARATOR)) {
        final Random rnd = new SecureRandom(
            new byte[] {(byte) (rand_seed & 0xff), (byte) (rand_seed & 0xff00 >> 8)});
        try(final ComparatorOptions copt2 = new ComparatorOptions()
              .setUseDirectBuffer(true);
            final AbstractComparator comparator2 = new BytewiseComparator(copt2)) {
          final java.util.Comparator<String> jComparator = toJavaComparator(comparator2);
          doRandomIterationTest(db, jComparator, rnd);
        }
      }
    }
  }

  /**
   * Open the database using the Java DirectBytewiseComparator
   * and test the results against another Java DirectBytewiseComparator
   */
  @Test
  public void java_vs_java_directBytewiseComparator()
      throws IOException, RocksDBException {
    for(int rand_seed = 301; rand_seed < 306; rand_seed++) {
      final Path dbDir =
          FileSystems.getDefault().getPath(dbFolder.newFolder().getAbsolutePath());
      try (final ComparatorOptions copt = new ComparatorOptions()
           .setUseDirectBuffer(true);
          final AbstractComparator comparator = new BytewiseComparator(copt);
          final RocksDB db = openDatabase(dbDir, comparator)) {
        final Random rnd = new SecureRandom(
            new byte[] {(byte) (rand_seed & 0xff), (byte) (rand_seed & 0xff00 >> 8)});
        try(final ComparatorOptions copt2 = new ComparatorOptions()
              .setUseDirectBuffer(true);
            final AbstractComparator comparator2 = new BytewiseComparator(copt2)) {
          final java.util.Comparator<String> jComparator = toJavaComparator(comparator2);
          doRandomIterationTest(db, jComparator, rnd);
        }
      }
    }
  }

  /**
   * Open the database using the C++ ReverseBytewiseComparatorImpl
   * and test the results against our Java ReverseBytewiseComparator
   */
  @Test
  public void java_vs_cpp_reverseBytewiseComparator()
      throws IOException, RocksDBException {
    for(int rand_seed = 301; rand_seed < 306; rand_seed++) {
      final Path dbDir =
          FileSystems.getDefault().getPath(dbFolder.newFolder().getAbsolutePath());
      try(final RocksDB db = openDatabase(dbDir,
          BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR)) {
        final Random rnd = new SecureRandom(
            new byte[] {(byte) (rand_seed & 0xff), (byte) (rand_seed & 0xff00 >> 8)});
        try(final ComparatorOptions copt2 = new ComparatorOptions()
            .setUseDirectBuffer(false);
            final AbstractComparator comparator2 = new ReverseBytewiseComparator(copt2)) {
          final java.util.Comparator<String> jComparator = toJavaComparator(comparator2);
          doRandomIterationTest(db, jComparator, rnd);
        }
      }
    }
  }

  /**
   * Open the database using the Java ReverseBytewiseComparator
   * and test the results against another Java ReverseBytewiseComparator
   */
  @Test
  public void java_vs_java_reverseBytewiseComparator()
      throws IOException, RocksDBException {
    for(int rand_seed = 301; rand_seed < 306; rand_seed++) {
      final Path dbDir =
          FileSystems.getDefault().getPath(dbFolder.newFolder().getAbsolutePath());
      try (final ComparatorOptions copt = new ComparatorOptions()
           .setUseDirectBuffer(false);
           final AbstractComparator comparator = new ReverseBytewiseComparator(copt);
           final RocksDB db = openDatabase(dbDir, comparator)) {
        final Random rnd = new SecureRandom(
            new byte[] {(byte) (rand_seed & 0xff), (byte) (rand_seed & 0xff00 >> 8)});
        try(final ComparatorOptions copt2 = new ComparatorOptions()
            .setUseDirectBuffer(false);
            final AbstractComparator comparator2 = new ReverseBytewiseComparator(copt2)) {
          final java.util.Comparator<String> jComparator = toJavaComparator(comparator2);
          doRandomIterationTest(db, jComparator, rnd);
        }
      }
    }
  }

  private void doRandomIterationTest(final RocksDB db, final Comparator<String> javaComparator,
      final Random rnd) throws RocksDBException {
    final int num_writes = 8;
    final int num_iter_ops = 100;
    final int num_trigger_flush = 3;
    final TreeMap<String, String> map = new TreeMap<>(javaComparator);

    try (final FlushOptions flushOptions = new FlushOptions();
         final WriteOptions writeOptions = new WriteOptions()) {
      for (int i = 0; i < num_writes; i++) {
        if (i != 0 && i % num_trigger_flush == 0) {
          db.flush(flushOptions);
        }

        final int type = rnd.nextInt(2);
        final int index = rnd.nextInt(source_strings.size());
        final String key = source_strings.get(index);
        switch (type) {
          case 0:
            // put
            map.put(key, key);
            db.put(writeOptions, bytes(key), bytes(key));
            break;
          case 1:
            // delete
            map.remove(key);
            db.delete(writeOptions, bytes(key));
            break;

          default:
            fail("Should not be able to generate random outside range 1..2");
        }
      }
    }

    try (final ReadOptions readOptions = new ReadOptions();
         final RocksIterator iter = db.newIterator(readOptions)) {
      final KVIter<String, String> result_iter = new KVIter<>(map);

      boolean is_valid = false;
      for (int i = 0; i < num_iter_ops; i++) {
        // Random walk and make sure iter and result_iter returns the
        // same key and value
        final int type = rnd.nextInt(8);
        iter.status();
        switch (type) {
          case 0:
            // Seek to First
            iter.seekToFirst();
            result_iter.seekToFirst();
            break;
          case 1:
            // Seek to last
            iter.seekToLast();
            result_iter.seekToLast();
            break;
          case 2: {
            // Seek to random (existing or non-existing) key
            final int key_idx = rnd.nextInt(interleaving_strings.size());
            final String key = interleaving_strings.get(key_idx);
            iter.seek(bytes(key));
            result_iter.seek(bytes(key));
            break;
          }
          case 3: {
            // SeekForPrev to random (existing or non-existing) key
            final int key_idx = rnd.nextInt(interleaving_strings.size());
            final String key = interleaving_strings.get(key_idx);
            iter.seekForPrev(bytes(key));
            result_iter.seekForPrev(bytes(key));
            break;
          }
          case 4:
            // Next
            if (is_valid) {
              iter.next();
              result_iter.next();
            } else {
              // noinspection ContinueStatement
              continue;
            }
            break;
          case 5:
            // Prev
            if (is_valid) {
              iter.prev();
              result_iter.prev();
            } else {
              // noinspection ContinueStatement
              continue;
            }
            break;
          case 6:
            // Refresh
            iter.refresh();
            result_iter.refresh();
            iter.seekToFirst();
            result_iter.seekToFirst();
            break;
          default: {
            // noinspection ConstantConditions
            assert (type == 7);
            final int key_idx = rnd.nextInt(source_strings.size());
            final String key = source_strings.get(key_idx);
            final byte[] result = db.get(readOptions, bytes(key));
            if (map.containsKey(key)) {
              assertThat(result).containsExactly(bytes(map.get(key)));
            } else {
              assertThat(result).isNull();
            }
            break;
          }
        }

        assertThat(result_iter.isValid()).isEqualTo(iter.isValid());

        is_valid = iter.isValid();

        if (is_valid) {
          assertThat(bytes(result_iter.key())).containsExactly(iter.key());

          //note that calling value on a non-valid iterator from the Java API
          //results in a SIGSEGV
          assertThat(bytes(result_iter.value())).containsExactly(iter.value());
        }
      }
    }
  }

  /**
   * Open the database using a C++ Comparator
   */
  private static RocksDB openDatabase(final Path dbDir, final BuiltinComparator cppComparator)
      throws RocksDBException {
    final Options options = new Options()
        .setCreateIfMissing(true)
        .setComparator(cppComparator);
    return RocksDB.open(options, dbDir.toAbsolutePath().toString());
  }

  /**
   * Open the database using a Java Comparator
   */
  private static RocksDB openDatabase(final Path dbDir, final AbstractComparator javaComparator)
      throws RocksDBException {
    final Options options = new Options()
        .setCreateIfMissing(true)
        .setComparator(javaComparator);
    return RocksDB.open(options, dbDir.toAbsolutePath().toString());
  }

  private static java.util.Comparator<String> toJavaComparator(
      final AbstractComparator rocksComparator) {
    return (s1, s2) -> {
      final ByteBuffer bufS1;
      final ByteBuffer bufS2;
      if (rocksComparator.usingDirectBuffers()) {
        bufS1 = ByteBuffer.allocateDirect(s1.length());
        bufS2 = ByteBuffer.allocateDirect(s2.length());
      } else {
        bufS1 = ByteBuffer.allocate(s1.length());
        bufS2 = ByteBuffer.allocate(s2.length());
      }
      bufS1.put(bytes(s1));
      bufS1.flip();
      bufS2.put(bytes(s2));
      bufS2.flip();
      return rocksComparator.compare(bufS1, bufS2);
    };
  }

  private static class KVIter<K, V> implements RocksIteratorInterface {

    private final List<Map.Entry<K, V>> entries;
    private final java.util.Comparator<? super K> comparator;
    private int offset = -1;

    private KVIter(final TreeMap<K, V> map) {
      this.entries = new ArrayList<>();
      entries.addAll(map.entrySet());
      this.comparator = map.comparator();
    }

    @Override
    public boolean isValid() {
      return offset > -1 && offset < entries.size();
    }

    @Override
    public void seekToFirst() {
      offset = 0;
    }

    @Override
    public void seekToLast() {
      offset = entries.size() - 1;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void seek(final byte[] target) {
      for(offset = 0; offset < entries.size(); offset++) {
        if(comparator.compare(entries.get(offset).getKey(),
            (K)new String(target, UTF_8)) >= 0) {
          return;
        }
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void seekForPrev(final byte[] target) {
      for(offset = entries.size()-1; offset >= 0; offset--) {
        if(comparator.compare(entries.get(offset).getKey(),
            (K)new String(target, UTF_8)) <= 0) {
          return;
        }
      }
    }

    /**
     * Is `a` a prefix of `b`
     *
     * @return The length of the matching prefix, or 0 if it is not a prefix
     */
    @SuppressWarnings("unused")
    private static int isPrefix(final byte[] a, final byte[] b) {
      if(b.length >= a.length) {
        for(int i = 0; i < a.length; i++) {
          if(a[i] != b[i]) {
            return i;
          }
        }
        return a.length;
      } else {
        return 0;
      }
    }

    @Override
    public void next() {
      if(offset < entries.size()) {
        offset++;
      }
    }

    @Override
    public void prev() {
      if(offset >= 0) {
        offset--;
      }
    }

    @Override
    public void refresh() {
      offset = -1;
    }

    @Override
    public void status() throws RocksDBException {
      if(offset < 0 || offset >= entries.size()) {
        throw new RocksDBException(MessageFormat.format(
            "Index out of bounds. Size is: {0}, offset is: {1}", entries.size(), offset));
      }
    }

    @SuppressWarnings("unchecked")
    public K key() {
      if (isValid()) {
        return entries.get(offset).getKey();
      } else {
        // noinspection IfStatementWithTooManyBranches
        if (entries.isEmpty()) {
          return (K) "";
        } else if (offset == -1) {
          return entries.get(0).getKey();
        } else if (offset == entries.size()) {
          return entries.get(offset - 1).getKey();
        } else {
          return (K) "";
        }
      }
    }

    @SuppressWarnings("unchecked")
    public V value() {
      if (isValid()) {
        return entries.get(offset).getValue();
      } else {
        return (V) "";
      }
    }

    @Override
    public void seek(final ByteBuffer target) {
      throw new IllegalAccessError("Not implemented");
    }

    @Override
    public void seekForPrev(final ByteBuffer target) {
      throw new IllegalAccessError("Not implemented");
    }
  }
}
