package org.rocksdb.util;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.rocksdb.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class JNIComparatorTest {

  @Parameters(name = "{0}")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        { "bytewise_non-direct", BuiltinComparator.BYTEWISE_COMPARATOR, false },
        { "bytewise_direct", BuiltinComparator.BYTEWISE_COMPARATOR, true },
        { "reverse-bytewise_non-direct", BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR, false },
        { "reverse-bytewise_direct", BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR, true },
    });
  }

  @Parameter(0)
  public String name;

  @Parameter(1)
  public BuiltinComparator builtinComparator;

  @Parameter(2)
  public boolean useDirectBuffer;

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  private static final int MIN = Short.MIN_VALUE - 1;
  private static final int MAX = Short.MAX_VALUE + 1;

  @Test
  public void java_comparator_equals_cpp_comparator() throws RocksDBException, IOException {
    final int[] javaKeys;
    try (final ComparatorOptions comparatorOptions = new ComparatorOptions();
         final AbstractComparator comparator = builtinComparator == BuiltinComparator.BYTEWISE_COMPARATOR
             ? new BytewiseComparator(comparatorOptions)
             : new ReverseBytewiseComparator(comparatorOptions)) {
      final Path javaDbDir =
          FileSystems.getDefault().getPath(dbFolder.newFolder().getAbsolutePath());
      storeWithJavaComparator(javaDbDir, comparator);
      javaKeys = readAllWithJavaComparator(javaDbDir, comparator);
    }

    final Path cppDbDir =
        FileSystems.getDefault().getPath(dbFolder.newFolder().getAbsolutePath());
    storeWithCppComparator(cppDbDir, builtinComparator);
    final int[] cppKeys =
        readAllWithCppComparator(cppDbDir, builtinComparator);

    assertThat(javaKeys).isEqualTo(cppKeys);
  }

  private void storeWithJavaComparator(final Path dir,
      final AbstractComparator comparator) throws RocksDBException {
    final ByteBuffer buf = ByteBuffer.allocate(4);
    try (final Options options = new Options()
             .setCreateIfMissing(true)
             .setComparator(comparator);
         final RocksDB db =
             RocksDB.open(options, dir.toAbsolutePath().toString())) {
      for (int i = MIN; i < MAX; i++) {
        buf.putInt(i);
        buf.flip();

        db.put(buf.array(), buf.array());

        buf.clear();
      }
    }
  }

  private void storeWithCppComparator(final Path dir,
      final BuiltinComparator builtinComparator) throws RocksDBException {
    try (final Options options = new Options()
             .setCreateIfMissing(true)
             .setComparator(builtinComparator);
         final RocksDB db =
             RocksDB.open(options, dir.toAbsolutePath().toString())) {

      final ByteBuffer buf = ByteBuffer.allocate(4);
      for (int i = MIN; i < MAX; i++) {
        buf.putInt(i);
        buf.flip();

        db.put(buf.array(), buf.array());

        buf.clear();
      }
    }
  }

  private int[] readAllWithJavaComparator(final Path dir,
      final AbstractComparator comparator) throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setComparator(comparator);
         final RocksDB db =
             RocksDB.open(options, dir.toAbsolutePath().toString())) {

      try (final RocksIterator it = db.newIterator()) {
        it.seekToFirst();

        final ByteBuffer buf = ByteBuffer.allocate(4);
        final int[] keys = new int[MAX - MIN];
        int idx = 0;
        while (it.isValid()) {
          buf.put(it.key());
          buf.flip();

          final int thisKey = buf.getInt();
          keys[idx++] = thisKey;

          buf.clear();

          it.next();
        }

        return keys;
      }
    }
  }

  private int[] readAllWithCppComparator(final Path dir,
      final BuiltinComparator comparator) throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setComparator(comparator);
         final RocksDB db =
             RocksDB.open(options, dir.toAbsolutePath().toString())) {

      try (final RocksIterator it = db.newIterator()) {
        it.seekToFirst();

        final ByteBuffer buf = ByteBuffer.allocate(4);
        final int[] keys = new int[MAX - MIN];
        int idx = 0;
        while (it.isValid()) {
          buf.put(it.key());
          buf.flip();

          final int thisKey = buf.getInt();
          keys[idx++] = thisKey;

          buf.clear();

          it.next();
        }

        return keys;
      }
    }
  }
}
