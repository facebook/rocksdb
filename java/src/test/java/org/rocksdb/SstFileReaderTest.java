// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.rocksdb.util.ByteBufferAllocator;

@RunWith(Parameterized.class)
public class SstFileReaderTest {
  private static final String SST_FILE_NAME = "test.sst";

  static class KeyValueWithOp {
    KeyValueWithOp(final String key, final String value, final OpType opType) {
      this.key = key;
      this.value = value;
      this.opType = opType;
    }

    String getKey() {
      return key;
    }

    String getValue() {
      return value;
    }

    OpType getOpType() {
      return opType;
    }

    private final String key;
    private final String value;
    private final OpType opType;
  }

  @Rule public TemporaryFolder parentFolder = new TemporaryFolder();

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        {"direct", ByteBufferAllocator.DIRECT}, {"indirect", ByteBufferAllocator.HEAP}});
  }

  @Parameterized.Parameter() public String name;

  @Parameterized.Parameter(1) public ByteBufferAllocator byteBufferAllocator;

  enum OpType { PUT, PUT_BYTES, MERGE, MERGE_BYTES, DELETE, DELETE_BYTES }

  private File newSstFile(final List<KeyValueWithOp> keyValues)
      throws IOException, RocksDBException {
    final EnvOptions envOptions = new EnvOptions();
    final StringAppendOperator stringAppendOperator = new StringAppendOperator();
    final Options options = new Options().setMergeOperator(stringAppendOperator);
    final SstFileWriter sstFileWriter;
    sstFileWriter = new SstFileWriter(envOptions, options);

    final File sstFile = parentFolder.newFile(SST_FILE_NAME);
    try {
      sstFileWriter.open(sstFile.getAbsolutePath());
      for (final KeyValueWithOp keyValue : keyValues) {
        final Slice keySlice = new Slice(keyValue.getKey());
        final Slice valueSlice = new Slice(keyValue.getValue());
        final byte[] keyBytes = keyValue.getKey().getBytes();
        final byte[] valueBytes = keyValue.getValue().getBytes();
        switch (keyValue.getOpType()) {
          case PUT:
            sstFileWriter.put(keySlice, valueSlice);
            break;
          case PUT_BYTES:
            sstFileWriter.put(keyBytes, valueBytes);
            break;
          case MERGE:
            sstFileWriter.merge(keySlice, valueSlice);
            break;
          case MERGE_BYTES:
            sstFileWriter.merge(keyBytes, valueBytes);
            break;
          case DELETE:
            sstFileWriter.delete(keySlice);
            break;
          case DELETE_BYTES:
            sstFileWriter.delete(keyBytes);
            break;
          default:
            fail("Unsupported op type");
        }
        keySlice.close();
        valueSlice.close();
      }
      sstFileWriter.finish();
    } finally {
      assertThat(sstFileWriter).isNotNull();
      sstFileWriter.close();
      options.close();
      envOptions.close();
    }
    return sstFile;
  }

  @Test
  public void readSstFile() throws RocksDBException, IOException {
    final List<KeyValueWithOp> keyValues = new ArrayList<>();
    keyValues.add(new KeyValueWithOp("key1", "value1", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key2", "value2", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key3", "value3", OpType.PUT));

    final File sstFile = newSstFile(keyValues);
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options options =
             new Options().setCreateIfMissing(true).setMergeOperator(stringAppendOperator);
         final SstFileReader reader = new SstFileReader(options)) {
      // Open the sst file and iterator
      reader.open(sstFile.getAbsolutePath());
      final ReadOptions readOptions = new ReadOptions();
      final SstFileReaderIterator iterator = reader.newIterator(readOptions);

      // Use the iterator to read sst file
      iterator.seekToFirst();

      // Verify Checksum
      reader.verifyChecksum();

      // Verify Table Properties
      assertEquals(reader.getTableProperties().getNumEntries(), 3);

      // Check key and value
      assertThat(iterator.key()).isEqualTo("key1".getBytes());
      assertThat(iterator.value()).isEqualTo("value1".getBytes());

      final ByteBuffer byteBuffer = byteBufferAllocator.allocate(128);
      byteBuffer.put("key1".getBytes()).flip();
      iterator.seek(byteBuffer);
      assertThat(byteBuffer.position()).isEqualTo(4);
      assertThat(byteBuffer.limit()).isEqualTo(4);

      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key1".getBytes());
      assertThat(iterator.value()).isEqualTo("value1".getBytes());

      {
        byteBuffer.clear();
        assertThat(iterator.key(byteBuffer)).isEqualTo("key1".getBytes().length);
        final byte[] dst = new byte["key1".getBytes().length];
        byteBuffer.get(dst);
        assertThat(new String(dst)).isEqualTo("key1");
      }

      {
        byteBuffer.clear();
        byteBuffer.put("PREFIX".getBytes());
        final ByteBuffer slice = byteBuffer.slice();
        assertThat(iterator.key(byteBuffer)).isEqualTo("key1".getBytes().length);
        final byte[] dst = new byte["key1".getBytes().length];
        slice.get(dst);
        assertThat(new String(dst)).isEqualTo("key1");
      }

      {
        byteBuffer.clear();
        assertThat(iterator.value(byteBuffer)).isEqualTo("value1".getBytes().length);
        final byte[] dst = new byte["value1".getBytes().length];
        byteBuffer.get(dst);
        assertThat(new String(dst)).isEqualTo("value1");
      }

      byteBuffer.clear();
      byteBuffer.put("key1point5".getBytes()).flip();
      iterator.seek(byteBuffer);
      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key2".getBytes());
      assertThat(iterator.value()).isEqualTo("value2".getBytes());

      byteBuffer.clear();
      byteBuffer.put("key1point5".getBytes()).flip();
      iterator.seekForPrev(byteBuffer);
      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key1".getBytes());
      assertThat(iterator.value()).isEqualTo("value1".getBytes());

      byteBuffer.clear();
      byteBuffer.put("key2point5".getBytes()).flip();
      iterator.seek(byteBuffer);
      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key3".getBytes());
      assertThat(iterator.value()).isEqualTo("value3".getBytes());

      byteBuffer.clear();
      byteBuffer.put("key2point5".getBytes()).flip();
      iterator.seekForPrev(byteBuffer);
      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key2".getBytes());
      assertThat(iterator.value()).isEqualTo("value2".getBytes());

      byteBuffer.clear();
      byteBuffer.put("PREFIX".getBytes());
      final ByteBuffer slice = byteBuffer.slice();
      slice.put("key1point5".getBytes()).flip();
      iterator.seekForPrev(slice);
      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key1".getBytes());
      assertThat(iterator.value()).isEqualTo("value1".getBytes());
    }
  }

  private void assertInternalKey(final Options options, final ParsedEntryInfo parsedEntryInfo,
    final byte[] internalKey, final String expectedUserKey, final EntryType expectedEntryType) {
    parsedEntryInfo.parseEntry(options, internalKey);
    assertThat(expectedUserKey.getBytes()).isEqualTo(parsedEntryInfo.getUserKey());
    assertEquals(expectedEntryType, parsedEntryInfo.getEntryType());
  }

  private void seekTableIterator(final SstFileReaderIterator iterator, final ByteBuffer userKey,
                                 final ByteBuffer internalKey, final Options options) {
    int len = TypeUtil.getInternalKey(userKey, internalKey, options);
    assertEquals(len, internalKey.limit());
    iterator.seek(internalKey);
    assertThat(internalKey.position()).isEqualTo(len);
    assertThat(internalKey.limit()).isEqualTo(len);
    internalKey.clear();
  }

  private void seekTableIteratorForPrev(final SstFileReaderIterator iterator, final ByteBuffer userKey,
    final ByteBuffer internalKey, final Options options) {
    int len = TypeUtil.getInternalKeyForPrev(userKey, internalKey, options);
    assertEquals(len, internalKey.limit());
    iterator.seekForPrev(internalKey);
    assertThat(internalKey.position()).isEqualTo(len);
    assertThat(internalKey.limit()).isEqualTo(len);
    internalKey.clear();
  }

  @Test
  public void readSstFileTableIterator() throws RocksDBException, IOException {
    final List<KeyValueWithOp> keyValues = new ArrayList<>();
    keyValues.add(new KeyValueWithOp("key1", "value1", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key11", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key12", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key2", "value2", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key21", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key22", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key3", "value3", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key31", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key32", "", OpType.DELETE));

    final File sstFile = newSstFile(keyValues);
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options options =
             new Options().setCreateIfMissing(true).setMergeOperator(stringAppendOperator);
         final SstFileReader reader = new SstFileReader(options);
         final ParsedEntryInfo parsedEntryInfo = new ParsedEntryInfo()) {
      // Open the sst file and iterator
      reader.open(sstFile.getAbsolutePath());
      final ReadOptions readOptions = new ReadOptions();
      final SstFileReaderIterator iterator = reader.newTableIterator();

      // Use the iterator to read sst file
      iterator.seekToFirst();

      // Verify Checksum
      reader.verifyChecksum();

      // Verify Table Properties
      assertEquals(reader.getTableProperties().getNumEntries(), 9);

      // Check key and value
      assertInternalKey(options, parsedEntryInfo, iterator.key(), "key1", EntryType.kEntryPut);
      assertThat(iterator.value()).isEqualTo("value1".getBytes());

      final ByteBuffer userByteBuffer = byteBufferAllocator.allocate(128);
      final ByteBuffer internalKeyByteBuffer = byteBufferAllocator.allocate(128);
      userByteBuffer.put("key1".getBytes()).flip();
      seekTableIterator(iterator, userByteBuffer, internalKeyByteBuffer, options);
      assertThat(iterator.isValid()).isTrue();
      assertInternalKey(options, parsedEntryInfo, iterator.key(), "key1", EntryType.kEntryPut);
      assertThat(iterator.value()).isEqualTo("value1".getBytes());

      {
        userByteBuffer.clear();
        iterator.key(userByteBuffer);
        final byte[] dst = new byte["key1".getBytes().length];
        userByteBuffer.get(dst);
        assertInternalKey(options, parsedEntryInfo, dst, "key1", EntryType.kEntryPut);
      }

      {
        userByteBuffer.clear();
        userByteBuffer.put("PREFIX".getBytes());
        final ByteBuffer slice = userByteBuffer.slice();
        iterator.key(userByteBuffer);
        final byte[] dst = new byte["key1".getBytes().length];
        slice.get(dst);
        assertInternalKey(options, parsedEntryInfo, dst, "key1", EntryType.kEntryPut);
      }

      {
        userByteBuffer.clear();
        assertThat(iterator.value(userByteBuffer)).isEqualTo("value1".getBytes().length);
        final byte[] dst = new byte["value1".getBytes().length];
        userByteBuffer.get(dst);
        assertThat(new String(dst)).isEqualTo("value1");
      }

      userByteBuffer.clear();
      userByteBuffer.put("key10".getBytes()).flip();
      seekTableIterator(iterator, userByteBuffer, internalKeyByteBuffer, options);
      assertThat(iterator.isValid()).isTrue();
      assertInternalKey(options, parsedEntryInfo, iterator.key(), "key11", EntryType.kEntryDelete);

      userByteBuffer.clear();
      userByteBuffer.put("key1point5".getBytes()).flip();
      seekTableIteratorForPrev(iterator, userByteBuffer, internalKeyByteBuffer, options);
      assertThat(iterator.isValid()).isTrue();
      assertInternalKey(options, parsedEntryInfo, iterator.key(), "key12", EntryType.kEntryDelete);

      userByteBuffer.clear();
      userByteBuffer.put("key2point5".getBytes()).flip();
      seekTableIterator(iterator, userByteBuffer, internalKeyByteBuffer, options);
      assertThat(iterator.isValid()).isTrue();
      assertInternalKey(options, parsedEntryInfo, iterator.key(), "key3", EntryType.kEntryPut);
      assertThat(iterator.value()).isEqualTo("value3".getBytes());

      userByteBuffer.clear();
      userByteBuffer.put("key2point5".getBytes()).flip();
      seekTableIteratorForPrev(iterator, userByteBuffer, internalKeyByteBuffer, options);
      assertThat(iterator.isValid()).isTrue();
      assertInternalKey(options, parsedEntryInfo, iterator.key(), "key22", EntryType.kEntryDelete);

      userByteBuffer.clear();
      internalKeyByteBuffer.put("PREFIX".getBytes());
      final ByteBuffer slice = internalKeyByteBuffer.slice();
      userByteBuffer.put("key1point5".getBytes()).flip();
      seekTableIteratorForPrev(iterator, userByteBuffer, slice, options);
      iterator.seekForPrev(slice);
      assertThat(iterator.isValid()).isTrue();
      assertInternalKey(options, parsedEntryInfo, iterator.key(), "key12", EntryType.kEntryDelete);
    }
  }
}
