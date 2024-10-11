package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

public class IteratorClosedDBTest {

    @ClassRule
    public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
        new RocksNativeLibraryResource();

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Test
    public void resourceIterators() throws RocksDBException {
        try (Options options = new Options().setCreateIfMissing(true); RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {

            byte[] key = {0x1};
            byte[] value = {0x2};
            db.put(key, value);

            try (RocksIterator it = db.newIterator()) {
                it.seekToFirst();
                assertThat(it.key()).isEqualTo(key);
                assertThat(it.value()).isEqualTo(value);

                it.next();
                assertThat(it.isValid()).isFalse();
            }
        }
    }

    @Test
    public void ownedIterators() throws RocksDBException {
        try (Options options = new Options().setCreateIfMissing(true); RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {

            byte[] key = {0x1};
            byte[] value = {0x2};
            db.put(key, value);

            RocksIterator it = db.newIterator();
            it.seekToFirst();
            assertThat(it.key()).isEqualTo(key);
            assertThat(it.value()).isEqualTo(value);

            it.next();
            assertThat(it.isValid()).isFalse();
        } //iterator is still open when we close the DB, C++ assertion in DEBUG_LEVEL=1
    }

    @Test
    public void shouldCrashJavaRocks() throws RocksDBException {
        try (Options options = new Options().setCreateIfMissing(true)) {
            RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
            byte[] key = {0x1};
            byte[] value = {0x2};
            db.put(key, value);

            RocksIterator it = db.newIterator();
            assertThat(it.isValid()).isFalse();
            it.seekToFirst();
            assertThat(it.isValid()).isTrue();

            // Exception here (assertion failure in C++) - when built with DEBUG_LEVEL=1
            // Outstanding iterator has a reference to the column family which is being closed
            db.close();

            assertThat(it.isValid()).isFalse();
            it.close();
        }
    }
}
