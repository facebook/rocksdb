package org.rocksdb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class MultiGetTest {

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Test public void putNThenMultiGet() throws RocksDBException {

        try (final Options opt = new Options()
                .setCreateIfMissing(true);
             final RocksDB db = RocksDB.open(opt,
                     dbFolder.getRoot().getAbsolutePath())
        ) {
            db.put("key1".getBytes(), "value1ForKey1".getBytes());
            db.put("key2".getBytes(), "value2ForKey2".getBytes());
            db.put("key3".getBytes(), "value3ForKey3".getBytes());
            final List<byte[]> keys = Arrays.asList("key1".getBytes(), "key2".getBytes(), "key3".getBytes());
            final List<byte[]> values = db.multiGetAsList(keys);
            assertThat(values.size()).isEqualTo(keys.size());
            assertThat(values.get(0)).isEqualTo("value1ForKey1".getBytes());
            assertThat(values.get(1)).isEqualTo("value2ForKey2".getBytes());
            assertThat(values.get(2)).isEqualTo("value3ForKey3".getBytes());
        }
    }
}
