package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class WideColumnTest {


    @ClassRule
    public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
            new RocksNativeLibraryResource();

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Test
    public void simpleWrite() throws RocksDBException {
        try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {

            List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

            wideColumns.add(new WideColumn<>("columndName".getBytes(StandardCharsets.UTF_8), "columnValue".getBytes(StandardCharsets.UTF_8)));
            db.putEntity("someKey".getBytes(StandardCharsets.UTF_8), wideColumns);

        }
    }

    @Test
    public void simpleReadWrite() throws RocksDBException {
        try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {

            List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

            wideColumns.add(new WideColumn<>("columnName".getBytes(StandardCharsets.UTF_8), "columnValue".getBytes(StandardCharsets.UTF_8)));
            db.putEntity("someKey".getBytes(StandardCharsets.UTF_8), wideColumns);

            List<WideColumn<byte[]>> result = new ArrayList<>();

            Status status = db.getEntity("someKey".getBytes(), result);

            assertThat(status.getCode()).isEqualTo(Status.Code.Ok);

            assertThat(result).isNotEmpty();
            assertThat(result.get(0).getValue()).isEqualTo("columnValue".getBytes(StandardCharsets.UTF_8));
            assertThat(result.get(0).getName()).isEqualTo("columnName".getBytes(StandardCharsets.UTF_8));


        }
    }

    @Test
    public void simpleReadWriteWithColumnFamily() throws RocksDBException {
        try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {

            ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor("testColumnFamily".getBytes(StandardCharsets.UTF_8));

            try(ColumnFamilyHandle cfHandle = db.createColumnFamily(cfDescriptor)) {
                List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

                wideColumns.add(new WideColumn<>("columnName".getBytes(StandardCharsets.UTF_8), "columnValue".getBytes(StandardCharsets.UTF_8)));
                db.putEntity(cfHandle, "someKey".getBytes(StandardCharsets.UTF_8), wideColumns);

                List<WideColumn<byte[]>> result = new ArrayList<>();

                Status status = db.getEntity(cfHandle, "someKey".getBytes(), result);

                assertThat(status.getCode()).isEqualTo(Status.Code.Ok);

                assertThat(result).isNotEmpty();
                assertThat(result.get(0).getValue()).isEqualTo("columnValue".getBytes(StandardCharsets.UTF_8));
                assertThat(result.get(0).getName()).isEqualTo("columnName".getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    public void noResult() throws RocksDBException {
        try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {

            List<WideColumn<byte[]>> result = new ArrayList<>();

            Status status = db.getEntity("someKey".getBytes(), result);

            assertThat(status.getCode()).isEqualTo(Status.Code.NotFound);

            assertThat(result).isEmpty();

        }
    }



    public void largeReadWrite() throws RocksDBException {
        try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {

            List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

            for(int i = 100 ; i < 1000 ; i++) {
                wideColumns.add(new WideColumn<>(("columnName" + i).getBytes(StandardCharsets.UTF_8), ("columnValue" + i).getBytes(StandardCharsets.UTF_8)));
            }
            db.putEntity("someKey".getBytes(StandardCharsets.UTF_8), wideColumns);

            List<WideColumn<byte[]>> result = new ArrayList<>();

            Status status = db.getEntity("someKey".getBytes(), result);

            assertThat(status.getCode()).isEqualTo(Status.Code.Ok);

        }
    }






}
