package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
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
    public void simpleWriteArray() throws RocksDBException {
        try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {

            List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

            wideColumns.add(new WideColumn<>("columndName".getBytes(StandardCharsets.UTF_8), "columnValue".getBytes(StandardCharsets.UTF_8)));
            db.putEntity("someKey".getBytes(StandardCharsets.UTF_8), wideColumns);

        }
    }


    @Test
    public void simpleReadWriteDirectByteBuffer() throws RocksDBException {
        try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {

            List<WideColumn<ByteBuffer>> wideColumns = new ArrayList<>();

            ByteBuffer key = ByteBuffer.allocateDirect(100);
            ByteBuffer name = ByteBuffer.allocateDirect(100);
            ByteBuffer value = ByteBuffer.allocateDirect(100);

            key.put("someKey".getBytes(StandardCharsets.UTF_8));
            key.flip();

            name.put("columnName".getBytes(StandardCharsets.UTF_8));
            name.flip();

            value.put("columnValue".getBytes(StandardCharsets.UTF_8));
            value.flip();

            wideColumns.add(new WideColumn<>(name, value));
            db.putEntity(key, wideColumns);

            List<WideColumn<byte[]>> result = new ArrayList<>();

            Status status = db.getEntity("someKey".getBytes(), result);

            assertThat(status.getCode()).isEqualTo(Status.Code.Ok);

            assertThat(result).isNotEmpty();
            assertThat(result.get(0).getValue()).isEqualTo("columnValue".getBytes(StandardCharsets.UTF_8));
            assertThat(result.get(0).getName()).isEqualTo("columnName".getBytes(StandardCharsets.UTF_8));

        }

    }


    @Test
    public void getEntityDirect() throws RocksDBException {
        try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {

            List<WideColumn<byte[]>> wideColumns = new ArrayList<>();

            wideColumns.add(new WideColumn<>("columnName".getBytes(StandardCharsets.UTF_8), "columnValue".getBytes(StandardCharsets.UTF_8)));
            db.putEntity("someKey".getBytes(StandardCharsets.UTF_8), wideColumns);

            List<WideColumn.ByteBufferWideColumn> result = new ArrayList<>();
            result.add(new WideColumn.ByteBufferWideColumn(ByteBuffer.allocateDirect(10), ByteBuffer.allocateDirect(10)));
            ByteBuffer key = ByteBuffer.allocateDirect(20);
            key.put("someKey".getBytes(StandardCharsets.UTF_8));
            key.flip();

            Status s = db.getEntity(key, result);

            assertThat(s).isNotNull();
            assertThat(s.getCode()).isEqualTo(Status.Code.Ok);
            WideColumn.ByteBufferWideColumn column = result.get(0);
            assertThat(column.getNameRequiredSize()).isEqualTo(10);
            assertThat(column.getValueRequiredSize()).isEqualTo(11);

            ByteBuffer valueBuffer = column.getValue();
            assertThat(valueBuffer.position()).isEqualTo(valueBuffer.capacity());
            valueBuffer.flip();
            byte[] value = new byte[10];
            valueBuffer.get(value);
            assertThat(value).isEqualTo("columnValu".getBytes(StandardCharsets.UTF_8));


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
