package org.rocksdb;

import java.io.IOException;
import java.nio.file.Files;

public class SharedTempFileRocksDBMain {
    public static void main(final String[] args) throws IOException, RocksDBException, InterruptedException {

        RocksDB.loadLibrary();

        String tmpdir = Files.createTempDirectory("tmpRocksDBTestDir").toFile().getAbsolutePath();
        try (final RocksDB db = RocksDB.open(tmpdir)) {
            db.put("key".getBytes(), "value".getBytes());
        }
        Thread.sleep(10000);
    }

}
