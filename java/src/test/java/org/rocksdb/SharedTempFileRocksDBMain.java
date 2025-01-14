package org.rocksdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;

public class SharedTempFileRocksDBMain {
    public static void main(final String[] args) throws InterruptedException {

        boolean waitKill = false;
        for (String arg : args) {
            if ("--waitkill".equals(arg)) waitKill = true;
        }

        try {
            RocksDB.loadLibrary();

            Path tmpDir = Files.createTempDirectory("tmpRocksDBTestDir");
            try (final RocksDB db = RocksDB.open(tmpDir.toFile().getAbsolutePath())) {
                db.put("key".getBytes(), "value".getBytes());
                if (!Arrays.equals("value".getBytes(), db.get("key".getBytes()))) {
                    System.err.println("Get: key != value");
                    System.exit(2);
                }
            } finally {
                purgeDirectory(tmpDir.toFile());
                Files.delete(tmpDir);
            }
        } catch (Exception e) {
            System.err.println("Exit with exception: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }

        if (waitKill) {
            Thread.sleep(5 * 60 * 1000);
        }

        System.exit(0);
    }

    static void purgeDirectory(File dir) {
        for (File file: dir.listFiles()) {
            if (file.isDirectory())
                purgeDirectory(file);
            file.delete();
        }
    }

}
