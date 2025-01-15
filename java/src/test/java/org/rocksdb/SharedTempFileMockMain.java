package org.rocksdb;

import org.rocksdb.util.SharedTempFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.SharedTempFileLoaderTest.compare;

public class SharedTempFileMockMain {

    public static void main(final String[] args) throws IOException, InterruptedException {

        // uncouple precise start time from other processes
        // otherwise they all create their own temp
        final Random random = new Random();
        Thread.sleep(random.nextInt(1000));

        String tmpDir = System.getProperty("java.io.tmpdir");

        SharedTempFile.Instance instance = new SharedTempFile.Instance(tmpDir, "rocksdbmock", "jnilib");
        SharedTempFile sharedTemp = instance.searchOrCreate();
        System.err.println(sharedTemp + " created/found");
        Path content;
        try (SharedTempFile.Lock ignored = sharedTemp.lock(SharedTempFileLoaderTest::mockContent)) {
            content = sharedTemp.getContent();
            assertThat(Files.exists(content)).isTrue();
            try (BufferedReader shared = new BufferedReader(new InputStreamReader(Files.newInputStream(content))); BufferedReader resource = SharedTempFileLoaderTest.mockContentReader()) {
                compare(resource, shared);
            }
            Thread.sleep(random.nextInt(1000));
        }
        System.err.println(sharedTemp + " finished");

        Thread.sleep(2000);
    }

}
