package org.rocksdb;

import org.junit.Test;
import org.rocksdb.util.SharedTempFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class LeakedSharedObjectTest {

    final static Random random = new Random();

    static InputStream mockContent() throws IOException {

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream("shared-temp-file.txt");
        return new BufferedInputStream(is);
    }

    static BufferedReader mockContentReader() {

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream("shared-temp-file.txt");
        return new BufferedReader(new InputStreamReader(is));
    }

    static void compare(BufferedReader expected, BufferedReader actual) throws IOException {
        int lines = 0;
        String expectedLine = expected.readLine();
        String actualLine = actual.readLine();
        while (expectedLine != null) {
            lines++;
            assertThat(actualLine).isEqualTo(expectedLine);
            expectedLine = expected.readLine();
            actualLine = actual.readLine();
        }
        assertThat(actualLine).isNull();
    }

    @Test
    public void sharedTempFresh() throws IOException {
        String prefix = "rocksdbjnitest__" + random.nextLong() + "__";
        SharedTempFile.Instance instance = new SharedTempFile.Instance(prefix, "jnilib");
        assertThat(instance.search()).isEmpty();
        SharedTempFile sharedTemp = instance.create();
        System.err.println(sharedTemp);
        Path content;
        try (SharedTempFile.Lock ignored = sharedTemp.lock(LeakedSharedObjectTest::mockContent)) {
            content = sharedTemp.getContent();
            assertThat(Files.exists(content)).isTrue();
            try (BufferedReader shared = new BufferedReader(new InputStreamReader(Files.newInputStream(content))); BufferedReader resource = mockContentReader()) {
                compare(resource, shared);
            }
        }
        assertThat(Files.exists(content)).isFalse();
    }

    @Test
    public void sharedTempExists() throws IOException {

        String prefix = "rocksdbjnitest__" + random.nextLong() + "__";
        SharedTempFile.Instance instance = new SharedTempFile.Instance(prefix, "jnilib");
        assertThat(instance.search()).isEmpty();
        SharedTempFile sharedTemp = instance.create();
        System.err.println("Created: " + sharedTemp);
        List<SharedTempFile> existing = instance.search();
        assertThat(existing).isNotEmpty();
        sharedTemp = existing.get(0);
        Path content;
        try (SharedTempFile.Lock ignored = sharedTemp.lock(LeakedSharedObjectTest::mockContent)) {
            content = sharedTemp.getContent();
            assertThat(Files.exists(content)).isTrue();
            try (BufferedReader shared = new BufferedReader(new InputStreamReader(Files.newInputStream(content))); BufferedReader resource = mockContentReader()) {
                compare(resource, shared);
            }
        }
        assertThat(Files.exists(content)).isFalse();
    }


    @Test
    public void openJar() {
        RocksDB.loadLibrary();
    }

    @Test
    public void openManySharedTemp() throws IOException {
        openMany("org.rocksdb.SharedTempFileMockMain");
    }

    @Test
    public void openManyRocksDB() throws IOException {
        openMany("org.rocksdb.SharedTempFileRocksDBMain");
    }

    public void openMany(final String mainClass) throws IOException {

        final int DB_COUNT = 50;
        List<Process> processes = new ArrayList<>();
        List<BufferedReader> readers = new ArrayList<>();
        List<List<String>> lines = new ArrayList<>();

        for (int i = 0; i < DB_COUNT; i++) {

            Process process = Runtime.getRuntime()
                .exec("java -cp target/classes:target/test-classes:target/*:test-libs/assertj-core-2.9.0.jar " + mainClass);
            processes.add(process);
            BufferedReader err = new BufferedReader( new InputStreamReader(process.getErrorStream()));
            readers.add(err);
            lines.add(new ArrayList<>());
        }

        for (int i = 0; i < DB_COUNT; i++) {
            lines.get(i).add("|--------|" + i + "|--------|");
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        boolean finished = false;
        while (!finished) {
            finished = true;
            for (int i = 0; i < DB_COUNT; i++) {
                if (readers.get(i).ready()) {
                    finished = false;
                    lines.get(i).add(readers.get(i).readLine());
                }
            }
        }

        for (Process process : processes) {
            process.destroyForcibly();
        }

        for (int i = 0; i < DB_COUNT; i++) {
            try {
                processes.get(i).waitFor();
                lines.get(i).add("Exit value " + processes.get(i).exitValue());
            } catch (InterruptedException ie) {
                throw new RuntimeException("Process interrupted");
            }
        }

        for (List<String> linei : lines) {
            for (String line : linei) {
                System.err.println(line);
            }
        }

    }

}
