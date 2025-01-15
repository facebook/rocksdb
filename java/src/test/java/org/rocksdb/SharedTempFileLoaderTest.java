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

public class SharedTempFileLoaderTest {

    private final static int SIGKILL_CODE = 128 + 9;
    private final static int SIGTERM_CODE = 128 + 15;

    final static Random random = new Random();

    static InputStream mockContent() {

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
        String expectedLine = expected.readLine();
        String actualLine = actual.readLine();
        while (expectedLine != null) {
            assertThat(actualLine).isEqualTo(expectedLine);
            expectedLine = expected.readLine();
            actualLine = actual.readLine();
        }
        assertThat(actualLine).isNull();
    }

    private final String tmpDir = System.getProperty("java.io.tmpdir");

    @Test
    public void sharedTempFresh() throws IOException {
        String prefix = "rocksdbjnitest__" + random.nextLong() + "__";
        SharedTempFile.Instance instance = new SharedTempFile.Instance(tmpDir, prefix, "jnilib");
        assertThat(instance.search()).isEmpty();
        SharedTempFile sharedTemp = instance.create();
        System.err.println(sharedTemp);
        Path content;
        try (SharedTempFile.Lock ignored = sharedTemp.lock(SharedTempFileLoaderTest::mockContent)) {
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
        SharedTempFile.Instance instance = new SharedTempFile.Instance(tmpDir, prefix, "jnilib");
        assertThat(instance.search()).isEmpty();
        SharedTempFile sharedTemp = instance.create();
        System.err.println("Created: " + sharedTemp);
        List<SharedTempFile> existing = instance.search();
        assertThat(existing).isNotEmpty();
        sharedTemp = existing.get(0);
        Path content;
        try (SharedTempFile.Lock ignored = sharedTemp.lock(SharedTempFileLoaderTest::mockContent)) {
            content = sharedTemp.getContent();
            assertThat(Files.exists(content)).isTrue();
            try (BufferedReader shared = new BufferedReader(new InputStreamReader(Files.newInputStream(content))); BufferedReader resource = mockContentReader()) {
                compare(resource, shared);
            }
        }
        assertThat(Files.exists(content)).isFalse();
    }

    @Test
    public void openManySharedTemp() throws IOException {
        openMany("org.rocksdb.SharedTempFileMockMain", Kill.None);
    }

    @Test
    public void openManyRocksDBKill() throws IOException {
        // Do this to make processes wait a long time, and we will kill them exp;icitly before they exit
        openMany("org.rocksdb.SharedTempFileRocksDBMain", Kill.Term, "--waitkill");
    }

    @Test
    public void openManyRocksDBWait() throws IOException {
        //Do this to (1) not kill the process and let them exit by themselves
        openMany("org.rocksdb.SharedTempFileRocksDBMain", Kill.None);
    }

    enum Kill {
        None,
        Term,
        Kill
    };

    /**
     * Helper to create multiples subprocesses and have them run a java class main
     *
     * @param mainClass java class to run main of
     * @param kill should we kill the processes after a short delay ?
     * @param args to pass to the main() method
     * @throws IOException if there is a problem creating/communication with the processes
     */
    private void openMany(final String mainClass, final Kill kill, final String... args) throws IOException {

        final int DB_COUNT = 50;
        List<Process> processes = new ArrayList<>();
        List<BufferedReader> readers = new ArrayList<>();
        List<List<String>> lines = new ArrayList<>();
        List<Integer> exitCodes = new ArrayList<>();

        for (int i = 0; i < DB_COUNT; i++) {

            StringBuilder sb = new StringBuilder();
            for (String arg : args) {
                sb.append(" ").append(arg);
            }
            Process process = Runtime.getRuntime()
                .exec("java -cp target/classes:target/test-classes:target/*:test-libs/assertj-core-2.9.0.jar " + mainClass + sb);
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

        int expectedExitCode;
        switch (kill) {
            case Kill:
                expectedExitCode = SIGKILL_CODE;
                break;
            case Term:
                expectedExitCode = SIGTERM_CODE;
                break;
            case None:
            default:
                expectedExitCode = 0;
        }
        for (Process process : processes) {
            switch (kill) {
                case Kill:
                    process.destroyForcibly();
                    break;
                case Term:
                    process.destroy();
                    break;
                case None:
                default:
                    break;
            }
        }

        for (int i = 0; i < DB_COUNT; i++) {
            try {
                processes.get(i).waitFor();
                int exitCode = processes.get(i).exitValue();
                exitCodes.add(exitCode);
            } catch (InterruptedException ie) {
                throw new RuntimeException("Process interrupted");
            }
        }

        for (int i = 0; i < DB_COUNT; i++) {
            lines.get(i).add("|------------------|");
        }

        boolean ok = true;
        for (int i = 0; i < DB_COUNT; i++) {
            if (exitCodes.get(i) != expectedExitCode) {
                ok = false;
                System.err.println("Process " + i + " failed: " + exitCodes.get(i));
                for (String line : lines.get(i)) {
                    System.err.println(line);
                }
            }
        }
        assertThat(ok).as("all subprocesses return success").isTrue();
    }

}
