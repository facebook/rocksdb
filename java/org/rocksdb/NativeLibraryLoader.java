package org.rocksdb;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.rocksdb.util.Environment;

/**
 * This class is used to load the RocksDB shared library from within the jar.
 * The shared library is extracted to a temp folder and loaded from there.
 */
public class NativeLibraryLoader {
  //singleton
  private static final NativeLibraryLoader instance = new NativeLibraryLoader();

  private static final String sharedLibraryName = Environment.getJniLibraryName("rocksdb");
  private static final String tempFilePrefix = "librocksdbjni";
  private static final String tempFileSuffix = "." + Environment.getJniLibraryExtension();

  /**
   * Get a reference to the NativeLibraryLoader
   *
   * @return The NativeLibraryLoader
   */
  public static NativeLibraryLoader getInstance() {
    return instance;
  }

  /**
   * Attempts to extract the native RocksDB library
   * from the classpath and load it
   *
   * @param tmpDir A temporary directory to use
   *   to copy the native library to. If null,
   *   or the empty string, we rely on Java's
   *   {@link java.io.File#createTempFile(String, String)}
   *   function to provide a temporary location.
   *   The temporary file will be registered for deletion
   *   on exit.
   *
   * @throws java.io.IOException if a filesystem operation fails.
   */
  public void loadLibraryFromJar(final String tmpDir)
      throws IOException {
    final File temp;
    if(tmpDir == null || tmpDir.equals("")) {
      temp = File.createTempFile(tempFilePrefix, tempFileSuffix);
    } else {
      temp = new File(tmpDir, sharedLibraryName);
    }

    if (!temp.exists()) {
      throw new RuntimeException("File " + temp.getAbsolutePath() + " does not exist.");
    } else {
      temp.deleteOnExit();
    }

    // attempt to copy the library from the Jar file to the temp destination
    try(final InputStream is = getClass().getClassLoader().getResourceAsStream(sharedLibraryName)) {
      if (is == null) {
        throw new RuntimeException(sharedLibraryName + " was not found inside JAR.");
      } else {
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }

    System.load(temp.getAbsolutePath());
  }
  /**
   * Private constructor to disallow instantiation
   */
  private NativeLibraryLoader() {
  }
}
