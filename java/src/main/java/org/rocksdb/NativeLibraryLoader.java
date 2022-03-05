// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
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
  private static boolean initialized = false;

  private static final String sharedLibraryName = Environment.getSharedLibraryName("rocksdb");
  private static final String jniLibraryName = Environment.getJniLibraryName("rocksdb");
  private static final /* @Nullable */ String fallbackJniLibraryName =
      Environment.getFallbackJniLibraryName("rocksdb");
  private static final String jniLibraryFileName = Environment.getJniLibraryFileName("rocksdb");
  private static final /* @Nullable */ String fallbackJniLibraryFileName =
      Environment.getFallbackJniLibraryFileName("rocksdb");
  private static final String tempFilePrefix = "librocksdbjni";
  private static final String tempFileSuffix = Environment.getJniLibraryExtension();

  /**
   * Get a reference to the NativeLibraryLoader
   *
   * @return The NativeLibraryLoader
   */
  public static NativeLibraryLoader getInstance() {
    return instance;
  }

  /**
   * Firstly attempts to load the library from <i>java.library.path</i>,
   * if that fails then it falls back to extracting
   * the library from the classpath
   * {@link org.rocksdb.NativeLibraryLoader#loadLibraryFromJar(java.lang.String)}
   *
   * @param tmpDir A temporary directory to use
   *   to copy the native library to when loading from the classpath.
   *   If null, or the empty string, we rely on Java's
   *   {@link java.io.File#createTempFile(String, String)}
   *   function to provide a temporary location.
   *   The temporary file will be registered for deletion
   *   on exit.
   *
   * @throws java.io.IOException if a filesystem operation fails.
   */
  public synchronized void loadLibrary(final String tmpDir) throws IOException {
    try {
      // try dynamic library
      System.loadLibrary(sharedLibraryName);
      return;
    } catch (final UnsatisfiedLinkError ule) {
      // ignore - try from static library
    }

    try {
      // try static library
      System.loadLibrary(jniLibraryName);
      return;
    } catch (final UnsatisfiedLinkError ule) {
      // ignore - then try static library fallback or from jar
    }

    if (fallbackJniLibraryName != null) {
      try {
        // try static library fallback
        System.loadLibrary(fallbackJniLibraryName);
        return;
      } catch (final UnsatisfiedLinkError ule) {
        // ignore - then try from jar
      }
    }

    // try jar
    loadLibraryFromJar(tmpDir);
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
  void loadLibraryFromJar(final String tmpDir)
      throws IOException {
    if (!initialized) {
      System.load(loadLibraryFromJarToTemp(tmpDir).getAbsolutePath());
      initialized = true;
    }
  }

  File loadLibraryFromJarToTemp(final String tmpDir)
          throws IOException {
    InputStream is = null;
    try {
      // attempt to look up the static library in the jar file
      String libraryFileName = jniLibraryFileName;
      is = getClass().getClassLoader().getResourceAsStream(libraryFileName);

      if (is == null) {
        // is there a fallback we can try
        if (fallbackJniLibraryFileName == null) {
          throw new RuntimeException(libraryFileName + " was not found inside JAR.");
        }

        // attempt to look up the fallback static library in the jar file
        libraryFileName = fallbackJniLibraryFileName;
        is = getClass().getClassLoader().getResourceAsStream(libraryFileName);
        if (is == null) {
          throw new RuntimeException(libraryFileName + " was not found inside JAR.");
        }
      }

      // create a temporary file to copy the library to
      final File temp;
      if (tmpDir == null || tmpDir.isEmpty()) {
        temp = File.createTempFile(tempFilePrefix, tempFileSuffix);
      } else {
        final File parentDir = new File(tmpDir);
        if (!parentDir.exists()) {
          throw new RuntimeException(
              "Directory: " + parentDir.getAbsolutePath() + " does not exist!");
        }
        temp = new File(parentDir, libraryFileName);
        if (temp.exists() && !temp.delete()) {
          throw new RuntimeException(
              "File: " + temp.getAbsolutePath() + " already exists and cannot be removed.");
        }
        if (!temp.createNewFile()) {
          throw new RuntimeException("File: " + temp.getAbsolutePath() + " could not be created.");
        }
      }
      if (!temp.exists()) {
        throw new RuntimeException("File " + temp.getAbsolutePath() + " does not exist.");
      } else {
        temp.deleteOnExit();
      }

      // copy the library from the Jar file to the temp destination
      Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);

      // return the temporary library file
      return temp;

    } finally {
      if (is != null) {
        is.close();
      }
    }
  }

  /**
   * Private constructor to disallow instantiation
   */
  private NativeLibraryLoader() {
  }
}
