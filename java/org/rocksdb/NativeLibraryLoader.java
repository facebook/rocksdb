package org.rocksdb;

import java.io.*;

public class NativeLibraryLoader {

  private static String sharedLibraryName = "librocksdbjni.so";
  private static String tempFilePrefix = "librocksdbjni";
  private static String tempFileSuffix = ".so";
  /**
   * Private constructor - this class will never be instanced
   */
  private NativeLibraryLoader() {
  }

  public static void loadLibraryFromJar()
      throws IOException {

    File temp = File.createTempFile(tempFilePrefix, tempFileSuffix);
    temp.deleteOnExit();

    if (!temp.exists()) {
      throw new RuntimeException("File " + temp.getAbsolutePath() + " does not exist.");
    }

    byte[] buffer = new byte[1024];
    int readBytes;

    InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream(sharedLibraryName);
    if (is == null) {
      throw new RuntimeException(sharedLibraryName + " was not found inside JAR.");
    }

    OutputStream os = new FileOutputStream(temp);
    try {
      while ((readBytes = is.read(buffer)) != -1) {
        os.write(buffer, 0, readBytes);
      }
    } finally {
      os.close();
      is.close();
    }

    System.load(temp.getAbsolutePath());
  }
}
