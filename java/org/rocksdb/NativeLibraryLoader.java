package org.rocksdb;

import java.io.*;
import org.rocksdb.util.Environment;

/**
 * This class is used to load the RocksDB shared library from within the jar.
 * The shared library is extracted to a temp folder and loaded from there.
 */
public class NativeLibraryLoader {
  private static String sharedLibraryName = Environment.getJniLibraryName("rocksdb");
  private static String tempFileSuffix = "." + Environment.getJniLibraryExtension();

  public static void loadLibraryFromJar(String tmpDir)
      throws IOException {
    File temp;
    String tempFilePrefix = "librocksdbjni";
    if(tmpDir == null || tmpDir.equals(""))
      temp = File.createTempFile(tempFilePrefix, tempFileSuffix);
    else
      temp = new File(tmpDir + "/" + sharedLibraryName);

    temp.deleteOnExit();

    if (!temp.exists()) {
      throw new RuntimeException("File " + temp.getAbsolutePath() + " does not exist.");
    }

    byte[] buffer = new byte[102400];
    int readBytes;

    InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream(sharedLibraryName);
    if (is == null) {
      throw new RuntimeException(sharedLibraryName + " was not found inside JAR.");
    }

    OutputStream os = null;
    try {
      os = new FileOutputStream(temp);
      while ((readBytes = is.read(buffer)) != -1) {
        os.write(buffer, 0, readBytes);
      }
    } finally {
      if(os != null)
        os.close();
      is.close();
    }

    System.load(temp.getAbsolutePath());
  }
  /**
   * Private constructor to disallow instantiation
   */
  private NativeLibraryLoader() {
  }
}
