package org.rocksdb.util;

public class Environment {
  private static String OS = System.getProperty("os.name").toLowerCase();

  public static boolean isWindows() {
    return (OS.indexOf("win") >= 0);
  }

  public static boolean isMac() {
    return (OS.indexOf("mac") >= 0);
  }

  public static boolean isUnix() {
    return (OS.indexOf("nix") >= 0 ||
            OS.indexOf("nux") >= 0 ||
            OS.indexOf("aix") >= 0);
  }

  public static String getSharedLibraryName(String name) {
    if (isUnix()) {
      return String.format("lib%s.so", name);
    } else if (isMac()) {
      return String.format("lib%s.dylib", name);
    }
    throw new UnsupportedOperationException();
  }

  public static String getJniLibraryName(String name) {
    if (isUnix()) {
      return String.format("lib%s.so", name);
    } else if (isMac()) {
      return String.format("lib%s.jnilib", name);
    }
    throw new UnsupportedOperationException();
  }
}
