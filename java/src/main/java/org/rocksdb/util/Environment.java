package org.rocksdb.util;

public class Environment {
  private static String OS = System.getProperty("os.name").toLowerCase();
  private static String ARCH = System.getProperty("os.arch").toLowerCase();

  public static boolean isWindows() {
    return (OS.contains("win"));
  }

  public static boolean isMac() {
    return (OS.contains("mac"));
  }

  public static boolean isUnix() {
    return (OS.contains("nix") ||
        OS.contains("nux") ||
        OS.contains("aix"));
  }

  public static boolean is64Bit() {
    return (ARCH.indexOf("64") > 0);
  }

  public static String getSharedLibraryName(String name) {
    if (isUnix()) {
      return String.format("lib%sjni.so", name);
    } else if (isMac()) {
      return String.format("lib%sjni.dylib", name);
    }
    throw new UnsupportedOperationException();
  }

  public static String getJniLibraryName(String name) {
    if (isUnix()) {
      String arch = (is64Bit()) ? "64" : "32";
      return String.format("lib%sjni-linux%s.so", name, arch);
    } else if (isMac()) {
      return String.format("lib%sjni-osx.jnilib", name);
    }
    throw new UnsupportedOperationException();
  }

  public static String getJniLibraryExtension() {
    return (isMac()) ? ".jnilib" : ".so";
  }
}
