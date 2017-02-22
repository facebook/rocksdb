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

  public static boolean isAix() {
    return OS.contains("aix");
  }
  
  public static boolean isUnix() {
    return OS.contains("nix") ||
        OS.contains("nux");
  }

  public static boolean isSolaris() {
    return OS.contains("sunos");
  }

  public static boolean is64Bit() {
    if (ARCH.indexOf("sparcv9") >= 0) {
      return true;
    }
    return (ARCH.indexOf("64") > 0);
  }

  public static String getSharedLibraryName(final String name) {
    return name + "jni";
  }

  public static String getSharedLibraryFileName(final String name) {
    return appendLibOsSuffix("lib" + getSharedLibraryName(name), true);
  }

  public static String getJniLibraryName(final String name) {
    if (isUnix()) {
      return String.format("%sjni-%s", name, (is64Bit()) ? "linuxAMD64" : "linux32");
    } else if (isMac()) {
      return String.format("%sjni-osx", name);
    } else if (isAix() && is64Bit()) {
      return String.format("%sjni-aix64", name);
    } else if (isWindows()) {
      return String.format("%sjni-windows%s", name, is64Bit() ? "AMD64" : "32");
    } else if (isSolaris() && is64Bit()) {
      return String.format("%sjni-solarisSparc64", name);
    } 
    throw new UnsupportedOperationException();
  }

  public static String getJniLibraryFileName(final String name) {
    return appendLibOsSuffix("lib" + getJniLibraryName(name), false);
  }

  private static String appendLibOsSuffix(final String libraryFileName, final boolean shared) {
    if (isUnix() || isAix() || isSolaris()) {
      return libraryFileName + ".so";
    } else if (isMac()) {
      return libraryFileName + (shared ? ".dylib" : ".jnilib");
    } else if (isWindows()) {
      return libraryFileName + ".dll";
    }
    throw new UnsupportedOperationException();
  }

  public static String getJniLibraryExtension() {
    if (isWindows()) {
      return ".dll";
    }
    return (isMac()) ? ".jnilib" : ".so";
  }
}
