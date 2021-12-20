package org.rocksdb.api;

import org.rocksdb.CompressionType;
import org.rocksdb.DBOptionsInterface;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDBException;
import org.rocksdb.util.Environment;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class RocksDB extends RocksNative {

  private enum LibraryState {
    NOT_LOADED,
    LOADING,
    LOADED
  }

  private static final AtomicReference<org.rocksdb.api.RocksDB.LibraryState> libraryLoaded =
      new AtomicReference<>(org.rocksdb.api.RocksDB.LibraryState.NOT_LOADED);

  static {
    org.rocksdb.api.RocksDB.loadLibrary();
  }

  /**
   * Loads the necessary library files.
   * Calling this method twice will have no effect.
   * By default the method extracts the shared library for loading at
   * java.io.tmpdir, however, you can override this temporary location by
   * setting the environment variable ROCKSDB_SHAREDLIB_DIR.
   */
  public static void loadLibrary() {
    if (libraryLoaded.get() == org.rocksdb.api.RocksDB.LibraryState.LOADED) {
      return;
    }

    if (libraryLoaded.compareAndSet(org.rocksdb.api.RocksDB.LibraryState.NOT_LOADED,
        org.rocksdb.api.RocksDB.LibraryState.LOADING)) {
      final String tmpDir = System.getenv("ROCKSDB_SHAREDLIB_DIR");
      // loading possibly necessary libraries.
      for (final CompressionType compressionType : CompressionType.values()) {
        try {
          if (compressionType.getLibraryName() != null) {
            System.loadLibrary(compressionType.getLibraryName());
          }
        } catch (final UnsatisfiedLinkError e) {
          // since it may be optional, we ignore its loading failure here.
        }
      }
      try {
        NativeLibraryLoader.getInstance().loadLibrary(tmpDir);
      } catch (final IOException e) {
        libraryLoaded.set(org.rocksdb.api.RocksDB.LibraryState.NOT_LOADED);
        throw new RuntimeException("Unable to load the RocksDB shared library",
            e);
      }

      final int encodedVersion = version();
      version = org.rocksdb.api.RocksDB.Version.fromEncodedVersion(encodedVersion);

      libraryLoaded.set(org.rocksdb.api.RocksDB.LibraryState.LOADED);
      return;
    }

    while (libraryLoaded.get() == org.rocksdb.api.RocksDB.LibraryState.LOADING) {
      try {
        Thread.sleep(10);
      } catch(final InterruptedException e) {
        //ignore
      }
    }
  }

  /**
   * Tries to load the necessary library files from the given list of
   * directories.
   *
   * @param paths a list of strings where each describes a directory
   *     of a library.
   */
  public static void loadLibrary(final List<String> paths) {
    if (libraryLoaded.get() == org.rocksdb.api.RocksDB.LibraryState.LOADED) {
      return;
    }

    if (libraryLoaded.compareAndSet(org.rocksdb.api.RocksDB.LibraryState.NOT_LOADED,
        org.rocksdb.api.RocksDB.LibraryState.LOADING)) {
      for (final CompressionType compressionType : CompressionType.values()) {
        if (compressionType.equals(CompressionType.NO_COMPRESSION)) {
          continue;
        }
        for (final String path : paths) {
          try {
            System.load(path + "/" + Environment.getSharedLibraryFileName(
                compressionType.getLibraryName()));
            break;
          } catch (final UnsatisfiedLinkError e) {
            // since they are optional, we ignore loading fails.
          }
        }
      }
      boolean success = false;
      UnsatisfiedLinkError err = null;
      for (final String path : paths) {
        try {
          System.load(path + "/" +
              Environment.getJniLibraryFileName("rocksdbjni"));
          success = true;
          break;
        } catch (final UnsatisfiedLinkError e) {
          err = e;
        }
      }
      if (!success) {
        libraryLoaded.set(org.rocksdb.api.RocksDB.LibraryState.NOT_LOADED);
        throw err;
      }

      final int encodedVersion = version();
      version = org.rocksdb.api.RocksDB.Version.fromEncodedVersion(encodedVersion);

      libraryLoaded.set(org.rocksdb.api.RocksDB.LibraryState.LOADED);
      return;
    }

    while (libraryLoaded.get() == org.rocksdb.api.RocksDB.LibraryState.LOADING) {
      try {
        Thread.sleep(10);
      } catch(final InterruptedException e) {
        //ignore
      }
    }
  }

  protected RocksDB(long nativeReference) {
    super(nativeReference);
  }

  public static org.rocksdb.api.RocksDB open(final DBOptions options, final String path,
                                         final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
                                         final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {

    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    final long[] cfOptionHandles = new long[columnFamilyDescriptors.size()];
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor cfDescriptor = columnFamilyDescriptors
          .get(i);
      cfNames[i] = cfDescriptor.getName();
      cfOptionHandles[i] = cfDescriptor.getOptions().getNative();
    }

    final long[] handles = open(options.getNative(), path, cfNames,
        cfOptionHandles);
    final org.rocksdb.api.RocksDB db = new org.rocksdb.api.RocksDB(handles[0]);
    db.storeOptionsInstance(options);

    for (int i = 1; i < handles.length; i++) {
      final ColumnFamilyHandle columnFamilyHandle = new ColumnFamilyHandle(db, handles[i]);
      columnFamilyHandles.add(columnFamilyHandle);
    }

    return db;
  }

  protected void storeOptionsInstance(DBOptionsInterface<DBOptions> options) {
    options_ = options;
  }

  /**
   * @param optionsHandle Native handle pointing to an Options object
   * @param path The directory path for the database files
   * @param columnFamilyNames An array of column family names
   * @param columnFamilyOptions An array of native handles pointing to
   *                            ColumnFamilyOptions objects
   *
   * @return An array of native handles, [0] is the handle of the RocksDB object
   *   [1..1+n] are handles of the ColumnFamilyReferences
   *
   * @throws RocksDBException thrown if the database could not be opened
   */
  private native static long[] open(final long optionsHandle,
                                    final String path, final byte[][] columnFamilyNames,
                                    final long[] columnFamilyOptions) throws RocksDBException;

  @Override
  native void nativeClose(long nativeReference);

  private native static int version();

  protected DBOptionsInterface<DBOptions> options_;
  private static org.rocksdb.RocksDB.Version version;

  public static class Version {
    private final byte major;
    private final byte minor;
    private final byte patch;

    public Version(final byte major, final byte minor, final byte patch) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
    }

    public int getMajor() {
      return major;
    }

    public int getMinor() {
      return minor;
    }

    public int getPatch() {
      return patch;
    }

    @Override
    public String toString() {
      return getMajor() + "." + getMinor() + "." + getPatch();
    }

    private static org.rocksdb.RocksDB.Version fromEncodedVersion(int encodedVersion) {
      final byte patch = (byte) (encodedVersion & 0xff);
      encodedVersion >>= 8;
      final byte minor = (byte) (encodedVersion & 0xff);
      encodedVersion >>= 8;
      final byte major = (byte) (encodedVersion & 0xff);

      return new org.rocksdb.RocksDB.Version(major, minor, patch);
    }
  }

}
