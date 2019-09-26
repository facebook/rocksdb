package org.rocksdb;

/**
 * Fill DbPaths according to the target size set with the db path.
 * <br>
 * Newer data is placed into paths specified earlier in the vector while
 * older data gradually moves to paths specified later in the vector.
 * <br>
 * For example, you have a flash device with 10GB allocated for the DB,
 * as well as a hard drive of 2TB, you should config it to be:
 *   [{"/flash_path", 10GB}, {"/hard_drive", 2TB}]
 * <br>
 * The system will try to guarantee data under each path is close to but
 * not larger than the target size. But current and future file sizes used
 * by determining where to place a file are based on best-effort estimation,
 * which means there is a chance that the actual size under the directory
 * is slightly more than target size under some workloads. User should give
 * some buffer room for those cases.
 * <br>
 * If none of the paths has sufficient room to place a file, the file will
 * be placed to the last path anyway, despite to the target size.
 * <br>
 * Placing newer data to earlier paths is also best-efforts. User should
 * expect user files to be placed in higher levels in some extreme cases.
 */
public class GradualMoveOldDataDbPathSupplierFactory extends DbPathSupplierFactory {
    static {
        RocksDB.loadLibrary();
    }

    public GradualMoveOldDataDbPathSupplierFactory() {
        super(newFactoryObject());
    }

    @Override
    protected native void disposeInternal(long handle);

    /**
     * @return the address of a shared pointer, in which lives a factory object.
     * The type of this address should be: <br>
     * <code>
     * std::shared_ptr&lt;rocksdb::GradualMoveOldDataDbPathSupplierFactory&gt;*
     * </code>
     */
    private static native long newFactoryObject();
}
