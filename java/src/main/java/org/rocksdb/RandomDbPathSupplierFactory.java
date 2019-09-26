package org.rocksdb;

/**
 * Randomly distribute files into the list of db paths.
 * <br>
 * For example, you have a few data drives on your host that are mounted
 * as [/sdb1, /sdc1, /sdd1, /sde1]. Say that the database will create 6
 * table files -- 0[0-5].sst, then they will randomly end up on in these
 * places:
 * <ul>
 * <li>/sdb1/02.sst</li>
 * <li>/sdb1/04.sst</li>
 * <li>/sdc1/05.sst</li>
 * <li>/sdc1/03.sst</li>
 * <li>/sdd1/00.sst</li>
 * <li>/sde1/01.sst</li>
 * </ul>
 * This is useful if you want the database to evenly use a set of disks
 * mounted on your host. Note that the target_size attr in DbPath will
 * not be useful if this strategy is chosen.
 */
public class RandomDbPathSupplierFactory extends DbPathSupplierFactory {
    static {
        RocksDB.loadLibrary();
    }

    public RandomDbPathSupplierFactory() {
        super(newFactoryObject());
    }

    @Override
    protected native void disposeInternal(long handle);

    /**
     * @return the address of a shared pointer, in which lives a factory object.
     * <br>
     * The type of this address should be: <br>
     * <code>
     * std::shared_ptr&lt;rocksdb::RandomDbPathSupplierFactory&gt;*
     * </code>
     */
    private static native long newFactoryObject();
}
