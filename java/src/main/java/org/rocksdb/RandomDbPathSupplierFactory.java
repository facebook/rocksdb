package org.rocksdb;

/**
 * Randomly distribute files into the list of db paths.
 *
 * For example, you have a few data drives on your host that are mounted
 * as [/sdb1, /sdc1, /sdd1, /sde1]. Say that the database will create 6
 * table files -- 0[0-5].sst, then they will end up on in these places:
 *
 * /sdb1/02.sst
 * /sdb1/04.sst
 * /sdc1/05.sst
 * /sdc1/03.sst
 * /sdd1/00.sst
 * /sde1/01.sst
 *
 * This is useful if you want the database to evenly use a set of disks
 * mounted on your host.
 *
 * Note that the target_size attr in DbPath will not be useful if this
 * strategy is chosen.
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

    private static native long newFactoryObject();
}
