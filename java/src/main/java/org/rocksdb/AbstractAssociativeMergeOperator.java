package org.rocksdb;


public abstract class AbstractAssociativeMergeOperator extends MergeOperator
{


    private native static long newOperator();
    private native boolean initOperator(long handle);

    public AbstractAssociativeMergeOperator()  {
        super(newOperator());
       initOperator(nativeHandle_) ;
    }




    @Override protected final native void disposeInternal(final long handle);


    abstract public byte[] merge(byte[] key,byte[] oldvalue,byte[] newvalue)throws RocksDBException;

}
