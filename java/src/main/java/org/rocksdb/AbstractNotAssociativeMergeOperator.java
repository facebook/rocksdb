package org.rocksdb;

import java.util.List;

public abstract class AbstractNotAssociativeMergeOperator extends MergeOperator {


    private native static long newOperator(boolean allowSingleOperand, boolean allowShouldMerge, boolean allowPartialMultiMerge);

    private native  void initOperator(long handle);

    public AbstractNotAssociativeMergeOperator(boolean allowSingleOperand,boolean allowShouldMerge,boolean partialMultiMerge) {
        super(newOperator( allowSingleOperand,allowShouldMerge,partialMultiMerge));
        initOperator(nativeHandle_);
    }


    @Override
    protected final native void disposeInternal(final long handle);

    abstract public byte[] fullMerge(byte[] key, byte[] oldvalue,byte[][] operands,ReturnType rt) throws RocksDBException;
    abstract public byte[] partialMultiMerge(byte[] key, byte[][] operands,ReturnType rt)throws RocksDBException;

    abstract public byte[] partialMerge(byte[] key, byte[] left,byte[] right,ReturnType rt)throws RocksDBException;


    abstract public boolean shouldMerge(byte[][] operands)throws RocksDBException;
}
