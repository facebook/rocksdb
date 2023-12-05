package org.rocksdb;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class MergeOperatorV2 extends RocksCallbackObject implements MergeOperator {

    private final String operatorName;

    public MergeOperatorV2(final String operatorName) {
        super(new long[]{toCString(operatorName)});
        this.operatorName = operatorName;
    }

    /**
     * All parameters of this method are valid only during the call. If you want to keep this, you need to make deep copy/clone.
     *
     *
     * @param key
     * @param existingValue
     * @param operand
     * @return
     */
    public abstract MergeOperatorOutput fullMergeV2(final ByteBuffer key, final ByteBuffer existingValue, final List<ByteBuffer> operand);


    final MergeOperatorOutput mergeInternal(final ByteBuffer key, final ByteBuffer existingValue, final ByteBuffer[] operand) {
        List<ByteBuffer> operandList = Arrays.stream(operand)
                .map(ByteBuffer::asReadOnlyBuffer)
                .collect(Collectors.toUnmodifiableList());

        return fullMergeV2(key.asReadOnlyBuffer(), existingValue.asReadOnlyBuffer(), operandList);
    }


    @Override
    final public long nativeHandler() {
        return nativeHandle_;
    }

    @Override
    protected long initializeNative(long... nativeParameterHandles) {
        return newMergeOperator(nativeParameterHandles[0]);
    }

    @Override
    protected void disposeInternal() {
        disposeInternal(nativeHandle_);
    }

    public String getOperatorName() {
        return operatorName;
    }

    private native long newMergeOperator(long operatorName);

    private static native long toCString(String operatorName);

    private static native void disposeInternal(long nativeHandle);

}