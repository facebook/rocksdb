/**
*  Created by Jesse Ma on 2019-05-23.
*  for bytes append operator
*/

package org.rocksdb;

/**
 * BytesAppendOperator is a merge operator that concatenates
 * two strings.
 */
public class BytesAppendOperator extends MergeOperator {
    public BytesAppendOperator() {
        super(newSharedBytesAppendOperator());
    }

    private native static long newSharedBytesAppendOperator();
    @Override protected final native void disposeInternal(final long handle);
}
