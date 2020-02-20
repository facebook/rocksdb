// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A Merge Operator where the value of a key/value pair in the database is
 * actually a Collection of records (byte strings).
 *
 * Provides support for both Set and Vector like collections.
 *
 * The value sent to db.merge(key, value) should be a byte string with the
 * following format:
 *     [RecordType, Record*]
 *
 * RecordType is a single byte and must be one of CollectionOperation.
 * Records is a byte string of zero or more record(s), each record must be
 * of length fixed_record_len.
 *
 * For example (for a fixed_record_len of 4):
 *     db.merge(key1, [kAdd, 100020003000])
 *     db.merge(key1, [kRemove, 2000])
 *
 *     db.get(key1) == [10003000]
 *
 * @author Adam Retter
 */
//@ThreadSafe
public class CollectionMergeOperator extends MergeOperator {

  /**
   * Creates a Collection Merge Operator.
   *
   * No uniqueness constraint is imposed, so the Collection
   * will operate like an ArrayList as opposed to a Set.
   *
   * No Comparator is applied so the Collection will be un-ordered.
   *
   * @param fixedRecordLen the fixed size of each record.
   */
  public CollectionMergeOperator(final short fixedRecordLen) {
    this(fixedRecordLen, (AbstractComparator)null);
  }

  /**
   * Creates a Collection Merge Operator.
   *
   * No uniqueness constraint is imposed, so the Collection
   * will operate like an ArrayList as opposed to a Set.
   *
   * @param fixedRecordLen the fixed size of each record.
   * @param comparator if records should be ordered, a comparator to order them.
   */
  public CollectionMergeOperator(final short fixedRecordLen,
      final AbstractComparator comparator) {
    this(fixedRecordLen, comparator, UniqueConstraint.NONE);
  }

  /**
   * Creates a Collection Merge Operator.
   *
   * No uniqueness constraint is imposed, so the Collection
   * will operate like an ArrayList as opposed to a Set.
   *
   * @param fixedRecordLen the fixed size of each record.
   * @param builtinComparator if records should be ordered, a comparator
   *     to order them.
   */
  public CollectionMergeOperator(final short fixedRecordLen,
      final BuiltinComparator builtinComparator) {
    this(fixedRecordLen, builtinComparator, UniqueConstraint.NONE);
  }

  /**
   * Creates a Collection Merge Operator.
   *
   * @param fixedRecordLen the fixed size of each record.
   * @param uniqueConstraint A constraint on whether records should be unique
   *     or not, controls Set vs Vector behaviour.
   */
  public CollectionMergeOperator(final short fixedRecordLen,
      final UniqueConstraint uniqueConstraint) {
    this(fixedRecordLen, (AbstractComparator)null, uniqueConstraint);
  }

  /**
   * Creates a Collection Merge Operator.
   *
   * @param fixedRecordLen the fixed size of each record.
   * @param builtinComparator if records should be ordered, a comparator to
   *     order them.
   * @param uniqueConstraint A constraint on whether records should be unique
   *     or not, controls Set vs Vector behaviour.
   */
  public CollectionMergeOperator(final short fixedRecordLen,
      final BuiltinComparator builtinComparator,
      final UniqueConstraint uniqueConstraint) {
    super(newCollectionMergeOperator(fixedRecordLen,
        builtinComparator.getValue(),
        uniqueConstraint.value));
  }

  /**
   * Creates a Collection Merge Operator.
   *
   * @param fixedRecordLen the fixed size of each record.
   * @param comparator if records should be ordered, a comparator to order them.
   * @param uniqueConstraint A constraint on whether records should be unique
   *     or not, controls Set vs Vector behaviour.
   */
  public CollectionMergeOperator(final short fixedRecordLen,
      final AbstractComparator comparator, final UniqueConstraint uniqueConstraint) {
    super(newCollectionMergeOperator(fixedRecordLen,
        comparator  == null ? 0 : comparator.nativeHandle_,
        comparator == null ? ComparatorType.JAVA_NATIVE_COMPARATOR_WRAPPER.getValue() : comparator.getComparatorType().getValue(),
        uniqueConstraint.value));
  }

  private native static long newCollectionMergeOperator(
      final short fixedRecordLen, final long comparatorHandle,
      byte comparatorType, final byte uniqueConstraint);
  private native static long newCollectionMergeOperator(
      final short fixedRecordLen, final byte builtinComparator,
      final byte uniqueConstraint);
  @Override protected final native void disposeInternal(final long handle);

  /**
   * A constraint on the uniqueness of items within a Collection.
   */
  public enum UniqueConstraint {

    /**
     * Duplicate values in the Collection will be removed.
     */
    MAKE_UNIQUE((byte) 0x0),

    /**
     * If duplicate values are detected, the merge aborts.
     */
    ENFORCE_UNIQUE((byte) 0x1),

    /**
     * No uniqueness constraint on values in the Collection.
     */
    NONE((byte) 0x2);

    private final byte value;

    UniqueConstraint(final byte value) {
      this.value = value;
    }

    public byte getValue() {
      return value;
    }
  }

  /**
   * The Operation to apply to a Collection.
   */
  public enum CollectionOperation {

    /**
     * Add one or more records to the Collection.
     */
    ADD((byte)0x0),

    /**
     * Remove one or more records from the Collection.
     */
    REMOVE((byte)0x1),

    /**
     * Clear all records from the Collection.
     */
    CLEAR((byte)0x2);

    private final byte value;

    CollectionOperation(final byte value) {
      this.value = value;
    }

    /**
     * Get the byte value of the Collection Operation.
     *
     * @return The byte value of the Collection Operation.
     */
    public byte getValue() {
      return value;
    }

    /**
     * Produces an ADD Collection Operation.
     *
     * @param records The records to add to the Collection.
     * @return a byte string of the operation.
     */
    public static byte[] add(final byte[] records) {
      return operation(ADD, records);
    }

    /**
     * Produces an ADD Collection Operation.
     *
     * @param records The records to add to the Collection.
     * @return a byte string of the operation.
     */
    public static byte[] add(final String records) {
      return operation(ADD, records.getBytes(UTF_8));
    }

    /**
     * Produces a REMOVE Collection Operation.
     *
     * @param records The records to remove from the Collection.
     * @return a byte string of the operation.
     */
    public static byte[] remove(final byte[] records) {
      return operation(REMOVE, records);
    }

    /**
     * Produces a REMOVE Collection Operation.
     *
     * @param records The records to remove from the Collection.
     * @return a byte string of the operation.
     */
    public static byte[] remove(final String records) {
      return operation(REMOVE, records.getBytes(UTF_8));
    }

    /**
     * Produces a CLEAR Collection Operation.
     *
     * @return a byte string of the operation.
     */
    public static byte[] clear() {
      return operation(CLEAR, null);
    }

    /**
     * Produces a byte string operand for a Collection Operation of records.
     *
     * @param collectionOperation The Collection Operation to perform.
     * @param records The records for the operation, or null if the collection
     *     operation is a CLEAR.
     *
     * @return the byte string.
     */
    public static byte[] operation(
        final CollectionOperation collectionOperation,
        /*@Nullable*/ final byte[] records) {
      if(records == null || records.length == 0) {
        if(collectionOperation == CLEAR) {
          return new byte[] {CLEAR.value};
        } else {
          throw new IllegalStateException("Only CLEAR is permissible" +
              "without records");
        }
      }

      final byte[] operation = new byte[1 + records.length];
      operation[0] = collectionOperation.value;
      System.arraycopy(records, 0, operation, 1, records.length);
      return operation;
    }
  }
}
