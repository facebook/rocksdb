// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

/**
 * <p>A WalIterator reads {@link WriteBatch}es out of a DB's write-ahead log,
 * in increasing sequence number order. It is obtained from
 * {@link RocksDB#getUpdatesSince(long)} and is the basis of "WAL tailing":
 * following a DB's writes as they happen, for replication, change data
 * capture, and similar.</p>
 *
 * <p><strong>This only sees the WAL.</strong> Writes made with
 * {@link WriteOptions#setDisableWAL(boolean)}, and sequence numbers consumed
 * by {@code ingestExternalFile}, advance the DB's sequence number without
 * writing any WAL record. They leave permanent holes in the sequence numbers
 * visible here, which this API reports as the end of the run rather than
 * skipping. A DB that uses either of them can still be followed up to the
 * first such hole, but cannot be followed continuously.</p>
 *
 * <p>The WAL is not retained indefinitely. Set
 * {@code WAL_ttl_seconds} and/or {@code WAL_size_limit_MB} large enough to
 * cover how far behind a consumer may fall.</p>
 *
 * <p>An iterator is in exactly one of three states:</p>
 * <ol>
 *   <li>{@link #isValid()} is true: positioned at a WriteBatch, and
 *       {@link #getBatch()} may be called.</li>
 *   <li>{@link #isValid()} is false and {@link #status()} does not throw:
 *       caught up. This is not the end of iteration; more writes may arrive,
 *       so calling {@link #next()} again later is how a consumer tails a DB.
 *       Note that this is polling: next() never waits for new writes and
 *       there is no notification when they arrive, so the consumer chooses
 *       its own retry interval.</li>
 *   <li>{@link #isValid()} is false and {@link #status()} throws: the run is
 *       over and the iterator is spent. {@link #next()} has no further
 *       effect. To continue, obtain a new iterator -- but see the warning on
 *       {@link RocksDB#getUpdatesSince(long)} first.</li>
 * </ol>
 *
 * <p><strong>Resuming a run.</strong> Within a single run this iterator stops
 * rather than skipping over a gap in sequence numbers, so a run is contiguous
 * and a consumer need not re-check that. What is not guaranteed is the seam
 * between runs: {@link RocksDB#getUpdatesSince(long)} silently starts later
 * if the requested sequence number is unavailable. A consumer applying these
 * batches to a copy of the DB diverges permanently and undetectably if it
 * misses one, so on the first batch of each new iterator it should check that
 * {@link BatchResult#sequenceNumber()} equals the last delivered batch's
 * sequence number plus its {@link WriteBatch#count()}. On a mismatch the
 * intervening updates are gone from the WAL, so re-seed from a checkpoint
 * rather than resuming.</p>
 */
public class WalIterator extends RocksObject {
  /**
   * <p>An iterator is either positioned at a WriteBatch
   * or not valid. This method returns true if the iterator
   * is valid. Can read data from a valid iterator.</p>
   *
   * @return true if iterator position is valid.
   */
  public boolean isValid() {
    return isValid(nativeHandle_);
  }

  /**
   * <p>Moves the iterator to the next WriteBatch.</p>
   *
   * <p>Unlike most RocksDB iterators, this does not require
   * {@link #isValid()}. Calling next() on an iterator whose
   * {@link #isValid()} is false but whose {@link #status()} does not throw is
   * how a consumer polls for writes that have happened since it caught up,
   * and is the intended way to tail a DB. It does not wait for new writes.
   * Calling next() on a spent iterator has no effect.</p>
   */
  public void next() {
    next(nativeHandle_);
  }

  /**
   * <p>Does nothing while the iterator is usable, including when it is merely
   * caught up. Throws the reason the run ended otherwise.</p>
   *
   * @throws org.rocksdb.RocksDBException if something went
   *     wrong in the underlying C++ code.
   */
  public void status() throws RocksDBException {
    status(nativeHandle_);
  }

  /**
   * <p>Returns the current write batch and the sequence number of the first
   * update it contains.</p>
   *
   * <p>ONLY use if {@link #isValid()} is true.</p>
   *
   * @return {@link org.rocksdb.WalIterator.BatchResult}
   *     instance.
   */
  public BatchResult getBatch() {
    assert (isValid());
    return getBatch(nativeHandle_);
  }

  /**
   * <p>WalIterator constructor.</p>
   *
   * @param nativeHandle address to native address.
   */
  WalIterator(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * <p>BatchResult represents a data structure returned
   * by a WalIterator containing a sequence
   * number and a {@link WriteBatch} instance.</p>
   */
  public static final class BatchResult {
    /**
     * <p>Constructor of BatchResult class.</p>
     *
     * @param sequenceNumber related to this BatchResult instance.
     * @param nativeHandle to {@link org.rocksdb.WriteBatch}
     *     native instance.
     */
    public BatchResult(final long sequenceNumber, final long nativeHandle) {
      sequenceNumber_ = sequenceNumber;
      writeBatch_ = new WriteBatch(nativeHandle, true);
    }

    /**
     * <p>Return the sequence number of the <em>first</em> update in this
     * batch. The batch covers the sequence number range
     * {@code [sequenceNumber, sequenceNumber + writeBatch().count() - 1]},
     * so the next expected sequence number is
     * {@code sequenceNumber + writeBatch().count()}.</p>
     *
     * @return Sequence number.
     */
    public long sequenceNumber() {
      return sequenceNumber_;
    }

    /**
     * <p>Return contained {@link org.rocksdb.WriteBatch}
     * instance</p>
     *
     * @return {@link org.rocksdb.WriteBatch} instance.
     */
    public WriteBatch writeBatch() {
      return writeBatch_;
    }

    private final long sequenceNumber_;
    private final WriteBatch writeBatch_;
  }

  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);
  private static native boolean isValid(long handle);
  private static native void next(long handle);
  private static native void status(long handle) throws RocksDBException;
  private static native BatchResult getBatch(long handle);
}
