package org.rocksdb;

import java.lang.reflect.Field;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import sun.misc.Unsafe;

/**
 * This class has a similar methods to {@link java.nio.ByteBuffer}.
 * Internally it operates on off-heap memory via {@link sun.misc.Unsafe}.
 */
public class FastBuffer extends RocksMutableObject {
  private static Unsafe unsafe;

  private boolean isUnsafeAllocated;

  private int position;
  private int limit;
  private int capacity;

  FastBuffer(final long handle, final int capacity) {
    this(handle, capacity, false);
  }

  private FastBuffer(final long handle, final int capacity, final boolean isUnsafeAllocated) {
    super(handle);
    this.isUnsafeAllocated = isUnsafeAllocated;
    this.limit = capacity;
    this.capacity = capacity;
    this.position = 0;

    if (null == unsafe) {
      initializeUnsafe();
    }
  }

  private static void initializeUnsafe() {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      unsafe = (Unsafe) f.get(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static FastBuffer allocate(int bytes) {
    return new FastBuffer(unsafe.allocateMemory(bytes), bytes, true);
  }

  public final long handle() {
    return getNativeHandle();
  }

  /**
   * Returns this buffer's capacity.
   *
   * @return  The capacity of this buffer
   */
  public final int capacity() {
    return capacity;
  }

  /**
   * Returns this buffer's position.
   *
   * @return  The position of this buffer
   */
  public final int position() {
    return position;
  }

  /**
   * Sets this buffer's position. If the mark is defined and larger than the
   * new position then it is discarded.
   *
   * @param  newPosition
   *         The new position value; must be non-negative
   *         and no larger than the current limit
   *
   * @return  This buffer
   *
   * @throws  IllegalArgumentException
   *          If the preconditions on <tt>newPosition</tt> do not hold
   */
  public final FastBuffer position(int newPosition) {
    if ((newPosition > limit) || (newPosition < 0))
      throw new IllegalArgumentException();
    position = newPosition;
    return this;
  }

  /**
   * Returns this buffer's limit.
   *
   * @return  The limit of this buffer
   */
  public final int limit() {
    return limit;
  }

  /**
   * Sets this buffer's limit.  If the position is larger than the new limit
   * then it is set to the new limit.  If the mark is defined and larger than
   * the new limit then it is discarded.
   *
   * @param  newLimit
   *         The new limit value; must be non-negative
   *         and no larger than this buffer's capacity
   *
   * @return  This buffer
   *
   * @throws  IllegalArgumentException
   *          If the preconditions on <tt>newLimit</tt> do not hold
   */
  public final FastBuffer limit(int newLimit) {
    if ((newLimit > capacity) || (newLimit < 0))
      throw new IllegalArgumentException();
    limit = newLimit;
    if (position > limit)
      position = limit;
    return this;
  }
  /**
   * Flips this buffer.  The limit is set to the current position and then
   * the position is set to zero.
   *
   * <p> After a sequence of channel-read or <i>put</i> operations, invoke
   * this method to prepare for a sequence of channel-write or relative
   * <i>get</i> operations.
   *
   * @return  This buffer
   */

  public final FastBuffer flip() {
    limit = position;
    position = 0;
    return this;
  }

  /**
   * Rewinds this buffer.  The position is set to zero.
   *
   * @return  This buffer
   */
  public final FastBuffer rewind() {
    position = 0;
    return this;
  }

  /**
   * Returns the number of elements between the current position and the
   * limit.
   *
   * @return  The number of elements remaining in this buffer
   */
  public final int remaining() {
    return limit - position;
  }

  /**
   * Tells whether there are any elements between the current position and
   * the limit.
   *
   * @return  <tt>true</tt> if, and only if, there is at least one element
   *          remaining in this buffer
   */
  public final boolean hasRemaining() {
    return position < limit;
  }

  /**
   * Relative <i>get</i> method.  Reads the byte at this buffer's
   * current position, and then increments the position.
   *
   * @return The byte at the buffer's current position
   * @throws BufferUnderflowException If the buffer's current position is not smaller than its limit
   */
  public byte get() {
    if (position >= limit) {
      throw new BufferUnderflowException();
    }
    return unsafe.getByte(getNativeHandle() + position++);
  }

  /**
   * Relative <i>put</i> method&nbsp;&nbsp;<i>(optional operation)</i>.
   *
   * <p> Writes the given byte into this buffer at the current
   * position, and then increments the position. </p>
   *
   * @param b The byte to be written
   * @return This buffer
   * @throws BufferOverflowException If this buffer's current position is not smaller than its limit
   */
  public FastBuffer put(byte b) {
    if (position >= limit) {
      throw new BufferOverflowException();
    }
    unsafe.putByte(getNativeHandle() + position, b);
    return this;
  }

  /**
   * Absolute <i>get</i> method.  Reads the byte at the given
   * index.
   *
   * @param index The index from which the byte will be read
   * @return The byte at the given index
   * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
   *                                   or not smaller than the buffer's limit
   */
  public byte get(int index) {
    if (index < 0 || index >= limit) {
      throw new IndexOutOfBoundsException();
    }
    return unsafe.getByte(getNativeHandle() + index);
  }

  /**
   * Absolute <i>put</i> method&nbsp;&nbsp;<i>(optional operation)</i>.
   *
   * <p> Writes the given byte into this buffer at the given
   * index. </p>
   *
   * @param index The index at which the byte will be written
   * @param b     The byte value to be written
   * @return This buffer
   * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
   *                                   or not smaller than the buffer's limit
   */
  public FastBuffer put(int index, byte b) {
    if (index < 0 || index >= limit) {
      throw new IndexOutOfBoundsException();
    }
    unsafe.putByte(getNativeHandle() + index, b);
    return this;
  }

  @Override
  protected void disposeInternal(final long handle) {
    if (isUnsafeAllocated) {
      assert getNativeHandle() == handle;
      unsafe.freeMemory(handle);
    }
  }
}
