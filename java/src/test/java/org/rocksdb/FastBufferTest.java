package org.rocksdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

public class FastBufferTest {
  @Test
  public void basicFastBufferTest() {
    FastBuffer fb = FastBuffer.allocate(Byte.MAX_VALUE);
    for (byte i = 0; i < fb.capacity(); ++i) {
      fb.put(i);
    }
    fb.flip();
    assertEquals(0, fb.remaining());
    assertFalse(fb.hasRemaining());
    for (byte i = 0; i < fb.capacity(); ++i) {
      assertEquals(i, fb.get(i));
    }
    fb.rewind();
  }
}
