package org.rocksdb.util;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Varint32Test {

    @Test
    public void compareWithNaiveWrite() {
        ByteBuffer b1 = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer b2 = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < 32; i++) {
            for (int j = -7; j < +7; j++) {
                int test = (1 << i) + j;
                b1.clear();
                b2.clear();
                Varint32.writeNaive(b1, test);
                Varint32.write(b2, test);
                assertThat(b1.position()).isEqualTo(b2.position());
                b1.flip();
                b2.flip();
                while (b1.hasRemaining()) {
                    assertThat(b2.get()).as("Encoding of %x", test).isEqualTo(b1.get());
                }
                assertThat(b2.hasRemaining()).isFalse();
            }
        }
    }
}
