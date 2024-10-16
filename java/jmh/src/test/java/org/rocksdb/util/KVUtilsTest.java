package org.rocksdb.util;

import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class KVUtilsTest {

    @Test public void testBAFillValue() {
        byte[] ba;

        ba = new byte[16];
        KVUtils.baFillValue(ba, "pfx", 42L, 3, (byte)'0');
        assertThat(Arrays.toString(ba)).isEqualTo("[112, 102, 120, 48, 52, 50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]");

        ba = new byte[16];
        KVUtils.baFillValue(ba, "pfx", 4213L, 4, (byte)'0');
        assertThat(Arrays.toString(ba)).isEqualTo("[112, 102, 120, 52, 50, 49, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0]");

        ba = new byte[16];
        KVUtils.baFillValue(ba, "pfx", 4213L, 5, (byte)'0');
        assertThat(Arrays.toString(ba)).isEqualTo("[112, 102, 120, 48, 52, 50, 49, 51, 0, 0, 0, 0, 0, 0, 0, 0]");

        ba = new byte[16];
        KVUtils.baFillValue(ba, "pfx", 4213L, 6, (byte)'0');
        assertThat(Arrays.toString(ba)).isEqualTo("[112, 102, 120, 48, 48, 52, 50, 49, 51, 0, 0, 0, 0, 0, 0, 0]");

        ba = new byte[16];
        KVUtils.baFillValue(ba, "pfx", 4213L, 12, (byte)'0');
        assertThat(Arrays.toString(ba)).isEqualTo("[112, 102, 120, 48, 48, 48, 48, 48, 48, 48, 48, 52, 50, 49, 51, 0]");
    }

}
