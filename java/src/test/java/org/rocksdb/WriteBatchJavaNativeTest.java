package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class WriteBatchJavaNativeTest {
  @Test
  public void put() throws RocksDBException, UnsupportedEncodingException {
    try (WriteBatchJavaNative wb = new WriteBatchJavaNative(256)) {
      wb.put("k1".getBytes(), "v1".getBytes());
      /*
      wb.put("k02".getBytes(), "v02".getBytes());
      wb.put("k03".getBytes(), "v03".getBytes());
      wb.put("k04".getBytes(), "v04".getBytes());
      wb.put("k05".getBytes(), "v05".getBytes());
      wb.put("k06".getBytes(), "v06".getBytes());
      wb.put("k07".getBytes(), "v07".getBytes());
      wb.put("k08".getBytes(), "v08".getBytes());
      wb.put("k09".getBytes(), "v09".getBytes());
      wb.put("k10".getBytes(), "v10".getBytes());
       */

      wb.flush();

      assertThat(new String(getContents(wb), StandardCharsets.UTF_8))
          .isEqualTo("Put(baz, boo)@102"
              + "Delete(box)@101"
              + "Put(foo, bar)@100");
    }
  }

  static byte[] getContents(final WriteBatchJavaNative wb) {
    return WriteBatchTest.getContents(wb.nativeHandle_);
  }
}
