package br.gov.sus.opendata.dbf.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import br.gov.sus.opendata.dbc.DbcNativeDecompressor;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class DbfParquetTest {

  @Test
  void t() {
    var p = Path.of("/TMP");
    var p1 = Path.of("/tmp");

    assertEquals(p, p1);
  }
}
