package br.gov.sus.opendata.dbf.parquet;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linuxense.javadbf.DBFField;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class InternalDbfReaderTest {

  @Test
  public void readDbf() throws IOException {
    try (FileInputStream fis = new FileInputStream(getDbfPath())) {
      InternalDbfReader dbfReader = new InternalDbfReader(fis);
      assertEquals(23, dbfReader.schema.fields.length);
      assertEquals(195526, travel(dbfReader));
      validateFields(dbfReader);
    }
  }

  private void validateFields(InternalDbfReader dbfReader) {
    List<String> expectedFields =
        asList(
            "ANO_DIAGN",
            "ANOMES_DIA",
            "ANO_TRATAM",
            "ANOMES_TRA",
            "UF_RESID",
            "MUN_RESID",
            "UF_TRATAM",
            "MUN_TRATAM",
            "UF_DIAGN",
            "MUN_DIAG",
            "TRATAMENTO",
            "DIAGNOSTIC",
            "IDADE",
            "SEXO",
            "ESTADIAM",
            "CNES_DIAG",
            "CNES_TRAT",
            "TEMPO_TRAT",
            "CNS_PAC",
            "DIAG_DETH",
            "DT_DIAG",
            "DT_TRAT",
            "DT_NASC");
    List<String> dbfFields = Arrays.stream(dbfReader.schema.fields).map(DBFField::getName).toList();
    expectedFields.forEach(expectedField -> assertTrue(dbfFields.contains(expectedField)));
  }

  private int travel(final InternalDbfReader reader) {
    int count = 0;
    while (reader.nextRecord() != null) {
      ++count;
    }
    return count;
  }

  private String getDbfPath() {
    return getClass().getClassLoader().getResource("dbf/POBR2023.dbf").getPath();
  }
}
