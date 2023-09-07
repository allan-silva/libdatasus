package br.gov.sus.opendata.dbf.parquet;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFRow;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Test;

class InternalDbfReaderTest {

  @Test
  public void exploratoryTypes() throws IOException {
    String dbfPath = TestTypeValueFixture.createDbf();
    try (FileInputStream fis = new FileInputStream(dbfPath)) {
      InternalDbfReader dbfReader = new InternalDbfReader(fis);
      DBFRow dbfRow;
      while ((dbfRow = dbfReader.nextRow()) != null) {
        Object charValue = dbfRow.getObject(TestTypeValueFixture.CHARACTER_FIELD.getIndex());
        Object numericValue = dbfRow.getObject(TestTypeValueFixture.NUMERIC_FIELD.getIndex());
        Object logicalValue = dbfRow.getObject(TestTypeValueFixture.LOGICAL_FIELD.getIndex());
        Object dateValue = dbfRow.getObject(TestTypeValueFixture.DATE_FIELD.getIndex());
        Object floatValue = dbfRow.getObject(TestTypeValueFixture.FLOAT_FIELD.getIndex());

        assertEquals(TestTypeValueFixture.CHARACTER_FIELD.getValue(), charValue);
        assertEquals(
            TestTypeValueFixture.FLOAT_FIELD.getValue(), ((BigDecimal) floatValue).floatValue());
        assertEquals(
            0, TestTypeValueFixture.NUMERIC_FIELD.getValue().compareTo((BigDecimal) numericValue));

        assertEquals(TestTypeValueFixture.LOGICAL_FIELD.getValue(), logicalValue);
        assertEquals(0, TestTypeValueFixture.DATE_FIELD.getValue().compareTo((Date) dateValue));
      }
    }
  }

  @Test
  public void readDbf() throws IOException {
    try (FileInputStream fis = new FileInputStream(TestUtils.getResourcePath("dbf/POBR2023.dbf"))) {
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
}
