package br.gov.sus.opendata.dbf.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.exasol.parquetio.data.Row;
import com.exasol.parquetio.reader.RowParquetReader;
import com.linuxense.javadbf.DBFRow;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.jupiter.api.Test;

class DbfParquetWriterTest {

  @Test
  void writeTypesToParquetTest() throws IOException {
    String dbfPath = TestTypeValueFixture.createDbf();
    String parquetPath = dbfPath + ".parquet";

    try (FileInputStream fis = new FileInputStream(dbfPath);
        InternalDbfReader dbfReader = new InternalDbfReader(fis);
        ParquetWriter<DBFRow> parquetWriter =
            DbfParquetWriter.builder(parquetPath).withDbfSchema(dbfReader.schema).build()) {
      DBFRow dbfRow;
      if ((dbfRow = dbfReader.nextRow()) != null) {
        parquetWriter.write(dbfRow);
      }
    }

    Configuration config = new Configuration();
    Path hadoopPath = new Path(parquetPath);
    InputFile hadoopInputFile = HadoopInputFile.fromPath(hadoopPath, config);

    try (final ParquetReader<Row> reader = RowParquetReader.builder(hadoopInputFile).build()) {
      Row row = reader.read();

      Arrays.stream(TestTypeValueFixture.FIELDS)
          .forEach(
              fieldFixtureDefinition ->
                  row.hasFieldName(fieldFixtureDefinition.getField().getName()));

      Object characterField = row.getValue(TestTypeValueFixture.CHARACTER_FIELD.getIndex());
      assertEquals(TestTypeValueFixture.CHARACTER_FIELD.getValue(), characterField);

      Object numericField = row.getValue(TestTypeValueFixture.NUMERIC_FIELD.getIndex());
      assertEquals(TestTypeValueFixture.NUMERIC_FIELD.getValue(), numericField);

      Object dateField = row.getValue(TestTypeValueFixture.DATE_FIELD.getIndex());
      assertEquals(TestTypeValueFixture.DATE_FIELD.getValue(), dateField);

      Object floatField = row.getValue(TestTypeValueFixture.FLOAT_FIELD.getField().getName());
      assertEquals(TestTypeValueFixture.FLOAT_FIELD.getValue(), floatField);

      Object logicalField = row.getValue(TestTypeValueFixture.LOGICAL_FIELD.getField().getName());
      assertEquals(TestTypeValueFixture.LOGICAL_FIELD.getValue(), logicalField);
    }
  }
}
