package br.gov.sus.opendata.dbf.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.exasol.parquetio.data.Row;
import com.exasol.parquetio.reader.RowParquetReader;
import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.jupiter.api.Test;

class DbfParquetTest {

  @Test
  void convertFileToFileTest() throws IOException {
    Path testDir = TestUtils.createTempDir();

    Path inputFile = Path.of(TestUtils.getResourcePath("dbf/exaustive/RDDF9705.dbc"));

    Path copyTarget = testDir.resolve(inputFile.getFileName());
    inputFile = Files.copy(inputFile, copyTarget);
    inputFile.toFile().deleteOnExit();

    DbfParquet dbfParquet = DbfParquet.builder().build();
    dbfParquet.convert(inputFile);

    Path dbfFile = Path.of(inputFile.toString() + ".dbf");
    dbfFile.toFile().deleteOnExit();
    assertTrue(Files.exists(dbfFile));

    Path parquetFile = Path.of(inputFile.toString() + ".parquet");
    parquetFile.toFile().deleteOnExit();
    assertTrue(Files.exists(parquetFile));

    Configuration config = new Configuration();
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.toString());
    InputFile hadoopInputFile = HadoopInputFile.fromPath(hadoopPath, config);

    try(InternalDbfReader dbfReader = new InternalDbfReader(Files.newInputStream(dbfFile));
        ParquetReader<Row> parquetReader = RowParquetReader.builder(hadoopInputFile).build()
    ) {
      assertConvertedFile(dbfReader, parquetReader);
    }
  }

  void assertConvertedFile(InternalDbfReader dbfReader, ParquetReader<Row> parquetReader) throws IOException {
    DBFRow dbfRow = dbfNextRow(dbfReader);

    while(dbfRow != null) {
      Row parquetRow = parquetReader.read();

      List<Object> parquetValues = parquetRow.getValues();

      for(int columnIndex = 0; columnIndex < dbfReader.schema.fields.length; ++columnIndex) {
        Object dbfValue = dbfRow.getObject(columnIndex);
        Object parquetValue = parquetValues.get(columnIndex);
        assertEquals(dbfValue, parquetValue);
      }

      dbfRow = dbfNextRow(dbfReader);
    }
  }

  DBFRow dbfNextRow(DBFReader dbfReader){
    try {
      return dbfReader.nextRow();
    } catch (DBFException dbfException) {
      if (dbfException.getCause() instanceof EOFException) {
        return null;
      }
      throw dbfException;
    }
  }

  @Test
  void convertFileToDirectoryTest() {

  }

  @Test
  void convertDirectoryToFileTest() {

  }

  @Test
  void convertDirectoryToDirectoryTest() {

  }

  @Test
  void convertDirectoryCombiningTest() {

  }

}
