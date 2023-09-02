package br.gov.sus.opendata.dbf.parquet;

import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import java.io.InputStream;

class InternalDbfReader extends DBFReader {

  public final DbfSchema schema;

  InternalDbfReader(InputStream in) {
    this(in, "DBFFile");
  }

  InternalDbfReader(InputStream in, String schemaName) {
    super(in);
    schema = new DbfSchema(this, schemaName);
  }

  public static class DbfSchema {
    public final String name;

    public final DBFField[] fields;

    DbfSchema(DBFReader dbfReader, String schemaName) {
      this.name = schemaName;

      fields = new DBFField[dbfReader.getFieldCount()];
      for (int fieldIndex = 0; fieldIndex < fields.length; ++fieldIndex) {
        fields[fieldIndex] = dbfReader.getField(fieldIndex);
      }
    }
  }
}
