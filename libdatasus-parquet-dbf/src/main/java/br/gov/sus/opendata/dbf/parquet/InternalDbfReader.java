package br.gov.sus.opendata.dbf.parquet;

import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

class InternalDbfReader extends DBFReader {

  public final DbfSchema schema;

  InternalDbfReader(InputStream in) {
    this(in, null);
  }

  InternalDbfReader(InputStream in, String schemaName) {
    super(in);
    schema =
        new DbfSchema(this, Optional.ofNullable(schemaName).orElse(DbfSchema.DEFAULT_SCHEMA_NAME));
  }

  public static class DbfSchema {

    public static final String DEFAULT_SCHEMA_NAME = "DBFFile";

    public String name;

    public DBFField[] fields;

    DbfSchema(DBFReader dbfReader, String schemaName) {
      this.name = schemaName;

      fields = new DBFField[dbfReader.getFieldCount()];
      for (int fieldIndex = 0; fieldIndex < fields.length; ++fieldIndex) {
        fields[fieldIndex] = dbfReader.getField(fieldIndex);
      }
    }

    public DbfSchema(String name, DBFField[] fields) {}

    public static DbfSchema of(String name, DBFField[] fields) {
      return new DbfSchema(name, fields);
    }
  }
}
