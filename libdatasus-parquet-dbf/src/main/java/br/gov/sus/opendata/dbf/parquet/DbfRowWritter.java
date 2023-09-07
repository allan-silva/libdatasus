package br.gov.sus.opendata.dbf.parquet;

import br.gov.sus.opendata.dbf.parquet.InternalDbfReader.DbfSchema;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFRow;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Date;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;

class DbfRowWritter {

  private final DbfSchema dbfSchema;

  private final GroupType parquetSchema;

  private final RecordConsumer recordConsumer;

  private final FieldWriter[] writers;

  public DbfRowWritter(
      DbfSchema dbfSchema, GroupType parquetSchema, RecordConsumer recordConsumer) {
    this.dbfSchema = dbfSchema;
    this.parquetSchema = parquetSchema;
    this.recordConsumer = recordConsumer;
    this.writers = new FieldWriter[dbfSchema.fields.length];
    createWriters();
  }

  private void createWriters() {
    for (int i = 0; i < dbfSchema.fields.length; ++i) {
      DBFField dbfField = dbfSchema.fields[i];
      String fieldName = dbfField.getName();
      int parquetIndex = parquetSchema.getFieldIndex(dbfField.getName());

      switch (dbfField.getType()) {
        case CHARACTER:
          writers[i] = new CharacterWriter(fieldName, parquetIndex);
          break;
        case DATE:
          writers[i] = new DateWriter(fieldName, parquetIndex);
          break;
        case NUMERIC:
          writers[i] = new NumericWriter(fieldName, parquetIndex);
          break;
        case FLOATING_POINT:
          writers[i] = new FloatWriter(fieldName, parquetIndex);
          break;
        case LOGICAL:
          writers[i] = new LogicalWriter(fieldName, parquetIndex);
          break;
        default:
          throw new UnsupportedOperationException(
              "Cannot convert DBF file: unknown type " + dbfField.getType().name());
      }
    }
  }

  public void write(DBFRow dbfRow) {
    recordConsumer.startMessage();
    writeFields(dbfRow);
    recordConsumer.endMessage();
  }

  private void writeFields(DBFRow dbfRow) {
    for (FieldWriter fieldWriter : writers) {
      fieldWriter.writeField(dbfRow.getObject(fieldWriter.getFieldName()));
    }
  }

  /* Inspired by Protobuf parquet-mr writers. */

  abstract class FieldWriter {
    private final String fieldName;

    private final int index;

    public FieldWriter(String fieldName, int index) {
      this.fieldName = fieldName;
      this.index = index;
    }

    void writeField(Object value) {
      recordConsumer.startField(getFieldName(), getIndex());
      write(value);
      recordConsumer.endField(getFieldName(), getIndex());
    }

    abstract void write(Object value);

    public String getFieldName() {
      return fieldName;
    }

    public int getIndex() {
      return index;
    }
  }

  class CharacterWriter extends FieldWriter {

    public CharacterWriter(String fieldName, int index) {
      super(fieldName, index);
    }

    @Override
    void write(Object value) {
      Binary bynaryString = Binary.fromString((String) value);
      recordConsumer.addBinary(bynaryString);
    }
  }

  class NumericWriter extends FieldWriter {

    public NumericWriter(String fieldName, int index) {
      super(fieldName, index);
    }

    @Override
    void write(Object value) {
      assert value instanceof BigDecimal;
      Binary numericBinary =
          Binary.fromConstantByteArray(((BigDecimal) value).unscaledValue().toByteArray());
      recordConsumer.addBinary(numericBinary);
    }
  }

  class DateWriter extends FieldWriter {

    public DateWriter(String fieldName, int index) {
      super(fieldName, index);
    }

    @Override
    void write(Object value) {
      assert value instanceof Date;
      Date date = (Date) value;
      int epochDay =
          (int) date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate().toEpochDay();
      recordConsumer.addInteger(epochDay);
    }
  }

  class LogicalWriter extends FieldWriter {

    public LogicalWriter(String fieldName, int index) {
      super(fieldName, index);
    }

    @Override
    void write(Object value) {
      recordConsumer.addBoolean((Boolean) value);
    }
  }

  class FloatWriter extends FieldWriter {

    public FloatWriter(String fieldName, int index) {
      super(fieldName, index);
    }

    @Override
    void write(Object value) {
      recordConsumer.addFloat(((Number) value).floatValue());
    }
  }
}
