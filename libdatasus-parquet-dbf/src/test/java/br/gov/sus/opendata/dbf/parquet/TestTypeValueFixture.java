package br.gov.sus.opendata.dbf.parquet;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

class TestTypeValueFixture {
  public static final FieldFixtureDefinition<String> CHARACTER_FIELD = dbfCharacter();

  public static final FieldFixtureDefinition<BigDecimal> NUMERIC_FIELD = dbfNumeric();

  public static final FieldFixtureDefinition<Date> DATE_FIELD = dbfDate();

  public static final FieldFixtureDefinition<Float> FLOAT_FIELD = dbfFloat();

  public static final FieldFixtureDefinition<Boolean> LOGICAL_FIELD = dbfLogical();

  public static final FieldFixtureDefinition[] FIELDS =
      new FieldFixtureDefinition[] {
        CHARACTER_FIELD, NUMERIC_FIELD, DATE_FIELD, FLOAT_FIELD, LOGICAL_FIELD
      };

  public static String createDbf() throws IOException {
    File tempFile = File.createTempFile("libdatasus", "testTypes.dbf");
    tempFile.deleteOnExit();

    try (FileOutputStream fos = new FileOutputStream(tempFile);
        DBFWriter dbfWriter = new DBFWriter(fos)) {
      dbfWriter.setFields(
          Arrays.stream(FIELDS).map(FieldFixtureDefinition::getField).toArray(DBFField[]::new));

      dbfWriter.addRecord(
          Arrays.stream(FIELDS).map(FieldFixtureDefinition::getValue).toArray(Object[]::new));
    }

    return tempFile.getPath();
  }

  private static FieldFixtureDefinition<String> dbfCharacter() {
    DBFField dbfField = new DBFField();
    dbfField.setName("CHARACTER");
    dbfField.setType(DBFDataType.CHARACTER);
    dbfField.setLength(30);

    return new FieldFixtureDefinition<>(dbfField, 0, "A vida, o universo e tudo mais");
  }

  private static FieldFixtureDefinition<BigDecimal> dbfNumeric() {
    DBFField dbfField = new DBFField();
    dbfField.setName("NUMERIC");
    dbfField.setType(DBFDataType.NUMERIC);
    dbfField.setLength(8);
    dbfField.setDecimalCount(2);

    return new FieldFixtureDefinition<>(dbfField, 1, BigDecimal.valueOf(4200024, 2));
  }

  private static FieldFixtureDefinition<Date> dbfDate() {
    DBFField dbfField = new DBFField();
    dbfField.setName("DATE");
    dbfField.setType(DBFDataType.DATE);

    Calendar cal = Calendar.getInstance();
    cal.set(2023, Calendar.JANUARY, 31, 0,0,0);
    cal.clear(Calendar.MILLISECOND);
    return new FieldFixtureDefinition<>(dbfField, 2, cal.getTime());
  }

  private static FieldFixtureDefinition<Float> dbfFloat() {
    DBFField dbfField = new DBFField();
    dbfField.setName("FLOAT");
    dbfField.setType(DBFDataType.FLOATING_POINT);
    dbfField.setLength(8);
    dbfField.setDecimalCount(2);

    return new FieldFixtureDefinition<>(dbfField, 3, 24000.42F);
  }

  private static FieldFixtureDefinition<Boolean> dbfLogical() {
    DBFField dbfField = new DBFField();
    dbfField.setName("LOGICAL");
    dbfField.setType(DBFDataType.LOGICAL);

    return new FieldFixtureDefinition<>(dbfField, 4, true);
  }

  static class FieldFixtureDefinition<T> {
    private final int index;

    private final DBFField field;

    private final T value;

    private FieldFixtureDefinition(DBFField field, int index, T value) {
      this.field = field;
      this.index = index;
      this.value = value;
    }

    public int getIndex() {
      return index;
    }

    public DBFField getField() {
      return field;
    }

    public T getValue() {
      return value;
    }
  }
}
