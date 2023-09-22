package br.gov.sus.opendata.dbf.parquet;

import static org.junit.jupiter.api.Assertions.fail;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.util.Date;
import org.junit.jupiter.api.Test;

public class DbfExploratoryTest {

  @Test
  public void test() throws FileNotFoundException {

//    DBFField[] fields = new DBFField[5];
//
//    fields[0] = new DBFField();
//    fields[0].setName("CHARACTER");
//    fields[0].setType(DBFDataType.CHARACTER);
//    fields[0].setLength(10);
//
//    fields[1] = new DBFField();
//    fields[1].setName("NUMERIC");
//    fields[1].setType(DBFDataType.NUMERIC);
//    fields[1].setLength(12);
//    fields[1].setDecimalCount(2);
//
//    fields[2] = new DBFField();
//    fields[2].setName("DATE");
//    fields[2].setType(DBFDataType.DATE);
//
//    fields[3] = new DBFField();
//    fields[3].setName("FLOAT");
//    fields[3].setType(DBFDataType.FLOATING_POINT);
//    fields[3].setLength(12);
//    fields[3].setDecimalCount(2);
//
//    fields[4] = new DBFField();
//    fields[4].setName("LOGICAL");
//    fields[4].setType(DBFDataType.LOGICAL);
//
//    DBFWriter writer = new DBFWriter(new FileOutputStream("/home/allan/code.allan/testTypes.dbf"));
//    writer.setFields(fields);
//
//    Object[] rowData = new Object[5];
//    rowData[0] = "abcdefghij";
//    rowData[1] = BigDecimal.valueOf(42.024D);
//    rowData[2] = new Date(2023,1,30);
//    rowData[3] = 240.42f;
//    rowData[4] = true;
//
//    writer.addRecord(rowData);
//    writer.close();
  }
}
