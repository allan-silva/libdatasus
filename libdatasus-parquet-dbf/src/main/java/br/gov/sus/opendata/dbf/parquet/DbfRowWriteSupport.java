package br.gov.sus.opendata.dbf.parquet;

import com.linuxense.javadbf.DBFRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;

public class DbfRowWriteSupport extends WriteSupport<DBFRow> {
  @Override
  public WriteContext init(Configuration configuration) {
    return null;
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {}

  @Override
  public void write(DBFRow dbfRow) {}
}
