package br.gov.sus.opendata.dbf.parquet;

import br.gov.sus.opendata.dbf.parquet.InternalDbfReader.DbfSchema;
import com.linuxense.javadbf.DBFRow;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

class DbfRowWriteSupport extends WriteSupport<DBFRow> {

  private final DbfSchema dbfSchema;

  private final Map<String, String> extraMetadata;

  private DbfRowWritter dbfRowWritter;

  private MessageType rootSchema;

  public DbfRowWriteSupport(DbfSchema dbfSchema, Map<String, String> extraMetadata) {
    this.dbfSchema = dbfSchema;
    this.extraMetadata = extraMetadata;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    rootSchema = new DbfSchemaConverter().convert(dbfSchema);
    return new WriteContext(rootSchema, extraMetadata);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    dbfRowWritter = new DbfRowWritter(dbfSchema, rootSchema, recordConsumer);
  }

  @Override
  public void write(DBFRow dbfRow) {
    dbfRowWritter.write(dbfRow);
  }
}
