package br.gov.sus.opendata.dbf.parquet;

import br.gov.sus.opendata.dbf.parquet.InternalDbfReader.DbfSchema;
import com.linuxense.javadbf.DBFRow;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

public class DbfParquetWriter extends ParquetWriter<DBFRow> {

  public DbfParquetWriter(
      Path file,
      WriteSupport<DBFRow> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion,
      Configuration conf)
      throws IOException {
    super(
        file,
        writeSupport,
        compressionCodecName,
        blockSize,
        pageSize,
        dictionaryPageSize,
        enableDictionary,
        validating,
        writerVersion,
        conf);
  }

  public static Builder builder(String path) {
    return builder(new Path(path));
  }

  public static Builder builder(Path file) {
    return new Builder(file);
  }

  public static Builder builder(OutputFile file) {
    return new Builder(file);
  }

  public static class Builder extends ParquetWriter.Builder<DBFRow, Builder> {

    private DbfSchema dbfSchema = null;

    private Map<String, String> extraMetaData = new HashMap<>();

    protected Builder(Path path) {
      super(path);
    }

    protected Builder(OutputFile path) {
      super(path);
    }

    @Override
    protected Builder self() {
      return this;
    }

    public DbfParquetWriter.Builder withDbfSchema(DbfSchema dbfSchema) {
      this.dbfSchema = dbfSchema;
      return this;
    }

    public DbfParquetWriter.Builder withExtraMetaData(Map<String, String> extraMetaData) {
      this.extraMetaData = extraMetaData;
      return this;
    }

    @Override
    protected WriteSupport<DBFRow> getWriteSupport(Configuration configuration) {
      return new DbfRowWriteSupport(dbfSchema, extraMetaData);
    }

    @Override
    public ParquetWriter<DBFRow> build() throws IOException {
      withWriteMode(ParquetFileWriter.Mode.OVERWRITE);
      return super.build();
    }
  }
}
