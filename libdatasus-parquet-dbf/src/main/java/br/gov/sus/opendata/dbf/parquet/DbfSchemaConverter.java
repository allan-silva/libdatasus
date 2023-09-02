package br.gov.sus.opendata.dbf.parquet;

import br.gov.sus.opendata.dbf.parquet.InternalDbfReader.DbfSchema;
import com.linuxense.javadbf.DBFField;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.GroupBuilder;

class DbfSchemaConverter {
  public MessageType convert(DbfSchema dbfSchema) {
    GroupBuilder<MessageType> groupBuilder = Types.buildMessage();
    groupBuilder = convertFields(groupBuilder, dbfSchema.fields);
    return groupBuilder.named(dbfSchema.name);
  }

  private <T> GroupBuilder<T> convertFields(GroupBuilder<T> groupBuilder, DBFField[] dbfFields) {
    for (int id = 0; id < dbfFields.length; ++id) {
      DBFField dbfField = dbfFields[id];
      ParquetDefinition parquetDefinition = getParquetType(dbfField);
      groupBuilder =
          groupBuilder
              .primitive(parquetDefinition.getPrimitiveTypeName(), Repetition.REQUIRED)
              .as(parquetDefinition.getLogicalTypeAnnotation())
              .id(id)
              .named(dbfField.getName());
    }
    return groupBuilder;
  }

  private ParquetDefinition getParquetType(DBFField dbfField) {
    switch (dbfField.getType()) {
      case CHARACTER:
        return ParquetDefinition.of(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
      case DATE:
        return ParquetDefinition.of(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType());
      case NUMERIC:
        return ParquetDefinition.of(
            PrimitiveTypeName.BINARY,
            LogicalTypeAnnotation.decimalType(dbfField.getDecimalCount(), dbfField.getLength()));
      case FLOATING_POINT:
        return ParquetDefinition.of(PrimitiveTypeName.FLOAT);
      case LOGICAL:
        return ParquetDefinition.of(PrimitiveTypeName.BOOLEAN);
      default:
        throw new UnsupportedOperationException(
            "Cannot convert DBF file: unknown type " + dbfField.getType().name());
    }
  }

  static class ParquetDefinition {
    private final PrimitiveTypeName primitiveTypeName;
    private final LogicalTypeAnnotation logicalTypeAnnotation;

    ParquetDefinition(
        PrimitiveTypeName primitiveTypeName, LogicalTypeAnnotation logicalTypeAnnotation) {
      this.primitiveTypeName = primitiveTypeName;
      this.logicalTypeAnnotation = logicalTypeAnnotation;
    }

    static ParquetDefinition of(
        PrimitiveTypeName primitiveTypeName, LogicalTypeAnnotation logicalTypeAnnotation) {
      return new ParquetDefinition(primitiveTypeName, logicalTypeAnnotation);
    }

    static ParquetDefinition of(PrimitiveTypeName primitiveTypeName) {
      return of(primitiveTypeName, null);
    }

    public PrimitiveTypeName getPrimitiveTypeName() {
      return primitiveTypeName;
    }

    public LogicalTypeAnnotation getLogicalTypeAnnotation() {
      return logicalTypeAnnotation;
    }
  }
}
