package br.gov.sus.opendata.dbf.parquet;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import br.gov.sus.opendata.dbf.parquet.DbfSchemaConverter.ParquetDefinition;
import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFField;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class DbfSchemaConverterTest {

  private final List<String> datasusRequiredRepetitionFields =
      asList(
          "ANO_DIAGN",
          "ANOMES_DIA",
          "ANO_TRATAM",
          "ANOMES_TRA",
          "UF_RESID",
          "MUN_RESID",
          "UF_TRATAM",
          "MUN_TRATAM",
          "UF_DIAGN",
          "MUN_DIAG",
          "TRATAMENTO",
          "DIAGNOSTIC",
          "IDADE",
          "SEXO",
          "ESTADIAM",
          "CNES_DIAG",
          "CNES_TRAT",
          "TEMPO_TRAT",
          "CNS_PAC",
          "DIAG_DETH",
          "DT_DIAG",
          "DT_TRAT",
          "DT_NASC");

  private final Map<String, ParquetDefinition> typesRequiredRepetitionFields =
      Map.of(
          "CHARACTER",
              ParquetDefinition.of(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()),
          "NUMERIC",
              ParquetDefinition.of(
                  PrimitiveTypeName.BINARY, LogicalTypeAnnotation.decimalType(2, 12)),
          "DATE", ParquetDefinition.of(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType()),
          "FLOAT", ParquetDefinition.of(PrimitiveTypeName.FLOAT),
          "LOGICAL", ParquetDefinition.of(PrimitiveTypeName.BOOLEAN));

  @Test
  void convert() throws IOException {
    DbfSchemaConverter schemaConverter = new DbfSchemaConverter();

    try (FileInputStream fis = new FileInputStream(TestUtils.getResourcePath("dbf/POBR2023.dbf"));
        InternalDbfReader dbfReader = new InternalDbfReader(fis, "PainelOncologia")) {

      MessageType messageType = schemaConverter.convert(dbfReader.schema);
      assertEquals(dbfReader.schema.name, messageType.getName());

      assertTrue(messageType.getFieldCount() > 0);

      for (int id = 0; id < messageType.getFieldCount(); ++id) {
        Type field = messageType.getFields().get(id);
        assertEquals(Repetition.REQUIRED, field.getRepetition());
        assertEquals(id, field.getId().intValue());
        assertTrue(datasusRequiredRepetitionFields.contains(field.getName()));
      }
    }
  }

  @Test
  void convertValidateType() throws IOException {
    DbfSchemaConverter schemaConverter = new DbfSchemaConverter();

    try (FileInputStream fis = new FileInputStream(TestUtils.getResourcePath("dbf/testTypes.dbf"));
        InternalDbfReader dbfReader = new InternalDbfReader(fis, "testTypes")) {
      MessageType messageType = schemaConverter.convert(dbfReader.schema);
      messageType
          .getFields()
          .forEach(
              field -> {
                assertEquals(Repetition.REQUIRED, field.getRepetition());
                assertTrue(typesRequiredRepetitionFields.containsKey(field.getName()));

                ParquetDefinition definition = typesRequiredRepetitionFields.get(field.getName());

                assertEquals(
                    definition.getPrimitiveTypeName(),
                    field.asPrimitiveType().getPrimitiveTypeName());

                if (definition.getLogicalTypeAnnotation() != null) {
                  assertNotNull(field.getLogicalTypeAnnotation());
                  assertEquals(
                      definition.getLogicalTypeAnnotation(), field.getLogicalTypeAnnotation());
                }
              });
    }
  }

  private static List<String> datasusFilesSource() {
    String directory = TestUtils.getResourcePath("dbf/exaustive");
    return TestUtils.listDbf(directory);
  }

  @ParameterizedTest
  @MethodSource("datasusFilesSource")
  void datasusExaustiveSchemaTest(String dbfFile) throws IOException {
    DbfSchemaConverter schemaConverter = new DbfSchemaConverter();
    try (FileInputStream fis = new FileInputStream(dbfFile);
        InternalDbfReader dbfReader = new InternalDbfReader(fis, "ExaustiveDatasusTest")) {
      MessageType messageType = schemaConverter.convert(dbfReader.schema);
      assertTrue(messageType.getFieldCount() > 0);

      for (int id = 0; id < messageType.getFieldCount(); ++id) {
        Type field = messageType.getFields().get(id);
        assertEquals(Repetition.REQUIRED, field.getRepetition());
        assertEquals(id, field.getId().intValue());

        DBFField dbfField = dbfReader.getField(id);

        assertEquals(dbfField.getName(), field.getName());

        switch (dbfField.getType()) {
          case CHARACTER:
            assertEquals(PrimitiveTypeName.BINARY, field.asPrimitiveType().getPrimitiveTypeName());
            assertEquals(LogicalTypeAnnotation.stringType(), field.getLogicalTypeAnnotation());
            break;
          case DATE:
            assertEquals(PrimitiveTypeName.INT32, field.asPrimitiveType().getPrimitiveTypeName());
            assertEquals(
                LogicalTypeAnnotation.dateType(),
                field.asPrimitiveType().getLogicalTypeAnnotation());
            break;
          case NUMERIC:
            assertEquals(PrimitiveTypeName.BINARY, field.asPrimitiveType().getPrimitiveTypeName());
            assertEquals(
                LogicalTypeAnnotation.decimalType(dbfField.getDecimalCount(), dbfField.getLength()),
                field.asPrimitiveType().getLogicalTypeAnnotation());
            break;
          case FLOATING_POINT:
            assertEquals(PrimitiveTypeName.FLOAT, field.asPrimitiveType().getPrimitiveTypeName());
            break;
          case LOGICAL:
            assertEquals(PrimitiveTypeName.BOOLEAN, field.asPrimitiveType().getPrimitiveTypeName());
          default:
            fail("Field type not yet supported: " + dbfField.getType().name());
        }
      }
    }
  }

  private static List<String> underInvestigationDatasusFilesSource() {
    String directory = TestUtils.getResourcePath("dbf/investigating");
    return TestUtils.listDbf(directory);
  }

  @ParameterizedTest
  @MethodSource("underInvestigationDatasusFilesSource")
  void investigationTest(String dbfFile) throws IOException {
    assertThrows(DBFException.class, () -> {
      DbfSchemaConverter schemaConverter = new DbfSchemaConverter();
      try (FileInputStream fis = new FileInputStream(dbfFile);
          InternalDbfReader dbfReader = new InternalDbfReader(fis, "ExaustiveDatasusTest")) {
        fail("No exception thrown");
      }
    });
  }
}
