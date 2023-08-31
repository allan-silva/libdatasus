package br.gov.sus.opendata.dbf.parquet;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import br.gov.sus.opendata.dbf.parquet.DbfSchemaConverter.ParquetDefinition;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
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
        assertEquals(dbfReader.getField(id).getName(), field.getName());
      }
    }
  }

  @Test
  void testError() throws IOException {
    String dbc = TestUtils.getResourcePath("dbf/exaustive/CCSE0703.dbc");
    String dbf = TestUtils.decompressDBC(Path.of(dbc));
    DbfSchemaConverter schemaConverter = new DbfSchemaConverter();
    try (FileInputStream fis = new FileInputStream(dbc);
        InternalDbfReader dbfReader = new InternalDbfReader(fis, "ExaustiveDatasusTest")) {
      MessageType messageType = schemaConverter.convert(dbfReader.schema);
      messageType
          .getFields()
          .forEach(
              field -> {
                assertTrue(messageType.getFieldCount() > 0);
              });
    }
  }
}
