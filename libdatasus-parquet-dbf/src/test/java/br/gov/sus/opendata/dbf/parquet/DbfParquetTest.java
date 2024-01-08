package br.gov.sus.opendata.dbf.parquet;

import com.exasol.parquetio.data.Row;
import com.exasol.parquetio.reader.RowParquetReader;
import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DbfParquetTest {

    private static Path testDir;

    @BeforeAll
    static void setup() throws IOException {
        testDir = TestUtils.createTempDir();
    }

    @AfterAll
    static void tearDown() {
        recursiveDeleteDirectory(testDir.toFile());
    }

    @ParameterizedTest
    @MethodSource("datasusFilesSource")
    void convertFileToFileTest(Path inputPathItem) throws IOException {
        Path copyTarget = testDir.resolve(inputPathItem.getFileName());
        Path inputFile = Files.copy(inputPathItem, copyTarget, StandardCopyOption.REPLACE_EXISTING);

        DbfParquet dbfParquet = DbfParquet.builder().build();
        dbfParquet.convert(inputFile);

        Path dbfFile = dbfFilePath(inputFile);
        assertTrue(Files.exists(dbfFile));

        Path parquetFile = Path.of(inputFile.toString() + ".parquet");
        assertTrue(Files.exists(parquetFile));

        assertConvertedFile(dbfFile, parquetFile);
    }

    @Test
    void convertFileToDirectoryTest() throws IOException {
        Path output = Files.createDirectory(testDir.resolve("output"));
        Path inputFile = Path.of(TestUtils.getResourcePath("dbf/conversion/exhaustive/CIHAAC1201.dbc"));
        inputFile = Files.copy(inputFile, testDir.resolve(inputFile.getFileName()), StandardCopyOption.REPLACE_EXISTING);

        DbfParquet dbfParquet = DbfParquet.builder().build();
        dbfParquet.convert(inputFile, output);

        Path dbfFile = dbfFilePath(inputFile);
        assertTrue(Files.exists(dbfFile));

        Path parquetFile = output.resolve(inputFile.getFileName() + ".parquet");
        assertTrue(Files.exists(parquetFile));

        assertConvertedFile(dbfFile, parquetFile);
    }


    @Test
    void convertDirectoryToFileTest() throws IOException {
        Path directoryTestDir = Files.createDirectory(testDir.resolve("directory"));
        Path directoryTestResourcesPath = Path.of(TestUtils.getResourcePath("dbf/conversion/directory"));
        try (Stream<Path> files = Files.list(directoryTestResourcesPath)) {
            for (Path filePath : files.toList()) {
                Files.copy(filePath, directoryTestDir.resolve(filePath.getFileName()));
            }
        }

        DbfParquet dbfParquet = DbfParquet.builder().build();
        dbfParquet.convert(directoryTestDir);

        assert (Files.exists(directoryTestDir.resolve("CIHAAM1104.dbc")));
        assert (Files.exists(directoryTestDir.resolve("HBRS2104.dbc")));
        assert (Files.exists(directoryTestDir.resolve("SRMS1205.dbc")));

        Path dbfA = directoryTestDir.resolve("CIHAAM1104.dbc.dbf");
        Path dbfB = directoryTestDir.resolve("HBRS2104.dbc.dbf");
        Path dbfC = directoryTestDir.resolve("SRMS1205.dbc.dbf");

        assert (Files.exists(dbfA));
        assert (Files.exists(dbfB));
        assert (Files.exists(dbfC));

        Path parquetA = directoryTestDir.resolve("CIHAAM1104.dbc.parquet");
        Path parquetB = directoryTestDir.resolve("HBRS2104.dbc.parquet");
        Path parquetC = directoryTestDir.resolve("SRMS1205.dbc.parquet");

        assert (Files.exists(parquetA));
        assert (Files.exists(parquetB));
        assert (Files.exists(parquetC));

        assertConvertedFile(dbfA, parquetA);
        assertConvertedFile(dbfB, parquetB);
        assertConvertedFile(dbfC, parquetC);
    }

    @Test
    void convertDirectoryCombiningTest() throws IOException {
        Path directoryTestDir = Files.createDirectory(testDir.resolve("directoryCombining"));
        Path directoryTestResourcesPath = Path.of(TestUtils.getResourcePath("dbf/conversion/directoryCombining"));
        try (Stream<Path> files = Files.list(directoryTestResourcesPath)) {
            for (Path filePath : files.toList()) {
                Files.copy(filePath, directoryTestDir.resolve(filePath.getFileName()));
            }
        }

        Path parquetFile = directoryTestDir.resolve("combined.parquet");
        DbfParquet dbfParquet = new DbfParquetCustomReaders(DbfParquet.builder()
                .addConvertItem(
                        ConvertTask.builder()
                                .input(directoryTestDir)
                                .output(parquetFile)
                                .combineFiles()
                                .build()));
        dbfParquet.convert();

        Configuration config = new Configuration();
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.toString());
        InputFile hadoopInputFile = HadoopInputFile.fromPath(hadoopPath, config);

        try(ParquetReader<Row> parquetReader = RowParquetReader.builder(hadoopInputFile).build()) {
            List<InternalDbfReader> readers = DbfParquetCustomReaders.createReadersCombined(directoryTestDir);
            for(InternalDbfReader dbfReader : readers) {
                assertConvertedFile(dbfReader, parquetReader);
            }
        }
    }



    private void assertConvertedFile(Path dbfFile, Path parquetFile) throws IOException {
        Configuration config = new Configuration();
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.toString());
        InputFile hadoopInputFile = HadoopInputFile.fromPath(hadoopPath, config);

        try (InternalDbfReader dbfReader = new InternalDbfReader(Files.newInputStream(dbfFile));
             ParquetReader<Row> parquetReader = RowParquetReader.builder(hadoopInputFile).build()
        ) {
            assertConvertedFile(dbfReader, parquetReader);
        }
    }

    private void assertConvertedFile(InternalDbfReader dbfReader, ParquetReader<Row> parquetReader) throws IOException {
        DBFRow dbfRow = dbfNextRow(dbfReader);

        while (dbfRow != null) {
            Row parquetRow = parquetReader.read();

            List<Object> parquetValues = parquetRow.getValues();

            for (int columnIndex = 0; columnIndex < dbfReader.schema.fields.length; ++columnIndex) {
                Object dbfValue = dbfRow.getObject(columnIndex);
                Object parquetValue = parquetValues.get(columnIndex);
                assertEquals(dbfValue, parquetValue);
            }

            dbfRow = dbfNextRow(dbfReader);
        }
    }

    private Path dbfFilePath(Path path) {
        if (TestUtils.isCompressedFile(path)) {
            return Path.of(path.toString() + ".dbf");
        }
        return path;
    }

    static Stream<Path> datasusFilesSource() throws IOException {
        return Files.list(Path.of(TestUtils.getResourcePath("dbf/conversion/exhaustive")));
    }

    private DBFRow dbfNextRow(DBFReader dbfReader) {
        try {
            return dbfReader.nextRow();
        } catch (DBFException dbfException) {
            if (dbfException.getCause() instanceof EOFException) {
                return null;
            }
            throw dbfException;
        }
    }

    private static void recursiveDeleteDirectory(File file) {
        File[] objects = file.listFiles();
        if (objects != null) {
            for (File object : objects) {
                recursiveDeleteDirectory(object);
            }
        }
        file.delete();
    }

    class DbfParquetCustomReaders extends DbfParquet {
        private DbfParquetCustomReaders(DbfParquet.Builder builder) {
            super(builder);
        }

        @Override
        List<InternalDbfReader> createReaders(ConvertTask convertTask) throws IOException {
            super.createReaders(convertTask);
            return createReadersCombined(convertTask.getInput());
        }

        public static List<InternalDbfReader> createReadersCombined(Path directory) throws IOException {
            List<InternalDbfReader> readers = new ArrayList<>();
            readers.add(new InternalDbfReader(
                    new FileInputStream(directory.resolve("CIHAAC1109.dbc.dbf").toFile()), "Combined"));
            readers.add(new InternalDbfReader(
                    new FileInputStream(directory.resolve("CIHAAC1112.dbc.dbf").toFile()), "Combined"));
            readers.add(new InternalDbfReader(
                    new FileInputStream(directory.resolve("CIHAAC1201.dbc.dbf").toFile()), "Combined"));
            readers.add(new InternalDbfReader(
                    new FileInputStream(directory.resolve("CIHAAL1203.dbc.dbf").toFile()), "Combined"));
            readers.add(new InternalDbfReader(
                    new FileInputStream(directory.resolve("CIHAMG2004.dbc.dbf").toFile()), "Combined"));
            readers.add(new InternalDbfReader(
                    new FileInputStream(directory.resolve("CIHASP2307.dbc.dbf").toFile()), "Combined"));
            return readers;
        }
    }
}
