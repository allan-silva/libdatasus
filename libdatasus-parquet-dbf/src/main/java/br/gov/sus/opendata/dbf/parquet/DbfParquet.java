package br.gov.sus.opendata.dbf.parquet;

import br.gov.sus.opendata.dbc.DbcNativeDecompressor;
import br.gov.sus.opendata.dbf.parquet.InternalDbfReader.DbfSchema;
import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFRow;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;

public class DbfParquet {
    private final static String EXTENSION = ".parquet";

    private final Set<ConvertTask> convertTasks;

    private final Consumer<Object> onProgress;

    DbfParquet(Builder builder) {
        this.convertTasks = builder.convertTasks;
        this.onProgress = Optional.ofNullable(builder.onProgress).orElse(this::logProgress);
    }

    public void convert(Path input) throws IOException {
        Path output = Path.of(input.toString() + EXTENSION);

        if(Files.isDirectory(input)) {
            output = input;
        }

        convert(input, output);
    }

    public void convert(Path input, Path output) throws IOException {
        convertTasks.add(ConvertTask.builder().input(input).output(output).build());
        convert();
    }

    public void convert() throws IOException {
        for (ConvertTask convertTask : convertTasks) {
            if (Files.isDirectory(convertTask.getInput())) {
                convertFromDirectory(convertTask);
                continue;
            }
            convertFile(convertTask);
        }
    }

    private void convertFromDirectory(ConvertTask convertTask) throws IOException {
        if (convertTask.combine()) {
            convertCombining(convertTask);
            return;
        }

        try (DirectoryStream<Path> directoryStream =
                     Files.newDirectoryStream(convertTask.getInput(), this::isSupportedFile)) {
            for (Path input : directoryStream) {
                convertFile(input, convertTask.getOutput().resolve(input.getFileName() + EXTENSION), convertTask.getSchemaName());
            }
        }
    }

    private void convertCombining(ConvertTask convertTask) throws IOException {
        List<InternalDbfReader> readers = createReaders(convertTask);
        if (readers.isEmpty()) return;

        String schemaName =
                Optional.ofNullable(convertTask.getSchemaName()).orElse(DbfSchema.DEFAULT_SCHEMA_NAME);
        DbfSchema combinedSchema = createCombinedSchema(readers, schemaName);

        try (ParquetWriter<DBFRow> parquetWriter =
                     DbfParquetWriter.builder(convertTask.getOutput().toString())
                             .withDbfSchema(combinedSchema)
                             .withValidation(false)
                             .build()) {

            for (InternalDbfReader reader : readers) {
                write(reader, parquetWriter);
            }
        }
    }

    private DbfSchema createCombinedSchema(List<InternalDbfReader> readers, String schemaName) {
        LinkedHashMap<String, DBFField> fields = new LinkedHashMap<>();

        for (InternalDbfReader reader : readers) {
            for (DBFField dbfField : reader.schema.fields) {
                if (fields.containsKey(dbfField.getName())) {
                    // TODO: Log
                    continue;
                }

                fields.put(dbfField.getName(), dbfField);
            }
        }

        return DbfSchema.of(schemaName, fields.values().toArray(DBFField[]::new));
    }

    List<InternalDbfReader> createReaders(ConvertTask convertTask) throws IOException {
        List<InternalDbfReader> readers = new ArrayList<>();

        try (DirectoryStream<Path> directoryStream =
                     Files.newDirectoryStream(convertTask.getInput(), this::isSupportedFile)) {

            for (Path input : directoryStream) {
                readers.add(
                        new InternalDbfReader(
                                new FileInputStream(getInputFile(input)), convertTask.getSchemaName()));
            }
        }

        return readers;
    }

    private void convertFile(ConvertTask convertTask) throws IOException {
        convertFile(convertTask.getInput(), convertTask.getOutput(), convertTask.getSchemaName());
    }

    private void convertFile(Path input, Path output, String schemaName) throws IOException {
        try (FileInputStream fis = new FileInputStream(getInputFile(input));
             InternalDbfReader dbfReader = new InternalDbfReader(fis, schemaName);
             ParquetWriter<DBFRow> parquetWriter =
                     DbfParquetWriter.builder(getOutputPath(input, output).toString())
                             .withDbfSchema(dbfReader.schema)
                             .withWriterVersion(WriterVersion.PARQUET_2_0)
                             .build()) {
            write(dbfReader, parquetWriter);
        }
    }

    private Path getOutputPath(Path input, Path output) {
        if (Files.isDirectory(output)) {
            return output.resolve(input.getFileName() + EXTENSION);
        }
        return output;
    }

    private File getInputFile(Path input) {
        if (isCompressed(input)) {
            File inputFile = Path.of(DbcNativeDecompressor.decompress(input).getOutputFileName()).toFile();
            inputFile.deleteOnExit();
            return inputFile;
        }
        return input.toFile();
    }

    private void write(InternalDbfReader dbfReader, ParquetWriter<DBFRow> parquetWriter)
            throws IOException {

        DBFRow dbfRow = nextRow(dbfReader);
        int rowCount = 0;

        while (dbfRow != null) {
            parquetWriter.write(dbfRow);
            ++rowCount;
            dbfRow = nextRow(dbfReader);
        }
    }

    private DBFRow nextRow(InternalDbfReader dbfReader) {
        try {
            return dbfReader.nextRow();
        } catch (DBFException dbfException) {
            if (dbfException.getCause() instanceof EOFException) {
                return null;
            }
            throw dbfException;
        }
    }

    private boolean isCompressed(Path path) {
        String sPath = path.toString().toLowerCase();
        return sPath.endsWith(".dbc");
    }

    private boolean isSupportedFile(Path path) {
        String sPath = path.toString().toLowerCase();
        return Files.isRegularFile(path) && (sPath.endsWith(".dbc") || sPath.endsWith(".dbf"));
    }

    private void logProgress(Object o) {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final Set<ConvertTask> convertTasks = new HashSet<>();

        private Consumer<Object> onProgress;

        public Builder addConvertItem(String source) {
            return addConvertItem(Path.of(source));
        }

        public Builder addConvertItem(String source, String destination) {
            return addConvertItem(Path.of(source), Path.of(destination));
        }

        public Builder addConvertItem(Path source) {
            addConvertItem(ConvertTask.builder().input(source).build());
            return this;
        }

        public Builder addConvertItem(Path source, Path destination) {
            addConvertItem(ConvertTask.builder().input(source).output(destination).build());
            return this;
        }

        public Builder addConvertItem(ConvertTask convertTask) {
            this.convertTasks.add(convertTask);
            return this;
        }

        public Builder onProgress(Consumer<Object> onProgress) {
            this.onProgress = onProgress;
            return this;
        }

        public DbfParquet build() {
            return new DbfParquet(this);
        }
    }
}
