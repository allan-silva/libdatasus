package br.gov.sus.opendata.dbf.parquet;

import br.gov.sus.opendata.dbc.DbcNativeDecompressor;
import br.gov.sus.opendata.dbf.parquet.InternalDbfReader.DbfSchema;
import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFRow;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

/**
 * {@link DbfParquet} converts a DBC/DBF file to parquet file.
 */
public class DbfParquet {
    private final static String EXTENSION = ".parquet";

    private final Set<ConvertTask> convertTasks;

    private final Consumer<Object> onProgress;

    private static final Logger logger = LogManager.getLogger(DbfParquet.class);

    private final Configuration conf;

    DbfParquet(Builder builder) {
        this.conf = builder.conf;
        this.convertTasks = builder.convertTasks;
        this.onProgress = Optional.ofNullable(builder.onProgress).orElse(this::logProgress);
    }

    /**
     * Converts the given input to parquet file.
     * The result file will be auto named. E.g.:
     * <pre>
     *         Input file PNA1212.dbc
     *         Results in PNA1212.dbc.parquet
     * </pre>
     *
     * <pre>
     *     Path inputFile = Path.of("file.dbc");
     *     DbfParquet dbfParquet = DbfParquet.builder().build();
     *     dbfParquet.convert(inputFile);
     * </pre>
     *
     * @param input file or directory to be converted.
     * @throws IOException
     */
    public void convert(Path input) throws IOException {
        Path output = Path.of(input.toString() + EXTENSION);

        if (Files.isDirectory(input)) {
            output = input;
        }

        convert(input, output);
    }

    /**
     * Converts the given input to parquet file.
     * If output is a directory the result file will be "file.dbc.parquet".
     *
     * <pre>
     *     Path inputFile = Path.of("file.dbc");
     *     Path outputFile = Path.of("converted.parquet");
     *     DbfParquet dbfParquet = DbfParquet.builder().build();
     *     dbfParquet.convert(inputFile, outputFile);
     * </pre>
     *
     * @param input  input file or directory.
     * @param output output file or directory.
     * @throws IOException
     */
    public void convert(Path input, Path output) throws IOException {
        convertTasks.add(ConvertTask.builder().input(input).output(output).build());
        convert();
    }

    /**
     * Execute the list of conversion tasks.
     * <pre>
     *         ConvertTask simpleFileTask = ConvertTask.builder()
     *                 .input(Path.of("/home/allan/teste/CIHADF1206.dbc"))
     *                 .output(Path.of("/home/allan/teste/CIHADF1206.dbc.parquet"))
     *                 .build();
     *
     *         ConvertTask convertFromDirectoryTask = ConvertTask.builder()
     *                 .input(Path.of("/home/allan/teste/inputDirectory"))
     *                 .output(Path.of("/home/allan/teste/outputDirectory"))
     *                 .build();
     *
     *         ConvertTask convertCombiningTask =
     *                 ConvertTask.builder()
     *                         .input(Path.of("/home/allan/teste/inputDirectory"))
     *                         .output(Path.of("/home/allan/teste/combined.parquet"))
     *                         .combineFiles()
     *                         .build();
     *
     *         DbfParquet dbfParquet = DbfParquet.builder()
     *                 .addConvertItem(simpleFileTask)
     *                 .addConvertItem(convertFromDirectoryTask)
     *                 .addConvertItem(convertCombiningTask)
     *                 .build();
     *
     *         dbfParquet.convert();
     * </pre>
     *
     * @throws IOException
     */
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
                                getInputStream(input), convertTask.getSchemaName()));
            }
        }

        return readers;
    }

    private void convertFile(ConvertTask convertTask) throws IOException {
        convertFile(convertTask.getInput(), convertTask.getOutput(), convertTask.getSchemaName());
    }

    private void convertFile(Path input, Path output, String schemaName) throws IOException {
        logger.info(String.format("File conversion started - %s => %s", input.toUri(), output.toUri()));
        try (InputStream inputStream = getInputStream(input);
             InternalDbfReader dbfReader = new InternalDbfReader(inputStream, schemaName);
             ParquetWriter<DBFRow> parquetWriter =
                     DbfParquetWriter.builder(getOutputFile(input, output))
                             .withDbfSchema(dbfReader.schema)
                             .withWriterVersion(WriterVersion.PARQUET_2_0)
                             .build()) {
            write(dbfReader, parquetWriter);
        }
    }

    private HadoopOutputFile getOutputFile(Path input, Path output) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath =
                new org.apache.hadoop.fs.Path(getOutputPath(input, output).toUri());
        Configuration hadoopConf = this.conf != null ? this.conf : new Configuration();
        FileSystem hadoopFS = hadoopPath.getFileSystem(hadoopConf);

        if(hadoopFS != null)
            logger.info(String.format("Hadoop output FS: %s", hadoopPath.getFileSystem(hadoopConf).getUri()));
        else
            logger.warn("Null Hadoop FS");

        return HadoopOutputFile.fromPath(hadoopPath, hadoopConf);
    }

    private Path getOutputPath(Path input, Path output) {
        if (Files.isDirectory(output)) {
            return output.resolve(input.getFileName() + EXTENSION);
        }
        return output;
    }

    private InputStream getInputStream(Path input) throws IOException {
        if (isCompressed(input)) {
            logger.info(String.format("File %s is compressed. Decompressing...", input.toUri()));
            Path inputFilePath = Paths.get(DbcNativeDecompressor.decompress(input).getOutputFileName());
            logger.info(String.format("Decompressed file: %s", inputFilePath.toUri()));
            inputFilePath.toFile().deleteOnExit();
            return Files.newInputStream(inputFilePath);
        }
        logger.info(String.format("File %s is not compressed", input.toUri()));
        return Files.newInputStream(input);
    }

    private void write(InternalDbfReader dbfReader, ParquetWriter<DBFRow> parquetWriter)
            throws IOException {

        DBFRow dbfRow = nextRow(dbfReader);
        int rowCount = 0;

        logger.info("Writing started");

        while (dbfRow != null) {
            parquetWriter.write(dbfRow);
            ++rowCount;
            dbfRow = nextRow(dbfReader);
        }

        logger.info(String.format("Write finished - %s row(s) converted", rowCount));
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

        private Configuration conf;

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

        public Builder withHadoopConf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public DbfParquet build() {
            return new DbfParquet(this);
        }
    }
}
