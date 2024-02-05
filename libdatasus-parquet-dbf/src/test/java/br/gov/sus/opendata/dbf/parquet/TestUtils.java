package br.gov.sus.opendata.dbf.parquet;

import br.gov.sus.opendata.dbc.DbcNativeDecompressor;
import br.gov.sus.opendata.dbc.DbcNativeDecompressor.DecompressStats;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TestUtils {

  public static final String TEST_TEMP_DIR = "libdatasustesttempdir";

  public static String getResourcePath(String resourceName) {
    return TestUtils.class.getClassLoader().getResource(resourceName).getPath();
  }

  public static List<String> listDbf(String directory) throws IOException {
    File file = new File(directory);
    assert file.isDirectory();

    String[] files = file.list();
    assert files != null;

    Path tmpDecompressPath = createTempDir();

    return Arrays.stream(files)
        .filter(
            fileName ->
                fileName.toLowerCase().contains(".dbf") || fileName.toLowerCase().contains(".dbc"))
        .map(fileName -> Paths.get(directory, fileName))
        .filter(Files::isRegularFile)
        .map(
            path -> {
              if (path.toString().contains(".dbc")) {
                try {
                  return decompressDBC(path, tmpDecompressPath);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
              return path.toString();
            })
            .collect(Collectors.toList());
  }

  public static String decompressDBC(Path dbcPath, Path tmpDecompressPath) throws IOException {
    Path outputFile = tmpDecompressPath.resolve(dbcPath.getFileName() + ".dbf");
    outputFile.toFile().deleteOnExit();
    DecompressStats stats = new DecompressStats();
    DbcNativeDecompressor.decompressTo(dbcPath.toString(), outputFile.toString(), stats);
    return stats.getOutputFileName();
  }

  public static Path createTempDir() throws IOException {
    Path tmpDecompressPath = Files.createTempDirectory(TEST_TEMP_DIR);
    tmpDecompressPath.toFile().deleteOnExit();
    return tmpDecompressPath;
  }

  public static boolean isCompressedFile(Path path) {
    return path.toString().toLowerCase().endsWith(".dbc");
  }
}
