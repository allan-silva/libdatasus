package br.gov.sus.opendata.dbc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DbcNativeDecompressor {
  private static final String PLATFORM = System.getProperty("os.name", "Linux");

  static {
    try {
      NativeLoader.load();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static native void decompress(String inputFile, DecompressStats decompressStats);

  public static native void decompressTo(
      String inputFile, String outputFile, DecompressStats decompressStats);

  public static DecompressStats decompress(String inputFile) {
    Path inputFilePath = Paths.get(inputFile);
    return decompress(inputFilePath);
  }

  public static DecompressStats decompress(String inputFile, String outputFile) {
    Path inputFilePath = Paths.get(inputFile);
    Path outputFilePath = Paths.get(outputFile);
    return decompress(inputFilePath, outputFilePath);
  }

  public static List<DecompressStats> decompressFromDirectory(String inputDirectory) {
    Path directoryPath = Paths.get(inputDirectory);
    return decompressFromDirectory(directoryPath);
  }

  public static List<DecompressStats> decompressFromDirectory(Path directoryPath) {
    if (!directoryPath.toFile().isDirectory()) {
      throw new IllegalArgumentException("`inputDirectory` is not a directory");
    }

    List<DecompressStats> stats = new ArrayList<>();

    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directoryPath, "*.dbc")) {
      directoryStream.forEach(
          path -> {
            if (path.toFile().isFile()) {
              stats.add(decompress(path));
            }
          });
    } catch (Exception e) {
      stats.add(
          new DecompressStats() {
            {
              setError(e.getMessage());
            }
          });
    }

    return stats;
  }

  public static DecompressStats decompress(Path inputFilePath) {
    assertFile(inputFilePath);

    DecompressStats decompressStats = new DecompressStats();
    decompress(inputFilePath.toString(), decompressStats);

    return decompressStats;
  }

  public static DecompressStats decompress(Path inputFilePath, Path outputFilePath) {
    assertFile(inputFilePath);

    DecompressStats decompressStats = new DecompressStats();
    decompressTo(inputFilePath.toString(), outputFilePath.toString(), decompressStats);

    return decompressStats;
  }

  private static void assertFile(Path path) {
    if (!path.toFile().isFile()) {
      throw new IllegalArgumentException("`inputFile` is not a file");
    }
  }

  public static class DecompressStats {
    private long inputFileSize;

    private String inputFileName;

    private long outputFileSize;

    private String outputFileName;

    private long decompressTime;

    private int decompressStatusCode;

    private String error;

    public long getInputFileSize() {
      return inputFileSize;
    }

    public void setInputFileSize(long inputFileSize) {
      this.inputFileSize = inputFileSize;
    }

    public String getInputFileName() {
      return inputFileName;
    }

    public void setInputFileName(String inputFileName) {
      this.inputFileName = inputFileName;
    }

    public long getOutputFileSize() {
      return outputFileSize;
    }

    public void setOutputFileSize(long outputFileSize) {
      this.outputFileSize = outputFileSize;
    }

    public String getOutputFileName() {
      return outputFileName;
    }

    public void setOutputFileName(String outputFileName) {
      this.outputFileName = outputFileName;
    }

    public long getDecompressTime() {
      return decompressTime;
    }

    public void setDecompressTime(long decompressTime) {
      this.decompressTime = decompressTime;
    }

    public int getDecompressStatusCode() {
      return decompressStatusCode;
    }

    public void setDecompressStatusCode(int decompressStatusCode) {
      this.decompressStatusCode = decompressStatusCode;
    }

    public String getError() {
      return error;
    }

    public void setError(String error) {
      this.error = error;
    }
  }

  private static class NativeLoader {
    private static void load() throws IOException {
      if (!PLATFORM.equalsIgnoreCase("LINUX")) throw new PlatformNotSupportedException();

      String[] nativeLibs = extractLibs();
      for (String lib : nativeLibs) {
        System.load(lib);
      }
    }

    private static String[] extractLibs() throws IOException {
      File tempDir = Files.createTempDirectory("datasus-dbc").toFile();
      tempDir.deleteOnExit();

      File tempNativeLib = new File(tempDir, "libblast_middleware_rs.so");
      tempNativeLib.deleteOnExit();

      try (InputStream nativeLibResouce =
              Thread.currentThread()
                  .getContextClassLoader()
                  .getResourceAsStream(
                      "native/x86_64-unknown-linux-gnu/libblast_middleware_rs.so");
          OutputStream nativeLibOutputStream = new FileOutputStream(tempNativeLib)) {
        assert nativeLibResouce != null;
        nativeLibOutputStream.write(nativeLibResouce.readAllBytes());

        String[] loadedLibs = new String[1];
        loadedLibs[0] = tempNativeLib.getAbsolutePath();
        return loadedLibs;
      }
    }

    static class PlatformNotSupportedException extends Error {
      PlatformNotSupportedException() {
        super(
            PLATFORM
                + " platform is not supported. Do you want contribute? see: https://github.com/allan-silva/parquet-datasus/issues/1");
      }
    }
  }
}
