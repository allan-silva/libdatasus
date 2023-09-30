/**
 * Copyright 2023 Allan Silva (allan [at] allansilva [dot] com [dot] br)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

/**
 * {@link DbcNativeDecompressor} provides helper functions to extract DBF files from DBC files. <br>
 * <br>
 * Platform specific note: Currently {@link DbcNativeDecompressor} only works on Linux see: <a
 * href="https://github.com/allan-silva/parquet-datasus/issues/1">https://github.com/allan-silva/parquet-datasus/issues/1</a>
 */
public class DbcNativeDecompressor {
  private static final String PLATFORM = System.getProperty("os.name", "Linux");

  static {
    try {
      NativeLoader.load();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Behaves like {@link DbcNativeDecompressor#decompress(Path)}, except this function does not
   * throw {@link IllegalArgumentException} if path is not a file. <br>
   * This is the JNI function interface with libblast-middleware. A reference object of type {@link
   * DecompressStats} must be provided, and will be filled by the JNI implementation.
   *
   * @param inputFile input file name
   * @param decompressStats A {@link DecompressStats} object reference.
   */
  public static native void decompress(String inputFile, DecompressStats decompressStats);

  /**
   * Behaves like {@link DbcNativeDecompressor#decompress(Path, Path)}, except this function does
   * not throw {@link IllegalArgumentException} if path is not a file. <br>
   * This is the JNI function interface with libblast-middleware. A reference object of type {@link
   * DecompressStats} must be provided, and will be filled by the JNI implementation.
   *
   * @param inputFile input file name
   * @param outputFile output file name
   * @param decompressStats A {@link DecompressStats} object reference.
   */
  public static native void decompressTo(
      String inputFile, String outputFile, DecompressStats decompressStats);

  /**
   * Behaves like {@link DbcNativeDecompressor#decompress(Path)} except this function takes an input
   * file as {@link String}.
   *
   * @param inputFile Input file name.
   */
  public static DecompressStats decompress(String inputFile) {
    Path inputFilePath = Paths.get(inputFile);
    return decompress(inputFilePath);
  }

  /**
   * Behaves like {@link DbcNativeDecompressor#decompress(Path, Path)} except this function takes an
   * input file and output file as {@link String}.
   *
   * @param inputFile Input file name.
   * @param outputFile Output file name.
   */
  public static DecompressStats decompress(String inputFile, String outputFile) {
    Path inputFilePath = Paths.get(inputFile);
    Path outputFilePath = Paths.get(outputFile);
    return decompress(inputFilePath, outputFilePath);
  }

  /**
   * Behaves like {@link DbcNativeDecompressor#decompressFromDirectory(Path)} except this function
   * takes an input directory as {@link String}.
   *
   * @param inputDirectory directory containing DBC files.
   * @return A {@link List} of {@link DecompressStats} object with information about decompress
   *     process for each processed file.
   */
  public static List<DecompressStats> decompressFromDirectory(String inputDirectory) {
    Path directoryPath = Paths.get(inputDirectory);
    return decompressFromDirectory(directoryPath);
  }

  /**
   * Decompress DBC files to DBF files from <code>directoryPath</code>. <br>
   * Each output file derives from respective input file name found in <code>directoryPath</code>
   * and the <code>.dbf</code> extension will be appended to input file name. <br>
   *
   * <pre>
   *   Example:
   *   Input file: /path-to-dbc/file.dbc
   *   Output file: /path-to-dbc/file.dbc.dbf
   * </pre>
   *
   * <br>
   * Note: This function will not visit subdirectories and uses a naive approach to detect DBC files
   * the file magic number will not be tested, it is just takes the files with <code>.dbc</code>
   * extension.
   *
   * @param directoryPath directory containing DBC files.
   * @return A {@link List} of {@link DecompressStats} object with information about decompress
   *     process for each processed file.
   * @throws IllegalArgumentException if <code>directoryPath</code> is not a directory.
   */
  public static List<DecompressStats> decompressFromDirectory(Path directoryPath) {
    if (!directoryPath.toFile().isDirectory()) {
      throw new IllegalArgumentException("`inputDirectory` is not a directory");
    }

    List<DecompressStats> stats = new ArrayList<>();

    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directoryPath)) {
      directoryStream.forEach(
          path -> {
            if (path.toFile().isFile() && path.toString().toLowerCase().endsWith(".dbc")) {
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

  /**
   * Decompress a DBC file resulting in a DBF file output. The output file derives from input file
   * name and the <code>.dbf</code> extension will be appended to inputfile name. <br>
   *
   * <pre>
   *   Example:
   *   Input file: /path-to-dbc/file.dbc
   *   Output file: /path-to-dbc/file.dbc.dbf
   * </pre>
   *
   * <br>
   * It is possible get output file name through {@link DecompressStats#getOutputFileName()} field.
   *
   * @param inputFilePath {@link Path} to dbc file.
   * @return A {@link DecompressStats} object with information about decompress process.
   * @throws IllegalArgumentException if <code>inputFilePath</code> is not a file.
   */
  public static DecompressStats decompress(Path inputFilePath) {
    assertFile(inputFilePath);

    DecompressStats decompressStats = new DecompressStats();
    decompress(inputFilePath.toString(), decompressStats);

    return decompressStats;
  }

  /**
   * Decompress a DBC file resulting in a DBF file output.
   *
   * @param inputFilePath {@link Path} to dbc file.
   * @param outputFilePath {@link Path} to dbf result file.
   * @return A {@link DecompressStats} object with information about decompress process.
   * @throws IllegalArgumentException if <code>inputFilePath</code> is not a file.
   */
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

  /** Provides information about decompression process. */
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

    /**
     * @return file output size, it is used to be greater than {@link
     *     DecompressStats#getInputFileSize()}.
     */
    public long getOutputFileSize() {
      return outputFileSize;
    }

    public void setOutputFileSize(long outputFileSize) {
      this.outputFileSize = outputFileSize;
    }

    /**
     * @return output file name, this function is useful when output path is not provided to
     *     decompress.
     * @see DbcNativeDecompressor#decompress(Path)
     */
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

    /**
     * @return status code returned by native library on decompress process.
     */
    public int getDecompressStatusCode() {
      return decompressStatusCode;
    }

    public void setDecompressStatusCode(int decompressStatusCode) {
      this.decompressStatusCode = decompressStatusCode;
    }

    /**
     * @return on decompress processes which targets a directory, this field provides error details
     *     about this instance file stats.
     * @see DbcNativeDecompressor#decompressFromDirectory(Path)
     */
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
