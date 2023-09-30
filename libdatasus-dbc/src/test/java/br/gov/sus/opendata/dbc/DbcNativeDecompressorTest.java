package br.gov.sus.opendata.dbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import br.gov.sus.opendata.dbc.DbcNativeDecompressor.DecompressStats;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Test;

public class DbcNativeDecompressorTest {

  @Test
  public void decompress() throws IOException {
    String dbcPath = getDbcPath();
    DecompressStats stats = DbcNativeDecompressor.decompress(dbcPath);
    assertTrue(stats.getOutputFileSize() > stats.getInputFileSize());
    assertEquals(dbcPath, stats.getInputFileName());
    assertEquals(dbcPath + ".dbf", stats.getOutputFileName());
    Files.deleteIfExists(Paths.get(dbcPath + ".dbf"));
  }

  @Test
  public void decompressTo() throws IOException {
    String dbcPath = getDbcPath();
    String dbfpath = dbcPath + ".dbf";
    DbcNativeDecompressor.DecompressStats stats =
        DbcNativeDecompressor.decompress(dbcPath, dbfpath);
    assertTrue(stats.getOutputFileSize() > stats.getInputFileSize());
    assertEquals(dbcPath, stats.getInputFileName());
    assertEquals(dbfpath, stats.getOutputFileName());
    Files.deleteIfExists(Paths.get(dbfpath));
  }

  @Test
  public void decompressFromDirectory() throws IOException {
    String dbcDirPath = getDbcDirectory();
    String dbcPath = getDbcPath();
    List<DecompressStats> statsList = DbcNativeDecompressor.decompressFromDirectory(dbcDirPath);
    assertFalse(statsList.isEmpty());
    DecompressStats stats = statsList.get(0);
    assertTrue(stats.getOutputFileSize() > stats.getInputFileSize());
    assertEquals(dbcPath, stats.getInputFileName());
    assertEquals(dbcPath + ".dbf", stats.getOutputFileName());
    Files.deleteIfExists(Paths.get(dbcPath + ".dbf"));
  }

  @Test
  public void ShouldErrorWhenPathIsNotFile() {
    String dbcPath = getDbcDirectory();
    assertThrows(IllegalArgumentException.class, () -> DbcNativeDecompressor.decompress(dbcPath));
  }

  @Test
  public void ShouldErrorWhenPathIsNotDirectory() {
    String dbcPath = getDbcPath();
    assertThrows(
        IllegalArgumentException.class,
        () -> DbcNativeDecompressor.decompressFromDirectory(dbcPath));
  }

  private String getDbcPath() {
    return getClass().getClassLoader().getResource("dbc/POBR2023.dbc").getPath();
  }

  private String getDbcDirectory() {
    return getClass().getClassLoader().getResource("dbc/").getPath();
  }
}
