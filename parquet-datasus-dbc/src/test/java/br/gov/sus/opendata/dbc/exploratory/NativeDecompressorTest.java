package br.gov.sus.opendata.dbc.exploratory;

import br.gov.sus.opendata.dbc.NativeDecompressor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NativeDecompressorTest {

    @Test
    public void explore() throws IOException {
        String dbcPath = getDbcPath();
        NativeDecompressor.DecompressStats stats = new NativeDecompressor.DecompressStats();
        NativeDecompressor.decompress(dbcPath, stats);
        assertTrue(stats.getOutputFileSize() > stats.getInputFileSize());
        assertEquals(dbcPath, stats.getInputFileName());
        assertEquals(dbcPath + ".dbf", stats.getOutputFileName());
        Files.deleteIfExists(Paths.get(dbcPath + ".dbf"));
    }

    @Test
    public void exploreTo() throws IOException {
        String dbcPath = getDbcPath();
        String dbfpath = dbcPath + ".dbf";
        NativeDecompressor.DecompressStats stats = new NativeDecompressor.DecompressStats();
        NativeDecompressor.decompressTo(dbcPath, dbfpath, stats);
        assertTrue(stats.getOutputFileSize() > stats.getInputFileSize());
        assertEquals(dbcPath, stats.getInputFileName());
        assertEquals(dbfpath, stats.getOutputFileName());
        Files.deleteIfExists(Paths.get(dbfpath));
    }

    private String getDbcPath() {
        return getClass().getClassLoader().getResource("dbc/POBR2023.dbc").getPath();
    }
}
